package com.aws.ssa.keyspaces.throttler;

import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.BurstyRateLimiterFactory;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
    * A request throttler that limits the rate of requests per second. The limit is configurable through client driver
    * configuration. The rate of request is controlled by Guava SmoothBursty Ratelimiter that allows two minutes of capacity to
    * aggregate if not used. The second limiter is dynamically configured based on the number of connections defined in the pool setting
    * that limits overall throughput during burst behavior. The limiter will control the number of cql request per second but expects the table
    * to have proper capacity for the table.
    *
    * This is a blocking implementation but it will timeout based on the configured request timeout
    *
    * The most well known usecase for this rate limiter is bulk loading data at consistent rates or batch processing.
    *
     * <p>To activate this throttler, modify the {@code advanced.throttler} section in the driver
     * configuration, for example:
     *
     * <pre>
     * datastax-java-driver {
     *    advanced.throttler = {
     *           class = com.aws.ssa.keyspaces.throttler.AmazonKeyspacesFixedRateThrottler
     *           max-requests-per-second = 1000
     *           endpoint-type = VPC
     *           register-timeout = 3 seconds
     *     }
     * }
     * </pre>
     *  max-requests-per-second : the number of CQL request per second max. Average over 2 minutes
     *  endpoint-type : connected through private endpoint or public endpoint
     *  register-timeout : time to wait for permits from limiter. Should be less than request timeout
     *
     */
@ThreadSafe
public class AmazonKeyspacesFixedRateThrottler implements RequestThrottler {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonKeyspacesFixedRateThrottler.class);

    private final String logPrefix;

    public static int REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND_DEFAULT = 1000;

    /*** Amazon Keyspaces supports up to 3000 CQL queries per TCP connection per second, but there is no limit on
     * the number of connections a driver can establish. The Cassandra drivers establish a
     * connection pool to Cassandra and load balance queries over that pool of connections. The following setting is
     * set to 2000 request per second to allow for some overhead ***/
    public static int REQUEST_PER_CONNECTION_DEFAULT = 2000;

    /*** Amazon Keyspaces will store some unused capacity which allow for spikes in traffic to bust above the provisioned rate.
     * The rate limiter will generate configured maxRequestRate every second, and will be usable for two minutes   ***/
    public static int REQUEST_BURST_CAPACITY_IN_SECONDS = 120;

    /***
     * Amazon Keyspaces exposes 9 peer IP addresses to drivers, and the default behavior of most drivers is to establish a single connection to each peer IP address.
     */
    public static int PUBLIC_ENDPOINT_DEFAULT_HOST = 9;

    /***
     * Amazon Keyspaces Virtual Private Cloud Endpoint (VPCE) exposes host per availability zone. The default behavior will establish one connection to each peer IP address.
     * the driver.
     */
    public static int VPC_ENDPOINT_DEFAULT_HOST = 2;

    /***
     * Rate limiter used to meter the CQL Request Per Second up to maxRequestsPerSecond
     */
    private final RateLimiter limiter;

    /***
     * Rate limiter used to meter the CQL Request Per Second up to the total number of numberOfConnections
     */
    private final RateLimiter maxConnectionsLimiter;

    /***
     * Number of hosts available when creating to a new session
     */
    private final Integer numberOfHosts;

    /***
     * Configured Rate of desired throughput
     */
    private long maxRequestsPerSecond;

    /***
     * Configured timeout per operation or time to wait for permits from ratelimiter
     */
    private long registerTimeoutInMs;

    /***
     * Configured number of connections for each host IP available
     */
    private int numberOfConnectionsPerHost;

    /*** Default constructor that takes in values from the configuration ***/
        public AmazonKeyspacesFixedRateThrottler(DriverContext context) {
            this(context,
                    context.getConfig()
                            .getDefaultProfile()
                            .getLong(DefaultDriverOption.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND,
                            REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND_DEFAULT),
                    context.getConfig()
                            .getDefaultProfile()
                            .getDuration(KeyspacesThrottleOption.KEYSPACES_THROTTLE_TIMEOUT,context.getConfig()
                            .getDefaultProfile()
                            .getDuration(DefaultDriverOption.REQUEST_TIMEOUT)).toMillis(),
                    context.getConfig()
                            .getDefaultProfile()
                            .getInt(KeyspacesThrottleOption.KEYSPACES_THROTTLE_NUMBER_OF_HOSTS, KeyspacesThrottleOption.DEFAULT_NUMBER_OF_HOSTS),
                    context.getConfig()
                            .getDefaultProfile()
                            .getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE));
        }


        /*** Initialization of the Throttler ***/
        public AmazonKeyspacesFixedRateThrottler(DriverContext context, long maxRequestsPerSecond, long registerTimeoutInMs
                , int numberOfHosts, int numberOfConnectionsPerHost) {
            this.logPrefix = context.getSessionName();

            this.maxRequestsPerSecond = maxRequestsPerSecond;

            this.registerTimeoutInMs = registerTimeoutInMs;

            this.numberOfHosts = numberOfHosts;

            this.numberOfConnectionsPerHost = numberOfConnectionsPerHost;

            //must be greater than 0
            if(this.maxRequestsPerSecond <= 0){
                LOG.error(
                        "[{}]  Throttler max request per second (advanced.throttler.max-requests-per-second) must be set greater than zero, currently {}",
                        logPrefix,
                        maxRequestsPerSecond);

                throw new IllegalArgumentException("Throttler maxRequestsPerSecond (advanced.throttler.max-requests-per-second) must be set greater than zero, currently " + maxRequestsPerSecond );
            }

            //Requires some timeout
            if(this.registerTimeoutInMs <= 0){
                LOG.error(
                        "[{}]  Throttler register timeout (advanced.throttler.register-timeout) must be set greater than zero, currently {}",
                        logPrefix,
                        registerTimeoutInMs);

                throw new IllegalArgumentException("Invalid timeout for registerTimeoutInMs (advanced.throttler.register-timeout) must be set greater or equal to zero, currently " + registerTimeoutInMs );
            }
            long requestTimeout = context.getConfig()
                            .getDefaultProfile()
                            .getDuration(DefaultDriverOption.REQUEST_TIMEOUT).toMillis();

            if(this.registerTimeoutInMs > requestTimeout){
                LOG.error(
                        "[{}]  Throttler register timeout (advanced.throttler.register-timeout) must be less than or equal to request-timeout (basic.request.timeout), currently {}",
                        logPrefix,
                        registerTimeoutInMs);

                throw new IllegalArgumentException("Invalid timeout set for registerTimeoutInMs (advanced.throttler.register-timeout) must be set greater or equal to request timeout (basic.request.timeout), register timeout:" + registerTimeoutInMs + "ms , request timeout:"+ requestTimeout +" ms");

            }


            LOG.info(
                    "[{}] Initializing with maxRequestsPerSecond = {} and registerTimeoutInMs = {}",
                    logPrefix,
                    maxRequestsPerSecond,
                    registerTimeoutInMs);

            LOG.info(
                    "[{}] Based on Throttler max of {} request per second, the recommended number of connections for number of hosts: {}, currently {}",
                    logPrefix,
                    maxRequestsPerSecond,
                    simpleConnectionRecommendation(maxRequestsPerSecond, numberOfHosts),
                    numberOfConnectionsPerHost);

            LOG.info(
                    "[{}] Based on Throttler max of {} request per second, the recommended number of connections for the Public Endpoint is: {}, currently {}",
                    logPrefix,
                    maxRequestsPerSecond,
                    simpleConnectionRecommendation(maxRequestsPerSecond, numberOfHosts),
                    numberOfConnectionsPerHost);


            //The rate for the number of request per second based on the number of connections. Should be greater than maxRequestsPerSecond
            int maxRequestPerSecondByForConnections = calculateConnectionMaxRequestPerSecond(numberOfHosts, numberOfConnectionsPerHost);

            if(maxRequestPerSecondByForConnections < maxRequestsPerSecond){
                LOG.warn(
                        "[{}] Cannot reach Max Request Per Second of {}. Specified number of hosts {}, and number of connections {} will provide at most {} request per second. Try increasing advanced.connection.pool.local.size or check system.peers table fo the number of hosts ",
                        logPrefix,
                        maxRequestsPerSecond,
                        numberOfHosts,
                        numberOfConnectionsPerHost,
                        maxRequestPerSecondByForConnections);

                throw new IllegalArgumentException("maxRequestsPerSecond (advanced.throttler.max-requests-per-second) greater than throughput for numberOfConnectionsPerHost (advanced.connection.pool.local.size): " + numberOfConnectionsPerHost );
            }

            //Aggregate permits over two minutes to allow for burst of unused capacity
            this.limiter = BurstyRateLimiterFactory.create(maxRequestsPerSecond, REQUEST_BURST_CAPACITY_IN_SECONDS);

            //Fixed number of permits that expire every second. Ceiling with no bursting
            this.maxConnectionsLimiter = RateLimiter.create(maxRequestPerSecondByForConnections);

        }

    /***
     * Calculate the number of request per second based on the number of connections, number of hosts, and 2000 request per second.
     * @param numberOfHosts Number of hosts in the peers table. Depends on region and end point
     * @param numberOfConnectionsPerHost Number of connections for each host ip
     * @return max rate per second based on the number of connections
     */
        public static int calculateConnectionMaxRequestPerSecond(Integer numberOfHosts, int numberOfConnectionsPerHost){
            return  numberOfConnectionsPerHost * numberOfHosts * REQUEST_PER_CONNECTION_DEFAULT;
        }

    /***
     * Calculate recommended connections based on the current maxRequestRate specified. Max Connections rate should be greater than configured max request rate
     * @param maxRequestsPerSecond number of configured request per second
     * @param numberOfHosts type of endpoint that will be used to identify the number of hosts
     * @return
     */
        public static double simpleConnectionRecommendation(long maxRequestsPerSecond, Integer numberOfHosts){

            return Math.max(1.0, Math.ceil(maxRequestsPerSecond/(double)(numberOfHosts * REQUEST_PER_CONNECTION_DEFAULT)));
        }

    /***
     * Blocking Rate limiter on register. Will timeout based on the configured timeout.
     * @param request
     */
    @Override
        public void register(@NonNull Throttled request) {

            long startTime = System.currentTimeMillis();

            //fail if connections not available which should be higher limit than maxRequestRate
           if(maxConnectionsLimiter.tryAcquire(1, registerTimeoutInMs, TimeUnit.MILLISECONDS) == false){
               fail(request, String.format("Timeout waiting for connection permits. Increase number of connections. request timeout: %d seconds)", this.maxRequestsPerSecond, this.registerTimeoutInMs));
           }

            long elapsedTime = System.currentTimeMillis() - startTime;

            //registerTimeoutInMs should account for acquiring from both limiters. Ensure that this value is greater than or equal to 0
            long timeoutForRequestPermits = (elapsedTime>=registerTimeoutInMs)?0:registerTimeoutInMs - elapsedTime;

            if(limiter.tryAcquire(1, timeoutForRequestPermits, TimeUnit.MILLISECONDS)){
                request.onThrottleReady(false);
            }else{
                fail(request, String.format("Timeout waiting for rate permits. Increase maxRequestsPerSecond (current maxrequests/s: %d, request timeout: %d seconds)", this.maxRequestsPerSecond, this.registerTimeoutInMs));
            }
        }

        private static void fail(Throttled request, String message) {
            request.onThrottleFailure(new RequestThrottlingException(message));
        }

        @Override
        public void signalSuccess(@NonNull Throttled request) {
           //nothing to do
        }

        @Override
        public void signalError(@NonNull Throttled request, @NonNull Throwable error) {
            LOG.warn(logPrefix + " signalError Throttled Request", error);
        }

        @Override
        public void signalTimeout(@NonNull Throttled request) {
            LOG.warn( "[{}] Timeout Throttled Request signalTimeout", logPrefix);
        }

        @Override
        public void close() { }

        public long getMaxRequestsPerSecond(){
            return this.maxRequestsPerSecond;
        }

        public void setMaxRequestsPerSecond(long maxRequestsPerSecond){
            this.maxRequestsPerSecond = maxRequestsPerSecond;
            limiter.setRate(maxRequestsPerSecond);
        }
    }
