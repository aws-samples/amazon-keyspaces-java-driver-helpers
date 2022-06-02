package com.aws.ssa.keyspaces.validation;

import com.aws.ssa.keyspaces.retry.AmazonKeyspacesRetryPolicy;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class SessionValidator {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonKeyspacesRetryPolicy.class);

    public SessionValidator(DriverContext context) {

        validateConfiguration(context);
    }

    public SessionValidator(){
    };

    public void validateConfiguration(DriverContext context){

        DriverExecutionProfile profile = context.getConfig().getDefaultProfile();

        if(isHostValidationTrue(profile)){
            LOG.warn ("Amazon Keyspaces validator: hostname-validation should be set to 'false': advanced.ssl-engine-factory.hostname-validation = false");
        }
        if(isReconnectOnInitTrue(profile)){
            LOG.warn ("Amazon Keyspaces validator: recommended to set reconnect-on-init to 'true': advanced.reconnect-on-init = true");
        }
        if(isUsingTheDefaultPolicy(profile)){
            LOG.warn ("Amazon Keyspaces validator: recommended to change the retry policy from default to AmazonKeyspacesRetryPolicy or AmazonKeyspacesExponentialRetryPolicy: advanced.retry-policy.class");
        }
        if(isValidDC(profile)){
            LOG.warn ("Amazon Keyspaces validator: localDC name not found in local list of regions: basic.load-balancing-policy.local-datacenter");
        }
        if(validContactPoints(profile)){
            LOG.warn ("Amazon Keyspaces validator: required to specify one contact point: basic.contact-points");
        }
        if(isProtocolVersionNot4(profile)){
            LOG.warn ("Amazon Keyspaces validator: recommended to use protocol version 4: advanced.protocol.version = v4");
        }
        if(isRemoteConnectionPoolSizeSetHigh(profile)){
            LOG.warn ("Amazon Keyspaces validator: Number of remote connections specified is greater than one. Remote connections are not currently used in Amazon Keyspaces: advanced.connection.pool.remote.size = 1");
        }
        if(isMaxNumberOfRequestPerConnectionSetHigh(profile)){
            LOG.warn ("Amazon Keyspaces validator: Number of request per connection is greater than connection throughput quota for Amazon Keyspaces. Set request per connection to 3000. advanced.connection.max-requests-per-connection = 3000");
        }

        LOG.info("Amazon Keyspaces validator: To calculate the available throughput per session" +
                " multiply (number of connections configured * 3000 * number of host in system.peers table)" +
                ". Currently {} connection(s) specified in configuration: advanced.connection.pool.local.size", localConnectionPoolSize(profile));

        /***
         * advanced.ssl-engine-factory {
         *       class = DefaultSslEngineFactory
         *       #truststore-path = "/Users/user/.cassandra/cassandra_truststore.jks"
         *       #truststore-password = "amazon"
         *       hostname-validation = false
         *     }
         */


    }
    public static void validateSetup(CqlSession session){

        CompletionStage<String> partitionerResultSetFuture =  retrievePartitioner(session);

        partitionerResultSetFuture.whenComplete(
                (resultSet, error) -> {
                    if (error != null) {
                        LOG.warn ("Amazon Keyspaces validator: error retrieving partitioner from system.local table: {}", error.getMessage());
                    } else {
                        LOG.info("Amazon Keyspaces validator: detected partitioner {}", resultSet);
                    }
                });

        CompletionStage<Integer> resultSetFuture =  validatePeersAsync(session);

        resultSetFuture.whenComplete(
                (resultSet, error) -> {
                    if (error != null) {
                        LOG.warn ("Amazon Keyspaces validator: error retrieving peers from system.peers table: {}", error.getMessage());
                    } else {
                        if(resultSet == 0){
                            LOG.warn("Amazon Keyspaces validator: no peers found");
                        }else if(resultSet == 1){
                            LOG.warn("Amazon Keyspaces validator: only one peer detected. With proper setup. You should have one peer per availability zone in your vpc or 9 if you are using the public endpoint.");
                        }else {
                            LOG.info("Amazon Keyspaces validator: total number of peers {}", resultSet);
                        }

                    }
                });
    }
    private static Boolean isMaxNumberOfRequestPerConnectionSetHigh(DriverExecutionProfile profile){
        Integer maxRequestPerConnection = profile.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS);

        return maxRequestPerConnection > 3000;
    }
    private static Boolean isRemoteConnectionPoolSizeSetHigh(DriverExecutionProfile profile){
        Integer remoteConnectionPoolSize = profile.getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE);

        return remoteConnectionPoolSize > 1;
    }
    private static Integer localConnectionPoolSize(DriverExecutionProfile profile){

        Integer localConnectionPoolSize = profile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE);

        return localConnectionPoolSize;
    }
    private static Boolean isProtocolVersionNot4(DriverExecutionProfile profile){

        String protocolVersion = profile.getString(DefaultDriverOption.PROTOCOL_VERSION, null);

        return !(protocolVersion == null || protocolVersion.equals("V4"));
    }
    private static Boolean validContactPoints(DriverExecutionProfile profile){

        List<String> contactPoints = profile.getStringList(DefaultDriverOption.CONTACT_POINTS);

        return contactPoints.size() != 1;
    }
    private static Boolean isValidDC(DriverExecutionProfile profile){
        String localDc = profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER);

        return !Arrays.asList("us-east-1", "us-east-2").contains(localDc);
    }
    private static Boolean isUsingTheDefaultPolicy(DriverExecutionProfile profile){
        String retryPolicyClassName = profile.getString(DefaultDriverOption.RETRY_POLICY_CLASS);

        return retryPolicyClassName.equals(DefaultRetryPolicy.class.getName());
    }
    private static Boolean isHostValidationTrue(DriverExecutionProfile profile){

        Boolean hostNameValidation = profile.getBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION);

        return hostNameValidation == true;
    }
    private static Boolean isReconnectOnInitTrue(DriverExecutionProfile profile){

        Boolean reconnectOnInit = profile.getBoolean(DefaultDriverOption.RECONNECT_ON_INIT);

        return reconnectOnInit != true;
    }

    protected static CompletionStage<String> retrievePartitioner(CqlSession session){

        CompletionStage<AsyncResultSet> resultSetFuture =
                session.executeAsync("SELECT * FROM system.local");


        return resultSetFuture.thenCompose(rs -> {
            return CompletableFuture.completedFuture(rs.one().getString("partitioner"));
        });

    }
    public static CompletionStage<Integer> validatePeersAsync(CqlSession session){

        CompletionStage<AsyncResultSet> resultSetFuture =
                session.executeAsync("SELECT * FROM system.peers");

        return resultSetFuture.thenCompose(rs -> countRows(rs, 0));

    }
    private static CompletionStage<Integer> countRows(AsyncResultSet resultSet, int previousPagesCount) {
        int count = previousPagesCount;

        for (Row row : resultSet.currentPage()) {
            count += 1;
        }
        if (resultSet.hasMorePages()) {
            int finalCount = count; // need a final variable to use in the lambda below
            return resultSet.fetchNextPage().thenCompose(rs -> countRows(rs, finalCount));
        } else {
            return CompletableFuture.completedFuture(count);
        }
    }
}
