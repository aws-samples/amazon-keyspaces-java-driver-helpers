package com.aws.ssa.keyspaces.loadbalancing;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.loadbalancing.BasicLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.MandatoryLocalDcHelper;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/***
 * AmazonKeyspacesLoadBalancingPolicy is a round robin policy that randomizes host order. While you may see a three to nine
 * node cluster when connecting to Amazon Keyspaces, connections are loadbalanced service side to multiple request handlers. This
 * policy provides even distribution across the driver connection pool. Traditional token-aware policies and latency aware policies
 * are not necessary for good performance in Amazon Keyspaces.
 */
public class AmazonKeyspacesRoundRobinLoadBalancingPolicy extends BasicLoadBalancingPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(AmazonKeyspacesRoundRobinLoadBalancingPolicy.class);

    public AmazonKeyspacesRoundRobinLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
        super(context, profileName);
    }

    public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
        super.init(nodes, distanceReporter);
        LOG.info("Total number of nodes visible to driver: " + ((nodes == null)?0:nodes.size()));
    }

    @NonNull
    protected Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
        return (new MandatoryLocalDcHelper(this.context, this.profile, this.logPrefix)).discoverLocalDc(nodes);
    }

    protected int getInFlight(@NonNull Node node, @NonNull Session session) {
        ChannelPool pool = (ChannelPool)((DefaultSession)session).getPools().get(node);
        return pool == null ? 0 : pool.getInFlight();
    }
    protected int getSize(@NonNull Node node, @NonNull Session session) {
        ChannelPool pool = (ChannelPool)((DefaultSession)session).getPools().get(node);
        return pool == null ? 0 : pool.size();
    }
    /***
     * Fisher–Yates or Richard Durstenfeld shuffle implemented from lowest index to highest
     * https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle#The_modern_algorithm
     * @param currentNodes
     * @return shuffledNodes
     */
    public static void reverseDurstenfeldShuffle(Object[] currentNodes, ThreadLocalRandom random){
        int totalNodes = currentNodes.length;

        for(int currentNodeIndex = 0; currentNodeIndex < totalNodes-1; currentNodeIndex++) {

            int swapNodeIndex = random.nextInt(currentNodeIndex, totalNodes);

            ArrayUtils.swap(currentNodes, currentNodeIndex, swapNodeIndex);
        }
    }
    @NonNull
    public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {

        Object[] currentNodes = this.getLiveNodes().dc(this.getLocalDatacenter()).toArray();

        Queue<Node> queryPlan = newQueryPlan(request, session, currentNodes);

        int totalNodes = currentNodes.length;

        if (LOG.isTraceEnabled()) {
            if (totalNodes > 0) {
                //int currentSize = getSize((Node) currentNodes[0], session);

                int inflight = getInFlight((Node) currentNodes[0], session);

                String firstNode = ((Node) currentNodes[0]).getEndPoint().toString();

                int openConnections = ((Node) currentNodes[0]).getOpenConnections();

                int requestPerMostUsedConnection = (openConnections > 0) ? (inflight / openConnections) : 0;

                LOG.trace(" Total local nodes: [{}], First Node [{}], Number of Connections: [{}], Total inflight:[{}], Number of Request per connection: [{}]", totalNodes, firstNode, openConnections, inflight, requestPerMostUsedConnection);
            }
        }
        return queryPlan;
    }

    @NonNull
    public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session, Object[] currentNodes ) {
        int totalNodes = currentNodes.length;

        if(totalNodes == 0) {

            LOG.trace(" Total local nodes is 0, returning empty query plan");

            return this.maybeAddDcFailover(request, QueryPlan.EMPTY);
        }
        if(totalNodes > 1){

            ThreadLocalRandom random = ThreadLocalRandom.current();

            reverseDurstenfeldShuffle(currentNodes, random);
        }

        QueryPlan plan = new SimpleQueryPlan(currentNodes);

        return this.maybeAddDcFailover(request, (Queue) plan);
    }
}
