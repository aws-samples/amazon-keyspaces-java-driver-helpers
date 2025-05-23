package com.aws.ssa.keyspaces.loadbalancing;


import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;

public class AmazonKeyspacesLoadbalancingPolicyTest {

    @Test
    public void emptyQueryPlan() {

        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesRoundRobinLoadBalancingPolicy st = new AmazonKeyspacesRoundRobinLoadBalancingPolicy(context, "default");

        assertEquals(QueryPlan.EMPTY, st.newQueryPlan(null, null, new Node[0]));
    }
    @Test
    public void notEmptyQueryPlanExceed() {

        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesRoundRobinLoadBalancingPolicy st = new AmazonKeyspacesRoundRobinLoadBalancingPolicy(context, "default");

        assertNotEquals(QueryPlan.EMPTY, st.newQueryPlan(null, null, new Node[1]));

    }
    @Test
    public void twoNodeShuffle() {

        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesRoundRobinLoadBalancingPolicy st = new AmazonKeyspacesRoundRobinLoadBalancingPolicy(context, "default");

        Integer[] original = new Integer[2];
        Integer[] clone = new Integer[2];

        for(int i=0; i<original.length;i++){
            original[i] = i;
            clone[i] = i;
        }

        Queue<Node> queryPlan = st.newQueryPlan(null, null, original);

        assertEquals(original.length, clone.length);
        assertNotEquals(QueryPlan.EMPTY, queryPlan);

    }
    @Test
    public void largeShuffle() {

        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesRoundRobinLoadBalancingPolicy st = new AmazonKeyspacesRoundRobinLoadBalancingPolicy(context, "default");

        Integer[] original = new Integer[100];
        Integer[] clone = new Integer[100];

        for(int i=0; i<original.length;i++){
            original[i] = i;
            clone[i] = i;
        }

       Queue<Node> queryPlan = st.newQueryPlan(null, null, original);

        assertFalse(Arrays.deepEquals(original,clone));
        assertNotEquals(QueryPlan.EMPTY, queryPlan);
    }
    @Test
    public void testShuffleAlgorithm() {

        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesRoundRobinLoadBalancingPolicy st = new AmazonKeyspacesRoundRobinLoadBalancingPolicy(context, "default");

        Integer[] original = new Integer[100];
        Integer[] clone = new Integer[100];

        for(int i=0; i<original.length;i++){
            original[i] = i;
            clone[i] = i;
        }

        AmazonKeyspacesRoundRobinLoadBalancingPolicy.reverseDurstenfeldShuffle(original, ThreadLocalRandom.current());

        assertFalse(Arrays.deepEquals(original,clone));
    }


    @Test
    public void testConfig() {
        assertEquals("us-east-1", DriverConfigLoader.fromClasspath("loadbalancer-example").getInitialConfig().getDefaultProfile().getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER));
    }

}

