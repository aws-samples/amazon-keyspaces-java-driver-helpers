package com.aws.ssa.keyspaces.retry;


import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import org.junit.Assert;
import org.junit.Test;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;

public class AmazonKeyspacesExponentialRetryPolicyTest {

    @Test
    public void determineRetryDecisionExceed() {

        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);

        Assert.assertEquals(RetryDecision.RETHROW, st.determineRetryDecision(4));

    }
    @Test
    public void determineRetryTimeToMinWhenRetry() {

        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());

        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 1);

        Stopwatch stopwatch = Stopwatch.createStarted();

        st.determineRetryDecision(0);

        stopwatch.stop();

        long millsObserved = stopwatch.elapsed().toMillis();

        Assert.assertTrue(millsObserved > 1 && millsObserved < 20);

    }
    @Test
    public void determineRetryTimeToMinWhenNotRetry() {

        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());

        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 0);

        Stopwatch stopwatch = Stopwatch.createStarted();

        st.determineRetryDecision(0);

        stopwatch.stop();

        long millsObserved = stopwatch.elapsed().toMillis();

        System.out.println(millsObserved);

        Assert.assertTrue(millsObserved >= 0 && millsObserved < 5);

    }
    @Test
    public void determineRetryTimeToMinWait() {

        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());

        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 1);

        Stopwatch stopwatch = Stopwatch.createStarted();

        st.timeToWait(0);

        stopwatch.stop();

        long millsObserved = stopwatch.elapsed().toMillis();

        Assert.assertTrue(millsObserved > 1 && millsObserved < 20);

    }
    @Test
    public void determineRetryTimeToWaitMax() {

        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());

        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 1);

        Stopwatch stopwatch = Stopwatch.createStarted();

        st.timeToWait(100000);

        stopwatch.stop();

        long millsObserved = stopwatch.elapsed().toMillis();

        System.out.println(millsObserved);

        Assert.assertTrue(millsObserved > 999 && millsObserved < 1010);

    }

    @Test
    public void determineRetryDecisionMin() {
        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);

        Assert.assertEquals(RetryDecision.RETRY_SAME, st.determineRetryDecision(0));

    }

    @Test
    public void determineRetryDecisionMid() {
        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);

        Assert.assertEquals(RetryDecision.RETRY_SAME, st.determineRetryDecision(1));

    }

    @Test
    public void onWriteTimeout() {
        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);

        Assert.assertEquals(RetryDecision.RETRY_SAME, st.onWriteTimeout(null, ConsistencyLevel.LOCAL_QUORUM, WriteType.SIMPLE, 2, 0, 1));

    }

    @Test
    public void onWriteTimeoutExceed() {
        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);

        Assert.assertEquals(RetryDecision.RETHROW, st.onWriteTimeout(null, ConsistencyLevel.LOCAL_QUORUM, WriteType.SIMPLE, 2, 0, 4));

    }

    @Test
    public void onReadTimeout() {
        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);

        Assert.assertEquals(RetryDecision.RETRY_SAME, st.onReadTimeout(null, ConsistencyLevel.LOCAL_QUORUM, 2, 0, false, 1));

    }

    @Test
    public void onReadTimeoutExceed() {
        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);

        Assert.assertEquals(RetryDecision.RETHROW, st.onReadTimeout(null, ConsistencyLevel.LOCAL_QUORUM, 2, 0, false, 4));

    }

    @Test
    public void oneError() {
        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);

        Assert.assertEquals(RetryDecision.RETRY_SAME, st.onErrorResponse(null, new WriteFailureException(null, ConsistencyLevel.LOCAL_QUORUM, 0, 2, WriteType.SIMPLE, 1, null), 2));

    }

    @Test
    public void oneErrorExceed() {
        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);


        Assert.assertEquals(RetryDecision.RETHROW, st.onErrorResponse(null, new WriteFailureException(null, ConsistencyLevel.LOCAL_QUORUM, 0, 2, WriteType.SIMPLE, 1, null), 4));

    }

    @Test
    public void onUnavailable() {
        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);

        Assert.assertEquals(RetryDecision.RETRY_SAME, st.onUnavailable(null, ConsistencyLevel.LOCAL_QUORUM, 1, 1, 2));

    }

    @Test
    public void onUnavailableExceed() {
        DriverContext context = new DefaultDriverContext(new DefaultProgrammaticDriverConfigLoaderBuilder().build(), ProgrammaticArguments.builder().build());
        AmazonKeyspacesExponentialRetryPolicy st = new AmazonKeyspacesExponentialRetryPolicy(context, 3);


        Assert.assertEquals(RetryDecision.RETHROW, st.onUnavailable(null, ConsistencyLevel.LOCAL_QUORUM, 1, 1, 4));
    }

    @Test
    public void testConfig() {
        Assert.assertEquals(3, DriverConfigLoader.fromClasspath("retry-example").getInitialConfig().getDefaultProfile().getInt(KeyspacesRetryOption.KEYSPACES_RETRY_MAX_ATTEMPTS, KeyspacesRetryOption.DEFAULT_KEYSPACES_RETRY_MAX_ATTEMPTS));
    }

}

