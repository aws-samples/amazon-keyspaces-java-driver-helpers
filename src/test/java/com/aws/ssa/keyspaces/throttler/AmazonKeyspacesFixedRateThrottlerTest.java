package com.aws.ssa.keyspaces.throttler;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import org.junit.Assert;
import org.junit.Test;

public class AmazonKeyspacesFixedRateThrottlerTest {

    @Test
    public void simpleConnectionReconmendationTest() {
        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
        AmazonKeyspacesFixedRateThrottler st = new AmazonKeyspacesFixedRateThrottler(context, 3, 2, 9, 2);

        Assert.assertEquals(1.0, st.simpleConnectionRecommendation(3500, 9), 0.0);

        Assert.assertEquals(2.0, st.simpleConnectionRecommendation(3500, 1), 0.0);

        Assert.assertEquals(3.0, st.simpleConnectionRecommendation(40000, 9), 0.0);

        Assert.assertEquals(20, st.simpleConnectionRecommendation(40000, 1), 0.0);

        Assert.assertEquals(7, st.simpleConnectionRecommendation(125000, 9), 0.0);

        Assert.assertEquals(63, st.simpleConnectionRecommendation(125000, 1), 0.0);

    }

    @Test
    public void requestPerSecondDefault() {

        Assert.assertEquals(1000, AmazonKeyspacesFixedRateThrottler.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND_DEFAULT);

    }

    @Test
    public void requestPerSecondConfig() {

        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
        AmazonKeyspacesFixedRateThrottler st = new AmazonKeyspacesFixedRateThrottler(context);

        Assert.assertEquals(1000, st.getMaxRequestsPerSecond());

    }
    @Test
    public void requestPerSecondProgramatic() {

        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
        AmazonKeyspacesFixedRateThrottler st = new AmazonKeyspacesFixedRateThrottler(context, 3, 2, 9, 2);

        Assert.assertEquals(3, st.getMaxRequestsPerSecond());

    }
    @Test(expected = IllegalArgumentException.class)
    public void requestPerSecondBadValueException() {

        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
        AmazonKeyspacesFixedRateThrottler st = new AmazonKeyspacesFixedRateThrottler(context, -1, 2, 9, 2);

    }
    @Test(expected = IllegalArgumentException.class)
    public void timeoutBadValueException() {

        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
        AmazonKeyspacesFixedRateThrottler st = new AmazonKeyspacesFixedRateThrottler(context, 1000, -2, 9, 2);

    }
    @Test(expected = IllegalArgumentException.class)
    public void timeOutGreaterThenRequestTimeout() {

        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
        AmazonKeyspacesFixedRateThrottler st = new AmazonKeyspacesFixedRateThrottler(context, 1000, 10000, 9, 2);

    }
    @Test(expected = IllegalArgumentException.class)
    public void requestPerConnectionLessThanThroughput() {

        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
        AmazonKeyspacesFixedRateThrottler st = new AmazonKeyspacesFixedRateThrottler(context, 10000, 1000, 1, 2);

    }
}
