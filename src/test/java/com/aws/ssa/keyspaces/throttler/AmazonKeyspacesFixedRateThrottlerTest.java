package com.aws.ssa.keyspaces.throttler;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class AmazonKeyspacesFixedRateThrottlerTest {

    @Test
    public void simpleConnectionReconmendationTest() {
        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
        AmazonKeyspacesFixedRateThrottler st = new AmazonKeyspacesFixedRateThrottler(context, 3, 2, 9, 2);

        assertEquals(1.0, st.simpleConnectionRecommendation(3500, 9), 0.0);

        assertEquals(2.0, st.simpleConnectionRecommendation(3500, 1), 0.0);

        assertEquals(3.0, st.simpleConnectionRecommendation(40000, 9), 0.0);

        assertEquals(20, st.simpleConnectionRecommendation(40000, 1), 0.0);

        assertEquals(7, st.simpleConnectionRecommendation(125000, 9), 0.0);

        assertEquals(63, st.simpleConnectionRecommendation(125000, 1), 0.0);

    }

    @Test
    public void requestPerSecondDefault() {

        assertEquals(1000, AmazonKeyspacesFixedRateThrottler.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND_DEFAULT);

    }

    @Test
    public void requestPerSecondConfig() {

        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
        AmazonKeyspacesFixedRateThrottler st = new AmazonKeyspacesFixedRateThrottler(context);

        assertEquals(1000, st.getMaxRequestsPerSecond());

    }
    @Test
    public void requestPerSecondProgramatic() {

        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
        AmazonKeyspacesFixedRateThrottler st = new AmazonKeyspacesFixedRateThrottler(context, 3, 2, 9, 2);

        assertEquals(3, st.getMaxRequestsPerSecond());

    }
    @Test
    public void requestPerSecondBadValueException() {
        assertThrows(IllegalArgumentException.class, () -> {
            DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
            new AmazonKeyspacesFixedRateThrottler(context, -1, 2, 9, 2);
        });
    }
    @Test
    public void timeoutBadValueException() {
        assertThrows(IllegalArgumentException.class, () -> {
            DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
            new AmazonKeyspacesFixedRateThrottler(context, 1000, -2, 9, 2);
        });
    }
    @Test
    public void timeOutGreaterThenRequestTimeout() {
        assertThrows(IllegalArgumentException.class, () -> {
            DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
            new AmazonKeyspacesFixedRateThrottler(context, 1000, 10000, 9, 2);
        });
    }
    @Test
    public void requestPerConnectionLessThanThroughput() {
        assertThrows(IllegalArgumentException.class, () -> {
            DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());
            new AmazonKeyspacesFixedRateThrottler(context, 10000, 1000, 1, 2);
        });
    }
}
