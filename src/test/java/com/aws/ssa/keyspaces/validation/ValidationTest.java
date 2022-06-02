package com.aws.ssa.keyspaces.validation;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ValidationTest {

    @Test
    public void testRetryConfigurationExample() {
        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("retry-example"), ProgrammaticArguments.builder().build());

        SessionValidator st = new SessionValidator(context);

    }
    @Test
    public void testThrottlerConfigurationExample() {
        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("throttler-example"), ProgrammaticArguments.builder().build());

        SessionValidator st = new SessionValidator(context);

    }
    @Test
    public void testValidationConfigurationExample() {
        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("validation-example"), ProgrammaticArguments.builder().build());

        SessionValidator st = new SessionValidator(context);

    }
    @Test
    public void validationTest() {
        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("validation-example"), ProgrammaticArguments.builder().build());

        CqlSession session = CqlSession.builder().withConfigLoader(context.getConfigLoader()).build();

        SessionValidator st = new SessionValidator(context);

        st.validateSetup(session);

        for(int i =0;i< 5;i++){
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }

    }
    @Test
    public void validationTestPass() {
        DriverContext context = new DefaultDriverContext(DriverConfigLoader.fromClasspath("validation-example"), ProgrammaticArguments.builder().build());

        CqlSession session = CqlSession.builder().withConfigLoader(context.getConfigLoader()).build();



    }

}
