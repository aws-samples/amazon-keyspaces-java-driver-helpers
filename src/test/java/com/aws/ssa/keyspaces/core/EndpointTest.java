package com.aws.ssa.keyspaces.core;


import com.aws.ssa.keyspaces.retry.AmazonKeyspacesRetryPolicy;
import com.aws.ssa.keyspaces.retry.KeyspacesRetryOption;
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

public class EndpointTest {


    @Test
    public void determineEndpointTestToString() {
        Assert.assertEquals(EndpointType.VPC.toString(), "VPC");
        Assert.assertEquals(EndpointType.PUBLIC.toString(), "PUBLIC");
    }
    @Test
    public void determineEndpointTestPublicName() {
        Assert.assertEquals(EndpointType.VPC.name(), "VPC");
        Assert.assertEquals(EndpointType.PUBLIC.name(), "PUBLIC");
    }
    @Test
    public void determineEndpointTestPublicOrdinal() {
        Assert.assertEquals(EndpointType.PUBLIC.ordinal(), 1);
        Assert.assertEquals(EndpointType.VPC.ordinal(), 0);
    }
    @Test
    public void determineEndpointTestPublicValueOf() {
        Assert.assertEquals(EndpointType.PUBLIC, EndpointType.valueOf("PUBLIC"));
        Assert.assertEquals(EndpointType.VPC, EndpointType.valueOf("VPC"));

    }


}

