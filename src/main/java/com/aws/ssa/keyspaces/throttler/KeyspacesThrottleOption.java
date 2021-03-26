package com.aws.ssa.keyspaces.throttler;

import com.aws.ssa.keyspaces.core.EndpointType;
import com.datastax.oss.driver.api.core.config.DriverOption;

public enum KeyspacesThrottleOption implements DriverOption {

    KEYSPACES_THROTTLE_ENDPOINT_TYPE("advanced.throttler.endpoint-type"),
    KEYSPACES_THROTTLE_TIMEOUT("advanced.throttler.register-timeout");;

    public static final EndpointType DEFAULT_ENDPOINT_TYPE = EndpointType.VPC;


    private final String path;

    KeyspacesThrottleOption(String path) {
        this.path = path;
    }

    @Override
    public String getPath() {
        return path;
    }



}
