package com.aws.ssa.keyspaces.throttler;

import com.datastax.oss.driver.api.core.config.DriverOption;

public enum KeyspacesThrottleOption implements DriverOption {

    KEYSPACES_THROTTLE_NUMBER_OF_HOSTS("advanced.throttler.number-of-hosts"),
    KEYSPACES_THROTTLE_TIMEOUT("advanced.throttler.register-timeout");;

    public static final int DEFAULT_NUMBER_OF_HOSTS = 1;

    private final String path;

    KeyspacesThrottleOption(String path) {
        this.path = path;
    }

    @Override
    public String getPath() {
        return path;
    }



}
