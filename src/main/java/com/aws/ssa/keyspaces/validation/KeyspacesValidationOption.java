package com.aws.ssa.keyspaces.validation;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverOption;

public enum KeyspacesValidationOption implements DriverOption {

    KEYSPACES_VALIDATION_CLASS("advanced.validation.class");


    private final String path;

    KeyspacesValidationOption(String path) {
        this.path = path;
    }

    @Override
    public String getPath() {
        return path;
    }

}
