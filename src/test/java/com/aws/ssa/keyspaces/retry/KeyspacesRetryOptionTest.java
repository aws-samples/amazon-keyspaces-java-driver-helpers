package com.aws.ssa.keyspaces.retry;

import org.junit.Assert;
import org.junit.Test;

public class KeyspacesRetryOptionTest {

    @Test
    public void testPathMethod() {
        Assert.assertEquals("advanced.retry-policy.max-attempts", KeyspacesRetryOption.KEYSPACES_RETRY_MAX_ATTEMPTS.getPath());
    }

    @Test
    public void testDefaults() {
        Assert.assertSame(3, KeyspacesRetryOption.DEFAULT_KEYSPACES_RETRY_MAX_ATTEMPTS);
    }
}
