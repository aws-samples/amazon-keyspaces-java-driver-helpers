package com.aws.ssa.keyspaces.retry;

import com.aws.ssa.keyspaces.retry.KeyspacesRetryOption;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class KeyspacesRetryOptionTest {

    @Test
    public void testPathMethod() {
        assertEquals("advanced.retry-policy.max-attempts", KeyspacesRetryOption.KEYSPACES_RETRY_MAX_ATTEMPTS.getPath());
    }

    @Test
    public void testDefaults() {
        assertSame(3, KeyspacesRetryOption.DEFAULT_KEYSPACES_RETRY_MAX_ATTEMPTS);
    }
}
