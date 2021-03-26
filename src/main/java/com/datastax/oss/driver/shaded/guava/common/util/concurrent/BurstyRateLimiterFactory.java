package com.datastax.oss.driver.shaded.guava.common.util.concurrent;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter.SleepingStopwatch;

public class BurstyRateLimiterFactory {

    public static RateLimiter create(double permitsPerSecond, double maxBurstSeconds) {

        return create(SleepingStopwatch.createFromSystemTimer(), permitsPerSecond, maxBurstSeconds);
    }

    public static RateLimiter create(SleepingStopwatch stopwatch, double permitsPerSecond, double maxBurstSeconds) {
        RateLimiter rateLimiter = new SmoothRateLimiter.SmoothBursty(stopwatch, maxBurstSeconds /* maxBurstSeconds */);
        rateLimiter.setRate(permitsPerSecond);
        return rateLimiter;
    }

}
