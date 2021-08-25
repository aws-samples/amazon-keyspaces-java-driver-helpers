package com.aws.ssa.keyspaces.export;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import io.netty.handler.timeout.WriteTimeoutException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class test {

    static Semaphore semaphore = new Semaphore(1000);

    public static void main(String[] args) throws Exception {

        CqlSession session = CqlSession.builder().withConfigLoader(DriverConfigLoader.fromClasspath("throttler-example")).build();

        String query = "INSERT INTO tlp_stress.keyvalue (key, value) VALUES (?, '001.20.2779')";

        PreparedStatement t =  session.prepare(query);

        RateLimiter limiter = RateLimiter.create(4000);

        ExecutorService executor = Executors.newFixedThreadPool(10);


        while (true) {


            SimpleStatement simpleStatement = SimpleStatement.builder("INSERT INTO tlp_stress.keyvalue (key, value) VALUES ('"+ThreadLocalRandom.current().nextLong()+"', '001.20.2779')").build();
            simpleStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            simpleStatement.setIdempotent(true);

            BoundStatement st = t.bind(ThreadLocalRandom.current().nextLong() + "");
            st.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            st.setIdempotent(true);

            BatchStatement bs = BatchStatement.builder(BatchType.UNLOGGED).addStatement(simpleStatement).build();

            semaphore.acquireUninterruptibly(1);
            limiter.acquire(1);

            session.executeAsync(bs).whenComplete((resultSet, error) -> {
                if (error instanceof WriteTimeoutException) {
                    System.out.println("hi");
                }
                if (error instanceof DriverException) {
                    System.out.println("no");
                }
                semaphore.release(1);
            });
        }

    }

}
