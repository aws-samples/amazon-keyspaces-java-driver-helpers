package com.aws.ssa.keyspaces.throttler;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import io.netty.util.internal.ThreadLocalRandom;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class AmazonKeyspacesIntegrationTest {

    private CqlSession session = connectAndCreateSession();

    public static void main(String[] args) throws InterruptedException {
        new AmazonKeyspacesIntegrationTest().executeTest();
    }
    private CqlSession connectAndCreateSession(){
        return  CqlSession.builder().withConfigLoader(DriverConfigLoader.fromClasspath("throttler-example")).build();
    }

    private void executeTest() throws InterruptedException {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "100");

        this.session = connectAndCreateSession();


        PreparedStatement statement = session.prepare("insert into tlp_stress.map_stress(id,data)VALUES(?, ?)");

        PreparedStatement rd = session.prepare("SELECT * FROM tlp_stress.map_stress where id = ?");

        int mapsize = 2;
        long size = 0;
        Map data = new HashMap(mapsize);

        for(int j=0; j<mapsize;j++){
            String key = "" + ThreadLocalRandom.current().nextLong();
            String value = "constant";
            data.put(key,value);
            size += key.length();
            size += value.length();
        }

        RateLimiter limiter = RateLimiter.create(500);

        Stopwatch stopwatch = Stopwatch.createStarted();

        ResultSet resultSet = session.execute(rd.bind("8552296878058997004").setConsistencyLevel(ConsistencyLevel.LOCAL_ONE));

       System.out.println(resultSet.one().getString(0));


        while(stopwatch.elapsed().toMillis() < 5*60*1000){
            String id = "" +  ThreadLocalRandom.current().nextLong();

            limiter.acquire();

            CompletableFuture.supplyAsync(() -> session.execute(statement.bind(id,data)));
        }



        limiter.setRate(10000);

        Stopwatch stopwatch2 = Stopwatch.createStarted();

        while(stopwatch2.elapsed().toMillis() < 15*60*1000){
            String id = "" +  ThreadLocalRandom.current().nextLong();

            limiter.acquire();

            CompletableFuture.supplyAsync(() -> executeTranaction(session, statement, id, data));
        }
        /*
        for(int i =0;i<10000000;i++){

            try {
                String id = "" +  ThreadLocalRandom.current().nextLong();

                semaphore.acquire(1);
                CompletableFuture.supplyAsync(() -> session.execute(statement.bind(id,data)));


                //session.executeAsync(statement.bind(id,data));

            }catch(DriverException t){
                t.printStackTrace();
            }finally {

            }
        }
        */


        session.close();

    }
    private ResultSet executeTranaction(CqlSession session, PreparedStatement statement, String id, Map data){
        try {
            return session.execute(statement.bind(id, data));
        }catch (Throwable t){
            t.printStackTrace();
            return null;
        }
    }



}
