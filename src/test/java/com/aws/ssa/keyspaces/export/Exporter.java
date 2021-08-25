package com.aws.ssa.keyspaces.export;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class Exporter {

    private  static BlockingQueue<Map<ByteBuffer,  List<Row>>> blockingQueue = new LinkedBlockingDeque<>(2000);

    private static int PAGE_SIZE = 5000;

    private static String query = "";

    private static ByteBuffer pagingState = null;

    private static AtomicLong count = new AtomicLong(0L);

    private static AtomicLong queueCount = new AtomicLong(0L);

    private RateLimiter limiter = RateLimiter.create(10000);

    public static void main(String[] args){


        //query = args[0];

        query = "SELECT * FROM tlp_stress.counter_wide";

        SimpleStatement t = new SimpleStatementBuilder(query).build();

        t.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        t.setIdempotent(true);
        t.setPageSize(PAGE_SIZE);


        CqlSession session = CqlSession.builder().withConfigLoader(DriverConfigLoader.fromClasspath("throttler-example")).build();

       new Exporter().saveAllRows(session, t);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future task = executor.submit(() -> {

            String threadName = Thread.currentThread().getName();
            System.out.println("Hello " + threadName);

            do{
                try {
                    Map<ByteBuffer, List<Row>> results = blockingQueue.poll(10000, TimeUnit.MILLISECONDS);
                    //results.entrySet().forEach();
                    if (results != null) {
                        results.entrySet().forEach(x -> {
                            x.getValue().forEach(y -> {
                                queueCount.incrementAndGet();
                            });
                        });
                        //System.out.println(results);
                    }
                }catch (Exception ex){

                }
                System.out.println("Fin " + count + " " + queueCount);
            }while(true);
        });

        while(task.isDone()!=true){
            try{
                Thread.sleep(100);
            }catch (Exception ex){

            }
        }
        System.out.println("Fin " + count + " " + queueCount);

    }
    private void saveAllRows(CqlSession session, SimpleStatement statement){

        CompletionStage<AsyncResultSet> resultSetFuture =
                session.executeAsync(statement);

        CompletionStage<Void> fetchRows = resultSetFuture.thenCompose(this::queueUpPages);

        fetchRows.whenComplete(
                (resultSet, error) -> {
                    if (error != null) {
                        System.out.printf("Error: %s%n", error.getMessage());

                        SimpleStatement t = new SimpleStatementBuilder(query).build();

                        t.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
                        t.setIdempotent(true);
                        t.setPageSize(PAGE_SIZE);
                        t.setPagingState(pagingState);

                        saveAllRows(session, t);

                    } else {
                        //String version = resultSet.one().getString(0);
                        System.out.printf("Finished");
                    }
                });



    }
    private CompletionStage<Void> queueUpPages(AsyncResultSet resultSet)  {

        List<Row> rowsPerPage = new ArrayList<Row>(PAGE_SIZE+50);

        int totalCount = 0;

        for (Row row : resultSet.currentPage()) {
            count.incrementAndGet();
            rowsPerPage.add(row);
            totalCount++;
        }

        Map<ByteBuffer, List<Row>> oneState = new HashMap<>(1);

        pagingState = resultSet.getExecutionInfo().getPagingState();

        oneState.put(pagingState, rowsPerPage);

        try {
            blockingQueue.put(oneState);
        }catch (Exception ex){
            throw new CompletionException(ex);
        }

        if (resultSet.hasMorePages()) {
            limiter.acquire(totalCount);
            return resultSet.fetchNextPage().thenCompose(this::queueUpPages);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }
}
