package com.fasterxml.jackson.perf.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.util.BufferRecyclerPool;
import com.fasterxml.jackson.jr.ob.JSON;
import com.fasterxml.jackson.perf.model.MediaItems;
import com.fasterxml.jackson.perf.util.NopOutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@State(value = Scope.Benchmark)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
@Fork(2)
public class JacksonMultithreadWriteVanilla {
    private JSON json;

    private Object item;

    private Consumer<Runnable> runner;

    @Param({"true", "false"})
    private boolean useVirtualThreads;

//    @Param({"10", "100", "1000"})
    @Param({"100"})
    private int parallelTasks;

//    @Param({"large", "small"})
    @Param({"small"})
    private String objectSize;

    @Param({"THREAD_LOCAL", "HYBRID_JCTOOLS", "HYBRID_LOCK_FREE", "HYBRID_JCTOOLS_UNSAFE", "HYBRID_LOCK_FREE_UNSAFE"})
    private String poolStrategy;

    @Setup
    public void setup() {
        this.json = createJson();
        this.runner = createRunner();
        this.item = objectSize.equalsIgnoreCase("large") ? MediaItems.stdMediaItem() : new Person("Mario", "Fusco", 49);
    }

    private JSON createJson() {
        BufferRecyclerPool pool = Pools.PoolStrategy.valueOf(poolStrategy).getPool();
        JsonFactory jsonFactory = new JsonFactory().setBufferRecyclerPool(pool);
        return new JSON(jsonFactory);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void writePojoMediaItem(Blackhole bh) {
        CountDownLatch countDown = new CountDownLatch(parallelTasks);

        for (int i = 0; i < parallelTasks; i++) {
            runner.accept(() -> {
                bh.consume(write(item, json));
                countDown.countDown();
            });
        }

        try {
            countDown.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected final int write(Object value, JSON writer) {
        NopOutputStream out = new NopOutputStream();
        try {
            writer.write(value, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.size();
    }

    private Consumer<Runnable> createRunner() {
        if (useVirtualThreads) {
            return Thread::startVirtualThread;
        } else {
            return Executors.newWorkStealingPool()::execute;
        }
    }

    static class Person {
        private String firstName;
        private String lastName;
        private int age;

        public Person(String firstName, String lastName, int age) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public int getAge() {
            return age;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) {
        JacksonMultithreadWriteVanilla benchmark = new JacksonMultithreadWriteVanilla();
        benchmark.poolStrategy = "HYBRID_LOCK_FREE";
        benchmark.objectSize = "small";
        benchmark.parallelTasks = 100;
        benchmark.useVirtualThreads = true;
        benchmark.setup();

        Blackhole bh = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
        for (int i = 0; i < 10; i++) {
            benchmark.writePojoMediaItem(bh);
        }
    }
}
