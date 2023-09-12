package com.fasterxml.jackson.perf.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.util.BufferRecycler;
import com.fasterxml.jackson.core.util.BufferRecyclerPool;
import com.fasterxml.jackson.jr.ob.JSON;
import com.fasterxml.jackson.perf.model.MediaItems;
import com.fasterxml.jackson.perf.util.NopOutputStream;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(value = Scope.Benchmark)
@Measurement(iterations = 10)
@Fork(2)
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class JacksonWriteVanilla {

    private JSON json;

    private Object item;

    @Param({"large", "small"})
    private String objectSize;

//    @Param({"NO_OP", "THREAD_LOCAL", "LOCK_FREE", "STRIPED_LOCK_FREE", "CONCURRENT_DEQUEUE", "JCTOOLS", "STRIPED_JCTOOLS"})
    @Param({"STRIPED_LOCK_FREE", "STRIPED_JCTOOLS"})
    private String poolStrategy;

    @Param({"0", "10", "100"})
    private int acquireDelay;

    @Setup
    public void setup() {
        this.json = createJson();
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
        bh.consume(write(item, json));
        if (acquireDelay > 0) {
            bh.consumeCPU(acquireDelay);
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
        JacksonWriteVanilla benchmark = new JacksonWriteVanilla();
        benchmark.poolStrategy = "STRIPED_LOCK_FREE";
        benchmark.objectSize = "small";
        benchmark.setup();

        Blackhole bh = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
        for (int i = 0; i < 10; i++) {
            benchmark.writePojoMediaItem(bh);
        }
    }
}
