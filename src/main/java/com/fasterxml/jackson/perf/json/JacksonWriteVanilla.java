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
@Fork(1)
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class JacksonWriteVanilla {

    /**
     * Benchmark                               (objectSize)      (poolStrategy)   Mode  Cnt        Score        Error  Units
     * JacksonWriteVanilla.writePojoMediaItem         large               NO_OP  thrpt   10   751326.678 ±   2682.525  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         large        THREAD_LOCAL  thrpt   10  1563622.185 ±  13327.590  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         large           LOCK_FREE  thrpt   10  1564139.884 ±   6202.993  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         large  CONCURRENT_DEQUEUE  thrpt   10  1449827.384 ±   6733.889  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         large             JCTOOLS  thrpt   10   751345.199 ±   2841.050  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         large     JCTOOLS_RELAXED  thrpt   10   381658.315 ± 308411.917  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         small               NO_OP  thrpt   10   431962.030 ±  17287.333  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         small        THREAD_LOCAL  thrpt   10  3503147.257 ±  96102.345  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         small           LOCK_FREE  thrpt   10  1113022.347 ±  34818.978  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         small  CONCURRENT_DEQUEUE  thrpt   10  1681424.667 ±  36802.516  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         small             JCTOOLS  thrpt   10   455114.659 ±  13543.961  ops/s
     * JacksonWriteVanilla.writePojoMediaItem         small     JCTOOLS_RELAXED  thrpt   10   444419.688 ±  11659.952  ops/s
     */

    private JSON json;

    private Object item;

    @Param({"large", "small"})
    private String objectSize;

    @Param({"NO_OP", "THREAD_LOCAL", "LOCK_FREE", "CONCURRENT_DEQUEUE", "JCTOOLS", "JCTOOLS_RELAXED"})
    private String poolStrategy;

    @Setup
    public void setup() {
        this.json = createJson();
        this.item = objectSize.equalsIgnoreCase("large") ? MediaItems.stdMediaItem() : new Person("Mario", "Fusco", 49);
    }

    private JSON createJson() {
        BufferRecyclerPool pool = PoolStrategy.valueOf(poolStrategy).getPool();
        JsonFactory jsonFactory = new JsonFactory().setBufferRecyclerPool(pool);
        return new JSON(jsonFactory);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void writePojoMediaItem(Blackhole bh) throws Exception {
        bh.consume(write(item, json));
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

    enum PoolStrategy {
        NO_OP(BufferRecyclerPool.nonRecyclingPool()),
        THREAD_LOCAL(BufferRecyclerPool.threadLocalPool()),
        CONCURRENT_DEQUEUE(BufferRecyclerPool.ConcurrentDequePool.shared()),
        LOCK_FREE(BufferRecyclerPool.LockFreePool.shared()),
        JCTOOLS(JCToolsPool.INSTANCE),
        JCTOOLS_RELAXED(JCToolsRelaxedPool.INSTANCE);

        private final BufferRecyclerPool pool;

        PoolStrategy(BufferRecyclerPool pool) {
            this.pool = pool;
        }

        public BufferRecyclerPool getPool() {
            return pool;
        }
    }

    static class JCToolsPool implements BufferRecyclerPool {

        static final BufferRecyclerPool INSTANCE = new JCToolsPool();

        private final MpmcUnboundedXaddArrayQueue<BufferRecycler> queue = new MpmcUnboundedXaddArrayQueue<>(8);

        @Override
        public BufferRecycler acquireBufferRecycler() {
            BufferRecycler bufferRecycler = queue.poll();
            return bufferRecycler != null ? bufferRecycler : new BufferRecycler();
        }

        @Override
        public void releaseBufferRecycler(BufferRecycler recycler) {
            queue.offer(recycler);
        }
    }

    static class JCToolsRelaxedPool implements BufferRecyclerPool {

        static final BufferRecyclerPool INSTANCE = new JCToolsRelaxedPool();

        private final MpmcUnboundedXaddArrayQueue<BufferRecycler> queue = new MpmcUnboundedXaddArrayQueue<>(8);

        @Override
        public BufferRecycler acquireBufferRecycler() {
            BufferRecycler bufferRecycler = queue.relaxedPoll();
            return bufferRecycler != null ? bufferRecycler : new BufferRecycler();
        }

        @Override
        public void releaseBufferRecycler(BufferRecycler recycler) {
            queue.offer(recycler);
        }
    }
}
