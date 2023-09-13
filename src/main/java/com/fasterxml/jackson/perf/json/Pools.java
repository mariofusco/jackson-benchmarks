package com.fasterxml.jackson.perf.json;

import com.fasterxml.jackson.core.util.BufferRecycler;
import com.fasterxml.jackson.core.util.BufferRecyclerPool;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.util.UnsafeAccess;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class Pools {
    enum PoolStrategy {
        NO_OP(BufferRecyclerPool.nonRecyclingPool()),
        THREAD_LOCAL(BufferRecyclerPool.threadLocalPool()),
        CONCURRENT_DEQUEUE(BufferRecyclerPool.ConcurrentDequePool.shared()),
        LOCK_FREE(BufferRecyclerPool.LockFreePool.shared()),
        JCTOOLS(JCToolsPool.INSTANCE),
        STRIPED_LOCK_FREE(STRIPED_LOCK_FREE_INSTANCE),
        STRIPED_JCTOOLS(STRIPED_JCTOOLS_INSTANCE),
        HYBRID(HybridPool.INSTANCE);

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

        private final MpmcUnboundedXaddArrayQueue<BufferRecycler> queue = new MpmcUnboundedXaddArrayQueue<>(256);

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

    private static final BufferRecyclerPool STRIPED_JCTOOLS_INSTANCE = new StripedPool(JCToolsPool::new, 4);

    private static final BufferRecyclerPool STRIPED_LOCK_FREE_INSTANCE = new StripedPool(BufferRecyclerPool.LockFreePool::nonShared, 4);

    static class StripedPool implements BufferRecyclerPool {

        private static final long PROBE = getProbeOffset();

        private final int mask;

        private final BufferRecyclerPool[] pools;

        public StripedPool(Supplier<BufferRecyclerPool> poolFactory, int slots) {
            this.mask = slots-1;
            this.pools = new BufferRecyclerPool[slots];
            for (int i = 0; i < slots; i++) {
                this.pools[i] = poolFactory.get();
            }
        }

        private static long getProbeOffset() {
            try {
                return UnsafeAccess.UNSAFE.objectFieldOffset(Thread.class.getDeclaredField("threadLocalRandomProbe"));
            } catch (NoSuchFieldException e) {
                throw new UnsupportedOperationException(e);
            }
        }

        private int index() {
            return probe() & mask;
        }

        private int probe() {
            int probe;
            if ((probe = UnsafeAccess.UNSAFE.getInt(Thread.currentThread(), PROBE)) == 0) {
                ThreadLocalRandom.current(); // force initialization
                probe = UnsafeAccess.UNSAFE.getInt(Thread.currentThread(), PROBE);
            }
            return probe;
        }

        @Override
        public BufferRecycler acquireBufferRecycler() {
            return pools[index()].acquireBufferRecycler();
        }

        @Override
        public void releaseBufferRecycler(BufferRecycler recycler) {
            pools[index()].releaseBufferRecycler(recycler);
        }
    }

    static class HybridPool implements BufferRecyclerPool {

        static final BufferRecyclerPool INSTANCE = new HybridPool();

        private final BufferRecyclerPool nativePool = BufferRecyclerPool.threadLocalPool();
        private final BufferRecyclerPool virtualPool = STRIPED_JCTOOLS_INSTANCE;

        @Override
        public BufferRecycler acquireBufferRecycler() {
            return Thread.currentThread().isVirtual() ? virtualPool.acquireBufferRecycler() : nativePool.acquireBufferRecycler();
        }

        @Override
        public void releaseBufferRecycler(BufferRecycler bufferRecycler) {
            if (Thread.currentThread().isVirtual()) virtualPool.releaseBufferRecycler(bufferRecycler);
            else nativePool.releaseBufferRecycler(bufferRecycler);
        }
    }
}
