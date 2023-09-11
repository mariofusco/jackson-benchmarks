package com.fasterxml.jackson.perf.json;

import com.fasterxml.jackson.core.util.BufferRecycler;
import com.fasterxml.jackson.core.util.BufferRecyclerPool;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.util.UnsafeAccess;

public class Pools {
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

    static class JCToolsRelaxedPool implements BufferRecyclerPool {

        static final BufferRecyclerPool INSTANCE = new JCToolsRelaxedPool();

        private final MpmcUnboundedXaddArrayQueue<BufferRecycler> queue = new MpmcUnboundedXaddArrayQueue<>(256);

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

    static class JCToolsStripedPool implements BufferRecyclerPool {

        private static final long PROBE = getProbeOffset();

        private static long getProbeOffset() {
            try {
                return UnsafeAccess.UNSAFE.objectFieldOffset(Thread.class.getDeclaredField("threadLocalRandomProbe"));
            } catch (NoSuchFieldException e) {
                throw new UnsupportedOperationException(e);
            }
        }

        @Override
        public BufferRecycler acquireBufferRecycler() {
            return null;
        }

        @Override
        public void releaseBufferRecycler(BufferRecycler recycler) {

        }
    }

}
