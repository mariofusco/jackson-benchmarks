package com.fasterxml.jackson.perf.json;

import com.fasterxml.jackson.core.util.BufferRecycler;
import com.fasterxml.jackson.core.util.BufferRecyclerPool;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.util.Pow2;
import org.jctools.util.UnsafeAccess;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
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

        private static final Predicate<Thread> isVirtual = findIsVirtual();

        private static Predicate<Thread> findIsVirtual() {
            try {
                MethodHandle virtualMh = MethodHandles.publicLookup().findVirtual(Thread.class, "isVirtual", MethodType.methodType(boolean.class));
                return t -> {
                    try {
                        return (boolean) virtualMh.invokeExact(t);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                };
            } catch (Exception e) {
                return t -> false;
            }
        }

        private final BufferRecyclerPool nativePool = BufferRecyclerPool.threadLocalPool();

        static class VirtualPoolHolder {
            // Lazy on-demand initialization
            private static final BufferRecyclerPool virtualPool = new StripedJCToolsPool(4);
        }

        @Override
        public BufferRecycler acquireBufferRecycler() {
            return isVirtual.test(Thread.currentThread()) ?
                    VirtualPoolHolder.virtualPool.acquireBufferRecycler() :
                    nativePool.acquireBufferRecycler();
        }

        @Override
        public void releaseBufferRecycler(BufferRecycler bufferRecycler) {
            if (bufferRecycler instanceof VThreadBufferRecycler) {
                // if it is a PooledBufferRecycler it has been acquired by a virtual thread, so it has to be release to the same pool
                VirtualPoolHolder.virtualPool.releaseBufferRecycler(bufferRecycler);
            }
            // the native thread pool is based on ThreadLocal, so it doesn't have anything to do on release
        }

        static class StripedJCToolsPool implements BufferRecyclerPool {

            private static final long PROBE = getProbeOffset();

            private final int mask;

            private final MpmcUnboundedXaddArrayQueue<BufferRecycler>[] queues;

            public StripedJCToolsPool(int stripesCount) {
                if (stripesCount <= 0) {
                    throw new IllegalArgumentException("Expecting a stripesCount that is larger than 0");
                }

                int size = Pow2.roundToPowerOfTwo(stripesCount);
                mask = (size - 1);

                this.queues = new MpmcUnboundedXaddArrayQueue[size];
                for (int i = 0; i < size; i++) {
                    this.queues[i] = new MpmcUnboundedXaddArrayQueue<>(128);
                }
            }

            private static long getProbeOffset() {
                try {
                    return UnsafeAccess.UNSAFE.objectFieldOffset(Thread.class.getDeclaredField("threadLocalRandomProbe"));
                } catch (NoSuchFieldException e) {
                    return -1L;
                }
            }

            private int index() {
                return probe() & mask;
            }

            private int probe() {
                // Fast path for reliable well-distributed probe, available from JDK 7+.
                // As long as PROBE is final static this branch will be constant folded
                // (i.e removed).
                if (PROBE != -1) {
                    int probe;
                    if ((probe = UnsafeAccess.UNSAFE.getInt(Thread.currentThread(), PROBE)) == 0) {
                        ThreadLocalRandom.current(); // force initialization
                        probe = UnsafeAccess.UNSAFE.getInt(Thread.currentThread(), PROBE);
                    }
                    return probe;
                }

                /*
                 * Else use much worse (for values distribution) method:
                 * Mix thread id with golden ratio and then xorshift it
                 * to spread consecutive ids (see Knuth multiplicative method as reference).
                 */
                int probe = (int) ((Thread.currentThread().getId() * 0x9e3779b9) & Integer.MAX_VALUE);
                // xorshift
                probe ^= probe << 13;
                probe ^= probe >>> 17;
                probe ^= probe << 5;
                return probe;
            }

            @Override
            public BufferRecycler acquireBufferRecycler() {
                int index = index();
                BufferRecycler bufferRecycler = queues[index].poll();
                return bufferRecycler != null ? bufferRecycler : new VThreadBufferRecycler(index);
            }

            @Override
            public void releaseBufferRecycler(BufferRecycler recycler) {
                queues[((VThreadBufferRecycler) recycler).slot].offer(recycler);
            }
        }

        static class VThreadBufferRecycler extends BufferRecycler {
            private final int slot;

            VThreadBufferRecycler(int slot) {
                this.slot = slot;
            }
        }
    }
}
