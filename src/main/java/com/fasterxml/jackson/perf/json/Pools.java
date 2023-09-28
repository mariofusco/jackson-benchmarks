package com.fasterxml.jackson.perf.json;

import com.fasterxml.jackson.core.util.BufferRecycler;
import com.fasterxml.jackson.core.util.RecyclerPool;
import com.fasterxml.jackson.core.util.JsonBufferRecyclers;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.util.UnsafeAccess;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Pools {
    enum PoolStrategy {
        NO_OP(JsonBufferRecyclers.nonRecyclingPool()),
        THREAD_LOCAL(JsonBufferRecyclers.threadLocalPool()),
        CONCURRENT_DEQUEUE(JsonBufferRecyclers.sharedConcurrentDequePool()),
        LOCK_FREE(JsonBufferRecyclers.sharedLockFreePool()),
        JCTOOLS(JCToolsPool.INSTANCE),
        STRIPED_LOCK_FREE(STRIPED_LOCK_FREE_INSTANCE),
        STRIPED_JCTOOLS(STRIPED_JCTOOLS_INSTANCE),
        HYBRID_JCTOOLS(HybridJCToolsPool.INSTANCE),
        HYBRID_JCTOOLS_UNSAFE(HybridJCToolsPoolUnsafe.INSTANCE),
        HYBRID_LOCK_FREE(HybridLockFreePool.INSTANCE),
        HYBRID_LOCK_FREE_UNSAFE(HybridLockFreePoolUnsafe.INSTANCE);

        private final RecyclerPool<BufferRecycler> pool;

        PoolStrategy(RecyclerPool pool) {
            this.pool = pool;
        }

        public RecyclerPool<BufferRecycler> getPool() {
            return pool;
        }
    }

    static class JCToolsPool implements RecyclerPool<BufferRecycler> {

        static final RecyclerPool INSTANCE = new JCToolsPool();

        private final MpmcUnboundedXaddArrayQueue<BufferRecycler> queue = new MpmcUnboundedXaddArrayQueue<>(256);

        @Override
        public BufferRecycler acquirePooled() {
            BufferRecycler bufferRecycler = queue.poll();
            return bufferRecycler != null ? bufferRecycler : new BufferRecycler();
        }

        @Override
        public void releasePooled(BufferRecycler recycler) {
            queue.offer(recycler);
        }
    }

    private static final RecyclerPool STRIPED_JCTOOLS_INSTANCE = new StripedPool(JCToolsPool::new, 4);

    private static final RecyclerPool STRIPED_LOCK_FREE_INSTANCE = new StripedPool(JsonBufferRecyclers::newLockFreePool, 4);

    static class StripedPool implements RecyclerPool<BufferRecycler> {

        private static final long PROBE = getProbeOffset();

        private final int mask;

        private final RecyclerPool<BufferRecycler>[] pools;

        public StripedPool(Supplier<RecyclerPool<BufferRecycler>> poolFactory, int slots) {
            this.mask = slots-1;
            this.pools = new RecyclerPool[slots];
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
        public BufferRecycler acquirePooled() {
            return pools[index()].acquirePooled();
        }

        @Override
        public void releasePooled(BufferRecycler recycler) {
            pools[index()].releasePooled(recycler);
        }
    }

    static class VirtualPredicate {
        private static final MethodHandle virtualMh = findVirtualMH();

        private static MethodHandle findVirtualMH() {
            try {
                return MethodHandles.publicLookup().findVirtual(Thread.class, "isVirtual", MethodType.methodType(boolean.class));
            } catch (Exception e) {
                return null;
            }
        }

        private static Predicate<Thread> findIsVirtualPredicate() {
            return virtualMh != null ? t -> {
                try {
                    return (boolean) virtualMh.invokeExact(t);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            } : t -> false;
        }
    }

    static abstract class ThreadProbe {
        abstract int index();

        static ThreadProbe createThreadProbe(int mask, boolean allowUnsafe) {
            if (allowUnsafe && UnsafeThreadProbe.getProbeOffset() == -1L) {
                throw new UnsupportedOperationException("Cannot use unsafe probe");
            }
            return allowUnsafe ? new UnsafeThreadProbe(mask) : new XorShiftThreadProbe(mask);
        }
    }

    static class UnsafeThreadProbe extends ThreadProbe {

        private static final long PROBE = getProbeOffset();

        private final int mask;


        UnsafeThreadProbe(int mask) {
            this.mask = mask;
        }

        private static long getProbeOffset() {
            try {
                return UnsafeAccess.UNSAFE.objectFieldOffset(Thread.class.getDeclaredField("threadLocalRandomProbe"));
            } catch (NoSuchFieldException e) {
                return -1L;
            }
        }

        @Override
        public int index() {
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
    }

    static class XorShiftThreadProbe extends ThreadProbe {

        private final int mask;


        XorShiftThreadProbe(int mask) {
            this.mask = mask;
        }

        @Override
        public int index() {
            return probe() & mask;
        }

        private int probe() {
            int probe = (int) ((Thread.currentThread().getId() * 0x9e3779b9) & Integer.MAX_VALUE);
            // xorshift
            probe ^= probe << 13;
            probe ^= probe >>> 17;
            probe ^= probe << 5;
            return probe;
        }
    }

    static class HybridJCToolsPoolUnsafe implements RecyclerPool<BufferRecycler> {

        static final RecyclerPool<BufferRecycler> INSTANCE = new HybridJCToolsPoolUnsafe();

        private static final Predicate<Thread> isVirtual = VirtualPredicate.findIsVirtualPredicate();

        private final RecyclerPool<BufferRecycler> nativePool = JsonBufferRecyclers.threadLocalPool();

        static class VirtualPoolHolder {
            // Lazy on-demand initialization
            private static final RecyclerPool<BufferRecycler> virtualPool = new StripedJCToolsPool(4, true);
        }

        @Override
        public BufferRecycler acquirePooled() {
            return isVirtual.test(Thread.currentThread()) ?
                    VirtualPoolHolder.virtualPool.acquirePooled() :
                    nativePool.acquirePooled();
        }

        @Override
        public void releasePooled(BufferRecycler bufferRecycler) {
            if (bufferRecycler instanceof VThreadBufferRecycler) {
                // if it is a PooledBufferRecycler it has been acquired by a virtual thread, so it has to be released to the same pool
                VirtualPoolHolder.virtualPool.releasePooled(bufferRecycler);
            }
            // the native thread pool is based on ThreadLocal, so it doesn't have anything to do on release
        }
    }

    static class HybridJCToolsPool implements RecyclerPool<BufferRecycler> {

        static final RecyclerPool<BufferRecycler> INSTANCE = new HybridJCToolsPool();

        private static final Predicate<Thread> isVirtual = VirtualPredicate.findIsVirtualPredicate();

        private final RecyclerPool<BufferRecycler> nativePool = JsonBufferRecyclers.threadLocalPool();

        static class VirtualPoolHolder {
            // Lazy on-demand initialization
            private static final RecyclerPool<BufferRecycler> virtualPool = new StripedJCToolsPool(4, false);
        }

        @Override
        public BufferRecycler acquirePooled() {
            return isVirtual.test(Thread.currentThread()) ?
                    VirtualPoolHolder.virtualPool.acquirePooled() :
                    nativePool.acquirePooled();
        }

        @Override
        public void releasePooled(BufferRecycler bufferRecycler) {
            if (bufferRecycler instanceof VThreadBufferRecycler) {
                // if it is a PooledBufferRecycler it has been acquired by a virtual thread, so it has to be released to the same pool
                VirtualPoolHolder.virtualPool.releasePooled(bufferRecycler);
            }
            // the native thread pool is based on ThreadLocal, so it doesn't have anything to do on release
        }
    }

    static class StripedJCToolsPool implements RecyclerPool<BufferRecycler> {

        private final ThreadProbe threadProbe;

        private final MpmcUnboundedXaddArrayQueue<BufferRecycler>[] queues;

        public StripedJCToolsPool(int stripesCount, boolean allowUnsafe) {
            if (stripesCount <= 0) {
                throw new IllegalArgumentException("Expecting a stripesCount that is larger than 0");
            }

            int size = roundToPowerOfTwo(stripesCount);
            this.threadProbe = ThreadProbe.createThreadProbe(size - 1, allowUnsafe);

            this.queues = new MpmcUnboundedXaddArrayQueue[size];
            for (int i = 0; i < size; i++) {
                this.queues[i] = new MpmcUnboundedXaddArrayQueue<>(128);
            }
        }

        @Override
        public BufferRecycler acquirePooled() {
            int index = threadProbe.index();
            BufferRecycler bufferRecycler = queues[index].poll();
            return bufferRecycler != null ? bufferRecycler : new VThreadBufferRecycler(index);
        }

        @Override
        public void releasePooled(BufferRecycler recycler) {
            queues[((VThreadBufferRecycler) recycler).slot].offer(recycler);
        }
    }

    static class VThreadBufferRecycler extends BufferRecycler {
        private final int slot;

        VThreadBufferRecycler(int slot) {
            this.slot = slot;
        }
    }

    static class HybridLockFreePoolUnsafe implements RecyclerPool<BufferRecycler> {

        static final RecyclerPool<BufferRecycler> INSTANCE = new HybridLockFreePoolUnsafe();

        private static final Predicate<Thread> isVirtual = VirtualPredicate.findIsVirtualPredicate();

        private final RecyclerPool<BufferRecycler> nativePool = JsonBufferRecyclers.threadLocalPool();

        static class VirtualPoolHolder {
            // Lazy on-demand initialization
            private static final RecyclerPool<BufferRecycler> virtualPool = new StripedLockFreePool(4, true);
        }

        @Override
        public BufferRecycler acquirePooled() {
            return isVirtual.test(Thread.currentThread()) ?
                    VirtualPoolHolder.virtualPool.acquirePooled() :
                    nativePool.acquirePooled();
        }

        @Override
        public void releasePooled(BufferRecycler bufferRecycler) {
            if (bufferRecycler instanceof VThreadBufferRecycler) {
                // if it is a PooledBufferRecycler it has been acquired by a virtual thread, so it has to be released to the same pool
                VirtualPoolHolder.virtualPool.releasePooled(bufferRecycler);
            }
            // the native thread pool is based on ThreadLocal, so it doesn't have anything to do on release
        }
    }

    static class HybridLockFreePool implements RecyclerPool<BufferRecycler> {

        static final RecyclerPool INSTANCE = new HybridLockFreePool();

        private static final Predicate<Thread> isVirtual = VirtualPredicate.findIsVirtualPredicate();

        private final RecyclerPool<BufferRecycler> nativePool = JsonBufferRecyclers.threadLocalPool();

        static class VirtualPoolHolder {
            // Lazy on-demand initialization
            private static final RecyclerPool<BufferRecycler> virtualPool = new StripedLockFreePool(4, false);
        }

        @Override
        public BufferRecycler acquirePooled() {
            return isVirtual.test(Thread.currentThread()) ?
                    VirtualPoolHolder.virtualPool.acquirePooled() :
                    nativePool.acquirePooled();
        }

        @Override
        public void releasePooled(BufferRecycler bufferRecycler) {
            if (bufferRecycler instanceof VThreadBufferRecycler) {
                // if it is a PooledBufferRecycler it has been acquired by a virtual thread, so it has to be released to the same pool
                VirtualPoolHolder.virtualPool.releasePooled(bufferRecycler);
            }
            // the native thread pool is based on ThreadLocal, so it doesn't have anything to do on release
        }
    }

    static class StripedLockFreePool implements RecyclerPool<BufferRecycler> {

        private static final int CACHE_LINE_SHIFT = 4;

        private static final int CACHE_LINE_PADDING = 1 << CACHE_LINE_SHIFT;

        private final ThreadProbe threadProbe;

        private final AtomicReferenceArray<Node> heads;

        public StripedLockFreePool(int stripesCount, boolean allowUnsafe) {
            if (stripesCount <= 0) {
                throw new IllegalArgumentException("Expecting a stripesCount that is larger than 0");
            }

            int size = roundToPowerOfTwo(stripesCount);
            this.heads = new AtomicReferenceArray<>(size * CACHE_LINE_PADDING);

            int mask = (size - 1) << CACHE_LINE_SHIFT;
            this.threadProbe = ThreadProbe.createThreadProbe(mask, allowUnsafe);
        }

        @Override
        public BufferRecycler acquirePooled() {
            int index = threadProbe.index();

            Node currentHead = heads.get(index);
            while (true) {
                if (currentHead == null) {
                    return new VThreadBufferRecycler(index);
                }

                Node witness = heads.compareAndExchange(index, currentHead, currentHead.next);
                if (witness == currentHead) {
                    currentHead.next = null;
                    return currentHead.value;
                } else {
                    currentHead = witness;
                }
            }
        }

        @Override
        public void releasePooled(BufferRecycler recycler) {
            VThreadBufferRecycler vThreadBufferRecycler = (VThreadBufferRecycler) recycler;
            Node newHead = new Node(vThreadBufferRecycler);

            Node next = heads.get(vThreadBufferRecycler.slot);
            while (true) {
                Node witness = heads.compareAndExchange(vThreadBufferRecycler.slot, next, newHead);
                if (witness == next) {
                    newHead.next = next;
                    return;
                } else {
                    next = witness;
                }
            }
        }

        private static class Node {
            final VThreadBufferRecycler value;
            Node next;

            Node(VThreadBufferRecycler value) {
                this.value = value;
            }
        }
    }

    public static final int MAX_POW2 = 1 << 30;

    public static int roundToPowerOfTwo(final int value) {
        if (value > MAX_POW2) {
            throw new IllegalArgumentException("There is no larger power of 2 int for value:"+value+" since it exceeds 2^31.");
        }
        if (value < 0) {
            throw new IllegalArgumentException("Given value:"+value+". Expecting value >= 0.");
        }
        final int nextPow2 = 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
        return nextPow2;
    }
}
