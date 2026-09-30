/* Copyright (c) Huahai Yang. All rights reserved.
 * Distributed under the Eclipse Public License 2.0. */
package datalevin.utl;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Native lifetime accounting at transaction borrow boundaries. */
public final class NativeUsers {
    private final Gate gate;
    private final ArrayList<Slot> slots = new ArrayList<>();
    private final ThreadLocal<Slot> local = new ThreadLocal<>();

    private static final class Gate {
        final ReentrantLock lock;
        final Condition changed;
        volatile boolean open = true;

        Gate(ReentrantLock lock, Condition changed) {
            this.lock = lock;
            this.changed = changed;
        }
    }

    public NativeUsers(ReentrantLock lock, Condition changed) {
        this.gate = new Gate(lock, changed);
    }

    /** Exactly one close per borrow; return may occur on another thread. */
    public AutoCloseable borrow() {
        if (!gate.open) throw new IllegalStateException("Native environment is fenced");
        Slot slot = local.get();
        if (slot == null) slot = register();
        slot.count.incrementAndGet();
        // Fence precedes the closing thread's scan. If that scan missed this
        // entrant, this second volatile read rejects it before native access.
        if (gate.open) return slot;
        slot.close();
        throw new IllegalStateException("Native environment is fenced");
    }

    private Slot register() {
        gate.lock.lock();
        try {
            if (!gate.open) throw new IllegalStateException("Native environment is fenced");
            slots.removeIf(Slot::retired);
            Slot slot = new Slot(gate, Thread.currentThread());
            slots.add(slot);
            local.set(slot);
            return slot;
        } finally {
            gate.lock.unlock();
        }
    }

    /** Caller holds the lifecycle lock, before scanning or awaiting users. */
    public void fence() { gate.open = false; }

    /** Caller holds the lifecycle lock; active slots are never removed. */
    public long countActive() {
        long count = 0;
        for (Slot slot : slots) count += slot.count.get();
        return count;
    }

    // A thread-local value must not retain NativeUsers (and thus its own weak
    // ThreadLocal key), otherwise closed environments leak on pooled threads.
    private static final class Slot implements AutoCloseable {
        private final Gate gate;
        private final AtomicInteger count = new AtomicInteger();
        private final WeakReference<Thread> thread;

        Slot(Gate gate, Thread thread) {
            this.gate = gate;
            this.thread = new WeakReference<>(thread);
        }

        boolean retired() {
            Thread owner = thread.get();
            return count.get() == 0 && (owner == null || !owner.isAlive());
        }

        @Override
        public void close() {
            count.decrementAndGet();
            // No shared read-side mutex. Only a draining closer needs a wake.
            if (!gate.open) {
                gate.lock.lock();
                try { gate.changed.signalAll(); }
                finally { gate.lock.unlock(); }
            }
        }
    }
}
