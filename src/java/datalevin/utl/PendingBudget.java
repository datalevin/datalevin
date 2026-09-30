/* Copyright (c) Huahai Yang. All rights reserved.
 * Distributed under the Eclipse Public License 2.0. */
package datalevin.utl;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;

/** Atomic byte/request admission accounting, independent of application locks. */
public final class PendingBudget {
    public static final class Usage {
        public final long bytes;
        public final long requests;

        Usage(long bytes, long requests) {
            this.bytes = bytes;
            this.requests = requests;
        }
    }

    private final long maxBytes;
    private final long maxRequests;
    private final int requestBits;
    private final long requestMask;
    private final AtomicLong packed;
    private final AtomicReference<Usage> used;

    public PendingBudget(long maxBytes, long maxRequests) {
        if (maxBytes <= 0 || maxRequests <= 0)
            throw new IllegalArgumentException("Pending limits must be positive");
        this.maxBytes = maxBytes;
        this.maxRequests = maxRequests;
        requestBits = 64 - Long.numberOfLeadingZeros(maxRequests);
        requestMask = -1L >>> (64 - requestBits);
        // Default limits fit in one word. Keep full-long limits supported by
        // the immutable-pair fallback instead of imposing a smaller API bound.
        if (maxBytes <= (Long.MAX_VALUE >>> requestBits)) {
            packed = new AtomicLong();
            used = null;
        } else {
            packed = null;
            used = new AtomicReference<>(new Usage(0, 0));
        }
    }

    public Usage snapshot() {
        if (packed == null) return used.get();
        long value = packed.get();
        return new Usage(value >>> requestBits, value & requestMask);
    }

    public boolean tryReserve(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("Negative reservation");
        if (packed != null) {
            for (;;) {
                long before = packed.get();
                if (bytes > maxBytes - (before >>> requestBits)
                    || (before & requestMask) >= maxRequests) return false;
                if (packed.compareAndSet(before, before + (bytes << requestBits) + 1))
                    return true;
            }
        }
        for (;;) {
            Usage before = used.get();
            if (bytes > maxBytes - before.bytes || before.requests >= maxRequests)
                return false;
            Usage after = new Usage(before.bytes + bytes, before.requests + 1);
            if (used.compareAndSet(before, after)) return true;
        }
    }

    public void release(long bytes) {
        if (packed != null) {
            for (;;) {
                long before = packed.get();
                if (bytes < 0 || (before >>> requestBits) < bytes
                    || (before & requestMask) == 0)
                    throw new IllegalStateException("Unbalanced pending reservation");
                if (packed.compareAndSet(before, before - (bytes << requestBits) - 1))
                    return;
            }
        }
        for (;;) {
            Usage before = used.get();
            if (bytes < 0 || before.bytes < bytes || before.requests == 0)
                throw new IllegalStateException("Unbalanced pending reservation");
            Usage after = new Usage(before.bytes - bytes, before.requests - 1);
            if (used.compareAndSet(before, after)) return;
        }
    }
}
