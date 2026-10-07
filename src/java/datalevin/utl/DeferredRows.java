/* Copyright (c) Huahai Yang. All rights reserved.
 * Distributed under the Eclipse Public License 2.0. */
package datalevin.utl;

import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.function.Supplier;

/** A frozen region whose size is known before its rows are encoded.
 * Concurrent native and WAL readers share one encoding and one failure.
 * The supplier must use owned input and must not access a native transaction.
 */
public final class DeferredRows extends AbstractList<Object> implements RandomAccess {
    private final int size;
    private final FutureTask<List<?>> encoded;
    private volatile List<?> rows;

    public DeferredRows(int size, Supplier<? extends List<?>> encoder) {
        if (size < 0) throw new IllegalArgumentException("Negative row count");
        this.size = size;
        this.encoded = new FutureTask<>(() -> {
            List<?> rows = Objects.requireNonNull(encoder.get());
            if (rows.size() != size) throw new IllegalStateException("Row count changed during encoding");
            return rows;
        });
    }

    @Override public int size() { return size; }

    @Override public Object get(int index) {
        Objects.checkIndex(index, size);
        List<?> current = rows;
        if (current != null) return current.get(index);
        encoded.run();
        try {
            current = encoded.get();
            rows = current;
            return current.get(index);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while encoding rows", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) throw (RuntimeException) cause;
            if (cause instanceof Error) throw (Error) cause;
            throw new IllegalStateException("Row encoding failed", cause);
        }
    }
}
