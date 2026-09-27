package datalevin;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Reusable pull, query, or KV point read. Holds metadata, not an open read
 * transaction or borrowed buffer, and needs no separate close operation.
 * Transaction-bound preparations retain their transaction's lifetime and
 * thread restrictions. Local pulls and KV reads may use an explicit view.
 */
public final class PreparedRead {
    private final Object prepared;
    private final Function<Object, Object> input;
    private final boolean query;

    PreparedRead(Object prepared, Function<Object, Object> input, boolean query) {
        this.prepared = Objects.requireNonNull(prepared, "prepared");
        this.input = input;
        this.query = query;
    }

    /** Executes with an entity id/lookup ref, query input list, or KV key. */
    public Object execute(Object value) {
        return ClojureRuntime.core("execute-prepared", prepared, input.apply(value));
    }

    /**
     * Executes a local preparation against an explicit database or KV view.
     * Remote preparations and prepared queries reject explicit views.
     */
    public Object execute(Object view, Object value) {
        return ClojureRuntime.core("execute-prepared", prepared, view(view), input.apply(value));
    }

    /** Executes and materializes results for foreign-language bridges. */
    public Object executeBridge(Object value) {
        return bridge(execute(value));
    }

    /** Executes against an explicit view and materializes bridge results. */
    public Object executeBridge(Object view, Object value) {
        return bridge(execute(view, value));
    }

    private Object bridge(Object value) {
        return query ? ClojureCodec.bridgeQueryOutput(value) : ClojureCodec.bridgeOutput(value);
    }

    static Object view(Object value) {
        if (value instanceof Connection conn) {
            return DatalevinInterop.connectionDb(conn);
        }
        if (value instanceof DatabaseValue db) {
            return db.handle();
        }
        if (value instanceof HandleResource resource) {
            return resource.handle();
        }
        return value;
    }

    static Function<Object, Object> queryInput(Object query) {
        Function<List<?>, List<?>> inputs = query instanceof DatalogQuery typed
                ? typed.preparedInputs() : values -> values;
        return value -> {
            if (!(value instanceof List<?> values)) {
                throw new IllegalArgumentException("Prepared query input must be a list.");
            }
            List<Object> normalized = new ArrayList<>();
            for (Object item : inputs.apply(values)) {
                normalized.add(ClojureCodec.runtimeInput(view(item)));
            }
            return ClojureCodec.runtimeInput(normalized);
        };
    }
}
