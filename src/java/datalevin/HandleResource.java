package datalevin;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

abstract class HandleResource implements AutoCloseable {

    private final String closeOp;
    private final String handleArg;
    private String handle;

    HandleResource(String handle, String closeOp, String handleArg) {
        this.handle = Objects.requireNonNull(handle, "handle");
        this.closeOp = Objects.requireNonNull(closeOp, "closeOp");
        this.handleArg = Objects.requireNonNull(handleArg, "handleArg");
    }

    public final String handle() {
        ensureOpen();
        return handle;
    }

    public final boolean isOpen() {
        return handle != null;
    }

    protected final Object call(String op) {
        return call(op, Map.of());
    }

    protected final Object call(String op, Map<String, ?> args) {
        ensureOpen();
        LinkedHashMap<String, Object> request = new LinkedHashMap<>();
        request.put(handleArg, handle);
        if (args != null) {
            request.putAll(args);
        }
        return JsonBridge.call(op, request);
    }

    protected final void releaseLocalHandle() {
        handle = null;
    }

    protected final boolean isReleased() {
        return handle == null;
    }

    private void ensureOpen() {
        if (handle == null) {
            throw new IllegalStateException(getClass().getSimpleName() + " is closed.");
        }
    }

    @Override
    public void close() {
        if (handle == null) {
            return;
        }

        LinkedHashMap<String, Object> request = new LinkedHashMap<>();
        request.put(handleArg, handle);
        JsonBridge.call(closeOp, request);
        handle = null;
    }
}
