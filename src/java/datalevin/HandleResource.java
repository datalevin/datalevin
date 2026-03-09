package datalevin;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

abstract class HandleResource implements AutoCloseable {

    @FunctionalInterface
    interface ResourceCloser {
        void close(Object resource);
    }

    private final ResourceCloser closer;
    private final String jsonHandlePrefix;
    private final String jsonHandleArg;
    private Object resource;

    HandleResource(Object resource,
                   ResourceCloser closer,
                   String jsonHandlePrefix,
                   String jsonHandleArg) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.closer = Objects.requireNonNull(closer, "closer");
        this.jsonHandlePrefix = Objects.requireNonNull(jsonHandlePrefix, "jsonHandlePrefix");
        this.jsonHandleArg = Objects.requireNonNull(jsonHandleArg, "jsonHandleArg");
    }

    public final Object handle() {
        ensureOpen();
        return resource;
    }

    public final boolean isOpen() {
        return resource != null;
    }

    protected final Object resource() {
        ensureOpen();
        return resource;
    }

    protected final Object execJson(String op) {
        return execJson(op, Map.of());
    }

    protected final Object execJson(String op, Map<String, ?> args) {
        ensureOpen();
        String handle = (String) ClojureRuntime.jsonApi("register!",
                                                        jsonHandlePrefix,
                                                        ClojureCodec.keyword(jsonHandlePrefix),
                                                        resource);
        try {
            LinkedHashMap<String, Object> request = new LinkedHashMap<>();
            request.put(jsonHandleArg, handle);
            if (args != null) {
                request.putAll(args);
            }
            return JsonBridge.call(op, request);
        } finally {
            ClojureRuntime.jsonApi("unregister!", handle);
        }
    }

    protected final boolean isReleased() {
        return resource == null;
    }

    private void ensureOpen() {
        if (resource == null) {
            throw new IllegalStateException(getClass().getSimpleName() + " is closed.");
        }
    }

    @Override
    public void close() {
        if (resource == null) {
            return;
        }
        closer.close(resource);
        resource = null;
    }
}
