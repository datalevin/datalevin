package datalevin;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

abstract class HandleResource implements AutoCloseable {

    @FunctionalInterface
    interface ResourceCloser {
        void close(Object resource);
    }

    private final ResourceCloser closer;
    private final String jsonHandlePrefix;
    private final String jsonHandleArg;
    private final AtomicReference<Object> resource;
    private final Object jsonSession;
    private final Object jsonLock;
    private String jsonHandle;
    private int inFlightJsonCalls;

    HandleResource(Object resource,
                   ResourceCloser closer,
                   String jsonHandlePrefix,
                   String jsonHandleArg) {
        this.resource = new AtomicReference<>(Objects.requireNonNull(resource, "resource"));
        this.closer = Objects.requireNonNull(closer, "closer");
        this.jsonHandlePrefix = Objects.requireNonNull(jsonHandlePrefix, "jsonHandlePrefix");
        this.jsonHandleArg = Objects.requireNonNull(jsonHandleArg, "jsonHandleArg");
        this.jsonSession = ClojureRuntime.jsonApi("new-session-state");
        this.jsonLock = new Object();
    }

    public final Object handle() {
        return requireResource();
    }

    public final boolean isOpen() {
        return resource.get() != null;
    }

    protected final Object resource() {
        return requireResource();
    }

    protected final Object execJson(String op) {
        return execJson(op, Map.of());
    }

    protected final Object execJson(String op, Map<String, ?> args) {
        String handle = beginJsonCall();
        try {
            LinkedHashMap<String, Object> request = new LinkedHashMap<>();
            request.put(jsonHandleArg, handle);
            if (args != null) {
                request.putAll(args);
            }
            return JsonBridge.call(jsonSession, op, request);
        } finally {
            endJsonCall();
        }
    }

    protected final boolean isReleased() {
        return resource.get() == null;
    }

    private Object requireResource() {
        Object current = resource.get();
        if (current == null) {
            throw new IllegalStateException(getClass().getSimpleName() + " is closed.");
        }
        return current;
    }

    private String beginJsonCall() {
        synchronized (jsonLock) {
            Object current = requireResource();
            if (jsonHandle == null) {
                jsonHandle = (String) ClojureRuntime.jsonApi("register!",
                                                             jsonSession,
                                                             jsonHandlePrefix,
                                                             ClojureCodec.keyword(jsonHandlePrefix),
                                                             current);
            }
            inFlightJsonCalls++;
            return jsonHandle;
        }
    }

    private void endJsonCall() {
        synchronized (jsonLock) {
            inFlightJsonCalls--;
            if (inFlightJsonCalls == 0) {
                jsonLock.notifyAll();
            }
        }
    }

    private void awaitJsonCalls() {
        boolean interrupted = false;
        while (inFlightJsonCalls > 0) {
            try {
                jsonLock.wait();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        Object current = resource.getAndSet(null);
        if (current == null) {
            return;
        }
        String handleToUnregister;
        synchronized (jsonLock) {
            awaitJsonCalls();
            handleToUnregister = jsonHandle;
            jsonHandle = null;
        }
        try {
            if (handleToUnregister != null) {
                ClojureRuntime.jsonApi("unregister!", jsonSession, handleToUnregister);
            }
        } finally {
            closer.close(current);
        }
    }
}
