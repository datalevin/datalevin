package datalevin;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Handle for an in-process Datalevin server.
 *
 * <p>This class is packaged in the {@code org.datalevin:datalevin-java-server}
 * artifact. Use it with try-with-resources when embedding a server in a JVM
 * process.
 */
public final class DatalevinServer implements AutoCloseable {
    private static final String SERVER_NS = "datalevin.server";

    private final AtomicReference<Object> server;

    private DatalevinServer(Object server) {
        this.server = new AtomicReference<>(Objects.requireNonNull(server, "server"));
    }

    /**
     * Creates a server with Datalevin's default server options.
     */
    public static DatalevinServer create() {
        return create(Map.of());
    }

    /**
     * Creates a server rooted at {@code root}.
     */
    public static DatalevinServer create(String root) {
        return create(Map.of("root", Objects.requireNonNull(root, "root")));
    }

    /**
     * Creates a server with raw option map keys such as {@code host},
     * {@code port}, {@code root}, {@code idle-timeout}, and {@code verbose}.
     */
    public static DatalevinServer create(Map<?, ?> opts) {
        ClojureRuntime.requireNamespace(SERVER_NS);
        return new DatalevinServer(
                ClojureRuntime.invoke(SERVER_NS,
                                      "create",
                                      DatalevinForms.optionsInput(opts == null ? Map.of() : opts)));
    }

    /**
     * Returns the underlying Clojure server value.
     */
    public Object handle() {
        return requireServer();
    }

    /**
     * Returns whether this Java handle has not been closed.
     */
    public boolean isOpen() {
        return server.get() != null;
    }

    /**
     * Starts the server and returns this handle.
     */
    public DatalevinServer start() {
        ClojureRuntime.invoke(SERVER_NS, "start", requireServer());
        return this;
    }

    /**
     * Stops the server. This method is idempotent and releases the Java handle.
     */
    public void stop() {
        close();
    }

    @Override
    public void close() {
        Object current = server.getAndSet(null);
        if (current != null) {
            ClojureRuntime.invoke(SERVER_NS, "stop", current);
        }
    }

    private Object requireServer() {
        Object current = server.get();
        if (current == null) {
            throw new IllegalStateException("DatalevinServer is closed.");
        }
        return current;
    }
}
