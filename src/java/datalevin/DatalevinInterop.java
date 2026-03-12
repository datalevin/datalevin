package datalevin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Small bridge-oriented interop layer for non-Java bindings.
 *
 * <p>This surface favors plain JDK collections, opaque raw handles, and
 * unambiguous method names over Java-specific ergonomics. It is intended as a
 * foundation for higher-level Python or JavaScript bindings that talk to the
 * JVM through a bridge such as JPype or node-java-bridge.
 */
public final class DatalevinInterop {

    private DatalevinInterop() {
    }

    /**
     * Invokes a Datalevin core function and returns the raw Clojure result.
     */
    public static Object coreInvoke(String function, List<?> args) {
        return ClojureRuntime.core(function, normalizeArgs(args));
    }

    /**
     * Invokes a Datalevin core function and normalizes the result for bridge
     * runtimes.
     */
    public static Object coreInvokeBridge(String function, List<?> args) {
        return ClojureCodec.bridgeOutput(coreInvoke(function, args));
    }

    /**
     * Invokes a Datalevin client function and returns the raw Clojure result.
     */
    public static Object clientInvoke(String function, List<?> args) {
        return ClojureRuntime.client(function, normalizeArgs(args));
    }

    /**
     * Invokes a Datalevin client function and normalizes the result for bridge
     * runtimes.
     */
    public static Object clientInvokeBridge(String function, List<?> args) {
        return ClojureCodec.bridgeOutput(clientInvoke(function, args));
    }

    /**
     * Creates or opens a raw connection handle.
     */
    public static Object createConnection(String dir,
                                          Map<?, ?> schema,
                                          Map<?, ?> opts) {
        if (dir == null) {
            return ClojureRuntime.core("create-conn");
        }
        if (opts != null) {
            return ClojureRuntime.core("create-conn",
                                       dir,
                                       DatalevinForms.schemaInput(schema),
                                       DatalevinForms.optionsInput(opts));
        }
        if (schema != null) {
            return ClojureRuntime.core("create-conn",
                                       dir,
                                       DatalevinForms.schemaInput(schema));
        }
        return ClojureRuntime.core("create-conn", dir);
    }

    /**
     * Returns a shared raw connection handle.
     *
     * <p>When {@code dir} is {@code null}, this mirrors
     * {@link #createConnection(String, Map, Map)} because the underlying
     * Clojure API only supports shared lookup for path-addressed connections.
     */
    public static Object getConnection(String dir,
                                       Map<?, ?> schema,
                                       Map<?, ?> opts) {
        if (dir == null) {
            return createConnection(null, schema, opts);
        }
        if (opts != null) {
            return ClojureRuntime.core("get-conn",
                                       dir,
                                       DatalevinForms.schemaInput(schema),
                                       DatalevinForms.optionsInput(opts));
        }
        if (schema != null) {
            return ClojureRuntime.core("get-conn",
                                       dir,
                                       DatalevinForms.schemaInput(schema));
        }
        return ClojureRuntime.core("get-conn", dir);
    }

    /**
     * Closes a raw connection handle.
     */
    public static void closeConnection(Object conn) {
        ClojureRuntime.core("close", conn);
    }

    /**
     * Returns whether a raw connection handle has been closed.
     */
    public static boolean connectionClosed(Object conn) {
        return ClojureCodec.javaBoolean(ClojureRuntime.core("closed?", conn));
    }

    /**
     * Returns the raw immutable database value for a connection handle.
     */
    public static Object connectionDb(Object conn) {
        return ClojureRuntime.core("db", conn);
    }

    /**
     * Opens a raw KV handle.
     */
    public static Object openKeyValue(String dir, Map<?, ?> opts) {
        if (opts == null) {
            return ClojureRuntime.core("open-kv", dir);
        }
        return ClojureRuntime.core("open-kv", dir, DatalevinForms.optionsInput(opts));
    }

    /**
     * Closes a raw KV handle.
     */
    public static void closeKeyValue(Object kv) {
        ClojureRuntime.core("close-kv", kv);
    }

    /**
     * Returns whether a raw KV handle has been closed.
     */
    public static boolean keyValueClosed(Object kv) {
        return ClojureCodec.javaBoolean(ClojureRuntime.core("closed-kv?", kv));
    }

    /**
     * Opens a raw remote client handle.
     */
    public static Object newClient(String uri, Map<?, ?> opts) {
        if (opts == null) {
            return ClojureRuntime.client("new-client", uri);
        }
        return ClojureRuntime.client("new-client", uri, DatalevinForms.optionsInput(opts));
    }

    /**
     * Closes a raw remote client handle.
     */
    public static void closeClient(Object client) {
        ClojureRuntime.client("disconnect", client);
    }

    /**
     * Returns whether a raw remote client handle has been disconnected.
     */
    public static boolean clientDisconnected(Object client) {
        return ClojureCodec.javaBoolean(ClojureRuntime.client("disconnected?", client));
    }

    /**
     * Reads EDN text into a raw Clojure value.
     */
    public static Object readEdn(String edn) {
        return ClojureRuntime.readEdn(edn);
    }

    /**
     * Returns the current Datalevin/Clojure context class loader.
     *
     * <p>Bridge runtimes such as node-java-bridge can use this loader when
     * proxying dynamically generated Clojure classes back into another
     * language runtime.
     */
    public static ClassLoader currentClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    /**
     * Normalizes JVM results into bridge-safe JDK collections and scalar values.
     *
     * <p>This is primarily useful for runtimes such as Node's `java-bridge`
     * that struggle with opaque Clojure implementation classes nested inside
     * otherwise ordinary collections.
     */
    public static Object bridgeResult(Object value) {
        return ClojureCodec.bridgeOutput(value);
    }

    /**
     * Normalizes a keyword-like string into a raw Clojure keyword.
     */
    public static Object keyword(String value) {
        return ClojureCodec.keyword(value);
    }

    /**
     * Normalizes a symbol-like string into a raw Clojure symbol.
     */
    public static Object symbol(String value) {
        return ClojureCodec.symbol(value);
    }

    /**
     * Normalizes a schema map into the raw Clojure form expected by Datalevin.
     */
    public static Object schema(Map<?, ?> schema) {
        return DatalevinForms.schemaInput(schema);
    }

    /**
     * Normalizes an options map into the raw Clojure form expected by Datalevin.
     */
    public static Object options(Map<?, ?> opts) {
        return DatalevinForms.optionsInput(opts);
    }

    /**
     * Normalizes a UDF descriptor into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object udfDescriptor(Map<?, ?> descriptor) {
        return DatalevinForms.udfDescriptorInput(descriptor);
    }

    /**
     * Creates a raw UDF registry handle.
     */
    public static Object createUdfRegistry() {
        return ClojureRuntime.invoke("datalevin.udf", "create-registry");
    }

    /**
     * Registers a Java-backed UDF in a registry.
     */
    public static Object registerUdf(Object registry,
                                     Map<?, ?> descriptor,
                                     UdfFunction fn) {
        Object normalizedDescriptor = DatalevinForms.udfDescriptorInput(descriptor);
        return ClojureRuntime.invoke("datalevin.udf",
                                     "register!",
                                     registry,
                                     normalizedDescriptor,
                                     ClojureFns.udfFunction(fn, normalizedDescriptor));
    }

    /**
     * Unregisters a UDF from a registry.
     */
    public static Object unregisterUdf(Object registry, Map<?, ?> descriptor) {
        return ClojureRuntime.invoke("datalevin.udf",
                                     "unregister!",
                                     registry,
                                     DatalevinForms.udfDescriptorInput(descriptor));
    }

    /**
     * Returns whether a descriptor is registered in a registry.
     */
    public static boolean registeredUdf(Object registry, Map<?, ?> descriptor) {
        return ClojureCodec.javaBoolean(
                ClojureRuntime.invoke("datalevin.udf",
                                      "registered?",
                                      registry,
                                      DatalevinForms.udfDescriptorInput(descriptor)));
    }

    /**
     * Normalizes a schema rename map into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object renameMap(Map<?, ?> renameMap) {
        return DatalevinForms.renameMapInput(renameMap);
    }

    /**
     * Normalizes a collection of attribute names into the raw Clojure form
     * expected by Datalevin.
     */
    public static Object deleteAttrs(Collection<?> attrs) {
        return DatalevinForms.deleteAttrsInput(attrs);
    }

    /**
     * Normalizes a lookup ref value into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object lookupRef(Object value) {
        return DatalevinForms.lookupRefInput(value);
    }

    /**
     * Normalizes Datalog transaction data into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object txData(Object txData) {
        return DatalevinForms.txDataInput(txData);
    }

    /**
     * Normalizes KV transaction data into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object kvTxs(Object txs) {
        return DatalevinForms.kvTxsInput(txs);
    }

    /**
     * Normalizes a KV type value into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object kvType(Object value) {
        return DatalevinForms.typeInput(value);
    }

    /**
     * Normalizes a database type string into the raw Clojure form expected by
     * the client API.
     */
    public static Object databaseType(String dbType) {
        return DatalevinForms.createDatabaseType(dbType);
    }

    /**
     * Normalizes a role name into the raw Clojure form expected by the client
     * API.
     */
    public static Object role(String role) {
        return DatalevinForms.roleInput(role);
    }

    /**
     * Normalizes a permission keyword into the raw Clojure form expected by the
     * client API.
     */
    public static Object permissionKeyword(String value) {
        return DatalevinForms.permissionKeyword(value);
    }

    /**
     * Normalizes a permission target into the raw Clojure form expected by the
     * client API.
     */
    public static Object permissionTarget(String objectType, Object target) {
        return DatalevinForms.permissionTarget(objectType, target);
    }

    private static Object[] normalizeArgs(List<?> args) {
        if (args == null || args.isEmpty()) {
            return new Object[0];
        }
        Object[] normalized = new Object[args.size()];
        int i = 0;
        for (Object arg : args) {
            if (arg instanceof HandleResource handle) {
                normalized[i++] = ClojureCodec.runtimeInput(handle.handle());
            } else {
                normalized[i++] = ClojureCodec.runtimeInput(arg);
            }
        }
        return normalized;
    }
}
