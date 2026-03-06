package datalevin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Static entry point for the high-level Java API.
 *
 * <p>This class creates typed handles for local Datalog databases, local KV
 * stores, and remote admin clients. It also exposes small builder and utility
 * helpers used throughout the Java wrapper layer.
 */
public final class Datalevin {

    /** Database type constant for Datalog databases. */
    public static final String DB_DATALOG = "datalog";
    /** Database type constant for KV databases. */
    public static final String DB_KV = "kv";
    /** Database type constant for engine databases. */
    public static final String DB_ENGINE = "engine";

    private Datalevin() {
    }

    /**
     * Returns JSON API metadata such as version and supported operations.
     */
    public static Map<String, Object> apiInfo() {
        return JsonBridge.asMap(JsonBridge.call("api-info"));
    }

    /**
     * Executes a raw JSON API operation without arguments.
     */
    public static Object exec(String op) {
        return JsonBridge.call(op);
    }

    /**
     * Executes a raw JSON API operation with the given argument map.
     */
    public static Object exec(String op, Map<String, ?> args) {
        return JsonBridge.call(op, args);
    }

    /**
     * Creates an anonymous in-memory-like Datalog connection managed by the
     * underlying Datalevin runtime.
     */
    public static Connection createConn() {
        return new Connection(JsonBridge.asString(JsonBridge.call("create-conn")));
    }

    /**
     * Creates or opens a Datalog connection rooted at {@code dir}.
     */
    public static Connection createConn(String dir) {
        return new Connection(JsonBridge.asString(JsonBridge.call("create-conn", mapOf("dir", dir))));
    }

    /**
     * Creates or opens a Datalog connection with a raw schema map.
     */
    public static Connection createConn(String dir, Map<String, ?> schema) {
        return new Connection(JsonBridge.asString(
                JsonBridge.call("create-conn", mapOf("dir", dir, "schema", schema))
        ));
    }

    /**
     * Creates or opens a Datalog connection with a typed schema builder.
     */
    public static Connection createConn(String dir, Schema schema) {
        return createConn(dir, schema == null ? null : schema.build());
    }

    /**
     * Creates or opens a Datalog connection with a raw schema map and options.
     */
    public static Connection createConn(String dir, Map<String, ?> schema, Map<String, ?> opts) {
        return new Connection(JsonBridge.asString(
                JsonBridge.call("create-conn", mapOf("dir", dir, "schema", schema, "opts", opts))
        ));
    }

    /**
     * Creates or opens a Datalog connection with a typed schema builder and
     * options.
     */
    public static Connection createConn(String dir, Schema schema, Map<String, ?> opts) {
        return createConn(dir, schema == null ? null : schema.build(), opts);
    }

    /**
     * Returns the current shared connection when one is available.
     */
    public static Connection getConn() {
        return new Connection(JsonBridge.asString(JsonBridge.call("get-conn")));
    }

    /**
     * Returns a shared connection for {@code dir}, opening it if needed.
     */
    public static Connection getConn(String dir) {
        return new Connection(JsonBridge.asString(JsonBridge.call("get-conn", mapOf("dir", dir))));
    }

    /**
     * Returns a shared connection and updates it with the given raw schema.
     */
    public static Connection getConn(String dir, Map<String, ?> schema) {
        return new Connection(JsonBridge.asString(
                JsonBridge.call("get-conn", mapOf("dir", dir, "schema", schema))
        ));
    }

    /**
     * Returns a shared connection and updates it with the given typed schema.
     */
    public static Connection getConn(String dir, Schema schema) {
        return getConn(dir, schema == null ? null : schema.build());
    }

    /**
     * Returns a shared connection with the given raw schema and options.
     */
    public static Connection getConn(String dir, Map<String, ?> schema, Map<String, ?> opts) {
        return new Connection(JsonBridge.asString(
                JsonBridge.call("get-conn", mapOf("dir", dir, "schema", schema, "opts", opts))
        ));
    }

    /**
     * Returns a shared connection with the given typed schema and options.
     */
    public static Connection getConn(String dir, Schema schema, Map<String, ?> opts) {
        return getConn(dir, schema == null ? null : schema.build(), opts);
    }

    /**
     * Opens a local KV store rooted at {@code dir}.
     */
    public static KV openKV(String dir) {
        return new KV(JsonBridge.asString(JsonBridge.call("open-kv", mapOf("dir", dir))));
    }

    /**
     * Opens a local KV store with the given options.
     */
    public static KV openKV(String dir, Map<String, ?> opts) {
        return new KV(JsonBridge.asString(
                JsonBridge.call("open-kv", mapOf("dir", dir, "opts", opts))
        ));
    }

    /**
     * Opens a remote admin client for the given Datalevin URI.
     */
    public static Client newClient(String uri) {
        return new Client(JsonBridge.asString(JsonBridge.call("new-client", mapOf("uri", uri))));
    }

    /**
     * Opens a remote admin client for the given Datalevin URI and options.
     */
    public static Client newClient(String uri, Map<String, ?> opts) {
        return new Client(JsonBridge.asString(
                JsonBridge.call("new-client", mapOf("uri", uri, "opts", opts))
        ));
    }

    /**
     * Creates a typed Datalog query builder.
     */
    public static DatalogQuery query() {
        return new DatalogQuery();
    }

    /**
     * Creates a typed transaction builder.
     */
    public static TxData tx() {
        return new TxData();
    }

    /**
     * Creates a typed rules builder for Datalog queries.
     */
    public static Rules rules() {
        return new Rules();
    }

    /**
     * Creates a typed pull selector builder.
     */
    public static PullSelector pull() {
        return new PullSelector();
    }

    /**
     * Creates a typed schema builder.
     */
    public static Schema schema() {
        return new Schema();
    }

    /**
     * Normalizes a keyword-like string to the {@code :keyword} representation
     * used by the Java wrapper.
     */
    public static String keyword(String value) {
        Objects.requireNonNull(value, "value");
        return value.startsWith(":") ? value : ":" + value;
    }

    /**
     * Marks raw EDN text for APIs that accept explicit EDN values.
     */
    public static Object edn(String value) {
        Objects.requireNonNull(value, "value");
        return new EdnLiteral(value);
    }

    /**
     * Marks a keyword value such as {@code :person/name} for APIs that need an
     * EDN keyword rather than a Java string.
     */
    public static Object kw(String value) {
        return edn(keyword(value));
    }

    /**
     * Marks a Datalog variable such as {@code ?e} for query builder positions
     * that accept either variables or literal values.
     */
    public static Object var(String value) {
        Objects.requireNonNull(value, "value");
        return new EdnLiteral(value.startsWith("?") ? value : "?" + value);
    }

    /**
     * Creates an ordered string-keyed map from alternating key and value pairs.
     */
    public static LinkedHashMap<String, Object> mapOf(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("mapOf expects an even number of arguments.");
        }

        LinkedHashMap<String, Object> map = new LinkedHashMap<>(keyValues.length / 2);
        for (int i = 0; i < keyValues.length; i += 2) {
            Object key = keyValues[i];
            if (!(key instanceof String s)) {
                throw new IllegalArgumentException("mapOf expects string keys, got: " + key);
            }
            map.put(s, keyValues[i + 1]);
        }
        return map;
    }

    /**
     * Creates an ordered map from alternating key and value pairs.
     */
    public static LinkedHashMap<Object, Object> orderedMap(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("orderedMap expects an even number of arguments.");
        }

        LinkedHashMap<Object, Object> map = new LinkedHashMap<>(keyValues.length / 2);
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    /**
     * Creates a mutable list from the given values.
     */
    public static ArrayList<Object> listOf(Object... values) {
        ArrayList<Object> list = new ArrayList<>(values.length);
        for (Object value : values) {
            list.add(value);
        }
        return list;
    }

    /**
     * Creates a mutable insertion-ordered set from the given values.
     */
    public static LinkedHashSet<Object> setOf(Object... values) {
        LinkedHashSet<Object> set = new LinkedHashSet<>(values.length);
        for (Object value : values) {
            set.add(value);
        }
        return set;
    }

    @SuppressWarnings("unchecked")
    /**
     * Casts a result value to a string-keyed map.
     */
    public static Map<String, Object> mapResult(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    /**
     * Casts a result value to a list.
     */
    public static List<Object> listResult(Object value) {
        return (List<Object>) value;
    }

    @SuppressWarnings("unchecked")
    /**
     * Casts a result value to a set.
     */
    public static Set<Object> setResult(Object value) {
        return (Set<Object>) value;
    }
}
