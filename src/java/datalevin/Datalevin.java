package datalevin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import clojure.lang.Keyword;
import clojure.lang.Symbol;

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
        return new Connection(ClojureRuntime.core("create-conn"));
    }

    /**
     * Creates or opens a Datalog connection rooted at {@code dir}.
     */
    public static Connection createConn(String dir) {
        return new Connection(ClojureRuntime.core("create-conn", dir));
    }

    /**
     * Creates or opens a Datalog connection with a raw schema map.
     */
    public static Connection createConn(String dir, Map<?, ?> schema) {
        return new Connection(ClojureRuntime.core("create-conn",
                                                 dir,
                                                 DatalevinForms.schemaInput(schema)));
    }

    /**
     * Creates or opens a Datalog connection with a typed schema builder.
     */
    public static Connection createConn(String dir, Schema schema) {
        return new Connection(ClojureRuntime.core("create-conn",
                                                 dir,
                                                 schema == null ? null : schema.buildForm()));
    }

    /**
     * Creates or opens a Datalog connection with a raw schema map and options.
     */
    public static Connection createConn(String dir, Map<?, ?> schema, Map<?, ?> opts) {
        return new Connection(ClojureRuntime.core("create-conn",
                                                 dir,
                                                 DatalevinForms.schemaInput(schema),
                                                 DatalevinForms.optionsInput(opts)));
    }

    /**
     * Creates or opens a Datalog connection with a typed schema builder and
     * options.
     */
    public static Connection createConn(String dir, Schema schema, Map<?, ?> opts) {
        return new Connection(ClojureRuntime.core("create-conn",
                                                 dir,
                                                 schema == null ? null : schema.buildForm(),
                                                 DatalevinForms.optionsInput(opts)));
    }

    /**
     * Returns an anonymous connection managed by the Datalevin runtime.
     *
     * <p>The underlying Clojure API only supports shared lookup for
     * path-addressed connections, so the no-argument Java convenience mirrors
     * {@link #createConn()}.
     */
    public static Connection getConn() {
        return createConn();
    }

    /**
     * Returns a shared connection for {@code dir}, opening it if needed.
     */
    public static Connection getConn(String dir) {
        return new Connection(ClojureRuntime.core("get-conn", dir));
    }

    /**
     * Returns a shared connection and updates it with the given raw schema.
     */
    public static Connection getConn(String dir, Map<?, ?> schema) {
        return new Connection(ClojureRuntime.core("get-conn",
                                                 dir,
                                                 DatalevinForms.schemaInput(schema)));
    }

    /**
     * Returns a shared connection and updates it with the given typed schema.
     */
    public static Connection getConn(String dir, Schema schema) {
        return new Connection(ClojureRuntime.core("get-conn",
                                                 dir,
                                                 schema == null ? null : schema.buildForm()));
    }

    /**
     * Returns a shared connection with the given raw schema and options.
     */
    public static Connection getConn(String dir, Map<?, ?> schema, Map<?, ?> opts) {
        return new Connection(ClojureRuntime.core("get-conn",
                                                 dir,
                                                 DatalevinForms.schemaInput(schema),
                                                 DatalevinForms.optionsInput(opts)));
    }

    /**
     * Returns a shared connection with the given typed schema and options.
     */
    public static Connection getConn(String dir, Schema schema, Map<?, ?> opts) {
        return new Connection(ClojureRuntime.core("get-conn",
                                                 dir,
                                                 schema == null ? null : schema.buildForm(),
                                                 DatalevinForms.optionsInput(opts)));
    }

    /**
     * Opens a local KV store rooted at {@code dir}.
     */
    public static KV openKV(String dir) {
        return new KV(ClojureRuntime.core("open-kv", dir));
    }

    /**
     * Opens a local KV store with the given options.
     */
    public static KV openKV(String dir, Map<?, ?> opts) {
        return new KV(ClojureRuntime.core("open-kv", dir, DatalevinForms.optionsInput(opts)));
    }

    /**
     * Opens a remote admin client for the given Datalevin URI.
     */
    public static Client newClient(String uri) {
        return new Client(ClojureRuntime.client("new-client", uri));
    }

    /**
     * Opens a remote admin client for the given Datalevin URI and options.
     */
    public static Client newClient(String uri, Map<?, ?> opts) {
        return new Client(ClojureRuntime.client("new-client",
                                               uri,
                                               DatalevinForms.optionsInput(opts)));
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
     * Creates the unbounded range spec {@code [:all]}.
     */
    public static RangeSpec allRange() {
        return RangeSpec.all();
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
    public static Keyword kw(String value) {
        return ClojureCodec.keyword(value);
    }

    /**
     * Marks a Datalog variable such as {@code ?e} for query builder positions
     * that accept either variables or literal values.
     */
    public static Symbol var(String value) {
        Objects.requireNonNull(value, "value");
        return ClojureCodec.symbol(value.startsWith("?") ? value : "?" + value);
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
     * Casts a result value to a map.
     */
    public static Map<Object, Object> mapResult(Object value) {
        return (Map<Object, Object>) value;
    }

    @SuppressWarnings("unchecked")
    /**
     * Casts a result value to a list.
     */
    public static List<?> listResult(Object value) {
        return (List<?>) value;
    }

    @SuppressWarnings("unchecked")
    /**
     * Casts a result value to a set.
     */
    public static Set<?> setResult(Object value) {
        return (Set<?>) value;
    }
}
