package datalevin;

import datalevin.llm.LlamaEmbedder;
import datalevin.llm.LlamaGenerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

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
     * Creates a Datalog connection by bulk-loading Datom values.
     */
    public static Connection initDb(Object datoms) {
        return connFromDatoms(datoms, null, null, null);
    }

    /**
     * Creates or replaces a path-addressed Datalog database by bulk-loading
     * Datom values.
     */
    public static Connection initDb(Object datoms, String dir) {
        return connFromDatoms(datoms, dir, null, null);
    }

    /**
     * Creates or replaces a path-addressed Datalog database by bulk-loading
     * Datom values with a raw schema map.
     */
    public static Connection initDb(Object datoms, String dir, Map<?, ?> schema) {
        return connFromDatoms(datoms, dir, DatalevinForms.schemaInput(schema), null);
    }

    /**
     * Creates or replaces a path-addressed Datalog database by bulk-loading
     * Datom values with a typed schema builder.
     */
    public static Connection initDb(Object datoms, String dir, Schema schema) {
        return connFromDatoms(datoms, dir, schema == null ? null : schema.buildForm(), null);
    }

    /**
     * Creates or replaces a path-addressed Datalog database by bulk-loading
     * Datom values with a raw schema map and options.
     */
    public static Connection initDb(Object datoms, String dir, Map<?, ?> schema, Map<?, ?> opts) {
        return connFromDatoms(datoms,
                              dir,
                              DatalevinForms.schemaInput(schema),
                              DatalevinForms.optionsInput(opts));
    }

    /**
     * Creates or replaces a path-addressed Datalog database by bulk-loading
     * Datom values with a typed schema builder and options.
     */
    public static Connection initDb(Object datoms, String dir, Schema schema, Map<?, ?> opts) {
        return connFromDatoms(datoms,
                              dir,
                              schema == null ? null : schema.buildForm(),
                              DatalevinForms.optionsInput(opts));
    }

    /**
     * Bulk-loads Datom values into an existing connection and returns it.
     */
    public static Connection fillDb(Connection conn, Object datoms) {
        Objects.requireNonNull(conn, "conn");
        return conn.fillDb(datoms);
    }

    /**
     * Transacts raw transaction data asynchronously on {@code conn}.
     */
    public static CompletableFuture<Map<?, ?>> transactAsync(Connection conn, Object txData) {
        Objects.requireNonNull(conn, "conn");
        return conn.transactAsync(txData);
    }

    /**
     * Transacts raw transaction data asynchronously on {@code conn} with
     * optional transaction metadata.
     */
    public static CompletableFuture<Map<?, ?>> transactAsync(Connection conn,
                                                             Object txData,
                                                             Map<?, ?> txMeta) {
        Objects.requireNonNull(conn, "conn");
        return conn.transactAsync(txData, txMeta);
    }

    /**
     * Applies raw transaction data against {@code conn}'s current database
     * value without committing and returns the simulated transaction report.
     */
    public static Map<?, ?> txDataToSimulatedReport(Connection conn, Object txData) {
        Objects.requireNonNull(conn, "conn");
        return conn.txDataToSimulatedReport(txData);
    }

    /**
     * Applies typed transaction data against {@code conn}'s current database
     * value without committing and returns the simulated transaction report.
     */
    public static Map<?, ?> txDataToSimulatedReport(Connection conn, TxData txData) {
        Objects.requireNonNull(conn, "conn");
        return conn.txDataToSimulatedReport(txData);
    }

    /**
     * Registers a transaction listener with an auto-generated key.
     */
    public static Object listen(Connection conn, Consumer<Map<?, ?>> listener) {
        Objects.requireNonNull(conn, "conn");
        return conn.listen(listener);
    }

    /**
     * Registers or replaces a transaction listener under {@code key}.
     */
    public static Object listen(Connection conn, Object key, Consumer<Map<?, ?>> listener) {
        Objects.requireNonNull(conn, "conn");
        return conn.listen(key, listener);
    }

    /**
     * Removes a transaction listener by key.
     */
    public static void unlisten(Connection conn, Object key) {
        Objects.requireNonNull(conn, "conn");
        conn.unlisten(key);
    }

    /**
     * Returns the KV handle backing a Datalog connection.
     *
     * <p>The returned handle is borrowed from {@code conn}. Closing it does not
     * close the underlying store; close the Datalog connection instead.
     */
    public static KV datalogKV(Connection conn) {
        Objects.requireNonNull(conn, "conn");
        return conn.datalogKV();
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
     * Opens an explicit KV write transaction.
     */
    public static KVTransaction beginTransaction(KV kv) {
        Objects.requireNonNull(kv, "kv");
        return kv.beginTransaction();
    }

    /**
     * Runs {@code fn} inside a single KV write transaction.
     */
    public static <T> T withTransaction(KV kv, Function<KV, T> fn) {
        Objects.requireNonNull(kv, "kv");
        return kv.withTransaction(fn);
    }

    /**
     * Sets or clears LMDB environment flags for a KV store.
     */
    public static void setEnvFlags(KV kv, Collection<?> flags, boolean onOff) {
        Objects.requireNonNull(kv, "kv");
        kv.setEnvFlags(flags, onOff);
    }

    /**
     * Returns the LMDB environment flags currently in effect for a KV store.
     */
    public static Set<?> getEnvFlags(KV kv) {
        Objects.requireNonNull(kv, "kv");
        return kv.getEnvFlags();
    }

    /**
     * Runs {@code fn} inside a single Datalog write transaction.
     */
    public static <T> T withTransaction(Connection conn, Function<Connection, T> fn) {
        Objects.requireNonNull(conn, "conn");
        return conn.withTransaction(fn);
    }

    /**
     * Creates a batched full-text search index writer for {@code kv}.
     */
    public static SearchIndexWriter searchIndexWriter(KV kv) {
        Objects.requireNonNull(kv, "kv");
        return kv.searchIndexWriter();
    }

    /**
     * Creates a batched full-text search index writer with raw options.
     */
    public static SearchIndexWriter searchIndexWriter(KV kv, Map<?, ?> opts) {
        Objects.requireNonNull(kv, "kv");
        return kv.searchIndexWriter(opts);
    }

    /**
     * Creates a batched full-text search index writer with typed options.
     */
    public static SearchIndexWriter searchIndexWriter(KV kv, RetrievalOptions opts) {
        Objects.requireNonNull(kv, "kv");
        return kv.searchIndexWriter(opts);
    }

    /**
     * Creates a full-text search engine over {@code kv}.
     */
    public static SearchEngine newSearchEngine(KV kv) {
        Objects.requireNonNull(kv, "kv");
        return kv.newSearchEngine();
    }

    /**
     * Creates a full-text search engine with raw options.
     */
    public static SearchEngine newSearchEngine(KV kv, Map<?, ?> opts) {
        Objects.requireNonNull(kv, "kv");
        return kv.newSearchEngine(opts);
    }

    /**
     * Creates a full-text search engine with typed options.
     */
    public static SearchEngine newSearchEngine(KV kv, RetrievalOptions opts) {
        Objects.requireNonNull(kv, "kv");
        return kv.newSearchEngine(opts);
    }

    /**
     * Creates a standalone vector index over {@code kv} with raw options.
     */
    public static VectorIndex newVectorIndex(KV kv, Map<?, ?> opts) {
        Objects.requireNonNull(kv, "kv");
        return kv.newVectorIndex(opts);
    }

    /**
     * Creates a standalone vector index over {@code kv} with typed options.
     */
    public static VectorIndex newVectorIndex(KV kv, RetrievalOptions opts) {
        Objects.requireNonNull(kv, "kv");
        return kv.newVectorIndex(opts);
    }

    /**
     * Rebuilds a full-text search engine from stored raw text.
     */
    public static SearchEngine reIndex(SearchEngine engine) {
        Objects.requireNonNull(engine, "engine");
        return engine.reIndex();
    }

    /**
     * Rebuilds a full-text search engine from stored raw text with raw options.
     */
    public static SearchEngine reIndex(SearchEngine engine, Map<?, ?> opts) {
        Objects.requireNonNull(engine, "engine");
        return engine.reIndex(opts);
    }

    /**
     * Rebuilds a full-text search engine from stored raw text with typed options.
     */
    public static SearchEngine reIndex(SearchEngine engine, RetrievalOptions opts) {
        Objects.requireNonNull(engine, "engine");
        return engine.reIndex(opts);
    }

    /**
     * Rebuilds a standalone vector index with its previous options.
     */
    public static VectorIndex reIndex(VectorIndex index) {
        Objects.requireNonNull(index, "index");
        return index.reIndex();
    }

    /**
     * Rebuilds a standalone vector index with raw options.
     */
    public static VectorIndex reIndex(VectorIndex index, Map<?, ?> opts) {
        Objects.requireNonNull(index, "index");
        return index.reIndex(opts);
    }

    /**
     * Rebuilds a standalone vector index with typed options.
     */
    public static VectorIndex reIndex(VectorIndex index, RetrievalOptions opts) {
        Objects.requireNonNull(index, "index");
        return index.reIndex(opts);
    }

    /**
     * Rebuilds a KV store index and returns the same wrapper.
     */
    public static KV reIndex(KV kv) {
        Objects.requireNonNull(kv, "kv");
        return kv.reIndex();
    }

    /**
     * Rebuilds a KV store index with options and returns the same wrapper.
     */
    public static KV reIndex(KV kv, Map<?, ?> opts) {
        Objects.requireNonNull(kv, "kv");
        return kv.reIndex(opts);
    }

    /**
     * Rebuilds a Datalog database index and returns the same wrapper.
     */
    public static Connection reIndex(Connection conn) {
        Objects.requireNonNull(conn, "conn");
        return conn.reIndex();
    }

    /**
     * Rebuilds a Datalog database index with options and returns the same wrapper.
     */
    public static Connection reIndex(Connection conn, Map<?, ?> opts) {
        Objects.requireNonNull(conn, "conn");
        return conn.reIndex(opts);
    }

    /**
     * Rebuilds a Datalog database index with a raw schema and options.
     */
    public static Connection reIndex(Connection conn, Map<?, ?> schema, Map<?, ?> opts) {
        Objects.requireNonNull(conn, "conn");
        return conn.reIndex(schema, opts);
    }

    /**
     * Rebuilds a Datalog database index with a typed schema and options.
     */
    public static Connection reIndex(Connection conn, Schema schema, Map<?, ?> opts) {
        Objects.requireNonNull(conn, "conn");
        return conn.reIndex(schema, opts);
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
     * Creates a local llama.cpp text embedder using native defaults.
     */
    public static LlamaEmbedder newLlamaEmbedder(String modelPath) {
        return new LlamaEmbedder(modelPath);
    }

    /**
     * Creates a local llama.cpp text embedder with explicit tuning options.
     */
    public static LlamaEmbedder newLlamaEmbedder(String modelPath,
                                                 int gpuLayers,
                                                 int ctxSize,
                                                 int batchSize,
                                                 int threads) {
        return new LlamaEmbedder(modelPath, gpuLayers, ctxSize, batchSize, threads);
    }

    /**
     * Creates a local llama.cpp text generator using native defaults.
     */
    public static LlamaGenerator newLlamaGenerator(String modelPath) {
        return new LlamaGenerator(modelPath);
    }

    /**
     * Creates a local llama.cpp text generator with explicit tuning options.
     */
    public static LlamaGenerator newLlamaGenerator(String modelPath,
                                                   int gpuLayers,
                                                   int ctxSize,
                                                   int threads) {
        return new LlamaGenerator(modelPath, gpuLayers, ctxSize, threads);
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
     * Creates a full-text query/default option builder.
     */
    public static RetrievalOptions searchOptions() {
        return RetrievalOptions.search();
    }

    /**
     * Creates a full-text domain option builder.
     */
    public static RetrievalOptions searchDomain() {
        return RetrievalOptions.searchDomain();
    }

    /**
     * Creates a vector index/domain option builder with required dimensions.
     */
    public static RetrievalOptions vectorOptions(long dimensions) {
        return RetrievalOptions.vector(dimensions);
    }

    /**
     * Creates an embedding provider/domain option builder.
     */
    public static RetrievalOptions embeddingOptions() {
        return RetrievalOptions.embedding();
    }

    /**
     * Creates an idoc match option builder.
     */
    public static RetrievalOptions idocOptions() {
        return RetrievalOptions.idoc();
    }

    /**
     * Creates a raw UDF registry handle.
     */
    public static Object createUdfRegistry() {
        return DatalevinInterop.createUdfRegistry();
    }

    /**
     * Creates a typed UDF registry wrapper.
     */
    public static UdfRegistry udfRegistry() {
        return new UdfRegistry(DatalevinInterop.createUdfRegistry());
    }

    /**
     * Creates a query function UDF descriptor.
     */
    public static UdfDescriptor queryUdf(String id) {
        return UdfDescriptor.queryFn(id);
    }

    /**
     * Creates a query predicate UDF descriptor.
     */
    public static UdfDescriptor predicateUdf(String id) {
        return UdfDescriptor.predicate(id);
    }

    /**
     * Creates a transaction function UDF descriptor.
     */
    public static UdfDescriptor txUdf(String id) {
        return UdfDescriptor.txFn(id);
    }

    /**
     * Creates a full-text analyzer UDF descriptor.
     */
    public static UdfDescriptor analyzerUdf(String id) {
        return UdfDescriptor.analyzer(id);
    }

    /**
     * Creates a full-text query analyzer UDF descriptor.
     */
    public static UdfDescriptor queryAnalyzerUdf(String id) {
        return UdfDescriptor.queryAnalyzer(id);
    }

    /**
     * Normalizes a UDF descriptor into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object udfDescriptor(Map<?, ?> descriptor) {
        return DatalevinInterop.udfDescriptor(descriptor);
    }

    /**
     * Normalizes a typed UDF descriptor into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object udfDescriptor(UdfDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        return descriptor.buildForm();
    }

    /**
     * Creates a typed UDF descriptor for the given kind and id.
     */
    public static UdfDescriptor udfDescriptor(String kind, String id) {
        return UdfDescriptor.of(kind, id);
    }

    /**
     * Registers a Java-backed UDF in a registry.
     */
    public static Object registerUdf(Object registry, Map<?, ?> descriptor, UdfFunction fn) {
        return DatalevinInterop.registerUdf(registry, descriptor, fn);
    }

    /**
     * Unregisters a UDF from a registry.
     */
    public static Object unregisterUdf(Object registry, Map<?, ?> descriptor) {
        return DatalevinInterop.unregisterUdf(registry, descriptor);
    }

    /**
     * Returns whether a descriptor is registered in a registry.
     */
    public static boolean registeredUdf(Object registry, Map<?, ?> descriptor) {
        return DatalevinInterop.registeredUdf(registry, descriptor);
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
     * Reads EDN text into the corresponding JVM/Clojure value.
     */
    public static Object readEdn(String value) {
        Objects.requireNonNull(value, "value");
        return ClojureRuntime.readEdn(value);
    }

    /**
     * Writes a JVM/Clojure value as EDN text.
     */
    public static String writeEdn(Object value) {
        return Edn.render(ClojureCodec.runtimeInput(value));
    }

    /**
     * Marks a keyword value such as {@code :person/name} for APIs that need an
     * EDN keyword rather than a Java string.
     */
    public static Keyword kw(String value) {
        return ClojureCodec.keyword(value);
    }

    /**
     * Marks a symbol value such as {@code person/name} or {@code ?e}.
     */
    public static Symbol sym(String value) {
        Objects.requireNonNull(value, "value");
        return ClojureCodec.symbol(value);
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
     * Creates a Datalevin Datom from entity id, attribute, and value.
     */
    public static Object datom(Object e, Object attr, Object value) {
        return DatalevinForms.datom(e, attr, value);
    }

    /**
     * Creates a Datalevin Datom with an explicit transaction id.
     */
    public static Object datom(Object e, Object attr, Object value, Object tx) {
        return DatalevinForms.datom(e, attr, value, tx);
    }

    /**
     * Creates a Datalevin Datom with an explicit transaction id and assertion
     * flag.
     */
    public static Object datom(Object e, Object attr, Object value, Object tx, Object added) {
        return DatalevinForms.datom(e, attr, value, tx, added);
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

    private static Connection connFromDatoms(Object datoms, String dir, Object schema, Object opts) {
        Object normalizedDatoms = DatalevinForms.datomsInput(datoms);
        if (opts != null) {
            return new Connection(ClojureRuntime.core("conn-from-datoms",
                                                     normalizedDatoms,
                                                     dir,
                                                     schema,
                                                     opts));
        }
        if (schema != null) {
            return new Connection(ClojureRuntime.core("conn-from-datoms",
                                                     normalizedDatoms,
                                                     dir,
                                                     schema));
        }
        if (dir != null) {
            return new Connection(ClojureRuntime.core("conn-from-datoms", normalizedDatoms, dir));
        }
        return new Connection(ClojureRuntime.core("conn-from-datoms", normalizedDatoms));
    }
}
