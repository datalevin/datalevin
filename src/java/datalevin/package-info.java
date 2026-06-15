/**
 * User-facing Java wrappers for Datalevin.
 *
 * <p>The main entry point is {@link datalevin.Datalevin}, which opens typed
 * handles for Datalog, KV, and remote admin operations. For bridge-oriented
 * bindings such as JPype or node-java-bridge, {@link datalevin.DatalevinInterop}
 * exposes a smaller raw-handle surface with direct Clojure runtime values.
 *
 * <p>The canonical Java style is to use the typed builders for schemas,
 * transactions, queries, pull selectors, and rules. Use
 * {@link datalevin.Datalevin#kw}, {@link datalevin.Datalevin#sym},
 * {@link datalevin.Datalevin#readEdn}, and
 * {@link datalevin.Datalevin#writeEdn} when explicit EDN values are needed.
 * Raw EDN text can be marked with {@link datalevin.Datalevin#edn} for APIs
 * that accept an EDN form.
 *
 * <p>Typical usage:
 *
 * <pre>{@code
 * try (Connection conn = Datalevin.createConn("/tmp/example",
 *         Datalevin.schema()
 *             .attr("name", Schema.attribute().valueType(Schema.ValueType.STRING)))) {
 *     conn.transact(Datalevin.tx()
 *         .entity(Tx.entity(-1).put("name", "Alice")));
 *
 *     List<String> names = conn.queryCollection(Datalevin.query()
 *         .findAll("?name")
 *         .whereDatom(Datalevin.var("e"), "name", Datalevin.var("name")),
 *         String.class);
 * }
 * }</pre>
 *
 * <p>For async ingestion workloads, use
 * {@link datalevin.Connection#transactAsync}, which returns a
 * {@link java.util.concurrent.CompletableFuture} containing the transaction
 * report.
 *
 * <p>For runtime UDFs, use {@link datalevin.Datalevin#udfRegistry} and
 * {@link datalevin.UdfDescriptor}. Pass the registry through connection
 * runtime options and call descriptors from query with Datalevin's {@code udf}
 * function:
 *
 * <pre>{@code
 * UdfRegistry registry = Datalevin.udfRegistry()
 *     .queryFn("math/inc", args -> ((Number) args.get(0)).longValue() + 1);
 * UdfDescriptor descriptor = UdfDescriptor.queryFn("math/inc");
 *
 * try (Connection conn = Datalevin.createConn("/tmp/example-udf", (Map<?, ?>) null,
 *         Map.of(":runtime-opts", Map.of(":udf-registry", registry)))) {
 *     Object value = conn.query(
 *         "[:find ?v . :in $ ?desc ?n :where [(udf ?desc ?n) ?v]]",
 *         descriptor,
 *         41L);
 * }
 * }</pre>
 *
 * <p>For bulk load, use Datom-shaped input with {@link datalevin.Datalevin#initDb}
 * and {@link datalevin.Connection#fillDb}:
 *
 * <pre>{@code
 * try (Connection conn = Datalevin.initDb(
 *         Datalevin.listOf(Datalevin.datom(1, "name", "Alice")),
 *         "/tmp/example-bulk",
 *         Datalevin.schema()
 *             .attr("name", Schema.attribute().valueType(Schema.ValueType.STRING)))) {
 *     conn.fillDb(Datalevin.listOf(Datalevin.datom(2, "name", "Bob")));
 * }
 * }</pre>
 *
 * <p>Operational helpers are available on local handles, for example
 * {@link datalevin.KV#copy}, {@link datalevin.KV#sync},
 * {@link datalevin.KV#createSnapshot}, {@link datalevin.KV#txLogWatermarks},
 * {@link datalevin.KV#openTxLog}, and {@link datalevin.KV#gcTxLogSegments}.
 *
 * <p>Datalog connections expose lower-level inspection helpers including
 * {@link datalevin.Connection#datoms}, {@link datalevin.Connection#seekDatoms},
 * {@link datalevin.Connection#indexRange},
 * {@link datalevin.Connection#countDatoms}, and
 * {@link datalevin.Connection#fulltextDatoms}.
 *
 * <p>Use {@link datalevin.Connection#datalogKV} when you need the KV handle
 * backing a Datalog connection. The returned KV wrapper is borrowed from the
 * connection; close the connection, not that KV wrapper.
 */
package datalevin;
