/**
 * User-facing Java wrappers for Datalevin.
 *
 * <p>The main entry point is {@link datalevin.Datalevin}, which opens typed
 * handles for Datalog, KV, and remote admin operations. For bridge-oriented
 * bindings such as JPype or node-java-bridge, {@link datalevin.DatalevinInterop}
 * exposes a smaller raw-handle surface with direct Clojure runtime values.
 *
 * <p>Keyword and symbol values can be passed directly as
 * {@link clojure.lang.Keyword} and {@link clojure.lang.Symbol}. Query,
 * transaction, schema, pull, and rule builders are available to avoid
 * hand-writing most EDN forms.
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
 */
package datalevin;
