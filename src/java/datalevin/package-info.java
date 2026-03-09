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
 */
package datalevin;
