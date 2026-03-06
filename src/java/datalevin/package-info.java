/**
 * User-facing Java wrappers for Datalevin.
 *
 * <p>The main entry point is {@link datalevin.Datalevin}, which opens typed
 * handles for Datalog, KV, and remote admin operations. These wrappers sit on
 * top of the JSON API and translate common Java values to and from Datalevin's
 * data model.
 *
 * <p>Keyword-like values are represented as strings such as {@code ":name"} and
 * {@code ":db.type/string"}. Query, transaction, schema, pull, and rule
 * builders are available to avoid hand-writing most EDN forms.
 *
 * <p>Typical usage:
 *
 * <pre>{@code
 * try (Connection conn = Datalevin.createConn("/tmp/example",
 *         Datalevin.schema()
 *             .attr("name", Schema.attribute().valueType("db.type/string")))) {
 *     conn.transact(Datalevin.tx()
 *         .entity(Tx.entity(-1).put("name", "Alice")));
 *
 *     Object names = conn.query(Datalevin.query()
 *         .findAll("?name")
 *         .whereDatom(Datalevin.var("e"), "name", Datalevin.var("name")));
 * }
 * }</pre>
 */
package datalevin;
