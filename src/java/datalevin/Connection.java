package datalevin;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Handle for a Datalog connection.
 *
 * <p>Use instances with try-with-resources when you own the handle lifecycle.
 * Query and transaction methods accept either raw EDN-like values or the typed
 * builders in this package.
 */
public final class Connection extends HandleResource {

    Connection(String handle) {
        super(handle, "close", "conn");
    }

    /**
     * Returns whether this handle has been closed.
     */
    public boolean closed() {
        return isReleased() || JsonBridge.asBoolean(call("closed?"));
    }

    /**
     * Returns the current schema map.
     */
    public Map<String, Object> schema() {
        return JsonBridge.asMap(call("schema"));
    }

    /**
     * Applies a raw schema update and returns the updated schema.
     */
    public Map<String, Object> updateSchema(Map<String, ?> schemaUpdate) {
        return JsonBridge.asMap(call("update-schema", Datalevin.mapOf("schema-update", schemaUpdate)));
    }

    /**
     * Applies a typed schema update and returns the updated schema.
     */
    public Map<String, Object> updateSchema(Schema schemaUpdate) {
        return updateSchema(schemaUpdate == null ? null : schemaUpdate.build());
    }

    /**
     * Applies a raw schema update with attribute deletion and rename options.
     */
    public Map<String, Object> updateSchema(Map<String, ?> schemaUpdate,
                                            Collection<?> delAttrs,
                                            Map<?, ?> renameMap) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("schema-update", schemaUpdate);
        if (delAttrs != null) {
            args.put("del-attrs", delAttrs);
        }
        if (renameMap != null) {
            args.put("rename-map", renameMap);
        }
        return JsonBridge.asMap(call("update-schema", args));
    }

    /**
     * Applies a typed schema update with attribute deletion and rename options.
     */
    public Map<String, Object> updateSchema(Schema schemaUpdate,
                                            Collection<?> delAttrs,
                                            Map<?, ?> renameMap) {
        return updateSchema(schemaUpdate == null ? null : schemaUpdate.build(), delAttrs, renameMap);
    }

    /**
     * Returns the connection option map.
     */
    public Map<String, Object> opts() {
        return JsonBridge.asMap(call("opts"));
    }

    /**
     * Clears all data from the underlying database.
     */
    public void clear() {
        call("clear");
    }

    /**
     * Returns the highest entity id currently allocated.
     */
    public long maxEid() {
        return JsonBridge.asLong(call("max-eid"));
    }

    /**
     * Returns the current datalog index cache limit.
     */
    public long datalogIndexCacheLimit() {
        return JsonBridge.asLong(call("datalog-index-cache-limit"));
    }

    /**
     * Sets and returns the datalog index cache limit.
     */
    public long datalogIndexCacheLimit(long limit) {
        return JsonBridge.asLong(call("datalog-index-cache-limit", Datalevin.mapOf("limit", limit)));
    }

    /**
     * Resolves an entity id or lookup ref to an entity id.
     */
    public Object entid(Object eid) {
        return call("entid", Datalevin.mapOf("eid", eid));
    }

    /**
     * Returns a touched entity map for the given entity id or lookup ref.
     */
    public Map<String, Object> entity(Object eid) {
        return JsonBridge.asMapOrNull(call("entity", Datalevin.mapOf("eid", eid)));
    }

    /**
     * Pulls one entity using a raw selector value.
     */
    public Map<String, Object> pull(Object selector, Object eid) {
        return JsonBridge.asMapOrNull(call("pull", Datalevin.mapOf("selector", selector, "eid", eid)));
    }

    /**
     * Pulls one entity using a typed selector builder.
     */
    public Map<String, Object> pull(PullSelector selector, Object eid) {
        return pull(selector == null ? null : selector.build(), eid);
    }

    /**
     * Pulls many entities using a raw selector value.
     */
    public List<Object> pullMany(Object selector, List<?> eids) {
        return JsonBridge.asList(call("pull-many", Datalevin.mapOf("selector", selector, "eids", eids)));
    }

    /**
     * Pulls many entities using a typed selector builder.
     */
    public List<Object> pullMany(PullSelector selector, List<?> eids) {
        return pullMany(selector == null ? null : selector.build(), eids);
    }

    /**
     * Runs a query expressed as EDN text with positional inputs.
     */
    public Object query(String query, Object... inputs) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("query", query);
        if (inputs.length > 0) {
            args.put("inputs", Datalevin.listOf(inputs));
        }
        return call("q", args);
    }

    /**
     * Runs a query expressed as EDN text with positional inputs.
     */
    public Object query(String query, List<?> inputs) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("query", query);
        if (inputs != null && !inputs.isEmpty()) {
            args.put("inputs", inputs);
        }
        return call("q", args);
    }

    /**
     * Runs a typed query with no explicit extra inputs.
     */
    public Object query(DatalogQuery query) {
        Objects.requireNonNull(query, "query");
        return query(query.toEdn(), query.prepareInputs((List<?>) null));
    }

    /**
     * Runs a typed query with positional inputs.
     */
    public Object query(DatalogQuery query, Object... inputs) {
        Objects.requireNonNull(query, "query");
        return query(query.toEdn(), query.prepareInputs(inputs));
    }

    /**
     * Runs a typed query with positional inputs.
     */
    public Object query(DatalogQuery query, List<?> inputs) {
        Objects.requireNonNull(query, "query");
        return query(query.toEdn(), query.prepareInputs(inputs));
    }

    /**
     * Explains a query expressed as EDN text with positional inputs.
     */
    public Object explain(String query, Object... inputs) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("query", query);
        if (inputs.length > 0) {
            args.put("inputs", Datalevin.listOf(inputs));
        }
        return call("explain", args);
    }

    /**
     * Explains a query expressed as EDN text with positional inputs.
     */
    public Object explain(String query, List<?> inputs) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("query", query);
        if (inputs != null && !inputs.isEmpty()) {
            args.put("inputs", inputs);
        }
        return call("explain", args);
    }

    /**
     * Explains a query expressed as EDN text using explicit explain options.
     */
    public Object explain(String optsEdn, String query, List<?> inputs) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("opts", optsEdn, "query", query);
        if (inputs != null && !inputs.isEmpty()) {
            args.put("inputs", inputs);
        }
        return call("explain", args);
    }

    /**
     * Explains a typed query with no explicit extra inputs.
     */
    public Object explain(DatalogQuery query) {
        Objects.requireNonNull(query, "query");
        return explain(query.toEdn(), query.prepareInputs((List<?>) null));
    }

    /**
     * Explains a typed query with positional inputs.
     */
    public Object explain(DatalogQuery query, Object... inputs) {
        Objects.requireNonNull(query, "query");
        return explain(query.toEdn(), query.prepareInputs(inputs));
    }

    /**
     * Explains a typed query with positional inputs.
     */
    public Object explain(DatalogQuery query, List<?> inputs) {
        Objects.requireNonNull(query, "query");
        return explain(query.toEdn(), query.prepareInputs(inputs));
    }

    /**
     * Explains a typed query using explicit explain options.
     */
    public Object explain(String optsEdn, DatalogQuery query, List<?> inputs) {
        Objects.requireNonNull(query, "query");
        return explain(optsEdn, query.toEdn(), query.prepareInputs(inputs));
    }

    /**
     * Transacts raw transaction data and returns the transaction report.
     */
    public Map<String, Object> transact(Object txData) {
        return JsonBridge.asMap(call("transact!", Datalevin.mapOf("tx-data", txData)));
    }

    /**
     * Transacts typed transaction data and returns the transaction report.
     */
    public Map<String, Object> transact(TxData txData) {
        return transact(txData.build());
    }

    /**
     * Transacts raw transaction data with optional transaction metadata.
     */
    public Map<String, Object> transact(Object txData, Map<String, ?> txMeta) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("tx-data", txData);
        if (txMeta != null) {
            args.put("tx-meta", txMeta);
        }
        return JsonBridge.asMap(call("transact!", args));
    }

    /**
     * Transacts typed transaction data with optional transaction metadata.
     */
    public Map<String, Object> transact(TxData txData, Map<String, ?> txMeta) {
        return transact(txData.build(), txMeta);
    }

    /**
     * Escape hatch for calling a connection-scoped JSON API operation directly.
     */
    public Object exec(String op, Map<String, ?> args) {
        return super.call(op, args);
    }
}
