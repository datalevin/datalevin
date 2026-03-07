package datalevin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    Connection(Object conn) {
        super(conn,
              resource -> ClojureBridge.core("close", resource),
              "conn",
              "conn");
    }

    /**
     * Returns whether this handle has been closed.
     */
    public boolean closed() {
        return isReleased() || ClojureBridge.javaBoolean(ClojureBridge.core("closed?", resource()));
    }

    /**
     * Returns the current schema map.
     */
    public Map<String, Object> schema() {
        return ClojureBridge.javaMap(ClojureBridge.core("schema", resource()));
    }

    /**
     * Applies a raw schema update and returns the updated schema.
     */
    public Map<String, Object> updateSchema(Map<String, ?> schemaUpdate) {
        return ClojureBridge.javaMap(
                ClojureBridge.core("update-schema",
                                   resource(),
                                   ClojureBridge.schemaInput(schemaUpdate))
        );
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
        Object normalizedSchema = ClojureBridge.schemaInput(schemaUpdate);
        Object normalizedDelAttrs = ClojureBridge.deleteAttrsInput(delAttrs);
        Object normalizedRenameMap = ClojureBridge.renameMapInput(renameMap);
        if (renameMap != null) {
            return ClojureBridge.javaMap(
                    ClojureBridge.core("update-schema",
                                       resource(),
                                       normalizedSchema,
                                       normalizedDelAttrs,
                                       normalizedRenameMap)
            );
        }
        if (delAttrs != null) {
            return ClojureBridge.javaMap(
                    ClojureBridge.core("update-schema",
                                       resource(),
                                       normalizedSchema,
                                       normalizedDelAttrs)
            );
        }
        return ClojureBridge.javaMap(
                ClojureBridge.core("update-schema", resource(), normalizedSchema)
        );
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
        return ClojureBridge.javaMap(ClojureBridge.core("opts", resource()));
    }

    /**
     * Clears all data from the underlying database.
     */
    public void clear() {
        ClojureBridge.core("clear", resource());
    }

    /**
     * Returns the highest entity id currently allocated.
     */
    public long maxEid() {
        return ClojureBridge.javaLong(ClojureBridge.core("max-eid", db()));
    }

    /**
     * Returns the current datalog index cache limit.
     */
    public long datalogIndexCacheLimit() {
        return ClojureBridge.javaLong(ClojureBridge.core("datalog-index-cache-limit", db()));
    }

    /**
     * Sets and returns the datalog index cache limit.
     */
    public long datalogIndexCacheLimit(long limit) {
        return ClojureBridge.javaLong(
                ClojureBridge.core("datalog-index-cache-limit", db(), limit)
        );
    }

    /**
     * Resolves an entity id or lookup ref to an entity id.
     */
    public Object entid(Object eid) {
        return ClojureBridge.toJava(
                ClojureBridge.core("entid", db(), ClojureBridge.lookupRefInput(eid))
        );
    }

    /**
     * Returns a touched entity map for the given entity id or lookup ref.
     */
    public Map<String, Object> entity(Object eid) {
        Object entity = ClojureBridge.core("entity", db(), ClojureBridge.lookupRefInput(eid));
        if (entity == null) {
            return null;
        }
        return ClojureBridge.javaMapOrNull(ClojureBridge.core("touch", entity));
    }

    /**
     * Pulls one entity using a raw selector value.
     */
    public Map<String, Object> pull(Object selector, Object eid) {
        return ClojureBridge.javaMapOrNull(
                ClojureBridge.core("pull",
                                   db(),
                                   ClojureBridge.pullSelectorInput(selector),
                                   ClojureBridge.lookupRefInput(eid))
        );
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
        ArrayList<Object> normalized = new ArrayList<>(eids.size());
        for (Object eid : eids) {
            normalized.add(ClojureBridge.lookupRefInput(eid));
        }
        return ClojureBridge.javaList(
                ClojureBridge.core("pull-many",
                                   db(),
                                   ClojureBridge.pullSelectorInput(selector),
                                   ClojureBridge.genericInput(normalized))
        );
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
        return runQuery(ClojureBridge.queryForm(query), Arrays.asList(inputs));
    }

    /**
     * Runs a query expressed as EDN text with positional inputs.
     */
    public Object query(String query, List<?> inputs) {
        return runQuery(ClojureBridge.queryForm(query), inputs);
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
        return runExplain(ClojureBridge.explainOpts(null),
                          ClojureBridge.queryForm(query),
                          Arrays.asList(inputs));
    }

    /**
     * Explains a query expressed as EDN text with positional inputs.
     */
    public Object explain(String query, List<?> inputs) {
        return runExplain(ClojureBridge.explainOpts(null),
                          ClojureBridge.queryForm(query),
                          inputs);
    }

    /**
     * Explains a query expressed as EDN text using explicit explain options.
     */
    public Object explain(String optsEdn, String query, List<?> inputs) {
        return runExplain(ClojureBridge.explainOpts(optsEdn),
                          ClojureBridge.queryForm(query),
                          inputs);
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
        return ClojureBridge.javaMap(
                ClojureBridge.core("transact!", resource(), ClojureBridge.txDataInput(txData))
        );
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
        if (txMeta == null) {
            return transact(txData);
        }
        return ClojureBridge.javaMap(
                ClojureBridge.core("transact!",
                                   resource(),
                                   ClojureBridge.txDataInput(txData),
                                   ClojureBridge.genericInput(txMeta))
        );
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
        return execJson(op, args);
    }

    private Object db() {
        return ClojureBridge.core("db", resource());
    }

    private Object runQuery(Object queryForm, List<?> inputs) {
        ArrayList<Object> args = new ArrayList<>();
        args.add(queryForm);
        args.add(db());
        if (inputs != null) {
            for (Object input : inputs) {
                args.add(ClojureBridge.genericInput(input));
            }
        }
        return ClojureBridge.toJava(
                ClojureBridge.core("q", args.toArray())
        );
    }

    private Object runExplain(Object opts, Object queryForm, List<?> inputs) {
        ArrayList<Object> args = new ArrayList<>();
        args.add(opts);
        args.add(queryForm);
        args.add(db());
        if (inputs != null) {
            for (Object input : inputs) {
                args.add(ClojureBridge.genericInput(input));
            }
        }
        return ClojureBridge.toJava(
                ClojureBridge.core("explain", args.toArray())
        );
    }
}
