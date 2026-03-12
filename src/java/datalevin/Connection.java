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

    private volatile Object cachedDb;

    Connection(Object conn) {
        super(conn,
              resource -> ClojureRuntime.core("close", resource),
              "conn",
              "conn");
    }

    /**
     * Returns whether this handle has been closed.
     */
    public boolean closed() {
        return isReleased() || ClojureCodec.javaBoolean(ClojureRuntime.core("closed?", resource()));
    }

    /**
     * Returns the current schema map.
     */
    public Map<?, ?> schema() {
        return (Map<?, ?>) ClojureRuntime.core("schema", resource());
    }

    /**
     * Applies a raw schema update and returns the updated schema.
     */
    public Map<?, ?> updateSchema(Map<?, ?> schemaUpdate) {
        invalidateDb();
        return (Map<?, ?>) ClojureRuntime.core("update-schema",
                                               resource(),
                                               DatalevinForms.schemaInput(schemaUpdate));
    }

    /**
     * Applies a typed schema update and returns the updated schema.
     */
    public Map<?, ?> updateSchema(Schema schemaUpdate) {
        invalidateDb();
        return (Map<?, ?>) ClojureRuntime.core("update-schema",
                                               resource(),
                                               schemaUpdate == null ? null : schemaUpdate.buildForm());
    }

    /**
     * Applies a raw schema update with attribute deletion and rename options.
     */
    public Map<?, ?> updateSchema(Map<?, ?> schemaUpdate,
                                  Collection<?> delAttrs,
                                  Map<?, ?> renameMap) {
        Object normalizedSchema = DatalevinForms.schemaInput(schemaUpdate);
        Object normalizedDelAttrs = DatalevinForms.deleteAttrsInput(delAttrs);
        Object normalizedRenameMap = DatalevinForms.renameMapInput(renameMap);
        invalidateDb();
        if (renameMap != null) {
            return (Map<?, ?>) ClojureRuntime.core("update-schema",
                                                  resource(),
                                                  normalizedSchema,
                                                  normalizedDelAttrs,
                                                  normalizedRenameMap);
        }
        if (delAttrs != null) {
            return (Map<?, ?>) ClojureRuntime.core("update-schema",
                                                  resource(),
                                                  normalizedSchema,
                                                  normalizedDelAttrs);
        }
        return (Map<?, ?>) ClojureRuntime.core("update-schema", resource(), normalizedSchema);
    }

    /**
     * Applies a typed schema update with attribute deletion and rename options.
     */
    public Map<?, ?> updateSchema(Schema schemaUpdate,
                                  Collection<?> delAttrs,
                                  Map<?, ?> renameMap) {
        return updateSchema(schemaUpdate == null ? null : (Map<?, ?>) schemaUpdate.buildForm(),
                            delAttrs,
                            renameMap);
    }

    /**
     * Returns the connection option map.
     */
    public Map<?, ?> opts() {
        return (Map<?, ?>) ClojureRuntime.core("opts", resource());
    }

    /**
     * Clears all data from the underlying database.
     */
    public void clear() {
        invalidateDb();
        ClojureRuntime.core("clear", resource());
    }

    /**
     * Returns the highest entity id currently allocated.
     */
    public long maxEid() {
        return ClojureCodec.javaLong(ClojureRuntime.core("max-eid", db()));
    }

    /**
     * Returns the current datalog index cache limit.
     */
    public long datalogIndexCacheLimit() {
        return ClojureCodec.javaLong(ClojureRuntime.core("datalog-index-cache-limit", db()));
    }

    /**
     * Sets and returns the datalog index cache limit.
     */
    public long datalogIndexCacheLimit(long limit) {
        ClojureRuntime.core("datalog-index-cache-limit", db(), limit);
        invalidateDb();
        return datalogIndexCacheLimit();
    }

    /**
     * Resolves an entity id or lookup ref to an entity id.
     */
    public Object entid(Object eid) {
        return ClojureRuntime.core("entid", db(), DatalevinForms.lookupRefInput(eid));
    }

    /**
     * Returns a touched entity map for the given entity id or lookup ref.
     */
    public Object entity(Object eid) {
        Object entity = ClojureRuntime.core("entity", db(), DatalevinForms.lookupRefInput(eid));
        if (entity == null) {
            return null;
        }
        return ClojureRuntime.core("touch", entity);
    }

    /**
     * Pulls one entity using a raw selector value.
     */
    public Map<?, ?> pull(Object selector, Object eid) {
        return (Map<?, ?>) ClojureRuntime.core("pull",
                                              db(),
                                              DatalevinForms.pullSelectorInput(selector),
                                              DatalevinForms.lookupRefInput(eid));
    }

    /**
     * Pulls one entity using a typed selector builder.
     */
    public Map<?, ?> pull(PullSelector selector, Object eid) {
        return pull((Object) selector, eid);
    }

    /**
     * Pulls many entities using a raw selector value.
     */
    public List<?> pullMany(Object selector, List<?> eids) {
        return (List<?>) ClojureRuntime.core("pull-many",
                                            db(),
                                            DatalevinForms.pullSelectorInput(selector),
                                            DatalevinForms.entityIdsInput(eids));
    }

    /**
     * Pulls many entities using a typed selector builder.
     */
    public List<?> pullMany(PullSelector selector, List<?> eids) {
        return pullMany((Object) selector, eids);
    }

    /**
     * Runs a query expressed as EDN text with positional inputs.
     */
    public Object query(String query, Object... inputs) {
        return runQuery(DatalevinForms.queryForm(query), Arrays.asList(inputs));
    }

    /**
     * Runs a query expressed as EDN text with positional inputs.
     */
    public Object query(String query, List<?> inputs) {
        return runQuery(DatalevinForms.queryForm(query), inputs);
    }

    /**
     * Runs a query expressed as a raw EDN-like form with positional inputs.
     */
    public Object queryForm(Object queryForm, List<?> inputs) {
        return runQuery(DatalevinForms.queryFormInput(queryForm), inputs);
    }

    /**
     * Runs a typed query with no explicit extra inputs.
     */
    public Object query(DatalogQuery query) {
        Objects.requireNonNull(query, "query");
        return runQuery(query.buildForm(), query.prepareInputs((List<?>) null), query.requiresDb());
    }

    /**
     * Runs a typed query with positional inputs.
     */
    public Object query(DatalogQuery query, Object... inputs) {
        Objects.requireNonNull(query, "query");
        return runQuery(query.buildForm(), query.prepareInputs(inputs), query.requiresDb());
    }

    /**
     * Runs a typed query with positional inputs.
     */
    public Object query(DatalogQuery query, List<?> inputs) {
        Objects.requireNonNull(query, "query");
        return runQuery(query.buildForm(), query.prepareInputs(inputs), query.requiresDb());
    }

    /**
     * Runs a typed scalar query and coerces the result to {@code type}.
     */
    public <T> T queryScalar(DatalogQuery query, Class<T> type) {
        return queryScalar(query, type, (List<?>) null);
    }

    /**
     * Runs a typed scalar query with positional inputs and coerces the result to
     * {@code type}.
     */
    public <T> T queryScalar(DatalogQuery query, Class<T> type, Object... inputs) {
        return queryScalar(query, type, Arrays.asList(inputs));
    }

    /**
     * Runs a typed scalar query with positional inputs and coerces the result to
     * {@code type}.
     */
    public <T> T queryScalar(DatalogQuery query, Class<T> type, List<?> inputs) {
        requireShape(query, DatalogQuery.ResultShape.SCALAR, "queryScalar");
        return ResultSupport.coerce(runQuery(query.buildForm(), query.prepareInputs(inputs), query.requiresDb()), type);
    }

    /**
     * Runs a typed collection query and coerces each value to {@code type}.
     */
    public <T> List<T> queryCollection(DatalogQuery query, Class<T> type) {
        return queryCollection(query, type, (List<?>) null);
    }

    /**
     * Runs a typed collection query with positional inputs and coerces each
     * value to {@code type}.
     */
    public <T> List<T> queryCollection(DatalogQuery query, Class<T> type, Object... inputs) {
        return queryCollection(query, type, Arrays.asList(inputs));
    }

    /**
     * Runs a typed collection query with positional inputs and coerces each
     * value to {@code type}.
     */
    public <T> List<T> queryCollection(DatalogQuery query, Class<T> type, List<?> inputs) {
        requireShape(query, DatalogQuery.ResultShape.COLLECTION, "queryCollection");
        return ResultSupport.typedSequence(runQuery(query.buildForm(),
                                                    query.prepareInputs(inputs),
                                                    query.requiresDb()),
                                          type);
    }

    /**
     * Runs a typed tuple query.
     */
    public List<?> queryTuple(DatalogQuery query) {
        return queryTuple(query, (List<?>) null);
    }

    /**
     * Runs a typed tuple query with positional inputs.
     */
    public List<?> queryTuple(DatalogQuery query, Object... inputs) {
        return queryTuple(query, Arrays.asList(inputs));
    }

    /**
     * Runs a typed tuple query with positional inputs.
     */
    public List<?> queryTuple(DatalogQuery query, List<?> inputs) {
        requireShape(query, DatalogQuery.ResultShape.TUPLE, "queryTuple");
        return ResultSupport.sequence(runQuery(query.buildForm(), query.prepareInputs(inputs), query.requiresDb()));
    }

    /**
     * Runs a typed relation query.
     */
    public List<?> queryRelation(DatalogQuery query) {
        return queryRelation(query, (List<?>) null);
    }

    /**
     * Runs a typed relation query with positional inputs.
     */
    public List<?> queryRelation(DatalogQuery query, Object... inputs) {
        return queryRelation(query, Arrays.asList(inputs));
    }

    /**
     * Runs a typed relation query with positional inputs.
     */
    public List<?> queryRelation(DatalogQuery query, List<?> inputs) {
        requireShape(query, DatalogQuery.ResultShape.RELATION, "queryRelation");
        return ResultSupport.sequence(runQuery(query.buildForm(), query.prepareInputs(inputs), query.requiresDb()));
    }

    /**
     * Runs a keyed query using {@code :keys}, {@code :strs}, or {@code :syms}.
     */
    public List<?> queryKeyed(DatalogQuery query) {
        return queryKeyed(query, (List<?>) null);
    }

    /**
     * Runs a keyed query with positional inputs.
     */
    public List<?> queryKeyed(DatalogQuery query, Object... inputs) {
        return queryKeyed(query, Arrays.asList(inputs));
    }

    /**
     * Runs a keyed query with positional inputs.
     */
    public List<?> queryKeyed(DatalogQuery query, List<?> inputs) {
        requireShape(query, DatalogQuery.ResultShape.KEYED, "queryKeyed");
        return ResultSupport.sequence(runQuery(query.buildForm(),
                                              query.prepareInputs(inputs),
                                              query.requiresDb()));
    }

    /**
     * Explains a query expressed as EDN text with positional inputs.
     */
    public Object explain(String query, Object... inputs) {
        return runExplain(DatalevinForms.explainOpts(null),
                          DatalevinForms.queryForm(query),
                          Arrays.asList(inputs));
    }

    /**
     * Explains a query expressed as EDN text with positional inputs.
     */
    public Object explain(String query, List<?> inputs) {
        return runExplain(DatalevinForms.explainOpts(null),
                          DatalevinForms.queryForm(query),
                          inputs);
    }

    /**
     * Explains a query expressed as a raw EDN-like form with positional inputs.
     */
    public Object explainForm(Object queryForm, List<?> inputs) {
        return runExplain(DatalevinForms.explainOpts(null),
                          DatalevinForms.queryFormInput(queryForm),
                          inputs);
    }

    /**
     * Explains a query expressed as EDN text using explicit explain options.
     */
    public Object explain(String optsEdn, String query, List<?> inputs) {
        return runExplain(DatalevinForms.explainOpts(optsEdn),
                          DatalevinForms.queryForm(query),
                          inputs);
    }

    /**
     * Explains a raw EDN-like query using explicit explain options.
     */
    public Object explainForm(String optsEdn, Object queryForm, List<?> inputs) {
        return runExplain(DatalevinForms.explainOpts(optsEdn),
                          DatalevinForms.queryFormInput(queryForm),
                          inputs);
    }

    /**
     * Explains a typed query with no explicit extra inputs.
     */
    public Object explain(DatalogQuery query) {
        Objects.requireNonNull(query, "query");
        return runExplain(DatalevinForms.explainOpts(null),
                          query.buildForm(),
                          query.prepareInputs((List<?>) null),
                          query.requiresDb());
    }

    /**
     * Explains a typed query with positional inputs.
     */
    public Object explain(DatalogQuery query, Object... inputs) {
        Objects.requireNonNull(query, "query");
        return runExplain(DatalevinForms.explainOpts(null),
                          query.buildForm(),
                          query.prepareInputs(inputs),
                          query.requiresDb());
    }

    /**
     * Explains a typed query with positional inputs.
     */
    public Object explain(DatalogQuery query, List<?> inputs) {
        Objects.requireNonNull(query, "query");
        return runExplain(DatalevinForms.explainOpts(null),
                          query.buildForm(),
                          query.prepareInputs(inputs),
                          query.requiresDb());
    }

    /**
     * Explains a typed query using explicit explain options.
     */
    public Object explain(String optsEdn, DatalogQuery query, List<?> inputs) {
        Objects.requireNonNull(query, "query");
        return runExplain(DatalevinForms.explainOpts(optsEdn),
                          query.buildForm(),
                          query.prepareInputs(inputs),
                          query.requiresDb());
    }

    /**
     * Transacts raw transaction data and returns the transaction report.
     */
    public Map<?, ?> transact(Object txData) {
        invalidateDb();
        return (Map<?, ?>) ClojureRuntime.core("transact!",
                                               resource(),
                                               DatalevinForms.txDataInput(txData));
    }

    /**
     * Transacts typed transaction data and returns the transaction report.
     */
    public Map<?, ?> transact(TxData txData) {
        invalidateDb();
        return (Map<?, ?>) ClojureRuntime.core("transact!",
                                               resource(),
                                               txData == null ? null : txData.buildForm());
    }

    /**
     * Transacts raw transaction data with optional transaction metadata.
     */
    public Map<?, ?> transact(Object txData, Map<?, ?> txMeta) {
        if (txMeta == null) {
            return transact(txData);
        }
        invalidateDb();
        return (Map<?, ?>) ClojureRuntime.core("transact!",
                                              resource(),
                                              DatalevinForms.txDataInput(txData),
                                              ClojureCodec.runtimeInput(txMeta));
    }

    /**
     * Transacts typed transaction data with optional transaction metadata.
     */
    public Map<?, ?> transact(TxData txData, Map<?, ?> txMeta) {
        if (txMeta == null) {
            return transact(txData);
        }
        invalidateDb();
        return (Map<?, ?>) ClojureRuntime.core("transact!",
                                              resource(),
                                              txData == null ? null : txData.buildForm(),
                                              ClojureCodec.runtimeInput(txMeta));
    }

    /**
     * Escape hatch for calling a connection-scoped JSON API operation directly.
     */
    public Object exec(String op, Map<String, ?> args) {
        return execJson(op, args);
    }

    private Object db() {
        Object db = cachedDb;
        if (db == null) {
            db = ClojureRuntime.core("db", resource());
            cachedDb = db;
        }
        return db;
    }

    private Object runQuery(Object queryForm, List<?> inputs) {
        return runQuery(queryForm, inputs, true);
    }

    private Object runQuery(Object queryForm, List<?> inputs, boolean includeDb) {
        int inputCount = inputs == null ? 0 : inputs.size();
        if (inputCount == 0) {
            return includeDb ? ClojureRuntime.core("q", queryForm, db())
                    : ClojureRuntime.core("q", queryForm);
        }
        if (inputCount == 1) {
            Object input = ClojureCodec.runtimeInput(inputs.get(0));
            return includeDb ? ClojureRuntime.core("q", queryForm, db(), input)
                    : ClojureRuntime.core("q", queryForm, input);
        }

        int base = includeDb ? 2 : 1;
        Object[] args = new Object[base + inputCount];
        args[0] = queryForm;
        if (includeDb) {
            args[1] = db();
        }
        for (int i = 0; i < inputCount; i++) {
            args[base + i] = ClojureCodec.runtimeInput(inputs.get(i));
        }
        return ClojureRuntime.core("q", args);
    }

    private Object runExplain(Object opts, Object queryForm, List<?> inputs) {
        return runExplain(opts, queryForm, inputs, true);
    }

    private Object runExplain(Object opts, Object queryForm, List<?> inputs, boolean includeDb) {
        int inputCount = inputs == null ? 0 : inputs.size();
        if (inputCount == 0) {
            return includeDb ? ClojureRuntime.core("explain", opts, queryForm, db())
                    : ClojureRuntime.core("explain", opts, queryForm);
        }
        if (inputCount == 1) {
            Object input = ClojureCodec.runtimeInput(inputs.get(0));
            return includeDb ? ClojureRuntime.core("explain", opts, queryForm, db(), input)
                    : ClojureRuntime.core("explain", opts, queryForm, input);
        }

        int base = includeDb ? 3 : 2;
        Object[] args = new Object[base + inputCount];
        args[0] = opts;
        args[1] = queryForm;
        if (includeDb) {
            args[2] = db();
        }
        for (int i = 0; i < inputCount; i++) {
            args[base + i] = ClojureCodec.runtimeInput(inputs.get(i));
        }
        return ClojureRuntime.core("explain", args);
    }

    private static void requireShape(DatalogQuery query,
                                     DatalogQuery.ResultShape expected,
                                     String method) {
        Objects.requireNonNull(query, "query");
        if (query.resultShape() != expected) {
            throw new IllegalArgumentException(method + " requires a "
                    + expected.name().toLowerCase() + " query, got "
                    + query.resultShape().name().toLowerCase() + ".");
        }
    }

    private void invalidateDb() {
        cachedDb = null;
    }
}
