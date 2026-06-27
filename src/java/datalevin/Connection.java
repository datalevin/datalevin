package datalevin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Handle for a Datalog connection.
 *
 * <p>Use instances with try-with-resources when you own the handle lifecycle.
 * Query and transaction methods accept either raw EDN-like values or the typed
 * builders in this package.
 */
public final class Connection extends HandleResource {
    Connection(Object conn) {
        this(conn, true);
    }

    Connection(Object conn, boolean owned) {
        super(conn,
              owned ? resource -> ClojureRuntime.core("close", resource)
                    : resource -> {
                    },
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
        return (Map<?, ?>) ClojureRuntime.core("update-schema",
                                               resource(),
                                               DatalevinForms.schemaInput(schemaUpdate));
    }

    /**
     * Applies a typed schema update and returns the updated schema.
     */
    public Map<?, ?> updateSchema(Schema schemaUpdate) {
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
        ClojureRuntime.core("clear", resource());
    }

    /**
     * Runs {@code fn} inside a single Datalevin write transaction.
     */
    @SuppressWarnings("unchecked")
    public <T> T withTransaction(Function<Connection, T> fn) {
        Objects.requireNonNull(fn, "fn");
        Function<Object, Object> wrapped = rawConn -> fn.apply(new Connection(rawConn, false));
        return (T) ClojureRuntime.core("with-transaction-fn", resource(), wrapped);
    }

    /**
     * Runs {@code fn} inside a single Datalevin write transaction with a
     * timeout in milliseconds.
     */
    @SuppressWarnings("unchecked")
    public <T> T withTransaction(long timeoutMs, Function<Connection, T> fn) {
        return withTransaction(Long.valueOf(timeoutMs), fn);
    }

    /**
     * Runs {@code fn} inside a single Datalevin write transaction with an
     * optional timeout in milliseconds. {@code null} disables the default
     * timeout for this call.
     */
    @SuppressWarnings("unchecked")
    public <T> T withTransaction(Long timeoutMs, Function<Connection, T> fn) {
        Objects.requireNonNull(fn, "fn");
        Function<Object, Object> wrapped = rawConn -> fn.apply(new Connection(rawConn, false));
        return (T) ClojureRuntime.core("with-transaction-fn",
                                       resource(),
                                       timeoutMs,
                                       wrapped);
    }

    /**
     * Rebuilds this Datalog database index and returns this handle.
     */
    public Connection reIndex() {
        return reIndex((Map<?, ?>) null);
    }

    /**
     * Rebuilds this Datalog database index with options and returns this handle.
     */
    public Connection reIndex(Map<?, ?> opts) {
        Object next = ClojureRuntime.core("re-index",
                                          resource(),
                                          DatalevinForms.optionsInput(opts == null ? Map.of() : opts));
        replaceResource(next);
        return this;
    }

    /**
     * Rebuilds this Datalog database index with a raw schema and options.
     */
    public Connection reIndex(Map<?, ?> schema, Map<?, ?> opts) {
        Object next = ClojureRuntime.core("re-index",
                                          resource(),
                                          DatalevinForms.schemaInput(schema),
                                          DatalevinForms.optionsInput(opts == null ? Map.of() : opts));
        replaceResource(next);
        return this;
    }

    /**
     * Rebuilds this Datalog database index with a typed schema and options.
     */
    public Connection reIndex(Schema schema, Map<?, ?> opts) {
        Object next = ClojureRuntime.core("re-index",
                                          resource(),
                                          schema == null ? null : schema.buildForm(),
                                          DatalevinForms.optionsInput(opts == null ? Map.of() : opts));
        replaceResource(next);
        return this;
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
     * Returns a bridge-safe touched entity map for the given entity id or lookup
     * ref.
     */
    public Map<?, ?> entityMap(Object eid) {
        Object entity = ClojureRuntime.core("entity", db(), DatalevinForms.lookupRefInput(eid));
        if (entity == null) {
            return null;
        }
        return (Map<?, ?>) ClojureCodec.bridgeOutput(ClojureRuntime.core("touch", entity));
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
        return (Map<?, ?>) ClojureRuntime.core("transact!",
                                               resource(),
                                               DatalevinForms.txDataInput(txData));
    }

    /**
     * Transacts typed transaction data and returns the transaction report.
     */
    public Map<?, ?> transact(TxData txData) {
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
        return (Map<?, ?>) ClojureRuntime.core("transact!",
                                              resource(),
                                              txData == null ? null : txData.buildForm(),
                                              ClojureCodec.runtimeInput(txMeta));
    }

    /**
     * Applies raw transaction data against this connection's current database
     * value without committing and returns the simulated transaction report.
     */
    public Map<?, ?> txDataToSimulatedReport(Object txData) {
        return (Map<?, ?>) ClojureRuntime.core("tx-data->simulated-report",
                                               db(),
                                               DatalevinForms.txDataInput(txData));
    }

    /**
     * Applies typed transaction data against this connection's current database
     * value without committing and returns the simulated transaction report.
     */
    public Map<?, ?> txDataToSimulatedReport(TxData txData) {
        return (Map<?, ?>) ClojureRuntime.core("tx-data->simulated-report",
                                               db(),
                                               txData == null ? null : txData.buildForm());
    }

    /**
     * Transacts raw transaction data asynchronously.
     *
     * <p>The returned {@link CompletableFuture} completes with the transaction
     * report when the underlying Datalevin async transaction commits.
     */
    public CompletableFuture<Map<?, ?>> transactAsync(Object txData) {
        return transactAsync(txData, null);
    }

    /**
     * Transacts typed transaction data asynchronously.
     */
    public CompletableFuture<Map<?, ?>> transactAsync(TxData txData) {
        return transactAsync(txData, null);
    }

    /**
     * Transacts raw transaction data asynchronously with optional transaction
     * metadata.
     */
    public CompletableFuture<Map<?, ?>> transactAsync(Object txData, Map<?, ?> txMeta) {
        Object future = txMeta == null
                ? ClojureRuntime.core("transact-async",
                                      resource(),
                                      DatalevinForms.txDataInput(txData))
                : ClojureRuntime.core("transact-async",
                                      resource(),
                                      DatalevinForms.txDataInput(txData),
                                      ClojureCodec.runtimeInput(txMeta));
        return txReportFuture(future);
    }

    /**
     * Transacts typed transaction data asynchronously with optional transaction
     * metadata.
     */
    public CompletableFuture<Map<?, ?>> transactAsync(TxData txData, Map<?, ?> txMeta) {
        Object future = txMeta == null
                ? ClojureRuntime.core("transact-async",
                                      resource(),
                                      txData == null ? null : txData.buildForm())
                : ClojureRuntime.core("transact-async",
                                      resource(),
                                      txData == null ? null : txData.buildForm(),
                                      ClojureCodec.runtimeInput(txMeta));
        return txReportFuture(future);
    }

    /**
     * Registers a transaction listener with an auto-generated key.
     *
     * <p>The listener receives a bridge-safe transaction report whenever
     * {@code transact} applies a transaction to this connection.
     */
    public Object listen(Consumer<Map<?, ?>> listener) {
        Objects.requireNonNull(listener, "listener");
        Consumer<Object> wrapped = report -> listener.accept((Map<?, ?>) report);
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("listen!",
                                                             resource(),
                                                             ClojureFns.txReportConsumer(wrapped)));
    }

    /**
     * Registers or replaces a transaction listener under {@code key}.
     */
    public Object listen(Object key, Consumer<Map<?, ?>> listener) {
        Objects.requireNonNull(listener, "listener");
        Consumer<Object> wrapped = report -> listener.accept((Map<?, ?>) report);
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("listen!",
                                                             resource(),
                                                             ClojureCodec.runtimeInput(key),
                                                             ClojureFns.txReportConsumer(wrapped)));
    }

    /**
     * Removes the transaction listener registered under {@code key}.
     */
    public void unlisten(Object key) {
        ClojureRuntime.core("unlisten!", resource(), ClojureCodec.runtimeInput(key));
    }

    /**
     * Bulk-loads Datom values into this connection and returns this handle.
     *
     * <p>Datoms may be raw Datalevin Datom objects, 3/4/5-element collections
     * in {@code [e, attr, value, tx?, added?]} shape, or maps with
     * {@code :e}, {@code :a}, and {@code :v} keys.
     */
    public Connection fillDb(Object datoms) {
        Object newDb = ClojureRuntime.core("fill-db", db(), DatalevinForms.datomsInput(datoms));
        ClojureRuntime.core("reset-conn!", resource(), newDb);
        return this;
    }

    /**
     * Returns the KV handle backing this Datalog connection.
     *
     * <p>The returned handle is borrowed from the connection. Closing it does
     * not close the underlying store; close this connection instead.
     */
    public KV datalogKV() {
        return new KV(ClojureRuntime.core("datalog-kv", resource()), false);
    }

    /**
     * Returns datoms from {@code index}, ordered by index order.
     */
    public List<?> datoms(Object index) {
        return datoms(index, null, null, null);
    }

    /**
     * Returns datoms from {@code index} matching the first index component.
     */
    public List<?> datoms(Object index, Object c1) {
        return datoms(index, c1, null, null);
    }

    /**
     * Returns datoms from {@code index} matching the first two index components.
     */
    public List<?> datoms(Object index, Object c1, Object c2) {
        return datoms(index, c1, c2, null);
    }

    /**
     * Returns datoms from {@code index} matching the supplied index components.
     */
    public List<?> datoms(Object index, Object c1, Object c2, Object c3) {
        return datomIndexRead("datoms", index, c1, c2, c3, null);
    }

    /**
     * Returns up to {@code n} datoms from {@code index} matching the supplied
     * index components.
     */
    public List<?> datoms(Object index, Object c1, Object c2, Object c3, long n) {
        return datomIndexRead("datoms", index, c1, c2, c3, n);
    }

    /**
     * Returns datoms matching the entity, attribute, and value pattern. A
     * {@code null} component is a wildcard.
     */
    public List<?> searchDatoms(Object e, Object attr, Object value) {
        return ResultSupport.sequence(ClojureRuntime.core("search-datoms",
                                                          db(),
                                                          ClojureCodec.runtimeInput(e),
                                                          DatalevinForms.datalogAttrInput(attr),
                                                          ClojureCodec.runtimeInput(value)));
    }

    /**
     * Counts datoms matching the entity, attribute, and value pattern. A
     * {@code null} component is a wildcard.
     */
    public long countDatoms(Object e, Object attr, Object value) {
        return ClojureCodec.javaLong(ClojureRuntime.core("count-datoms",
                                                         db(),
                                                         ClojureCodec.runtimeInput(e),
                                                         DatalevinForms.datalogAttrInput(attr),
                                                         ClojureCodec.runtimeInput(value)));
    }

    /**
     * Seeks forward in {@code index} from the supplied components.
     */
    public List<?> seekDatoms(Object index) {
        return seekDatoms(index, null, null, null);
    }

    /**
     * Seeks forward in {@code index} from the supplied components.
     */
    public List<?> seekDatoms(Object index, Object c1) {
        return seekDatoms(index, c1, null, null);
    }

    /**
     * Seeks forward in {@code index} from the supplied components.
     */
    public List<?> seekDatoms(Object index, Object c1, Object c2) {
        return seekDatoms(index, c1, c2, null);
    }

    /**
     * Seeks forward in {@code index} from the supplied components.
     */
    public List<?> seekDatoms(Object index, Object c1, Object c2, Object c3) {
        return datomIndexRead("seek-datoms", index, c1, c2, c3, null);
    }

    /**
     * Seeks forward in {@code index}, returning up to {@code n} datoms.
     */
    public List<?> seekDatoms(Object index, Object c1, Object c2, Object c3, long n) {
        return datomIndexRead("seek-datoms", index, c1, c2, c3, n);
    }

    /**
     * Seeks backward in {@code index} from the supplied components.
     */
    public List<?> rseekDatoms(Object index) {
        return rseekDatoms(index, null, null, null);
    }

    /**
     * Seeks backward in {@code index} from the supplied components.
     */
    public List<?> rseekDatoms(Object index, Object c1) {
        return rseekDatoms(index, c1, null, null);
    }

    /**
     * Seeks backward in {@code index} from the supplied components.
     */
    public List<?> rseekDatoms(Object index, Object c1, Object c2) {
        return rseekDatoms(index, c1, c2, null);
    }

    /**
     * Seeks backward in {@code index} from the supplied components.
     */
    public List<?> rseekDatoms(Object index, Object c1, Object c2, Object c3) {
        return datomIndexRead("rseek-datoms", index, c1, c2, c3, null);
    }

    /**
     * Seeks backward in {@code index}, returning up to {@code n} datoms.
     */
    public List<?> rseekDatoms(Object index, Object c1, Object c2, Object c3, long n) {
        return datomIndexRead("rseek-datoms", index, c1, c2, c3, n);
    }

    /**
     * Returns datoms in AVE order for {@code attr} values in the inclusive
     * {@code [start, end]} range.
     */
    public List<?> indexRange(Object attr, Object start, Object end) {
        return ResultSupport.sequence(ClojureRuntime.core("index-range",
                                                          db(),
                                                          DatalevinForms.datalogAttrInput(attr),
                                                          ClojureCodec.runtimeInput(start),
                                                          ClojureCodec.runtimeInput(end)));
    }

    /**
     * Returns datoms found by the fulltext query.
     */
    public List<?> fulltextDatoms(String query) {
        return ResultSupport.sequence(ClojureRuntime.core("fulltext-datoms", db(), query));
    }

    /**
     * Returns datoms found by the fulltext query using Datalevin fulltext
     * options.
     */
    public List<?> fulltextDatoms(String query, Map<?, ?> opts) {
        if (opts == null) {
            return fulltextDatoms(query);
        }
        return ResultSupport.sequence(ClojureRuntime.core("fulltext-datoms",
                                                          db(),
                                                          query,
                                                          DatalevinForms.optionsInput(opts)));
    }

    /**
     * Returns datoms found by the fulltext query using typed Datalevin fulltext
     * options.
     */
    public List<?> fulltextDatoms(String query, RetrievalOptions opts) {
        if (opts == null) {
            return fulltextDatoms(query);
        }
        return ResultSupport.sequence(ClojureRuntime.core("fulltext-datoms",
                                                          db(),
                                                          query,
                                                          opts.buildForm()));
    }

    /**
     * Copies the Datalog database to {@code dest}.
     */
    public void copy(String dest) {
        ClojureRuntime.core("copy", db(), dest);
    }

    /**
     * Copies the Datalog database to {@code dest}, optionally compacting pages.
     */
    public void copy(String dest, boolean compact) {
        ClojureRuntime.core("copy", db(), dest, compact);
    }

    /**
     * Returns WAL watermarks for this connection's backing store.
     */
    public Map<?, ?> txLogWatermarks() {
        return (Map<?, ?>) ClojureRuntime.core("txlog-watermarks", store());
    }

    /**
     * Reads committed WAL records from {@code fromLsn}, inclusive.
     */
    public List<?> openTxLog(long fromLsn) {
        return ResultSupport.sequence(ClojureRuntime.core("open-tx-log", store(), fromLsn));
    }

    /**
     * Reads committed WAL records in the inclusive LSN range.
     */
    public List<?> openTxLog(long fromLsn, long uptoLsn) {
        return ResultSupport.sequence(ClojureRuntime.core("open-tx-log", store(), fromLsn, uptoLsn));
    }

    /**
     * Creates or rotates the LMDB snapshot for this connection's backing store.
     */
    public Map<?, ?> createSnapshot() {
        return (Map<?, ?>) ClojureRuntime.core("create-snapshot!", store());
    }

    /**
     * Lists available LMDB snapshots for this connection's backing store.
     */
    public List<?> listSnapshots() {
        return ResultSupport.sequence(ClojureRuntime.core("list-snapshots", store()));
    }

    /**
     * Runs WAL segment GC for this connection's backing store.
     */
    public Map<?, ?> gcTxLogSegments() {
        return (Map<?, ?>) ClojureRuntime.core("gc-txlog-segments!", store());
    }

    /**
     * Runs WAL segment GC while retaining records from {@code retainFloorLsn}.
     */
    public Map<?, ?> gcTxLogSegments(long retainFloorLsn) {
        return (Map<?, ?>) ClojureRuntime.core("gc-txlog-segments!", store(), retainFloorLsn);
    }

    /**
     * Escape hatch for calling a connection-scoped JSON API operation directly.
     */
    public Object exec(String op, Map<String, ?> args) {
        return execJson(op, args);
    }

    private Object db() {
        return ClojureRuntime.core("db", resource());
    }

    private static Object queryInput(Object input) {
        if (input instanceof Connection conn) {
            return conn.db();
        }
        return ClojureCodec.runtimeInput(input);
    }

    private Object store() {
        return ClojureRuntime.invoke("clojure.core",
                                     "get",
                                     db(),
                                     ClojureCodec.keyword(":store"));
    }

    private List<?> datomIndexRead(String function,
                                   Object index,
                                   Object c1,
                                   Object c2,
                                   Object c3,
                                   Long limit) {
        Object normalizedIndex = DatalevinForms.datalogIndexInput(index);
        Object nc1 = DatalevinForms.datalogIndexComponentInput(normalizedIndex, 0, c1);
        Object nc2 = DatalevinForms.datalogIndexComponentInput(normalizedIndex, 1, c2);
        Object nc3 = DatalevinForms.datalogIndexComponentInput(normalizedIndex, 2, c3);
        if (limit == null) {
            return ResultSupport.sequence(ClojureRuntime.core(function, db(), normalizedIndex, nc1, nc2, nc3));
        }
        return ResultSupport.sequence(ClojureRuntime.core(function, db(), normalizedIndex, nc1, nc2, nc3, limit));
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
            Object input = queryInput(inputs.get(0));
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
            args[base + i] = queryInput(inputs.get(i));
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
            Object input = queryInput(inputs.get(0));
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
            args[base + i] = queryInput(inputs.get(i));
        }
        return ClojureRuntime.core("explain", args);
    }

    private CompletableFuture<Map<?, ?>> txReportFuture(Object future) {
        CompletableFuture<Map<?, ?>> result = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                result.complete((Map<?, ?>) ClojureRuntime.deref(future));
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        });
        return result;
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

}
