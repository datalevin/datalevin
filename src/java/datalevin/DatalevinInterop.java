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
import java.util.function.Function;
import java.util.function.Consumer;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Small bridge-oriented interop layer for non-Java bindings.
 *
 * <p>This surface favors plain JDK collections, opaque raw handles, and
 * unambiguous method names over Java-specific ergonomics. It is intended as a
 * foundation for higher-level Python or JavaScript bindings that talk to the
 * JVM through a bridge such as JPype or node-java-bridge.
 */
public final class DatalevinInterop {

    private DatalevinInterop() {
    }

    /**
     * Invokes a Datalevin core function and returns the raw Clojure result.
     */
    public static Object coreInvoke(String function, List<?> args) {
        return ClojureRuntime.core(function, normalizeArgs(args));
    }

    /**
     * Invokes a Datalevin core function and normalizes the result for bridge
     * runtimes.
     */
    public static Object coreInvokeBridge(String function, List<?> args) {
        return ClojureCodec.bridgeOutput(coreInvoke(function, args));
    }

    /**
     * Invokes a Datalevin client function and returns the raw Clojure result.
     */
    public static Object clientInvoke(String function, List<?> args) {
        return ClojureRuntime.client(function, normalizeArgs(args));
    }

    /**
     * Invokes a Datalevin client function and normalizes the result for bridge
     * runtimes.
     */
    public static Object clientInvokeBridge(String function, List<?> args) {
        return ClojureCodec.bridgeOutput(clientInvoke(function, args));
    }

    /**
     * Creates or opens a raw connection handle.
     */
    public static Object createConnection(String dir,
                                          Map<?, ?> schema,
                                          Map<?, ?> opts) {
        if (dir == null) {
            return ClojureRuntime.core("create-conn");
        }
        if (opts != null) {
            return ClojureRuntime.core("create-conn",
                                       dir,
                                       DatalevinForms.schemaInput(schema),
                                       DatalevinForms.optionsInput(opts));
        }
        if (schema != null) {
            return ClojureRuntime.core("create-conn",
                                       dir,
                                       DatalevinForms.schemaInput(schema));
        }
        return ClojureRuntime.core("create-conn", dir);
    }

    /**
     * Returns a shared raw connection handle.
     *
     * <p>When {@code dir} is {@code null}, this mirrors
     * {@link #createConnection(String, Map, Map)} because the underlying
     * Clojure API only supports shared lookup for path-addressed connections.
     */
    public static Object getConnection(String dir,
                                       Map<?, ?> schema,
                                       Map<?, ?> opts) {
        if (dir == null) {
            return createConnection(null, schema, opts);
        }
        if (opts != null) {
            return ClojureRuntime.core("get-conn",
                                       dir,
                                       DatalevinForms.schemaInput(schema),
                                       DatalevinForms.optionsInput(opts));
        }
        if (schema != null) {
            return ClojureRuntime.core("get-conn",
                                       dir,
                                       DatalevinForms.schemaInput(schema));
        }
        return ClojureRuntime.core("get-conn", dir);
    }

    /**
     * Creates a raw connection handle by bulk-loading datoms.
     */
    public static Object initDb(Object datoms,
                                String dir,
                                Map<?, ?> schema,
                                Map<?, ?> opts) {
        Object normalizedDatoms = DatalevinForms.datomsInput(datoms);
        if (opts != null) {
            return ClojureRuntime.core("conn-from-datoms",
                                       normalizedDatoms,
                                       dir,
                                       DatalevinForms.schemaInput(schema),
                                       DatalevinForms.optionsInput(opts));
        }
        if (schema != null) {
            return ClojureRuntime.core("conn-from-datoms",
                                       normalizedDatoms,
                                       dir,
                                       DatalevinForms.schemaInput(schema));
        }
        if (dir != null) {
            return ClojureRuntime.core("conn-from-datoms", normalizedDatoms, dir);
        }
        return ClojureRuntime.core("conn-from-datoms", normalizedDatoms);
    }

    /**
     * Bulk-loads datoms into an existing raw connection handle.
     */
    public static Object fillDb(Object conn, Object datoms) {
        Object newDb = ClojureRuntime.core("fill-db",
                                           ClojureRuntime.core("db", conn),
                                           DatalevinForms.datomsInput(datoms));
        ClojureRuntime.core("reset-conn!", conn, newDb);
        return conn;
    }

    /**
     * Closes a raw connection handle.
     */
    public static void closeConnection(Object conn) {
        ClojureRuntime.core("close", conn);
    }

    /**
     * Returns whether a raw connection handle has been closed.
     */
    public static boolean connectionClosed(Object conn) {
        return ClojureCodec.javaBoolean(ClojureRuntime.core("closed?", conn));
    }

    /**
     * Returns the raw database value for a connection handle.
     */
    public static Object connectionDb(Object conn) {
        return ClojureRuntime.core("db", rawResource(conn));
    }

    /**
     * Returns a bridge-safe database value handle for a connection handle.
     */
    public static DatabaseValue connectionDbBridge(Object conn) {
        return new DatabaseValue(connectionDb(conn));
    }

    /**
     * Resolves an entity id or lookup ref against a database value.
     */
    public static Object databaseEntid(Object db, Object eid) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("entid",
                                                             rawResource(db),
                                                             DatalevinForms.lookupRefInput(eid)));
    }

    /**
     * Returns a Java handle for a lazy entity id or lookup ref against an
     * database value.
     */
    public static LazyEntity databaseEntity(Object db, Object eid) {
        Object entity = ClojureRuntime.core("entity",
                                            rawResource(db),
                                            DatalevinForms.lookupRefInput(eid));
        return entity == null ? null : new LazyEntity(entity);
    }

    /**
     * Returns a touched entity map for an entity id or lookup ref against an
     * database value.
     */
    public static Object databaseEntityMap(Object db, Object eid) {
        Object entity = ClojureRuntime.core("entity",
                                            rawResource(db),
                                            DatalevinForms.lookupRefInput(eid));
        return entity == null ? null : ClojureCodec.bridgeOutput(ClojureRuntime.core("touch", entity));
    }

    /**
     * Pulls one entity from a database value.
     */
    public static Object databasePull(Object db, Object selector, Object eid) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("pull",
                                                             rawResource(db),
                                                             DatalevinForms.pullSelectorInput(selector),
                                                             DatalevinForms.lookupRefInput(eid)));
    }

    /**
     * Pulls many entities from a database value.
     */
    public static Object databasePullMany(Object db, Object selector, List<?> eids) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("pull-many",
                                                             rawResource(db),
                                                             DatalevinForms.pullSelectorInput(selector),
                                                             DatalevinForms.entityIdsInput(eids)));
    }

    /**
     * Returns the number of distinct values for an attribute in a database
     * value.
     */
    public static long databaseCardinality(Object db, Object attr) {
        return ClojureCodec.javaLong(ClojureRuntime.core("cardinality",
                                                         rawResource(db),
                                                         DatalevinForms.datalogAttrInput(attr)));
    }

    /**
     * Collects query-planner statistics for all attributes in a database value.
     */
    public static Object databaseAnalyze(Object db) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("analyze",
                                                             rawResource(db)));
    }

    /**
     * Collects query-planner statistics for one attribute in a database value.
     */
    public static Object databaseAnalyze(Object db, Object attr) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("analyze",
                                                             rawResource(db),
                                                             DatalevinForms.datalogAttrInput(attr)));
    }

    /**
     * Returns a Java handle for a lazy entity id or lookup ref.
     */
    public static LazyEntity connectionEntity(Object conn, Object eid) {
        Object entity = ClojureRuntime.core("entity",
                                            connectionDb(conn),
                                            DatalevinForms.lookupRefInput(eid));
        return entity == null ? null : new LazyEntity(entity);
    }

    /**
     * Returns whether a value is a Datalevin lazy entity handle.
     */
    public static boolean entityIs(Object value) {
        return value instanceof LazyEntity || rawEntity(value);
    }

    /**
     * Returns the bridge-safe entity id for a lazy entity handle.
     */
    public static Object entityId(Object entity) {
        return ClojureCodec.bridgeOutput(
                ClojureRuntime.invoke("clojure.core",
                                      "get",
                                      rawResource(entity),
                                      ClojureCodec.keyword(":db/id")));
    }

    /**
     * Reads one attribute from a lazy entity handle without touching the whole
     * entity.
     */
    public static Object entityGet(Object entity, Object attr) {
        return bridgeEntityValue(ClojureRuntime.invoke("clojure.core",
                                                       "get",
                                                       rawResource(entity),
                                                       DatalevinForms.datalogAttrInput(attr)));
    }

    /**
     * Returns whether a lazy entity handle has a value for the supplied
     * attribute.
     */
    public static boolean entityContains(Object entity, Object attr) {
        return ClojureCodec.javaBoolean(
                ClojureRuntime.invoke("clojure.core",
                                      "contains?",
                                      rawResource(entity),
                                      DatalevinForms.datalogAttrInput(attr)));
    }

    /**
     * Touches and materializes a lazy entity handle into bridge-safe data.
     */
    public static Object entityTouch(Object entity) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("touch", rawResource(entity)));
    }

    /**
     * Returns the raw KV handle backing a Datalog connection handle.
     */
    public static Object connectionDatalogKv(Object conn) {
        return ClojureRuntime.core("datalog-kv", rawResource(conn));
    }

    /**
     * Runs a Datalevin async transaction and blocks until it commits, returning
     * a bridge-safe transaction report.
     */
    public static Object connectionTransact(Object conn,
                                            Object txData,
                                            Map<?, ?> txMeta) {
        Object rawConn = rawResource(conn);
        Object future = txMeta == null
                ? ClojureRuntime.core("transact",
                                      rawConn,
                                      DatalevinForms.txDataInput(txData))
                : ClojureRuntime.core("transact",
                                      rawConn,
                                      DatalevinForms.txDataInput(txData),
                                      ClojureCodec.runtimeInput(txMeta));
        return DatalevinForms.txReportOutput(ClojureRuntime.deref(future));
    }

    /**
     * Starts an async transaction and returns a bridge-safe Java future.
     */
    public static CompletableFuture<Object> connectionTransactAsync(Object conn,
                                                                    Object txData,
                                                                    Map<?, ?> txMeta) {
        Object rawConn = rawResource(conn);
        Object future = txMeta == null
                ? ClojureRuntime.core("transact-async",
                                      rawConn,
                                      DatalevinForms.txDataInput(txData))
                : ClojureRuntime.core("transact-async",
                                      rawConn,
                                      DatalevinForms.txDataInput(txData),
                                      ClojureCodec.runtimeInput(txMeta));
        return derefFuture(future, DatalevinForms::txReportOutput);
    }

    /**
     * Applies transaction data against a raw connection's current database
     * value without committing and returns a bridge-safe simulated report.
     */
    public static Object connectionTxDataToSimulatedReport(Object conn, Object txData) {
        Object report = ClojureRuntime.core("tx-data->simulated-report",
                                            connectionDb(conn),
                                            DatalevinForms.txDataInput(txData));
        return DatalevinForms.txReportOutput(report, true);
    }

    /**
     * Applies transaction data against a raw connection's current database
     * value without committing and returns a bridge-safe simulated report whose
     * database values are wrapped in Java handles.
     */
    public static Object connectionTxDataToSimulatedReportBridge(Object conn, Object txData) {
        Object report = ClojureRuntime.core("tx-data->simulated-report",
                                            connectionDb(conn),
                                            DatalevinForms.txDataInput(txData));
        return DatalevinForms.txReportOutput(report, true, DatabaseValue::new);
    }

    /**
     * Registers a transaction listener with an auto-generated key.
     */
    public static Object connectionListen(Object conn, Consumer<Object> listener) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("listen!",
                                                             rawResource(conn),
                                                             ClojureFns.txReportConsumer(listener)));
    }

    /**
     * Registers or replaces a transaction listener under {@code key}.
     */
    public static Object connectionListen(Object conn, Object key, Consumer<Object> listener) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("listen!",
                                                             rawResource(conn),
                                                             ClojureCodec.runtimeInput(key),
                                                             ClojureFns.txReportConsumer(listener)));
    }

    /**
     * Removes a transaction listener by key.
     */
    public static void connectionUnlisten(Object conn, Object key) {
        ClojureRuntime.core("unlisten!", rawResource(conn), ClojureCodec.runtimeInput(key));
    }

    /**
     * Returns bridge-safe datoms from a raw connection handle.
     */
    public static Object connectionDatoms(Object conn,
                                          Object index,
                                          Object c1,
                                          Object c2,
                                          Object c3,
                                          Long limit) {
        return ClojureCodec.bridgeOutput(connectionDatomIndexRead("datoms", conn, index, c1, c2, c3, limit));
    }

    /**
     * Returns bridge-safe datoms matching entity, attribute, and value.
     */
    public static Object connectionSearchDatoms(Object conn, Object e, Object attr, Object value) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("search-datoms",
                                                             connectionDb(conn),
                                                             ClojureCodec.runtimeInput(e),
                                                             DatalevinForms.datalogAttrInput(attr),
                                                             ClojureCodec.runtimeInput(value)));
    }

    /**
     * Counts datoms matching entity, attribute, and value.
     */
    public static Object connectionCountDatoms(Object conn, Object e, Object attr, Object value) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("count-datoms",
                                                             connectionDb(conn),
                                                             ClojureCodec.runtimeInput(e),
                                                             DatalevinForms.datalogAttrInput(attr),
                                                             ClojureCodec.runtimeInput(value)));
    }

    /**
     * Seeks bridge-safe datoms forward from the supplied index components.
     */
    public static Object connectionSeekDatoms(Object conn,
                                              Object index,
                                              Object c1,
                                              Object c2,
                                              Object c3,
                                              Long limit) {
        return ClojureCodec.bridgeOutput(connectionDatomIndexRead("seek-datoms", conn, index, c1, c2, c3, limit));
    }

    /**
     * Seeks bridge-safe datoms backward from the supplied index components.
     */
    public static Object connectionRseekDatoms(Object conn,
                                               Object index,
                                               Object c1,
                                               Object c2,
                                               Object c3,
                                               Long limit) {
        return ClojureCodec.bridgeOutput(connectionDatomIndexRead("rseek-datoms", conn, index, c1, c2, c3, limit));
    }

    /**
     * Returns bridge-safe datoms in an inclusive AVE value range.
     */
    public static Object connectionIndexRange(Object conn, Object attr, Object start, Object end) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("index-range",
                                                             connectionDb(conn),
                                                             DatalevinForms.datalogAttrInput(attr),
                                                             ClojureCodec.runtimeInput(start),
                                                             ClojureCodec.runtimeInput(end)));
    }

    /**
     * Returns bridge-safe fulltext datom results.
     */
    public static Object connectionFulltextDatoms(Object conn, String query, Map<?, ?> opts) {
        Object result = opts == null
                ? ClojureRuntime.core("fulltext-datoms", connectionDb(conn), query)
                : ClojureRuntime.core("fulltext-datoms",
                                      connectionDb(conn),
                                      query,
                                      DatalevinForms.optionsInput(opts));
        return ClojureCodec.bridgeOutput(result);
    }

    /**
     * Copies the database backing a raw connection handle.
     */
    public static void connectionCopy(Object conn, String dest, Boolean compact) {
        if (compact == null) {
            ClojureRuntime.core("copy", connectionDb(conn), dest);
        } else {
            ClojureRuntime.core("copy", connectionDb(conn), dest, compact);
        }
    }

    /**
     * Returns WAL watermarks for the database backing a raw connection handle.
     */
    public static Object connectionTxLogWatermarks(Object conn) {
        return ClojureRuntime.core("txlog-watermarks", connectionStore(conn));
    }

    /**
     * Opens WAL records for the database backing a raw connection handle.
     */
    public static Object connectionOpenTxLog(Object conn, long fromLsn, Long uptoLsn) {
        if (uptoLsn == null) {
            return ClojureRuntime.core("open-tx-log", connectionStore(conn), fromLsn);
        }
        return ClojureRuntime.core("open-tx-log", connectionStore(conn), fromLsn, uptoLsn);
    }

    /**
     * Creates an LMDB snapshot for the database backing a raw connection handle.
     */
    public static Object connectionCreateSnapshot(Object conn) {
        return ClojureRuntime.core("create-snapshot!", connectionStore(conn));
    }

    /**
     * Lists LMDB snapshots for the database backing a raw connection handle.
     */
    public static Object connectionListSnapshots(Object conn) {
        return ClojureRuntime.core("list-snapshots", connectionStore(conn));
    }

    /**
     * Runs WAL segment GC for the database backing a raw connection handle.
     */
    public static Object connectionGcTxLogSegments(Object conn, Long retainFloorLsn) {
        if (retainFloorLsn == null) {
            return ClojureRuntime.core("gc-txlog-segments!", connectionStore(conn));
        }
        return ClojureRuntime.core("gc-txlog-segments!", connectionStore(conn), retainFloorLsn);
    }

    /**
     * Opens a raw KV handle.
     */
    public static Object openKeyValue(String dir, Map<?, ?> opts) {
        if (opts == null) {
            return ClojureRuntime.core("open-kv", dir);
        }
        return ClojureRuntime.core("open-kv", dir, DatalevinForms.optionsInput(opts));
    }

    /**
     * Closes a raw KV handle.
     */
    public static void closeKeyValue(Object kv) {
        ClojureRuntime.core("close-kv", kv);
    }

    /**
     * Returns whether a raw KV handle has been closed.
     */
    public static boolean keyValueClosed(Object kv) {
        return ClojureCodec.javaBoolean(ClojureRuntime.core("closed-kv?", kv));
    }

    /**
     * Opens an explicit KV write transaction.
     */
    public static Object keyValueBeginTransaction(Object kv) {
        Object rawKv = rawResource(kv);
        return new KVTransaction(new KV(rawKv, false),
                                 ClojureRuntime.core("begin-kv-transaction", rawKv));
    }

    /**
     * Commits an explicit KV write transaction.
     */
    public static Object keyValueCommitTransaction(Object tx) {
        if (tx instanceof KVTransaction transaction) {
            return ClojureCodec.bridgeOutput(transaction.commit());
        }
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("commit-kv-transaction", rawResource(tx)));
    }

    /**
     * Aborts an explicit KV write transaction.
     */
    public static Object keyValueAbortTransaction(Object tx) {
        if (tx instanceof KVTransaction transaction) {
            return ClojureCodec.bridgeOutput(transaction.abort());
        }
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("abort-kv-transaction", rawResource(tx)));
    }

    /**
     * Runs a function inside a single KV write transaction.
     */
    public static Object keyValueWithTransaction(Object kv, Function<Object, Object> fn) {
        Function<Object, Object> wrapped = rawKv -> fn.apply(new KV(rawKv, false));
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("with-transaction-kv-fn",
                                                             rawResource(kv),
                                                             wrapped));
    }

    /**
     * Runs a function inside a single KV write transaction with a timeout in
     * milliseconds.
     */
    public static Object keyValueWithTransaction(Object kv,
                                                 Long timeoutMs,
                                                 Function<Object, Object> fn) {
        Function<Object, Object> wrapped = rawKv -> fn.apply(new KV(rawKv, false));
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("with-transaction-kv-fn",
                                                             rawResource(kv),
                                                             timeoutMs,
                                                             wrapped));
    }

    /**
     * Runs a function inside a single Datalog write transaction.
     */
    public static Object connectionWithTransaction(Object conn, Function<Object, Object> fn) {
        Function<Object, Object> wrapped = rawConn -> fn.apply(new Connection(rawConn, false));
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("with-transaction-fn",
                                                             rawResource(conn),
                                                             wrapped));
    }

    /**
     * Runs a function inside a single Datalog write transaction with a timeout
     * in milliseconds.
     */
    public static Object connectionWithTransaction(Object conn,
                                                   Long timeoutMs,
                                                   Function<Object, Object> fn) {
        Function<Object, Object> wrapped = rawConn -> fn.apply(new Connection(rawConn, false));
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("with-transaction-fn",
                                                             rawResource(conn),
                                                             timeoutMs,
                                                             wrapped));
    }

    /**
     * Rebuilds a raw KV handle's index and returns a bridge-safe handle.
     */
    public static Object keyValueReIndex(Object kv, Map<?, ?> opts) {
        Object next = ClojureRuntime.core("re-index",
                                          rawResource(kv),
                                          DatalevinForms.optionsInput(opts == null ? Map.of() : opts));
        if (kv instanceof KV keyValue) {
            keyValue.replaceResource(next);
            return keyValue;
        }
        return next;
    }

    /**
     * Rebuilds a raw connection handle's index and returns a bridge-safe handle.
     */
    public static Object connectionReIndex(Object conn, Map<?, ?> schema, Map<?, ?> opts) {
        Object next = schema == null
                ? ClojureRuntime.core("re-index",
                                      rawResource(conn),
                                      DatalevinForms.optionsInput(opts == null ? Map.of() : opts))
                : ClojureRuntime.core("re-index",
                                      rawResource(conn),
                                      DatalevinForms.schemaInput(schema),
                                      DatalevinForms.optionsInput(opts == null ? Map.of() : opts));
        if (conn instanceof Connection connection) {
            connection.replaceResource(next);
            return connection;
        }
        return next;
    }

    /**
     * Creates a raw full-text search engine handle.
     */
    public static Object newSearchEngine(Object kv, Map<?, ?> opts) {
        Object rawKv = rawResource(kv);
        if (opts == null) {
            Object optsForm = DatalevinForms.optionsInput(Map.of());
            return new SearchEngine(ClojureRuntime.core("new-search-engine", rawKv, optsForm), optsForm);
        }
        Object optsForm = DatalevinForms.optionsInput(opts);
        return new SearchEngine(ClojureRuntime.core("new-search-engine", rawKv, optsForm), optsForm);
    }

    /**
     * Adds one document to a full-text search engine handle.
     */
    public static Object searchAddDoc(Object search, Object docRef, String docText, Boolean checkExist) {
        Object result = checkExist == null
                ? ClojureRuntime.core("add-doc",
                                      rawResource(search),
                                      ClojureCodec.runtimeInput(docRef),
                                      docText)
                : ClojureRuntime.core("add-doc",
                                      rawResource(search),
                                      ClojureCodec.runtimeInput(docRef),
                                      docText,
                                      checkExist);
        return ClojureCodec.bridgeOutput(result);
    }

    /**
     * Removes one document from a full-text search engine handle.
     */
    public static Object searchRemoveDoc(Object search, Object docRef) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("remove-doc",
                                                             rawResource(search),
                                                             ClojureCodec.runtimeInput(docRef)));
    }

    /**
     * Clears a full-text search engine handle.
     */
    public static Object searchClearDocs(Object search) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("clear-docs", rawResource(search)));
    }

    /**
     * Returns whether one document is indexed in a full-text search engine.
     */
    public static boolean searchDocIndexed(Object search, Object docRef) {
        return ClojureRuntime.core("doc-indexed?",
                                   rawResource(search),
                                   ClojureCodec.runtimeInput(docRef)) != null;
    }

    /**
     * Returns the document count for a full-text search engine.
     */
    public static long searchDocCount(Object search) {
        return ClojureCodec.javaLong(ClojureRuntime.core("doc-count", rawResource(search)));
    }

    /**
     * Searches a full-text search engine.
     */
    public static Object search(Object search, String query, Map<?, ?> opts) {
        Object result = opts == null
                ? ClojureRuntime.core("search", rawResource(search), query)
                : ClojureRuntime.core("search",
                                      rawResource(search),
                                      query,
                                      DatalevinForms.optionsInput(opts));
        return ClojureCodec.bridgeOutput(result);
    }

    /**
     * Rebuilds a full-text search engine from stored raw text.
     */
    public static Object searchReIndex(Object search, Map<?, ?> opts) {
        if (search instanceof SearchEngine engine) {
            engine.reIndex(opts);
            return engine;
        }
        Object optsForm = DatalevinForms.optionsInput(opts == null ? Map.of() : opts);
        Object next = ClojureRuntime.core("re-index",
                                          rawResource(search),
                                          optsForm);
        return new SearchEngine(next, optsForm);
    }

    /**
     * Creates a raw batched full-text search index writer handle.
     */
    public static Object searchIndexWriter(Object kv, Map<?, ?> opts) {
        Object rawKv = rawResource(kv);
        if (opts == null) {
            return new SearchIndexWriter(ClojureRuntime.core("search-index-writer", rawKv));
        }
        return new SearchIndexWriter(ClojureRuntime.core("search-index-writer",
                                                        rawKv,
                                                        DatalevinForms.optionsInput(opts)));
    }

    /**
     * Adds one document to a raw full-text search index writer handle.
     */
    public static Object searchWrite(Object writer, Object docRef, String docText) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("write",
                                                             rawResource(writer),
                                                             ClojureCodec.runtimeInput(docRef),
                                                             docText));
    }

    /**
     * Flushes all pending documents in a raw full-text search index writer.
     */
    public static Object searchCommit(Object writer) {
        Object result = ClojureCodec.bridgeOutput(ClojureRuntime.core("commit", rawResource(writer)));
        if (writer instanceof SearchIndexWriter searchWriter) {
            searchWriter.close();
        }
        return result;
    }

    /**
     * Creates a raw standalone vector index handle.
     */
    public static Object newVectorIndex(Object kv, Map<?, ?> opts) {
        Object optsForm = DatalevinForms.optionsInput(opts == null ? Map.of() : opts);
        return new VectorIndex(ClojureRuntime.core("new-vector-index", rawResource(kv), optsForm), optsForm);
    }

    /**
     * Closes a standalone vector index handle.
     */
    public static void closeVectorIndex(Object index) {
        if (index instanceof VectorIndex vectorIndex) {
            vectorIndex.close();
        } else {
            ClojureRuntime.core("close-vector-index", rawResource(index));
        }
    }

    /**
     * Returns whether a standalone vector index handle has been closed.
     */
    public static boolean vectorIndexClosed(Object index) {
        if (index instanceof VectorIndex vectorIndex) {
            return vectorIndex.closed();
        }
        return ClojureCodec.javaBoolean(ClojureRuntime.invoke("datalevin.interface",
                                                              "vec-closed?",
                                                              rawResource(index)));
    }

    /**
     * Adds one vector to a standalone vector index handle.
     */
    public static Object vectorAddVec(Object index, Object vecRef, Object vecData) {
        Object result = ClojureRuntime.core("add-vec",
                                            rawResource(index),
                                            ClojureCodec.runtimeInput(vecRef),
                                            ClojureCodec.runtimeInput(vecData));
        return ClojureCodec.bridgeOutput(result);
    }

    /**
     * Removes all vectors for a reference from a standalone vector index.
     */
    public static Object vectorRemoveVec(Object index, Object vecRef) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("remove-vec",
                                                             rawResource(index),
                                                             ClojureCodec.runtimeInput(vecRef)));
    }

    /**
     * Returns whether one vector reference is indexed.
     */
    public static boolean vectorIndexed(Object index, Object vecRef) {
        return ClojureRuntime.invoke("datalevin.interface",
                                     "vec-indexed?",
                                     rawResource(index),
                                     ClojureCodec.runtimeInput(vecRef)) != null;
    }

    /**
     * Searches a standalone vector index.
     */
    public static Object vectorSearch(Object index, Object queryVec, Map<?, ?> opts) {
        Object result = opts == null
                ? ClojureRuntime.core("search-vec",
                                      rawResource(index),
                                      ClojureCodec.runtimeInput(queryVec))
                : ClojureRuntime.core("search-vec",
                                      rawResource(index),
                                      ClojureCodec.runtimeInput(queryVec),
                                      DatalevinForms.optionsInput(opts));
        return ClojureCodec.bridgeOutput(result);
    }

    /**
     * Rebuilds a standalone vector index and returns a bridge-safe handle.
     */
    public static Object vectorReIndex(Object index, Map<?, ?> opts) {
        if (index instanceof VectorIndex vectorIndex) {
            vectorIndex.reIndex(opts);
            return vectorIndex;
        }
        Object optsForm = DatalevinForms.optionsInput(opts == null ? Map.of() : opts);
        Object next = ClojureRuntime.core("re-index", rawResource(index), optsForm);
        return new VectorIndex(next, optsForm);
    }

    /**
     * Clears a standalone vector index from memory and disk.
     */
    public static Object vectorClear(Object index) {
        if (index instanceof VectorIndex vectorIndex) {
            vectorIndex.clear();
        } else {
            ClojureRuntime.core("clear-vector-index", rawResource(index));
        }
        return true;
    }

    /**
     * Forces vector checkpoint persistence to the backing KV store.
     */
    public static Object vectorForceCheckpoint(Object index) {
        ClojureRuntime.core("force-vec-checkpoint!", rawResource(index));
        return true;
    }

    /**
     * Returns bridge-safe standalone vector index metadata.
     */
    public static Object vectorInfo(Object index) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("vector-index-info", rawResource(index)));
    }

    /**
     * Returns bridge-safe checkpoint metadata for a standalone vector index.
     */
    public static Object vectorCheckpointState(Object index) {
        return ClojureCodec.bridgeOutput(ClojureRuntime.core("vector-checkpoint-state", rawResource(index)));
    }

    /**
     * Opens a raw remote client handle.
     */
    public static Object newClient(String uri, Map<?, ?> opts) {
        if (opts == null) {
            return ClojureRuntime.client("new-client", uri);
        }
        return ClojureRuntime.client("new-client", uri, DatalevinForms.optionsInput(opts));
    }

    /**
     * Closes a raw remote client handle.
     */
    public static void closeClient(Object client) {
        ClojureRuntime.client("close-client", client);
    }

    /**
     * Returns whether a raw remote client handle has been disconnected.
     */
    public static boolean clientDisconnected(Object client) {
        return ClojureCodec.javaBoolean(ClojureRuntime.client("disconnected?", client));
    }

    /**
     * Reads EDN text into a raw Clojure value.
     */
    public static Object readEdn(String edn) {
        return ClojureRuntime.readEdn(edn);
    }

    /**
     * Writes a JVM/Clojure value as EDN text.
     */
    public static String writeEdn(Object value) {
        return Edn.render(ClojureCodec.runtimeInput(value));
    }

    /**
     * Returns the current Datalevin/Clojure context class loader.
     *
     * <p>Bridge runtimes such as node-java-bridge can use this loader when
     * proxying dynamically generated Clojure classes back into another
     * language runtime.
     */
    public static ClassLoader currentClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    /**
     * Normalizes JVM results into bridge-safe JDK collections and scalar values.
     *
     * <p>This is primarily useful for runtimes such as Node's `java-bridge`
     * that struggle with opaque Clojure implementation classes nested inside
     * otherwise ordinary collections.
     */
    public static Object bridgeResult(Object value) {
        return ClojureCodec.bridgeOutput(value);
    }

    /**
     * Normalizes a keyword-like string into a raw Clojure keyword.
     */
    public static Object keyword(String value) {
        return ClojureCodec.keyword(value);
    }

    /**
     * Normalizes a symbol-like string into a raw Clojure symbol.
     */
    public static Object symbol(String value) {
        return ClojureCodec.symbol(value);
    }

    /**
     * Normalizes a schema map into the raw Clojure form expected by Datalevin.
     */
    public static Object schema(Map<?, ?> schema) {
        return DatalevinForms.schemaInput(schema);
    }

    /**
     * Normalizes an options map into the raw Clojure form expected by Datalevin.
     */
    public static Object options(Map<?, ?> opts) {
        return DatalevinForms.optionsInput(opts);
    }

    /**
     * Normalizes a UDF descriptor into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object udfDescriptor(Map<?, ?> descriptor) {
        return DatalevinForms.udfDescriptorInput(descriptor);
    }

    /**
     * Creates a raw UDF registry handle.
     */
    public static Object createUdfRegistry() {
        return ClojureRuntime.invoke("datalevin.udf", "create-registry");
    }

    /**
     * Registers a Java-backed UDF in a registry.
     */
    public static Object registerUdf(Object registry,
                                     Map<?, ?> descriptor,
                                     UdfFunction fn) {
        Object normalizedDescriptor = DatalevinForms.udfDescriptorInput(descriptor);
        return ClojureRuntime.invoke("datalevin.udf",
                                     "register!",
                                     rawResource(registry),
                                     normalizedDescriptor,
                                     ClojureFns.udfFunction(fn, normalizedDescriptor));
    }

    /**
     * Unregisters a UDF from a registry.
     */
    public static Object unregisterUdf(Object registry, Map<?, ?> descriptor) {
        return ClojureRuntime.invoke("datalevin.udf",
                                     "unregister!",
                                     rawResource(registry),
                                     DatalevinForms.udfDescriptorInput(descriptor));
    }

    /**
     * Returns whether a descriptor is registered in a registry.
     */
    public static boolean registeredUdf(Object registry, Map<?, ?> descriptor) {
        return ClojureCodec.javaBoolean(
                ClojureRuntime.invoke("datalevin.udf",
                                      "registered?",
                                      rawResource(registry),
                                      DatalevinForms.udfDescriptorInput(descriptor)));
    }

    /**
     * Normalizes a schema rename map into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object renameMap(Map<?, ?> renameMap) {
        return DatalevinForms.renameMapInput(renameMap);
    }

    /**
     * Normalizes a collection of attribute names into the raw Clojure form
     * expected by Datalevin.
     */
    public static Object deleteAttrs(Collection<?> attrs) {
        return DatalevinForms.deleteAttrsInput(attrs);
    }

    /**
     * Normalizes a lookup ref value into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object lookupRef(Object value) {
        return DatalevinForms.lookupRefInput(value);
    }

    /**
     * Creates a raw Datalevin datom.
     */
    public static Object datom(Object e, Object attr, Object value) {
        return DatalevinForms.datom(e, attr, value);
    }

    /**
     * Creates a raw Datalevin datom with an explicit transaction id.
     */
    public static Object datom(Object e, Object attr, Object value, Object tx) {
        return DatalevinForms.datom(e, attr, value, tx);
    }

    /**
     * Creates a raw Datalevin datom with an explicit transaction id and
     * assertion flag.
     */
    public static Object datom(Object e, Object attr, Object value, Object tx, Object added) {
        return DatalevinForms.datom(e, attr, value, tx, added);
    }

    /**
     * Normalizes datom input into the raw Clojure form expected by Datalevin.
     */
    public static Object datoms(Object datoms) {
        return DatalevinForms.datomsInput(datoms);
    }

    /**
     * Normalizes Datalog transaction data into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object txData(Object txData) {
        return DatalevinForms.txDataInput(txData);
    }

    /**
     * Normalizes KV transaction data into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object kvTxs(Object txs) {
        return DatalevinForms.kvTxsInput(txs);
    }

    /**
     * Normalizes KV transaction data with default key and value types.
     */
    public static Object kvTxs(Object txs, Object kType, Object vType) {
        return DatalevinForms.kvTxsInput(txs, kType, vType);
    }

    /**
     * Normalizes a typed KV key or value into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object kvInput(Object value, Object type) {
        return DatalevinForms.kvInput(value, type);
    }

    /**
     * Normalizes a KV range form with a typed bound specification.
     */
    public static Object kvRange(List<?> range, Object type) {
        return DatalevinForms.rangeInput(range, type);
    }

    /**
     * Normalizes a KV type value into the raw Clojure form expected by
     * Datalevin.
     */
    public static Object kvType(Object value) {
        return DatalevinForms.typeInput(value);
    }

    public static Object kvGetByRank(Object kv,
                                     String dbiName,
                                     long rank,
                                     Object kType,
                                     Object vType,
                                     boolean ignoreKey) {
        return ClojureRuntime.core("get-by-rank",
                                   runtimeArg(kv),
                                   dbiName,
                                   rank,
                                   typeInput(kType),
                                   typeInput(vType),
                                   ignoreKey);
    }

    public static Object kvGetEntryByRank(Object kv,
                                          String dbiName,
                                          long rank,
                                          Object kType,
                                          Object vType) {
        return kvGetByRank(kv, dbiName, rank, kType, vType, false);
    }

    public static List<?> kvSample(Object kv,
                                   String dbiName,
                                   long n,
                                   Object kType,
                                   Object vType,
                                   boolean ignoreKey) {
        return ResultSupport.sequence(ClojureRuntime.core("sample-kv",
                                                          runtimeArg(kv),
                                                          dbiName,
                                                          n,
                                                          typeInput(kType),
                                                          typeInput(vType),
                                                          ignoreKey));
    }

    public static void kvVisitList(Object kv,
                                   String listName,
                                   Consumer<Object> visitor,
                                   Object key,
                                   Object kType,
                                   Object vType) {
        ClojureRuntime.core("visit-list",
                            runtimeArg(kv),
                            listName,
                            ClojureFns.consumer(visitor),
                            ClojureCodec.runtimeInput(key),
                            typeInput(kType),
                            typeInput(vType),
                            false);
    }

    public static void kvVisitListRaw(Object kv,
                                      String listName,
                                      Consumer<RawBuffer> visitor,
                                      Object key,
                                      Object kType) {
        ClojureRuntime.core("visit-list",
                            runtimeArg(kv),
                            listName,
                            ClojureFns.rawBufferConsumer(visitor),
                            ClojureCodec.runtimeInput(key),
                            typeInput(kType),
                            null,
                            true);
    }

    public static void kvVisitListRange(Object kv,
                                        String listName,
                                        BiConsumer<Object, Object> visitor,
                                        Object kRange,
                                        Object kType,
                                        Object vRange,
                                        Object vType) {
        ClojureRuntime.core("visit-list-range",
                            runtimeArg(kv),
                            listName,
                            ClojureFns.biConsumer(visitor),
                            rangeInput(kRange, kType),
                            typeInput(kType),
                            rangeInput(vRange, vType),
                            typeInput(vType),
                            false);
    }

    public static void kvVisitListRangeRaw(Object kv,
                                           String listName,
                                           Consumer<RawKV> visitor,
                                           Object kRange,
                                           Object kType,
                                           Object vRange,
                                           Object vType) {
        ClojureRuntime.core("visit-list-range",
                            runtimeArg(kv),
                            listName,
                            ClojureFns.rawKvConsumer(visitor),
                            rangeInput(kRange, kType),
                            typeInput(kType),
                            rangeInput(vRange, vType),
                            typeInput(vType),
                            true);
    }

    public static List<?> kvListRangeFilter(Object kv,
                                            String listName,
                                            BiPredicate<Object, Object> predicate,
                                            Object kRange,
                                            Object kType,
                                            Object vRange,
                                            Object vType) {
        return ResultSupport.sequence(ClojureRuntime.core("list-range-filter",
                                                          runtimeArg(kv),
                                                          listName,
                                                          ClojureFns.biPredicate(predicate),
                                                          rangeInput(kRange, kType),
                                                          typeInput(kType),
                                                          rangeInput(vRange, vType),
                                                          typeInput(vType),
                                                          false));
    }

    public static List<?> kvListRangeFilterRaw(Object kv,
                                               String listName,
                                               Predicate<RawKV> predicate,
                                               Object kRange,
                                               Object kType,
                                               Object vRange,
                                               Object vType) {
        return ResultSupport.sequence(ClojureRuntime.core("list-range-filter",
                                                          runtimeArg(kv),
                                                          listName,
                                                          ClojureFns.rawKvPredicate(predicate),
                                                          rangeInput(kRange, kType),
                                                          typeInput(kType),
                                                          rangeInput(vRange, vType),
                                                          typeInput(vType),
                                                          true));
    }

    public static long kvListRangeFilterCount(Object kv,
                                              String listName,
                                              BiPredicate<Object, Object> predicate,
                                              Object kRange,
                                              Object kType,
                                              Object vRange,
                                              Object vType) {
        return ClojureCodec.javaLong(ClojureRuntime.core("list-range-filter-count",
                                                         runtimeArg(kv),
                                                         listName,
                                                         ClojureFns.biPredicate(predicate),
                                                         rangeInput(kRange, kType),
                                                         typeInput(kType),
                                                         rangeInput(vRange, vType),
                                                         typeInput(vType),
                                                         false));
    }

    public static long kvListRangeFilterCountRaw(Object kv,
                                                 String listName,
                                                 Predicate<RawKV> predicate,
                                                 Object kRange,
                                                 Object kType,
                                                 Object vRange,
                                                 Object vType) {
        return ClojureCodec.javaLong(ClojureRuntime.core("list-range-filter-count",
                                                         runtimeArg(kv),
                                                         listName,
                                                         ClojureFns.rawKvPredicate(predicate),
                                                         rangeInput(kRange, kType),
                                                         typeInput(kType),
                                                         rangeInput(vRange, vType),
                                                         typeInput(vType),
                                                         true));
    }

    public static List<?> kvListRangeKeep(Object kv,
                                          String listName,
                                          BiFunction<Object, Object, ?> fn,
                                          Object kRange,
                                          Object kType,
                                          Object vRange,
                                          Object vType) {
        return ResultSupport.sequence(ClojureRuntime.core("list-range-keep",
                                                          runtimeArg(kv),
                                                          listName,
                                                          ClojureFns.biFunction(fn),
                                                          rangeInput(kRange, kType),
                                                          typeInput(kType),
                                                          rangeInput(vRange, vType),
                                                          typeInput(vType),
                                                          false));
    }

    public static List<?> kvListRangeKeepRaw(Object kv,
                                             String listName,
                                             Function<RawKV, ?> fn,
                                             Object kRange,
                                             Object kType,
                                             Object vRange,
                                             Object vType) {
        return ResultSupport.sequence(ClojureRuntime.core("list-range-keep",
                                                          runtimeArg(kv),
                                                          listName,
                                                          ClojureFns.rawKvFunction(fn),
                                                          rangeInput(kRange, kType),
                                                          typeInput(kType),
                                                          rangeInput(vRange, vType),
                                                          typeInput(vType),
                                                          true));
    }

    public static Object kvListRangeSome(Object kv,
                                         String listName,
                                         BiFunction<Object, Object, ?> fn,
                                         Object kRange,
                                         Object kType,
                                         Object vRange,
                                         Object vType) {
        return ClojureRuntime.core("list-range-some",
                                   runtimeArg(kv),
                                   listName,
                                   ClojureFns.biFunction(fn),
                                   rangeInput(kRange, kType),
                                   typeInput(kType),
                                   rangeInput(vRange, vType),
                                   typeInput(vType),
                                   false);
    }

    public static Object kvListRangeSomeRaw(Object kv,
                                            String listName,
                                            Function<RawKV, ?> fn,
                                            Object kRange,
                                            Object kType,
                                            Object vRange,
                                            Object vType) {
        return ClojureRuntime.core("list-range-some",
                                   runtimeArg(kv),
                                   listName,
                                   ClojureFns.rawKvFunction(fn),
                                   rangeInput(kRange, kType),
                                   typeInput(kType),
                                   rangeInput(vRange, vType),
                                   typeInput(vType),
                                   true);
    }

    public static LlamaEmbedder newLlamaEmbedder(String modelPath,
                                                 int gpuLayers,
                                                 int ctxSize,
                                                 int batchSize,
                                                 int threads) {
        return Datalevin.newLlamaEmbedder(modelPath, gpuLayers, ctxSize, batchSize, threads);
    }

    public static void closeLlamaEmbedder(LlamaEmbedder embedder) {
        embedder.close();
    }

    public static boolean llamaEmbedderClosed(LlamaEmbedder embedder) {
        return embedder.closed();
    }

    public static String llamaEmbedderModelPath(LlamaEmbedder embedder) {
        return embedder.modelPath();
    }

    public static int llamaEmbedderGpuLayers(LlamaEmbedder embedder) {
        return embedder.gpuLayers();
    }

    public static int llamaEmbedderCtxSize(LlamaEmbedder embedder) {
        return embedder.ctxSize();
    }

    public static int llamaEmbedderContextSize(LlamaEmbedder embedder) {
        return embedder.contextSize();
    }

    public static int llamaEmbedderBatchSize(LlamaEmbedder embedder) {
        return embedder.batchSize();
    }

    public static int llamaEmbedderThreads(LlamaEmbedder embedder) {
        return embedder.threads();
    }

    public static int llamaEmbedderDimensions(LlamaEmbedder embedder) {
        return embedder.dimensions();
    }

    public static List<Double> llamaEmbedderEmbed(LlamaEmbedder embedder, String text) {
        return floatArrayAsList(embedder.embed(text));
    }

    public static List<List<Double>> llamaEmbedderEmbedAll(LlamaEmbedder embedder, List<?> texts) {
        ArrayList<String> input = new ArrayList<>(texts.size());
        for (Object text : texts) {
            input.add((String) Objects.requireNonNull(text, "text"));
        }
        List<float[]> vectors = embedder.embedAll(input);
        ArrayList<List<Double>> result = new ArrayList<>(vectors.size());
        for (float[] vector : vectors) {
            result.add(floatArrayAsList(vector));
        }
        return result;
    }

    public static int llamaEmbedderTokenCount(LlamaEmbedder embedder, String text) {
        return embedder.tokenCount(text);
    }

    public static List<Integer> llamaEmbedderTokenize(LlamaEmbedder embedder, String text) {
        return intArrayAsList(embedder.tokenize(text));
    }

    public static String llamaEmbedderDetokenize(LlamaEmbedder embedder, List<?> tokens) {
        return embedder.detokenize(intListAsArray(tokens));
    }

    public static String llamaEmbedderTruncateText(LlamaEmbedder embedder, String text, int maxTokens) {
        return embedder.truncateText(text, maxTokens);
    }

    public static LlamaGenerator newLlamaGenerator(String modelPath,
                                                   int gpuLayers,
                                                   int ctxSize,
                                                   int threads) {
        return Datalevin.newLlamaGenerator(modelPath, gpuLayers, ctxSize, threads);
    }

    public static void closeLlamaGenerator(LlamaGenerator generator) {
        generator.close();
    }

    public static boolean llamaGeneratorClosed(LlamaGenerator generator) {
        return generator.closed();
    }

    public static String llamaGeneratorModelPath(LlamaGenerator generator) {
        return generator.modelPath();
    }

    public static int llamaGeneratorGpuLayers(LlamaGenerator generator) {
        return generator.gpuLayers();
    }

    public static int llamaGeneratorCtxSize(LlamaGenerator generator) {
        return generator.ctxSize();
    }

    public static int llamaGeneratorContextSize(LlamaGenerator generator) {
        return generator.contextSize();
    }

    public static int llamaGeneratorThreads(LlamaGenerator generator) {
        return generator.threads();
    }

    public static int llamaGeneratorTokenCount(LlamaGenerator generator, String text) {
        return generator.tokenCount(text);
    }

    public static String llamaGeneratorGenerate(LlamaGenerator generator, String prompt, int maxTokens) {
        return generator.generate(prompt, maxTokens);
    }

    public static String llamaGeneratorSummarize(LlamaGenerator generator, String text, int maxTokens) {
        return generator.summarize(text, maxTokens);
    }

    /**
     * Normalizes a database type string into the raw Clojure form expected by
     * the client API.
     */
    public static Object databaseType(String dbType) {
        return DatalevinForms.createDatabaseType(dbType);
    }

    /**
     * Normalizes a role name into the raw Clojure form expected by the client
     * API.
     */
    public static Object role(String role) {
        return DatalevinForms.roleInput(role);
    }

    /**
     * Normalizes a permission keyword into the raw Clojure form expected by the
     * client API.
     */
    public static Object permissionKeyword(String value) {
        return DatalevinForms.permissionKeyword(value);
    }

    /**
     * Normalizes a permission target into the raw Clojure form expected by the
     * client API.
     */
    public static Object permissionTarget(String objectType, Object target) {
        return DatalevinForms.permissionTarget(objectType, target);
    }

    private static Object[] normalizeArgs(List<?> args) {
        if (args == null || args.isEmpty()) {
            return new Object[0];
        }
        Object[] normalized = new Object[args.size()];
        int i = 0;
        for (Object arg : args) {
            normalized[i++] = runtimeArg(arg);
        }
        return normalized;
    }

    private static Object runtimeArg(Object arg) {
        if (arg instanceof HandleResource handle) {
            return ClojureCodec.runtimeInput(handle.handle());
        }
        return ClojureCodec.runtimeInput(arg);
    }

    private static Object typeInput(Object type) {
        return DatalevinForms.typeInput(type);
    }

    private static Object rangeInput(Object range, Object type) {
        Object normalizedType = typeInput(type);
        if (range instanceof RangeSpec spec) {
            return DatalevinForms.rangeInput(spec.build(), normalizedType);
        }
        return DatalevinForms.rangeInput((List<?>) range, normalizedType);
    }

    private static List<Double> floatArrayAsList(float[] values) {
        ArrayList<Double> result = new ArrayList<>(values.length);
        for (float value : values) {
            result.add((double) value);
        }
        return result;
    }

    private static List<Integer> intArrayAsList(int[] values) {
        ArrayList<Integer> result = new ArrayList<>(values.length);
        for (int value : values) {
            result.add(value);
        }
        return result;
    }

    private static int[] intListAsArray(List<?> values) {
        int[] result = new int[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (!(value instanceof Number number)) {
                throw new IllegalArgumentException("token ids must be numbers");
            }
            result[i] = number.intValue();
        }
        return result;
    }

    private static CompletableFuture<Object> derefFuture(Object future,
                                                         Function<Object, Object> converter) {
        CompletableFuture<Object> result = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                result.complete(converter.apply(ClojureRuntime.deref(future)));
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    private static Object rawResource(Object value) {
        if (value instanceof HandleResource handle) {
            return handle.handle();
        }
        if (value instanceof LazyEntity entity) {
            return entity.handle();
        }
        if (value instanceof DatabaseValue db) {
            return db.handle();
        }
        if (value instanceof UdfRegistry registry) {
            return registry.rawHandle();
        }
        return value;
    }

    private static boolean rawEntity(Object value) {
        return value != null && "datalevin.entity.Entity".equals(value.getClass().getName());
    }

    private static Object bridgeEntityValue(Object value) {
        if (value == null) {
            return null;
        }

        if (rawEntity(value)) {
            return new LazyEntity(value);
        }

        if (value instanceof Map<?, ?> map) {
            LinkedHashMap<Object, Object> result = new LinkedHashMap<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(ClojureCodec.bridgeOutput(entry.getKey()),
                           bridgeEntityValue(entry.getValue()));
            }
            return result;
        }

        if (value instanceof Set<?> set) {
            LinkedHashSet<Object> result = new LinkedHashSet<>(set.size());
            for (Object item : set) {
                result.add(bridgeEntityValue(item));
            }
            return result;
        }

        if (value instanceof Collection<?> collection) {
            ArrayList<Object> result = new ArrayList<>(collection.size());
            for (Object item : collection) {
                result.add(bridgeEntityValue(item));
            }
            return result;
        }

        if (value instanceof Iterable<?> iterable) {
            ArrayList<Object> result = new ArrayList<>();
            for (Object item : iterable) {
                result.add(bridgeEntityValue(item));
            }
            return result;
        }

        if (value instanceof Object[] array) {
            ArrayList<Object> result = new ArrayList<>(array.length);
            for (Object item : array) {
                result.add(bridgeEntityValue(item));
            }
            return result;
        }

        return ClojureCodec.bridgeOutput(value);
    }

    private static Object connectionStore(Object conn) {
        return ClojureRuntime.invoke("clojure.core",
                                     "get",
                                     connectionDb(conn),
                                     ClojureCodec.keyword(":store"));
    }

    private static Object connectionDatomIndexRead(String function,
                                                   Object conn,
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
            return ClojureRuntime.core(function, connectionDb(conn), normalizedIndex, nc1, nc2, nc3);
        }
        return ClojureRuntime.core(function, connectionDb(conn), normalizedIndex, nc1, nc2, nc3, limit);
    }
}
