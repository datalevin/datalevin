import { toEdnForm, toJava, toJs, toQueryInput } from "./convert.js";
import { DatalevinError } from "./errors.js";
import { callJavaMethod, classes, javaBridgeModule, jvmStarted, startJvm } from "./jvm.js";

export const CONNECTION_WITH_TRANSACTION_UNSUPPORTED =
  "Connection withTransaction is not exposed by the JavaScript binding because Java interface callbacks deadlock when the callback calls back into Datalevin. Use transact() for a single Datalog transaction, or KV withTransaction for explicit KV transactions.";

async function unwrapInteropHandle(value) {
  if (typeof value?.rawHandle === "function") {
    return value.rawHandle();
  }
  return value;
}

function hasValue(value) {
  return value !== null && value !== undefined;
}

function datomMapValue(value, key) {
  return value?.[`:${key}`] ?? value?.[key] ?? null;
}

function datomField(value, key, index) {
  if (Array.isArray(value)) {
    return index < value.length ? value[index] : null;
  }
  if (value !== null && typeof value === "object") {
    return datomMapValue(value, key);
  }
  throw new TypeError("Expected datom-shaped array or object.");
}

async function normalizeInteropArgs(args = []) {
  const normalized = [];
  for (const arg of args) {
    normalized.push(await unwrapInteropHandle(arg));
  }
  return normalized;
}

async function createFunctionProxy(fn) {
  const { newProxy } = await javaBridgeModule();
  return newProxy("java.util.function.Function", {
    apply: async (value) => {
      const result = await fn(value);
      return toJava(result === undefined ? null : result);
    }
  });
}

let interfaceProxyEventLoopDepth = 0;
let interfaceProxyEventLoopPrevious = false;

async function withInterfaceProxyEventLoop(fn) {
  const bridge = await javaBridgeModule();
  if (interfaceProxyEventLoopDepth === 0) {
    interfaceProxyEventLoopPrevious = bridge.config.runEventLoopWhenInterfaceProxyIsActive;
    bridge.config.runEventLoopWhenInterfaceProxyIsActive = true;
  }
  interfaceProxyEventLoopDepth += 1;
  try {
    return await fn();
  } finally {
    interfaceProxyEventLoopDepth -= 1;
    if (interfaceProxyEventLoopDepth === 0) {
      bridge.config.runEventLoopWhenInterfaceProxyIsActive = interfaceProxyEventLoopPrevious;
    }
  }
}

class InteropBindings {
  async apiInfoRaw() {
    const cls = await classes();
    return callJavaMethod(cls.datalevin, "apiInfo");
  }

  async execJson(op, args = null) {
    const cls = await classes();
    if (args === null || args === undefined) {
      return callJavaMethod(cls.datalevin, "exec", op);
    }
    return callJavaMethod(cls.datalevin, "exec", op, await toJava(args));
  }

  async coreInvoke(functionName, args = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "coreInvokeBridge",
      functionName,
      await toJava(await normalizeInteropArgs([...(args || [])]))
    );
  }

  async coreInvokeRaw(functionName, args = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "coreInvoke",
      functionName,
      await toJava(await normalizeInteropArgs([...(args || [])]))
    );
  }

  async clientInvoke(functionName, args = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "clientInvokeBridge",
      functionName,
      await toJava(await normalizeInteropArgs([...(args || [])]))
    );
  }

  async createConnection(dir = null, schema = null, opts = null, { shared = false } = {}) {
    const cls = await classes();
    const target = shared ? "getConn" : "createConn";

    if (opts !== null && opts !== undefined) {
      return callJavaMethod(cls.datalevin, target, dir, await toJava(schema), await toJava(opts));
    }
    if (schema !== null && schema !== undefined) {
      return callJavaMethod(cls.datalevin, target, dir, await toJava(schema));
    }
    if (dir !== null && dir !== undefined) {
      return callJavaMethod(cls.datalevin, target, dir);
    }
    return callJavaMethod(cls.datalevin, target);
  }

  async initDb(datoms, dir = null, schema = null, opts = null) {
    const cls = await classes();
    if (opts !== null && opts !== undefined) {
      return callJavaMethod(cls.datalevin, "initDb", await toJava(datoms), dir, await toJava(schema), await toJava(opts));
    }
    if (schema !== null && schema !== undefined) {
      return callJavaMethod(cls.datalevin, "initDb", await toJava(datoms), dir, await toJava(schema));
    }
    if (dir !== null && dir !== undefined) {
      return callJavaMethod(cls.datalevin, "initDb", await toJava(datoms), dir);
    }
    return callJavaMethod(cls.datalevin, "initDb", await toJava(datoms));
  }

  async fillDb(conn, datoms) {
    const handle = await unwrapInteropHandle(conn);
    await callJavaMethod(handle, "fillDb", await toJava(datoms));
    return handle;
  }

  async closeConnection(handle) {
    await callJavaMethod(handle, "close");
  }

  async connectionClosed(handle) {
    return Boolean(await callJavaMethod(handle, "closed"));
  }

  async connectionDb(handle) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "connectionDbBridge", handle);
  }

  async connectionQuery(handle, query, inputs, { form = false } = {}) {
    const cls = await classes();
    const bridgeMethod = form
      ? "connectionQueryFormBridge"
      : "connectionQueryBridge";
    if (typeof cls.interop[bridgeMethod] === "function") {
      return callJavaMethod(cls.interop, bridgeMethod, handle, query, inputs);
    }

    // Compatibility for runtimes predating the in-JVM query bridge. Runtime
    // builds that return SpillableSet require the bridge method above.
    const method = form ? "queryForm" : "query";
    const result = await callJavaMethod(handle, method, query, inputs);
    return callJavaMethod(cls.interop, "bridgeResult", result);
  }

  async connectionEntity(handle, eid) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionEntity",
      await unwrapInteropHandle(handle),
      await toJava(eid)
    );
  }

  async databaseEntid(db, eid) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "databaseEntid",
      await unwrapInteropHandle(db),
      await toJava(eid)
    );
  }

  async databaseEntity(db, eid) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "databaseEntity",
      await unwrapInteropHandle(db),
      await toJava(eid)
    );
  }

  async databaseEntityMap(db, eid) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "databaseEntityMap",
      await unwrapInteropHandle(db),
      await toJava(eid)
    );
  }

  async databasePull(db, selector, eid) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "databasePull",
      await unwrapInteropHandle(db),
      selector,
      await toJava(eid)
    );
  }

  async databasePullMany(db, selector, eids) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "databasePullMany",
      await unwrapInteropHandle(db),
      selector,
      await toJava(eids)
    );
  }

  async databaseCardinality(db, attr) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "databaseCardinality",
      await unwrapInteropHandle(db),
      await toJava(attr)
    );
  }

  async databaseAnalyze(db, attr = null) {
    const cls = await classes();
    const rawDb = await unwrapInteropHandle(db);
    if (attr === null || attr === undefined) {
      return callJavaMethod(cls.interop, "databaseAnalyze", rawDb);
    }
    return callJavaMethod(cls.interop, "databaseAnalyze", rawDb, await toJava(attr));
  }

  async entityIs(value) {
    if (value === null || value === undefined || (typeof value !== "object" && typeof value !== "function")) {
      return false;
    }
    const cls = await classes();
    return Boolean(await callJavaMethod(cls.interop, "entityIs", value));
  }

  async entityId(entity) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "entityId", await unwrapInteropHandle(entity));
  }

  async entityGet(entity, attr) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "entityGet",
      await unwrapInteropHandle(entity),
      await toJava(attr)
    );
  }

  async entityContains(entity, attr) {
    const cls = await classes();
    return Boolean(
      await callJavaMethod(
        cls.interop,
        "entityContains",
        await unwrapInteropHandle(entity),
        await toJava(attr)
      )
    );
  }

  async entityTouch(entity) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "entityTouch", await unwrapInteropHandle(entity));
  }

  async connectionTransactAsync(handle, txData, txMeta = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionTransactAsync",
      await unwrapInteropHandle(handle),
      await toJava(txData),
      hasValue(txMeta) ? await toJava(txMeta) : null
    );
  }

  async connectionTransact(handle, txData, txMeta = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionTransact",
      await unwrapInteropHandle(handle),
      await toJava(txData),
      hasValue(txMeta) ? await toJava(txMeta) : null
    );
  }

  async connectionAbortTransact(handle) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionAbortTransact",
      await unwrapInteropHandle(handle)
    );
  }

  async connectionTxDataToSimulatedReport(handle, txData) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionTxDataToSimulatedReportBridge",
      await unwrapInteropHandle(handle),
      await toJava(txData)
    );
  }

  async connectionListen(handle, keyOrListener, listener = null) {
    const cls = await classes();
    if (hasValue(listener)) {
      return callJavaMethod(
        cls.interop,
        "connectionListen",
        await unwrapInteropHandle(handle),
        await toJava(keyOrListener),
        listener
      );
    }
    return callJavaMethod(
      cls.interop,
      "connectionListen",
      await unwrapInteropHandle(handle),
      keyOrListener
    );
  }

  async connectionUnlisten(handle, key) {
    const cls = await classes();
    await callJavaMethod(
      cls.interop,
      "connectionUnlisten",
      await unwrapInteropHandle(handle),
      await toJava(key)
    );
  }

  async connectionDatoms(handle, index, c1 = null, c2 = null, c3 = null, limit = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionDatoms",
      await unwrapInteropHandle(handle),
      await toJava(index),
      await toJava(c1),
      await toJava(c2),
      await toJava(c3),
      hasValue(limit) ? await toJava(limit) : null
    );
  }

  async connectionSearchDatoms(handle, e = null, attr = null, value = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionSearchDatoms",
      await unwrapInteropHandle(handle),
      await toJava(e),
      await toJava(attr),
      await toJava(value)
    );
  }

  async connectionCountDatoms(handle, e = null, attr = null, value = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionCountDatoms",
      await unwrapInteropHandle(handle),
      await toJava(e),
      await toJava(attr),
      await toJava(value)
    );
  }

  async connectionSeekDatoms(handle, index, c1 = null, c2 = null, c3 = null, limit = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionSeekDatoms",
      await unwrapInteropHandle(handle),
      await toJava(index),
      await toJava(c1),
      await toJava(c2),
      await toJava(c3),
      hasValue(limit) ? await toJava(limit) : null
    );
  }

  async connectionRseekDatoms(handle, index, c1 = null, c2 = null, c3 = null, limit = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionRseekDatoms",
      await unwrapInteropHandle(handle),
      await toJava(index),
      await toJava(c1),
      await toJava(c2),
      await toJava(c3),
      hasValue(limit) ? await toJava(limit) : null
    );
  }

  async connectionIndexRange(handle, attr, start, end) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionIndexRange",
      await unwrapInteropHandle(handle),
      await toJava(attr),
      await toJava(start),
      await toJava(end)
    );
  }

  async connectionFulltextDatoms(handle, query, opts = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionFulltextDatoms",
      await unwrapInteropHandle(handle),
      query,
      hasValue(opts) ? await toJava(opts) : null
    );
  }

  async connectionCopy(handle, dest, compact = null) {
    const cls = await classes();
    await callJavaMethod(
      cls.interop,
      "connectionCopy",
      await unwrapInteropHandle(handle),
      dest,
      hasValue(compact) ? Boolean(compact) : null
    );
  }

  async connectionTxLogWatermarks(handle) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "connectionTxLogWatermarks", await unwrapInteropHandle(handle));
  }

  async connectionOpenTxLog(handle, fromLsn, uptoLsn = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionOpenTxLog",
      await unwrapInteropHandle(handle),
      await toJava(fromLsn),
      hasValue(uptoLsn) ? await toJava(uptoLsn) : null
    );
  }

  async connectionCreateSnapshot(handle) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "connectionCreateSnapshot", await unwrapInteropHandle(handle));
  }

  async connectionListSnapshots(handle) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "connectionListSnapshots", await unwrapInteropHandle(handle));
  }

  async connectionGcTxLogSegments(handle, retainFloorLsn = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionGcTxLogSegments",
      await unwrapInteropHandle(handle),
      hasValue(retainFloorLsn) ? await toJava(retainFloorLsn) : null
    );
  }

  async connectionReIndex(handle, schema = null, opts = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "connectionReIndex",
      await unwrapInteropHandle(handle),
      hasValue(schema) ? await toJava(schema) : null,
      hasValue(opts) ? await toJava(opts) : null
    );
  }

  async openKeyValue(dir, opts = null) {
    const cls = await classes();
    if (opts !== null && opts !== undefined) {
      return callJavaMethod(cls.datalevin, "openKV", dir, await toJava(opts));
    }
    return callJavaMethod(cls.datalevin, "openKV", dir);
  }

  async closeKeyValue(handle) {
    await callJavaMethod(handle, "close");
  }

  async keyValueClosed(handle) {
    return Boolean(await callJavaMethod(handle, "closed"));
  }

  async keyValueBeginTransaction(handle) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "keyValueBeginTransaction", await unwrapInteropHandle(handle));
  }

  async keyValueCommitTransaction(tx) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "keyValueCommitTransaction", await unwrapInteropHandle(tx));
  }

  async keyValueAbortTransaction(tx) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "keyValueAbortTransaction", await unwrapInteropHandle(tx));
  }

  async keyValueWithTransaction(handle, fn) {
    const cls = await classes();
    const proxy = await createFunctionProxy(fn);
    try {
      return await withInterfaceProxyEventLoop(async () => (
        callJavaMethod(
          cls.interop,
          "keyValueWithTransaction",
          await unwrapInteropHandle(handle),
          proxy
        )
      ));
    } finally {
      proxy.reset?.();
    }
  }

  async keyValueReIndex(handle, opts = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "keyValueReIndex",
      await unwrapInteropHandle(handle),
      hasValue(opts) ? await toJava(opts) : null
    );
  }

  async kvGetByRank(handle, dbiName, rank, kType, vType, ignoreKey) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvGetByRank",
      await unwrapInteropHandle(handle),
      dbiName,
      await toJava(rank),
      await toJava(kType),
      await toJava(vType),
      Boolean(ignoreKey)
    );
  }

  async kvGetEntryByRank(handle, dbiName, rank, kType, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvGetEntryByRank",
      await unwrapInteropHandle(handle),
      dbiName,
      await toJava(rank),
      await toJava(kType),
      await toJava(vType)
    );
  }

  async kvSample(handle, dbiName, n, kType, vType, ignoreKey) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvSample",
      await unwrapInteropHandle(handle),
      dbiName,
      await toJava(n),
      await toJava(kType),
      await toJava(vType),
      Boolean(ignoreKey)
    );
  }

  async kvVisit(handle, dbiName, visitor, kRange, kType, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvVisit",
      await unwrapInteropHandle(handle),
      dbiName,
      visitor,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vType)
    );
  }

  async kvVisitRaw(handle, dbiName, visitor, kRange, kType, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvVisitRaw",
      await unwrapInteropHandle(handle),
      dbiName,
      visitor,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vType)
    );
  }

  async kvVisitKeyRange(handle, dbiName, visitor, kRange, kType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvVisitKeyRange",
      await unwrapInteropHandle(handle),
      dbiName,
      visitor,
      await toJava(kRange),
      await toJava(kType)
    );
  }

  async kvVisitKeyRangeRaw(handle, dbiName, visitor, kRange, kType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvVisitKeyRangeRaw",
      await unwrapInteropHandle(handle),
      dbiName,
      visitor,
      await toJava(kRange),
      await toJava(kType)
    );
  }

  async kvVisitList(handle, listName, visitor, key, kType, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvVisitList",
      await unwrapInteropHandle(handle),
      listName,
      visitor,
      await toJava(key),
      await toJava(kType),
      await toJava(vType)
    );
  }

  async kvVisitListRaw(handle, listName, visitor, key, kType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvVisitListRaw",
      await unwrapInteropHandle(handle),
      listName,
      visitor,
      await toJava(key),
      await toJava(kType)
    );
  }

  async kvVisitListRange(handle, listName, visitor, kRange, kType, vRange, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvVisitListRange",
      await unwrapInteropHandle(handle),
      listName,
      visitor,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vRange),
      await toJava(vType)
    );
  }

  async kvVisitListRangeRaw(handle, listName, visitor, kRange, kType, vRange, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvVisitListRangeRaw",
      await unwrapInteropHandle(handle),
      listName,
      visitor,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vRange),
      await toJava(vType)
    );
  }

  async kvListRangeFilter(handle, listName, predicate, kRange, kType, vRange, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvListRangeFilter",
      await unwrapInteropHandle(handle),
      listName,
      predicate,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vRange),
      await toJava(vType)
    );
  }

  async kvListRangeFilterRaw(handle, listName, predicate, kRange, kType, vRange, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvListRangeFilterRaw",
      await unwrapInteropHandle(handle),
      listName,
      predicate,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vRange),
      await toJava(vType)
    );
  }

  async kvListRangeFilterCount(handle, listName, predicate, kRange, kType, vRange, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvListRangeFilterCount",
      await unwrapInteropHandle(handle),
      listName,
      predicate,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vRange),
      await toJava(vType)
    );
  }

  async kvListRangeFilterCountRaw(handle, listName, predicate, kRange, kType, vRange, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvListRangeFilterCountRaw",
      await unwrapInteropHandle(handle),
      listName,
      predicate,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vRange),
      await toJava(vType)
    );
  }

  async kvListRangeKeep(handle, listName, fn, kRange, kType, vRange, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvListRangeKeep",
      await unwrapInteropHandle(handle),
      listName,
      fn,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vRange),
      await toJava(vType)
    );
  }

  async kvListRangeKeepRaw(handle, listName, fn, kRange, kType, vRange, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvListRangeKeepRaw",
      await unwrapInteropHandle(handle),
      listName,
      fn,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vRange),
      await toJava(vType)
    );
  }

  async kvListRangeSome(handle, listName, fn, kRange, kType, vRange, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvListRangeSome",
      await unwrapInteropHandle(handle),
      listName,
      fn,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vRange),
      await toJava(vType)
    );
  }

  async kvListRangeSomeRaw(handle, listName, fn, kRange, kType, vRange, vType) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvListRangeSomeRaw",
      await unwrapInteropHandle(handle),
      listName,
      fn,
      await toJava(kRange),
      await toJava(kType),
      await toJava(vRange),
      await toJava(vType)
    );
  }

  async newSearchEngine(kv, opts = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "newSearchEngine",
      await unwrapInteropHandle(kv),
      hasValue(opts) ? await toJava(opts) : null
    );
  }

  async searchAddDoc(search, docRef, docText, checkExist = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "searchAddDoc",
      await unwrapInteropHandle(search),
      await toJava(docRef),
      docText,
      hasValue(checkExist) ? Boolean(checkExist) : null
    );
  }

  async searchRemoveDoc(search, docRef) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "searchRemoveDoc",
      await unwrapInteropHandle(search),
      await toJava(docRef)
    );
  }

  async searchClearDocs(search) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "searchClearDocs", await unwrapInteropHandle(search));
  }

  async searchDocIndexed(search, docRef) {
    const cls = await classes();
    return Boolean(
      await callJavaMethod(
        cls.interop,
        "searchDocIndexed",
        await unwrapInteropHandle(search),
        await toJava(docRef)
      )
    );
  }

  async searchDocCount(search) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "searchDocCount", await unwrapInteropHandle(search));
  }

  async search(search, query, opts = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "search",
      await unwrapInteropHandle(search),
      query,
      hasValue(opts) ? await toJava(opts) : null
    );
  }

  async searchReIndex(search, opts = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "searchReIndex",
      await unwrapInteropHandle(search),
      hasValue(opts) ? await toJava(opts) : null
    );
  }

  async searchIndexWriter(kv, opts = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "searchIndexWriter",
      await unwrapInteropHandle(kv),
      hasValue(opts) ? await toJava(opts) : null
    );
  }

  async searchUtilsCreateAnalyzer(opts = null) {
    const cls = await classes();
    return callJavaMethod(cls.searchUtils, "createAnalyzer", await toJava(opts ?? {}));
  }

  async searchUtilsLowerCaseTokenFilter() {
    const cls = await classes();
    return callJavaMethod(cls.searchUtils, "lowerCaseTokenFilter");
  }

  async searchUtilsUnaccentTokenFilter() {
    const cls = await classes();
    return callJavaMethod(cls.searchUtils, "unaccentTokenFilter");
  }

  async searchUtilsCreateStopWordsTokenFilter(stopWordsOrPredicate) {
    const cls = await classes();
    return callJavaMethod(
      cls.searchUtils,
      "createStopWordsTokenFilter",
      await toJava(stopWordsOrPredicate)
    );
  }

  async searchUtilsEnStopWordsTokenFilter() {
    const cls = await classes();
    return callJavaMethod(cls.searchUtils, "enStopWordsTokenFilter");
  }

  async searchUtilsPrefixTokenFilter() {
    const cls = await classes();
    return callJavaMethod(cls.searchUtils, "prefixTokenFilter");
  }

  async searchUtilsCreateNgramTokenFilter(minGramSize, maxGramSize = null) {
    const cls = await classes();
    if (!hasValue(maxGramSize)) {
      return callJavaMethod(cls.searchUtils, "createNgramTokenFilter", await toJava(minGramSize));
    }
    return callJavaMethod(
      cls.searchUtils,
      "createNgramTokenFilter",
      await toJava(minGramSize),
      await toJava(maxGramSize)
    );
  }

  async searchUtilsCreateMinLengthTokenFilter(minLength) {
    const cls = await classes();
    return callJavaMethod(cls.searchUtils, "createMinLengthTokenFilter", await toJava(minLength));
  }

  async searchUtilsCreateMaxLengthTokenFilter(maxLength) {
    const cls = await classes();
    return callJavaMethod(cls.searchUtils, "createMaxLengthTokenFilter", await toJava(maxLength));
  }

  async searchUtilsCreateStemmingTokenFilter(language) {
    const cls = await classes();
    return callJavaMethod(cls.searchUtils, "createStemmingTokenFilter", language);
  }

  async searchUtilsCreateRegexpTokenizer(pattern) {
    const cls = await classes();
    return callJavaMethod(cls.searchUtils, "createRegexpTokenizer", pattern);
  }

  async searchWrite(writer, docRef, docText) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "searchWrite",
      await unwrapInteropHandle(writer),
      await toJava(docRef),
      docText
    );
  }

  async searchCommit(writer) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "searchCommit", await unwrapInteropHandle(writer));
  }

  async newVectorIndex(kv, opts) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "newVectorIndex",
      await unwrapInteropHandle(kv),
      hasValue(opts) ? await toJava(opts) : null
    );
  }

  async closeVectorIndex(index) {
    const cls = await classes();
    await callJavaMethod(cls.interop, "closeVectorIndex", await unwrapInteropHandle(index));
  }

  async vectorIndexClosed(index) {
    const cls = await classes();
    return Boolean(await callJavaMethod(cls.interop, "vectorIndexClosed", await unwrapInteropHandle(index)));
  }

  async vectorAddVec(index, vecRef, vecData) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "vectorAddVec",
      await unwrapInteropHandle(index),
      await toJava(vecRef),
      await toJava(vecData)
    );
  }

  async vectorRemoveVec(index, vecRef) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "vectorRemoveVec",
      await unwrapInteropHandle(index),
      await toJava(vecRef)
    );
  }

  async vectorIndexed(index, vecRef) {
    const cls = await classes();
    return Boolean(
      await callJavaMethod(
        cls.interop,
        "vectorIndexed",
        await unwrapInteropHandle(index),
        await toJava(vecRef)
      )
    );
  }

  async vectorSearch(index, queryVec, opts = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "vectorSearch",
      await unwrapInteropHandle(index),
      await toJava(queryVec),
      hasValue(opts) ? await toJava(opts) : null
    );
  }

  async vectorReIndex(index, opts = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "vectorReIndex",
      await unwrapInteropHandle(index),
      hasValue(opts) ? await toJava(opts) : null
    );
  }

  async vectorClear(index) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "vectorClear", await unwrapInteropHandle(index));
  }

  async vectorForceCheckpoint(index) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "vectorForceCheckpoint", await unwrapInteropHandle(index));
  }

  async vectorInfo(index) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "vectorInfo", await unwrapInteropHandle(index));
  }

  async vectorCheckpointState(index) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "vectorCheckpointState", await unwrapInteropHandle(index));
  }

  async newLlamaEmbedder(modelPath, { gpuLayers = 0, ctxSize = 0, batchSize = 0, threads = 0 } = {}) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "newLlamaEmbedder",
      modelPath,
      Number(gpuLayers),
      Number(ctxSize),
      Number(batchSize),
      Number(threads)
    );
  }

  async closeLlamaEmbedder(embedder) {
    const cls = await classes();
    await callJavaMethod(cls.interop, "closeLlamaEmbedder", await unwrapInteropHandle(embedder));
  }

  async llamaEmbedderClosed(embedder) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderClosed", await unwrapInteropHandle(embedder));
  }

  async llamaEmbedderModelPath(embedder) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderModelPath", await unwrapInteropHandle(embedder));
  }

  async llamaEmbedderGpuLayers(embedder) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderGpuLayers", await unwrapInteropHandle(embedder));
  }

  async llamaEmbedderCtxSize(embedder) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderCtxSize", await unwrapInteropHandle(embedder));
  }

  async llamaEmbedderContextSize(embedder) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderContextSize", await unwrapInteropHandle(embedder));
  }

  async llamaEmbedderBatchSize(embedder) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderBatchSize", await unwrapInteropHandle(embedder));
  }

  async llamaEmbedderThreads(embedder) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderThreads", await unwrapInteropHandle(embedder));
  }

  async llamaEmbedderDimensions(embedder) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderDimensions", await unwrapInteropHandle(embedder));
  }

  async llamaEmbedderEmbed(embedder, text) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderEmbed", await unwrapInteropHandle(embedder), text);
  }

  async llamaEmbedderEmbedAll(embedder, texts) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderEmbedAll", await unwrapInteropHandle(embedder), await toJava(texts));
  }

  async llamaEmbedderTokenCount(embedder, text) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderTokenCount", await unwrapInteropHandle(embedder), text);
  }

  async llamaEmbedderTokenize(embedder, text) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderTokenize", await unwrapInteropHandle(embedder), text);
  }

  async llamaEmbedderDetokenize(embedder, tokens) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaEmbedderDetokenize", await unwrapInteropHandle(embedder), await toJava(tokens));
  }

  async llamaEmbedderTruncateText(embedder, text, maxTokens) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "llamaEmbedderTruncateText",
      await unwrapInteropHandle(embedder),
      text,
      Number(maxTokens)
    );
  }

  async newLlamaGenerator(modelPath, { gpuLayers = 0, ctxSize = 0, threads = 0 } = {}) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "newLlamaGenerator",
      modelPath,
      Number(gpuLayers),
      Number(ctxSize),
      Number(threads)
    );
  }

  async closeLlamaGenerator(generator) {
    const cls = await classes();
    await callJavaMethod(cls.interop, "closeLlamaGenerator", await unwrapInteropHandle(generator));
  }

  async llamaGeneratorClosed(generator) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaGeneratorClosed", await unwrapInteropHandle(generator));
  }

  async llamaGeneratorModelPath(generator) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaGeneratorModelPath", await unwrapInteropHandle(generator));
  }

  async llamaGeneratorGpuLayers(generator) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaGeneratorGpuLayers", await unwrapInteropHandle(generator));
  }

  async llamaGeneratorCtxSize(generator) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaGeneratorCtxSize", await unwrapInteropHandle(generator));
  }

  async llamaGeneratorContextSize(generator) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaGeneratorContextSize", await unwrapInteropHandle(generator));
  }

  async llamaGeneratorThreads(generator) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaGeneratorThreads", await unwrapInteropHandle(generator));
  }

  async llamaGeneratorTokenCount(generator, text) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "llamaGeneratorTokenCount", await unwrapInteropHandle(generator), text);
  }

  async llamaGeneratorGenerate(generator, prompt, maxTokens) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "llamaGeneratorGenerate",
      await unwrapInteropHandle(generator),
      prompt,
      Number(maxTokens)
    );
  }

  async llamaGeneratorSummarize(generator, text, maxTokens) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "llamaGeneratorSummarize",
      await unwrapInteropHandle(generator),
      text,
      Number(maxTokens)
    );
  }

  async newClient(uri, opts = null) {
    const cls = await classes();
    if (opts !== null && opts !== undefined) {
      return callJavaMethod(cls.datalevin, "newClient", uri, await toJava(opts));
    }
    return callJavaMethod(cls.datalevin, "newClient", uri);
  }

  async closeClient(handle) {
    await callJavaMethod(handle, "close");
  }

  async clientDisconnected(handle) {
    return Boolean(await callJavaMethod(handle, "disconnected"));
  }

  async bridgeResult(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "bridgeResult", value);
  }

  async readEdn(edn) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "readEdn", edn);
  }

  async writeEdn(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "writeEdn", await toJava(value));
  }

  async keyword(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "keyword", value);
  }

  async symbol(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "symbol", value);
  }

  async schema(schema) {
    if (schema === null || schema === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "schema", await toJava(schema));
  }

  async options(opts) {
    if (opts === null || opts === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "options", await toJava(opts));
  }

  async udfDescriptor(descriptor) {
    if (descriptor === null || descriptor === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "udfDescriptor", await toJava(descriptor));
  }

  async createUdfRegistry() {
    const cls = await classes();
    return callJavaMethod(cls.interop, "createUdfRegistry");
  }

  async registerUdf(registry, descriptor, fn) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "registerUdf", registry, await toJava(descriptor), fn);
  }

  async unregisterUdf(registry, descriptor) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "unregisterUdf", registry, await toJava(descriptor));
  }

  async registeredUdf(registry, descriptor) {
    const cls = await classes();
    return Boolean(
      await callJavaMethod(cls.interop, "registeredUdf", registry, await toJava(descriptor))
    );
  }

  async renameMap(renameMap) {
    if (renameMap === null || renameMap === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "renameMap", await toJava(renameMap));
  }

  async deleteAttrs(attrs) {
    if (attrs === null || attrs === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "deleteAttrs", await toJava([...attrs]));
  }

  async lookupRef(value) {
    if (value === null || value === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "lookupRef", await toJava(value));
  }

  async datom(e, attr, value, tx = undefined, added = undefined) {
    const cls = await classes();
    if (added !== undefined) {
      if (tx === undefined) {
        throw new TypeError("tx is required when added is provided");
      }
      return callJavaMethod(
        cls.datalevin,
        "datom",
        await toJava(e),
        await toJava(attr),
        await toJava(value),
        await toJava(tx),
        await toJava(added)
      );
    }
    if (tx !== undefined) {
      return callJavaMethod(
        cls.datalevin,
        "datom",
        await toJava(e),
        await toJava(attr),
        await toJava(value),
        await toJava(tx)
      );
    }
    return callJavaMethod(cls.datalevin, "datom", await toJava(e), await toJava(attr), await toJava(value));
  }

  async txData(txData) {
    if (txData === null || txData === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "txData", await toJava(txData));
  }

  async kvTxs(txs) {
    if (txs === null || txs === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "kvTxs", await toJava(txs));
  }

  async kvTxsWithTypes(txs, kType = null, vType = null) {
    if (txs === null || txs === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvTxs",
      await toJava(txs),
      await toJava(kType),
      await toJava(vType)
    );
  }

  async kvInput(value, type) {
    if (value === null || value === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "kvInput", await toJava(value), await toJava(type));
  }

  async kvRange(range, type) {
    if (range === null || range === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "kvRange", await toEdnForm(range), await toJava(type));
  }

  async kvType(value) {
    if (value === null || value === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "kvType", await toJava(value));
  }

  async databaseType(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "databaseType", value);
  }

  async role(role) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "role", role);
  }

  async permissionKeyword(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "permissionKeyword", value);
  }

  async permissionTarget(objectType, target) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "permissionTarget", objectType, await toJava(target));
  }
}

export const _BINDINGS = new InteropBindings();

export async function apiInfo() {
  return toJs(await _BINDINGS.apiInfoRaw());
}

export async function execJson(op, args = null) {
  return toJs(await _BINDINGS.execJson(op, args));
}

export async function connect(dir = null, { schema = null, opts = null, shared = false } = {}) {
  const { Connection } = await import("./connection.js");
  return new Connection(await _BINDINGS.createConnection(dir, schema, opts, { shared }));
}

export async function initDb(datoms, { dir = null, schema = null, opts = null } = {}) {
  const { Connection } = await import("./connection.js");
  return new Connection(await _BINDINGS.initDb(datoms, dir, schema, opts));
}

export async function fillDb(conn, datoms) {
  await _BINDINGS.fillDb(conn, datoms);
  return conn;
}

export async function transact(conn, txData, txMeta = null) {
  return conn.transact(txData, txMeta);
}

export async function transactAsync(conn, txData, txMeta = null) {
  return conn.transactAsync(txData, txMeta);
}

export async function abortTransact(conn) {
  return conn.abortTransact();
}

export async function txDataToSimulatedReport(conn, txData) {
  return conn.txDataToSimulatedReport(txData);
}

export async function datalogKv(conn) {
  return conn.datalogKv();
}

export async function maxEid(conn) {
  return conn.maxEid();
}

export async function cardinality(conn, attr) {
  return conn.cardinality(attr);
}

export async function analyze(conn, attr = null) {
  return conn.analyze(attr);
}

export async function explicitTransactionTimeout(timeoutMs = undefined) {
  const args = hasValue(timeoutMs) ? [timeoutMs] : [];
  return toJsResult(await _BINDINGS.coreInvoke("explicit-transaction-timeout", args));
}

export async function setExplicitTransactionTimeout(timeoutMs) {
  return toJsResult(
    await _BINDINGS.coreInvoke("set-explicit-transaction-timeout!", [timeoutMs])
  );
}

export async function reIndex(target, opts = null, options = {}) {
  if (typeof target?.reIndex !== "function") {
    throw new TypeError("target must provide reIndex().");
  }
  return target.reIndex(opts, options);
}

export async function withTransaction(target, fnOrOptions, maybeFnOrOptions = null) {
  if (target?.constructor?.name === "Connection" &&
      typeof target.rawHandle === "function" &&
      typeof target.transact === "function" &&
      typeof target.query === "function") {
    throw new DatalevinError(CONNECTION_WITH_TRANSACTION_UNSUPPORTED);
  }
  if (typeof target?.withTransaction !== "function") {
    throw new TypeError("target must provide withTransaction().");
  }
  return target.withTransaction(fnOrOptions, maybeFnOrOptions);
}

export async function keyword(value) {
  return _BINDINGS.keyword(value);
}

export async function symbol(value) {
  return _BINDINGS.symbol(value);
}

export async function readEdn(edn) {
  return _BINDINGS.readEdn(edn);
}

export async function writeEdn(value) {
  return _BINDINGS.writeEdn(await toEdnForm(value));
}

export function schemaAttr({
  valueType = null,
  cardinality = null,
  unique = null,
  index = null,
  fulltext = null,
  isComponent = null,
  noHistory = null,
  doc = null,
  tupleType = null,
  tupleTypes = null,
  tupleAttrs = null,
  attrPreds = null,
  fulltextDomains = null,
  fulltextAutoDomain = null,
  embedding = null,
  embeddingDomains = null,
  embeddingAutoDomain = null,
  vectorDomains = null,
  idocFormat = null,
  idocDomain = null,
  idocIndexedPaths = null,
  idocExcludedPaths = null,
  extra = null,
  ...props
} = {}) {
  const spec = { ...props };
  if (valueType !== null && valueType !== undefined) {
    spec[":db/valueType"] = valueType;
  }
  if (cardinality !== null && cardinality !== undefined) {
    spec[":db/cardinality"] = cardinality;
  }
  if (unique !== null && unique !== undefined) {
    spec[":db/unique"] = unique;
  }
  if (index !== null && index !== undefined) {
    spec[":db/index"] = Boolean(index);
  }
  if (fulltext !== null && fulltext !== undefined) {
    spec[":db/fulltext"] = Boolean(fulltext);
  }
  if (isComponent !== null && isComponent !== undefined) {
    spec[":db/isComponent"] = Boolean(isComponent);
  }
  if (noHistory !== null && noHistory !== undefined) {
    spec[":db/noHistory"] = Boolean(noHistory);
  }
  if (doc !== null && doc !== undefined) {
    spec[":db/doc"] = doc;
  }
  if (tupleType !== null && tupleType !== undefined) {
    spec[":db/tupleType"] = tupleType;
  }
  if (tupleTypes !== null && tupleTypes !== undefined) {
    spec[":db/tupleTypes"] = [...tupleTypes];
  }
  if (tupleAttrs !== null && tupleAttrs !== undefined) {
    spec[":db/tupleAttrs"] = [...tupleAttrs];
  }
  if (attrPreds !== null && attrPreds !== undefined) {
    spec[":db.attr/preds"] = attrPreds;
  }
  if (fulltextDomains !== null && fulltextDomains !== undefined) {
    spec[":db.fulltext/domains"] = [...fulltextDomains];
  }
  if (fulltextAutoDomain !== null && fulltextAutoDomain !== undefined) {
    spec[":db.fulltext/autoDomain"] = Boolean(fulltextAutoDomain);
  }
  if (embedding !== null && embedding !== undefined) {
    spec[":db/embedding"] = Boolean(embedding);
  }
  if (embeddingDomains !== null && embeddingDomains !== undefined) {
    spec[":db.embedding/domains"] = [...embeddingDomains];
  }
  if (embeddingAutoDomain !== null && embeddingAutoDomain !== undefined) {
    spec[":db.embedding/autoDomain"] = Boolean(embeddingAutoDomain);
  }
  if (vectorDomains !== null && vectorDomains !== undefined) {
    spec[":db.vec/domains"] = [...vectorDomains];
  }
  if (idocFormat !== null && idocFormat !== undefined) {
    spec[":db/idocFormat"] = keywordValue(idocFormat);
  }
  if (idocDomain !== null && idocDomain !== undefined) {
    spec[":db/domain"] = idocDomain;
  }
  if (idocIndexedPaths !== null && idocIndexedPaths !== undefined) {
    spec[":db.idoc/indexedPaths"] = [...idocIndexedPaths];
  }
  if (idocExcludedPaths !== null && idocExcludedPaths !== undefined) {
    spec[":db.idoc/excludedPaths"] = [...idocExcludedPaths];
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(spec, extra);
  }
  return spec;
}

export function fulltextAttr({
  valueType = ":db.type/string",
  domains = null,
  autoDomain = null,
  extra = null,
  ...schemaProps
} = {}) {
  const merged = { ":db/fulltext": true };
  if (domains !== null && domains !== undefined) {
    merged[":db.fulltext/domains"] = [...domains];
  }
  if (autoDomain !== null && autoDomain !== undefined) {
    merged[":db.fulltext/autoDomain"] = Boolean(autoDomain);
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(merged, extra);
  }
  return schemaAttr({ valueType, extra: merged, ...schemaProps });
}

export function embeddingAttr({
  valueType = ":db.type/string",
  domains = null,
  autoDomain = null,
  extra = null,
  ...schemaProps
} = {}) {
  const merged = { ":db/embedding": true };
  if (domains !== null && domains !== undefined) {
    merged[":db.embedding/domains"] = [...domains];
  }
  if (autoDomain !== null && autoDomain !== undefined) {
    merged[":db.embedding/autoDomain"] = Boolean(autoDomain);
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(merged, extra);
  }
  return schemaAttr({ valueType, extra: merged, ...schemaProps });
}

export function vectorAttr({ domains = null, extra = null, ...schemaProps } = {}) {
  const merged = {};
  if (domains !== null && domains !== undefined) {
    merged[":db.vec/domains"] = [...domains];
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(merged, extra);
  }
  return schemaAttr({ valueType: ":db.type/vec", extra: merged, ...schemaProps });
}

export function idocAttr({
  format = null,
  domain = null,
  indexedPaths = null,
  excludedPaths = null,
  extra = null,
  ...schemaProps
} = {}) {
  const merged = {};
  if (format !== null && format !== undefined) {
    merged[":db/idocFormat"] = keywordValue(format);
  }
  if (domain !== null && domain !== undefined) {
    merged[":db/domain"] = domain;
  }
  if (indexedPaths !== null && indexedPaths !== undefined) {
    merged[":db.idoc/indexedPaths"] = [...indexedPaths];
  }
  if (excludedPaths !== null && excludedPaths !== undefined) {
    merged[":db.idoc/excludedPaths"] = [...excludedPaths];
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(merged, extra);
  }
  return schemaAttr({ valueType: ":db.type/idoc", extra: merged, ...schemaProps });
}

export function searchOptions({
  top = null,
  limit = null,
  offset = null,
  pagingCachePages = null,
  display = null,
  domains = null,
  proximityExpansion = null,
  proximityMaxDist = null,
  indexingMode = null,
  analyzer = null,
  queryAnalyzer = null,
  extra = null
} = {}) {
  const opts = {};
  if (top !== null && top !== undefined) {
    opts[":top"] = top;
  }
  if (limit !== null && limit !== undefined) {
    opts[":limit"] = limit;
  }
  if (offset !== null && offset !== undefined) {
    opts[":offset"] = offset;
  }
  if (pagingCachePages !== null && pagingCachePages !== undefined) {
    opts[":paging-cache-pages"] = pagingCachePages;
  }
  if (display !== null && display !== undefined) {
    opts[":display"] = keywordValue(display);
  }
  if (domains !== null && domains !== undefined) {
    opts[":domains"] = [...domains];
  }
  if (proximityExpansion !== null && proximityExpansion !== undefined) {
    opts[":proximity-expansion"] = proximityExpansion;
  }
  if (proximityMaxDist !== null && proximityMaxDist !== undefined) {
    opts[":proximity-max-dist"] = proximityMaxDist;
  }
  if (indexingMode !== null && indexingMode !== undefined) {
    opts[":indexing-mode"] = keywordValue(indexingMode);
  }
  if (analyzer !== null && analyzer !== undefined) {
    opts[":analyzer"] = analyzer;
  }
  if (queryAnalyzer !== null && queryAnalyzer !== undefined) {
    opts[":query-analyzer"] = queryAnalyzer;
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(opts, extra);
  }
  return opts;
}

export function searchDomain({
  domain = null,
  indexPosition = null,
  includeText = null,
  indexingMode = null,
  analyzer = null,
  queryAnalyzer = null,
  extra = null
} = {}) {
  const opts = {};
  if (domain !== null && domain !== undefined) {
    opts[":domain"] = domain;
  }
  if (indexPosition !== null && indexPosition !== undefined) {
    opts[":index-position?"] = Boolean(indexPosition);
  }
  if (includeText !== null && includeText !== undefined) {
    opts[":include-text?"] = Boolean(includeText);
  }
  if (indexingMode !== null && indexingMode !== undefined) {
    opts[":indexing-mode"] = keywordValue(indexingMode);
  }
  if (analyzer !== null && analyzer !== undefined) {
    opts[":analyzer"] = analyzer;
  }
  if (queryAnalyzer !== null && queryAnalyzer !== undefined) {
    opts[":query-analyzer"] = queryAnalyzer;
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(opts, extra);
  }
  return opts;
}

export function vectorOptions({
  dimensions = null,
  metricType = null,
  quantization = null,
  connectivity = null,
  expansionAdd = null,
  expansionSearch = null,
  domain = null,
  indexingMode = null,
  extra = null
} = {}) {
  const opts = {};
  if (dimensions !== null && dimensions !== undefined) {
    opts[":dimensions"] = dimensions;
  }
  if (metricType !== null && metricType !== undefined) {
    opts[":metric-type"] = keywordValue(metricType);
  }
  if (quantization !== null && quantization !== undefined) {
    opts[":quantization"] = keywordValue(quantization);
  }
  if (connectivity !== null && connectivity !== undefined) {
    opts[":connectivity"] = connectivity;
  }
  if (expansionAdd !== null && expansionAdd !== undefined) {
    opts[":expansion-add"] = expansionAdd;
  }
  if (expansionSearch !== null && expansionSearch !== undefined) {
    opts[":expansion-search"] = expansionSearch;
  }
  if (domain !== null && domain !== undefined) {
    opts[":domain"] = domain;
  }
  if (indexingMode !== null && indexingMode !== undefined) {
    opts[":indexing-mode"] = keywordValue(indexingMode);
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(opts, extra);
  }
  return opts;
}

export function embeddingOptions({
  provider = null,
  model = null,
  baseUrl = null,
  endpoint = null,
  apiKey = null,
  apiKeyEnv = null,
  headers = null,
  timeoutMs = null,
  queryPrefix = null,
  documentPrefix = null,
  requestDimensions = null,
  embeddingMetadata = null,
  dimensions = null,
  metricType = null,
  indexingMode = null,
  extra = null
} = {}) {
  const opts = {};
  if (provider !== null && provider !== undefined) {
    opts[":provider"] = keywordValue(provider);
  }
  if (model !== null && model !== undefined) {
    opts[":model"] = model;
  }
  if (baseUrl !== null && baseUrl !== undefined) {
    opts[":base-url"] = baseUrl;
  }
  if (endpoint !== null && endpoint !== undefined) {
    opts[":endpoint"] = endpoint;
  }
  if (apiKey !== null && apiKey !== undefined) {
    opts[":api-key"] = apiKey;
  }
  if (apiKeyEnv !== null && apiKeyEnv !== undefined) {
    opts[":api-key-env"] = apiKeyEnv;
  }
  if (headers !== null && headers !== undefined) {
    opts[":headers"] = { ...headers };
  }
  if (timeoutMs !== null && timeoutMs !== undefined) {
    opts[":timeout-ms"] = timeoutMs;
  }
  if (queryPrefix !== null && queryPrefix !== undefined) {
    opts[":query-prefix"] = queryPrefix;
  }
  if (documentPrefix !== null && documentPrefix !== undefined) {
    opts[":document-prefix"] = documentPrefix;
  }
  if (requestDimensions !== null && requestDimensions !== undefined) {
    opts[":request-dimensions"] = requestDimensions;
  }
  if (embeddingMetadata !== null && embeddingMetadata !== undefined) {
    opts[":embedding-metadata"] = { ...embeddingMetadata };
  }
  if (dimensions !== null && dimensions !== undefined) {
    opts[":dimensions"] = dimensions;
  }
  if (metricType !== null && metricType !== undefined) {
    opts[":metric-type"] = keywordValue(metricType);
  }
  if (indexingMode !== null && indexingMode !== undefined) {
    opts[":indexing-mode"] = keywordValue(indexingMode);
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(opts, extra);
  }
  return opts;
}

export function idocDomain({ indexedPaths = null, excludedPaths = null, extra = null } = {}) {
  const opts = {};
  if (indexedPaths !== null && indexedPaths !== undefined) {
    opts[":indexed-paths"] = [...indexedPaths];
  }
  if (excludedPaths !== null && excludedPaths !== undefined) {
    opts[":excluded-paths"] = [...excludedPaths];
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(opts, extra);
  }
  return opts;
}

export function idocOptions({ domains = null, extra = null } = {}) {
  const opts = {};
  if (domains !== null && domains !== undefined) {
    opts[":domains"] = [...domains];
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(opts, extra);
  }
  return opts;
}

export function txEntity(dbId = null, attrs = {}) {
  const entity = { ...attrs };
  if (dbId !== null && dbId !== undefined) {
    entity[":db/id"] = dbId;
  }
  return entity;
}

export function txAdd(entityId, attr, value) {
  return [":db/add", entityId, attrKey(attr), value];
}

export function txRetract(entityId, attr, value) {
  return [":db/retract", entityId, attrKey(attr), value];
}

export function txRetractEntity(entityId) {
  return [":db/retractEntity", entityId];
}

export function txEnsure(predicate, ...args) {
  return [":db/ensure", predicate, ...args];
}

export function datom(e, attr, value, tx = undefined, added = undefined) {
  if (added !== undefined && tx === undefined) {
    throw new TypeError("tx is required when added is provided");
  }
  if (added !== undefined) {
    return [e, attr, value, tx, added];
  }
  if (tx !== undefined) {
    return [e, attr, value, tx];
  }
  return [e, attr, value];
}

export function datomIs(value) {
  if (Array.isArray(value)) {
    return value.length >= 3 && value.length <= 5;
  }
  return value !== null
    && typeof value === "object"
    && datomMapValue(value, "e") !== null
    && datomMapValue(value, "a") !== null
    && datomMapValue(value, "v") !== null;
}

export function datomE(value) {
  return datomField(value, "e", 0);
}

export function datomA(value) {
  return datomField(value, "a", 1);
}

export function datomV(value) {
  return datomField(value, "v", 2);
}

export function datomTx(value) {
  return datomField(value, "tx", 3);
}

export function datomAdded(value) {
  return datomField(value, "added", 4);
}

export async function openKv(dir, opts = null) {
  const { KV } = await import("./kv.js");
  return new KV(await _BINDINGS.openKeyValue(dir, opts));
}

export async function newSearchEngine(kv, opts = null) {
  const { SearchEngine } = await import("./search.js");
  return new SearchEngine(await _BINDINGS.newSearchEngine(kv, opts));
}

export async function newVectorIndex(kv, opts) {
  const { VectorIndex } = await import("./vector.js");
  return new VectorIndex(await _BINDINGS.newVectorIndex(kv, opts));
}

export async function newLlamaEmbedder(modelPath, opts = {}) {
  const { LlamaEmbedder } = await import("./llm.js");
  return new LlamaEmbedder(await _BINDINGS.newLlamaEmbedder(modelPath, opts));
}

export async function newLlamaGenerator(modelPath, opts = {}) {
  const { LlamaGenerator } = await import("./llm.js");
  return new LlamaGenerator(await _BINDINGS.newLlamaGenerator(modelPath, opts));
}

export async function searchIndexWriter(kv, opts = null) {
  const { SearchIndexWriter } = await import("./search.js");
  return new SearchIndexWriter(await _BINDINGS.searchIndexWriter(kv, opts));
}

export async function newClient(uri, opts = null) {
  const { Client } = await import("./client.js");
  return new Client(await _BINDINGS.newClient(uri, opts));
}

export { jvmStarted, startJvm, toEdnForm, toJava, toJs, toQueryInput };

function attrKey(attr) {
  return typeof attr === "string" && attr.startsWith(":") ? attr : `:${attr}`;
}

function keywordValue(value) {
  const text = String(value);
  return text.startsWith(":") ? text : `:${text}`;
}
