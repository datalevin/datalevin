import { CONNECTION_WITH_TRANSACTION_UNSUPPORTED, _BINDINGS } from "./interop.js";
import { toJava } from "./convert.js";
import { callJavaMethod } from "./jvm.js";
import { toJsResult } from "./result.js";

function datomTuple(e, attr, value, tx = undefined, added = undefined) {
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

function resourceHandle(value) {
  if (typeof value?.rawHandle === "function") {
    return value.rawHandle();
  }
  return value;
}

function hasValue(value) {
  return value !== null && value !== undefined;
}

class RawInterop {
  async apiInfo() {
    return _BINDINGS.apiInfoRaw();
  }

  async execJson(op, args = null) {
    return _BINDINGS.execJson(op, args);
  }

  async coreInvoke(functionName, args = null) {
    return _BINDINGS.coreInvoke(functionName, args);
  }

  async clientInvoke(functionName, args = null) {
    return _BINDINGS.clientInvoke(functionName, args);
  }

  async createConnection(dir = null, schema = null, opts = null, { shared = false } = {}) {
    return _BINDINGS.createConnection(dir, schema, opts, { shared });
  }

  async initDb(datoms, dir = null, schema = null, opts = null) {
    return _BINDINGS.initDb(datoms, dir, schema, opts);
  }

  async fillDb(conn, datoms) {
    return _BINDINGS.fillDb(conn, datoms);
  }

  async closeConnection(handle) {
    await _BINDINGS.closeConnection(resourceHandle(handle));
  }

  async connectionClosed(handle) {
    return _BINDINGS.connectionClosed(resourceHandle(handle));
  }

  async connectionDb(handle) {
    const { Database } = await import("./database.js");
    return new Database(await _BINDINGS.connectionDb(resourceHandle(handle)));
  }

  async connectionEntity(handle, eid) {
    return _BINDINGS.connectionEntity(resourceHandle(handle), eid);
  }

  async databaseEntid(db, eid) {
    return _BINDINGS.databaseEntid(resourceHandle(db), eid);
  }

  async databaseEntity(db, eid) {
    return _BINDINGS.databaseEntity(resourceHandle(db), eid);
  }

  async databaseEntityMap(db, eid) {
    return toJsResult(await _BINDINGS.databaseEntityMap(resourceHandle(db), eid), { bridge: true });
  }

  async databasePull(db, selector, eid) {
    return toJsResult(await _BINDINGS.databasePull(resourceHandle(db), selector, eid), { bridge: true });
  }

  async databasePullMany(db, selector, eids) {
    return toJsResult(await _BINDINGS.databasePullMany(resourceHandle(db), selector, eids), { bridge: true });
  }

  async connectionTxDataToSimulatedReport(handle, txData) {
    return _BINDINGS.connectionTxDataToSimulatedReport(resourceHandle(handle), txData);
  }

  async entityIs(value) {
    return _BINDINGS.entityIs(value);
  }

  async entityId(entity) {
    return _BINDINGS.entityId(resourceHandle(entity));
  }

  async entityGet(entity, attr) {
    return _BINDINGS.entityGet(resourceHandle(entity), attr);
  }

  async entityContains(entity, attr) {
    return _BINDINGS.entityContains(resourceHandle(entity), attr);
  }

  async entityTouch(entity) {
    return toJsResult(await _BINDINGS.entityTouch(resourceHandle(entity)), { bridge: true });
  }

  async connectionDatalogKv(handle) {
    return callJavaMethod(resourceHandle(handle), "datalogKV");
  }

  async connectionTransactAsync(handle, txData, txMeta = null) {
    const future = await _BINDINGS.connectionTransactAsync(resourceHandle(handle), txData, txMeta);
    return toJsResult(await callJavaMethod(future, "get"), { bridge: true });
  }

  async connectionDatoms(handle, index, c1 = null, c2 = null, c3 = null, limit = null) {
    const args = [resourceHandle(handle), "datoms", await toJava(index), await toJava(c1), await toJava(c2), await toJava(c3)];
    if (hasValue(limit)) {
      args.push(await toJava(limit));
    }
    return toJsResult(await callJavaMethod(...args), { bridge: true });
  }

  async connectionSearchDatoms(handle, e = null, attr = null, value = null) {
    return toJsResult(
      await callJavaMethod(
        resourceHandle(handle),
        "searchDatoms",
        await toJava(e),
        await toJava(attr),
        await toJava(value)
      ),
      { bridge: true }
    );
  }

  async connectionCountDatoms(handle, e = null, attr = null, value = null) {
    return toJsResult(
      await callJavaMethod(
        resourceHandle(handle),
        "countDatoms",
        await toJava(e),
        await toJava(attr),
        await toJava(value)
      )
    );
  }

  async connectionSeekDatoms(handle, index, c1 = null, c2 = null, c3 = null, limit = null) {
    const args = [
      resourceHandle(handle),
      "seekDatoms",
      await toJava(index),
      await toJava(c1),
      await toJava(c2),
      await toJava(c3)
    ];
    if (hasValue(limit)) {
      args.push(await toJava(limit));
    }
    return toJsResult(await callJavaMethod(...args), { bridge: true });
  }

  async connectionRseekDatoms(handle, index, c1 = null, c2 = null, c3 = null, limit = null) {
    const args = [
      resourceHandle(handle),
      "rseekDatoms",
      await toJava(index),
      await toJava(c1),
      await toJava(c2),
      await toJava(c3)
    ];
    if (hasValue(limit)) {
      args.push(await toJava(limit));
    }
    return toJsResult(await callJavaMethod(...args), { bridge: true });
  }

  async connectionIndexRange(handle, attr, start, end) {
    return toJsResult(
      await callJavaMethod(
        resourceHandle(handle),
        "indexRange",
        await toJava(attr),
        await toJava(start),
        await toJava(end)
      ),
      { bridge: true }
    );
  }

  async connectionFulltextDatoms(handle, query, opts = null) {
    const result = hasValue(opts)
      ? await callJavaMethod(resourceHandle(handle), "fulltextDatoms", query, await toJava(opts))
      : await callJavaMethod(resourceHandle(handle), "fulltextDatoms", query);
    return toJsResult(result, { bridge: true });
  }

  async connectionCopy(handle, dest, compact = null) {
    if (hasValue(compact)) {
      return callJavaMethod(resourceHandle(handle), "copy", dest, Boolean(compact));
    }
    return callJavaMethod(resourceHandle(handle), "copy", dest);
  }

  async connectionTxLogWatermarks(handle) {
    return toJsResult(await callJavaMethod(resourceHandle(handle), "txLogWatermarks"), { bridge: true });
  }

  async connectionOpenTxLog(handle, fromLsn, uptoLsn = null) {
    const result = hasValue(uptoLsn)
      ? await callJavaMethod(resourceHandle(handle), "openTxLog", await toJava(fromLsn), await toJava(uptoLsn))
      : await callJavaMethod(resourceHandle(handle), "openTxLog", await toJava(fromLsn));
    return toJsResult(result, { bridge: true });
  }

  async connectionCreateSnapshot(handle) {
    return toJsResult(await callJavaMethod(resourceHandle(handle), "createSnapshot"), { bridge: true });
  }

  async connectionListSnapshots(handle) {
    return toJsResult(await callJavaMethod(resourceHandle(handle), "listSnapshots"), { bridge: true });
  }

  async connectionGcTxLogSegments(handle, retainFloorLsn = null) {
    if (hasValue(retainFloorLsn)) {
      return toJsResult(
        await callJavaMethod(resourceHandle(handle), "gcTxLogSegments", await toJava(retainFloorLsn)),
        { bridge: true }
      );
    }
    return toJsResult(await callJavaMethod(resourceHandle(handle), "gcTxLogSegments"), { bridge: true });
  }

  async connectionWithTransaction(handle, fn) {
    throw new DatalevinError(CONNECTION_WITH_TRANSACTION_UNSUPPORTED);
  }

  async connectionReIndex(handle, schema = null, opts = null) {
    return _BINDINGS.connectionReIndex(resourceHandle(handle), schema, opts);
  }

  async openKeyValue(dir, opts = null) {
    return _BINDINGS.openKeyValue(dir, opts);
  }

  async closeKeyValue(handle) {
    await _BINDINGS.closeKeyValue(resourceHandle(handle));
  }

  async keyValueClosed(handle) {
    return _BINDINGS.keyValueClosed(resourceHandle(handle));
  }

  async keyValueBeginTransaction(handle) {
    return _BINDINGS.keyValueBeginTransaction(resourceHandle(handle));
  }

  async keyValueCommitTransaction(tx) {
    return toJsResult(await _BINDINGS.keyValueCommitTransaction(resourceHandle(tx)));
  }

  async keyValueAbortTransaction(tx) {
    return toJsResult(await _BINDINGS.keyValueAbortTransaction(resourceHandle(tx)));
  }

  async keyValueWithTransaction(handle, fn) {
    return toJsResult(await _BINDINGS.keyValueWithTransaction(resourceHandle(handle), fn), { bridge: true });
  }

  async keyValueReIndex(handle, opts = null) {
    return _BINDINGS.keyValueReIndex(resourceHandle(handle), opts);
  }

  async newSearchEngine(kv, opts = null) {
    return _BINDINGS.newSearchEngine(resourceHandle(kv), opts);
  }

  async searchAddDoc(search, docRef, docText, checkExist = null) {
    return _BINDINGS.searchAddDoc(resourceHandle(search), docRef, docText, checkExist);
  }

  async searchRemoveDoc(search, docRef) {
    return _BINDINGS.searchRemoveDoc(resourceHandle(search), docRef);
  }

  async searchClearDocs(search) {
    return _BINDINGS.searchClearDocs(resourceHandle(search));
  }

  async searchDocIndexed(search, docRef) {
    return _BINDINGS.searchDocIndexed(resourceHandle(search), docRef);
  }

  async searchDocCount(search) {
    return toJsResult(await _BINDINGS.searchDocCount(resourceHandle(search)));
  }

  async search(search, query, opts = null) {
    return toJsResult(await _BINDINGS.search(resourceHandle(search), query, opts));
  }

  async searchReIndex(search, opts = null) {
    return _BINDINGS.searchReIndex(resourceHandle(search), opts);
  }

  async searchIndexWriter(kv, opts = null) {
    return _BINDINGS.searchIndexWriter(resourceHandle(kv), opts);
  }

  async searchWrite(writer, docRef, docText) {
    return _BINDINGS.searchWrite(resourceHandle(writer), docRef, docText);
  }

  async searchCommit(writer) {
    return toJsResult(await _BINDINGS.searchCommit(resourceHandle(writer)));
  }

  async newVectorIndex(kv, opts) {
    return _BINDINGS.newVectorIndex(resourceHandle(kv), opts);
  }

  async closeVectorIndex(index) {
    await _BINDINGS.closeVectorIndex(resourceHandle(index));
  }

  async vectorIndexClosed(index) {
    return _BINDINGS.vectorIndexClosed(resourceHandle(index));
  }

  async vectorAddVec(index, vecRef, vecData) {
    return toJsResult(await _BINDINGS.vectorAddVec(resourceHandle(index), vecRef, vecData));
  }

  async vectorRemoveVec(index, vecRef) {
    return toJsResult(await _BINDINGS.vectorRemoveVec(resourceHandle(index), vecRef));
  }

  async vectorIndexed(index, vecRef) {
    return _BINDINGS.vectorIndexed(resourceHandle(index), vecRef);
  }

  async vectorSearch(index, queryVec, opts = null) {
    return toJsResult(await _BINDINGS.vectorSearch(resourceHandle(index), queryVec, opts));
  }

  async vectorReIndex(index, opts = null) {
    return _BINDINGS.vectorReIndex(resourceHandle(index), opts);
  }

  async vectorClear(index) {
    return toJsResult(await _BINDINGS.vectorClear(resourceHandle(index)));
  }

  async vectorForceCheckpoint(index) {
    return toJsResult(await _BINDINGS.vectorForceCheckpoint(resourceHandle(index)));
  }

  async vectorInfo(index) {
    return toJsResult(await _BINDINGS.vectorInfo(resourceHandle(index)));
  }

  async vectorCheckpointState(index) {
    return toJsResult(await _BINDINGS.vectorCheckpointState(resourceHandle(index)));
  }

  async newClient(uri, opts = null) {
    return _BINDINGS.newClient(uri, opts);
  }

  async closeClient(handle) {
    await _BINDINGS.closeClient(resourceHandle(handle));
  }

  async clientDisconnected(handle) {
    return _BINDINGS.clientDisconnected(resourceHandle(handle));
  }

  async readEdn(edn) {
    return _BINDINGS.readEdn(edn);
  }

  async writeEdn(value) {
    return _BINDINGS.writeEdn(value);
  }

  async keyword(value) {
    return _BINDINGS.keyword(value);
  }

  async symbol(value) {
    return _BINDINGS.symbol(value);
  }

  async schema(schema) {
    return _BINDINGS.schema(schema);
  }

  async options(opts) {
    return _BINDINGS.options(opts);
  }

  async udfDescriptor(descriptor) {
    return _BINDINGS.udfDescriptor(descriptor);
  }

  async createUdfRegistry() {
    return _BINDINGS.createUdfRegistry();
  }

  async registerUdf(registry, descriptor, fn) {
    return _BINDINGS.registerUdf(registry, descriptor, fn);
  }

  async unregisterUdf(registry, descriptor) {
    return _BINDINGS.unregisterUdf(registry, descriptor);
  }

  async registeredUdf(registry, descriptor) {
    return _BINDINGS.registeredUdf(registry, descriptor);
  }

  async renameMap(renameMap) {
    return _BINDINGS.renameMap(renameMap);
  }

  async deleteAttrs(attrs) {
    return _BINDINGS.deleteAttrs(attrs);
  }

  async lookupRef(value) {
    return _BINDINGS.lookupRef(value);
  }

  async datom(e, attr, value, tx = undefined, added = undefined) {
    return datomTuple(e, attr, value, tx, added);
  }

  async txData(txData) {
    return _BINDINGS.txData(txData);
  }

  async kvTxs(txs, kType = null, vType = null) {
    if (kType === null && vType === null) {
      return _BINDINGS.kvTxs(txs);
    }
    return _BINDINGS.kvTxsWithTypes(txs, kType, vType);
  }

  async kvInput(value, type) {
    return _BINDINGS.kvInput(value, type);
  }

  async kvRange(range, type) {
    return _BINDINGS.kvRange(range, type);
  }

  async kvType(value) {
    return _BINDINGS.kvType(value);
  }

  async databaseType(value) {
    return _BINDINGS.databaseType(value);
  }

  async role(role) {
    return _BINDINGS.role(role);
  }

  async permissionKeyword(value) {
    return _BINDINGS.permissionKeyword(value);
  }

  async permissionTarget(objectType, target) {
    return _BINDINGS.permissionTarget(objectType, target);
  }
}

const RAW_INTEROP = new RawInterop();

export function interop() {
  return RAW_INTEROP;
}
