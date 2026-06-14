import { DatalevinError } from "./errors.js";
import { _BINDINGS } from "./interop.js";

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
    throw new DatalevinError(
      "Raw database values are not exposed by the Node binding. Use Connection query/pull/entity methods instead."
    );
  }

  async connectionCopy(handle, dest, compact = null) {
    return _BINDINGS.connectionCopy(resourceHandle(handle), dest, compact);
  }

  async connectionTxLogWatermarks(handle) {
    return _BINDINGS.connectionTxLogWatermarks(resourceHandle(handle));
  }

  async connectionOpenTxLog(handle, fromLsn, uptoLsn = null) {
    return _BINDINGS.connectionOpenTxLog(resourceHandle(handle), fromLsn, uptoLsn);
  }

  async connectionCreateSnapshot(handle) {
    return _BINDINGS.connectionCreateSnapshot(resourceHandle(handle));
  }

  async connectionListSnapshots(handle) {
    return _BINDINGS.connectionListSnapshots(resourceHandle(handle));
  }

  async connectionGcTxLogSegments(handle, retainFloorLsn = null) {
    return _BINDINGS.connectionGcTxLogSegments(resourceHandle(handle), retainFloorLsn);
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
