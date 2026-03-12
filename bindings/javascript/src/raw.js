import { DatalevinError } from "./errors.js";
import { _BINDINGS } from "./interop.js";

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

  async txData(txData) {
    return _BINDINGS.txData(txData);
  }

  async kvTxs(txs) {
    return _BINDINGS.kvTxs(txs);
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
