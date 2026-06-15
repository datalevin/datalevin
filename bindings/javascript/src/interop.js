import { toEdnForm, toJava, toJs, toQueryInput } from "./convert.js";
import { callJavaMethod, classes, javaBridgeModule, jvmStarted, startJvm } from "./jvm.js";

async function unwrapInteropHandle(value) {
  if (typeof value?.rawHandle === "function") {
    return value.rawHandle();
  }
  return value;
}

function hasValue(value) {
  return value !== null && value !== undefined;
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
    return callJavaMethod(cls.interop, "connectionDb", handle);
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

  async connectionWithTransaction(handle, fn) {
    const cls = await classes();
    const proxy = await createFunctionProxy(fn);
    try {
      return await withInterfaceProxyEventLoop(async () => (
        callJavaMethod(
          cls.interop,
          "connectionWithTransaction",
          await unwrapInteropHandle(handle),
          proxy
        )
      ));
    } finally {
      proxy.reset?.();
    }
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

export async function transactAsync(conn, txData, txMeta = null) {
  return conn.transactAsync(txData, txMeta);
}

export async function datalogKv(conn) {
  return conn.datalogKv();
}

export async function reIndex(target, opts = null, options = {}) {
  if (typeof target?.reIndex !== "function") {
    throw new TypeError("target must provide reIndex().");
  }
  return target.reIndex(opts, options);
}

export async function withTransaction(target, fn) {
  if (typeof target?.withTransaction !== "function") {
    throw new TypeError("target must provide withTransaction().");
  }
  return target.withTransaction(fn);
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

export function idocAttr({ format = null, domain = null, extra = null, ...schemaProps } = {}) {
  const merged = {};
  if (format !== null && format !== undefined) {
    merged[":db/idocFormat"] = keywordValue(format);
  }
  if (domain !== null && domain !== undefined) {
    merged[":db/domain"] = domain;
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(merged, extra);
  }
  return schemaAttr({ valueType: ":db.type/idoc", extra: merged, ...schemaProps });
}

export function searchOptions({
  top = null,
  display = null,
  domains = null,
  proximityExpansion = null,
  proximityMaxDist = null,
  indexingMode = null,
  extra = null
} = {}) {
  const opts = {};
  if (top !== null && top !== undefined) {
    opts[":top"] = top;
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
  apiKeyEnv = null,
  requestDimensions = null,
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
  if (apiKeyEnv !== null && apiKeyEnv !== undefined) {
    opts[":api-key-env"] = apiKeyEnv;
  }
  if (requestDimensions !== null && requestDimensions !== undefined) {
    opts[":request-dimensions"] = requestDimensions;
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

export async function openKv(dir, opts = null) {
  const { KV } = await import("./kv.js");
  return new KV(await _BINDINGS.openKeyValue(dir, opts));
}

export async function newSearchEngine(kv, opts = null) {
  const { SearchEngine } = await import("./search.js");
  return new SearchEngine(await _BINDINGS.newSearchEngine(kv, opts));
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
