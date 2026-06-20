import { toEdnForm, toJava, toJs, toQueryInput } from "./convert.js";
import { _BINDINGS } from "./interop.js";
import { callJavaMethod, javaBridgeModule } from "./jvm.js";
import { ResourceWrapper } from "./resource.js";
import { toJsResult } from "./result.js";

async function pullSelector(value) {
  if (typeof value === "string") {
    return value;
  }
  return toEdnForm(value);
}

async function queryForm(query) {
  if (typeof query !== "string") {
    return toEdnForm(query);
  }
  return query;
}

function hasValue(value) {
  return value !== null && value !== undefined;
}

function sliceRows(rows, { limit = null, offset = 0 } = {}) {
  const start = Math.max(offset ?? 0, 0);
  if (!hasValue(limit)) {
    return start === 0 ? rows : rows.slice(start);
  }
  return rows.slice(start, start + Math.max(limit, 0));
}

function fetchLimit(limit, offset = 0) {
  if (!hasValue(limit)) {
    return null;
  }
  return Math.max(limit, 0) + Math.max(offset ?? 0, 0);
}

let listenerProxyEventLoopDepth = 0;
let listenerProxyEventLoopPrevious = false;

async function retainListenerProxyEventLoop() {
  const bridge = await javaBridgeModule();
  if (listenerProxyEventLoopDepth === 0) {
    listenerProxyEventLoopPrevious = bridge.config.runEventLoopWhenInterfaceProxyIsActive;
    bridge.config.runEventLoopWhenInterfaceProxyIsActive = true;
  }
  listenerProxyEventLoopDepth += 1;
  return () => {
    listenerProxyEventLoopDepth -= 1;
    if (listenerProxyEventLoopDepth === 0) {
      bridge.config.runEventLoopWhenInterfaceProxyIsActive = listenerProxyEventLoopPrevious;
    }
  };
}

async function createConsumerProxy(fn) {
  if (typeof fn !== "function") {
    throw new TypeError("callback must be a function.");
  }
  const { newProxy } = await javaBridgeModule();
  return newProxy("java.util.function.Consumer", {
    accept: async (value) => {
      await fn(await toJs(value));
    }
  });
}

export class Connection extends ResourceWrapper {
  constructor(handle, { owned = true } = {}) {
    super(
      handle,
      owned ? (rawHandle) => _BINDINGS.closeConnection(rawHandle) : async () => {},
      (rawHandle) => _BINDINGS.connectionClosed(rawHandle),
      "connection"
    );
    this._listenerProxies = new Map();
  }

  async close() {
    this._resetListenerProxies();
    await super.close();
  }

  _resetListenerProxies() {
    for (const { proxy, release } of this._listenerProxies.values()) {
      proxy.reset?.();
      release?.();
    }
    this._listenerProxies.clear();
  }

  async schema() {
    return toJsResult(await _BINDINGS.coreInvoke("schema", [this.rawHandle()]));
  }

  async opts() {
    return toJsResult(await _BINDINGS.coreInvoke("opts", [this.rawHandle()]));
  }

  async datalogIndexCacheLimit(limit = null) {
    if (hasValue(limit)) {
      return toJsResult(
        await callJavaMethod(this.rawHandle(), "datalogIndexCacheLimit", await toJava(limit))
      );
    }
    return toJsResult(await callJavaMethod(this.rawHandle(), "datalogIndexCacheLimit"));
  }

  async updateSchema(schemaUpdate, { delAttrs = null, renameMap = null } = {}) {
    const args = [
      this.rawHandle(),
      schemaUpdate === null || schemaUpdate === undefined ? null : await _BINDINGS.schema(schemaUpdate)
    ];

    if (renameMap !== null && renameMap !== undefined) {
      args.push(await _BINDINGS.deleteAttrs(delAttrs));
      args.push(await _BINDINGS.renameMap(renameMap));
    } else if (delAttrs !== null && delAttrs !== undefined) {
      args.push(await _BINDINGS.deleteAttrs(delAttrs));
    }

    return toJsResult(await _BINDINGS.coreInvoke("update-schema", args));
  }

  async clear() {
    await _BINDINGS.coreInvoke("clear", [this.rawHandle()]);
  }

  async entid(eid) {
    return toJsResult(await callJavaMethod(this.rawHandle(), "entid", await toJava(eid)));
  }

  async entity(eid) {
    const { Entity } = await import("./entity.js");
    const entity = await _BINDINGS.connectionEntity(this.rawHandle(), eid);
    return entity === null || entity === undefined ? null : new Entity(entity);
  }

  async entityMap(eid) {
    return toJsResult(await callJavaMethod(this.rawHandle(), "entityMap", await toJava(eid)));
  }

  async pull(selector, eid) {
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "pull", await pullSelector(selector), await toJava(eid)),
      { bridge: true }
    );
  }

  async pullMany(selector, eids) {
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "pullMany", await pullSelector(selector), await toJava(eids)),
      { bridge: true }
    );
  }

  async query(query, ...inputs) {
    const normalizedInputs = [];
    for (const input of inputs) {
      normalizedInputs.push(await toQueryInput(input));
    }
    if (typeof query === "string") {
      return toJsResult(
        await callJavaMethod(this.rawHandle(), "query", query, await toJava(normalizedInputs)),
        { bridge: true }
      );
    }
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "queryForm", await queryForm(query), await toJava(normalizedInputs)),
      { bridge: true }
    );
  }

  async explain(query, { inputs = [], optsEdn = null } = {}) {
    const normalizedInputs = [];
    for (const input of inputs) {
      normalizedInputs.push(await toQueryInput(input));
    }
    if (optsEdn !== null && optsEdn !== undefined) {
      if (typeof query === "string") {
        return toJsResult(
          await callJavaMethod(
            this.rawHandle(),
            "explain",
            optsEdn,
            query,
            await toJava(normalizedInputs)
          ),
          { bridge: true }
        );
      }
      return toJsResult(
        await callJavaMethod(
          this.rawHandle(),
          "explainForm",
          optsEdn,
          await queryForm(query),
          await toJava(normalizedInputs)
        ),
        { bridge: true }
      );
    }

    if (typeof query === "string") {
      return toJsResult(
        await callJavaMethod(this.rawHandle(), "explain", query, await toJava(normalizedInputs)),
        { bridge: true }
      );
    }
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "explainForm", await queryForm(query), await toJava(normalizedInputs)),
      { bridge: true }
    );
  }

  async transact(txData, txMeta = null) {
    const args = [this.rawHandle(), await _BINDINGS.txData(txData)];
    if (txMeta !== null && txMeta !== undefined) {
      args.push(await toJava(txMeta));
    }
    return toJsResult(await _BINDINGS.coreInvoke("transact!", args));
  }

  async transactAsync(txData, txMeta = null) {
    const future = await _BINDINGS.connectionTransactAsync(this.rawHandle(), txData, txMeta);
    return toJsResult(await callJavaMethod(future, "get"), { bridge: true });
  }

  async listen(callback, key = null) {
    const proxy = await createConsumerProxy(callback);
    let registeredKey;
    try {
      registeredKey = hasValue(key)
        ? await toJsResult(await _BINDINGS.connectionListen(this.rawHandle(), key, proxy), { bridge: true })
        : await toJsResult(await _BINDINGS.connectionListen(this.rawHandle(), proxy), { bridge: true });
    } catch (error) {
      proxy.reset?.();
      throw error;
    }
    const previous = this._listenerProxies.get(registeredKey);
    previous?.proxy.reset?.();
    previous?.release?.();
    this._listenerProxies.set(registeredKey, {
      proxy,
      release: await retainListenerProxyEventLoop()
    });
    return registeredKey;
  }

  async unlisten(key) {
    await _BINDINGS.connectionUnlisten(this.rawHandle(), key);
    const existing = this._listenerProxies.get(key);
    existing?.proxy.reset?.();
    existing?.release?.();
    this._listenerProxies.delete(key);
  }

  async fillDb(datoms) {
    await callJavaMethod(this.rawHandle(), "fillDb", await toJava(datoms));
    return this;
  }

  async datalogKv() {
    const { KV } = await import("./kv.js");
    return new KV(await callJavaMethod(this.rawHandle(), "datalogKV"), { owned: false });
  }

  async reIndex(opts = null, { schema = null } = {}) {
    this._handle = await _BINDINGS.connectionReIndex(this.rawHandle(), schema, opts);
    return this;
  }

  async datoms(index, { c1 = null, c2 = null, c3 = null, limit = null, offset = 0 } = {}) {
    const capped = fetchLimit(limit, offset);
    const args = [this.rawHandle(), "datoms", await toJava(index), await toJava(c1), await toJava(c2), await toJava(c3)];
    if (hasValue(capped)) {
      args.push(await toJava(capped));
    }
    const rows = await toJsResult(
      await callJavaMethod(...args),
      { bridge: true }
    );
    return sliceRows(rows, { limit, offset });
  }

  async searchDatoms({ e = null, attr = null, value = null, limit = null, offset = 0 } = {}) {
    const rows = await toJsResult(
      await callJavaMethod(this.rawHandle(), "searchDatoms", await toJava(e), await toJava(attr), await toJava(value)),
      { bridge: true }
    );
    return sliceRows(rows, { limit, offset });
  }

  async countDatoms({ e = null, attr = null, value = null } = {}) {
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "countDatoms", await toJava(e), await toJava(attr), await toJava(value))
    );
  }

  async seekDatoms(index, { c1 = null, c2 = null, c3 = null, limit = null, offset = 0 } = {}) {
    const capped = fetchLimit(limit, offset);
    const args = [
      this.rawHandle(),
      "seekDatoms",
      await toJava(index),
      await toJava(c1),
      await toJava(c2),
      await toJava(c3)
    ];
    if (hasValue(capped)) {
      args.push(await toJava(capped));
    }
    const rows = await toJsResult(
      await callJavaMethod(...args),
      { bridge: true }
    );
    return sliceRows(rows, { limit, offset });
  }

  async rseekDatoms(index, { c1 = null, c2 = null, c3 = null, limit = null, offset = 0 } = {}) {
    const capped = fetchLimit(limit, offset);
    const args = [
      this.rawHandle(),
      "rseekDatoms",
      await toJava(index),
      await toJava(c1),
      await toJava(c2),
      await toJava(c3)
    ];
    if (hasValue(capped)) {
      args.push(await toJava(capped));
    }
    const rows = await toJsResult(
      await callJavaMethod(...args),
      { bridge: true }
    );
    return sliceRows(rows, { limit, offset });
  }

  async indexRange(attr, start, end, { limit = null, offset = 0 } = {}) {
    const rows = await toJsResult(
      await callJavaMethod(this.rawHandle(), "indexRange", await toJava(attr), await toJava(start), await toJava(end)),
      { bridge: true }
    );
    return sliceRows(rows, { limit, offset });
  }

  async fulltextDatoms(query, { opts = null, limit = null, offset = 0 } = {}) {
    const result = hasValue(opts)
      ? await callJavaMethod(this.rawHandle(), "fulltextDatoms", query, await toJava(opts))
      : await callJavaMethod(this.rawHandle(), "fulltextDatoms", query);
    const rows = await toJsResult(
      result,
      { bridge: true }
    );
    return sliceRows(rows, { limit, offset });
  }

  async copy(dest, { compact = null } = {}) {
    if (hasValue(compact)) {
      await callJavaMethod(this.rawHandle(), "copy", dest, Boolean(compact));
      return;
    }
    await callJavaMethod(this.rawHandle(), "copy", dest);
  }

  async txLogWatermarks() {
    return toJsResult(await callJavaMethod(this.rawHandle(), "txLogWatermarks"), { bridge: true });
  }

  async openTxLog(fromLsn, { uptoLsn = null, limit = null } = {}) {
    const result = hasValue(uptoLsn)
      ? await callJavaMethod(this.rawHandle(), "openTxLog", await toJava(fromLsn), await toJava(uptoLsn))
      : await callJavaMethod(this.rawHandle(), "openTxLog", await toJava(fromLsn));
    const rows = await toJsResult(result, { bridge: true });
    if (limit === null || limit === undefined) {
      return rows;
    }
    return rows.slice(0, Math.max(limit, 0));
  }

  async createSnapshot() {
    return toJsResult(await callJavaMethod(this.rawHandle(), "createSnapshot"), { bridge: true });
  }

  async listSnapshots() {
    return toJsResult(await callJavaMethod(this.rawHandle(), "listSnapshots"), { bridge: true });
  }

  async gcTxLogSegments({ retainFloorLsn = null } = {}) {
    if (hasValue(retainFloorLsn)) {
      return toJsResult(
        await callJavaMethod(this.rawHandle(), "gcTxLogSegments", await toJava(retainFloorLsn)),
        { bridge: true }
      );
    }
    return toJsResult(await callJavaMethod(this.rawHandle(), "gcTxLogSegments"), { bridge: true });
  }
}
