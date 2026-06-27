import { toEdnForm, toJava, toJs } from "./convert.js";
import { DatalevinError } from "./errors.js";
import { _BINDINGS } from "./interop.js";
import { callJavaMethod, javaBridgeModule } from "./jvm.js";
import { ResourceWrapper } from "./resource.js";
import { toJsResult } from "./result.js";

const NO_TIMEOUT_OPTION = Symbol("no-timeout-option");

function slicePage(items, limit = null, offset = null) {
  const start = offset === null || offset === undefined ? 0 : Math.max(offset, 0);
  if (limit === null || limit === undefined) {
    return items.slice(start);
  }
  return items.slice(start, start + Math.max(limit, 0));
}

function hasValue(value) {
  return value !== null && value !== undefined;
}

function requireKType(kType, methodName) {
  if (!hasValue(kType)) {
    throw new TypeError(`kType is required for KV ${methodName}().`);
  }
}

function requireVType(vType, methodName) {
  if (!hasValue(vType)) {
    throw new TypeError(`vType is required for KV ${methodName}().`);
  }
}

function rejectVTypeWithoutKType(kType, vType, methodName) {
  if (hasValue(vType) && !hasValue(kType)) {
    throw new TypeError(`vType requires kType for KV ${methodName}().`);
  }
}

async function envFlags(flags) {
  const items = typeof flags === "string" ? [flags] : [...flags];
  const result = new Set();
  for (const flag of items) {
    const text = String(flag);
    result.add(await _BINDINGS.keyword(text.startsWith(":") ? text : `:${text}`));
  }
  return result;
}

function requireCallback(fn, methodName) {
  if (typeof fn !== "function") {
    throw new TypeError(`callback is required for KV ${methodName}().`);
  }
}

function normalizeTimeoutMs(value, methodName) {
  if (!hasValue(value)) {
    return null;
  }
  const timeoutMs = Number(value);
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
    throw new TypeError(`timeoutMs for KV ${methodName}() must be a positive integer.`);
  }
  return timeoutMs;
}

function timeoutOption(options, methodName) {
  if (!hasValue(options)) {
    return NO_TIMEOUT_OPTION;
  }
  if (typeof options === "number") {
    return normalizeTimeoutMs(options, methodName);
  }
  if (typeof options === "object") {
    if (Object.prototype.hasOwnProperty.call(options, "timeoutMs")) {
      return normalizeTimeoutMs(options.timeoutMs, methodName);
    }
    if (Object.prototype.hasOwnProperty.call(options, "timeout_ms")) {
      return normalizeTimeoutMs(options.timeout_ms, methodName);
    }
  }
  return NO_TIMEOUT_OPTION;
}

function withTransactionArgs(first, second) {
  if (typeof first === "function") {
    return { fn: first, timeoutOption: timeoutOption(second, "withTransaction") };
  }
  if (typeof second === "function") {
    return { fn: second, timeoutOption: timeoutOption(first, "withTransaction") };
  }
  throw new TypeError("fn must be a function.");
}

async function resolveExplicitTransactionTimeout(option) {
  if (option !== NO_TIMEOUT_OPTION) {
    return option;
  }
  return toJsResult(await _BINDINGS.coreInvoke("explicit-transaction-timeout", []));
}

function transactionTimeoutError(timeoutMs) {
  return new DatalevinError(`Explicit transaction timed out after ${timeoutMs} ms`, {
    typeName: "transaction/timeout",
    data: { timeoutMs }
  });
}

function requireRange(value, rangeName, methodName) {
  if (!hasValue(value)) {
    throw new TypeError(`${rangeName} is required for KV ${methodName}().`);
  }
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

async function withJavaProxy(interfaceName, implementation, fn) {
  const { newProxy } = await javaBridgeModule();
  const proxy = newProxy(interfaceName, implementation);
  try {
    return await withInterfaceProxyEventLoop(() => fn(proxy));
  } finally {
    proxy.reset?.();
  }
}

async function typedRangeArg(keyRange, kType) {
  if (hasValue(kType)) {
    return _BINDINGS.kvRange(keyRange, kType);
  }
  return toEdnForm(keyRange);
}

async function appendTypedArgs(args, { kType = null, vType = null, ignoreKey = null } = {}, methodName) {
  rejectVTypeWithoutKType(kType, vType, methodName);
  if (hasValue(ignoreKey) && (!hasValue(kType) || !hasValue(vType))) {
    throw new TypeError(`ignoreKey requires kType and vType for KV ${methodName}().`);
  }
  if (hasValue(kType)) {
    args.push(await _BINDINGS.kvType(kType));
  }
  if (hasValue(vType)) {
    args.push(await _BINDINGS.kvType(vType));
  }
  if (hasValue(ignoreKey)) {
    args.push(Boolean(ignoreKey));
  }
}

async function byteArrayToBuffer(value) {
  const converted = await toJs(value);
  if (Buffer.isBuffer(converted)) {
    return converted;
  }
  if (converted instanceof Uint8Array) {
    return Buffer.from(converted.buffer, converted.byteOffset, converted.byteLength);
  }
  if (Array.isArray(converted)) {
    return Buffer.from(converted.map((item) => Number(item) & 0xff));
  }
  return Buffer.from([]);
}

export class RawBuffer {
  constructor(handle) {
    this._handle = handle;
  }

  rawHandle() {
    return this._handle;
  }

  async bytes() {
    return byteArrayToBuffer(await callJavaMethod(this.rawHandle(), "bytes"));
  }

  async read(valueType = ":data") {
    if (!hasValue(valueType)) {
      return toJsResult(await callJavaMethod(this.rawHandle(), "read"));
    }
    return toJsResult(await callJavaMethod(this.rawHandle(), "read", await toJava(valueType)));
  }
}

export class RawKV {
  constructor(handle) {
    this._handle = handle;
  }

  rawHandle() {
    return this._handle;
  }

  async key() {
    return new RawBuffer(await callJavaMethod(this.rawHandle(), "key"));
  }

  async value() {
    return new RawBuffer(await callJavaMethod(this.rawHandle(), "value"));
  }

  async keyBytes() {
    return byteArrayToBuffer(await callJavaMethod(this.rawHandle(), "keyBytes"));
  }

  async valueBytes() {
    return byteArrayToBuffer(await callJavaMethod(this.rawHandle(), "valueBytes"));
  }

  async readKey(valueType = ":data") {
    if (!hasValue(valueType)) {
      return toJsResult(await callJavaMethod(this.rawHandle(), "readKey"));
    }
    return toJsResult(await callJavaMethod(this.rawHandle(), "readKey", await toJava(valueType)));
  }

  async readValue(valueType = ":data") {
    if (!hasValue(valueType)) {
      return toJsResult(await callJavaMethod(this.rawHandle(), "readValue"));
    }
    return toJsResult(await callJavaMethod(this.rawHandle(), "readValue", await toJava(valueType)));
  }
}

export class KV extends ResourceWrapper {
  constructor(handle, { owned = true } = {}) {
    super(
      handle,
      owned ? (rawHandle) => _BINDINGS.closeKeyValue(rawHandle) : async () => {},
      (rawHandle) => _BINDINGS.keyValueClosed(rawHandle),
      "kv"
    );
  }

  async dir() {
    return toJsResult(await _BINDINGS.coreInvoke("dir", [this.rawHandle()]));
  }

  async openDbi(name, opts = null) {
    const args = [this.rawHandle(), name];
    if (opts !== null && opts !== undefined) {
      args.push(await _BINDINGS.options(opts));
    }
    await _BINDINGS.coreInvoke("open-dbi", args);
  }

  async openListDbi(name, opts = null) {
    const args = [this.rawHandle(), name];
    if (opts !== null && opts !== undefined) {
      args.push(await _BINDINGS.options(opts));
    }
    await _BINDINGS.coreInvoke("open-list-dbi", args);
  }

  async beginTransaction() {
    return new KVTransaction(await _BINDINGS.keyValueBeginTransaction(this.rawHandle()));
  }

  async transaction() {
    return this.beginTransaction();
  }

  async withTransaction(fnOrOptions, maybeFnOrOptions = null) {
    const { fn, timeoutOption } = withTransactionArgs(fnOrOptions, maybeFnOrOptions);
    const timeoutMs = await resolveExplicitTransactionTimeout(timeoutOption);
    const tx = await this.beginTransaction();
    let timeoutId = null;
    try {
      const body = Promise.resolve().then(() => fn(tx));
      const result = hasValue(timeoutMs)
        ? await Promise.race([
            body,
            new Promise((_, reject) => {
              timeoutId = setTimeout(
                () => reject(transactionTimeoutError(timeoutMs)),
                timeoutMs
              );
            })
          ])
        : await body;
      if (tx.active()) {
        await tx.commit();
      }
      return result;
    } catch (error) {
      if (tx.active()) {
        await tx.abort();
      }
      throw error;
    } finally {
      if (timeoutId !== null) {
        clearTimeout(timeoutId);
      }
    }
  }

  async searchIndexWriter(opts = null) {
    const { SearchIndexWriter } = await import("./search.js");
    return new SearchIndexWriter(await _BINDINGS.searchIndexWriter(this.rawHandle(), opts));
  }

  async newSearchEngine(opts = null) {
    const { SearchEngine } = await import("./search.js");
    return new SearchEngine(await _BINDINGS.newSearchEngine(this.rawHandle(), opts));
  }

  async newVectorIndex(opts) {
    const { VectorIndex } = await import("./vector.js");
    return new VectorIndex(await _BINDINGS.newVectorIndex(this.rawHandle(), opts));
  }

  async reIndex(opts = null) {
    this._handle = await _BINDINGS.keyValueReIndex(this.rawHandle(), opts);
    return this;
  }

  async listDbis() {
    return toJsResult(await _BINDINGS.coreInvoke("list-dbis", [this.rawHandle()]));
  }

  async entries(dbiName) {
    return toJsResult(await _BINDINGS.coreInvoke("entries", [this.rawHandle(), dbiName]));
  }

  async stat(dbiName = null) {
    const args = [this.rawHandle()];
    if (hasValue(dbiName)) {
      args.push(dbiName);
    }
    return toJsResult(await _BINDINGS.coreInvoke("stat", args));
  }

  async copy(dest, { compact = null } = {}) {
    const args = [this.rawHandle(), dest];
    if (hasValue(compact)) {
      args.push(Boolean(compact));
    }
    return toJsResult(await _BINDINGS.coreInvoke("copy", args));
  }

  async sync(force = null) {
    const args = [this.rawHandle()];
    if (hasValue(force)) {
      args.push(force);
    }
    return toJsResult(await _BINDINGS.coreInvoke("sync", args));
  }

  async setEnvFlags(flags, onOff) {
    return toJsResult(
      await _BINDINGS.coreInvoke("set-env-flags", [this.rawHandle(), await envFlags(flags), onOff ? true : null])
    );
  }

  async getEnvFlags() {
    return toJsResult(await _BINDINGS.coreInvoke("get-env-flags", [this.rawHandle()]));
  }

  async txLogWatermarks() {
    return toJsResult(await _BINDINGS.coreInvoke("txlog-watermarks", [this.rawHandle()]));
  }

  async openTxLog(fromLsn, { uptoLsn = null, limit = null } = {}) {
    const args = [this.rawHandle(), fromLsn];
    if (hasValue(uptoLsn)) {
      args.push(uptoLsn);
    }
    const rows = await toJsResult(await _BINDINGS.coreInvoke("open-tx-log", args));
    if (!hasValue(limit)) {
      return rows;
    }
    return rows.slice(0, Math.max(limit, 0));
  }

  async createSnapshot() {
    return toJsResult(await _BINDINGS.coreInvoke("create-snapshot!", [this.rawHandle()]));
  }

  async listSnapshots() {
    return toJsResult(await _BINDINGS.coreInvoke("list-snapshots", [this.rawHandle()]));
  }

  async gcTxLogSegments({ retainFloorLsn = null } = {}) {
    const args = [this.rawHandle()];
    if (hasValue(retainFloorLsn)) {
      args.push(retainFloorLsn);
    }
    return toJsResult(await _BINDINGS.coreInvoke("gc-txlog-segments!", args));
  }

  async transact(txs, { dbiName = null, kType = null, vType = null } = {}) {
    if (dbiName === null && (kType !== null || vType !== null)) {
      throw new TypeError("kType and vType require dbiName for KV transact().");
    }
    if (vType !== null && kType === null) {
      throw new TypeError("vType requires kType for KV transact().");
    }

    const args = [this.rawHandle()];
    const normalizedTxs = kType === null ? await _BINDINGS.kvTxs(txs) : await _BINDINGS.kvTxsWithTypes(txs, kType, vType);
    if (dbiName === null) {
      args.push(normalizedTxs);
    } else {
      args.push(dbiName);
      args.push(normalizedTxs);
      if (kType !== null) {
        args.push(await _BINDINGS.kvType(kType));
        if (vType !== null) {
          args.push(await _BINDINGS.kvType(vType));
        }
      }
    }
    return toJsResult(await _BINDINGS.coreInvoke("transact-kv", args));
  }

  async getValue(dbiName, key, { kType = null, vType = null, ignoreKey = false } = {}) {
    if ((kType === null) !== (vType === null)) {
      throw new TypeError("kType and vType must be provided together for KV getValue().");
    }

    const args = [this.rawHandle(), dbiName];
    if (kType === null) {
      args.push(await toJava(key));
      return toJsResult(await _BINDINGS.coreInvoke("get-value", args));
    }
    return toJsResult(
      await callJavaMethod(
        this.rawHandle(),
        "getValue",
        dbiName,
        await toJava(key),
        kType,
        vType,
        Boolean(ignoreKey)
      )
    );
  }

  async getRank(dbiName, key, { kType = null } = {}) {
    const args = [this.rawHandle(), dbiName, await toJava(key)];
    if (hasValue(kType)) {
      args.push(await _BINDINGS.kvType(kType));
    }
    return toJsResult(await _BINDINGS.coreInvoke("get-rank", args));
  }

  async getByRank(dbiName, rank, { kType = null, vType = null, ignoreKey = null } = {}) {
    const args = [this.rawHandle(), dbiName, rank];
    rejectVTypeWithoutKType(kType, vType, "getByRank");
    if (hasValue(kType) && hasValue(vType)) {
      return toJsResult(
        await _BINDINGS.kvGetByRank(
          this.rawHandle(),
          dbiName,
          rank,
          kType,
          vType,
          hasValue(ignoreKey) ? Boolean(ignoreKey) : true
        )
      );
    }
    await appendTypedArgs(args, { kType, vType, ignoreKey }, "getByRank");
    return toJsResult(await _BINDINGS.coreInvoke("get-by-rank", args));
  }

  async getEntryByRank(dbiName, rank, { kType = null, vType = null } = {}) {
    requireKType(kType, "getEntryByRank");
    requireVType(vType, "getEntryByRank");
    return toJsResult(await _BINDINGS.kvGetEntryByRank(this.rawHandle(), dbiName, rank, kType, vType));
  }

  async sampleKv(dbiName, n, { kType = null, vType = null, ignoreKey = null } = {}) {
    const args = [this.rawHandle(), dbiName, n];
    rejectVTypeWithoutKType(kType, vType, "sampleKv");
    if (hasValue(kType) && hasValue(vType)) {
      return toJsResult(
        await _BINDINGS.kvSample(
          this.rawHandle(),
          dbiName,
          n,
          kType,
          vType,
          hasValue(ignoreKey) ? Boolean(ignoreKey) : true
        )
      );
    }
    await appendTypedArgs(args, { kType, vType, ignoreKey }, "sampleKv");
    return toJsResult(await _BINDINGS.coreInvoke("sample-kv", args));
  }

  async getFirst(dbiName, keyRange, { kType = null, vType = null, ignoreKey = null } = {}) {
    if (keyRange === null || keyRange === undefined) {
      throw new TypeError("keyRange is required for KV getFirst().");
    }
    const args = [this.rawHandle(), dbiName, await typedRangeArg(keyRange, kType)];
    await appendTypedArgs(args, { kType, vType, ignoreKey }, "getFirst");
    return toJsResult(await _BINDINGS.coreInvoke("get-first", args));
  }

  async getFirstN(dbiName, n, keyRange, { kType = null, vType = null, ignoreKey = null } = {}) {
    if (keyRange === null || keyRange === undefined) {
      throw new TypeError("keyRange is required for KV getFirstN().");
    }
    const args = [this.rawHandle(), dbiName, n, await typedRangeArg(keyRange, kType)];
    await appendTypedArgs(args, { kType, vType, ignoreKey }, "getFirstN");
    return toJsResult(await _BINDINGS.coreInvoke("get-first-n", args));
  }

  async getRange(
    dbiName,
    keyRange,
    { kType = null, vType = null, limit = null, offset = null } = {}
  ) {
    if (keyRange === null || keyRange === undefined) {
      throw new TypeError("keyRange is required for KV getRange().");
    }
    if (vType !== null && kType === null) {
      throw new TypeError("vType requires kType for KV getRange().");
    }

    const args = [this.rawHandle(), dbiName];
    if (kType === null) {
      args.push(await toEdnForm(keyRange));
    } else {
      args.push(await _BINDINGS.kvRange(keyRange, kType));
      args.push(await _BINDINGS.kvType(kType));
      if (vType !== null) {
        args.push(await _BINDINGS.kvType(vType));
      }
    }
    return slicePage(await toJsResult(await _BINDINGS.coreInvoke("get-range", args)), limit, offset);
  }

  async keyRange(dbiName, keyRange, { kType = null, limit = null, offset = null } = {}) {
    if (keyRange === null || keyRange === undefined) {
      throw new TypeError("keyRange is required for KV keyRange().");
    }

    const args = [this.rawHandle(), dbiName, await toEdnForm(keyRange)];
    if (hasValue(kType)) {
      args.push(await _BINDINGS.kvType(kType));
    }
    return slicePage(await toJsResult(await _BINDINGS.coreInvoke("key-range", args)), limit, offset);
  }

  async keyRangeCount(dbiName, keyRange, { kType = null } = {}) {
    if (keyRange === null || keyRange === undefined) {
      throw new TypeError("keyRange is required for KV keyRangeCount().");
    }

    const args = [this.rawHandle(), dbiName, await toEdnForm(keyRange)];
    if (hasValue(kType)) {
      args.push(await _BINDINGS.kvType(kType));
    }
    return toJsResult(await _BINDINGS.coreInvoke("key-range-count", args));
  }

  async rangeCount(dbiName, keyRange, { kType = null } = {}) {
    if (keyRange === null || keyRange === undefined) {
      throw new TypeError("keyRange is required for KV rangeCount().");
    }

    const args = [this.rawHandle(), dbiName, await toEdnForm(keyRange)];
    if (hasValue(kType)) {
      args.push(await _BINDINGS.kvType(kType));
    }
    return toJsResult(await _BINDINGS.coreInvoke("range-count", args));
  }

  async putListItems(listName, key, values, { kType = null, vType = null } = {}) {
    requireKType(kType, "putListItems");
    requireVType(vType, "putListItems");
    await _BINDINGS.coreInvoke("put-list-items", [
      this.rawHandle(),
      listName,
      await toJava(key),
      await toJava(values),
      await _BINDINGS.kvType(kType),
      await _BINDINGS.kvType(vType)
    ]);
  }

  async delListItems(listName, key, { kType = null, values = null, vType = null } = {}) {
    requireKType(kType, "delListItems");
    const args = [this.rawHandle(), listName, await toJava(key)];
    if (hasValue(values)) {
      requireVType(vType, "delListItems");
      args.push(await toJava(values), await _BINDINGS.kvType(kType), await _BINDINGS.kvType(vType));
    } else {
      if (hasValue(vType)) {
        throw new TypeError("vType requires values for KV delListItems().");
      }
      args.push(await _BINDINGS.kvType(kType));
    }
    await _BINDINGS.coreInvoke("del-list-items", args);
  }

  async getList(listName, key, { kType = null, vType = null, limit = null, offset = null } = {}) {
    requireKType(kType, "getList");
    requireVType(vType, "getList");
    const items = await toJsResult(
      await _BINDINGS.coreInvoke("get-list", [
        this.rawHandle(),
        listName,
        await toJava(key),
        await _BINDINGS.kvType(kType),
        await _BINDINGS.kvType(vType)
      ])
    );
    return slicePage(items, limit, offset);
  }

  async listCount(listName, key, { kType = null } = {}) {
    requireKType(kType, "listCount");
    return toJsResult(
      await _BINDINGS.coreInvoke("list-count", [
        this.rawHandle(),
        listName,
        await toJava(key),
        await _BINDINGS.kvType(kType)
      ])
    );
  }

  async inList(listName, key, value, { kType = null, vType = null } = {}) {
    requireKType(kType, "inList");
    requireVType(vType, "inList");
    return Boolean(
      await toJsResult(
        await _BINDINGS.coreInvoke("in-list?", [
          this.rawHandle(),
          listName,
          await toJava(key),
          await toJava(value),
          await _BINDINGS.kvType(kType),
          await _BINDINGS.kvType(vType)
        ])
      )
    );
  }

  async visitList(listName, visitor, key, { kType = null, vType = null } = {}) {
    requireCallback(visitor, "visitList");
    requireKType(kType, "visitList");
    requireVType(vType, "visitList");
    await withJavaProxy(
      "java.util.function.Consumer",
      {
        accept: async (value) => {
          await visitor(await toJs(value));
        }
      },
      async (proxy) => _BINDINGS.kvVisitList(
        this.rawHandle(),
        listName,
        proxy,
        await toJava(key),
        kType,
        vType
      )
    );
  }

  async visitListRaw(listName, visitor, key, { kType = null } = {}) {
    requireCallback(visitor, "visitListRaw");
    requireKType(kType, "visitListRaw");
    await withJavaProxy(
      "java.util.function.Consumer",
      {
        accept: async (value) => {
          await visitor(new RawBuffer(value));
        }
      },
      async (proxy) => _BINDINGS.kvVisitListRaw(
        this.rawHandle(),
        listName,
        proxy,
        await toJava(key),
        kType
      )
    );
  }

  async visitListRange(listName, visitor, kRange, { kType = null, vRange = null, vType = null } = {}) {
    requireRange(kRange, "kRange", "visitListRange");
    requireRange(vRange, "vRange", "visitListRange");
    requireCallback(visitor, "visitListRange");
    requireKType(kType, "visitListRange");
    requireVType(vType, "visitListRange");
    const normalizedKRange = await toEdnForm(kRange);
    const normalizedVRange = await toEdnForm(vRange);
    await withJavaProxy(
      "java.util.function.BiConsumer",
      {
        accept: async (key, value) => {
          await visitor(await toJs(key), await toJs(value));
        }
      },
      async (proxy) => _BINDINGS.kvVisitListRange(
        this.rawHandle(),
        listName,
        proxy,
        normalizedKRange,
        kType,
        normalizedVRange,
        vType
      )
    );
  }

  async visitListRangeRaw(listName, visitor, kRange, { kType = null, vRange = null, vType = null } = {}) {
    requireRange(kRange, "kRange", "visitListRangeRaw");
    requireRange(vRange, "vRange", "visitListRangeRaw");
    requireCallback(visitor, "visitListRangeRaw");
    requireKType(kType, "visitListRangeRaw");
    requireVType(vType, "visitListRangeRaw");
    const normalizedKRange = await toEdnForm(kRange);
    const normalizedVRange = await toEdnForm(vRange);
    await withJavaProxy(
      "java.util.function.Consumer",
      {
        accept: async (value) => {
          await visitor(new RawKV(value));
        }
      },
      async (proxy) => _BINDINGS.kvVisitListRangeRaw(
        this.rawHandle(),
        listName,
        proxy,
        normalizedKRange,
        kType,
        normalizedVRange,
        vType
      )
    );
  }

  async listRangeFilter(
    listName,
    predicate,
    kRange,
    { kType = null, vRange = null, vType = null, limit = null, offset = null } = {}
  ) {
    requireRange(kRange, "kRange", "listRangeFilter");
    requireRange(vRange, "vRange", "listRangeFilter");
    requireCallback(predicate, "listRangeFilter");
    requireKType(kType, "listRangeFilter");
    requireVType(vType, "listRangeFilter");
    const normalizedKRange = await toEdnForm(kRange);
    const normalizedVRange = await toEdnForm(vRange);
    const items = await toJsResult(await withJavaProxy(
      "java.util.function.BiPredicate",
      {
        test: async (key, value) => Boolean(await predicate(await toJs(key), await toJs(value)))
      },
      async (proxy) => _BINDINGS.kvListRangeFilter(
        this.rawHandle(),
        listName,
        proxy,
        normalizedKRange,
        kType,
        normalizedVRange,
        vType
      )
    ));
    return slicePage(items, limit, offset);
  }

  async listRangeFilterRaw(
    listName,
    predicate,
    kRange,
    { kType = null, vRange = null, vType = null, limit = null, offset = null } = {}
  ) {
    requireRange(kRange, "kRange", "listRangeFilterRaw");
    requireRange(vRange, "vRange", "listRangeFilterRaw");
    requireCallback(predicate, "listRangeFilterRaw");
    requireKType(kType, "listRangeFilterRaw");
    requireVType(vType, "listRangeFilterRaw");
    const normalizedKRange = await toEdnForm(kRange);
    const normalizedVRange = await toEdnForm(vRange);
    const items = await toJsResult(await withJavaProxy(
      "java.util.function.Predicate",
      {
        test: async (value) => Boolean(await predicate(new RawKV(value)))
      },
      async (proxy) => _BINDINGS.kvListRangeFilterRaw(
        this.rawHandle(),
        listName,
        proxy,
        normalizedKRange,
        kType,
        normalizedVRange,
        vType
      )
    ));
    return slicePage(items, limit, offset);
  }

  async listRangeFilterCount(
    listName,
    predicate,
    kRange,
    { kType = null, vRange = null, vType = null } = {}
  ) {
    requireRange(kRange, "kRange", "listRangeFilterCount");
    requireRange(vRange, "vRange", "listRangeFilterCount");
    requireCallback(predicate, "listRangeFilterCount");
    requireKType(kType, "listRangeFilterCount");
    requireVType(vType, "listRangeFilterCount");
    const normalizedKRange = await toEdnForm(kRange);
    const normalizedVRange = await toEdnForm(vRange);
    return toJsResult(await withJavaProxy(
      "java.util.function.BiPredicate",
      {
        test: async (key, value) => Boolean(await predicate(await toJs(key), await toJs(value)))
      },
      async (proxy) => _BINDINGS.kvListRangeFilterCount(
        this.rawHandle(),
        listName,
        proxy,
        normalizedKRange,
        kType,
        normalizedVRange,
        vType
      )
    ));
  }

  async listRangeFilterCountRaw(
    listName,
    predicate,
    kRange,
    { kType = null, vRange = null, vType = null } = {}
  ) {
    requireRange(kRange, "kRange", "listRangeFilterCountRaw");
    requireRange(vRange, "vRange", "listRangeFilterCountRaw");
    requireCallback(predicate, "listRangeFilterCountRaw");
    requireKType(kType, "listRangeFilterCountRaw");
    requireVType(vType, "listRangeFilterCountRaw");
    const normalizedKRange = await toEdnForm(kRange);
    const normalizedVRange = await toEdnForm(vRange);
    return toJsResult(await withJavaProxy(
      "java.util.function.Predicate",
      {
        test: async (value) => Boolean(await predicate(new RawKV(value)))
      },
      async (proxy) => _BINDINGS.kvListRangeFilterCountRaw(
        this.rawHandle(),
        listName,
        proxy,
        normalizedKRange,
        kType,
        normalizedVRange,
        vType
      )
    ));
  }

  async listRangeKeep(
    listName,
    fn,
    kRange,
    { kType = null, vRange = null, vType = null, limit = null, offset = null } = {}
  ) {
    requireRange(kRange, "kRange", "listRangeKeep");
    requireRange(vRange, "vRange", "listRangeKeep");
    requireCallback(fn, "listRangeKeep");
    requireKType(kType, "listRangeKeep");
    requireVType(vType, "listRangeKeep");
    const normalizedKRange = await toEdnForm(kRange);
    const normalizedVRange = await toEdnForm(vRange);
    const items = await toJsResult(await withJavaProxy(
      "java.util.function.BiFunction",
      {
        apply: async (key, value) => {
          const result = await fn(await toJs(key), await toJs(value));
          return toJava(result === undefined ? null : result);
        }
      },
      async (proxy) => _BINDINGS.kvListRangeKeep(
        this.rawHandle(),
        listName,
        proxy,
        normalizedKRange,
        kType,
        normalizedVRange,
        vType
      )
    ));
    return slicePage(items, limit, offset);
  }

  async listRangeKeepRaw(
    listName,
    fn,
    kRange,
    { kType = null, vRange = null, vType = null, limit = null, offset = null } = {}
  ) {
    requireRange(kRange, "kRange", "listRangeKeepRaw");
    requireRange(vRange, "vRange", "listRangeKeepRaw");
    requireCallback(fn, "listRangeKeepRaw");
    requireKType(kType, "listRangeKeepRaw");
    requireVType(vType, "listRangeKeepRaw");
    const normalizedKRange = await toEdnForm(kRange);
    const normalizedVRange = await toEdnForm(vRange);
    const items = await toJsResult(await withJavaProxy(
      "java.util.function.Function",
      {
        apply: async (value) => {
          const result = await fn(new RawKV(value));
          return toJava(result === undefined ? null : result);
        }
      },
      async (proxy) => _BINDINGS.kvListRangeKeepRaw(
        this.rawHandle(),
        listName,
        proxy,
        normalizedKRange,
        kType,
        normalizedVRange,
        vType
      )
    ));
    return slicePage(items, limit, offset);
  }

  async listRangeSome(listName, fn, kRange, { kType = null, vRange = null, vType = null } = {}) {
    requireRange(kRange, "kRange", "listRangeSome");
    requireRange(vRange, "vRange", "listRangeSome");
    requireCallback(fn, "listRangeSome");
    requireKType(kType, "listRangeSome");
    requireVType(vType, "listRangeSome");
    const normalizedKRange = await toEdnForm(kRange);
    const normalizedVRange = await toEdnForm(vRange);
    return toJsResult(await withJavaProxy(
      "java.util.function.BiFunction",
      {
        apply: async (key, value) => {
          const result = await fn(await toJs(key), await toJs(value));
          return toJava(result === undefined ? null : result);
        }
      },
      async (proxy) => _BINDINGS.kvListRangeSome(
        this.rawHandle(),
        listName,
        proxy,
        normalizedKRange,
        kType,
        normalizedVRange,
        vType
      )
    ));
  }

  async listRangeSomeRaw(listName, fn, kRange, { kType = null, vRange = null, vType = null } = {}) {
    requireRange(kRange, "kRange", "listRangeSomeRaw");
    requireRange(vRange, "vRange", "listRangeSomeRaw");
    requireCallback(fn, "listRangeSomeRaw");
    requireKType(kType, "listRangeSomeRaw");
    requireVType(vType, "listRangeSomeRaw");
    const normalizedKRange = await toEdnForm(kRange);
    const normalizedVRange = await toEdnForm(vRange);
    return toJsResult(await withJavaProxy(
      "java.util.function.Function",
      {
        apply: async (value) => {
          const result = await fn(new RawKV(value));
          return toJava(result === undefined ? null : result);
        }
      },
      async (proxy) => _BINDINGS.kvListRangeSomeRaw(
        this.rawHandle(),
        listName,
        proxy,
        normalizedKRange,
        kType,
        normalizedVRange,
        vType
      )
    ));
  }

  async listRange(
    listName,
    kRange,
    { kType = null, vRange = null, vType = null, limit = null, offset = null } = {}
  ) {
    requireRange(kRange, "kRange", "listRange");
    requireRange(vRange, "vRange", "listRange");
    requireKType(kType, "listRange");
    requireVType(vType, "listRange");
    const items = await toJsResult(
      await _BINDINGS.coreInvoke("list-range", [
        this.rawHandle(),
        listName,
        await _BINDINGS.kvRange(kRange, kType),
        await _BINDINGS.kvType(kType),
        await _BINDINGS.kvRange(vRange, vType),
        await _BINDINGS.kvType(vType)
      ])
    );
    return slicePage(items, limit, offset);
  }

  async listRangeCount(listName, kRange, { kType = null } = {}) {
    requireRange(kRange, "kRange", "listRangeCount");
    requireKType(kType, "listRangeCount");
    return toJsResult(
      await _BINDINGS.coreInvoke("list-range-count", [
        this.rawHandle(),
        listName,
        await _BINDINGS.kvRange(kRange, kType),
        await _BINDINGS.kvType(kType)
      ])
    );
  }

  async listRangeFirst(listName, kRange, { kType = null, vRange = null, vType = null } = {}) {
    requireRange(kRange, "kRange", "listRangeFirst");
    requireRange(vRange, "vRange", "listRangeFirst");
    requireKType(kType, "listRangeFirst");
    requireVType(vType, "listRangeFirst");
    return toJsResult(
      await _BINDINGS.coreInvoke("list-range-first", [
        this.rawHandle(),
        listName,
        await _BINDINGS.kvRange(kRange, kType),
        await _BINDINGS.kvType(kType),
        await _BINDINGS.kvRange(vRange, vType),
        await _BINDINGS.kvType(vType)
      ])
    );
  }

  async listRangeFirstN(listName, n, kRange, { kType = null, vRange = null, vType = null } = {}) {
    requireRange(kRange, "kRange", "listRangeFirstN");
    requireRange(vRange, "vRange", "listRangeFirstN");
    requireKType(kType, "listRangeFirstN");
    requireVType(vType, "listRangeFirstN");
    return toJsResult(
      await _BINDINGS.coreInvoke("list-range-first-n", [
        this.rawHandle(),
        listName,
        n,
        await _BINDINGS.kvRange(kRange, kType),
        await _BINDINGS.kvType(kType),
        await _BINDINGS.kvRange(vRange, vType),
        await _BINDINGS.kvType(vType)
      ])
    );
  }

  async keyRangeListCount(listName, kRange, { kType = null } = {}) {
    requireRange(kRange, "kRange", "keyRangeListCount");
    requireKType(kType, "keyRangeListCount");
    return toJsResult(
      await _BINDINGS.coreInvoke("key-range-list-count", [
        this.rawHandle(),
        listName,
        await _BINDINGS.kvRange(kRange, kType),
        await _BINDINGS.kvType(kType)
      ])
    );
  }

  async clearDbi(dbiName) {
    await _BINDINGS.coreInvoke("clear-dbi", [this.rawHandle(), dbiName]);
  }

  async dropDbi(dbiName) {
    await _BINDINGS.coreInvoke("drop-dbi", [this.rawHandle(), dbiName]);
  }
}

export class KVTransaction extends KV {
  constructor(handle) {
    super(handle, { owned: false });
  }

  active() {
    return !this._closed;
  }

  async commit() {
    this.#requireActive();
    try {
      return toJsResult(await _BINDINGS.keyValueCommitTransaction(this.rawHandle()));
    } finally {
      this._closed = true;
    }
  }

  async abort() {
    this.#requireActive();
    try {
      return toJsResult(await _BINDINGS.keyValueAbortTransaction(this.rawHandle()));
    } finally {
      this._closed = true;
    }
  }

  async close() {
    if (!this._closed) {
      await this.abort();
    }
  }

  #requireActive() {
    if (this._closed) {
      throw new Error("KV transaction is closed.");
    }
  }
}
