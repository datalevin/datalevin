import { toEdnForm, toJava } from "./convert.js";
import { _BINDINGS } from "./interop.js";
import { callJavaMethod } from "./jvm.js";
import { ResourceWrapper } from "./resource.js";
import { toJsResult } from "./result.js";

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

export class KV extends ResourceWrapper {
  constructor(handle) {
    super(
      handle,
      (rawHandle) => _BINDINGS.closeKeyValue(rawHandle),
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
    await appendTypedArgs(args, { kType, vType, ignoreKey }, "getByRank");
    return toJsResult(await _BINDINGS.coreInvoke("get-by-rank", args));
  }

  async sampleKv(dbiName, n, { kType = null, vType = null, ignoreKey = null } = {}) {
    const args = [this.rawHandle(), dbiName, n];
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

  async clearDbi(dbiName) {
    await _BINDINGS.coreInvoke("clear-dbi", [this.rawHandle(), dbiName]);
  }

  async dropDbi(dbiName) {
    await _BINDINGS.coreInvoke("drop-dbi", [this.rawHandle(), dbiName]);
  }
}
