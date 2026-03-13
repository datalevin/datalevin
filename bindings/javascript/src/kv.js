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

  async clearDbi(dbiName) {
    await _BINDINGS.coreInvoke("clear-dbi", [this.rawHandle(), dbiName]);
  }

  async dropDbi(dbiName) {
    await _BINDINGS.coreInvoke("drop-dbi", [this.rawHandle(), dbiName]);
  }
}
