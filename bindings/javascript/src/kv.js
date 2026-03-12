import { toEdnForm, toJava } from "./convert.js";
import { _BINDINGS } from "./interop.js";
import { callJavaMethod } from "./jvm.js";
import { ResourceWrapper } from "./resource.js";
import { toJsResult } from "./result.js";

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
    return toJsResult(await callJavaMethod(this.rawHandle(), "dir"));
  }

  async openDbi(name, opts = null) {
    if (opts !== null && opts !== undefined) {
      await callJavaMethod(this.rawHandle(), "openDbi", name, await toJava(opts));
      return;
    }
    await callJavaMethod(this.rawHandle(), "openDbi", name);
  }

  async openListDbi(name, opts = null) {
    if (opts !== null && opts !== undefined) {
      await callJavaMethod(this.rawHandle(), "openListDbi", name, await toJava(opts));
      return;
    }
    await callJavaMethod(this.rawHandle(), "openListDbi", name);
  }

  async listDbis() {
    return toJsResult(await callJavaMethod(this.rawHandle(), "listDbis"));
  }

  async entries(dbiName) {
    return toJsResult(await callJavaMethod(this.rawHandle(), "entries", dbiName));
  }

  async transact(txs, { dbiName = null, kType = null, vType = null } = {}) {
    if (dbiName === null && (kType !== null || vType !== null)) {
      throw new TypeError("kType and vType require dbiName for KV transact().");
    }
    if (vType !== null && kType === null) {
      throw new TypeError("vType requires kType for KV transact().");
    }

    const normalizedTxs = await _BINDINGS.kvTxs(txs);
    if (dbiName === null) {
      return toJsResult(await callJavaMethod(this.rawHandle(), "transact", normalizedTxs));
    }
    if (kType === null) {
      return toJsResult(await callJavaMethod(this.rawHandle(), "transact", dbiName, normalizedTxs));
    }
    if (vType === null) {
      return toJsResult(await callJavaMethod(this.rawHandle(), "transact", dbiName, normalizedTxs, kType));
    }
    return toJsResult(await callJavaMethod(this.rawHandle(), "transact", dbiName, normalizedTxs, kType, vType));
  }

  async getValue(dbiName, key, { kType = null, vType = null, ignoreKey = false } = {}) {
    if ((kType === null) !== (vType === null)) {
      throw new TypeError("kType and vType must be provided together for KV getValue().");
    }

    if (kType === null) {
      return toJsResult(await callJavaMethod(this.rawHandle(), "getValue", dbiName, await toJava(key)));
    }
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "getValue", dbiName, await toJava(key), kType, vType, Boolean(ignoreKey))
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

    const normalizedRange = await toEdnForm(keyRange);
    if (kType === null) {
      return toJsResult(await callJavaMethod(this.rawHandle(), "getRange", dbiName, normalizedRange));
    }
    return toJsResult(
      await callJavaMethod(
        this.rawHandle(),
        "getRange",
        dbiName,
        normalizedRange,
        kType,
        vType,
        limit,
        offset
      )
    );
  }

  async clearDbi(dbiName) {
    await callJavaMethod(this.rawHandle(), "clearDbi", dbiName);
  }

  async dropDbi(dbiName) {
    await callJavaMethod(this.rawHandle(), "dropDbi", dbiName);
  }
}
