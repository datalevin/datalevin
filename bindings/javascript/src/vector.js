import util from "node:util";

import { _BINDINGS } from "./interop.js";
import { toJsResult } from "./result.js";

export class VectorIndex {
  constructor(handle) {
    this._handle = handle;
  }

  rawHandle() {
    if (this._handle === null) {
      throw new Error("vector index is closed.");
    }
    return this._handle;
  }

  async closed() {
    if (this._handle === null) {
      return true;
    }
    return Boolean(await _BINDINGS.vectorIndexClosed(this.rawHandle()));
  }

  async close() {
    if (this._handle === null) {
      return;
    }
    const handle = this._handle;
    this._handle = null;
    await _BINDINGS.closeVectorIndex(handle);
  }

  async addVec(vecRef, vecData) {
    await _BINDINGS.vectorAddVec(this.rawHandle(), vecRef, vecData);
    return this;
  }

  async removeVec(vecRef) {
    await _BINDINGS.vectorRemoveVec(this.rawHandle(), vecRef);
    return this;
  }

  async vecIndexed(vecRef) {
    return Boolean(await _BINDINGS.vectorIndexed(this.rawHandle(), vecRef));
  }

  async searchVec(queryVec, opts = null) {
    return toJsResult(await _BINDINGS.vectorSearch(this.rawHandle(), queryVec, opts));
  }

  async reIndex(opts = null) {
    this._handle = await _BINDINGS.vectorReIndex(this.rawHandle(), opts);
    return this;
  }

  async clear() {
    await _BINDINGS.vectorClear(this.rawHandle());
    this._handle = null;
    return this;
  }

  async forceCheckpoint() {
    await _BINDINGS.vectorForceCheckpoint(this.rawHandle());
    return this;
  }

  async info() {
    return toJsResult(await _BINDINGS.vectorInfo(this.rawHandle()));
  }

  async checkpointState() {
    return toJsResult(await _BINDINGS.vectorCheckpointState(this.rawHandle()));
  }

  toString() {
    return `<${this.constructor.name} ${this._handle === null ? "closed" : "open"}>`;
  }

  [util.inspect.custom]() {
    return this.toString();
  }

  async [Symbol.asyncDispose]() {
    await this.close();
  }
}
