import util from "node:util";

import { _BINDINGS } from "./interop.js";
import { toJsResult } from "./result.js";

export class SearchIndexWriter {
  constructor(handle) {
    this._handle = handle;
  }

  rawHandle() {
    if (this._handle === null) {
      throw new Error("search index writer is closed.");
    }
    return this._handle;
  }

  async closed() {
    return this._handle === null;
  }

  async close() {
    this._handle = null;
  }

  async write(docRef, docText) {
    await _BINDINGS.searchWrite(this.rawHandle(), docRef, docText);
    return this;
  }

  async commit() {
    const result = await toJsResult(await _BINDINGS.searchCommit(this.rawHandle()));
    this._handle = null;
    return result;
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
