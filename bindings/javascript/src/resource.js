import util from "node:util";

export class ResourceWrapper {
  constructor(handle, closeFn, closedFn, label) {
    this._handle = handle;
    this._closeFn = closeFn;
    this._closedFn = closedFn;
    this._label = label;
    this._closed = false;
  }

  rawHandle() {
    return this._handle;
  }

  async close() {
    if (this._closed) {
      return;
    }
    await this._closeFn(this._handle);
    this._closed = true;
  }

  async closed() {
    if (this._closed) {
      return true;
    }
    this._closed = Boolean(await this._closedFn(this._handle));
    return this._closed;
  }

  toString() {
    return `<${this.constructor.name} ${this._closed ? "closed" : "open"}>`;
  }

  [util.inspect.custom]() {
    return this.toString();
  }

  async [Symbol.asyncDispose]() {
    await this.close();
  }
}
