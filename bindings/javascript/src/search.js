import util from "node:util";

import { _BINDINGS } from "./interop.js";
import { toJsResult } from "./result.js";

export async function createAnalyzer(options = {}) {
  const {
    tokenizer = null,
    tokenFilters = null,
    extra = null,
    ...opts
  } = options ?? {};
  const merged = { ...opts };
  if (tokenizer !== null && tokenizer !== undefined) {
    merged[":tokenizer"] = tokenizer;
  }
  if (tokenFilters !== null && tokenFilters !== undefined) {
    merged[":token-filters"] = [...tokenFilters];
  }
  if (extra !== null && extra !== undefined) {
    Object.assign(merged, extra);
  }
  return _BINDINGS.searchUtilsCreateAnalyzer(merged);
}

export async function lowerCaseTokenFilter() {
  return _BINDINGS.searchUtilsLowerCaseTokenFilter();
}

export async function unaccentTokenFilter() {
  return _BINDINGS.searchUtilsUnaccentTokenFilter();
}

export async function createStopWordsTokenFilter(stopWordsOrPredicate) {
  return _BINDINGS.searchUtilsCreateStopWordsTokenFilter(stopWordsOrPredicate);
}

export async function enStopWordsTokenFilter() {
  return _BINDINGS.searchUtilsEnStopWordsTokenFilter();
}

export async function prefixTokenFilter() {
  return _BINDINGS.searchUtilsPrefixTokenFilter();
}

export async function createNgramTokenFilter(minGramSize, maxGramSize = null) {
  return _BINDINGS.searchUtilsCreateNgramTokenFilter(minGramSize, maxGramSize);
}

export async function createMinLengthTokenFilter(minLength) {
  return _BINDINGS.searchUtilsCreateMinLengthTokenFilter(minLength);
}

export async function createMaxLengthTokenFilter(maxLength) {
  return _BINDINGS.searchUtilsCreateMaxLengthTokenFilter(maxLength);
}

export async function createStemmingTokenFilter(language) {
  return _BINDINGS.searchUtilsCreateStemmingTokenFilter(language);
}

export async function createRegexpTokenizer(pattern) {
  return _BINDINGS.searchUtilsCreateRegexpTokenizer(pattern);
}

export class SearchEngine {
  constructor(handle) {
    this._handle = handle;
  }

  rawHandle() {
    if (this._handle === null) {
      throw new Error("search engine is closed.");
    }
    return this._handle;
  }

  async closed() {
    return this._handle === null;
  }

  async close() {
    this._handle = null;
  }

  async addDoc(docRef, docText, { checkExist = null } = {}) {
    await _BINDINGS.searchAddDoc(this.rawHandle(), docRef, docText, checkExist);
    return this;
  }

  async removeDoc(docRef) {
    await _BINDINGS.searchRemoveDoc(this.rawHandle(), docRef);
    return this;
  }

  async clearDocs() {
    await _BINDINGS.searchClearDocs(this.rawHandle());
    return this;
  }

  async docIndexed(docRef) {
    return Boolean(await _BINDINGS.searchDocIndexed(this.rawHandle(), docRef));
  }

  async docCount() {
    return toJsResult(await _BINDINGS.searchDocCount(this.rawHandle()));
  }

  async search(query, opts = null) {
    return toJsResult(await _BINDINGS.search(this.rawHandle(), query, opts));
  }

  async reIndex(opts = null) {
    this._handle = await _BINDINGS.searchReIndex(this.rawHandle(), opts);
    return this;
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
