import { Form, Keyword, RawForm, formData } from "./form.js";
import { kw } from "./query.js";

const MISSING = Symbol("missing");
const PATCH_UPDATE_OPERATIONS = new Set([
  "conj",
  "merge",
  "assoc",
  "dissoc",
  "inc",
  "dec"
]);

function attribute(value) {
  return typeof value === "string" ? kw(value) : value;
}

function callable(value) {
  return typeof value === "string" ? kw(value) : value;
}

function patchPath(path) {
  const vectorPath = Array.isArray(path);
  if (!vectorPath && typeof path !== "string" && !(path instanceof Keyword)) {
    throw new TypeError(
      "An idoc patch path must be a string, keyword, or array."
    );
  }
  const segments = vectorPath ? path : [path];
  if (segments.length === 0) {
    throw new TypeError("An idoc patch path must not be empty.");
  }
  for (const segment of segments) {
    const integer = (
      (typeof segment === "number" && Number.isSafeInteger(segment))
      || typeof segment === "bigint"
    );
    if (integer) {
      if (segment < 0) {
        throw new TypeError("An idoc patch path index must be non-negative.");
      }
      continue;
    }
    if (segment instanceof Keyword) {
      if (segment.name === "?" || segment.name === "*") {
        throw new TypeError("An idoc patch path does not allow keyword wildcards.");
      }
      continue;
    }
    if (typeof segment !== "string") {
      throw new TypeError(
        "Idoc patch path segments must be strings, keywords, or integers."
      );
    }
  }
  return vectorPath ? Object.freeze([...path]) : path;
}

function patchUpdateOperation(operation) {
  const result = typeof operation === "string" ? kw(operation) : operation;
  if (!(result instanceof Keyword)) {
    throw new TypeError(
      "An idoc patch update operation must be a keyword or keyword name."
    );
  }
  if (!PATCH_UPDATE_OPERATIONS.has(result.name)) {
    throw new TypeError(
      `Unknown idoc patch update operation ${result}; expected one of ${
        [...PATCH_UPDATE_OPERATIONS].sort().join(", ")
      }.`
    );
  }
  return result;
}

export class TxItem extends Form {
  constructor(form) {
    super();
    this.form = form;
    Object.freeze(this);
  }

  toForm() {
    return this.form;
  }
}

export class LookupRef extends Form {
  constructor(attributeName, value) {
    super();
    this.attribute = attribute(attributeName);
    this.value = value;
    Object.freeze(this);
  }

  toForm() {
    return [this.attribute, this.value];
  }

  asData() {
    return formData(this);
  }
}

export class PatchOp extends Form {
  constructor(form) {
    super();
    this.form = Object.freeze([...form]);
    Object.freeze(this);
  }

  toForm() {
    return this.form;
  }

  asData() {
    return formData(this);
  }
}

export class TxData extends Form {
  constructor(items = []) {
    super();
    this.items = Object.freeze([...items]);
    Object.freeze(this);
  }

  get length() {
    return this.items.length;
  }

  at(index) {
    return this.items.at(index);
  }

  [Symbol.iterator]() {
    return this.items[Symbol.iterator]();
  }

  toForm() {
    return this.items.map((item) => item instanceof Form ? item.toForm() : item);
  }

  asData() {
    return formData(this);
  }
}

export function data(...items) {
  if (items.length === 1 && Array.isArray(items[0])) {
    items = items[0];
  }
  return new TxData(items);
}

export function entity(dbId = null, attrs = {}) {
  const entries = attrs instanceof Map ? attrs.entries() : Object.entries(attrs);
  const result = new Map();
  for (const [key, value] of entries) {
    result.set(attribute(key), value);
  }
  if (dbId !== null && dbId !== undefined) {
    result.set(kw("db/id"), dbId);
  }
  return new TxItem(result);
}

export function lookupRef(attributeName, value) {
  return new LookupRef(attributeName, value);
}

export function add(entityId, attributeName, value) {
  return new TxItem([kw("db/add"), entityId, attribute(attributeName), value]);
}

export function retract(entityId, attributeName, value) {
  return new TxItem([kw("db/retract"), entityId, attribute(attributeName), value]);
}

export function retractAttribute(entityId, attributeName) {
  return new TxItem([kw("db.fn/retractAttribute"), entityId, attribute(attributeName)]);
}

export function retractEntity(entityId) {
  return new TxItem([kw("db/retractEntity"), entityId]);
}

export function compareAndSwap(entityId, attributeName, oldValue, newValue) {
  return new TxItem([
    kw("db.fn/cas"),
    entityId,
    attribute(attributeName),
    oldValue,
    newValue
  ]);
}

export const cas = compareAndSwap;

export function call(functionValue, ...args) {
  return new TxItem([kw("db.fn/call"), callable(functionValue), ...args]);
}

export function invoke(functionValue, ...args) {
  return new TxItem([callable(functionValue), ...args]);
}

export function ensure(predicateValue, ...args) {
  return new TxItem([kw("db/ensure"), callable(predicateValue), ...args]);
}

export function patchSet(path, value) {
  return new PatchOp([kw("set"), patchPath(path), value]);
}

export function patchUnset(path) {
  return new PatchOp([kw("unset"), patchPath(path)]);
}

export function patchUpdate(path, operation, ...args) {
  return new PatchOp([
    kw("update"),
    patchPath(path),
    patchUpdateOperation(operation),
    ...args
  ]);
}

export function patchIdoc(
  entityId,
  attributeName,
  patch,
  { oldValue = MISSING } = {}
) {
  if (patch instanceof PatchOp) {
    patch = [patch];
  }
  const result = [kw("db.fn/patchIdoc"), entityId, attribute(attributeName)];
  if (oldValue !== MISSING) {
    result.push(oldValue);
  }
  result.push(patch);
  return new TxItem(result);
}

export function raw(form) {
  return new RawForm(form);
}

export const tx = Object.freeze({
  add,
  call,
  cas,
  compareAndSwap,
  data,
  ensure,
  entity,
  invoke,
  lookupRef,
  patchIdoc,
  patchSet,
  patchUnset,
  patchUpdate,
  raw,
  retract,
  retractAttribute,
  retractEntity
});
