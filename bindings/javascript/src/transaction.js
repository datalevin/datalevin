import { Form, RawForm, formData } from "./form.js";
import { kw } from "./query.js";

const MISSING = Symbol("missing");

function attribute(value) {
  return typeof value === "string" ? kw(value) : value;
}

function callable(value) {
  return typeof value === "string" ? kw(value) : value;
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

export function ensure(predicateValue, ...args) {
  return new TxItem([kw("db/ensure"), callable(predicateValue), ...args]);
}

export function patchIdoc(
  entityId,
  attributeName,
  patch,
  { oldValue = MISSING } = {}
) {
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
  patchIdoc,
  raw,
  retract,
  retractAttribute,
  retractEntity
});
