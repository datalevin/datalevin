import { isJavaObject, toJs } from "./convert.js";
import { _BINDINGS } from "./interop.js";
import { classes } from "./jvm.js";

function materializeJavaCollection(collection) {
  if (typeof collection?.toArraySync === "function") {
    try {
      const items = collection.toArraySync();
      return Array.isArray(items) ? items : Array.from(items);
    } catch {
      // Fall back to iterator-based traversal when array materialization is unsupported.
    }
  }

  const items = [];
  const iterator = collection.iteratorSync();
  while (iterator.hasNextSync()) {
    items.push(iterator.nextSync());
  }
  return items;
}

function instanceOf(value, type) {
  if (value === null || typeof value !== "object" || typeof value.instanceOf !== "function") {
    return false;
  }

  try {
    return value.instanceOf(type);
  } catch {
    return false;
  }
}

function isPlainObject(value) {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

async function entityValue(value) {
  if (
    value === null
    || typeof value === "boolean"
    || typeof value === "number"
    || typeof value === "string"
    || typeof value === "bigint"
  ) {
    return value;
  }

  if (Array.isArray(value)) {
    return Promise.all(value.map((item) => entityValue(item)));
  }

  if (!isJavaObject(value)) {
    if (value instanceof Set) {
      const items = [];
      for (const item of value) {
        items.push(await entityValue(item));
      }
      return new Set(items);
    }
    if (value instanceof Map) {
      const entries = [];
      for (const [key, item] of value) {
        entries.push([await entityValue(key), await entityValue(item)]);
      }
      return new Map(entries);
    }
    if (isPlainObject(value)) {
      const result = {};
      for (const [key, item] of Object.entries(value)) {
        result[key] = await entityValue(item);
      }
      return result;
    }
    return value;
  }

  if (await _BINDINGS.entityIs(value)) {
    return new Entity(value);
  }

  const cls = await classes();

  if (instanceOf(value, cls.mapType) || typeof value.entrySetSync === "function") {
    const entries = [];
    for (const entry of materializeJavaCollection(value.entrySetSync())) {
      entries.push([await toJs(entry.getKeySync()), await entityValue(entry.getValueSync())]);
    }

    if (entries.every(([key]) => typeof key === "string")) {
      return Object.fromEntries(entries);
    }
    return new Map(entries);
  }

  if (instanceOf(value, cls.setType)) {
    const items = [];
    for (const item of materializeJavaCollection(value)) {
      items.push(await entityValue(item));
    }
    return new Set(items);
  }

  if (
    instanceOf(value, cls.listType)
    || instanceOf(value, cls.collectionType)
    || typeof value.iteratorSync === "function"
  ) {
    const items = [];
    for (const item of materializeJavaCollection(value)) {
      items.push(await entityValue(item));
    }
    return items;
  }

  return toJs(value);
}

export class Entity {
  constructor(handle) {
    this.handle = handle;
  }

  rawHandle() {
    return this.handle;
  }

  async id() {
    return toJs(await _BINDINGS.entityId(this.rawHandle()));
  }

  async has(attr) {
    return _BINDINGS.entityContains(this.rawHandle(), attr);
  }

  async get(attr, defaultValue = undefined) {
    if (!(await this.has(attr))) {
      return defaultValue;
    }
    return entityValue(await _BINDINGS.entityGet(this.rawHandle(), attr));
  }

  async touch() {
    return toJs(await _BINDINGS.entityTouch(this.rawHandle()));
  }

  async entries() {
    const value = await this.touch();
    if (value instanceof Map) {
      return Array.from(value.entries());
    }
    return Object.entries(value ?? {});
  }

  async keys() {
    return (await this.entries()).map(([key]) => key);
  }

  async values() {
    return (await this.entries()).map(([, value]) => value);
  }

  toString() {
    return "<Entity lazy>";
  }
}
