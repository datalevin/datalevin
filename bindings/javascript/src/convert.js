import { classes, javaBridgeModule } from "./jvm.js";

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
const FORM_SYMBOL_STRINGS = new Set(["*", "...", ".", "$", "%", "_"]);

function isPlainObject(value) {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
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

async function toJavaMap(entries, converter) {
  const cls = await classes();
  const jmap = new cls.linkedHashMap();
  for (const [key, item] of entries) {
    jmap.putSync(await converter(key), await converter(item));
  }
  return jmap;
}

async function toJavaSet(items, converter) {
  const cls = await classes();
  const jset = new cls.linkedHashSet();
  for (const item of items) {
    jset.addSync(await converter(item));
  }
  return jset;
}

async function toJavaList(items, converter) {
  const cls = await classes();
  const jlist = new cls.arrayList();
  for (const item of items) {
    jlist.addSync(await converter(item));
  }
  return jlist;
}

export function isJavaObject(value) {
  return value !== null
    && (typeof value === "object" || typeof value === "function")
    && (typeof value.getClassSync === "function" || typeof value.instanceOf === "function");
}

export async function toJava(value) {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return value;
  }

  if (typeof value === "number") {
    if (!Number.isInteger(value)) {
      return value;
    }
    if (!Number.isSafeInteger(value)) {
      throw new RangeError("Unsafe integer number; use bigint for Datalevin integer values.");
    }
    const cls = await classes();
    return new cls.longClass(String(value));
  }

  if (typeof value === "bigint") {
    const cls = await classes();
    if (value >= INT64_MIN && value <= INT64_MAX) {
      return new cls.longClass(value.toString());
    }
    return new cls.bigInteger(value.toString());
  }

  if (isJavaObject(value)) {
    return value;
  }

  if (typeof value?.rawHandle === "function") {
    return value.rawHandle();
  }

  if (value instanceof Date) {
    const cls = await classes();
    return cls.instant.parseSync(value.toISOString());
  }

  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    const bridge = await javaBridgeModule();
    if (typeof bridge.newArray === "function") {
      return bridge.newArray("byte", Array.from(value));
    }
    return Buffer.from(value);
  }

  if (value instanceof Map) {
    return toJavaMap(value.entries(), toJava);
  }

  if (value instanceof Set) {
    return toJavaSet(value, toJava);
  }

  if (Array.isArray(value)) {
    return toJavaList(value, toJava);
  }

  if (isPlainObject(value)) {
    return toJavaMap(Object.entries(value), toJava);
  }

  return value;
}

export async function toJs(value) {
  if (
    value === null
    || typeof value === "boolean"
    || typeof value === "number"
    || typeof value === "string"
    || typeof value === "bigint"
  ) {
    return value;
  }

  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value);
  }

  if (Array.isArray(value)) {
    return Promise.all(value.map((item) => toJs(item)));
  }

  if (!isJavaObject(value)) {
    if (isPlainObject(value)) {
      const result = {};
      for (const [key, item] of Object.entries(value)) {
        result[key] = await toJs(item);
      }
      return result;
    }
    return value;
  }

  const cls = await classes();

  if (instanceOf(value, cls.uuid)) {
    return value.toStringSync();
  }

  if (instanceOf(value, cls.instant)) {
    return new Date(value.toStringSync());
  }

  if (instanceOf(value, cls.date)) {
    return new Date(Number(value.getTimeSync()));
  }

  if (instanceOf(value, cls.bigInteger)) {
    return BigInt(value.toStringSync());
  }

  if (instanceOf(value, cls.bigDecimal)) {
    return value.toStringSync();
  }

  if (instanceOf(value, cls.keywordType) || instanceOf(value, cls.symbolType)) {
    return value.toStringSync();
  }

  if (instanceOf(value, cls.mapType) || typeof value.entrySetSync === "function") {
    const entries = [];
    const iterator = value.entrySetSync().iteratorSync();
    while (iterator.hasNextSync()) {
      const entry = iterator.nextSync();
      entries.push([await toJs(entry.getKeySync()), await toJs(entry.getValueSync())]);
    }

    if (entries.every(([key]) => typeof key === "string")) {
      return Object.fromEntries(entries);
    }
    return new Map(entries);
  }

  if (instanceOf(value, cls.setType)) {
    const items = [];
    const iterator = value.iteratorSync();
    while (iterator.hasNextSync()) {
      items.push(await toJs(iterator.nextSync()));
    }
    return new Set(items);
  }

  if (
    instanceOf(value, cls.listType)
    || instanceOf(value, cls.collectionType)
    || typeof value.iteratorSync === "function"
  ) {
    const items = [];
    const iterator = value.iteratorSync();
    while (iterator.hasNextSync()) {
      items.push(await toJs(iterator.nextSync()));
    }
    return items;
  }

  return value;
}

async function toJavaFormMap(entries, converter) {
  const cls = await classes();
  const jmap = new cls.linkedHashMap();
  for (const [key, item] of entries) {
    jmap.putSync(await converter(key), await converter(item));
  }
  return jmap;
}

export async function toEdnForm(value) {
  if (value === null || isJavaObject(value)) {
    return value;
  }

  if (typeof value?.rawHandle === "function") {
    return value.rawHandle();
  }

  if (typeof value === "string") {
    const cls = await classes();
    if (value.startsWith(":")) {
      return cls.interop.keywordSync(value);
    }
    if (value.startsWith("?") || FORM_SYMBOL_STRINGS.has(value)) {
      return cls.interop.symbolSync(value);
    }
    return value;
  }

  if (value instanceof Map) {
    return toJavaFormMap(value.entries(), toEdnForm);
  }

  if (value instanceof Set) {
    return toJavaSet(value, toEdnForm);
  }

  if (Array.isArray(value)) {
    return toJavaList(value, toEdnForm);
  }

  if (isPlainObject(value)) {
    return toJavaFormMap(Object.entries(value), toEdnForm);
  }

  return toJava(value);
}

export async function toQueryInput(value) {
  if (value === null || isJavaObject(value)) {
    return value;
  }

  if (typeof value?.rawHandle === "function") {
    return value.rawHandle();
  }

  if (typeof value === "string") {
    const cls = await classes();
    if (value.startsWith(":")) {
      return cls.interop.keywordSync(value);
    }
    return value;
  }

  if (value instanceof Map) {
    return toJavaFormMap(value.entries(), toQueryInput);
  }

  if (value instanceof Set) {
    return toJavaSet(value, toQueryInput);
  }

  if (Array.isArray(value)) {
    return toJavaList(value, toQueryInput);
  }

  if (isPlainObject(value)) {
    return toJavaFormMap(Object.entries(value), toQueryInput);
  }

  return toJava(value);
}
