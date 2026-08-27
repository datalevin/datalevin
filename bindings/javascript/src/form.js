function tokenText(value, prefix) {
  if (typeof value !== "string") {
    throw new TypeError(`Datalevin token names must be strings, got ${typeof value}.`);
  }
  const text = prefix && value.startsWith(prefix) ? value.slice(prefix.length) : value;
  if (text.length === 0) {
    throw new TypeError("Datalevin token names must not be empty.");
  }
  if (/\s/.test(text)) {
    throw new TypeError(`Datalevin token names must not contain whitespace: ${JSON.stringify(value)}.`);
  }
  return text;
}

function tokenPool() {
  const references = new Map();
  const finalizer = new FinalizationRegistry(({ name, reference }) => {
    if (references.get(name) === reference) {
      references.delete(name);
    }
  });

  return {
    get(name) {
      const value = references.get(name)?.deref();
      if (value === undefined) {
        references.delete(name);
      }
      return value;
    },
    set(name, value) {
      const reference = new WeakRef(value);
      references.set(name, reference);
      finalizer.register(value, { name, reference });
    }
  };
}

const KEYWORDS = tokenPool();
const DATALOG_SYMBOLS = tokenPool();

export class Keyword {
  constructor(name) {
    const text = tokenText(name, ":");
    const existing = KEYWORDS.get(text);
    if (existing !== undefined) {
      return existing;
    }
    this.name = text;
    Object.freeze(this);
    KEYWORDS.set(text, this);
  }

  toString() {
    return `:${this.name}`;
  }
}

export class DatalogSymbol {
  constructor(name) {
    const text = tokenText(name, "");
    const existing = DATALOG_SYMBOLS.get(text);
    if (existing !== undefined) {
      return existing;
    }
    this.name = text;
    Object.freeze(this);
    DATALOG_SYMBOLS.set(text, this);
  }

  toString() {
    return this.name;
  }
}

export class Form {
  toForm() {
    throw new TypeError(`${this.constructor.name}.toForm() is not implemented.`);
  }
}

const IMMUTABLE_SNAPSHOTS = new WeakSet();
const DATE_MUTATORS = [
  "setDate",
  "setFullYear",
  "setHours",
  "setMilliseconds",
  "setMinutes",
  "setMonth",
  "setSeconds",
  "setTime",
  "setUTCDate",
  "setUTCFullYear",
  "setUTCHours",
  "setUTCMilliseconds",
  "setUTCMinutes",
  "setUTCMonth",
  "setUTCSeconds",
  "setYear"
];
const BYTE_MUTATORS = new Set(["copyWithin", "fill", "reverse", "set", "sort"]);

function mutationError() {
  throw new TypeError("Datalevin form snapshots are immutable.");
}

function readonlyMap(entries) {
  const target = new Map(entries);
  Object.freeze(target);
  let result;
  result = new Proxy(target, {
    get(current, property) {
      if (property === "set" || property === "delete" || property === "clear") {
        return mutationError;
      }
      if (property === "valueOf") {
        return () => result;
      }
      if (property === "forEach") {
        return (callback, thisArg) => current.forEach(
          (item, key) => callback.call(thisArg, item, key, result)
        );
      }
      const item = Reflect.get(current, property, current);
      return typeof item === "function" ? item.bind(current) : item;
    },
    set: mutationError,
    defineProperty: mutationError,
    deleteProperty: mutationError,
    setPrototypeOf: mutationError
  });
  IMMUTABLE_SNAPSHOTS.add(result);
  return result;
}

function readonlySet(items) {
  const target = new Set(items);
  Object.freeze(target);
  let result;
  result = new Proxy(target, {
    get(current, property) {
      if (property === "add" || property === "delete" || property === "clear") {
        return mutationError;
      }
      if (property === "valueOf") {
        return () => result;
      }
      if (property === "forEach") {
        return (callback, thisArg) => current.forEach(
          (item) => callback.call(thisArg, item, item, result)
        );
      }
      const item = Reflect.get(current, property, current);
      return typeof item === "function" ? item.bind(current) : item;
    },
    set: mutationError,
    defineProperty: mutationError,
    deleteProperty: mutationError,
    setPrototypeOf: mutationError
  });
  IMMUTABLE_SNAPSHOTS.add(result);
  return result;
}

function readonlyDate(value) {
  const target = new Date(value.getTime());
  Object.freeze(target);
  const result = new Proxy(target, {
    get(current, property) {
      if (DATE_MUTATORS.includes(property)) {
        return mutationError;
      }
      const item = Reflect.get(current, property, current);
      return typeof item === "function" ? item.bind(current) : item;
    },
    set: mutationError,
    defineProperty: mutationError,
    deleteProperty: mutationError,
    setPrototypeOf: mutationError
  });
  IMMUTABLE_SNAPSHOTS.add(result);
  return result;
}

function readonlyBytes(value) {
  const target = Uint8Array.from(value);
  let result;
  result = new Proxy(target, {
    get(current, property) {
      if (property === "buffer") {
        return current.buffer.slice(
          current.byteOffset,
          current.byteOffset + current.byteLength
        );
      }
      if (property === "subarray") {
        return (...args) => Uint8Array.from(current.subarray(...args));
      }
      if (property === "valueOf") {
        return () => result;
      }
      if (BYTE_MUTATORS.has(property)) {
        return mutationError;
      }
      const item = Reflect.get(current, property, current);
      return typeof item === "function" ? item.bind(current) : item;
    },
    set: mutationError,
    defineProperty: mutationError,
    deleteProperty: mutationError,
    setPrototypeOf: mutationError
  });
  IMMUTABLE_SNAPSHOTS.add(result);
  return result;
}

function plainObject(value) {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function withContainer(value, active, build) {
  if (active.has(value)) {
    throw new TypeError("Datalevin forms cannot contain cyclic containers.");
  }
  active.add(value);
  try {
    return build();
  } finally {
    active.delete(value);
  }
}

export function immutableSnapshot(value, active = new Set()) {
  if (
    value === null
    || (typeof value !== "object" && typeof value !== "function")
    || value instanceof Keyword
    || value instanceof DatalogSymbol
    || value instanceof Form
    || IMMUTABLE_SNAPSHOTS.has(value)
  ) {
    return value;
  }

  if (value instanceof Date) {
    return readonlyDate(value);
  }
  if (value instanceof Uint8Array) {
    return readonlyBytes(value);
  }
  if (Array.isArray(value)) {
    return withContainer(value, active, () => {
      const result = value.map((item) => immutableSnapshot(item, active));
      IMMUTABLE_SNAPSHOTS.add(result);
      return Object.freeze(result);
    });
  }
  if (value instanceof Map) {
    return withContainer(value, active, () => readonlyMap(
      Array.from(
        value,
        ([key, item]) => [
          immutableSnapshot(key, active),
          immutableSnapshot(item, active)
        ]
      )
    ));
  }
  if (value instanceof Set) {
    return withContainer(value, active, () => readonlySet(
      Array.from(value, (item) => immutableSnapshot(item, active))
    ));
  }
  if (plainObject(value)) {
    return withContainer(value, active, () => {
      const result = Object.create(Object.getPrototypeOf(value));
      for (const [key, item] of Object.entries(value)) {
        Object.defineProperty(result, key, {
          configurable: false,
          enumerable: true,
          value: immutableSnapshot(item, active),
          writable: false
        });
      }
      IMMUTABLE_SNAPSHOTS.add(result);
      return Object.freeze(result);
    });
  }

  // Backend handles and other non-structural host objects are atomic values.
  return value;
}

export class RawForm extends Form {
  constructor(value) {
    super();
    this.value = immutableSnapshot(value);
    Object.freeze(this);
  }

  toForm() {
    return this.value;
  }
}

export function formData(value) {
  if (value instanceof Form) {
    return formData(value.toForm());
  }
  if (value instanceof Keyword || value instanceof DatalogSymbol) {
    return value.toString();
  }
  if (value instanceof Map) {
    return new Map(Array.from(value, ([key, item]) => [formData(key), formData(item)]));
  }
  if (value instanceof Set) {
    return new Set(Array.from(value, formData));
  }
  if (Array.isArray(value)) {
    return value.map(formData);
  }
  if (plainObject(value)) {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, formData(item)])
    );
  }
  return value;
}
