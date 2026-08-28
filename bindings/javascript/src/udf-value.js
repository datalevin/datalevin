import {
  DatalogSymbol,
  Form,
  Keyword,
  formData,
  immutableSnapshot
} from "./form.js";

export const UDF_KINDS = Object.freeze(new Set([
  "query-fn",
  "predicate",
  "tx-fn",
  "analyzer",
  "query-analyzer"
]));

const ALIASES = new Map([
  [":udf/lang", "lang"],
  ["lang", "lang"],
  [":udf/kind", "kind"],
  ["kind", "kind"],
  [":udf/id", "id"],
  ["id", "id"],
  ["udfId", "id"],
  [":udf/version", "version"],
  ["version", "version"]
]);
const MISSING = Symbol("missing-udf-value");
const DESCRIPTORS = new Map();
const DESCRIPTOR_FINALIZER = new FinalizationRegistry(({ key, reference }) => {
  if (DESCRIPTORS.get(key) === reference) {
    DESCRIPTORS.delete(key);
  }
});

function descriptorIdentity(data) {
  const version = data[":udf/version"];
  const versionType = version instanceof Keyword ? "keyword" : typeof version;
  return JSON.stringify([
    data[":udf/lang"],
    data[":udf/kind"],
    data[":udf/id"],
    versionType,
    version === undefined ? null : String(version)
  ]);
}

function keywordText(value, field) {
  let text;
  if (value instanceof Keyword) {
    text = value.name;
  } else if (typeof value === "string") {
    text = value.startsWith(":") ? value.slice(1) : value;
  } else {
    throw new TypeError(`UDF descriptor ${field} must be a keyword or string.`);
  }
  if (text.length === 0) {
    throw new TypeError(`UDF descriptor ${field} must not be empty.`);
  }
  if (/\s/.test(text)) {
    throw new TypeError(`UDF descriptor ${field} must not contain whitespace.`);
  }
  return `:${text}`;
}

function plainObject(value) {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function descriptorEntries(value) {
  if (value instanceof UdfDescriptor) {
    return Object.entries(value.asData());
  }
  if (value instanceof Map) {
    return Array.from(value.entries());
  }
  if (plainObject(value)) {
    return Object.entries(value);
  }
  return null;
}

function descriptorFields(value) {
  const entries = descriptorEntries(value);
  if (entries === null) {
    return { id: value };
  }

  const fields = {};
  const unknown = [];
  for (const [key, item] of entries) {
    const keyText = key instanceof Keyword ? key.toString() : key;
    const canonical = ALIASES.get(keyText);
    if (canonical === undefined) {
      unknown.push(key);
      continue;
    }
    if (Object.hasOwn(fields, canonical) && fields[canonical] !== item) {
      throw new TypeError(`Conflicting UDF descriptor values for ${canonical}.`);
    }
    fields[canonical] = item;
  }
  if (unknown.length > 0) {
    throw new TypeError(`Unsupported UDF descriptor key(s): ${unknown.join(", ")}.`);
  }
  return fields;
}

export function descriptorData(value, {
  kind = "query-fn",
  lang = "java",
  version = MISSING
} = {}) {
  const fields = descriptorFields(value);
  if (fields.id === null || fields.id === undefined) {
    throw new TypeError("udf id is required");
  }

  const normalizedLang = keywordText(fields.lang ?? lang, ":udf/lang");
  const normalizedKind = keywordText(fields.kind ?? kind, ":udf/kind");
  if (!UDF_KINDS.has(normalizedKind.slice(1))) {
    throw new TypeError(
      `Unsupported UDF kind ${normalizedKind}; expected one of ${
        [...UDF_KINDS].sort().join(", ")
      }.`
    );
  }
  const normalizedId = keywordText(fields.id, ":udf/id");
  const normalizedVersion = Object.hasOwn(fields, "version")
    ? fields.version
    : version;
  if (normalizedVersion !== MISSING && normalizedVersion !== null && normalizedVersion !== undefined) {
    const integer = (
      (typeof normalizedVersion === "number" && Number.isSafeInteger(normalizedVersion))
      || typeof normalizedVersion === "bigint"
    );
    if (!(normalizedVersion instanceof Keyword)
        && typeof normalizedVersion !== "string"
        && !integer) {
      throw new TypeError(
        "UDF descriptor :udf/version must be a keyword, string, integer, or null."
      );
    }
  }

  const result = {
    ":udf/lang": normalizedLang,
    ":udf/kind": normalizedKind,
    ":udf/id": normalizedId
  };
  if (normalizedVersion !== MISSING && normalizedVersion !== null && normalizedVersion !== undefined) {
    result[":udf/version"] = normalizedVersion;
  }
  return result;
}

export class UdfDescriptor extends Form {
  constructor(value = null, {
    kind = "query-fn",
    lang = "javascript",
    version = MISSING
  } = {}) {
    super();
    const data = descriptorData(value, { kind, lang, version });
    const identity = descriptorIdentity(data);
    const existing = DESCRIPTORS.get(identity)?.deref();
    if (existing !== undefined) {
      return existing;
    }
    const formEntries = Object.entries(data).map(([key, item]) => {
      let formItem = item;
      if (key !== ":udf/version"
          || (typeof item === "string" && item.startsWith(":"))) {
        formItem = item instanceof Keyword ? item : new Keyword(item);
      }
      return [new Keyword(key), formItem];
    });
    this.lang = data[":udf/lang"];
    this.kind = data[":udf/kind"];
    this.udfId = data[":udf/id"];
    this.version = data[":udf/version"] ?? null;
    this.data = immutableSnapshot(data);
    this.form = immutableSnapshot(new Map(formEntries));
    Object.freeze(this);
    const reference = new WeakRef(this);
    DESCRIPTORS.set(identity, reference);
    DESCRIPTOR_FINALIZER.register(this, { key: identity, reference });
  }

  toForm() {
    return this.form;
  }

  asData() {
    return formData(this.data);
  }

  static of(kind, udfId, { lang = "javascript", version = MISSING } = {}) {
    return new UdfDescriptor(udfId, { kind, lang, version });
  }

  static queryFn(udfId, options = {}) {
    return UdfDescriptor.of("query-fn", udfId, options);
  }

  static predicate(udfId, options = {}) {
    return UdfDescriptor.of("predicate", udfId, options);
  }

  static txFn(udfId, options = {}) {
    return UdfDescriptor.of("tx-fn", udfId, options);
  }

  static analyzer(udfId, options = {}) {
    return UdfDescriptor.of("analyzer", udfId, options);
  }

  static queryAnalyzer(udfId, options = {}) {
    return UdfDescriptor.of("query-analyzer", udfId, options);
  }

  static from(value, { defaultLang = "javascript" } = {}) {
    return value instanceof UdfDescriptor
      ? value
      : new UdfDescriptor(value, { lang: defaultLang });
  }
}

export function udfReference(value) {
  if (typeof value === "string") {
    return new Keyword(value);
  }
  if (value instanceof Map || plainObject(value)) {
    return UdfDescriptor.from(value, { defaultLang: "java" });
  }
  if (
    value instanceof Keyword
    || value instanceof DatalogSymbol
    || value instanceof Form
  ) {
    return value;
  }
  throw new TypeError(
    "A UDF reference must be a descriptor, keyword id, or query variable."
  );
}

export { MISSING as UDF_VALUE_MISSING };
