import { toJava, toJs } from "./convert.js";
import { Database } from "./database.js";
import { _BINDINGS } from "./interop.js";
import { javaBridgeModule } from "./jvm.js";
import { UdfDescriptor, descriptorData } from "./udf-value.js";

const DATABASE_CLASSES = new Set([
  "datalevin.db.DB",
  "datalevin.DatabaseValue"
]);

function descriptorKey(descriptor) {
  const normalized = descriptorData(descriptor);
  return [
    normalized[":udf/lang"],
    normalized[":udf/kind"],
    normalized[":udf/id"],
    normalized[":udf/version"] ?? ""
  ].join("\u0000");
}

function materializeJavaArgs(args) {
  if (Array.isArray(args)) {
    return args;
  }
  if (typeof args?.toArraySync === "function") {
    try {
      const items = args.toArraySync();
      return Array.isArray(items) ? items : Array.from(items);
    } catch {
      // Fall through to iterator traversal below.
    }
  }

  const items = [];
  const iterator = args.iteratorSync();
  while (iterator.hasNextSync()) {
    items.push(iterator.nextSync());
  }
  return items;
}

function materializeUdfArgs(args) {
  if (Array.isArray(args)) {
    return args;
  }
  if (typeof args?.sizeSync === "function" && typeof args?.getSync === "function") {
    const values = [];
    const size = Number(args.sizeSync());
    for (let index = 0; index < size; index += 1) {
      try {
        values.push(args.getSync(index));
      } catch (error) {
        const message = String(error?.message ?? error);
        if (index === 0 && message.includes("ClassNotFoundException: datalevin.db.DB")) {
          // Runtime jars before the DatabaseValue UDF bridge could not expose
          // the generated Clojure DB class to Node. Preserve their historical
          // null argument while newer runtimes provide an idiomatic Database.
          values.push(null);
          continue;
        }
        throw error;
      }
    }
    return values;
  }
  return materializeJavaArgs(args);
}

function javaClassName(value) {
  try {
    return value.getClassSync().getNameSync();
  } catch {
    return null;
  }
}

async function udfArgsToJs(args) {
  const values = materializeUdfArgs(args);
  return Promise.all(values.map(async (value) => {
    if (DATABASE_CLASSES.has(javaClassName(value))) {
      return new Database(value);
    }
    return toJs(value);
  }));
}

async function createProxy(fn) {
  const { newProxy } = await javaBridgeModule();
  return newProxy("datalevin.UdfFunction", {
    invoke: async (args) => {
      const values = await udfArgsToJs(args);
      const result = await fn(...values);
      return toJava(result === undefined ? null : result);
    }
  });
}

export function udfDescriptor(idOrDescriptor, {
  kind = ":query-fn",
  lang = ":java",
  version = null
} = {}) {
  return descriptorData(idOrDescriptor, { kind, lang, version });
}

export class UdfRegistry {
  constructor(handle) {
    this._handle = handle;
    this._proxies = new Map();
  }

  rawHandle() {
    return this._handle;
  }

  async register(descriptor, fn) {
    if (typeof fn !== "function") {
      throw new TypeError("fn must be a function");
    }

    const normalized = UdfDescriptor.from(descriptor, { defaultLang: "javascript" });
    const proxy = await createProxy(fn);
    await _BINDINGS.registerUdf(this._handle, normalized, proxy);
    this._proxies.set(descriptorKey(normalized), proxy);
    return fn;
  }

  async unregister(descriptor) {
    const normalized = UdfDescriptor.from(descriptor, { defaultLang: "javascript" });
    await _BINDINGS.unregisterUdf(this._handle, normalized);
    const key = descriptorKey(normalized);
    const proxy = this._proxies.get(key);
    if (proxy !== undefined) {
      proxy.reset();
      this._proxies.delete(key);
    }
  }

  async registered(descriptor) {
    return _BINDINGS.registeredUdf(
      this._handle,
      UdfDescriptor.from(descriptor, { defaultLang: "javascript" })
    );
  }

  async queryUdf(id, fn, options = {}) {
    return this.register(UdfDescriptor.queryFn(id, options), fn);
  }

  async predicateUdf(id, fn, options = {}) {
    return this.register(UdfDescriptor.predicate(id, options), fn);
  }

  async txUdf(id, fn, options = {}) {
    return this.register(UdfDescriptor.txFn(id, options), fn);
  }

  async analyzerUdf(id, fn, options = {}) {
    return this.register(UdfDescriptor.analyzer(id, options), fn);
  }

  async queryAnalyzerUdf(id, fn, options = {}) {
    return this.register(UdfDescriptor.queryAnalyzer(id, options), fn);
  }
}

export async function createUdfRegistry() {
  return new UdfRegistry(await _BINDINGS.createUdfRegistry());
}

export { UdfDescriptor };
