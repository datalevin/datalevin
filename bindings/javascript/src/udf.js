import { toJava, toJs } from "./convert.js";
import { Database } from "./database.js";
import { _BINDINGS } from "./interop.js";
import { javaBridgeModule } from "./jvm.js";
import { UdfDescriptor, descriptorData } from "./udf-value.js";
import { NativeCodec, createNativeEqualityProxy, descriptorKey, withNativeScope } from "./native.js";

const DATABASE_CLASSES = new Set([
  "datalevin.db.DB",
  "datalevin.DatabaseValue"
]);

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

async function createProxy(fn, descriptor, bindings) {
  const { newProxy } = await javaBridgeModule();
  return newProxy("datalevin.UdfFunction", {
    invoke: (args) => withNativeScope(bindings, async () => {
      const values = await udfArgsToJs(args);
      const result = await fn(...values);
      if (descriptor.kind === ":serializer"
          && !Buffer.isBuffer(result) && !(result instanceof Uint8Array)) {
        throw new TypeError("Custom serializer must return a byte array (Buffer or Uint8Array).");
      }
      if (descriptor.kind === ":deserializer") {
        let expected;
        for (const codec of bindings.types.values()) {
          if (descriptorKey(codec.deserialize) === descriptorKey(descriptor)) {
            expected ??= codec;
            if (codec.accepts(result)) return codec.wrapPayload(values[0]);
          }
        }
        expected?.checkValue(result);
      }
      return toJava(result === undefined ? null : result);
    })
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
    this._nativeBindings = { types: new Map(), functions: new Map(), ownerRef: new WeakRef(this) };
    this._nativeEqualityProxyPromise = null;
  }

  rawHandle() {
    return this._handle;
  }

  async register(descriptor, fn) {
    if (typeof fn !== "function") {
      throw new TypeError("fn must be a function");
    }

    const normalized = UdfDescriptor.from(descriptor, { defaultLang: "javascript" });
    const proxy = await createProxy(fn, normalized, this._nativeBindings);
    await _BINDINGS.registerUdf(this._handle, normalized, proxy);
    this._proxies.set(descriptorKey(normalized), proxy);
    this._nativeBindings.functions.set(descriptorKey(normalized), fn);
    return fn;
  }

  async unregister(descriptor) {
    const normalized = UdfDescriptor.from(descriptor, { defaultLang: "javascript" });
    await _BINDINGS.unregisterUdf(this._handle, normalized);
    const key = descriptorKey(normalized);
    this._nativeBindings.functions.delete(key);
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

  /** Bind an exact native class to payload UDFs before opening a local database.
   * Equality defaults to node:util.isDeepStrictEqual; classes with private state
   * can supply { equals: (left, right) => boolean }. This binding is not persisted.
   */
  async bindNativeType(typeName, nativeType, definition, options = {}) {
    const codec = new NativeCodec(this._nativeBindings, typeName, nativeType, definition, options);
    const existing = this._nativeBindings.types.get(nativeType.prototype);
    if (existing) {
      if (!existing.sameBinding(codec)) throw new TypeError("JavaScript class already has a different native type binding");
      return this;
    }
    if ([...this._nativeBindings.types.values()].some((c) => c.typeName === codec.typeName)) {
      throw new TypeError("Custom type already has a different JavaScript class binding");
    }
    this._nativeEqualityProxyPromise ??= createNativeEqualityProxy();
    this._nativeBindings.equalityProxyRef = new WeakRef(await this._nativeEqualityProxyPromise);
    // Recheck after the asynchronous proxy creation: concurrent conflicting
    // bindings must not overwrite a class installed by another call.
    const installed = this._nativeBindings.types.get(nativeType.prototype);
    if (installed) {
      if (!installed.sameBinding(codec)) throw new TypeError("JavaScript class already has a different native type binding");
      return this;
    }
    if ([...this._nativeBindings.types.values()].some((c) => c.typeName === codec.typeName)) {
      throw new TypeError("Custom type already has a different JavaScript class binding");
    }
    this._nativeBindings.types.set(nativeType.prototype, codec);
    codec.install();
    return this;
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

  async orderUdf(id, fn, options = {}) {
    return this.register(UdfDescriptor.orderFn(id, options), fn);
  }

  async serializerUdf(id, fn, options = {}) {
    return this.register(UdfDescriptor.serializer(id, options), fn);
  }

  async deserializerUdf(id, fn, options = {}) {
    return this.register(UdfDescriptor.deserializer(id, options), fn);
  }
}

export async function createUdfRegistry() {
  return new UdfRegistry(await _BINDINGS.createUdfRegistry());
}

export { UdfDescriptor };
