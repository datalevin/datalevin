import { toJava, toJs } from "./convert.js";
import { _BINDINGS } from "./interop.js";
import { javaBridgeModule } from "./jvm.js";

function keywordString(value) {
  const text = String(value);
  return text.startsWith(":") ? text : `:${text}`;
}

function descriptorKey(descriptor) {
  const normalized = udfDescriptor(descriptor);
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

async function txUdfArgsToJs(args) {
  if (Array.isArray(args)) {
    if (args.length === 0) {
      return args;
    }
    const convertedTail = await Promise.all(args.slice(1).map((value) => toJs(value)));
    return [args[0], ...convertedTail];
  }

  if (typeof args?.sizeSync === "function" && typeof args?.getSync === "function") {
    const size = Number(args.sizeSync());
    if (size === 0) {
      return [];
    }

    const values = [null];
    for (let index = 1; index < size; index += 1) {
      values.push(await toJs(args.getSync(index)));
    }
    return values;
  }

  const values = materializeJavaArgs(args);
  if (values.length === 0) {
    return values;
  }
  const convertedTail = await Promise.all(values.slice(1).map((value) => toJs(value)));
  return [values[0], ...convertedTail];
}

async function udfArgsToJs(args, descriptor) {
  if (descriptor[":udf/kind"] !== ":tx-fn") {
    const jsArgs = await toJs(args);
    return Array.isArray(jsArgs) ? jsArgs : [jsArgs];
  }

  return txUdfArgsToJs(args);
}

async function createProxy(fn, descriptor) {
  const { newProxy } = await javaBridgeModule();
  return newProxy("datalevin.UdfFunction", {
    invoke: async (args) => {
      const values = await udfArgsToJs(args, descriptor);
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
  if (idOrDescriptor !== null && typeof idOrDescriptor === "object") {
    const descriptor = idOrDescriptor;
    const id = descriptor[":udf/id"] ?? descriptor.id ?? descriptor.udfId;
    return udfDescriptor(id, {
      kind: descriptor[":udf/kind"] ?? descriptor.kind ?? kind,
      lang: descriptor[":udf/lang"] ?? descriptor.lang ?? lang,
      version: descriptor[":udf/version"] ?? descriptor.version ?? version
    });
  }

  if (idOrDescriptor === null || idOrDescriptor === undefined) {
    throw new TypeError("udf id is required");
  }

  const descriptor = {
    ":udf/lang": keywordString(lang),
    ":udf/kind": keywordString(kind),
    ":udf/id": keywordString(idOrDescriptor)
  };
  if (version !== null && version !== undefined) {
    descriptor[":udf/version"] = version;
  }
  return descriptor;
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

    const normalized = udfDescriptor(descriptor);
    const proxy = await createProxy(fn, normalized);
    await _BINDINGS.registerUdf(this._handle, normalized, proxy);
    this._proxies.set(descriptorKey(normalized), proxy);
    return fn;
  }

  async unregister(descriptor) {
    const normalized = udfDescriptor(descriptor);
    await _BINDINGS.unregisterUdf(this._handle, normalized);
    const key = descriptorKey(normalized);
    const proxy = this._proxies.get(key);
    if (proxy !== undefined) {
      proxy.reset();
      this._proxies.delete(key);
    }
  }

  async registered(descriptor) {
    return _BINDINGS.registeredUdf(this._handle, udfDescriptor(descriptor));
  }

  async queryUdf(id, fn, options = {}) {
    return this.register(udfDescriptor(id, { ...options, kind: ":query-fn" }), fn);
  }

  async predicateUdf(id, fn, options = {}) {
    return this.register(udfDescriptor(id, { ...options, kind: ":predicate" }), fn);
  }

  async txUdf(id, fn, options = {}) {
    return this.register(udfDescriptor(id, { ...options, kind: ":tx-fn" }), fn);
  }

  async analyzerUdf(id, fn, options = {}) {
    return this.register(udfDescriptor(id, { ...options, kind: ":analyzer" }), fn);
  }

  async queryAnalyzerUdf(id, fn, options = {}) {
    return this.register(udfDescriptor(id, { ...options, kind: ":query-analyzer" }), fn);
  }
}

export async function createUdfRegistry() {
  return new UdfRegistry(await _BINDINGS.createUdfRegistry());
}
