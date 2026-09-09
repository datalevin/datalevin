import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { javaBridgeModule } from "./jvm.js";
import { UdfDescriptor, descriptorData } from "./udf-value.js";

const scope = new AsyncLocalStorage();
const codecs = new Map();
const cleanup = new FinalizationRegistry((id) => codecs.delete(id));
const builtins = new Set([Object, Array, Map, Set, Date, RegExp, Buffer, Uint8Array,
  String, Number, Boolean, Function, Promise, WeakMap, WeakSet]);

export function descriptorKey(descriptor) {
  const d = descriptorData(descriptor);
  return [d[":udf/lang"], d[":udf/kind"], d[":udf/id"], d[":udf/version"] ?? ""].join("\u0000");
}

export function currentNativeRegistry() {
  const registry = scope.getStore() ?? null;
  return registry?.ownerRef?.deref() ?? registry;
}

export function withNativeScope(registry, fn) {
  return scope.run(registry, fn);
}

export function bindNativeCallback(fn) {
  const registry = currentNativeRegistry();
  return (...args) => withNativeScope(registry, () => fn(...args));
}

export function nativeMethods(cls) {
  for (const [name, property] of Object.entries(Object.getOwnPropertyDescriptors(cls.prototype))) {
    if (name === "constructor" || name.startsWith("_") || typeof property.value !== "function") continue;
    const fn = property.value;
    Object.defineProperty(cls.prototype, name, {
      ...property,
      value: function (...args) {
        const registry = this._nativeRegistry ?? null;
        return registry === currentNativeRegistry()
          ? fn.apply(this, args)
          : withNativeScope(registry, () => fn.apply(this, args));
      }
    });
  }
}

function field(map, key) {
  return map instanceof Map ? (map.get(key) ?? map.get(`:${key}`)) : (map?.[key] ?? map?.[`:${key}`]);
}

export function registryFromOpts(opts, dir = null) {
  const registry = field(field(opts, "runtime-opts"), "udf-registry");
  if (!registry?._nativeBindings) return null;
  if (registry._nativeBindings.types.size && String(dir).startsWith("dtlv://")) {
    throw new TypeError("Native JavaScript value transport currently requires a local database");
  }
  return registry;
}

export function currentNativeBindings() {
  const registry = currentNativeRegistry();
  return registry?._nativeBindings ?? registry;
}

export function nativeCodecFor(value) {
  if (value === null || typeof value !== "object") return null;
  return currentNativeBindings()?.types.get(Object.getPrototypeOf(value)) ?? null;
}

function valueCodec(value) {
  const codec = codecs.get(value.codecIdSync())?.deref();
  if (!codec) throw new Error(`Missing native type binding for ${value.typeNameSync()}`);
  return codec;
}

export async function nativeToJs(value) {
  return valueCodec(value).decode(value.payloadSync());
}

// This callback captures no registry or proxy owner. The registry retains the
// proxy; codecs and UDF callbacks retain only host state and a weak proxy ref.
async function nativeEquals(left, right) {
  const codec = valueCodec(left);
  const equal = await codec.equals(await nativeToJs(left), await nativeToJs(right));
  if (typeof equal !== "boolean") throw new TypeError("Native equality must return a boolean");
  return equal;
}

export async function createNativeEqualityProxy() {
  const { newProxy } = await javaBridgeModule();
  return newProxy("java.util.function.BiPredicate", { test: nativeEquals });
}

let nativeClassPromise;
async function nativeClass() {
  nativeClassPromise ??= javaBridgeModule().then((bridge) => bridge.importClass("datalevin.NativeValue"));
  return nativeClassPromise;
}

export class NativeCodec {
  constructor(bindings, typeName, nativeType, definition, { equals = isDeepStrictEqual } = {}) {
    if (typeof nativeType !== "function" || !nativeType.prototype || builtins.has(nativeType)) {
      throw new TypeError("nativeType must be a custom JavaScript class");
    }
    if (typeof equals !== "function") throw new TypeError("equals must be a function");
    const name = String(typeName).replace(/^:/, "");
    if (!/^[^/\s]+\/[^\s]+$/.test(name)) throw new TypeError("Native type name must be a namespaced keyword");
    const payload = field(definition, "payload");
    if (!payload || typeof payload !== "object") {
      throw new TypeError("Native types require serialize and deserialize UDF descriptors");
    }
    this.serialize = UdfDescriptor.from(field(payload, "serialize"), { defaultLang: "javascript" });
    this.deserialize = UdfDescriptor.from(field(payload, "deserialize"), { defaultLang: "javascript" });
    if (this.serialize.kind !== ":serializer" || this.deserialize.kind !== ":deserializer") {
      throw new TypeError("Native types require serializer and deserializer UDF kinds");
    }
    this.bindings = bindings;
    this.nativeType = nativeType;
    this.typeName = `:${name}`;
    this.equals = equals;
    this.id = randomUUID();
  }

  sameBinding(other) {
    return this.typeName === other.typeName && this.nativeType === other.nativeType
      && this.equals === other.equals
      && descriptorKey(this.serialize) === descriptorKey(other.serialize)
      && descriptorKey(this.deserialize) === descriptorKey(other.deserialize);
  }

  install() {
    codecs.set(this.id, new WeakRef(this));
    cleanup.register(this, this.id);
  }

  function(descriptor) {
    const fn = this.bindings.functions.get(descriptorKey(descriptor));
    if (!fn) throw new Error(`Missing native type UDF binding: ${descriptor.udfId}`);
    return fn;
  }

  accepts(value) {
    return value !== null && typeof value === "object" && Object.getPrototypeOf(value) === this.nativeType.prototype;
  }

  checkValue(value) {
    if (!this.accepts(value)) {
      throw new TypeError(`Deserializer for ${this.typeName} must return ${this.nativeType.name}`);
    }
  }

  async decode(payload) {
    const value = await this.function(this.deserialize)(Buffer.from(payload));
    this.checkValue(value);
    return value;
  }

  async wrap(value) {
    const payload = await this.function(this.serialize)(value);
    if (!Buffer.isBuffer(payload) && !(payload instanceof Uint8Array)) {
      throw new TypeError("Custom serializer must return a byte array (Buffer or Uint8Array)");
    }
    return this.wrapPayload(Buffer.from(payload));
  }

  async wrapPayload(payload) {
    const equality = this.bindings.equalityProxyRef?.deref();
    if (!equality) throw new Error(`Missing native equality binding for ${this.typeName}`);
    const NativeValue = await nativeClass();
    const bytes = Buffer.from(payload);
    return new NativeValue(this.id, this.typeName,
      Array.from(new Int8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength)), equality);
  }
}
