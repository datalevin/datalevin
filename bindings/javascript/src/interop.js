import { toEdnForm, toJava, toJs, toQueryInput } from "./convert.js";
import { callJavaMethod, classes, jvmStarted, startJvm } from "./jvm.js";

async function unwrapInteropHandle(value) {
  if (typeof value?.rawHandle === "function") {
    return value.rawHandle();
  }
  return value;
}

async function normalizeInteropArgs(args = []) {
  const normalized = [];
  for (const arg of args) {
    normalized.push(await unwrapInteropHandle(arg));
  }
  return normalized;
}

class InteropBindings {
  async apiInfoRaw() {
    const cls = await classes();
    return callJavaMethod(cls.datalevin, "apiInfo");
  }

  async execJson(op, args = null) {
    const cls = await classes();
    if (args === null || args === undefined) {
      return callJavaMethod(cls.datalevin, "exec", op);
    }
    return callJavaMethod(cls.datalevin, "exec", op, await toJava(args));
  }

  async coreInvoke(functionName, args = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "coreInvokeBridge",
      functionName,
      await toJava(await normalizeInteropArgs([...(args || [])]))
    );
  }

  async coreInvokeRaw(functionName, args = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "coreInvoke",
      functionName,
      await toJava(await normalizeInteropArgs([...(args || [])]))
    );
  }

  async clientInvoke(functionName, args = null) {
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "clientInvokeBridge",
      functionName,
      await toJava(await normalizeInteropArgs([...(args || [])]))
    );
  }

  async createConnection(dir = null, schema = null, opts = null, { shared = false } = {}) {
    const cls = await classes();
    const target = shared ? "getConn" : "createConn";

    if (opts !== null && opts !== undefined) {
      return callJavaMethod(cls.datalevin, target, dir, await toJava(schema), await toJava(opts));
    }
    if (schema !== null && schema !== undefined) {
      return callJavaMethod(cls.datalevin, target, dir, await toJava(schema));
    }
    if (dir !== null && dir !== undefined) {
      return callJavaMethod(cls.datalevin, target, dir);
    }
    return callJavaMethod(cls.datalevin, target);
  }

  async closeConnection(handle) {
    await callJavaMethod(handle, "close");
  }

  async connectionClosed(handle) {
    return Boolean(await callJavaMethod(handle, "closed"));
  }

  async connectionDb(handle) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "connectionDb", handle);
  }

  async openKeyValue(dir, opts = null) {
    const cls = await classes();
    if (opts !== null && opts !== undefined) {
      return callJavaMethod(cls.datalevin, "openKV", dir, await toJava(opts));
    }
    return callJavaMethod(cls.datalevin, "openKV", dir);
  }

  async closeKeyValue(handle) {
    await callJavaMethod(handle, "close");
  }

  async keyValueClosed(handle) {
    return Boolean(await callJavaMethod(handle, "closed"));
  }

  async newClient(uri, opts = null) {
    const cls = await classes();
    if (opts !== null && opts !== undefined) {
      return callJavaMethod(cls.datalevin, "newClient", uri, await toJava(opts));
    }
    return callJavaMethod(cls.datalevin, "newClient", uri);
  }

  async closeClient(handle) {
    await callJavaMethod(handle, "close");
  }

  async clientDisconnected(handle) {
    return Boolean(await callJavaMethod(handle, "disconnected"));
  }

  async bridgeResult(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "bridgeResult", value);
  }

  async readEdn(edn) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "readEdn", edn);
  }

  async keyword(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "keyword", value);
  }

  async symbol(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "symbol", value);
  }

  async schema(schema) {
    if (schema === null || schema === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "schema", await toJava(schema));
  }

  async options(opts) {
    if (opts === null || opts === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "options", await toJava(opts));
  }

  async udfDescriptor(descriptor) {
    if (descriptor === null || descriptor === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "udfDescriptor", await toJava(descriptor));
  }

  async createUdfRegistry() {
    const cls = await classes();
    return callJavaMethod(cls.interop, "createUdfRegistry");
  }

  async registerUdf(registry, descriptor, fn) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "registerUdf", registry, await toJava(descriptor), fn);
  }

  async unregisterUdf(registry, descriptor) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "unregisterUdf", registry, await toJava(descriptor));
  }

  async registeredUdf(registry, descriptor) {
    const cls = await classes();
    return Boolean(
      await callJavaMethod(cls.interop, "registeredUdf", registry, await toJava(descriptor))
    );
  }

  async renameMap(renameMap) {
    if (renameMap === null || renameMap === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "renameMap", await toJava(renameMap));
  }

  async deleteAttrs(attrs) {
    if (attrs === null || attrs === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "deleteAttrs", await toJava([...attrs]));
  }

  async lookupRef(value) {
    if (value === null || value === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "lookupRef", await toJava(value));
  }

  async txData(txData) {
    if (txData === null || txData === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "txData", await toJava(txData));
  }

  async kvTxs(txs) {
    if (txs === null || txs === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "kvTxs", await toJava(txs));
  }

  async kvTxsWithTypes(txs, kType = null, vType = null) {
    if (txs === null || txs === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(
      cls.interop,
      "kvTxs",
      await toJava(txs),
      await toJava(kType),
      await toJava(vType)
    );
  }

  async kvInput(value, type) {
    if (value === null || value === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "kvInput", await toJava(value), await toJava(type));
  }

  async kvRange(range, type) {
    if (range === null || range === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "kvRange", await toEdnForm(range), await toJava(type));
  }

  async kvType(value) {
    if (value === null || value === undefined) {
      return null;
    }
    const cls = await classes();
    return callJavaMethod(cls.interop, "kvType", await toJava(value));
  }

  async databaseType(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "databaseType", value);
  }

  async role(role) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "role", role);
  }

  async permissionKeyword(value) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "permissionKeyword", value);
  }

  async permissionTarget(objectType, target) {
    const cls = await classes();
    return callJavaMethod(cls.interop, "permissionTarget", objectType, await toJava(target));
  }
}

export const _BINDINGS = new InteropBindings();

export async function apiInfo() {
  return toJs(await _BINDINGS.apiInfoRaw());
}

export async function execJson(op, args = null) {
  return toJs(await _BINDINGS.execJson(op, args));
}

export async function connect(dir = null, { schema = null, opts = null, shared = false } = {}) {
  const { Connection } = await import("./connection.js");
  return new Connection(await _BINDINGS.createConnection(dir, schema, opts, { shared }));
}

export async function openKv(dir, opts = null) {
  const { KV } = await import("./kv.js");
  return new KV(await _BINDINGS.openKeyValue(dir, opts));
}

export async function newClient(uri, opts = null) {
  const { Client } = await import("./client.js");
  return new Client(await _BINDINGS.newClient(uri, opts));
}

export { jvmStarted, startJvm, toEdnForm, toJava, toJs, toQueryInput };
