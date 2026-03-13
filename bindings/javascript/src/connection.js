import { toEdnForm, toJava, toQueryInput } from "./convert.js";
import { _BINDINGS } from "./interop.js";
import { callJavaMethod } from "./jvm.js";
import { ResourceWrapper } from "./resource.js";
import { toJsResult } from "./result.js";

async function pullSelector(value) {
  if (typeof value === "string") {
    return value;
  }
  return toEdnForm(value);
}

async function queryForm(query) {
  if (typeof query !== "string") {
    return toEdnForm(query);
  }
  return query;
}

export class Connection extends ResourceWrapper {
  constructor(handle) {
    super(
      handle,
      (rawHandle) => _BINDINGS.closeConnection(rawHandle),
      (rawHandle) => _BINDINGS.connectionClosed(rawHandle),
      "connection"
    );
  }

  async schema() {
    return toJsResult(await _BINDINGS.coreInvoke("schema", [this.rawHandle()]));
  }

  async opts() {
    return toJsResult(await _BINDINGS.coreInvoke("opts", [this.rawHandle()]));
  }

  async updateSchema(schemaUpdate, { delAttrs = null, renameMap = null } = {}) {
    const args = [
      this.rawHandle(),
      schemaUpdate === null || schemaUpdate === undefined ? null : await _BINDINGS.schema(schemaUpdate)
    ];

    if (renameMap !== null && renameMap !== undefined) {
      args.push(await _BINDINGS.deleteAttrs(delAttrs));
      args.push(await _BINDINGS.renameMap(renameMap));
    } else if (delAttrs !== null && delAttrs !== undefined) {
      args.push(await _BINDINGS.deleteAttrs(delAttrs));
    }

    return toJsResult(await _BINDINGS.coreInvoke("update-schema", args));
  }

  async clear() {
    await _BINDINGS.coreInvoke("clear", [this.rawHandle()]);
  }

  async entid(eid) {
    return toJsResult(await callJavaMethod(this.rawHandle(), "entid", await toJava(eid)));
  }

  async entity(eid) {
    return toJsResult(await callJavaMethod(this.rawHandle(), "entityMap", await toJava(eid)));
  }

  async pull(selector, eid) {
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "pull", await pullSelector(selector), await toJava(eid)),
      { bridge: true }
    );
  }

  async pullMany(selector, eids) {
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "pullMany", await pullSelector(selector), await toJava(eids)),
      { bridge: true }
    );
  }

  async query(query, ...inputs) {
    const normalizedInputs = [];
    for (const input of inputs) {
      normalizedInputs.push(await toQueryInput(input));
    }
    if (typeof query === "string") {
      return toJsResult(
        await callJavaMethod(this.rawHandle(), "query", query, await toJava(normalizedInputs)),
        { bridge: true }
      );
    }
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "queryForm", await queryForm(query), await toJava(normalizedInputs)),
      { bridge: true }
    );
  }

  async explain(query, { inputs = [], optsEdn = null } = {}) {
    const normalizedInputs = [];
    for (const input of inputs) {
      normalizedInputs.push(await toQueryInput(input));
    }
    if (optsEdn !== null && optsEdn !== undefined) {
      if (typeof query === "string") {
        return toJsResult(
          await callJavaMethod(
            this.rawHandle(),
            "explain",
            optsEdn,
            query,
            await toJava(normalizedInputs)
          ),
          { bridge: true }
        );
      }
      return toJsResult(
        await callJavaMethod(
          this.rawHandle(),
          "explainForm",
          optsEdn,
          await queryForm(query),
          await toJava(normalizedInputs)
        ),
        { bridge: true }
      );
    }

    if (typeof query === "string") {
      return toJsResult(
        await callJavaMethod(this.rawHandle(), "explain", query, await toJava(normalizedInputs)),
        { bridge: true }
      );
    }
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "explainForm", await queryForm(query), await toJava(normalizedInputs)),
      { bridge: true }
    );
  }

  async transact(txData, txMeta = null) {
    const args = [this.rawHandle(), await _BINDINGS.txData(txData)];
    if (txMeta !== null && txMeta !== undefined) {
      args.push(await toJava(txMeta));
    }
    return toJsResult(await _BINDINGS.coreInvoke("transact!", args));
  }
}
