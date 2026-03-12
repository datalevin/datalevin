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
    return toJsResult(await callJavaMethod(this.rawHandle(), "schema"));
  }

  async opts() {
    return toJsResult(await callJavaMethod(this.rawHandle(), "opts"));
  }

  async updateSchema(schemaUpdate, { delAttrs = null, renameMap = null } = {}) {
    const normalizedSchema = schemaUpdate === null || schemaUpdate === undefined
      ? null
      : await toJava(schemaUpdate);

    if (renameMap !== null && renameMap !== undefined) {
      return toJsResult(
        await callJavaMethod(
          this.rawHandle(),
          "updateSchema",
          normalizedSchema,
          delAttrs === null ? null : [...delAttrs],
          await toJava(renameMap)
        )
      );
    }

    if (delAttrs !== null && delAttrs !== undefined) {
      return toJsResult(await callJavaMethod(this.rawHandle(), "updateSchema", normalizedSchema, [...delAttrs], null));
    }

    return toJsResult(await callJavaMethod(this.rawHandle(), "updateSchema", normalizedSchema));
  }

  async clear() {
    await callJavaMethod(this.rawHandle(), "clear");
  }

  async entid(eid) {
    return toJsResult(await callJavaMethod(this.rawHandle(), "entid", await toJava(eid)));
  }

  async entity(eid) {
    return toJsResult(await callJavaMethod(this.rawHandle(), "entity", await toJava(eid)));
  }

  async pull(selector, eid) {
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "pull", await pullSelector(selector), await toJava(eid))
    );
  }

  async pullMany(selector, eids) {
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "pullMany", await pullSelector(selector), await toJava(eids))
    );
  }

  async query(query, ...inputs) {
    const normalizedInputs = [];
    for (const input of inputs) {
      normalizedInputs.push(await toQueryInput(input));
    }
    if (typeof query === "string") {
      return toJsResult(
        await callJavaMethod(this.rawHandle(), "query", query, await toJava(normalizedInputs))
      );
    }
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "queryForm", await queryForm(query), await toJava(normalizedInputs))
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
          )
        );
      }
      return toJsResult(
        await callJavaMethod(
          this.rawHandle(),
          "explainForm",
          optsEdn,
          await queryForm(query),
          await toJava(normalizedInputs)
        )
      );
    }

    if (typeof query === "string") {
      return toJsResult(
        await callJavaMethod(this.rawHandle(), "explain", query, await toJava(normalizedInputs))
      );
    }
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "explainForm", await queryForm(query), await toJava(normalizedInputs))
    );
  }

  async transact(txData, txMeta = null) {
    const normalizedTxData = await _BINDINGS.txData(txData);
    if (txMeta !== null && txMeta !== undefined) {
      return toJsResult(await callJavaMethod(this.rawHandle(), "transact", normalizedTxData, await toJava(txMeta)));
    }
    return toJsResult(await callJavaMethod(this.rawHandle(), "transact", normalizedTxData));
  }
}
