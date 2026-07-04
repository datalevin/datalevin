import { toEdnForm, toJava } from "./convert.js";
import { _BINDINGS } from "./interop.js";
import { callJavaMethod } from "./jvm.js";
import { toJsResult } from "./result.js";

async function pullSelector(value) {
  if (typeof value === "string") {
    return value;
  }
  return toEdnForm(value);
}

function hasValue(value) {
  return value !== null && value !== undefined;
}

async function mapGet(map, key) {
  return callJavaMethod(map, "get", await _BINDINGS.keyword(key));
}

export class Database {
  constructor(handle) {
    this._handle = handle;
  }

  rawHandle() {
    return this._handle;
  }

  toString() {
    return "<Database>";
  }

  async entid(eid) {
    return toJsResult(await _BINDINGS.databaseEntid(this.rawHandle(), eid), { bridge: true });
  }

  async entity(eid) {
    const { Entity } = await import("./entity.js");
    const entity = await _BINDINGS.databaseEntity(this.rawHandle(), eid);
    return hasValue(entity) ? new Entity(entity) : null;
  }

  async entityMap(eid) {
    return toJsResult(await _BINDINGS.databaseEntityMap(this.rawHandle(), eid), { bridge: true });
  }

  async pull(selector, eid) {
    return toJsResult(
      await _BINDINGS.databasePull(this.rawHandle(), await pullSelector(selector), await toJava(eid)),
      { bridge: true }
    );
  }

  async pullMany(selector, eids) {
    return toJsResult(
      await _BINDINGS.databasePullMany(this.rawHandle(), await pullSelector(selector), await toJava(eids)),
      { bridge: true }
    );
  }

  async cardinality(attr) {
    return toJsResult(await _BINDINGS.databaseCardinality(this.rawHandle(), await toJava(attr)), { bridge: true });
  }

  async analyze(attr = null) {
    return toJsResult(await _BINDINGS.databaseAnalyze(this.rawHandle(), attr), { bridge: true });
  }
}

export async function txReportToJs(report) {
  const result = {};
  const dbBefore = await mapGet(report, ":db-before");
  const dbAfter = await mapGet(report, ":db-after");
  if (hasValue(dbBefore)) {
    result[":db-before"] = new Database(dbBefore);
  }
  if (hasValue(dbAfter)) {
    result[":db-after"] = new Database(dbAfter);
  }
  for (const key of [":tx-data", ":tempids", ":tx-id", ":tx-meta"]) {
    result[key] = await toJsResult(await mapGet(report, key), { bridge: true });
  }
  return result;
}
