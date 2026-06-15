import assert from "node:assert/strict";
import test from "node:test";

import * as datalevin from "../src/index.js";

test("public surface stays importable without starting the JVM", () => {
  assert.equal(typeof datalevin.apiInfo, "function");
  assert.equal(typeof datalevin.connect, "function");
  assert.equal(typeof datalevin.datom, "function");
  assert.equal(typeof datalevin.datalogKv, "function");
  assert.equal(typeof datalevin.execJson, "function");
  assert.equal(typeof datalevin.fillDb, "function");
  assert.equal(typeof datalevin.initDb, "function");
  assert.equal(typeof datalevin.interop, "function");
  assert.equal(typeof datalevin.jvmStarted, "function");
  assert.equal(typeof datalevin.keyword, "function");
  assert.equal(typeof datalevin.newClient, "function");
  assert.equal(typeof datalevin.openKv, "function");
  assert.equal(typeof datalevin.readEdn, "function");
  assert.equal(typeof datalevin.schemaAttr, "function");
  assert.equal(typeof datalevin.startJvm, "function");
  assert.equal(typeof datalevin.symbol, "function");
  assert.equal(typeof datalevin.transactAsync, "function");
  assert.equal(typeof datalevin.txAdd, "function");
  assert.equal(typeof datalevin.txEntity, "function");
  assert.equal(typeof datalevin.txRetract, "function");
  assert.equal(typeof datalevin.txRetractEntity, "function");
  assert.equal(typeof datalevin.writeEdn, "function");
  assert.deepEqual(datalevin.schemaAttr({ valueType: ":db.type/string", unique: ":db.unique/identity" }), {
    ":db/valueType": ":db.type/string",
    ":db/unique": ":db.unique/identity"
  });
  assert.deepEqual(datalevin.txEntity(-1, { ":name": "Ada" }), { ":db/id": -1, ":name": "Ada" });
  assert.deepEqual(datalevin.txAdd(1, "name", "Ada"), [":db/add", 1, ":name", "Ada"]);
  assert.deepEqual(datalevin.txRetract(1, ":name", "Ada"), [":db/retract", 1, ":name", "Ada"]);
  assert.deepEqual(datalevin.txRetractEntity(1), [":db/retractEntity", 1]);
  assert.equal(typeof datalevin.Connection, "function");
  assert.equal(typeof datalevin.KV, "function");
  assert.equal(typeof datalevin.Client, "function");
  assert.equal(typeof datalevin.Connection.prototype.fillDb, "function");
  for (const method of [
    "countDatoms",
    "copy",
    "createSnapshot",
    "datalogKv",
    "datoms",
    "fulltextDatoms",
    "gcTxLogSegments",
    "indexRange",
    "listSnapshots",
    "openTxLog",
    "rseekDatoms",
    "searchDatoms",
    "seekDatoms",
    "txLogWatermarks",
    "transactAsync"
  ]) {
    assert.equal(typeof datalevin.Connection.prototype[method], "function");
  }

  for (const method of [
    "copy",
    "createSnapshot",
    "delListItems",
    "gcTxLogSegments",
    "getByRank",
    "getFirst",
    "getFirstN",
    "getList",
    "getRank",
    "inList",
    "keyRange",
    "keyRangeCount",
    "listCount",
    "listSnapshots",
    "openTxLog",
    "putListItems",
    "rangeCount",
    "sampleKv",
    "stat",
    "sync",
    "txLogWatermarks"
  ]) {
    assert.equal(typeof datalevin.KV.prototype[method], "function");
  }
});
