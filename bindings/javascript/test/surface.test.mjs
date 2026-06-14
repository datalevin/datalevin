import assert from "node:assert/strict";
import test from "node:test";

import * as datalevin from "../src/index.js";

test("public surface stays importable without starting the JVM", () => {
  assert.equal(typeof datalevin.apiInfo, "function");
  assert.equal(typeof datalevin.connect, "function");
  assert.equal(typeof datalevin.datom, "function");
  assert.equal(typeof datalevin.execJson, "function");
  assert.equal(typeof datalevin.fillDb, "function");
  assert.equal(typeof datalevin.initDb, "function");
  assert.equal(typeof datalevin.interop, "function");
  assert.equal(typeof datalevin.jvmStarted, "function");
  assert.equal(typeof datalevin.newClient, "function");
  assert.equal(typeof datalevin.openKv, "function");
  assert.equal(typeof datalevin.startJvm, "function");
  assert.equal(typeof datalevin.Connection, "function");
  assert.equal(typeof datalevin.KV, "function");
  assert.equal(typeof datalevin.Client, "function");
  assert.equal(typeof datalevin.Connection.prototype.fillDb, "function");
  for (const method of [
    "copy",
    "createSnapshot",
    "gcTxLogSegments",
    "listSnapshots",
    "openTxLog",
    "txLogWatermarks"
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
