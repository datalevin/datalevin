import assert from "node:assert/strict";
import test from "node:test";

import * as datalevin from "../src/index.js";

test("public surface stays importable without starting the JVM", () => {
  assert.equal(typeof datalevin.apiInfo, "function");
  assert.equal(typeof datalevin.connect, "function");
  assert.equal(typeof datalevin.execJson, "function");
  assert.equal(typeof datalevin.interop, "function");
  assert.equal(typeof datalevin.jvmStarted, "function");
  assert.equal(typeof datalevin.newClient, "function");
  assert.equal(typeof datalevin.openKv, "function");
  assert.equal(typeof datalevin.startJvm, "function");
  assert.equal(typeof datalevin.Connection, "function");
  assert.equal(typeof datalevin.KV, "function");
  assert.equal(typeof datalevin.Client, "function");
});
