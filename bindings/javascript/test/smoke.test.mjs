import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { after, test } from "node:test";

import { connect } from "../src/index.js";
import { jvmStarted, resolveClasspath } from "../src/jvm.js";

const runtimeAvailable = (() => {
  try {
    resolveClasspath();
    return true;
  } catch {
    return false;
  }
})();

after(() => {
  if (jvmStarted()) {
    process.exit(0);
  }
});

test(
  "shared runtime jar smoke test",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-smoke-"));
    const conn = await connect(dir, {
      schema: {
        ":name": {
          ":db/valueType": ":db.type/string"
        }
      }
    });

    try {
      const tx = await conn.transact([
        { ":db/id": -1, ":name": "Ada" },
        { ":db/id": -2, ":name": "Bob" }
      ]);
      const namesFromString = await conn.query(
        "[:find [?name ...] :where [?e :name ?name]]"
      );
      const namesFromForm = await conn.query([
        ":find",
        ["?name", "..."],
        ":where",
        ["?e", ":name", "?name"]
      ]);

      assert.equal(Array.isArray(tx[":tx-data"]), true);
      assert.equal(tx[":tx-data"].length, 2);
      assert.deepEqual([...namesFromString].sort(), ["Ada", "Bob"]);
      assert.deepEqual([...namesFromForm].sort(), ["Ada", "Bob"]);
    } finally {
      await conn.close();
    }
  }
);
