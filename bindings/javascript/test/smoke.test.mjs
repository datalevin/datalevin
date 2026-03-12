import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { after, test } from "node:test";

import {
  DatalevinError,
  DatalevinJavaError,
  apiInfo,
  connect,
  execJson,
  interop,
  openKv
} from "../src/index.js";
import { toJs } from "../src/interop.js";
import { jvmStarted, resolveClasspath } from "../src/jvm.js";

const runtimeAvailable = (() => {
  try {
    resolveClasspath();
    return true;
  } catch {
    return false;
  }
})();

function intValue(value) {
  return typeof value === "bigint" ? Number(value) : value;
}

after(() => {
  if (jvmStarted()) {
    setImmediate(() => process.exit(process.exitCode ?? 0));
  }
});

test(
  "apiInfo matches execJson",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const info = await apiInfo();
    const jsonInfo = await execJson("api-info");

    assert.equal(info["datalevin-version"], jsonInfo["datalevin-version"]);
  }
);

test(
  "connection methods cover common local flow",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-conn-"));
    const conn = await connect(dir, {
      schema: {
        ":name": {
          ":db/valueType": ":db.type/string",
          ":db/unique": ":db.unique/identity"
        }
      }
    });

    try {
      assert.equal(String(conn), "<Connection open>");
      assert.equal(await conn.closed(), false);

      const tx = await conn.transact([
        { ":db/id": -1, ":name": "Ada" },
        { ":db/id": -2, ":name": "Bob" }
      ]);
      const entity = await conn.entity(1);
      const namesFromString = await conn.query(
        "[:find [?name ...] :where [?e :name ?name]]"
      );
      const entidFromForm = await conn.query([
        ":find",
        "?e",
        ".",
        ":in",
        "$",
        "?attr",
        "?value",
        ":where",
        ["?e", "?attr", "?value"]
      ], ":name", "Ada");
      const explain = await conn.explain("[:find ?e :where [?e :name _]]");

      await conn.updateSchema({
        ":age": {
          ":db/valueType": ":db.type/long"
        }
      });
      await conn.updateSchema(null, { delAttrs: [":age"] });

      assert.equal(Array.isArray(tx[":tx-data"]), true);
      assert.equal(tx[":tx-data"].length, 2);
      assert.equal(":name" in await conn.schema(), true);
      assert.equal(typeof await conn.opts(), "object");
      assert.equal(intValue(await conn.entid([":name", "Ada"])), 1);
      assert.equal(entity[":name"], "Ada");
      assert.deepEqual(await conn.pull([":name"], 1), { ":name": "Ada" });
      assert.deepEqual(await conn.pullMany([":name"], [1, [":name", "Bob"]]), [
        { ":name": "Ada" },
        { ":name": "Bob" }
      ]);
      assert.deepEqual([...namesFromString].sort(), ["Ada", "Bob"]);
      assert.equal(intValue(entidFromForm), 1);
      assert.equal(":plan" in explain, true);
      assert.equal(":age" in await conn.schema(), false);
    } finally {
      await conn.close();
    }

    assert.equal(await conn.closed(), true);
    assert.equal(String(conn), "<Connection closed>");
  }
);

test(
  "clear closes underlying connection",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-clear-"));
    const conn = await connect(dir, {
      schema: {
        ":name": {
          ":db/valueType": ":db.type/string",
          ":db/unique": ":db.unique/identity"
        }
      }
    });

    try {
      await conn.transact([{ ":db/id": -1, ":name": "Ada" }]);
      await conn.clear();

      assert.equal(await conn.closed(), true);
      await assert.rejects(
        async () => conn.query("[:find [?name ...] :where [?e :name ?name]]"),
        DatalevinJavaError
      );
    } finally {
      if (!(await conn.closed())) {
        await conn.close();
      }
    }

    assert.equal(String(conn), "<Connection closed>");
  }
);

test(
  "kv methods cover named and list dbis",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-kv-"));
    const kv = await openKv(dir);

    try {
      assert.equal(String(kv), "<KV open>");
      assert.equal(await kv.dir(), dir);

      await kv.openDbi("items");
      await kv.openListDbi("list");
      await kv.transact(
        [[":put", "a", "alpha"], [":put", "b", "beta"], [":put", "c", "gamma"]],
        { dbiName: "items", kType: ":string", vType: ":string" }
      );
      await kv.transact(
        [[":put", "a", 1], [":put", "a", 2], [":put", "b", 3]],
        { dbiName: "list", kType: ":string", vType: ":long" }
      );

      assert.deepEqual((await kv.listDbis()).sort(), ["items", "list"]);
      assert.equal(intValue(await kv.entries("items")), 3);
      assert.equal(
        await kv.getValue("items", "b", { kType: ":string", vType: ":string", ignoreKey: true }),
        "beta"
      );
      assert.deepEqual(
        await kv.getRange("items", [":all"], { kType: ":string", vType: ":string", limit: 2, offset: 1 }),
        [["b", "beta"], ["c", "gamma"]]
      );
      assert.deepEqual(
        (await kv.getRange("list", [":all"], { kType: ":string", vType: ":long" }))
          .map(([key, value]) => [key, intValue(value)]),
        [["a", 1], ["a", 2], ["b", 3]]
      );

      await kv.clearDbi("items");
      assert.equal(intValue(await kv.entries("items")), 0);

      await kv.dropDbi("items");
      assert.deepEqual((await kv.listDbis()).sort(), ["list"]);
    } finally {
      await kv.close();
    }

    assert.equal(await kv.closed(), true);
    assert.equal(String(kv), "<KV closed>");
  }
);

test(
  "kv argument validation",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-kv-validate-"));
    const kv = await openKv(dir);

    try {
      await kv.openDbi("items");

      await assert.rejects(
        async () => kv.transact([[":put", "a", "alpha"]], { kType: ":string" }),
        TypeError
      );
      await assert.rejects(
        async () => kv.getValue("items", "a", { kType: ":string" }),
        TypeError
      );
      await assert.rejects(
        async () => kv.getRange("items", [":all"], { vType: ":string" }),
        TypeError
      );
    } finally {
      await kv.close();
    }
  }
);

test(
  "raw interop exposes normalizers and kv calls",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const raw = interop();
    assert.equal(interop(), raw);

    assert.equal(await toJs(await raw.keyword(":name")), ":name");
    assert.equal(await toJs(await raw.symbol("?e")), "?e");
    assert.equal(await toJs(await raw.databaseType("kv")), ":key-value");
    assert.equal(
      await toJs(await raw.permissionTarget(":datalevin.server/role", ":admins")),
      ":admins"
    );
    assert.deepEqual(
      await toJs(
        await raw.udfDescriptor({
          ":udf/lang": ":java",
          ":udf/kind": ":query-fn",
          ":udf/id": ":math/inc"
        })
      ),
      {
        ":udf/lang": ":java",
        ":udf/kind": ":query-fn",
        ":udf/id": ":math/inc"
      }
    );

    const kvDir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-raw-kv-"));
    const kv = await raw.openKeyValue(kvDir);
    try {
      await raw.coreInvoke("open-dbi", [kv, "items"]);
      await raw.coreInvoke("transact-kv", [
        kv,
        "items",
        await raw.kvTxs([[":put", "a", "alpha"], [":put", "b", "beta"]]),
        await raw.kvType(":string"),
        await raw.kvType(":string")
      ]);

      assert.deepEqual(await toJs(await raw.coreInvoke("list-dbis", [kv])), ["items"]);
      assert.equal(intValue(await toJs(await raw.coreInvoke("entries", [kv, "items"]))), 2);
      assert.deepEqual(
        await toJs(
          await raw.coreInvoke("get-range", [
            kv,
            "items",
            await raw.readEdn("[:all]"),
            await raw.kvType(":string"),
            await raw.kvType(":string")
          ])
        ),
        [["a", "alpha"], ["b", "beta"]]
      );
    } finally {
      await raw.closeKeyValue(kv);
    }

    assert.equal(await raw.keyValueClosed(kv), true);

    const connDir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-raw-conn-"));
    const conn = await raw.createConnection(connDir, {
      ":name": {
        ":db/valueType": ":db.type/string"
      }
    });
    try {
      await assert.rejects(async () => raw.connectionDb(conn), DatalevinError);
    } finally {
      await raw.closeConnection(conn);
    }

    assert.equal(await raw.connectionClosed(conn), true);
  }
);
