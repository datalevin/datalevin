import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { after, test } from "node:test";

import {
  DatalevinError,
  DatalevinJavaError,
  Entity,
  KVTransaction,
  RawBuffer,
  RawKV,
  apiInfo,
  connect,
  createUdfRegistry,
  datom,
  datalogKv,
  execJson,
  fillDb,
  initDb,
  interop,
  keyword,
  newSearchEngine,
  newVectorIndex,
  openKv,
  reIndex,
  schemaAttr,
  searchDomain,
  searchIndexWriter,
  transactAsync,
  txEntity,
  udfDescriptor,
  withTransaction
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
  "UDF registry supports inline query functions",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-udf-"));
    const registry = await createUdfRegistry();
    const descriptor = udfDescriptor(":math/inc");

    await registry.queryUdf(":math/inc", (value) => Number(value) + 1);

    const conn = await connect(dir, {
      opts: { ":runtime-opts": { ":udf-registry": registry } }
    });

    try {
      assert.equal(await registry.registered(descriptor), true);
      assert.equal(
        intValue(await conn.query(
          "[:find ?v . :in $ ?desc ?n :where [(udf ?desc ?n) ?v]]",
          descriptor,
          41
        )),
        42
      );

      await registry.unregister(descriptor);
      assert.equal(await registry.registered(descriptor), false);
    } finally {
      await conn.close();
    }
  }
);

test(
  "UDF registry supports fulltext analyzers",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-fulltext-udf-"));
    const registry = await createUdfRegistry();
    const analyzerDescriptor = udfDescriptor(":text/hashtags", { kind: ":analyzer" });
    const queryDescriptor = udfDescriptor(":text/plain-query", { kind: ":query-analyzer" });

    await registry.analyzerUdf(":text/hashtags", (text) => {
      const tokens = [];
      const pattern = /#\w+/g;
      let match;
      const source = String(text);
      while ((match = pattern.exec(source)) !== null) {
        tokens.push([match[0].slice(1), tokens.length, match.index]);
      }
      return tokens;
    });
    await registry.queryAnalyzerUdf(":text/plain-query", (text) => (
      String(text).trim().split(/\s+/).filter(Boolean).map((token, position) => [token, position, position])
    ));

    const conn = await connect(dir, {
      schema: {
        ":text": schemaAttr({
          valueType: ":db.type/string",
          fulltext: true,
          extra: { ":db.fulltext/autoDomain": true }
        })
      },
      opts: {
        ":runtime-opts": { ":udf-registry": registry },
        ":search-domains": {
          text: searchDomain({
            indexPosition: true,
            extra: {
              ":analyzer": analyzerDescriptor,
              ":query-analyzer": queryDescriptor
            }
          })
        }
      }
    });

    try {
      await conn.transact([
        { ":db/id": 1, ":text": "alpha #needle" },
        { ":db/id": 2, ":text": "needle without hash" }
      ]);

      const matches = await conn.query(
        "[:find [?e ...] :in $ ?q :where [(fulltext $ :text ?q) [[?e ?a ?v]]]]",
        "needle"
      );
      assert.deepEqual(matches.map(intValue), [1]);
      assert.equal(await registry.registered(analyzerDescriptor), true);
      assert.equal(await registry.registered(queryDescriptor), true);
    } finally {
      await conn.close();
    }
  }
);

test(
  "connection methods cover common local flow",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-conn-"));
    const otherDir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-conn-other-"));
    const conn = await connect(dir, {
      schema: {
        ":name": schemaAttr({
          valueType: ":db.type/string",
          unique: ":db.unique/identity"
        }),
        ":bio": schemaAttr({
          valueType: ":db.type/string",
          fulltext: true,
          extra: { ":db.fulltext/autoDomain": true }
        }),
        ":status": schemaAttr({ valueType: ":db.type/keyword" }),
        ":friend": schemaAttr({ valueType: ":db.type/ref" })
      }
    });
    let otherConn = null;

    try {
      otherConn = await connect(otherDir, {
        schema: {
          ":name": schemaAttr({ valueType: ":db.type/string" })
        }
      });
      assert.equal(String(conn), "<Connection open>");
      assert.equal(await conn.closed(), false);

      const reports = [];
      const listenerKey = await conn.listen((report) => reports.push(report), "test-listener");
      assert.equal(listenerKey, "test-listener");
      const tx = await conn.transact([
        txEntity(-1, {
          ":name": "Ada",
          ":bio": "Ada builds database systems",
          ":status": await keyword(":active"),
          ":friend": -2
        }),
        txEntity(-2, {
          ":name": "Bob",
          ":bio": "Bob writes migration tools",
          ":status": await keyword(":draft")
        })
      ]);
      await otherConn.transact([
        { ":db/id": -1, ":name": "Cara" }
      ]);
      await conn.unlisten(listenerKey);
      const asyncTx = await conn.transactAsync([
        { ":db/id": -3, ":bio": "Async transactions help ingestion" }
      ]);
      const topAsyncTx = await transactAsync(conn, [
        { ":db/id": -4, ":bio": "Top-level async helper" }
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
      const namesFromOtherSource = await conn.query(
        "[:find [?name ...] :in $ $other :where [$ ?e :name \"Ada\"] [$other ?x :name ?name]]",
        otherConn
      );
      const explain = await conn.explain("[:find ?e :where [?e :name _]]");

      await conn.updateSchema({
        ":age": {
          ":db/valueType": ":db.type/long"
        }
      });
      await conn.updateSchema(null, { delAttrs: [":age"] });

      assert.equal(Array.isArray(tx[":tx-data"]), true);
      assert.equal(tx[":tx-data"].length, 7);
      assert.equal(reports.length, 1);
      assert.equal(Array.isArray(reports[0][":tx-data"]), true);
      assert.equal(reports[0][":tx-meta"], null);
      assert.equal(Array.isArray(asyncTx[":tx-data"]), true);
      assert.equal(Array.isArray(topAsyncTx[":tx-data"]), true);
      assert.equal(":name" in await conn.schema(), true);
      assert.equal(typeof await conn.opts(), "object");
      assert.equal(intValue(await conn.entid([":name", "Ada"])), 1);
      assert.equal(intValue(await entity.id()), 1);
      assert.equal(intValue(await entity.get(":db/id")), 1);
      assert.equal(await entity.get(":name"), "Ada");
      assert.equal(await entity.has(":name"), true);
      assert.equal(await entity.get(":missing", "fallback"), "fallback");
      const friend = await entity.get(":friend");
      assert.equal(friend instanceof Entity, true);
      assert.equal(await friend.get(":name"), "Bob");
      assert.equal((await entity.touch())[":name"], "Ada");
      assert.equal((await conn.entityMap([":name", "Ada"]))[":name"], "Ada");
      assert.equal((await conn.pull([":status"], 1))[":status"], ":active");
      assert.deepEqual(await conn.pull([":name"], 1), { ":name": "Ada" });
      assert.deepEqual(await conn.pullMany([":name"], [1, [":name", "Bob"]]), [
        { ":name": "Ada" },
        { ":name": "Bob" }
      ]);
      assert.equal((await conn.datoms(":eav", { c1: 1, c2: ":name", limit: 1 }))[0][":v"], "Ada");
      assert.equal((await conn.seekDatoms(":eav", { c1: 1, c2: ":name", limit: 1 }))[0][":v"], "Ada");
      assert.equal((await conn.rseekDatoms(":ave", { c1: ":name", c2: "Bob", limit: 1 }))[0][":v"], "Bob");
      assert.equal((await conn.searchDatoms({ attr: ":name", value: "Ada" }))[0][":v"], "Ada");
      assert.equal(intValue(await conn.countDatoms({ attr: ":name", value: "Ada" })), 1);
      assert.deepEqual(
        (await conn.indexRange(":name", "A", "C")).map((row) => row[":v"]),
        ["Ada", "Bob"]
      );
      assert.equal((await conn.fulltextDatoms("database", { opts: { ":top": 5 } }))[0][2], "Ada builds database systems");
      assert.equal(
        (await conn.fulltextDatoms("async", { opts: { ":top": 5 } }))
          .some((row) => row[2] === "Async transactions help ingestion"),
        true
      );
      assert.equal(await conn.reIndex(), conn);
      const namesAfterReIndex = await conn.query("[:find [?name ...] :where [?e :name ?name]]");
      assert.deepEqual(
        [...namesAfterReIndex].sort(),
        ["Ada", "Bob"]
      );
      const backingKv = await conn.datalogKv();
      await backingKv.openDbi("app-state");
      await backingKv.transact([[":put", "k", "v"]], {
        dbiName: "app-state",
        kType: ":string",
        vType: ":string"
      });
      assert.equal(await backingKv.getValue("app-state", "k", {
        kType: ":string",
        vType: ":string",
        ignoreKey: true
      }), "v");
      assert.equal(await (await datalogKv(conn)).dir(), dir);
      assert.deepEqual([...namesFromString].sort(), ["Ada", "Bob"]);
      assert.equal(intValue(entidFromForm), 1);
      assert.deepEqual(namesFromOtherSource, ["Cara"]);
      assert.equal(":plan" in explain, true);
      assert.equal(":age" in await conn.schema(), false);
    } finally {
      if (otherConn !== null) {
        await otherConn.close();
      }
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
  "bulk load initDb and fillDb cover datom inputs",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-bulk-"));
    const schema = {
      ":name": {
        ":db/valueType": ":db.type/string",
        ":db/unique": ":db.unique/identity"
      }
    };
    const conn = await initDb([[1, ":name", "Ada"]], { dir, schema });

    try {
      assert.deepEqual(await conn.query("[:find [?name ...] :where [?e :name ?name]]"), ["Ada"]);

      assert.equal(await fillDb(conn, [[2, ":name", "Bob"]]), conn);
      assert.equal(await conn.fillDb([datom(3, ":name", "Cara")]), conn);

      const names = await conn.query("[:find [?name ...] :where [?e :name ?name]]");
      assert.deepEqual([...names].sort(), ["Ada", "Bob", "Cara"]);
    } finally {
      await conn.close();
    }
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
      await kv.openDbi("blobs");
      await kv.transact(
        [[":put", "buf", Buffer.from([0, 1, 2, 255])], [":put", "arr", new Uint8Array([9, 8, 7])]],
        { dbiName: "blobs", kType: ":string", vType: ":bytes" }
      );
      await kv.openDbi("blob-keys");
      await kv.transact(
        [
          [":put", Buffer.from([0, 1]), Buffer.from([7, 8])],
          [":put", Buffer.from([0, 2]), Buffer.from([9, 10])]
        ],
        { dbiName: "blob-keys", kType: ":bytes", vType: ":bytes" }
      );
      await kv.putListItems("list", "a", [1, 2], { kType: ":string", vType: ":long" });
      await kv.putListItems("list", "b", [3], { kType: ":string", vType: ":long" });

      assert.deepEqual((await kv.listDbis()).sort(), ["blob-keys", "blobs", "items", "list"]);
      assert.equal(intValue(await kv.entries("items")), 3);
      const itemStat = await kv.stat("items");
      assert.equal(intValue(itemStat[":entries"] ?? itemStat.entries), 3);
      assert.equal(
        await kv.getValue("items", "b", { kType: ":string", vType: ":string", ignoreKey: true }),
        "beta"
      );
      assert.equal(intValue(await kv.getRank("items", "b", { kType: ":string" })), 1);
      assert.equal(
        await kv.getByRank("items", 1, { kType: ":string", vType: ":string" }),
        "beta"
      );
      assert.deepEqual(
        await kv.getByRank("items", 1, { kType: ":string", vType: ":string", ignoreKey: false }),
        ["b", "beta"]
      );
      assert.deepEqual(
        await kv.getEntryByRank("items", 1, { kType: ":string", vType: ":string" }),
        ["b", "beta"]
      );
      assert.deepEqual(
        await kv.getFirst("items", [":all"], { kType: ":string", vType: ":string" }),
        ["a", "alpha"]
      );
      assert.deepEqual(
        await kv.getFirstN("items", 2, [":all"], { kType: ":string", vType: ":string" }),
        [["a", "alpha"], ["b", "beta"]]
      );
      const samples = await kv.sampleKv("items", 2, {
        kType: ":string",
        vType: ":string",
        ignoreKey: false
      });
      const validSamples = new Set([
        JSON.stringify(["a", "alpha"]),
        JSON.stringify(["b", "beta"]),
        JSON.stringify(["c", "gamma"])
      ]);
      assert.equal(samples.length, 2);
      assert.equal(samples.every((sample) => validSamples.has(JSON.stringify(sample))), true);
      const blobFromBuffer = await kv.getValue("blobs", "buf", {
        kType: ":string",
        vType: ":bytes",
        ignoreKey: true
      });
      const blobFromUint8 = await kv.getValue("blobs", "arr", {
        kType: ":string",
        vType: ":bytes",
        ignoreKey: true
      });
      const blobKeyValue = await kv.getValue("blob-keys", Buffer.from([0, 2]), {
        kType: ":bytes",
        vType: ":bytes",
        ignoreKey: true
      });
      const blobKeyRange = await kv.getRange("blob-keys", [":closed", Buffer.from([0, 1]), Buffer.from([0, 2])], {
        kType: ":bytes",
        vType: ":bytes"
      });
      assert.deepEqual(
        await kv.getRange("items", [":all"], { kType: ":string", vType: ":string", limit: 2, offset: 1 }),
        [["b", "beta"], ["c", "gamma"]]
      );
      assert.deepEqual(
        await kv.keyRange("items", [":all"], { kType: ":string", limit: 2, offset: 1 }),
        ["b", "c"]
      );
      assert.equal(intValue(await kv.keyRangeCount("items", [":all"], { kType: ":string" })), 3);
      assert.equal(intValue(await kv.rangeCount("items", [":all"], { kType: ":string" })), 3);
      assert.equal(await kv.withTransaction(async (txKv) => {
        await txKv.transact([[":put", "d", "delta"]], {
          dbiName: "items",
          kType: ":string",
          vType: ":string"
        });
        return txKv.getValue("items", "d", {
          kType: ":string",
          vType: ":string",
          ignoreKey: true
        });
      }), "delta");
      assert.equal(await withTransaction(kv, async (txKv) => {
        await txKv.transact([[":put", "g", "gamma-2"]], {
          dbiName: "items",
          kType: ":string",
          vType: ":string"
        });
        return txKv.getValue("items", "g", {
          kType: ":string",
          vType: ":string",
          ignoreKey: true
        });
      }), "gamma-2");
      const explicitTx = await kv.beginTransaction();
      assert.equal(explicitTx instanceof KVTransaction, true);
      assert.equal(explicitTx.active(), true);
      await explicitTx.transact([[":put", "e", "epsilon"]], {
        dbiName: "items",
        kType: ":string",
        vType: ":string"
      });
      assert.equal(await explicitTx.commit(), ":committed");
      assert.equal(explicitTx.active(), false);
      const abortedTx = await kv.transaction();
      await abortedTx.transact([[":put", "f", "phi"]], {
        dbiName: "items",
        kType: ":string",
        vType: ":string"
      });
      assert.equal(await abortedTx.abort(), ":aborted");
      assert.equal(await kv.getValue("items", "e", {
        kType: ":string",
        vType: ":string",
        ignoreKey: true
      }), "epsilon");
      assert.equal(await kv.getValue("items", "f", {
        kType: ":string",
        vType: ":string",
        ignoreKey: true
      }), null);
      assert.equal(Buffer.isBuffer(blobFromBuffer), true);
      assert.equal(Buffer.isBuffer(blobFromUint8), true);
      assert.equal(Buffer.isBuffer(blobKeyValue), true);
      assert.deepEqual([...blobFromBuffer], [0, 1, 2, 255]);
      assert.deepEqual([...blobFromUint8], [9, 8, 7]);
      assert.deepEqual([...blobKeyValue], [9, 10]);
      assert.deepEqual(
        blobKeyRange.map(([key, value]) => [[...key], [...value]]),
        [[[0, 1], [7, 8]], [[0, 2], [9, 10]]]
      );
      assert.deepEqual(
        (await kv.getRange("list", [":all"], { kType: ":string", vType: ":long" }))
          .map(([key, value]) => [key, intValue(value)]),
        [["a", 1], ["a", 2], ["b", 3]]
      );
      assert.deepEqual(
        (await kv.listRange("list", [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long",
          limit: 1,
          offset: 1
        })).map(([key, value]) => [key, intValue(value)]),
        [["a", 2]]
      );
      assert.deepEqual(
        (await kv.listRange("list", [":closed", "a", "b"], {
          kType: ":string",
          vRange: [":closed", 2, 3],
          vType: ":long"
        })).map(([key, value]) => [key, intValue(value)]),
        [["a", 2], ["b", 3]]
      );
      assert.equal(intValue(await kv.listRangeCount("list", [":all"], { kType: ":string" })), 3);
      assert.deepEqual(
        (await kv.listRangeFirst("list", [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long"
        })).map((value) => typeof value === "bigint" ? intValue(value) : value),
        ["a", 1]
      );
      assert.deepEqual(
        (await kv.listRangeFirstN("list", 2, [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long"
        })).map(([key, value]) => [key, intValue(value)]),
        [["a", 1], ["a", 2]]
      );
      assert.equal(intValue(await kv.keyRangeListCount("list", [":all"], { kType: ":string" })), 3);
      assert.deepEqual((await kv.getList("list", "a", { kType: ":string", vType: ":long" })).map(intValue), [1, 2]);
      assert.deepEqual(
        (await kv.getList("list", "a", { kType: ":string", vType: ":long", limit: 1, offset: 1 })).map(intValue),
        [2]
      );
      assert.equal(intValue(await kv.listCount("list", "a", { kType: ":string" })), 2);
      assert.equal(await kv.inList("list", "a", 2, { kType: ":string", vType: ":long" }), true);
      assert.equal(await kv.inList("list", "a", 9, { kType: ":string", vType: ":long" }), false);
      await kv.delListItems("list", "a", { kType: ":string", values: [2], vType: ":long" });
      assert.deepEqual((await kv.getList("list", "a", { kType: ":string", vType: ":long" })).map(intValue), [1]);
      await kv.delListItems("list", "a", { kType: ":string" });
      assert.equal(intValue(await kv.listCount("list", "a", { kType: ":string" })), 0);

      await kv.sync();
      await kv.setEnvFlags(new Set(["nosync"]), true);
      assert.equal((await kv.getEnvFlags()).has(":nosync"), true);
      await kv.setEnvFlags([":nosync"], false);
      assert.equal((await kv.getEnvFlags()).has(":nosync"), false);

      const copyDir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-kv-copy-"));
      fs.rmSync(copyDir, { recursive: true, force: true });
      await kv.copy(copyDir);
      assert.equal(fs.existsSync(copyDir), true);

      await kv.clearDbi("items");
      assert.equal(intValue(await kv.entries("items")), 0);

      await kv.dropDbi("items");
      assert.deepEqual((await kv.listDbis()).sort(), ["blob-keys", "blobs", "list"]);

      await kv.openDbi("post-reindex");
      await kv.transact([[":put", "x", "xi"]], {
        dbiName: "post-reindex",
        kType: ":string",
        vType: ":string"
      });
      assert.equal(await kv.reIndex(), kv);
      assert.equal(intValue(await kv.entries("post-reindex")), 1);
    } finally {
      await kv.close();
    }

    assert.equal(await kv.closed(), true);
    assert.equal(String(kv), "<KV closed>");
  }
);

test(
  "kv list functional operations",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-kv-list-fns-"));
    const kv = await openKv(dir);

    try {
      await kv.openListDbi("list");
      await kv.putListItems("list", "a", [1, 2], { kType: ":string", vType: ":long" });
      await kv.putListItems("list", "b", [3], { kType: ":string", vType: ":long" });

      const values = [];
      await kv.visitList("list", (value) => {
        values.push(intValue(value));
      }, "a", { kType: ":string", vType: ":long" });
      assert.deepEqual(values, [1, 2]);

      const pairs = [];
      await kv.visitListRange("list", (key, value) => {
        pairs.push([key, intValue(value)]);
      }, [":all"], { kType: ":string", vRange: [":all"], vType: ":long" });
      assert.deepEqual(pairs, [["a", 1], ["a", 2], ["b", 3]]);

      assert.deepEqual(
        (await kv.listRangeFilter("list", (_key, value) => intValue(value) >= 2, [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long"
        })).map(([key, value]) => [key, intValue(value)]),
        [["a", 2], ["b", 3]]
      );
      assert.deepEqual(
        (await kv.listRangeFilter("list", () => true, [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long",
          limit: 1,
          offset: 1
        })).map(([key, value]) => [key, intValue(value)]),
        [["a", 2]]
      );
      assert.equal(
        intValue(await kv.listRangeFilterCount("list", (key) => key === "a", [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long"
        })),
        2
      );
      assert.deepEqual(
        await kv.listRangeKeep("list", (key, value) => intValue(value) > 1 ? `${key}:${intValue(value)}` : null, [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long"
        }),
        ["a:2", "b:3"]
      );
      assert.deepEqual(
        (await kv.listRangeSome("list", (key, value) => intValue(value) === 3 ? [key, intValue(value)] : null, [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long"
        })).map((value) => typeof value === "bigint" ? intValue(value) : value),
        ["b", 3]
      );

      const rawValues = [];
      await kv.visitListRaw("list", async (raw) => {
        rawValues.push([raw instanceof RawBuffer, intValue(await raw.read(":long")), (await raw.bytes()).length > 0]);
      }, "a", { kType: ":string" });
      assert.deepEqual(rawValues, [[true, 1, true], [true, 2, true]]);

      const rawPairs = [];
      await kv.visitListRangeRaw("list", async (raw) => {
        rawPairs.push([
          raw instanceof RawKV,
          await raw.readKey(":string"),
          intValue(await raw.readValue(":long")),
          (await raw.keyBytes()).length > 0
        ]);
      }, [":all"], { kType: ":string", vRange: [":all"], vType: ":long" });
      assert.deepEqual(rawPairs, [[true, "a", 1, true], [true, "a", 2, true], [true, "b", 3, true]]);

      assert.deepEqual(
        (await kv.listRangeFilterRaw("list", async (raw) => intValue(await raw.readValue(":long")) >= 2, [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long"
        })).map(([key, value]) => [key, intValue(value)]),
        [["a", 2], ["b", 3]]
      );
      assert.equal(
        intValue(await kv.listRangeFilterCountRaw("list", async (raw) => await raw.readKey(":string") === "a", [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long"
        })),
        2
      );
      assert.deepEqual(
        await kv.listRangeKeepRaw("list", async (raw) => {
          const value = intValue(await raw.readValue(":long"));
          return value > 1 ? `${await raw.readKey(":string")}:${value}` : null;
        }, [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long"
        }),
        ["a:2", "b:3"]
      );
      assert.deepEqual(
        (await kv.listRangeSomeRaw("list", async (raw) => {
          const value = intValue(await raw.readValue(":long"));
          return value === 3 ? [await raw.readKey(":string"), value] : null;
        }, [":all"], {
          kType: ":string",
          vRange: [":all"],
          vType: ":long"
        })).map((value) => typeof value === "bigint" ? intValue(value) : value),
        ["b", 3]
      );
    } finally {
      await kv.close();
    }
  }
);

test(
  "kv operational methods cover wal snapshots and tx log inspection",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-kv-ops-"));
    const kv = await openKv(dir, { ":wal?": true });

    try {
      await kv.openDbi("items");
      await kv.transact([[":put", "a", "alpha"]], {
        dbiName: "items",
        kType: ":string",
        vType: ":string"
      });

      const watermarks = await kv.txLogWatermarks();
      assert.equal(watermarks[":wal?"] ?? watermarks.wal, true);
      assert.equal(Array.isArray(await kv.openTxLog(1, { limit: 10 })), true);

      const snapshot = await kv.createSnapshot();
      const snapshots = await kv.listSnapshots();
      const gc = await kv.gcTxLogSegments();

      assert.equal(typeof snapshot, "object");
      assert.equal(Array.isArray(snapshots), true);
      assert.equal(typeof gc, "object");
    } finally {
      await kv.close();
    }
  }
);

test(
  "search index writer commits kv full-text batches",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-search-writer-"));
    const kv = await openKv(dir);

    try {
      const writer = await searchIndexWriter(kv, searchDomain({ includeText: true }));
      assert.equal(String(writer), "<SearchIndexWriter open>");
      assert.equal(await writer.write("doc-1", "pizza and pasta"), writer);
      await writer.write("doc-2", "just pie");
      assert.equal(await writer.commit(), ":transacted");
      assert.equal(await writer.closed(), true);
      assert.equal(String(writer), "<SearchIndexWriter closed>");

      assert.equal(intValue(await kv.entries("datalevin/docs")), 2);
      assert.equal(intValue(await kv.entries("datalevin/rawtext")), 2);
      assert.equal(intValue(await kv.entries("datalevin/terms")) > 0, true);

      const notesWriter = await kv.searchIndexWriter(searchDomain({ domain: "notes", includeText: true }));
      await notesWriter.write("note-1", "searchable local note");
      await notesWriter.commit();
      assert.equal(intValue(await kv.entries("notes/docs")), 1);

      const engine = await newSearchEngine(kv, searchDomain({ includeText: true }));
      assert.equal(String(engine), "<SearchEngine open>");
      assert.equal(await engine.addDoc("doc-3", "pizza search engine"), engine);
      await engine.addDoc("doc-4", "engine indexing", { checkExist: false });
      assert.equal(intValue(await engine.docCount()), 4);
      assert.equal(await engine.docIndexed("doc-3"), true);
      assert.deepEqual(await engine.search("pizza"), ["doc-1", "doc-3"]);
      assert.equal(await engine.reIndex(searchDomain({ includeText: true })), engine);
      assert.deepEqual(await engine.search("pizza"), ["doc-1", "doc-3"]);
      assert.equal(await reIndex(engine, searchDomain({ includeText: true })), engine);
      await engine.removeDoc("doc-3");
      assert.equal(await reIndex(engine), engine);
      assert.equal(await engine.docIndexed("doc-3"), false);
      await engine.clearDocs();
      assert.equal(intValue(await engine.docCount()), 0);
      await engine.close();
      assert.equal(await engine.closed(), true);
    } finally {
      await kv.close();
    }
  }
);

test(
  "standalone vector index supports add search checkpoint reindex and clear",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-vector-"));
    const kv = await openKv(dir);
    const opts = { ":dimensions": 2 };

    try {
      const index = await newVectorIndex(kv, opts);
      assert.equal(String(index), "<VectorIndex open>");
      assert.equal(intValue((await index.info())[":dimensions"]), 2);

      assert.equal(await index.addVec("vec-1", [1.0, 0.0]), index);
      assert.equal(await index.addVec("vec-2", [0.0, 1.0]), index);
      assert.equal(await index.vecIndexed("vec-1"), true);
      assert.deepEqual(await index.searchVec([1.0, 0.0], { ":top": 1 }), ["vec-1"]);
      assert.deepEqual(await index.searchVec([1.0, 0.0], { ":top": 1, ":display": ":refs+dists" }), [["vec-1", 0.0]]);

      assert.equal(await index.forceCheckpoint(), index);
      assert.equal(typeof await index.checkpointState(), "object");
      assert.equal(await reIndex(index), index);
      assert.deepEqual(await index.searchVec([0.0, 1.0], { ":top": 1 }), ["vec-2"]);
      assert.equal(await index.removeVec("vec-1"), index);
      assert.equal(await index.vecIndexed("vec-1"), false);

      assert.equal(await index.clear(), index);
      assert.equal(await index.closed(), true);
      assert.equal(String(index), "<VectorIndex closed>");

      const kvIndex = await kv.newVectorIndex(opts);
      try {
        assert.equal(intValue((await kvIndex.info())[":size"]), 0);
      } finally {
        await kvIndex.close();
      }
    } finally {
      await kv.close();
    }
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
      await assert.rejects(
        async () => kv.putListItems("items", "a", ["alpha"], { vType: ":string" }),
        TypeError
      );
      await assert.rejects(
        async () => kv.listRange("items", [":all"], { vRange: [":all"], vType: ":string" }),
        TypeError
      );
      await assert.rejects(
        async () => kv.listRange("items", [":all"], { kType: ":string", vRange: [":all"] }),
        TypeError
      );
      await assert.rejects(
        async () => kv.listRangeCount("items", [":all"]),
        TypeError
      );
      await assert.rejects(
        async () => kv.getByRank("items", 0, { ignoreKey: true }),
        TypeError
      );
      await assert.rejects(
        async () => kv.getEntryByRank("items", 0),
        TypeError
      );
      await assert.rejects(
        async () => kv.getEntryByRank("items", 0, { kType: ":string" }),
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
      await raw.coreInvoke("open-dbi", [kv, "blob-keys"]);
      await raw.coreInvoke("transact-kv", [
        kv,
        "items",
        await raw.kvTxs([[":put", "a", "alpha"], [":put", "b", "beta"]]),
        await raw.kvType(":string"),
        await raw.kvType(":string")
      ]);
      await raw.coreInvoke("transact-kv", [
        kv,
        "blob-keys",
        await raw.kvTxs(
          [[":put", Buffer.from([1]), Buffer.from([9])], [":put", Buffer.from([2]), Buffer.from([8])]],
          ":bytes",
          ":bytes"
        ),
        await raw.kvType(":bytes"),
        await raw.kvType(":bytes")
      ]);

      assert.deepEqual((await toJs(await raw.coreInvoke("list-dbis", [kv]))).sort(), ["blob-keys", "items"]);
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
      assert.deepEqual(
        (await toJs(
          await raw.coreInvoke("get-range", [
            kv,
            "blob-keys",
            await raw.kvRange([":closed", Buffer.from([1]), Buffer.from([2])], ":bytes"),
            await raw.kvType(":bytes"),
            await raw.kvType(":bytes")
          ])
        )).map(([key, value]) => [[...key], [...value]]),
        [[[1], [9]], [[2], [8]]]
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
