import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";

import { UdfDescriptor, connect, createUdfRegistry, datalogKv, openKv, q } from "../src/index.js";
import { resolveClasspath } from "../src/jvm.js";

const runtimeAvailable = (() => {
  try { return resolveClasspath().length > 0; } catch { return false; }
})();

const a = { rank: 1n, label: "a", extra: null };
const b = { rank: 1n, label: "b", extra: null };
const z = { rank: 2n, label: "λ", extra: null };

async function taskType(tupleOrder = false) {
  const registry = await createUdfRegistry();
  await registry.orderUdf("task/order", (v) => tupleOrder ? [v.rank, "task"] : v.rank, { version: 1 });
  await registry.serializerUdf("task/encode", (v) => {
    if (v.label === "bad") return "not bytes";
    return Buffer.from(JSON.stringify(v, (_key, value) =>
      typeof value === "bigint" ? Number(value) : value));
  }, { version: 1 });
  const decode = (payload) => {
    assert.equal(Buffer.isBuffer(payload), true);
    const value = JSON.parse(payload.toString("utf8"));
    return { ...value, rank: BigInt(value.rank) };
  };
  await registry.deserializerUdf("task/decode", decode, { version: 1 });
  const definition = {
    index: {
      type: tupleOrder ? [":long", ":string"] : ":long",
      "order-fn": UdfDescriptor.orderFn("task/order", { version: 1 })
    },
    payload: {
      serialize: UdfDescriptor.serializer("task/encode", { version: 1 }),
      deserialize: UdfDescriptor.deserializer("task/decode", { version: 1 })
    }
  };
  return { registry, definition, decode };
}

for (const tupleOrder of [false, true]) {
  test(`custom KV ordering and raw payloads (${tupleOrder ? "tuple" : "scalar"})`,
    { skip: !runtimeAvailable, timeout: 60000 }, async () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "datalevin-js-custom-"));
      const { registry, definition, decode } = await taskType(tupleOrder);
      const opts = { ":runtime-opts": { ":udf-registry": registry } };
      let kv;
      try {
        kv = await openKv(dir, opts);
        assert.equal(await kv.registerType("app/task", definition), ":app/task");
        assert.equal(await kv.registerType(":app/task", definition), ":app/task");
        await assert.rejects(() => kv.registerType("app/task", { ...definition, version: 2 }),
          /different definition/);
        await kv.openDbi("tasks", { ":key-type": ":app/task" });
        await kv.openListDbi("owners", { ":value-type": ":app/task" });
        await kv.transact([[":put", a, "a"], [":put", b, "b"], [":put", z, "z"]],
          { dbiName: "tasks" });
        await kv.putListItems("owners", "alice", [a, b, z], { kType: ":string", vType: ":app/task" });
        assert.equal(await kv.getValue("tasks", { ...a }), "a");
        assert.deepEqual(await kv.getRange("tasks", [":closed", a, b]), [[a, "a"], [b, "b"]]);
        assert.deepEqual(await kv.getRange("tasks", [":greater-than", b]), [[z, "z"]]);
        assert.deepEqual(await kv.getList("owners", "alice", { kType: ":string", vType: ":app/task" }), [a, b, z]);
        await assert.rejects(() => kv.transact([[":del", a],
          [":put", { rank: 3n, label: "bad" }, "bad"]], { dbiName: "tasks" }), /byte array/);
        assert.equal(await kv.getValue("tasks", a), "a");
        const tx = await kv.beginTransaction();
        try {
          await tx.registerType("app/aborted", definition);
          await tx.transact([[":del", b]], { dbiName: "tasks" });
        } finally {
          await tx.abort();
        }
        assert.equal(await kv.getValue("tasks", b), "b");
        assert.equal(await kv.registerType("app/aborted", { ...definition, version: 2 }), ":app/aborted");
        await kv.close();
        kv = await openKv(dir);
        await kv.openDbi("tasks");
        await assert.rejects(() => kv.getValue("tasks", a), /UDF registry/);
        await kv.close();
        kv = await openKv(dir, opts);
        await kv.openDbi("tasks");
        assert.deepEqual(await kv.getRange("tasks", [":all"]), [[a, "a"], [b, "b"], [z, "z"]]);
        const decoder = UdfDescriptor.deserializer("task/decode", { version: 1 });
        await registry.unregister(decoder);
        await assert.rejects(() => kv.getRange("tasks", [":all"]), /UDF/);
        await registry.register(decoder, decode);
        assert.equal(await kv.getValue("tasks", a), "a");
      } finally {
        if (kv) await kv.close();
        fs.rmSync(dir, { recursive: true, force: true });
      }
    });
}

test("custom Datalog attributes share registrations with KV",
  { skip: !runtimeAvailable, timeout: 60000 }, async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "datalevin-js-custom-db-"));
    const { registry, definition } = await taskType();
    const conn = await connect(dir, { opts: { ":runtime-opts": { ":udf-registry": registry } } });
    try {
      assert.equal(await conn.registerType("app/task", definition), ":app/task");
      await conn.updateSchema({
        "task/value": { ":db/valueType": ":app/task" },
        "task/id": { ":db/valueType": ":app/task", ":db/unique": ":db.unique/identity" }
      });
      await conn.transact([{ "db/id": 1, "task/value": a, "task/id": a },
        { "db/id": 2, "task/value": b, "task/id": b }]);
      const e = q.var("e"), v = q.var("v");
      const query = q.query({ find: [e], inputs: [q.DB, v], where: [q.pattern(e, "task/value", v)] });
      assert.deepEqual(await conn.query(query, a), [[1n]]);
      assert.deepEqual(await conn.query(query, b), [[2n]]);
      assert.deepEqual((await conn.pull("[*]", [":task/id", b]))[":task/value"], b);
      assert.deepEqual((await conn.indexRange("task/value", a, b)).map(row => row[":v"]), [a, b]);
      const kv = await datalogKv(conn);
      assert.equal(await kv.registerType("app/task", definition), ":app/task");
      await kv.openDbi("tasks", { ":key-type": ":app/task" });
      await kv.transact([[":put", a, "a"]], { dbiName: "tasks" });
      assert.equal(await kv.getValue("tasks", a), "a");
    } finally {
      await conn.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
