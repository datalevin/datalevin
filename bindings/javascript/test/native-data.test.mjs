import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { UdfDescriptor, connect, createUdfRegistry, datalogKv, openKv, q, tx } from "../src/index.js";
import { resolveClasspath } from "../src/jvm.js";

const available = (() => { try { return resolveClasspath().length > 0; } catch { return false; } })();
const options = { skip: !available, timeout: 60000 };
const tempDir = () => fs.mkdtempSync(path.join(os.tmpdir(), "datalevin-js-native-"));

class Task {
  constructor(rank, label) { this.rank = rank; this.label = label; }
}

async function taskType({ nativeType = Task, equals, prefix = "task", tupleOrder = false } = {}) {
  const registry = await createUdfRegistry();
  let nonce = 0;
  await registry.orderUdf("native/order", async (task) => {
    assert.ok(task instanceof nativeType);
    if (task.label === "bad-order") throw new Error("bad native order");
    return tupleOrder ? [task.rank, "task"] : task.rank;
  });
  await registry.serializerUdf("native/encode", async (task) => {
    if (task.label === "bad-payload") return "not bytes";
    // Equal values intentionally have different serialized representations.
    return new Uint8Array(Buffer.from(JSON.stringify([prefix, task.rank.toString(), task.label, ++nonce])));
  });
  const decode = async (payload) => {
    assert.ok(Buffer.isBuffer(payload));
    const [actualPrefix, rank, label] = JSON.parse(payload.toString("utf8"));
    assert.equal(actualPrefix, prefix);
    return new nativeType(BigInt(rank), label);
  };
  await registry.deserializerUdf("native/decode", decode);
  const definition = {
    index: { type: tupleOrder ? [":long", ":string"] : ":long", "order-fn": UdfDescriptor.orderFn("native/order") },
    payload: { serialize: UdfDescriptor.serializer("native/encode"),
      deserialize: UdfDescriptor.deserializer("native/decode") }
  };
  await registry.bindNativeType("app/task", nativeType, definition, { equals });
  return { registry, definition, decode, opts: { ":runtime-opts": { ":udf-registry": registry } } };
}

test("native JavaScript KV values use serde snapshots and complete-value matching", options, async () => {
  const dir = tempDir();
  const { registry, definition, opts } = await taskType();
  const a = new Task(1n, "a"), b = new Task(1n, "b"), z = new Task(2n, "λ");
  let kv;
  try {
    kv = await openKv(dir, opts);
    await kv.registerType("app/task", definition);
    await kv.openDbi("tasks", { ":key-type": ":app/task" });
    await kv.openListDbi("owners", { ":value-type": ":app/task" });
    await kv.transact([[":put", a, "a"], [":put", b, "b"], [":put", z, "z"]], { dbiName: "tasks" });
    await kv.putListItems("owners", "alice", [a, b, z], { kType: ":string", vType: ":app/task" });
    assert.equal(await kv.getValue("tasks", new Task(1n, "b")), "b");
    assert.deepEqual(await kv.getRange("tasks", [":closed", a, b]), [[a, "a"], [b, "b"]]);
    assert.deepEqual(await kv.getRange("tasks", [":greater-than", b]), [[z, "z"]]);
    assert.deepEqual(await kv.getList("owners", "alice", { kType: ":string", vType: ":app/task" }), [a, b, z]);
    assert.deepEqual(await kv.listRangeKeep("owners", (_key, value) => new Task(value.rank, value.label),
      [":all"], { kType: ":string", vRange: [":all"], vType: ":app/task" }), [a, b, z]);
    await kv.transact([[":put", new Task(1n, "a"), "updated"]], { dbiName: "tasks" });
    assert.equal(await kv.entries("tasks"), 3n);
    a.label = "changed after write";
    assert.equal(await kv.getValue("tasks", new Task(1n, "a")), "updated");
    for (const [label, error] of [["bad-order", /bad native order/], ["bad-payload", /byte array/]]) {
      await assert.rejects(() => kv.transact([[":del", b], [":put", new Task(3n, label), "bad"]],
        { dbiName: "tasks" }), error);
      assert.equal(await kv.getValue("tasks", b), "b");
    }
    await kv.registerType("app/wrong", definition);
    await kv.openDbi("wrong", { ":key-type": ":app/wrong" });
    await assert.rejects(() => kv.transact([[":put", b, "bad"]], { dbiName: "wrong" }), /different custom type/);
    assert.equal(await kv.entries("wrong"), 0n);
    await kv.openDbi("untyped");
    await assert.rejects(() => kv.transact([[":put", "key", b]], { dbiName: "untyped" }), /Unfreezable|freeze|serializ/);
    assert.equal(await kv.entries("untyped"), 0n);
    const writing = await kv.beginTransaction();
    try {
      await writing.transact([[":del", b]], { dbiName: "tasks" });
      assert.equal(await writing.getValue("tasks", b), null);
    } finally { await writing.abort(); }
    assert.equal(await kv.getValue("tasks", b), "b");
    await kv.withTransaction(async (writing) => writing.transact([[":del", b]], { dbiName: "tasks" }));
    assert.equal(await kv.getValue("tasks", b), null);
    await kv.close();
    const fresh = await taskType();
    kv = await openKv(dir, fresh.opts);
    await kv.openDbi("tasks");
    await fresh.registry.unregister(UdfDescriptor.serializer("native/encode"));
    assert.deepEqual(await kv.getRange("tasks", [":all"]), [[new Task(1n, "a"), "updated"], [z, "z"]]);
    await fresh.registry.unregister(UdfDescriptor.deserializer("native/decode"));
    await assert.rejects(() => kv.getRange("tasks", [":all"]), /UDF|binding/);
    await fresh.registry.register(UdfDescriptor.deserializer("native/decode"), fresh.decode);
    assert.equal((await kv.getRange("tasks", [":all"])).length, 2);
  } finally {
    if (kv) await kv.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("native Datalog inputs, joins, UDF results, pull and transactions", options, async () => {
  const dir = tempDir();
  const { registry, definition, opts } = await taskType();
  const conn = await connect(dir, { opts });
  const a = new Task(1n, "a"), b = new Task(1n, "b");
  try {
    await conn.registerType("app/task", definition);
    await conn.updateSchema({
      "task/value": { ":db/valueType": ":app/task" },
      "task/id": { ":db/valueType": ":app/task", ":db/unique": ":db.unique/identity" },
      "task/many": { ":db/valueType": ":app/task", ":db/cardinality": ":db.cardinality/many" }
    });
    await conn.transact([{ "db/id": 1, "task/value": a, "task/id": a, "task/many": [a, b] },
      { "db/id": 2, "task/value": b, "task/id": b }, { "db/id": 3, "task/value": new Task(1n, "a") }]);
    const e = q.var("e"), v = q.var("v"), rank = q.var("rank");
    const query = q.query({ find: [e], inputs: [q.DB, v], where: [q.pattern(e, "task/value", v)] });
    assert.deepEqual((await conn.query(query, new Task(1n, "a"))).sort(), [[1n], [3n]]);
    assert.deepEqual(await conn.query(query, new Task(1n, "b")), [[2n]]);
    assert.deepEqual((await conn.query("[:find ?v :where [?e :task/value ?v]]")).map(row => row[0].label).sort(), ["a", "b"]);
    // Java set insertion can call back into JavaScript equality. It must be asynchronous.
    assert.deepEqual((await conn.query("[:find ?e :in $ [?v ...] :where [?e :task/value ?v]]",
      new Set([new Task(1n, "a"), new Task(1n, "a"), b]))).sort(), [[1n], [2n], [3n]]);
    assert.deepEqual((await conn.pull("[*]", [":task/id", b]))[":task/value"], b);
    assert.deepEqual(await (await conn.entity(1)).get(":task/value"), a);
    assert.deepEqual((await conn.indexRange("task/value", a, b)).map(row => row[":v"].label).sort(), ["a", "a", "b"]);
    const report = await conn.txDataToSimulatedReport(tx.data(tx.add(2, "task/value", a)));
    assert.deepEqual((await report[":db-after"].pull("[*]", [":task/id", b]))[":task/value"], a);
    assert.deepEqual(await (await conn.entity(2)).get(":task/value"), b);
    await registry.queryUdf("native/make", async (rank) => new Task(rank, "made"));
    const made = q.query({ find: [v], inputs: [q.DB, rank],
      where: [q.bindUdf(UdfDescriptor.queryFn("native/make"), v, rank)] });
    assert.deepEqual(await conn.query(made, 3n), [[new Task(3n, "made")]]);
    await registry.queryUdf("native/echo", value => value);
    const echo = q.query({ find: [v], inputs: [q.DB, rank],
      where: [q.bindUdf(UdfDescriptor.queryFn("native/echo"), v, rank)] });
    const mapped = (await conn.query(echo, new Map([
      [new Task(1n, "a"), 1n], [new Task(1n, "a"), 2n], [b, 3n]
    ])))[0][0];
    assert.ok(mapped instanceof Map);
    assert.deepEqual([...mapped].map(([key, value]) => [key.label, value]).sort(), [["a", 2n], ["b", 3n]]);
    await conn.transact([{ "db/id": "upsert", "task/id": new Task(1n, "b"), "task/value": a }]);
    assert.deepEqual(await (await conn.entity(2)).get(":task/value"), a);
    await conn.transact([[":db/retract", 1, ":task/many", new Task(1n, "b")]]);
    assert.deepEqual((await conn.pull("[*]", 1))[":task/many"], [a]);
    await assert.rejects(() => conn.transact([[":db/retract", 1, ":task/value", a],
      [":db/add", 4, ":task/value", new Task(3n, "bad-order")]]), /bad native order/);
    assert.deepEqual(await (await conn.entity(1)).get(":task/value"), a);
    await conn.transactAsync(tx.data(tx.add(4, "task/value", new Task(4n, "async"))));
    assert.deepEqual(await (await conn.entity(4)).get(":task/value"), new Task(4n, "async"));
    const kv = await datalogKv(conn);
    await kv.openDbi("tasks", { ":key-type": ":app/task" });
    await kv.transact([[":put", a, "a"]], { dbiName: "tasks" });
    assert.equal(await kv.getValue("tasks", new Task(1n, "a")), "a");
  } finally {
    await conn.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("native classes with private state can supply value equality", options, async () => {
  class PrivateTask {
    #rank; #label;
    constructor(rank, label) { this.#rank = rank; this.#label = label; }
    get rank() { return this.#rank; }
    get label() { return this.#label; }
  }
  const equals = async (left, right) => left instanceof PrivateTask && right instanceof PrivateTask
    && left.rank === right.rank && left.label === right.label;
  const { registry, definition, opts } = await taskType({ nativeType: PrivateTask, equals, tupleOrder: true });
  assert.equal(await registry.bindNativeType("app/task", PrivateTask, definition, { equals }), registry);
  const dir = tempDir();
  const kv = await openKv(dir, opts);
  try {
    await kv.registerType("app/task", definition);
    await kv.openDbi("tasks", { ":key-type": ":app/task" });
    await kv.transact([[":put", new PrivateTask(1n, "a"), "a"], [":put", new PrivateTask(1n, "b"), "b"]], { dbiName: "tasks" });
    assert.equal(await kv.getValue("tasks", new PrivateTask(1n, "b")), "b");
    assert.deepEqual((await kv.getRange("tasks", [":all"])).map(([value]) => value.label), ["a", "b"]);
    await assert.rejects(() => registry.bindNativeType("app/task", PrivateTask, definition), /different native type binding/);
  } finally {
    await kv.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("native classes can share a deserializer descriptor", options, async () => {
  class Work extends Task {}
  const { registry, definition, opts } = await taskType();
  await registry.register(UdfDescriptor.deserializer("native/decode"), payload => {
    const [, rank, label] = JSON.parse(payload.toString("utf8"));
    return label.startsWith("work:") ? new Work(BigInt(rank), label) : new Task(BigInt(rank), label);
  });
  await registry.bindNativeType("app/work", Work, definition);
  const dir = tempDir();
  const kv = await openKv(dir, opts);
  try {
    await kv.registerType("app/task", definition);
    await kv.registerType("app/work", definition);
    await kv.openDbi("tasks", { ":key-type": ":app/task" });
    await kv.openDbi("work", { ":key-type": ":app/work" });
    await kv.transact([[":put", new Task(1n, "a"), "a"]], { dbiName: "tasks" });
    await kv.transact([[":put", new Work(1n, "work:a"), "work:a"]], { dbiName: "work" });
    assert.deepEqual(await kv.getRange("tasks", [":all"]), [[new Task(1n, "a"), "a"]]);
    assert.deepEqual(await kv.getRange("work", [":all"]), [[new Work(1n, "work:a"), "work:a"]]);
  } finally {
    await kv.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("native binding and callback failures are actionable and atomic", options, async () => {
  const { registry, definition, opts } = await taskType({ equals: () => "not a boolean" });
  assert.equal(await registry.bindNativeType("app/task", Task, definition,
    { equals: registry._nativeBindings.types.get(Task.prototype).equals }), registry);
  await assert.rejects(() => registry.bindNativeType("app/array", Array, definition), /custom JavaScript class/);
  await assert.rejects(() => registry.bindNativeType("app/task", class Other {}, definition), /different JavaScript class/);
  const dir = tempDir();
  const kv = await openKv(dir, opts);
  try {
    await kv.registerType("app/task", definition);
    await kv.openDbi("tasks", { ":key-type": ":app/task" });
    await kv.transact([[":put", new Task(1n, "a"), "a"]], { dbiName: "tasks" });
    await assert.rejects(() => kv.transact([[":put", new Task(2n, "z"), "z"],
      [":put", new Task(1n, "b"), "b"]], { dbiName: "tasks" }), /equality must return a boolean/);
    assert.equal(await kv.entries("tasks"), 1n);
    await registry.register(UdfDescriptor.deserializer("native/decode"), () => ({ rank: 1n, label: "a" }));
    await assert.rejects(() => kv.getRange("tasks", [":all"]), /must return Task/);
    assert.equal(await kv.entries("tasks"), 1n);
  } finally {
    await kv.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("concurrent operations retain their own native codec context", options, async () => {
  const dirs = [tempDir(), tempDir()];
  const types = await Promise.all([taskType({ prefix: "first" }), taskType({ prefix: "second" })]);
  const kvs = await Promise.all(types.map((type, i) => openKv(dirs[i], type.opts)));
  try {
    await Promise.all(kvs.map(async (kv, i) => {
      await kv.registerType("app/task", types[i].definition);
      await kv.openDbi("tasks", { ":key-type": ":app/task" });
      await kv.transact([[":put", new Task(1n, "same"), String(i)]], { dbiName: "tasks" });
    }));
    for (let n = 0; n < 3; n++) {
      assert.deepEqual(await Promise.all(kvs.map(kv => kv.getValue("tasks", new Task(1n, "same")))), ["0", "1"]);
    }
    const left = await types[0].registry._nativeBindings.types.get(Task.prototype).wrap(new Task(1n, "same"));
    const right = await types[1].registry._nativeBindings.types.get(Task.prototype).wrap(new Task(1n, "same"));
    assert.notEqual(left.codecIdSync(), right.codecIdSync());
    assert.equal(await left.equals(right), true);
    assert.equal(await right.equals(left), true);
    assert.equal(left.hashCodeSync(), right.hashCodeSync());
    await assert.rejects(() => openKv("dtlv://invalid/native", types[0].opts), /local database/);
  } finally {
    await Promise.all(kvs.map(kv => kv.close()));
    for (const dir of dirs) fs.rmSync(dir, { recursive: true, force: true });
  }
});
