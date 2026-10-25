import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { after, before, test } from "node:test";

import { connect, listenDb, openKv, q, unlistenDb, updateKv } from "../src/index.js";
import { javaBridgeModule } from "../src/jvm.js";
import { clojureCliAvailable, runtimeAvailable, startLiveServer } from "../test-support/live-server.mjs";

const runtime = runtimeAvailable();
const integration = runtime && clojureCliAvailable();
const increment = "(fn [old amount] (+ (long (or old 0)) (long amount)))";
const types = { kType: ":string", vType: ":long" };
const readTypes = { ...types, ignoreKey: true };
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
let server;

before(async () => { if (integration) server = await startLiveServer(); });
after(async () => { if (server) await server.stop(); });

async function waitFor(predicate) {
  const deadline = Date.now() + 5000;
  while (!predicate() && Date.now() < deadline) await delay(20);
  assert.ok(predicate(), "database notification did not arrive");
}

test("atomic KV updates use callbacks, source, extra arguments, and transaction rollback", {
  skip: !runtime, timeout: 30000
}, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-update-"));
  const kv = await openKv(dir);
  try {
    await kv.openDbi("counts");
    assert.equal(await updateKv(kv, "counts", "key", (old, amount) => {
      assert.equal(old, null);
      return amount;
    }, { ...types, args: [2] }), ":transacted");
    await Promise.all(Array.from({ length: 16 }, () => kv.update("counts", "key", old => Number(old) + 1, types)));
    assert.equal(Number(await kv.getValue("counts", "key", readTypes)), 18);
    await kv.update("counts", "key", increment, { ...types, args: [3] });
    const tx = await kv.transaction();
    await tx.update("counts", "key", old => Number(old) + 10, types);
    assert.equal(Number(await tx.getValue("counts", "key", readTypes)), 31);
    await tx.abort();
    assert.equal(Number(await kv.getValue("counts", "key", readTypes)), 21);
    await assert.rejects(kv.update("counts", "key", () => { throw new Error("update failed"); }, types), /update failed/);
    await assert.rejects(kv.update("counts", "key", () => null, types));
    assert.equal(Number(await kv.getValue("counts", "key", readTypes)), 21);
    await kv.openDbi("data");
    await kv.update("data", "key", old => ({ count: (old?.count ?? 0) + 1 }));
    await kv.update("data", "key", old => ({ count: Number(old.count) + 1 }));
    assert.equal(Number((await kv.getValue("data", "key")).count), 2);
    await kv.openDbi("keywords");
    await kv.update("keywords", q.kw("key"), () => q.kw("value"), { kType: ":keyword", vType: ":keyword" });
    assert.equal(await kv.getValue("keywords", q.kw("key"), {
      kType: ":keyword", vType: ":keyword", ignoreKey: true
    }), ":value");
  } finally {
    await kv.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("generated database subscription keys stop their polling threads", {
  skip: !integration, timeout: 30000
}, async () => {
  const name = `js-unlisten-${randomUUID()}`;
  const conn = await connect(server.databaseUri(name));
  try {
    const key = await conn.listenDb(() => {});
    const otherKey = await listenDb(conn, () => {}, null);
    assert.equal(typeof key, "string");
    assert.equal(typeof otherKey, "string");
    assert.notEqual(key, otherKey);

    const { importClass } = await javaBridgeModule();
    const Thread = importClass("java.lang.Thread");
    const threads = Thread.getAllStackTracesSync().keySetSync().toArraySync()
      .filter(thread => thread.getNameSync() === `datalevin-db-listener-${name}`);
    assert.equal(threads.length, 2);
    const liveThreads = () => threads.filter(thread => thread.isAliveSync()).length;

    await conn.unlistenDb(key);
    assert.equal(conn._dbListenerProxies.has(key), false);
    assert.equal(conn._dbListenerProxies.has(otherKey), true);
    // Proxy removal alone would also pass when the UUID key fails to round-trip.
    assert.equal(liveThreads(), 1, "unlistenDb must stop the matching polling thread");
    await unlistenDb(conn, otherKey);
    assert.equal(conn._dbListenerProxies.size, 0);
    assert.equal(liveThreads(), 0, "all subscriptions must stop before connection closure");
    await conn.unlistenDb(key);
  } finally {
    await conn.close();
  }
});

test("remote updates and database subscriptions cover other clients, replacement, abort, and cleanup", {
  skip: !integration, timeout: 30000
}, async () => {
  const name = `js-notify-${randomUUID()}`;
  const uri = server.databaseUri(name);
  const opts = { ":pool-size": 1, ":time-out": 5000 };
  const reader = await connect(uri, { schema: { ":name": { ":db/valueType": ":db.type/string" } }, opts });
  const writer = await connect(uri, { opts });
  const kv = await writer.datalogKv();
  try {
    await kv.openDbi("counts");
    const events = [];
    const oldEvents = [];
    assert.equal(await reader.listenDb(event => oldEvents.push(event), "watch"), "watch");
    assert.equal(await listenDb(reader, async event => {
      if (event[":type"] === ":db-changed") {
        events.push({ event, names: await reader.query("[:find [?name ...] :where [?e :name ?name]]") });
      }
    }, "watch"), "watch");
    await writer.transact([{ ":db/id": -1, ":name": "first" }]);
    await waitFor(() => events.some(item => item.names.includes("first")));
    assert.equal(events[0].event[":db-name"], name);
    assert.equal(oldEvents.length, 0);
    const beforeUpdate = events.length;
    assert.equal(await kv.update("counts", "key", increment, { ...types, args: [4] }), ":transacted");
    await waitFor(() => events.length > beforeUpdate);
    assert.equal(Number(await kv.getValue("counts", "key", readTypes)), 4);
    const beforeAbort = events.length;
    const tx = await kv.transaction();
    await updateKv(tx, "counts", "key", increment, { ...types, args: [5] });
    await tx.abort();
    await delay(200);
    assert.equal(events.length, beforeAbort);
    assert.equal(Number(await kv.getValue("counts", "key", readTypes)), 4);
    await unlistenDb(reader, "watch");
    await writer.transact([{ ":db/id": -2, ":name": "second" }]);
    await delay(200);
    assert.equal(events.length, beforeAbort);
    const autoEvents = [];
    const key = await reader.listenDb(event => autoEvents.push(event));
    assert.ok(key);
    await writer.transact([{ ":db/id": -3, ":name": "third" }]);
    await waitFor(() => autoEvents.length > 0);
    await reader.close();
    assert.equal(reader._dbListenerProxies.size, 0);
    const beforeClose = autoEvents.length;
    await writer.transact([{ ":db/id": -4, ":name": "fourth" }]);
    await delay(200);
    assert.equal(autoEvents.length, beforeClose);
  } finally {
    await reader.close();
    await writer.close();
  }
});
