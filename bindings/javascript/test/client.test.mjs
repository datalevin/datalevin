import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";

import { DatalevinJavaError, jvmStarted, newClient } from "../src/index.js";
import { clojureCliAvailable, runtimeAvailable, startLiveServer } from "../test-support/live-server.mjs";

const integrationAvailable = runtimeAvailable() && clojureCliAvailable();
const CLIENT_OPTS = {
  ":pool-size": 1,
  ":time-out": 5000,
  ":ha-write-retry-timeout-ms": 5000,
  ":ha-write-retry-delay-ms": 100
};
const SCHEMA = {
  ":name": {
    ":db/valueType": ":db.type/string",
    ":db/unique": ":db.unique/identity"
  }
};

function matchesPermission(permission, act, obj) {
  return permission[":permission/act"] === act && permission[":permission/obj"] === obj;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, { timeoutMs = 5000, intervalMs = 50 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return true;
    }
    await delay(intervalMs);
  }
  return predicate();
}

async function ignoreErrors(action) {
  try {
    await action();
  } catch {
    // Best-effort cleanup for integration resources.
  }
}

let liveServer = null;

before(async () => {
  if (integrationAvailable) {
    liveServer = await startLiveServer();
  }
});

after(async () => {
  if (liveServer !== null) {
    await liveServer.stop();
    liveServer = null;
  }
  if (jvmStarted()) {
    setImmediate(() => process.exit(process.exitCode ?? 0));
  }
});

test(
  "client database lifecycle works against a live server",
  { skip: !integrationAvailable, timeout: 30000 },
  async () => {
    const suffix = randomUUID();
    const dbName = `js-client-db-${suffix}`;
    const client = await newClient(liveServer.adminUri(), CLIENT_OPTS);

    try {
      assert.equal(await client.disconnected(), false);

      await client.createDatabase(dbName, "datalog");
      const openInfo = await client.openDatabase(dbName, "datalog", {
        schema: SCHEMA,
        info: true
      });
      assert.equal(typeof openInfo, "object");
      assert.equal((await client.listDatabases()).includes(dbName), true);
      assert.equal((await client.listDatabasesInUse()).includes(dbName), true);

      await client.closeDatabase(dbName);
      await client.openDatabase(dbName, "datalog");
      assert.equal((await client.listDatabasesInUse()).includes(dbName), true);
      assert.equal(
        (await client.querySystem(
          "[:find [?name ...] :where [?e :database/name ?name]]"
        )).includes(dbName),
        true
      );
    } finally {
      await ignoreErrors(() => client.closeDatabase(dbName));
      await ignoreErrors(() => client.dropDatabase(dbName));
      await ignoreErrors(() => client.disconnect());
    }
  }
);

test(
  "client user, role, and permission flow works against a live server",
  { skip: !integrationAvailable, timeout: 30000 },
  async () => {
    const suffix = randomUUID();
    const dbName = `js-client-admin-${suffix}`;
    const username = `js-user-${suffix}`;
    const password = "secret-one";
    const nextPassword = "secret-two";
    const role = `js-role-${suffix}`;
    const roleKey = `:${role}`;
    const client = await newClient(liveServer.adminUri(), CLIENT_OPTS);
    let userClient = null;

    try {
      await client.createDatabase(dbName, "datalog");
      await client.openDatabase(dbName, "datalog", {
        schema: SCHEMA,
        info: true
      });

      await client.createUser(username, password);
      await client.resetPassword(username, nextPassword);
      await client.createRole(role);
      await client.assignRole(role, username);
      await client.grantPermission(
        role,
        ":datalevin.server/alter",
        ":datalevin.server/database",
        dbName
      );

      assert.equal((await client.listUsers()).includes(username), true);
      assert.equal((await client.listRoles()).includes(roleKey), true);
      assert.equal((await client.listUserRoles(username)).includes(roleKey), true);
      assert.equal(
        (await client.listRolePermissions(role)).some((permission) =>
          matchesPermission(permission, ":datalevin.server/alter", ":datalevin.server/database")
        ),
        true
      );
      assert.equal(
        (await client.listUserPermissions(username)).some((permission) =>
          matchesPermission(permission, ":datalevin.server/alter", ":datalevin.server/database")
        ),
        true
      );
      assert.equal(
        (await client.querySystem([
          ":find",
          ["?u", "..."],
          ":where",
          ["?e", ":user/name", "?u"]
        ])).includes(username),
        true
      );

      userClient = await newClient(liveServer.adminUri(username, nextPassword), CLIENT_OPTS);
      assert.equal(await userClient.disconnected(), false);
      await assert.rejects(async () => userClient.listDatabases(), DatalevinJavaError);
      await userClient.disconnect();
      assert.equal(await userClient.disconnected(), true);

      await client.revokePermission(
        role,
        ":datalevin.server/alter",
        ":datalevin.server/database",
        dbName
      );
      await client.withdrawRole(role, username);

      assert.equal((await client.listUserRoles(username)).includes(roleKey), false);
      assert.equal(
        (await client.listRolePermissions(role)).some((permission) =>
          matchesPermission(permission, ":datalevin.server/alter", ":datalevin.server/database")
        ),
        false
      );
    } finally {
      await ignoreErrors(() => client.closeDatabase(dbName));
      await ignoreErrors(() => client.dropRole(role));
      await ignoreErrors(() => client.dropUser(username));
      await ignoreErrors(() => client.dropDatabase(dbName));
      if (userClient !== null) {
        await ignoreErrors(() => userClient.disconnect());
      }
      await ignoreErrors(() => client.disconnect());
    }
  }
);

test(
  "admin disconnect removes another client from showClients on a live server",
  { skip: !integrationAvailable, timeout: 30000 },
  async () => {
    const suffix = randomUUID();
    const dbName = `js-client-clients-${suffix}`;
    const client = await newClient(liveServer.adminUri(), CLIENT_OPTS);
    const otherClient = await newClient(liveServer.adminUri(), CLIENT_OPTS);

    try {
      const clientId = await client.clientId();
      const otherId = await otherClient.clientId();

      await client.createDatabase(dbName, "datalog");
      await client.openDatabase(dbName, "datalog", {
        schema: SCHEMA,
        info: true
      });

      const clients = await client.showClients();
      assert.equal(typeof clients[clientId], "object");
      assert.equal(typeof clients[otherId], "object");
      assert.equal(typeof clients[clientId][":open-dbs"], "object");
      assert.equal(typeof clients[clientId][":open-dbs"][dbName], "object");

      await client.disconnectClient(otherId);

      assert.equal(
        await waitFor(async () => {
          const currentClients = await client.showClients();
          return currentClients[otherId] === undefined;
        }),
        true
      );
    } finally {
      await ignoreErrors(() => client.closeDatabase(dbName));
      await ignoreErrors(() => client.dropDatabase(dbName));
      await ignoreErrors(() => otherClient.disconnect());
      await ignoreErrors(() => client.disconnect());
    }
  }
);
