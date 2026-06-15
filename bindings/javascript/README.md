# Datalevin Node Bindings

Node.js bindings for Datalevin over the JVM interop bridge.

## Install

```bash
npm install datalevin-node
```

Requirements:

- Node.js 20+
- Java 21+

The published package vendors the shared `datalevin-runtime-<version>.jar`, so
normal usage does not require building Datalevin from source.

## Quick Start

```js
import { connect } from "datalevin-node";

const conn = await connect("/tmp/dtlv-js", {
  schema: {
    ":name": {
      ":db/valueType": ":db.type/string",
      ":db/unique": ":db.unique/identity"
    }
  }
});

try {
  await conn.transact([
    { ":db/id": -1, ":name": "Ada" },
    { ":db/id": -2, ":name": "Bob" }
  ]);

  const names = await conn.query("[:find [?name ...] :where [?e :name ?name]]");
  const ada = await conn.pull([":name"], 1);

  console.log(names);
  console.log(ada);
} finally {
  await conn.close();
}
```

## Datalog-Backed KV Example

Use `datalogKv()` when you need ordinary KV tables in the same store as a
Datalog connection. The returned KV handle is borrowed from the connection; do
not close it separately.

```js
import { datalogKv } from "datalevin-node";

const kv = await datalogKv(conn);
await kv.openDbi("app-state");
await kv.transact([[":put", "k", "v"]], {
  dbiName: "app-state",
  kType: ":string",
  vType: ":string"
});
```

## Datom Inspection Example

Connection objects expose index-level reads for debugging, teaching, and
migration tooling. Datom reads return objects with `:e`, `:a`, `:v`, `:tx`,
and `:added` keys; `fulltextDatoms()` returns `[e, attr, value]` triples.

```js
console.log(await conn.datoms(":eav", { c1: 1, c2: ":name", limit: 10 }));
console.log(await conn.seekDatoms(":ave", { c1: ":name", c2: "Ada", limit: 5 }));
console.log(await conn.rseekDatoms(":ave", { c1: ":name", c2: "Bob", limit: 5 }));
console.log(await conn.indexRange(":name", "A", "C"));
console.log(await conn.countDatoms({ attr: ":name", value: "Ada" }));
console.log(await conn.fulltextDatoms("database", { opts: { ":top": 5 } }));
```

## Bulk Load Example

Use `initDb()` and `fillDb()` when you already have Datom-shaped data and want
the fast bulk-load path. Datoms can be compact arrays in
`[entityId, attr, value]` shape, and `datom()` creates the same shape.

```js
import { datom, fillDb, initDb } from "datalevin-node";

const schema = { ":name": { ":db/valueType": ":db.type/string" } };
const conn = await initDb([[1, ":name", "Ada"]], {
  dir: "/tmp/dtlv-js-bulk",
  schema
});

try {
  await fillDb(conn, [[2, ":name", "Bob"]]);
  await conn.fillDb([datom(3, ":name", "Cara")]);
} finally {
  await conn.close();
}
```

## KV Example

```js
import { openKv } from "datalevin-node";

const kv = await openKv("/tmp/dtlv-js-kv");

try {
  await kv.openDbi("items");
  await kv.transact(
    [[":put", 1, "alpha"], [":put", 2, "beta"]],
    { dbiName: "items", kType: ":long", vType: ":string" }
  );

  console.log(await kv.getValue("items", 2, {
    kType: ":long",
    vType: ":string",
    ignoreKey: true
  }));
  console.log(await kv.getRange("items", [":all"], {
    kType: ":long",
    vType: ":string"
  }));
  console.log(await kv.getRank("items", 2, { kType: ":long" }));
  console.log(await kv.getFirstN("items", 2, [":all"], {
    kType: ":long",
    vType: ":string"
  }));

  await kv.openListDbi("tags");
  await kv.putListItems("tags", "doc-1", ["clj", "db"], {
    kType: ":string",
    vType: ":string"
  });
  console.log(await kv.getList("tags", "doc-1", {
    kType: ":string",
    vType: ":string"
  }));
} finally {
  await kv.close();
}
```

## Operational Example

KV stores expose backup, durability, snapshot, and WAL inspection helpers
without raw JSON calls.

```js
import { openKv } from "datalevin-node";

const kv = await openKv("/tmp/dtlv-js-ops", { ":wal?": true });

try {
  await kv.openDbi("items");
  await kv.transact([[":put", "a", "alpha"]], {
    dbiName: "items",
    kType: ":string",
    vType: ":string"
  });

  await kv.sync();
  await kv.copy("/tmp/dtlv-js-ops-copy");

  console.log(await kv.txLogWatermarks());
  console.log(await kv.openTxLog(1, { limit: 10 }));
  console.log(await kv.createSnapshot());
  console.log(await kv.listSnapshots());
  console.log(await kv.gcTxLogSegments());
} finally {
  await kv.close();
}
```

## Remote Client Example

Use `newClient()` for server administration against a running Datalevin server:

```js
import { newClient } from "datalevin-node";

const clientOpts = {
  ":pool-size": 1,
  ":time-out": 5000,
  ":ha-write-retry-timeout-ms": 5000,
  ":ha-write-retry-delay-ms": 100
};

const client = await newClient("dtlv://datalevin:datalevin@localhost", clientOpts);
let created = false;
let opened = false;

try {
  await client.createDatabase("demo", "datalog");
  created = true;
  const info = await client.openDatabase("demo", "datalog", {
    schema: {
      ":name": {
        ":db/valueType": ":db.type/string",
        ":db/unique": ":db.unique/identity"
      }
    },
    info: true
  });
  opened = true;

  console.log(info);
  console.log(await client.listDatabases());
} finally {
  if (opened) {
    await client.closeDatabase("demo");
  }
  if (created) {
    await client.dropDatabase("demo");
  }
  await client.disconnect();
}
```

## Embedding Search Options

Node bindings pass Datalevin option maps through unchanged, so newer store
features such as `:embedding-opts`, `:embedding-domains`, and remote
`:openai-compatible` embedding providers are available directly from
`connect()`:

```js
import { connect } from "datalevin-node";

const conn = await connect("/tmp/dtlv-js-embed", {
  schema: {
    ":doc/id": {
      ":db/valueType": ":db.type/string",
      ":db/unique": ":db.unique/identity"
    },
    ":doc/text": {
      ":db/valueType": ":db.type/string",
      ":db/embedding": true,
      ":db.embedding/domains": ["docs"],
      ":db.embedding/autoDomain": true
    }
  },
  opts: {
    ":embedding-opts": {
      ":provider": ":openai-compatible",
      ":model": "text-embedding-3-small",
      ":base-url": "https://api.openai.com/v1",
      ":api-key-env": "OPENAI_API_KEY",
      ":request-dimensions": 1536,
      ":metric-type": ":cosine"
    }
  }
});

await conn.close();
```

## Notes

- Datalevin results are converted into JavaScript values by default.
- Large integer values are exposed as `bigint`.
- Remote client options such as `:ha-write-retry-timeout-ms` and
  `:ha-write-retry-delay-ms` can be passed to `newClient()`.
- `interop()` is intended for advanced bridge use.

## Development

From this repo, the wrapper can run against:

1. `DATALEVIN_JAR=/path/to/datalevin-runtime-<version>.jar`
2. a vendored jar under `jars/`
3. a repo-local build in `target/`

Typical local flow:

```bash
clojure -T:build vendor-jar
cd bindings/javascript
npm install
npm test
```

`vendor-jar` builds a platform-specific runtime jar for the current build host
by default. To keep the cross-platform native payloads, pass:

```bash
clojure -T:build vendor-jar :native-platform all
```

`npm run vendor-runtime` vendors the publishable shared runtime jar and defaults
to `DATALEVIN_NATIVE_PLATFORM=all`. Override that environment variable if you
want a host-specific vendored jar during development.

For ad hoc development against a different build, set `DATALEVIN_JAR` to point
at another embeddable Datalevin runtime jar, preferably
`target/datalevin-runtime-<version>.jar`.

`.github/workflows/release.javascript.yml` builds, tests, dry-runs the npm
package on demand, and uploads the package tarball as an artifact. It does not
publish to npm.

For the local manual release helper, see
[`script/deploy-javascript.md`](../../script/deploy-javascript.md).
