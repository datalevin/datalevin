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
} finally {
  await kv.close();
}
```

## Remote Client Example

Use `newClient()` for server administration against a running Datalevin server:

```js
import { newClient } from "datalevin-node";

const client = await newClient("dtlv://datalevin:datalevin@localhost");
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

## Notes

- Datalevin results are converted into JavaScript values by default.
- Large integer values are exposed as `bigint`.
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
package on demand, and publishes tagged releases to npm.
