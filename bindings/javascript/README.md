# Datalevin Node Bindings

Node.js bindings for Datalevin over the JVM interop bridge.

This package follows the same bridge substrate as the Python bindings:

- `java-bridge` for Node-to-JVM interop
- the shared `datalevin-runtime-<version>.jar`
- thin JavaScript wrappers for local Datalog, local KV, and remote admin usage

The public JS surface stays close to the Python binding surface. Internally the
Node wrapper uses the same Datalevin interop/form-shaping layer, while routing
resource handles through the Java wrapper classes where `java-bridge` needs a
stable JVM type.

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

## Example

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
  console.log(names);
} finally {
  await conn.close();
}
```

## Notes

- The current runtime requires Java 21+.
- Datalevin results are converted into JavaScript values by default.
- Large integer values are exposed as `bigint`.
- `interop()` is intended for advanced bridge use, but opaque raw database
  values are not exposed in Node. Use the high-level `Connection`, `KV`, and
  `Client` wrappers for normal operations.
- `.github/workflows/release.javascript.yml` builds, tests, dry-runs the npm
  package on demand, and publishes tagged releases to npm.
