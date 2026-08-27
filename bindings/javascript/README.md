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
import { connect, q, tx } from "datalevin-node";

const entity = q.var("entity");
const name = q.var("name");
const allNames = q.query({
  find: q.collection(name),
  where: [q.datom(entity, "name", name)]
});

const conn = await connect("/tmp/dtlv-js", {
  schema: {
    ":name": {
      ":db/valueType": ":db.type/string",
      ":db/unique": ":db.unique/identity"
    }
  }
});

try {
  await conn.transact(tx.data(
    tx.entity(-1, { name: "Ada" }),
    tx.entity(-2, { name: "Bob" })
  ));

  const names = await conn.query(allNames);
  const ada = await conn.pull([":name"], 1);

  console.log(names);
  console.log(ada);
} finally {
  await conn.close();
}
```

The `q` and `tx` builders are synchronous, pure JavaScript values. Composing
them does not start the JVM; a connection lowers them to the active backend only
when `query()` or `transact()` is called.

Their contract is an immutable snapshot, not a mutable builder. Construction
recursively detaches arrays, plain objects, `Map`, `Set`, `Date`, and byte-array
values. Arrays and objects are frozen; collection and mutable scalar snapshots
reject mutation, including through values returned by `toForm()`. Mutating an
input container later therefore cannot change an existing form. Keyword and
Datalog-symbol tokens are interned by normalized type and name, so
`q.kw("user/id") === q.kw(":user/id")` and repeated tokens occupy one `Map`
key. JavaScript compound objects still use the language's normal identity
comparison; the immutable snapshot, rather than structural `===`, is their
composition contract. Non-structural objects such as backend handles are
treated as atomic values.

## Composing Queries

Use normal JavaScript control flow to assemble clauses. Variables, attributes,
symbols, and keyword values are explicit, so strings beginning with `?` or `:`
remain literal strings.

The same rule applies recursively to runtime inputs passed to a typed query.
Use `q.kw("active")` when an input is a keyword. EDN-string and native-array
queries keep the legacy `":active"` keyword shorthand for compatibility.

```js
const entity = q.var("entity");
const name = q.var("name");
const age = q.var("age");
const minimum = q.var("minimum");

const where = [
  q.datom(entity, "person/name", name),
  q.datom(entity, "person/age", age)
];
if (adultsOnly) {
  where.push(q.predicate(">=", age, minimum));
}

const people = q.query({
  find: q.collection(name),
  inputs: adultsOnly ? [q.DB, minimum] : [],
  where
});

const names = adultsOnly
  ? await conn.query(people, 18)
  : await conn.query(people);
```

Available helpers cover relation, collection, tuple, and scalar finds;
aggregates and pull expressions; database patterns, predicate,
function-binding, rule, and logical clauses; input bindings, joins, rules,
ordering, limits, offsets, and timeouts. `q.datom(e, a, v)` is the common
three-term form; use variable-arity `q.pattern(e, a)` for a presence pattern or
other supported database-pattern arity. `q.raw()` remains the structured escape
hatch for new syntax; its tokens must use `q.kw()` and `q.sym()` explicitly.

### Composing pull selectors

Pull selectors use position-aware forms so option values remain ordinary
JavaScript data:

```js
const person = q.selector(
  "person/name",
  q.pullAttr("person/nickname", { default: "none", as: "display" }),
  q.pullAttr("person/age", { xform: "str" }),
  q.pullNested("person/friend", q.selector("person/name")),
  q.pullRecursive("person/manager", 2)
);

const result = await conn.pull(person, entityId);
```

`default` and `as` accept arbitrary values and preserve strings recursively.
A string `xform` is a symbol name; an explicit `q.sym()` is also accepted.
Omit the second argument to `pullRecursive` for unbounded `...` recursion.
Existing native-array selectors remain supported and are normalized by pull
grammar position rather than by their string contents.

## Composing Transactions

Transaction items compose the same way:

```js
const items = [tx.entity(-1, { "person/name": "Ada" })];
if (nickname !== null) {
  items.push(tx.add(-1, "person/nickname", nickname));
}

const report = await conn.transact(tx.data(items));
```

In addition to entity maps, `tx` provides `add`, `retract`,
`retractAttribute`, `retractEntity`, `compareAndSwap`/`cas`, `call`, `ensure`,
and `patchIdoc` operations. Context-sensitive transaction values also have
explicit builders:

```js
const eve = tx.lookupRef("user/handle", "eve");

const report = await conn.transact(tx.data(
  tx.entity(-1, {
    "user/handle": "alice",
    "user/friend": eve,
    "user/child": tx.entity(-2, { "user/handle": "child" })
  }),
  tx.patchIdoc(eve, "user/profile", [
    tx.patchSet(["status"], "active"),
    tx.patchUpdate(["visits"], "inc"),
    tx.patchUnset(["obsolete"])
  ]),
  tx.invoke("people/audit", eve)
));
```

`lookupRef` turns only its attribute into a keyword, so its lookup value stays
literal. The patch builders similarly type only `set`, `unset`, `update`, and
the update operation; paths, assigned values, and update arguments stay
ordinary JavaScript values. `invoke` emits the direct form for a transaction
function installed under a database ident, while `call` emits
`:db.fn/call`.

Wrap a nested entity object in `tx.entity(...)`, as above. A plain nested
object with ordinary string keys remains data; the builder does not recursively
convert it into an entity. More generally, values remain ordinary JavaScript
data; `q.kw()` and `q.sym()` add explicit Datalevin tokens where needed.

## Data Style

The existing EDN and native-form APIs remain supported alongside the builders:

- schemas are objects keyed by colon-prefixed attribute strings
- transaction data may use `tx`, objects/arrays, or existing `tx*` helpers
- query and pull forms may be builder values, EDN strings, or JavaScript arrays
- colon-prefixed strings are converted to keywords in schema/query/form
  positions for the legacy native-form API
- use synchronous `q.kw()` in builder values, or `keyword()` with the legacy
  API, when the stored value itself is a keyword

```js
import { keyword, readEdn, schemaAttr, txAdd, txEntity, writeEdn } from "datalevin-node";

const schema = {
  ":name": schemaAttr({ valueType: ":db.type/string", unique: ":db.unique/identity" }),
  ":status": schemaAttr({ valueType: ":db.type/keyword" })
};

const tx = [
  txEntity(-1, { ":name": "Ada", ":status": await keyword(":active") }),
  txAdd(-1, ":nickname", "A")
];

const form = await readEdn("[:find ?e :where [?e :name _]]");
const text = await writeEdn([":find", "?e", ":where", ["?e", ":name", "_"]]);
```

## Lazy Entity Example

`conn.entity()` returns a lazy entity wrapper. Use `get()` for individual
attributes, and call `touch()` only when you want a fully materialized object.
`entityMap()` is available for the old eager touched-object shape.

```js
const entity = await conn.entity([":name", "Ada"]);

console.log(await entity.id());
console.log(await entity.get(":name"));
console.log(await entity.get(":db/id"));

const touched = await entity.touch();
const eager = await conn.entityMap(1);
```

## Search, Vector, and Idoc Builders

Use helper builders for search/vector/idoc schema and option maps instead of
hand-writing every namespaced key:

```js
import {
  embeddingAttr,
  embeddingOptions,
  fulltextAttr,
  idocAttr,
  searchDomain,
  searchOptions,
  vectorAttr,
  vectorOptions
} from "datalevin-node";

const schema = {
  ":doc/text": fulltextAttr({ domains: ["docs"], autoDomain: true }),
  ":doc/body": embeddingAttr({ domains: ["docs"], autoDomain: true }),
  ":doc/vec": vectorAttr({ domains: ["docs"] }),
  ":doc/json": idocAttr({ format: "json", domain: "profiles" })
};

const opts = {
  ":search-domains": { docs: searchDomain({ indexPosition: true }) },
  ":search-opts": searchOptions({ top: 5, display: "refs+scores" }),
  ":vector-opts": vectorOptions({ dimensions: 384, metricType: "cosine" }),
  ":embedding-opts": embeddingOptions({
    provider: "openai-compatible",
    model: "text-embedding-3-small",
    baseUrl: "https://api.openai.com/v1",
    apiKeyEnv: "OPENAI_API_KEY",
    requestDimensions: 1536,
    metricType: "cosine"
  })
};
```

## Standalone Vector Index Example

Use `newVectorIndex()` when you want a KV-backed vector index without a Datalog
schema attribute.

```js
import { newVectorIndex, openKv, vectorOptions } from "datalevin-node";

const kv = await openKv("/tmp/dtlv-js-vec");

try {
  const index = await newVectorIndex(kv, vectorOptions({ dimensions: 2 }));
  await index.addVec("doc-1", [1.0, 0.0]);
  await index.addVec("doc-2", [0.0, 1.0]);

  console.log(await index.searchVec([1.0, 0.0], { ":top": 1 }));
  await index.forceCheckpoint();
  console.log(await index.info());
  await index.close();
} finally {
  await kv.close();
}
```

## Local Llama Example

Use `newLlamaEmbedder()` and `newLlamaGenerator()` with local GGUF models when
you want direct llama.cpp handles outside Datalog embedding/search setup.

```js
import { newLlamaEmbedder, newLlamaGenerator } from "datalevin-node";

const embedder = await newLlamaEmbedder("/models/embed.gguf");
try {
  const vector = await embedder.embed("Datalevin stores facts.");
  console.log(await embedder.dimensions(), vector.length);
  console.log(await embedder.tokenCount("Datalevin stores facts."));
} finally {
  await embedder.close();
}

const generator = await newLlamaGenerator("/models/generate.gguf");
try {
  console.log(await generator.generate("Write one database tagline:", 32));
} finally {
  await generator.close();
}
```

## Async Transaction Example

`transact()` uses Datalevin's async transaction batching path and waits until
the transaction commits before returning the report.

Use `transactAsync()` for ingestion and application-server workloads that
benefit from Datalevin's async transaction batching. It returns a normal
JavaScript `Promise`.

```js
const report = await conn.transactAsync([
  { ":db/id": -1, ":name": "Cara" }
]);
```

## UDF Example

Use `createUdfRegistry()` and `udfDescriptor()` for runtime UDFs. Pass the
registry in connection runtime options, then call the descriptor from query with
Datalevin's `udf` function. Fulltext analyzers use the same route with
`registry.analyzerUdf()` or `registry.queryAnalyzerUdf()` and descriptors in
`searchDomain({ analyzer, queryAnalyzer })`. Analyzer functions return
`[term, position, offset]` triples.

```js
import { connect, createUdfRegistry, udfDescriptor } from "datalevin-node";

const registry = await createUdfRegistry();
await registry.queryUdf(":math/inc", (value) => Number(value) + 1);

const descriptor = udfDescriptor(":math/inc");
const conn = await connect("/tmp/dtlv-js-udf", {
  opts: { ":runtime-opts": { ":udf-registry": registry } }
});

try {
  const value = await conn.query(
    "[:find ?v . :in $ ?desc ?n :where [(udf ?desc ?n) ?v]]",
    descriptor,
    41
  );
} finally {
  await conn.close();
}
```

## Fulltext Analyzer UDF Example

Use analyzer UDFs when a Datalog fulltext domain needs host-language tokenizing.
The document analyzer runs while transactions and re-indexing update the
fulltext index; the query analyzer runs during `fulltext` query evaluation.

```js
import {
  connect,
  createUdfRegistry,
  schemaAttr,
  searchDomain,
  udfDescriptor
} from "datalevin-node";

const registry = await createUdfRegistry();
const analyzer = udfDescriptor(":text/hashtags", { kind: ":analyzer" });
const queryAnalyzer = udfDescriptor(":text/plain-query", { kind: ":query-analyzer" });

await registry.analyzerUdf(":text/hashtags", (text) => {
  const tokens = [];
  const pattern = /#\w+/g;
  const source = String(text);
  let match;
  while ((match = pattern.exec(source)) !== null) {
    tokens.push([match[0].slice(1), tokens.length, match.index]);
  }
  return tokens;
});

await registry.queryAnalyzerUdf(":text/plain-query", (text) => (
  String(text).trim().split(/\s+/).filter(Boolean)
    .map((token, position) => [token, position, position])
));

const conn = await connect("/tmp/dtlv-js-fulltext-udf", {
  schema: {
    ":text": schemaAttr({
      valueType: ":db.type/string",
      fulltext: true,
      fulltextAutoDomain: true
    })
  },
  opts: {
    ":runtime-opts": { ":udf-registry": registry },
    ":search-domains": {
      text: searchDomain({
        indexPosition: true,
        analyzer,
        queryAnalyzer
      })
    }
  }
});

try {
  await conn.transact([
    { ":db/id": 1, ":text": "alpha #needle" },
    { ":db/id": 2, ":text": "needle without hash" }
  ]);

  console.log(await conn.query(
    "[:find [?e ...] :in $ ?q :where [(fulltext $ :text ?q) [[?e ?a ?v]]]]",
    "needle"
  ));
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
console.log(await conn.fulltextDatoms("database", { opts: searchOptions({ limit: 5, offset: 10 }) }));
console.log(await conn.datalogIndexCacheLimit());
await conn.datalogIndexCacheLimit(1024);
console.log(await conn.txDataToSimulatedReport([{ ":db/id": -1, ":name": "Dry Run" }]));
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
  console.log(await kv.getEntryByRank("items", 1, {
    kType: ":long",
    vType: ":string"
  }));
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
  console.log(await kv.listRange("tags", [":all"], {
    kType: ":string",
    vRange: [":all"],
    vType: ":string"
  }));
  console.log(await kv.listRangeFirst("tags", [":all"], {
    kType: ":string",
    vRange: [":all"],
    vType: ":string"
  }));
  console.log(await kv.listRangeFirstN("tags", 2, [":all"], {
    kType: ":string",
    vRange: [":all"],
    vType: ":string"
  }));
  console.log(await kv.listRangeCount("tags", [":all"], { kType: ":string" }));
  console.log(await kv.keyRangeListCount("tags", [":all"], {
    kType: ":string"
  }));
  console.log(await kv.listRangeFilter("tags", (key, value) => (
    key === "doc-1" && value.startsWith("c")
  ), [":all"], {
    kType: ":string",
    vRange: [":all"],
    vType: ":string"
  }));
  console.log(await kv.listRangeKeep("tags", (key, value) => (
    value === "db" ? `${key}:${value}` : null
  ), [":all"], {
    kType: ":string",
    vRange: [":all"],
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
  console.log(await client.replicaStatus("demo"));

  // For consensus HA databases, operator membership changes are available as:
  // await client.haUpdateMembership("demo", { ":ha-members": [...], ... });
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

Node bindings include helper builders for newer store features such as
`:embedding-opts`, `:embedding-domains`, and remote `:openai-compatible`
embedding providers. Raw Datalevin option maps are still passed through when
needed:

```js
import { connect, embeddingOptions } from "datalevin-node";

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
    ":embedding-opts": embeddingOptions({
      provider: "openai-compatible",
      model: "text-embedding-3-small",
      baseUrl: "https://api.openai.com/v1",
      apiKeyEnv: "OPENAI_API_KEY",
      requestDimensions: 1536,
      metricType: "cosine"
    })
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

The test suite also executes every case selected by the active release in the
sibling `dtlvtest` golden spec. It discovers `../dtlvtest` automatically; set
`DTLVTEST_ROOT=/path/to/dtlvtest` for a checkout elsewhere. The conformance
adapter reads `spec/manifest.edn`, preserves EDN keyword and symbol types, and
lowers each dataset, transaction, and query through the typed JavaScript
builders. The test is skipped only when no `dtlvtest` checkout is available.

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
