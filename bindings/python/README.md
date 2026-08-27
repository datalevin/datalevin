# Datalevin Python Bindings

Python bindings for Datalevin over the JVM interop bridge.

## Install

```bash
python -m pip install datalevin
```

Requirements:

- Python 3.10+
- Java 21+

Published wheels bundle the shared Datalevin runtime jar, so normal usage does
not require building Datalevin from source.

## Quick Start

```python
from datalevin import connect, q, tx

entity = q.var("entity")
name = q.var("name")
all_names = q.query(
    find=q.collection(name),
    where=[q.datom(entity, "name", name)],
)

with connect(
    "/tmp/dtlv-py",
    schema={
        ":name": {
            ":db/valueType": ":db.type/string",
            ":db/unique": ":db.unique/identity",
        }
    },
) as conn:
    conn.transact(
        tx.data(
            tx.entity(-1, {"name": "Ada"}),
            tx.entity(-2, {"name": "Bob"}),
        )
    )

    names = conn.query(all_names)
    ada = conn.pull([":name"], 1)

    print(names)
    print(ada)
```

The `q` and `tx` builders are pure Python values: composing them does not start
the JVM. The connection lowers them to the active backend only when `query()` or
`transact()` is called.

Their contract is an immutable snapshot, not a mutable builder. Construction
recursively detaches Python dictionaries, lists, sets, `bytearray`, and
`memoryview` values; mappings become read-only mappings, sequences become
tuples, and sets become `frozenset` values. Mutating an input container later
therefore cannot change an existing form, and `to_form()` does not expose a
mutable structural container. `q.kw()` and `q.sym()` tokens use Python value
equality and hashing. Non-structural objects such as backend handles are treated
as atomic values.

## Composing Queries

Use normal Python control flow to assemble clauses. Variables, attributes,
symbols, and keyword values are explicit, so strings beginning with `?` or `:`
remain literal strings.

The same rule applies recursively to runtime inputs passed to a typed query.
Use `q.kw("active")` when an input is a keyword. EDN-string and native-list
queries keep the legacy `":active"` keyword shorthand for compatibility.

```python
entity = q.var("entity")
name = q.var("name")
age = q.var("age")
minimum = q.var("minimum")

where = [
    q.datom(entity, "person/name", name),
    q.datom(entity, "person/age", age),
]
if adults_only:
    where.append(q.predicate(">=", age, minimum))

people = q.query(
    find=q.collection(name),
    inputs=[q.DB, minimum] if adults_only else [],
    where=where,
)

names = conn.query(people, 18) if adults_only else conn.query(people)
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
Python data:

```python
person = q.selector(
    "person/name",
    q.pull_attr("person/nickname", default="none", as_="display"),
    q.pull_attr("person/age", xform="str"),
    q.pull_nested("person/friend", q.selector("person/name")),
    q.pull_recursive("person/manager", depth=2),
)

result = conn.pull(person, entity_id)
```

`default` and `as_` accept arbitrary values and preserve strings recursively.
A string `xform` is a symbol name; an explicit `q.sym()` is also accepted.
Omit `depth` from `pull_recursive` for unbounded `...` recursion. Existing
native-list selectors remain supported and are normalized by pull grammar
position rather than by their string contents.

## Composing Transactions

Transaction items compose the same way:

```python
items = [tx.entity(-1, {"person/name": "Ada"})]
if nickname is not None:
    items.append(tx.add(-1, "person/nickname", nickname))

report = conn.transact(tx.data(items))
```

In addition to entity maps, `tx` provides `add`, `retract`,
`retract_attribute`, `retract_entity`, `compare_and_swap`/`cas`, `call`,
`ensure`, and `patch_idoc` operations. Context-sensitive transaction values
also have explicit builders:

```python
eve = tx.lookup_ref("user/handle", "eve")

report = conn.transact(tx.data(
    tx.entity(-1, {
        "user/handle": "alice",
        "user/friend": eve,
        "user/child": tx.entity(-2, {"user/handle": "child"}),
    }),
    tx.patch_idoc(eve, "user/profile", [
        tx.patch_set(["status"], "active"),
        tx.patch_update(["visits"], "inc"),
        tx.patch_unset(["obsolete"]),
    ]),
    tx.invoke("people/audit", eve),
))
```

`lookup_ref` turns only its attribute into a keyword, so its lookup value stays
literal. The patch builders similarly type only `set`, `unset`, `update`, and
the update operation; paths, assigned values, and update arguments stay
ordinary Python values. `invoke` emits the direct form for a transaction
function installed under a database ident, while `call` emits
`:db.fn/call`.

Wrap a nested entity map in `tx.entity(...)`, as above. A plain nested
dictionary with ordinary string keys remains data; the builder does not
recursively convert it into an entity. More generally, values remain ordinary
Python data; `q.kw()` and `q.sym()` add explicit Datalevin tokens where needed.

## Data Style

The existing EDN and native-form APIs remain supported alongside the builders:

- schemas are dictionaries keyed by colon-prefixed attribute strings
- transaction data may use `tx`, dictionaries/lists, or existing `tx_*` helpers
- query and pull forms may be builder values, EDN strings, or Python lists
- colon-prefixed strings are converted to keywords in schema/query/form
  positions for the legacy native-form API
- use pure `q.kw()` in builder values, or `keyword()` with the legacy API, when
  the stored value itself is a keyword

```python
from datalevin import keyword, read_edn, schema_attr, tx_add, tx_entity, write_edn

schema = {
    ":name": schema_attr(value_type=":db.type/string", unique=":db.unique/identity"),
    ":status": schema_attr(value_type=":db.type/keyword"),
}

tx = [
    tx_entity(-1, {":name": "Ada", ":status": keyword(":active")}),
    tx_add(-1, ":nickname", "A"),
]

form = read_edn("[:find ?e :where [?e :name _]]")
text = write_edn([":find", "?e", ":where", ["?e", ":name", "_"]])
```

## Lazy Entity Example

`conn.entity()` returns a lazy entity wrapper. Use `get()` or index syntax for
individual attributes, and call `touch()` only when you want a fully materialized
dictionary. `entity_map()` is available for the old eager touched-map shape.

```python
entity = conn.entity([":name", "Ada"])

print(entity.id)
print(entity.get(":name"))
print(entity[":db/id"])

touched = entity.touch()
eager = conn.entity_map(1)
```

## Search, Vector, and Idoc Builders

Use helper builders for search/vector/idoc schema and option maps instead of
hand-writing every namespaced key:

```python
from datalevin import (
    embedding_attr,
    embedding_options,
    fulltext_attr,
    idoc_attr,
    search_domain,
    search_options,
    vector_attr,
    vector_options,
)

schema = {
    ":doc/text": fulltext_attr(domains=["docs"], auto_domain=True),
    ":doc/body": embedding_attr(domains=["docs"], auto_domain=True),
    ":doc/vec": vector_attr(domains=["docs"]),
    ":doc/json": idoc_attr(format="json", domain="profiles"),
}

opts = {
    ":search-domains": {"docs": search_domain(index_position=True)},
    ":search-opts": search_options(top=5, display="refs+scores"),
    ":vector-opts": vector_options(dimensions=384, metric_type="cosine"),
    ":embedding-opts": embedding_options(
        provider="openai-compatible",
        model="text-embedding-3-small",
        base_url="https://api.openai.com/v1",
        api_key_env="OPENAI_API_KEY",
        request_dimensions=1536,
        metric_type="cosine",
    ),
}
```

## Standalone Vector Index Example

Use `new_vector_index()` when you want a KV-backed vector index without a
Datalog schema attribute.

```python
from datalevin import new_vector_index, open_kv, vector_options

with open_kv("/tmp/dtlv-py-vec") as kv:
    index = new_vector_index(kv, vector_options(dimensions=2))
    index.add_vec("doc-1", [1.0, 0.0])
    index.add_vec("doc-2", [0.0, 1.0])

    print(index.search_vec([1.0, 0.0], {":top": 1}))
    index.force_checkpoint()
    print(index.info())
    index.close()
```

## Local Llama Example

Use `new_llama_embedder()` and `new_llama_generator()` with local GGUF models
when you want direct llama.cpp handles outside Datalog embedding/search setup.

```python
from datalevin import new_llama_embedder, new_llama_generator

with new_llama_embedder("/models/embed.gguf") as embedder:
    vector = embedder.embed("Datalevin stores facts.")
    print(embedder.dimensions(), len(vector))
    print(embedder.token_count("Datalevin stores facts."))

with new_llama_generator("/models/generate.gguf") as generator:
    print(generator.generate("Write one database tagline:", 32))
```

## Async Transaction Example

`transact()` uses Datalevin's async transaction batching path and waits until
the transaction commits before returning the report.

Use `transact_async()` for ingestion and application-server workloads that
benefit from Datalevin's async transaction batching. It returns a standard
`concurrent.futures.Future`.

```python
future = conn.transact_async([{":db/id": -1, ":name": "Cara"}])
report = future.result(timeout=10)
```

## UDF Example

Use `create_udf_registry()` and `udf_descriptor()` for runtime UDFs. Pass the
registry in connection runtime options, then call the descriptor from query with
Datalevin's `udf` function. Fulltext analyzers use the same route with
`registry.analyzer_udf()` or `registry.query_analyzer_udf()` and descriptors in
`search_domain(analyzer=..., query_analyzer=...)`. Analyzer functions return
`[term, position, offset]` triples.

```python
from datalevin import connect, create_udf_registry, udf_descriptor

registry = create_udf_registry()

@registry.query_udf(":math/inc")
def inc(value):
    return value + 1

descriptor = udf_descriptor(":math/inc")

with connect(
    "/tmp/dtlv-py-udf",
    opts={":runtime-opts": {":udf-registry": registry}},
) as conn:
    value = conn.query(
        "[:find ?v . :in $ ?desc ?n :where [(udf ?desc ?n) ?v]]",
        descriptor,
        41,
    )
```

## Fulltext Analyzer UDF Example

Use analyzer UDFs when a Datalog fulltext domain needs host-language tokenizing.
The document analyzer runs while transactions and re-indexing update the
fulltext index; the query analyzer runs during `fulltext` query evaluation.

```python
import re

from datalevin import (
    connect,
    create_udf_registry,
    schema_attr,
    search_domain,
    udf_descriptor,
)

registry = create_udf_registry()
analyzer = udf_descriptor(":text/hashtags", kind=":analyzer")
query_analyzer = udf_descriptor(":text/plain-query", kind=":query-analyzer")

@registry.analyzer_udf(":text/hashtags")
def hashtags(text):
    return [
        [match.group(0)[1:], pos, match.start()]
        for pos, match in enumerate(re.finditer(r"#\w+", text))
    ]

@registry.query_analyzer_udf(":text/plain-query")
def plain_query(text):
    return [[token, pos, pos] for pos, token in enumerate(text.split())]

with connect(
    "/tmp/dtlv-py-fulltext-udf",
    schema={
        ":text": schema_attr(
            value_type=":db.type/string",
            fulltext=True,
            fulltext_auto_domain=True,
        )
    },
    opts={
        ":runtime-opts": {":udf-registry": registry},
        ":search-domains": {
            "text": search_domain(
                index_position=True,
                analyzer=analyzer,
                query_analyzer=query_analyzer,
            )
        },
    },
) as conn:
    conn.transact([
        {":db/id": 1, ":text": "alpha #needle"},
        {":db/id": 2, ":text": "needle without hash"},
    ])

    assert conn.query(
        "[:find [?e ...] :in $ ?q :where [(fulltext $ :text ?q) [[?e ?a ?v]]]]",
        "needle",
    ) == [1]
```

## Datalog-Backed KV Example

Use `datalog_kv()` when you need ordinary KV tables in the same store as a
Datalog connection. The returned KV handle is borrowed from the connection; do
not close it separately.

```python
from datalevin import datalog_kv

kv = datalog_kv(conn)
kv.open_dbi("app-state")
kv.transact([(":put", "k", "v")], "app-state", ":string", ":string")
```

## Datom Inspection Example

Connection objects expose index-level reads for debugging, teaching, and
migration tooling. Datom reads return dictionaries with `:e`, `:a`, `:v`,
`:tx`, and `:added` keys; `fulltext_datoms()` returns `[e, attr, value]`
triples.

```python
print(conn.datoms(":eav", 1, ":name", limit=10))
print(conn.seek_datoms(":ave", ":name", "Ada", limit=5))
print(conn.rseek_datoms(":ave", ":name", "Bob", limit=5))
print(conn.index_range(":name", "A", "C"))
print(conn.count_datoms(None, ":name", "Ada"))
print(conn.fulltext_datoms("database", opts=search_options(limit=5, offset=10)))
print(conn.datalog_index_cache_limit())
conn.datalog_index_cache_limit(1024)
print(conn.tx_data_to_simulated_report([{":db/id": -1, ":name": "Dry Run"}]))
```

## Bulk Load Example

Use `init_db()` and `fill_db()` when you already have Datom-shaped data and want
the fast bulk-load path. Datoms can be compact tuples in
`(entity_id, attr, value)` shape, and `datom()` creates the same shape.

```python
from datalevin import datom, fill_db, init_db

schema = {":name": {":db/valueType": ":db.type/string"}}

with init_db([(1, ":name", "Ada")], dir="/tmp/dtlv-py-bulk", schema=schema) as conn:
    fill_db(conn, [(2, ":name", "Bob")])
    conn.fill_db([datom(3, ":name", "Cara")])
```

## KV Example

```python
from datalevin import open_kv

with open_kv("/tmp/dtlv-py-kv") as kv:
    kv.open_dbi("items")
    kv.transact(
        [
            (":put", 1, "alpha"),
            (":put", 2, "beta"),
        ],
        dbi_name="items",
        k_type=":long",
        v_type=":string",
    )

    print(
        kv.get_value(
            "items",
            2,
            k_type=":long",
            v_type=":string",
            ignore_key=True,
        )
    )
    print(kv.get_range("items", [":all"], k_type=":long", v_type=":string"))
    print(kv.get_rank("items", 2, k_type=":long"))
    print(kv.get_entry_by_rank("items", 1, k_type=":long", v_type=":string"))
    print(kv.get_first_n("items", 2, [":all"], k_type=":long", v_type=":string"))

    kv.open_list_dbi("tags")
    kv.put_list_items("tags", "doc-1", ["clj", "db"], ":string", ":string")
    print(kv.get_list("tags", "doc-1", ":string", ":string"))
    print(kv.list_range("tags", [":all"], ":string", [":all"], ":string"))
    print(kv.list_range_first("tags", [":all"], ":string", [":all"], ":string"))
    print(kv.list_range_first_n("tags", 2, [":all"], ":string", [":all"], ":string"))
    print(kv.list_range_count("tags", [":all"], ":string"))
    print(kv.key_range_list_count("tags", [":all"], ":string"))
    print(
        kv.list_range_filter(
            "tags",
            lambda key, value: key == "doc-1" and value.startswith("c"),
            [":all"],
            ":string",
            [":all"],
            ":string",
        )
    )
    print(
        kv.list_range_keep(
            "tags",
            lambda key, value: f"{key}:{value}" if value == "db" else None,
            [":all"],
            ":string",
            [":all"],
            ":string",
        )
    )
```

## Operational Example

KV stores expose backup, durability, snapshot, and WAL inspection helpers
without raw JSON calls.

```python
from datalevin import open_kv

with open_kv("/tmp/dtlv-py-ops", opts={":wal?": True}) as kv:
    kv.open_dbi("items")
    kv.transact([(":put", "a", "alpha")], "items", ":string", ":string")

    kv.sync()
    kv.copy("/tmp/dtlv-py-ops-copy")

    print(kv.tx_log_watermarks())
    print(kv.open_tx_log(1, limit=10))
    print(kv.create_snapshot())
    print(kv.list_snapshots())
    print(kv.gc_tx_log_segments())
```

## Remote Client Example

Use `new_client()` for server administration against a running Datalevin server:

```python
from datalevin import new_client

CLIENT_OPTS = {
    ":pool-size": 1,
    ":time-out": 5000,
    ":ha-write-retry-timeout-ms": 5000,
    ":ha-write-retry-delay-ms": 100,
}

client = new_client("dtlv://datalevin:datalevin@localhost", opts=CLIENT_OPTS)
created = False
opened = False

try:
    client.create_database("demo", "datalog")
    created = True
    info = client.open_database(
        "demo",
        "datalog",
        schema={
            ":name": {
                ":db/valueType": ":db.type/string",
                ":db/unique": ":db.unique/identity",
            }
        },
        info=True,
    )
    opened = True

    print(info)
    print(client.list_databases())
    print(client.replica_status("demo"))

    # For consensus HA databases, operator membership changes are available as:
    # client.ha_update_membership("demo", {":ha-members": [...], ...})
finally:
    if opened:
        client.close_database("demo")
    if created:
        client.drop_database("demo")
    client.disconnect()
```

## Embedding Search Options

Python bindings include helper builders for newer store features such as
`:embedding-opts`, `:embedding-domains`, and remote `:openai-compatible`
embedding providers. Raw Datalevin option maps are still passed through when
needed:

```python
from datalevin import connect, embedding_options

with connect(
    "/tmp/dtlv-py-embed",
    schema={
        ":doc/id": {
            ":db/valueType": ":db.type/string",
            ":db/unique": ":db.unique/identity",
        },
        ":doc/text": {
            ":db/valueType": ":db.type/string",
            ":db/embedding": True,
            ":db.embedding/domains": ["docs"],
            ":db.embedding/autoDomain": True,
        },
    },
    opts={
        ":embedding-opts": embedding_options(
            provider="openai-compatible",
            model="text-embedding-3-small",
            base_url="https://api.openai.com/v1",
            api_key_env="OPENAI_API_KEY",
            request_dimensions=1536,
            metric_type="cosine",
        )
    },
) as conn:
    pass
```

## Notes

- Datalevin values come back as ordinary Python values where possible.
- Remote client options such as `:ha-write-retry-timeout-ms` and
  `:ha-write-retry-delay-ms` can be passed to `new_client()`.
- `interop()` is available for advanced raw-handle access when you need it.

## Development

From this repo, the wrapper can run against:

1. `DATALEVIN_JAR=/path/to/datalevin-runtime-<version>.jar`
2. a vendored jar under `src/datalevin/jars/`
3. a repo-local build in `target/`

Typical local flow:

```bash
clojure -T:build vendor-jar
cd bindings/python
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
```

The test suite also executes every case selected by the active release in the
sibling `dtlvtest` golden spec. It discovers `../dtlvtest` automatically; set
`DTLVTEST_ROOT=/path/to/dtlvtest` for a checkout elsewhere. The conformance
adapter reads `spec/manifest.edn`, preserves EDN keyword and symbol types, and
lowers each dataset, transaction, and query through the typed Python builders.
The test is skipped only when no `dtlvtest` checkout is available.

`vendor-jar` builds a platform-specific runtime jar for the current build host
by default. To keep the cross-platform native payloads, pass
`clojure -T:build vendor-jar :native-platform all`.

Wheel builds do this automatically with the all-platform runtime jar and produce
a universal `py3-none-any` wheel. The supported release path is wheel-only:

```bash
python -m pip wheel --no-build-isolation bindings/python -w dist/
```

FreeBSD users should use the platform's own package instead of the PyPI wheel.

`.github/workflows/release.python.yml` builds the universal wheel on demand,
smoke-tests it on Linux amd64, Linux arm64, macOS arm64, and Windows amd64, then
uploads the wheel as an artifact. It does not publish to PyPI or TestPyPI.

For a local manual packaging helper, see
[`script/deploy-python.md`](../../script/deploy-python.md).

The hosted package workflow currently smoke-tests Linux amd64, Linux arm64,
macOS arm64, and Windows amd64.

For ad hoc development against a different build, set `DATALEVIN_JAR` to point
at another embeddable Datalevin runtime jar, preferably
`target/datalevin-runtime-<version>.jar`.
