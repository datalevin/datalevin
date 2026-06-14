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
from datalevin import connect

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
        [
            {":db/id": -1, ":name": "Ada"},
            {":db/id": -2, ":name": "Bob"},
        ]
    )

    names = conn.query("[:find [?name ...] :where [?e :name ?name]]")
    ada = conn.pull([":name"], 1)

    print(names)
    print(ada)
```

Structured query forms and inputs can also be passed as normal Python lists and
dictionaries when that is more convenient than EDN strings.

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
    print(kv.get_first_n("items", 2, [":all"], k_type=":long", v_type=":string"))

    kv.open_list_dbi("tags")
    kv.put_list_items("tags", "doc-1", ["clj", "db"], ":string", ":string")
    print(kv.get_list("tags", "doc-1", ":string", ":string"))
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
finally:
    if opened:
        client.close_database("demo")
    if created:
        client.drop_database("demo")
    client.disconnect()
```

## Embedding Search Options

Python bindings pass Datalevin option maps through unchanged, so newer store
features such as `:embedding-opts`, `:embedding-domains`, and remote
`:openai-compatible` embedding providers are available directly from
`connect()`:

```python
from datalevin import connect

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
        ":embedding-opts": {
            ":provider": ":openai-compatible",
            ":model": "text-embedding-3-small",
            ":base-url": "https://api.openai.com/v1",
            ":api-key-env": "OPENAI_API_KEY",
            ":request-dimensions": 1536,
            ":metric-type": ":cosine",
        }
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
