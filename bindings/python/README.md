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

Wheel builds do this automatically and produce platform-tagged wheels. The
supported release path is wheel-only:

```bash
python -m pip wheel --no-build-isolation bindings/python -w dist/
```

Set `DATALEVIN_NATIVE_PLATFORM` to override the inferred target when building a
wheel from a different platform tag. Supported values are
`linux-x86_64`, `linux-arm64`, `macosx-arm64`, `windows-x86_64`,
`freebsd-x86_64`, and `all`.

FreeBSD amd64 wheel builds should run on a FreeBSD host so the wheel keeps the
native `freebsd_*_amd64` platform tag. Those builds vendor the shared runtime
jar (`:native-platform all`) and rely on the FreeBSD-provided native libraries.

GitHub Actions release workflows are split the same way:

- `.github/workflows/release.python.testpypi.yml` publishes a manual dry-run to
  TestPyPI.
- `.github/workflows/release.python.yml` publishes tagged releases to PyPI.

For a local manual release helper, including the FreeBSD wheel path, see
[`script/deploy-python.md`](../../script/deploy-python.md).

The hosted release workflows currently cover Linux, macOS arm64, and Windows.
FreeBSD amd64 wheels are a manual release path.

For ad hoc development against a different build, set `DATALEVIN_JAR` to point
at another embeddable Datalevin runtime jar, preferably
`target/datalevin-runtime-<version>.jar`.
