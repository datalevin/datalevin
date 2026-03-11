# Datalevin Python Bindings

Python bindings for Datalevin over the JVM interop bridge.

This scaffold uses:

- `JPype1` for Python-to-JVM interop
- `datalevin.DatalevinInterop` as the primary runtime substrate
- thin Python wrappers for local Datalog, local KV, and remote admin usage

## Development

From this repo, the wrapper can run against:

1. `DATALEVIN_JAR=/path/to/datalevin-python-runtime-<version>.jar`
2. a vendored jar under `src/datalevin/jars/`
3. a repo-local build in `target/`

Typical local flow:

```bash
clojure -T:build vendor-python-jar
cd bindings/python
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
```

`vendor-python-jar` now builds a platform-specific runtime jar for the current
build host by default. To keep the old cross-platform native payloads, pass
`clojure -T:build vendor-python-jar :native-platform all`.

Wheel builds do this automatically and produce platform-tagged wheels. The
supported release path is wheel-only:

```bash
python -m pip wheel --no-build-isolation bindings/python -w dist/
```

Set `DATALEVIN_NATIVE_PLATFORM` to override the inferred target when building a
wheel from a different platform tag.

GitHub Actions release workflows are split the same way:

- `.github/workflows/release.python.testpypi.yml` publishes a manual dry-run to
  TestPyPI.
- `.github/workflows/release.python.yml` publishes tagged releases to PyPI.

For ad hoc development against a different build, set `DATALEVIN_JAR` to point
at another embeddable Datalevin runtime jar, preferably
`target/datalevin-python-runtime-<version>.jar`.
