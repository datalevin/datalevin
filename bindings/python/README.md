# Datalevin Python Bindings

Python bindings for Datalevin over the JVM interop bridge.

This scaffold uses:

- `JPype1` for Python-to-JVM interop
- `datalevin.DatalevinInterop` as the primary runtime substrate
- thin Python wrappers for local Datalog, local KV, and remote admin usage

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
`linux-x86_64`, `linux-arm64`, `macosx-arm64`, `windows-x86_64`, `freebsd-x86_64`,
and `all`.

FreeBSD amd64 wheel builds should run on a FreeBSD host so the wheel keeps the
native `freebsd_*_amd64` platform tag. Those builds vendor the shared runtime
jar (`:native-platform all`) and rely on the FreeBSD-provided native libraries.

GitHub Actions release workflows are split the same way:

- `.github/workflows/release.python.testpypi.yml` publishes a manual dry-run to
  TestPyPI.
- `.github/workflows/release.python.yml` publishes tagged releases to PyPI.

The hosted release workflows currently cover Linux, macOS arm64, and Windows.
FreeBSD amd64 wheels are a manual release path.

For ad hoc development against a different build, set `DATALEVIN_JAR` to point
at another embeddable Datalevin runtime jar, preferably
`target/datalevin-runtime-<version>.jar`.
