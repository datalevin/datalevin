# Python Release

This document covers the manual release flow for the `datalevin` Python wheel.

## Scope

This script builds and uploads the current-host wheel. It is useful for manual
releases, especially the FreeBSD wheel, and for local dry runs. The normal
multi-platform Linux/macOS/Windows release still lives in the GitHub Actions
workflows under `.github/workflows/release.python*.yml`.

## Prerequisites

- Python 3.10+
- Clojure CLI
- A Python environment with `pip`
- A PyPI token or TestPyPI token

Typical credentials:

```bash
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=...
```

If you need to override the wheel target tag, set:

```bash
export DATALEVIN_NATIVE_PLATFORM=linux-x86_64
```

Supported values are `linux-x86_64`, `linux-arm64`, `macosx-arm64`,
`windows-x86_64`, `freebsd-x86_64`, and `all`.

## Dry Run

Build the current-host wheel and run the smoke test:

```bash
./script/deploy-python --dry-run
```

Skip the smoke test if you only want the wheel build:

```bash
./script/deploy-python --dry-run --skip-smoke
```

## Publish

Upload to PyPI:

```bash
./script/deploy-python
```

Upload to TestPyPI:

```bash
./script/deploy-python --testpypi
```
