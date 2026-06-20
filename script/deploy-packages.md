# Package Release

This document covers the release flow for packages outside the existing
Clojars embedded/core deploy script:

- `org.datalevin/datalevin-java` and `org.datalevin/datalevin-java-server` to
  Maven Central
- `datalevin-node` to npm
- `datalevin` to PyPI or TestPyPI

The embedded/core Clojars release remains handled by `script/deploy` or
`script/deploy-embedded`.

## Local Orchestration

Python no longer requires one wheel per platform. The wheel vendors the
all-platform Datalevin runtime jar and is tagged `py3-none-any`, so the package
upload can be done from a local release machine. The GitHub Actions Python
package workflow is still useful for smoke-testing the same wheel on Linux,
macOS, and Windows before publishing.

Dry-run all packages:

```bash
./script/deploy-packages --dry-run
```

Publish all packages:

```bash
./script/deploy-packages
```

Useful variants:

```bash
./script/deploy-packages --java-user-managed
./script/deploy-packages --python-testpypi
./script/deploy-packages --npm-tag next
./script/deploy-packages --skip-java
```

The script forwards credentials and package-specific options to the underlying
scripts:

- `script/deploy-java` uses `SONATYPE_CENTRAL_*` and optional `JAVA_GPG_*`.
- `script/deploy-javascript` uses npm auth, usually `NODE_AUTH_TOKEN` or an
  existing `npm login`.
- `script/deploy-python` uses `TWINE_USERNAME` and `TWINE_PASSWORD`.
