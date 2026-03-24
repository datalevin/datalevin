# JavaScript Release

This document covers the manual release flow for the `datalevin-node` npm
package.

## Prerequisites

- Node.js 20+
- npm
- Clojure CLI
- npm publish access for `datalevin-node`

Authenticate with `npm login` or set:

```bash
export NODE_AUTH_TOKEN=...
```

## Dry Run

Build the shared runtime jar, package the npm tarball, and stop before publish:

```bash
./script/deploy-javascript --dry-run
```

If you only want to validate packaging and skip dependency install or tests:

```bash
./script/deploy-javascript --dry-run --skip-install --skip-tests
```

## Publish

Publish to npm:

```bash
./script/deploy-javascript
```

Publish under a specific dist-tag:

```bash
./script/deploy-javascript --tag next
```

If your npm account uses two-factor auth for publish, pass the OTP explicitly:

```bash
./script/deploy-javascript --otp 123456
```

`--provenance` is available, but npm only supports provenance generation from
supported cloud CI/CD providers such as GitHub Actions and GitLab CI/CD.
