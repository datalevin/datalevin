# Java Release

This document covers the manual release flow for the `org.datalevin/datalevin-java`
artifact.

## Prerequisites

- A verified `org.datalevin` namespace in Sonatype Central.
- A Central Portal user token.
- A GPG key configured for the Java release, unless you are only doing a local
  dry run.
- JDK 21 and the normal Datalevin build toolchain.

The Java release bundle includes:

- `datalevin-java-<version>.jar`
- `datalevin-java-<version>.pom`
- `datalevin-java-<version>-sources.jar`
- `datalevin-java-<version>-javadoc.jar`

These are assembled by [`build.clj`](../build.clj) via the
`central-java-bundle` task.

## Credentials

The deploy script accepts either a pre-encoded bearer token or a token username
and password pair:

```bash
export SONATYPE_CENTRAL_USERNAME=...
export SONATYPE_CENTRAL_PASSWORD=...
```

Or:

```bash
export SONATYPE_CENTRAL_BEARER_TOKEN=...
```

If you use `SONATYPE_CENTRAL_USERNAME` and `SONATYPE_CENTRAL_PASSWORD`, the
script base64-encodes `username:password` and sends it as `Authorization:
Bearer <token>`, which matches Sonatype Central's Publisher API.

For signing:

```bash
export JAVA_GPG_HOME=/path/to/gnupg-home
export JAVA_GPG_KEY_ID=YOUR_KEY_ID
```

## Dry Run

Build the Maven Central bundle without uploading it:

```bash
./script/deploy-java --dry-run
```

For local validation without signatures:

```bash
./script/deploy-java --dry-run --no-sign
```

## Publish

The default mode uploads with `publishingType=AUTOMATIC` and waits until the
deployment reaches a terminal state:

```bash
./script/deploy-java
```

To upload for manual review in the Central Portal instead:

```bash
./script/deploy-java --user-managed
```

Useful environment variables:

- `SONATYPE_CENTRAL_URL`
  Defaults to `https://central.sonatype.com`
- `SONATYPE_CENTRAL_POLL_INTERVAL`
  Defaults to `10`
- `SONATYPE_CENTRAL_MAX_POLLS`
  Defaults to `90`

## Browse The Javadoc

Once the artifact has been published to Maven Central and synced, the Javadoc
should become browseable on `javadoc.io` at:

```text
https://javadoc.io/doc/org.datalevin/datalevin-java
```
