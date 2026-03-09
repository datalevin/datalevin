# Java Examples

This directory contains four small Java entrypoints:

- `DatalogQuickStart.java`: local Datalog connection, schema, transact, query, and pull.
- `KVQuickStart.java`: local KV store, typed DBI operations, list DBIs, and range scans.
- `ClientQuickStart.java`: remote admin client usage against a running Datalevin server.
- `InteropQuickStart.java`: raw-handle bridge usage with `DatalevinInterop`.

## Dependency coordinates

The Java artifact built by this repo is `org.datalevin:datalevin-java:0.10.7`.
It is published to Maven Central as a self-contained Datalevin Java runtime, so
Java consumers only need Maven Central enabled.

Maven:

```xml
<dependency>
  <groupId>org.datalevin</groupId>
  <artifactId>datalevin-java</artifactId>
  <version>0.10.7</version>
</dependency>
```

Gradle Kotlin DSL:

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("org.datalevin:datalevin-java:0.10.7")
}
```

From this repo you can also build and install the Java artifact into the local
release repository under `target/java-release/m2`:

```bash
clojure -T:build install-java
```

For the Maven Central release procedure for this artifact, see
[`doc/java-release.md`](../../doc/java-release.md).

## Compile and run from the repo

From the repo root:

```bash
clojure -T:build compile-java
mkdir -p target/example-classes
javac --release 21 -cp "$(clojure -Spath):target/classes" -d target/example-classes examples/java/*.java
java -cp "$(clojure -Spath):target/classes:target/example-classes" DatalogQuickStart
java -cp "$(clojure -Spath):target/classes:target/example-classes" KVQuickStart
java -cp "$(clojure -Spath):target/classes:target/example-classes" InteropQuickStart
```

`ClientQuickStart` needs a running Datalevin server. By default it connects to
`dtlv://datalevin:datalevin@localhost`. Override that with `DATALEVIN_URI`:

```bash
DATALEVIN_URI=dtlv://datalevin:datalevin@localhost \
  java -cp "$(clojure -Spath):target/classes:target/example-classes" ClientQuickStart
```

## Notes

- The Java API returns raw Clojure runtime classes where that is the natural
  Datalevin value, including `clojure.lang.Keyword` and persistent collections.
- `Datalevin` is the high-level entrypoint for Java users.
- `DatalevinInterop` is the smaller raw-handle surface intended for bridge
  consumers such as JPype or node-java-bridge.

## API docs

To generate Javadoc:

```bash
clojure -T:build javadoc
clojure -T:build javadoc-jar
```

This writes HTML docs to `target/java-release/javadoc/` and a Javadoc jar to
`target/datalevin-java-<version>-javadoc.jar`.
