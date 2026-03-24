# Datalevin Java Bindings

Use Datalevin from Java with the `org.datalevin:datalevin-java` artifact.

## Add the Dependency

Maven:

```xml
<dependency>
  <groupId>org.datalevin</groupId>
  <artifactId>datalevin-java</artifactId>
  <version>0.10.12</version>
</dependency>
```

Gradle Kotlin DSL:

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("org.datalevin:datalevin-java:0.10.12")
}
```

The published artifact is a self-contained Datalevin Java runtime from Maven
Central. The current runtime requires Java 21+.

## Datalog Quick Start

```java
import datalevin.Connection;
import datalevin.DatalogQuery;
import datalevin.Datalevin;
import datalevin.PullSelector;
import datalevin.Schema;
import datalevin.Tx;

try (Connection conn = Datalevin.createConn(
        "/tmp/dtlv-java",
        Datalevin.schema()
                .attr("person/name",
                        Schema.attribute()
                                .valueType(Schema.ValueType.STRING)
                                .unique(Schema.Unique.IDENTITY))
                .attr("person/age",
                        Schema.attribute()
                                .valueType(Schema.ValueType.LONG)))) {

    conn.transact(Datalevin.tx()
            .entity(Tx.entity(-1).put("person/name", "Alice").put("person/age", 30))
            .entity(Tx.entity(-2).put("person/name", "Bob").put("person/age", 25)));

    DatalogQuery adultsQuery = Datalevin.query()
            .findAll("?name")
            .whereDatom(Datalevin.var("e"), "person/name", Datalevin.var("name"))
            .whereDatom(Datalevin.var("e"), "person/age", Datalevin.var("age"))
            .wherePredicate(">=", Datalevin.var("age"), 30);

    PullSelector selector = Datalevin.pull()
            .attr("person/name")
            .attr("person/age");

    System.out.println(conn.queryCollection(adultsQuery, String.class));
    System.out.println(conn.pull(selector, Datalevin.listOf(Datalevin.kw("person/name"), "Alice")));
}
```

## KV Quick Start

```java
import datalevin.Datalevin;
import datalevin.KV;
import datalevin.KVType;

import java.util.List;

try (KV kv = Datalevin.openKV("/tmp/dtlv-java-kv")) {
    kv.openDbi("people");

    kv.transact("people",
                List.of(
                        List.of(":put", 1001L, "Alice"),
                        List.of(":put", 1002L, "Bob")),
                KVType.LONG,
                KVType.STRING);

    System.out.println(kv.getValue("people", 1002L, KVType.LONG, KVType.STRING, true));
    System.out.println(kv.getRange("people", Datalevin.allRange(), KVType.LONG, KVType.STRING, null, null));
}
```

## Remote Client Quick Start

Use `Datalevin.newClient()` against a running Datalevin server:

```java
import datalevin.Client;
import datalevin.DatabaseType;
import datalevin.Datalevin;

try (Client client = Datalevin.newClient("dtlv://datalevin:datalevin@localhost")) {
    client.createDatabase("demo", DatabaseType.DATALOG);
    try {
        System.out.println(client.openDatabaseInfo("demo", DatabaseType.DATALOG, null, null));
        System.out.println(client.listDatabases());
    } finally {
        client.closeDatabase("demo");
        client.dropDatabase("demo");
    }
}
```

## More Examples

This directory also contains four runnable Java entrypoints:

- `DatalogQuickStart.java`: local Datalog connection, schema, transact, query, and pull.
- `KVQuickStart.java`: local KV store, typed DBI operations, list DBIs, and range scans.
- `ClientQuickStart.java`: remote admin client usage against a running Datalevin server.
- `InteropQuickStart.java`: raw-handle bridge usage with `DatalevinInterop`.

## Run From The Repo

From this repo you can build and install the Java artifact into the local
release repository under `target/java-release/m2`:

```bash
clojure -T:build install-java
```

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

For the Maven Central release procedure for this artifact, including the
`script/deploy-java` helper, see [`script/deploy-java.md`](../../script/deploy-java.md).

## API docs

To generate Javadoc:

```bash
clojure -T:build javadoc
clojure -T:build javadoc-jar
```

This writes HTML docs to `target/java-release/javadoc/` and a Javadoc jar to
`target/datalevin-java-<version>-javadoc.jar`.
