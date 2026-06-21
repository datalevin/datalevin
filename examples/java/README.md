# Datalevin Java Bindings

Use Datalevin from Java with the `org.datalevin:datalevin-java` artifact.

## Add the Dependency

Maven:

```xml
<dependency>
  <groupId>org.datalevin</groupId>
  <artifactId>datalevin-java</artifactId>
  <version>0.10.18</version>
</dependency>
```

Gradle Kotlin DSL:

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("org.datalevin:datalevin-java:0.10.18")
}
```

The published artifact is a self-contained Datalevin Java runtime from Maven
Central. The current runtime requires Java 21+.

To embed a Datalevin server in the same JVM, add the server add-on artifact as
well:

```xml
<dependency>
  <groupId>org.datalevin</groupId>
  <artifactId>datalevin-java-server</artifactId>
  <version>0.10.18</version>
</dependency>
```

The server artifact depends on `datalevin-java`, so Maven and Gradle will pull
in the base Java API transitively.

## Data Style

Use the typed Java builders as the canonical style for schemas, transactions,
queries, pull selectors, and rules. Use `Datalevin.kw(...)`,
`Datalevin.sym(...)`, `Datalevin.readEdn(...)`, and
`Datalevin.writeEdn(...)` when explicit EDN values are needed. Use
`Datalevin.edn(...)` to mark raw EDN text for APIs that accept an EDN form.

Search, vector, embedding, and idoc maps have typed helpers as well:

```java
Map<Object, Object> schema = Datalevin.schema()
        .attr("doc/text", Schema.attribute()
                .valueType(Schema.ValueType.STRING)
                .fulltext(true)
                .fulltextDomains("docs")
                .fulltextAutoDomain(true))
        .attr("doc/body", Schema.attribute()
                .valueType(Schema.ValueType.STRING)
                .embedding(true)
                .embeddingDomains("docs")
                .embeddingAutoDomain(true))
        .attr("doc/vec", Schema.attribute()
                .valueType(Schema.ValueType.VEC)
                .vectorDomains("docs"))
        .attr("doc/json", Schema.attribute()
                .valueType(Schema.ValueType.IDOC)
                .idocFormat(Schema.IdocFormat.JSON)
                .domain("profiles"))
        .build();

Map<String, Object> opts = Map.of(
        ":search-domains", Map.of("docs",
                Datalevin.searchDomain().indexPosition(true).build()),
        ":search-opts", Datalevin.searchOptions()
                .top(5)
                .display("refs+scores")
                .build(),
        ":vector-opts", Datalevin.vectorOptions(384)
                .metricType("cosine")
                .build(),
        ":embedding-opts", Datalevin.embeddingOptions()
                .provider("default")
                .metricType("cosine")
                .build());
```

## UDF Style

Use `Datalevin.udfRegistry()` and `UdfDescriptor` for runtime UDFs. Pass the
registry in connection runtime options, then call the descriptor from query with
Datalevin's `udf` function.

```java
import datalevin.Connection;
import datalevin.Datalevin;
import datalevin.UdfDescriptor;
import datalevin.UdfRegistry;

import java.util.Map;

UdfRegistry registry = Datalevin.udfRegistry()
        .queryFn("math/inc", args -> ((Number) args.get(0)).longValue() + 1);
UdfDescriptor descriptor = UdfDescriptor.queryFn("math/inc");

try (Connection conn = Datalevin.createConn(
        "/tmp/dtlv-java-udf",
        (Map<?, ?>) null,
        Map.of(":runtime-opts", Map.of(":udf-registry", registry)))) {
    Object value = conn.query(
            "[:find ?v . :in $ ?desc ?n :where [(udf ?desc ?n) ?v]]",
            descriptor,
            41L);
}
```

Fulltext analyzers use the same registry route. Register an analyzer and query
analyzer, then put their descriptors in a Datalog search-domain option map. Each
analyzer returns `[term, position, offset]` triples.

```java
import datalevin.Connection;
import datalevin.Datalevin;
import datalevin.Schema;
import datalevin.UdfDescriptor;
import datalevin.UdfRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

UdfRegistry searchRegistry = Datalevin.udfRegistry();
UdfDescriptor analyzer = Datalevin.analyzerUdf("text/hashtags");
UdfDescriptor queryAnalyzer = Datalevin.queryAnalyzerUdf("text/plain-query");

searchRegistry.analyzer("text/hashtags", args -> {
    String text = (String) args.get(0);
    List<List<Object>> tokens = new ArrayList<>();
    int searchFrom = 0;
    while (searchFrom < text.length()) {
        int start = text.indexOf('#', searchFrom);
        if (start < 0) {
            break;
        }
        int end = start + 1;
        while (end < text.length() && Character.isLetterOrDigit(text.charAt(end))) {
            end++;
        }
        if (end > start + 1) {
            tokens.add(List.of(text.substring(start + 1, end), tokens.size(), start));
        }
        searchFrom = Math.max(end, start + 1);
    }
    return tokens;
});

searchRegistry.queryAnalyzer("text/plain-query", args -> {
    String text = (String) args.get(0);
    List<List<Object>> tokens = new ArrayList<>();
    for (String token : text.split("\\s+")) {
        if (!token.isBlank()) {
            tokens.add(List.of(token, tokens.size(), tokens.size()));
        }
    }
    return tokens;
});

try (Connection conn = Datalevin.createConn(
        "/tmp/dtlv-java-fulltext-udf",
        Datalevin.schema()
                .attr("text", Schema.attribute()
                        .valueType(Schema.ValueType.STRING)
                        .fulltext(true)
                        .fulltextAutoDomain(true)),
        Map.of(
                ":runtime-opts", Map.of(":udf-registry", searchRegistry),
                ":search-domains", Map.of(
                        "text",
                        Datalevin.searchDomain()
                                .indexPosition(true)
                                .prop("analyzer", analyzer)
                                .prop("query-analyzer", queryAnalyzer)
                                .build())))) {
    conn.transact(List.of(
            Map.of(":db/id", 1L, "text", "alpha #needle"),
            Map.of(":db/id", 2L, "text", "needle without hash")));

    Object matchingIds = conn.query(
            "[:find [?e ...] :in $ ?q :where [(fulltext $ :text ?q) [[?e ?a ?v]]]]",
            "needle");
}
```

## Datalog Quick Start

```java
import datalevin.Connection;
import datalevin.DatalogQuery;
import datalevin.Datalevin;
import datalevin.PullSelector;
import datalevin.Schema;
import datalevin.Tx;

import java.util.List;
import java.util.Map;

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
    System.out.println(conn.txDataToSimulatedReport(List.of(
            Map.of(":db/id", -3L, "person/name", "Dry Run", "person/age", 40L))));
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

import java.util.Map;

Map<String, Object> clientOpts = Map.of(
        ":pool-size", 1L,
        ":time-out", 5000L,
        ":ha-write-retry-timeout-ms", 5000L,
        ":ha-write-retry-delay-ms", 100L);

try (Client client = Datalevin.newClient("dtlv://datalevin:datalevin@localhost", clientOpts)) {
    client.createDatabase("demo", DatabaseType.DATALOG);
    try {
        System.out.println(client.openDatabaseInfo("demo", DatabaseType.DATALOG, null, null));
        System.out.println(client.listDatabases());
        System.out.println(client.replicaStatus("demo"));

        // For consensus HA databases, operator membership changes are available as:
        // client.haUpdateMembership("demo", Map.of(":ha-members", List.of(...)));
    } finally {
        client.closeDatabase("demo");
        client.dropDatabase("demo");
    }
}
```

The Java wrapper also passes raw Datalevin option maps through unchanged, so
store options like `:embedding-opts`, `:embedding-domains`, and remote
`:openai-compatible` embedding providers can be supplied directly to
`Datalevin.createConn(dir, schema, opts)`.

## In-Process Server

Use `DatalevinServer` from the `datalevin-java-server` artifact when a Java
process needs to host a Datalevin server directly:

```java
import datalevin.DatalevinServer;

import java.util.Map;

try (DatalevinServer server = DatalevinServer.create(Map.of(
        "host", "127.0.0.1",
        "port", 8898,
        "root", "/tmp/datalevin-server",
        "verbose", true))) {
    server.start();
    // Connect with Datalevin.newClient("dtlv://datalevin:datalevin@localhost").
}
```

## More Examples

This directory also contains four runnable Java entrypoints:

- `DatalogQuickStart.java`: local Datalog connection, schema, transact, query, and pull.
- `KVQuickStart.java`: local KV store, typed DBI operations, list DBIs, and range scans.
- `ClientQuickStart.java`: remote admin client usage against a running Datalevin server.
- `ServerQuickStart.java`: in-process Datalevin server lifecycle using `DatalevinServer`.
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
java -cp "$(clojure -Spath):target/classes:target/example-classes" ServerQuickStart
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
clojure -T:build java-server-javadoc-jar
```

This writes HTML docs to `target/java-release/javadoc/` and a Javadoc jar to
`target/datalevin-java-<version>-javadoc.jar`. Server wrapper docs are written
to `target/java-server-release/javadoc/` and
`target/datalevin-java-server-<version>-javadoc.jar`.
