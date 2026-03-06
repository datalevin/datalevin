# Java Quickstart

From the repo root:

```bash
clojure -T:build compile-java
javac -cp "$(clojure -Spath)" -d target/example-classes examples/java/DatalogQuickStart.java
java -cp "$(clojure -Spath):target/classes:target/example-classes" DatalogQuickStart
```

To generate API docs:

```bash
clojure -T:build javadoc
clojure -T:build javadoc-jar
```

This writes HTML docs to `target/javadoc/` and a Javadoc jar to
`target/datalevin-<version>-javadoc.jar`.
