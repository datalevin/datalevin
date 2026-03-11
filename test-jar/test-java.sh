#!/bin/bash

set -eou pipefail

source "$(dirname "$0")/common.sh"

jvm_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1 )
echo "Java version $jvm_version"

ensure_java_release_artifacts
assert_trimmed_java_artifact

cd "$test_jar_dir"
mkdir -p target/java-classes

classpath=$(java_artifact_classpath)
client_quickstart_classpath="$classpath:$test_jar_dir/target/java-classes"

javac --release 21 -cp "$classpath" -d target/java-classes \
    src-java/test_jar/JavaSmoke.java \
    "$repo_root/examples/java/ClientQuickStart.java"

java --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -cp "$classpath:target/java-classes" \
     test_jar.JavaSmoke

(
    cd "$repo_root"
    CLIENT_QUICKSTART_CLASSPATH="$client_quickstart_classpath" \
        clojure -M:test -m datalevin.client-quickstart-check
)
