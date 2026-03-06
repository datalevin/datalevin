#!/bin/bash

set -eou pipefail

source "$(dirname "$0")/common.sh"

jvm_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1 )
echo "Java version $jvm_version"

ensure_java_release_artifacts

cd "$test_jar_dir"
mkdir -p target/java-classes

classpath=$(java_artifact_classpath)

javac --release 21 -cp "$classpath" -d target/java-classes src-java/test_jar/JavaSmoke.java

java --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -cp "$classpath:target/java-classes" \
     test_jar.JavaSmoke
