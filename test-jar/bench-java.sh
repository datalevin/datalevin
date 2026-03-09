#!/bin/bash

set -eou pipefail

source "$(dirname "$0")/common.sh"

ensure_java_release_artifacts

cd "$test_jar_dir"
mkdir -p target/java-classes

classpath=$(java_artifact_classpath)
mapfile -t sources < <(find src-java -name '*.java' | sort)

javac --release 21 -cp "$classpath" -d target/java-classes "${sources[@]}"

java --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -cp "$classpath:target/java-classes" \
     datalevin.JavaApiPerf \
     "$@"
