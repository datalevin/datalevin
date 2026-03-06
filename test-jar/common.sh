#!/bin/bash

set -euo pipefail

test_jar_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$test_jar_dir/.." && pwd)
java_release_dir="$repo_root/target/java-release"

datalevin_version() {
    sed -n 's/^(def version "\(.*\)")/\1/p' "$repo_root/project.clj" | head -n1
}

datalevin_java_local_repo() {
    mkdir -p "$java_release_dir/m2"
    cd "$java_release_dir/m2" && pwd
}

java_artifact_jar() {
    local version local_repo
    version=$(datalevin_version)
    local_repo=$(datalevin_java_local_repo)
    printf '%s/datalevin/datalevin-java/%s/datalevin-java-%s.jar' \
        "$local_repo" "$version" "$version"
}

java_artifact_classpath() {
    local artifact_jar repo_classpath classpath
    artifact_jar=$(java_artifact_jar)
    repo_classpath=$(cd "$repo_root" && clojure -Spath)
    classpath=$(printf '%s' "$repo_classpath" | awk -v RS=: -v ORS=: \
        -v src="$repo_root/src" -v classes="$repo_root/target/classes" \
        '$0 != src && $0 != classes && length($0) > 0')
    printf '%s:%s\n' "$artifact_jar" "${classpath%:}"
}

ensure_java_release_artifacts() {
    (cd "$repo_root" && clojure -T:build install-java)
}
