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
    printf '%s/org/datalevin/datalevin-java/%s/datalevin-java-%s.jar' \
        "$local_repo" "$version" "$version"
}

java_artifact_pom() {
    local version local_repo
    version=$(datalevin_version)
    local_repo=$(datalevin_java_local_repo)
    printf '%s/org/datalevin/datalevin-java/%s/datalevin-java-%s.pom' \
        "$local_repo" "$version" "$version"
}

java_artifact_classpath() {
    local artifact_jar repo_classpath classpath
    artifact_jar=$(java_artifact_jar)
    repo_classpath=$(cd "$repo_root" && clojure -Spath)
    classpath=$(printf '%s' "$repo_classpath" | awk -v RS=: -v ORS=: \
        -v src="$repo_root/src" -v classes="$repo_root/target/classes" \
        '$0 != src && $0 != classes && $0 !~ /\/org\/clojars\/huahaiy\/dtlvnative-/ && length($0) > 0')
    printf '%s:%s\n' "$artifact_jar" "${classpath%:}"
}

ensure_java_release_artifacts() {
    (cd "$repo_root" && clojure -T:build install-java)
}

assert_trimmed_java_artifact() {
    local artifact_jar artifact_pom trimmed_entry_patterns trimmed_dep_patterns
    artifact_jar=$(java_artifact_jar)
    artifact_pom=$(java_artifact_pom)
    trimmed_entry_patterns='^(pod/|datalevin/main\.clj$|datalevin/server\.clj$|datalevin/interpret\.clj$|datalevin/ha(/|\.clj$))'
    trimmed_dep_patterns='babashka\.pods|<groupId>nrepl</groupId>|<artifactId>bencode</artifactId>|<groupId>org\.babashka</groupId>|<artifactId>sci</artifactId>|<artifactId>tools\.cli</artifactId>|<groupId>org\.bouncycastle</groupId>|<artifactId>jraft-core</artifactId>'

    if jar tf "$artifact_jar" | grep -Eq "$trimmed_entry_patterns"; then
        echo "Unexpected trimmed runtime entries found in $artifact_jar" >&2
        return 1
    fi

    if grep -Eq "$trimmed_dep_patterns" "$artifact_pom"; then
        echo "Unexpected trimmed runtime dependencies found in $artifact_pom" >&2
        return 1
    fi
}
