#!/bin/bash

set -eou pipefail

jvm_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1 )

echo $jvm_version

cd "$(dirname "$0")"

uberjar=../target/datalevin-0.10.8-standalone.jar

if jar tf "$uberjar" | grep -Eq '^(META-INF/maven/|META-INF/leiningen/.*/project\.clj$|META-INF/leiningen/.*/README(\.[^/]+)?$|com/caucho/hessian/test/|org/bouncycastle/util/test/)'; then
    echo "Unexpected third-party metadata or test payload found in $uberjar" >&2
    exit 1
fi

java --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -jar "$uberjar" exec << EOF
(def conn (get-conn "/tmp/test-db"))
(transact! conn [{:name "world"}])
(q '[:find ?g :where [_ :name ?g]] @conn)
(close conn)
EOF

echo "Uberjar test succeeded!"
