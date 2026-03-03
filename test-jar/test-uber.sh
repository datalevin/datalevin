#!/bin/bash

set -eou pipefail

jvm_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1 )

echo $jvm_version

cd "$(dirname "$0")"

java --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -jar ../target/datalevin-0.10.7-standalone.jar exec << EOF
(def conn (get-conn "/tmp/test-db"))
(transact! conn [{:name "world"}])
(q '[:find ?g :where [_ :name ?g]] @conn)
(close conn)
EOF

echo "Uberjar test succeeded!"
