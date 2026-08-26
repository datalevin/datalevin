#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEO4J_HOME="${NEO4J_HOME:-$(brew --prefix neo4j)/libexec}"
NEO4J_DATA_DIR="${NEO4J_DATA_DIR:-${SCRIPT_DIR}/data}"
NEO4J_IMPORT_DIR="${NEO4J_IMPORT_DIR:-${SCRIPT_DIR}/import}"
NEO4J_RUNTIME_DIR="${NEO4J_RUNTIME_DIR:-${SCRIPT_DIR}/runtime}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-neo4jtest}"
NEO4J_HEAP="${NEO4J_HEAP:-8g}"
NEO4J_PAGECACHE="${NEO4J_PAGECACHE:-4g}"
NEO4J_CYPHER_LANGUAGE="${NEO4J_CYPHER_LANGUAGE:-CYPHER_25}"

CONF_DIR="${NEO4J_RUNTIME_DIR}/conf"
LOGS_DIR="${NEO4J_RUNTIME_DIR}/logs"
RUN_DIR="${NEO4J_RUNTIME_DIR}/run"

usage() {
  cat <<'EOF'
Usage: server.sh prepare|set-password|start|stop|restart|status|console

Environment overrides:
  NEO4J_HOME              Neo4j installation home
  NEO4J_DATA_DIR          Benchmark data directory
  NEO4J_IMPORT_DIR        CSV import directory
  NEO4J_RUNTIME_DIR       Generated configuration/log/run directory
  NEO4J_PASSWORD          Password used by set-password (default neo4jtest)
  NEO4J_HEAP              Initial and maximum heap (default 8g)
  NEO4J_PAGECACHE         Page cache (default 4g)
  NEO4J_CYPHER_LANGUAGE   CYPHER_25 or CYPHER_5 (default CYPHER_25)
EOF
}

absolute_dir() {
  local path="$1"
  mkdir -p "$path"
  (cd "$path" && pwd -P)
}

prepare() {
  if [[ ! -x "${NEO4J_HOME}/bin/neo4j" ]]; then
    echo "ERROR: Neo4j executable not found under ${NEO4J_HOME}" >&2
    exit 1
  fi

  NEO4J_DATA_DIR="$(absolute_dir "$NEO4J_DATA_DIR")"
  NEO4J_IMPORT_DIR="$(absolute_dir "$NEO4J_IMPORT_DIR")"
  NEO4J_RUNTIME_DIR="$(absolute_dir "$NEO4J_RUNTIME_DIR")"
  CONF_DIR="${NEO4J_RUNTIME_DIR}/conf"
  LOGS_DIR="$(absolute_dir "${NEO4J_RUNTIME_DIR}/logs")"
  RUN_DIR="$(absolute_dir "${NEO4J_RUNTIME_DIR}/run")"
  mkdir -p "$CONF_DIR"

  for name in neo4j-admin.conf server-logs.xml user-logs.xml; do
    cp "${NEO4J_HOME}/conf/${name}" "${CONF_DIR}/${name}"
  done

  printf '%s\n' \
    "server.directories.data=${NEO4J_DATA_DIR}" \
    "server.directories.import=${NEO4J_IMPORT_DIR}" \
    "server.directories.logs=${LOGS_DIR}" \
    "server.directories.run=${RUN_DIR}" \
    "server.memory.heap.initial_size=${NEO4J_HEAP}" \
    "server.memory.heap.max_size=${NEO4J_HEAP}" \
    "server.memory.pagecache.size=${NEO4J_PAGECACHE}" \
    "server.jvm.additional=-XX:+ExitOnOutOfMemoryError" \
    "server.jvm.additional=--add-opens=java.base/java.nio=ALL-UNNAMED" \
    "server.jvm.additional=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
    "server.jvm.additional=--enable-native-access=ALL-UNNAMED" \
    "db.query.default_language=${NEO4J_CYPHER_LANGUAGE}" \
    "dbms.security.auth_enabled=true" \
    "dbms.usage_report.enabled=false" \
    "server.default_listen_address=127.0.0.1" \
    "server.bolt.enabled=true" \
    "server.bolt.listen_address=127.0.0.1:7687" \
    "server.http.enabled=false" \
    "server.https.enabled=false" \
    > "${CONF_DIR}/neo4j.conf"

  echo "Neo4j $(${NEO4J_HOME}/bin/neo4j --version)"
  echo "Configuration: ${CONF_DIR}/neo4j.conf"
  echo "Data: ${NEO4J_DATA_DIR}"
  echo "Heap: ${NEO4J_HEAP}; page cache: ${NEO4J_PAGECACHE}; language: ${NEO4J_CYPHER_LANGUAGE}"
}

run_neo4j() {
  prepare >/dev/null
  NEO4J_CONF="$CONF_DIR" "${NEO4J_HOME}/bin/neo4j" "$@"
}

command="${1:-}"
case "$command" in
  prepare)
    prepare
    ;;
  set-password)
    prepare >/dev/null
    NEO4J_CONF="$CONF_DIR" "${NEO4J_HOME}/bin/neo4j-admin" \
      dbms set-initial-password "$NEO4J_PASSWORD"
    ;;
  start)
    run_neo4j start
    ;;
  stop)
    run_neo4j stop
    ;;
  restart)
    run_neo4j restart
    ;;
  status)
    run_neo4j status
    ;;
  console)
    run_neo4j console
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
