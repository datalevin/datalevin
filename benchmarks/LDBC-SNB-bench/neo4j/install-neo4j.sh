#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: install-neo4j.sh [options]

Installs Neo4j when absent, verifies the embedded benchmark version, and
prepares the isolated import configuration (macOS via Homebrew).

Options:
  --password PASS    Initial Neo4j password (default: neo4jtest)
  --heap SIZE        Benchmark heap size (default: 8g)
  --pagecache SIZE   Benchmark page cache size (default: 4g)
  --start            Start the isolated benchmark server
  -h, --help         Show this help
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPECTED_NEO4J_VERSION="$(tr -d '[:space:]' < "${SCRIPT_DIR}/version.txt")"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-neo4jtest}"
HEAP_SIZE="8g"
PAGECACHE_SIZE="4g"
START_NEO4J="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --password)
      NEO4J_PASSWORD="${2:-}"
      shift 2
      ;;
    --heap)
      HEAP_SIZE="${2:-}"
      shift 2
      ;;
    --pagecache)
      PAGECACHE_SIZE="${2:-}"
      shift 2
      ;;
    --start)
      START_NEO4J="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "$(uname)" != "Darwin" ]]; then
  echo "This script is for macOS. For other platforms, install Neo4j manually."
  echo "See: https://neo4j.com/docs/operations-manual/current/installation/"
  exit 1
fi

if ! command -v brew >/dev/null 2>&1; then
  echo "ERROR: Homebrew is required. Install from https://brew.sh"
  exit 1
fi

echo "Installing Neo4j via Homebrew..."
if brew list neo4j &>/dev/null; then
  echo "Neo4j is already installed."
else
  brew install neo4j
fi

INSTALLED_NEO4J_VERSION="$(neo4j --version)"
echo "Installed Neo4j version: $INSTALLED_NEO4J_VERSION"
if [[ "$INSTALLED_NEO4J_VERSION" != "$EXPECTED_NEO4J_VERSION" ]]; then
  echo "ERROR: The embedded benchmark requires Neo4j $EXPECTED_NEO4J_VERSION." >&2
  echo "Install that version, or update deps.edn and neo4j/version.txt together." >&2
  exit 1
fi

NEO4J_HOME="$(brew --prefix neo4j)/libexec"

echo "Preparing the isolated benchmark configuration..."
echo "  NEO4J_HOME: $NEO4J_HOME"
echo "  Heap: $HEAP_SIZE, Page cache: $PAGECACHE_SIZE"
NEO4J_HOME="$NEO4J_HOME" NEO4J_PASSWORD="$NEO4J_PASSWORD" \
  NEO4J_HEAP="$HEAP_SIZE" NEO4J_PAGECACHE="$PAGECACHE_SIZE" \
  "${SCRIPT_DIR}/server.sh" prepare

# Set initial password
echo "Setting initial password..."
NEO4J_HOME="$NEO4J_HOME" NEO4J_PASSWORD="$NEO4J_PASSWORD" \
  NEO4J_HEAP="$HEAP_SIZE" NEO4J_PAGECACHE="$PAGECACHE_SIZE" \
  "${SCRIPT_DIR}/server.sh" set-password 2>/dev/null || true

echo ""
echo "Neo4j installation complete!"
echo ""

if [[ "$START_NEO4J" == "1" ]]; then
  echo "Starting Neo4j..."
  NEO4J_HOME="$NEO4J_HOME" NEO4J_HEAP="$HEAP_SIZE" \
    NEO4J_PAGECACHE="$PAGECACHE_SIZE" "${SCRIPT_DIR}/server.sh" start
  echo ""
  echo "Waiting for Neo4j to become ready..."
  for i in {1..30}; do
    if "${NEO4J_HOME}/bin/cypher-shell" -a bolt://localhost:7687 \
      -u neo4j -p "$NEO4J_PASSWORD" "RETURN 1;" >/dev/null 2>&1; then
      echo "Neo4j is ready!"
      exit 0
    fi
    sleep 2
  done
  echo "Neo4j may still be starting. Check with 'neo4j status'."
else
  echo "To start the benchmark server:"
  echo "  ./server.sh start"
  echo ""
  echo "To import data:"
  echo "  ./bulk-import-native.sh --start"
fi
