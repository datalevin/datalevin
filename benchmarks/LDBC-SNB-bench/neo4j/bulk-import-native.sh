#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: bulk-import-native.sh [options]

Bulk-imports LDBC SNB CSV data into Neo4j using native neo4j-admin.

Options:
  --data-dir PATH     Data dir (default: benchmarks/LDBC-SNB-bench/data)
  --import-dir PATH   Staging dir for neo4j-admin CSVs (default: benchmarks/LDBC-SNB-bench/neo4j/import)
  --neo4j-home PATH   Neo4j home directory (default: auto-detect from brew)
  --db-name NAME      Neo4j database name (default: neo4j)
  --password PASS     Neo4j password (default: neo4jtest)
  --bad-tolerance N   Max bad entries before abort (default: 1000)
  --report-file PATH  Import report path (default: benchmarks/LDBC-SNB-bench/neo4j/import/import.report)
  --wipe              Delete import-dir and database before import
  --start             Start Neo4j after import
  -h, --help          Show this help
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATALEVIN_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

DATA_DIR="${DATA_DIR:-${DATALEVIN_ROOT}/benchmarks/LDBC-SNB-bench/data}"
IMPORT_DIR="${IMPORT_DIR:-${DATALEVIN_ROOT}/benchmarks/LDBC-SNB-bench/neo4j/import}"
NEO4J_HOME="${NEO4J_HOME:-}"
DB_NAME="${DB_NAME:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-neo4jtest}"
BAD_TOLERANCE="${BAD_TOLERANCE:-1000}"
REPORT_FILE="${REPORT_FILE:-}"
WIPE="0"
START_NEO4J="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir)
      DATA_DIR="${2:-}"
      shift 2
      ;;
    --import-dir)
      IMPORT_DIR="${2:-}"
      shift 2
      ;;
    --neo4j-home)
      NEO4J_HOME="${2:-}"
      shift 2
      ;;
    --db-name)
      DB_NAME="${2:-}"
      shift 2
      ;;
    --password)
      NEO4J_PASSWORD="${2:-}"
      shift 2
      ;;
    --bad-tolerance)
      BAD_TOLERANCE="${2:-}"
      shift 2
      ;;
    --report-file)
      REPORT_FILE="${2:-}"
      shift 2
      ;;
    --wipe)
      WIPE="1"
      shift
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

DATA_DIR="${DATA_DIR%/}"
IMPORT_DIR="${IMPORT_DIR%/}"
CSV_ROOT="${DATA_DIR}/graphs/csv/raw/composite-merged-fk"

if [[ -z "$REPORT_FILE" ]]; then
  REPORT_FILE="${IMPORT_DIR}/import.report"
fi

if [[ -z "$BAD_TOLERANCE" || ! "$BAD_TOLERANCE" =~ ^[0-9]+$ ]]; then
  echo "ERROR: --bad-tolerance must be a non-negative integer." >&2
  exit 1
fi

if [[ ! -d "$CSV_ROOT" ]]; then
  echo "ERROR: CSV root not found: $CSV_ROOT" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is not available in PATH." >&2
  exit 1
fi

# Detect NEO4J_HOME
if [[ -z "$NEO4J_HOME" ]]; then
  if command -v brew >/dev/null 2>&1 && brew list neo4j &>/dev/null; then
    NEO4J_HOME="$(brew --prefix neo4j)/libexec"
  elif [[ -d "/usr/local/neo4j" ]]; then
    NEO4J_HOME="/usr/local/neo4j"
  elif [[ -d "$HOME/neo4j" ]]; then
    NEO4J_HOME="$HOME/neo4j"
  else
    echo "ERROR: Could not detect NEO4J_HOME. Please specify with --neo4j-home." >&2
    exit 1
  fi
fi

if [[ ! -d "$NEO4J_HOME" ]]; then
  echo "ERROR: NEO4J_HOME directory does not exist: $NEO4J_HOME" >&2
  exit 1
fi

echo "Using NEO4J_HOME: $NEO4J_HOME"

# Set NEO4J_CONF for neo4j-admin to find configuration
NEO4J_CONF="${NEO4J_HOME}/conf"
export NEO4J_CONF
echo "Using NEO4J_CONF: $NEO4J_CONF"

# Read actual data directory from config
NEO4J_DATA_DIR=""
if [[ -f "$NEO4J_CONF/neo4j.conf" ]]; then
  NEO4J_DATA_DIR=$(grep -E "^server.directories.data=" "$NEO4J_CONF/neo4j.conf" 2>/dev/null | cut -d= -f2 || true)
fi
if [[ -z "$NEO4J_DATA_DIR" ]]; then
  NEO4J_DATA_DIR="$NEO4J_HOME/data"
fi
echo "Using data directory: $NEO4J_DATA_DIR"

# Find neo4j-admin
NEO4J_ADMIN=""
if command -v neo4j-admin >/dev/null 2>&1; then
  NEO4J_ADMIN="neo4j-admin"
elif [[ -x "$NEO4J_HOME/bin/neo4j-admin" ]]; then
  NEO4J_ADMIN="$NEO4J_HOME/bin/neo4j-admin"
else
  echo "ERROR: neo4j-admin not found. Please ensure Neo4j is installed." >&2
  exit 1
fi

echo "Using neo4j-admin: $NEO4J_ADMIN"

# Stop Neo4j if running
if pgrep -f "neo4j" >/dev/null 2>&1; then
  echo "Stopping Neo4j..."
  neo4j stop 2>/dev/null || true
  sleep 2
fi

safe_wipe_dir() {
  local dir="$1"
  if [[ -z "$dir" || "$dir" == "/" || "$dir" == "." ]]; then
    echo "ERROR: Refusing to wipe unsafe dir: $dir" >&2
    exit 1
  fi
}

if [[ "$WIPE" == "1" ]]; then
  safe_wipe_dir "$IMPORT_DIR"
  if [[ -d "$IMPORT_DIR" ]]; then
    echo "Wiping import-dir: $IMPORT_DIR"
    rm -rf "$IMPORT_DIR"
  fi

  # Wipe the database using actual data directory
  DB_DIR="$NEO4J_DATA_DIR/databases/$DB_NAME"
  TX_DIR="$NEO4J_DATA_DIR/transactions/$DB_NAME"
  if [[ -d "$DB_DIR" ]]; then
    echo "Wiping database: $DB_DIR"
    rm -rf "$DB_DIR"
  fi
  if [[ -d "$TX_DIR" ]]; then
    echo "Wiping transactions: $TX_DIR"
    rm -rf "$TX_DIR"
  fi
fi

# Clean and create import dir
safe_wipe_dir "$IMPORT_DIR"
if [[ -d "$IMPORT_DIR" ]]; then
  rm -rf "$IMPORT_DIR"
fi
mkdir -p "$IMPORT_DIR"

echo "Generating neo4j-admin CSVs..."
CSV_ROOT="$CSV_ROOT" IMPORT_DIR="$IMPORT_DIR" python3 "${SCRIPT_DIR}/bulk-import.py"

echo "Running neo4j-admin import..."
"$NEO4J_ADMIN" database import full \
  --overwrite-destination=true \
  --report-file="$REPORT_FILE" \
  --id-type=INTEGER \
  --delimiter="|" \
  --array-delimiter=";" \
  --skip-bad-relationships=true \
  --bad-tolerance="$BAD_TOLERANCE" \
  --nodes=Place="$IMPORT_DIR/Place.nodes.csv" \
  --nodes=Organisation="$IMPORT_DIR/Organisation.nodes.csv" \
  --nodes=TagClass="$IMPORT_DIR/TagClass.nodes.csv" \
  --nodes=Tag="$IMPORT_DIR/Tag.nodes.csv" \
  --nodes=Person="$IMPORT_DIR/Person.nodes.csv" \
  --nodes=Forum="$IMPORT_DIR/Forum.nodes.csv" \
  --nodes=Post="$IMPORT_DIR/Post.nodes.csv" \
  --nodes=Comment="$IMPORT_DIR/Comment.nodes.csv" \
  --relationships=IS_PART_OF="$IMPORT_DIR/IS_PART_OF.rel.csv" \
  --relationships=IS_LOCATED_IN="$IMPORT_DIR/IS_LOCATED_IN.rel.csv" \
  --relationships=HAS_TYPE="$IMPORT_DIR/HAS_TYPE.rel.csv" \
  --relationships=IS_SUBCLASS_OF="$IMPORT_DIR/IS_SUBCLASS_OF.rel.csv" \
  --relationships=IS_LOCATED_IN="$IMPORT_DIR/PERSON_IS_LOCATED_IN.rel.csv" \
  --relationships=HAS_MODERATOR="$IMPORT_DIR/HAS_MODERATOR.rel.csv" \
  --relationships=HAS_CREATOR="$IMPORT_DIR/HAS_CREATOR_POST.rel.csv" \
  --relationships=IS_CONTAINED_IN="$IMPORT_DIR/IS_CONTAINED_IN.rel.csv" \
  --relationships=IS_LOCATED_IN="$IMPORT_DIR/POST_IS_LOCATED_IN.rel.csv" \
  --relationships=HAS_CREATOR="$IMPORT_DIR/HAS_CREATOR_COMMENT.rel.csv" \
  --relationships=IS_LOCATED_IN="$IMPORT_DIR/COMMENT_IS_LOCATED_IN.rel.csv" \
  --relationships=REPLY_OF_POST="$IMPORT_DIR/REPLY_OF_POST.rel.csv" \
  --relationships=REPLY_OF_COMMENT="$IMPORT_DIR/REPLY_OF_COMMENT.rel.csv" \
  --relationships=HAS_INTEREST="$IMPORT_DIR/HAS_INTEREST.rel.csv" \
  --relationships=KNOWS="$IMPORT_DIR/KNOWS.rel.csv" \
  --relationships=STUDY_AT="$IMPORT_DIR/STUDY_AT.rel.csv" \
  --relationships=WORK_AT="$IMPORT_DIR/WORK_AT.rel.csv" \
  --relationships=HAS_TAG="$IMPORT_DIR/FORUM_HAS_TAG.rel.csv" \
  --relationships=HAS_MEMBER="$IMPORT_DIR/HAS_MEMBER.rel.csv" \
  --relationships=HAS_TAG="$IMPORT_DIR/POST_HAS_TAG.rel.csv" \
  --relationships=HAS_TAG="$IMPORT_DIR/COMMENT_HAS_TAG.rel.csv" \
  --relationships=LIKES_POST="$IMPORT_DIR/LIKES_POST.rel.csv" \
  --relationships=LIKES_COMMENT="$IMPORT_DIR/LIKES_COMMENT.rel.csv" \
  -- "$DB_NAME"

if [[ -f "$REPORT_FILE" ]]; then
  echo "Import report written to: $REPORT_FILE"
fi

echo ""
echo "Neo4j import complete!"
echo ""

if [[ "$START_NEO4J" == "1" ]]; then
  echo "Starting Neo4j..."
  neo4j start

  echo "Waiting for Neo4j to become ready..."
  for i in {1..60}; do
    if cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "RETURN 1;" >/dev/null 2>&1; then
      echo "Neo4j is ready!"
      echo ""
      echo "To run benchmark queries:"
      echo "  ./run-queries.sh --password $NEO4J_PASSWORD"
      exit 0
    fi
    sleep 2
  done
  echo "WARNING: Neo4j may not be fully ready yet. Check with 'neo4j status'."
else
  echo "To start Neo4j and run queries:"
  echo "  neo4j start"
  echo "  ./run-queries.sh --password $NEO4J_PASSWORD"
fi
