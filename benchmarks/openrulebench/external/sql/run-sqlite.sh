#!/bin/bash
# SQLite benchmark runner for OpenRuleBench
# Usage: ./run-sqlite.sh [benchmark names...]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../../data/generated"
DB_FILE="/tmp/openrulebench_sqlite.db"

# Check if sqlite3 is available
if ! command -v sqlite3 &> /dev/null; then
    echo "sqlite3 not found."
    echo "---	" | tr -d '\n'
    exit 0
fi

run_tc_benchmark() {
    local data_type=$1
    local data_size=$2
    local query_type=$3

    local csv_file="$DATA_DIR/tc-${data_type}-${data_size}.csv"

    if [ ! -f "$csv_file" ]; then
        echo "---	" | tr -d '\n'
        return
    fi

    # Create fresh database and load data
    rm -f "$DB_FILE"
    sqlite3 "$DB_FILE" <<EOF
.mode csv
.headers on
CREATE TABLE edge (from_node INTEGER, to_node INTEGER);
.import $csv_file edge
DELETE FROM edge WHERE from_node = 'from';
CREATE INDEX idx_edge_from ON edge(from_node);
CREATE INDEX idx_edge_to ON edge(to_node);
EOF

    # Run TC query and time it
    start_time=$(python3 -c "import time; print(time.time() * 1000)")

    case "$query_type" in
        ff)
            sqlite3 "$DB_FILE" <<EOF
WITH RECURSIVE tc AS (
    SELECT from_node AS a, to_node AS b FROM edge
    UNION
    SELECT e.from_node, t.b FROM edge e JOIN tc t ON e.to_node = t.a
)
SELECT COUNT(*) FROM tc;
EOF
            ;;
        bf)
            sqlite3 "$DB_FILE" <<EOF
WITH RECURSIVE tc AS (
    SELECT to_node AS b FROM edge WHERE from_node = 0
    UNION
    SELECT e.to_node FROM edge e JOIN tc t ON e.from_node = t.b
)
SELECT COUNT(*) FROM tc;
EOF
            ;;
    esac

    end_time=$(python3 -c "import time; print(time.time() * 1000)")
    elapsed=$(python3 -c "print(f'{$end_time - $start_time:.1f}')")
    echo "${elapsed}	" | tr -d '\n'
}

run_sg_benchmark() {
    local data_size=$1
    local query_type=$2

    local par_file="$DATA_DIR/sg-${data_size}-par.csv"
    local sib_file="$DATA_DIR/sg-${data_size}-sib.csv"

    if [ ! -f "$par_file" ] || [ ! -f "$sib_file" ]; then
        echo "---	" | tr -d '\n'
        return
    fi

    rm -f "$DB_FILE"
    sqlite3 "$DB_FILE" <<EOF
.mode csv
.headers on
CREATE TABLE par (a INTEGER, b INTEGER);
CREATE TABLE sib (a INTEGER, b INTEGER);
.import $par_file par
.import $sib_file sib
DELETE FROM par WHERE a = 'from';
DELETE FROM sib WHERE a = 'from';
CREATE INDEX idx_par_a ON par(a);
CREATE INDEX idx_par_b ON par(b);
CREATE INDEX idx_sib_a ON sib(a);
CREATE INDEX idx_sib_b ON sib(b);
EOF

    start_time=$(python3 -c "import time; print(time.time() * 1000)")

    case "$query_type" in
        ff)
            sqlite3 "$DB_FILE" <<EOF
WITH RECURSIVE sg AS (
    SELECT a AS x, b AS y FROM sib
    UNION
    SELECT p1.a, p2.a
    FROM par p1, sg s, par p2
    WHERE p1.b = s.x AND p2.b = s.y
)
SELECT COUNT(*) FROM sg;
EOF
            ;;
        bf)
            sqlite3 "$DB_FILE" <<EOF
WITH RECURSIVE sg AS (
    SELECT a AS x, b AS y FROM sib
    UNION
    SELECT p1.a, p2.a
    FROM par p1, sg s, par p2
    WHERE p1.b = s.x AND p2.b = s.y
)
SELECT COUNT(*) FROM sg WHERE x = 0;
EOF
            ;;
    esac

    end_time=$(python3 -c "import time; print(time.time() * 1000)")
    elapsed=$(python3 -c "print(f'{$end_time - $start_time:.1f}')")
    echo "${elapsed}	" | tr -d '\n'
}

# Process each benchmark argument
for bench in "$@"; do
    case "$bench" in
        tc-cyclic-small-ff)  run_tc_benchmark cyclic small ff ;;
        tc-cyclic-small-bf)  run_tc_benchmark cyclic small bf ;;
        tc-cyclic-large-ff)  run_tc_benchmark cyclic large ff ;;
        tc-cyclic-large-bf)  run_tc_benchmark cyclic large bf ;;
        tc-acyclic-small-ff) run_tc_benchmark acyclic small ff ;;
        tc-acyclic-small-bf) run_tc_benchmark acyclic small bf ;;
        tc-acyclic-large-ff) run_tc_benchmark acyclic large ff ;;
        tc-acyclic-large-bf) run_tc_benchmark acyclic large bf ;;
        sg-small-ff)         run_sg_benchmark small ff ;;
        sg-small-bf)         run_sg_benchmark small bf ;;
        sg-large-ff)         run_sg_benchmark large ff ;;
        sg-large-bf)         run_sg_benchmark large bf ;;
        *)                   echo "---	" | tr -d '\n' ;;
    esac
done

echo ""
rm -f "$DB_FILE"
