#!/usr/bin/env bash
#
# Generate TPC-H data with dbgen.
#
# Usage: scripts/generate-data.sh [scale-factor]
#
# The default scale factor is 1 (about 1 GB of flat files, 6M lineitem rows).
# Use 0.01 for a quick smoke run. Data lands in data/tbl/ (git-ignored) and is
# shared by all three systems so every comparison runs on identical tables.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCALE="${1:-1}"
DBGEN_DIR="${DBGEN_DIR:-$HERE/dbgen}"
OUT_DIR="${TPCH_DATA_DIR:-$HERE/data/tbl}"

if [[ ! -x "$DBGEN_DIR/dbgen" ]]; then
  "$HERE/scripts/fetch-dbgen.sh"
fi

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"
echo "Generating TPC-H SF=${SCALE} data into $OUT_DIR"
"$DBGEN_DIR/dbgen" -b "$DBGEN_DIR/dists.dss" -f -s "$SCALE"
echo "Generated:"
ls -1 ./*.tbl
