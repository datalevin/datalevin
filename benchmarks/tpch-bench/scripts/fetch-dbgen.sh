#!/usr/bin/env bash
#
# Fetch and build the TPC-H dbgen/qgen tools used by this benchmark.
#
# The tools are the reference data and query generators published for the
# TPC-H specification. We pin a specific source revision so generated data and
# query substitutions are reproducible. Override DBGEN_DIR to reuse a build.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBGEN_DIR="${DBGEN_DIR:-$HERE/dbgen}"
TPCH_DBGEN_SHA="32f1c1b92d1664dba542e927d23d86ffa57aa253"
SRC_URL="https://github.com/electrum/tpch-dbgen/archive/${TPCH_DBGEN_SHA}.tar.gz"

if [[ -x "$DBGEN_DIR/dbgen" && -x "$DBGEN_DIR/qgen" ]]; then
  echo "dbgen/qgen already built in $DBGEN_DIR"
  exit 0
fi

mkdir -p "$DBGEN_DIR"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "Downloading TPC-H dbgen source ${TPCH_DBGEN_SHA}..."
curl -sSL -o "$tmp/dbgen.tar.gz" "$SRC_URL"
tar xzf "$tmp/dbgen.tar.gz" -C "$tmp"
src="$(find "$tmp" -maxdepth 1 -type d -name 'tpch-dbgen-*' | head -1)"
if [[ -z "$src" ]]; then
  echo "Could not find extracted dbgen source in $tmp" >&2
  exit 1
fi
cp -R "$src"/. "$DBGEN_DIR"/

cd "$DBGEN_DIR"
# DB2 is the most portable dialect: it avoids Oracle's rownum row-count
# artifact, which is invalid in PostgreSQL and SQLite.
make clean >/dev/null 2>&1 || true
make DATABASE=DB2 >/dev/null

echo "Built dbgen/qgen in $DBGEN_DIR"
