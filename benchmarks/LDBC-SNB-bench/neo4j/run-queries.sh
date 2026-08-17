#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$BENCH_DIR"
exec clj -M:neo4j-bench "$@"
