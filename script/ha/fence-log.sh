#!/bin/bash

set -euo pipefail

log_path="$1"
exit_code="${2:-0}"

mkdir -p "$(dirname "$log_path")"

printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
  "$(( $(date +%s) * 1000 ))" \
  "${DTLV_DB_NAME:-}" \
  "${DTLV_FENCE_OP_ID:-}" \
  "${DTLV_TERM_OBSERVED:-}" \
  "${DTLV_TERM_CANDIDATE:-}" \
  "${DTLV_NEW_LEADER_NODE_ID:-}" \
  "${DTLV_OLD_LEADER_NODE_ID:-}" \
  "${DTLV_OLD_LEADER_ENDPOINT:-}" \
  >> "$log_path"

exit "$exit_code"
