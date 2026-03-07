#!/bin/bash

set -euo pipefail

base_dir="${1:?clock skew state dir required}"
node_id="${DTLV_HA_NODE_ID:-}"
state_file="${base_dir}/clock-skew-${node_id}.txt"

if [[ -n "${node_id}" && -f "${state_file}" ]]; then
  cat "${state_file}"
else
  printf 0
fi
