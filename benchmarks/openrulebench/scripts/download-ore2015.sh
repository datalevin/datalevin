#!/bin/bash
# Download ORE 2015 corpus for OWL 2 RL benchmarking.
#
# Source: https://zenodo.org/records/18578 (ore2015_sample.zip)
# The archive is large; expect a lengthy download.
#
# Usage: ./scripts/download-ore2015.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/../data/ore2015"
ORE_URL="https://zenodo.org/records/18578/files/ore2015_sample.zip?download=1"
ARCHIVE="${DATA_DIR}/ore2015_sample.zip"

echo "ORE 2015 Download"
echo "================="
echo ""

mkdir -p "$DATA_DIR"

if [ -f "$ARCHIVE" ]; then
  echo "Archive already exists at $ARCHIVE"
  echo "Remove it if you want to re-download."
  exit 0
fi

echo "Downloading ORE 2015 corpus..."
echo "Source: $ORE_URL"
echo "Target: $ARCHIVE"
echo ""

if command -v curl &> /dev/null; then
  curl -L -o "$ARCHIVE" "$ORE_URL"
elif command -v wget &> /dev/null; then
  wget -O "$ARCHIVE" "$ORE_URL"
else
  echo "Error: curl or wget is required."
  exit 1
fi

echo ""
echo "Extracting..."
unzip -q "$ARCHIVE" -d "$DATA_DIR"

echo "Done."
echo "Data directory: $DATA_DIR"
