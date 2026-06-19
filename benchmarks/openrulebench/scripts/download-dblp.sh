#!/bin/bash
# Download DBLP XML data for OpenRuleBench
#
# DBLP (Digital Bibliography & Library Project) provides publication metadata.
# The full XML dump is ~4GB compressed, ~30GB uncompressed.
#
# Usage: ./scripts/download-dblp.sh
#
# This will download dblp.xml.gz to data/dblp/

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/../data/dblp"
DBLP_URL="https://dblp.uni-trier.de/xml/dblp.xml.gz"
DBLP_DTD_URL="https://dblp.uni-trier.de/xml/dblp.dtd"

echo "OpenRuleBench DBLP Data Download"
echo "================================"
echo ""

# Create data directory
mkdir -p "$DATA_DIR"

# Check if already downloaded
if [ -f "$DATA_DIR/dblp.xml.gz" ]; then
    echo "DBLP data already exists at $DATA_DIR/dblp.xml.gz"
    echo "To re-download, remove the file first."
    exit 0
fi

echo "Downloading DBLP XML data..."
echo "Source: $DBLP_URL"
echo "Target: $DATA_DIR/dblp.xml.gz"
echo ""
echo "Note: The file is approximately 4GB. This may take a while."
echo ""

# Download with progress
if command -v wget &> /dev/null; then
    wget -O "$DATA_DIR/dblp.xml.gz" "$DBLP_URL"
elif command -v curl &> /dev/null; then
    curl -L -o "$DATA_DIR/dblp.xml.gz" "$DBLP_URL"
else
    echo "Error: Neither wget nor curl is available."
    exit 1
fi

# Download DTD (required for XML parsing)
echo ""
echo "Downloading DBLP DTD..."
if command -v wget &> /dev/null; then
    wget -O "$DATA_DIR/dblp.dtd" "$DBLP_DTD_URL"
elif command -v curl &> /dev/null; then
    curl -L -o "$DATA_DIR/dblp.dtd" "$DBLP_DTD_URL"
fi

echo ""
echo "Download complete!"
echo ""
echo "Files:"
ls -lh "$DATA_DIR/"
echo ""
echo "To run DBLP benchmarks:"
echo "  ./bench.clj dblp:small dblp:medium"
