#!/bin/bash
# Generate LUBM (Lehigh University Benchmark) data for OpenRuleBench
#
# LUBM is a semantic web benchmark that generates university domain data.
# The official UBA generator is available at: https://github.com/rvesse/lubm-uba
#
# This benchmark suite includes synthetic LUBM data generation in Clojure,
# which produces equivalent data structures without requiring the UBA generator.
#
# Usage: ./scripts/generate-lubm.sh [num_universities]
#
# If UBA generator is installed (optional):
#   java -jar uba.jar -univ 1 -onto http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl
#
# Otherwise, synthetic data is generated automatically when running benchmarks.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/../data/lubm"

echo "OpenRuleBench LUBM Data Generation"
echo "==================================="
echo ""

# Create data directory
mkdir -p "$DATA_DIR"

NUM_UNIVERSITIES=${1:-1}

echo "LUBM Benchmark Information:"
echo "  - LUBM-1:  1 university  (~100K triples)"
echo "  - LUBM-10: 10 universities (~1M triples)"
echo "  - LUBM-50: 50 universities (~5M triples)"
echo ""

# Check if UBA generator is available
if [ -f "$DATA_DIR/uba.jar" ]; then
    echo "UBA generator found. Generating LUBM data with $NUM_UNIVERSITIES universities..."
    cd "$DATA_DIR"
    java -jar uba.jar -univ "$NUM_UNIVERSITIES" -onto "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl"
    echo ""
    echo "LUBM data generated in $DATA_DIR"
else
    echo "UBA generator not found at $DATA_DIR/uba.jar"
    echo ""
    echo "Option 1: Download UBA generator manually:"
    echo "  git clone https://github.com/rvesse/lubm-uba"
    echo "  cd lubm-uba && mvn package"
    echo "  cp target/uba*.jar $DATA_DIR/uba.jar"
    echo ""
    echo "Option 2: Use synthetic data generation (default):"
    echo "  The benchmark will automatically generate synthetic LUBM data"
    echo "  when you run: ./bench.clj lubm:lubm-1"
    echo ""
fi

echo "To run LUBM benchmarks:"
echo "  ./bench.clj lubm:lubm-1"
echo "  ./bench.clj lubm:lubm-10"
