#!/bin/bash
# XSB Prolog benchmark runner for OpenRuleBench
# Usage: ./run.sh [benchmark names...]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../../data/generated"
XSB_CMD="${XSB_PATH:-xsb}"

# Check if XSB is available
if ! command -v "$XSB_CMD" &> /dev/null; then
    echo "XSB not found. Set XSB_PATH or install XSB."
    echo "---	" | tr -d '\n'
    exit 0
fi

run_tc_benchmark() {
    local data_type=$1  # cyclic or acyclic
    local data_size=$2  # small or large
    local query_type=$3 # ff, bf, or fb

    local data_file="$DATA_DIR/tc-${data_type}-${data_size}.pl"

    if [ ! -f "$data_file" ]; then
        echo "---	" | tr -d '\n'
        return
    fi

    # Run XSB with the benchmark
    result=$("$XSB_CMD" --nobanner --quietload -e "
        ['$SCRIPT_DIR/tc'],
        ['$data_file'],
        cputime(Start),
        (findall((A,B), tc(A,B), _) -> true ; true),
        cputime(End),
        Time is (End - Start) * 1000,
        format('~3f', [Time]),
        halt.
    " 2>/dev/null)

    echo "${result:----}	" | tr -d '\n'
}

run_sg_benchmark() {
    local data_size=$1  # small or large
    local query_type=$2 # ff or bf

    local data_file="$DATA_DIR/sg-${data_size}.pl"

    if [ ! -f "$data_file" ]; then
        echo "---	" | tr -d '\n'
        return
    fi

    result=$("$XSB_CMD" --nobanner --quietload -e "
        ['$SCRIPT_DIR/sg'],
        ['$data_file'],
        cputime(Start),
        (findall((X,Y), sg(X,Y), _) -> true ; true),
        cputime(End),
        Time is (End - Start) * 1000,
        format('~3f', [Time]),
        halt.
    " 2>/dev/null)

    echo "${result:----}	" | tr -d '\n'
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
