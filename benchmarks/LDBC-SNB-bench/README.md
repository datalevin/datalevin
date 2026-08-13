# LDBC SNB read-query benchmark

This project implements the 14 Interactive Complex reads (IC1-IC14) and seven
Interactive Short reads (IS1-IS7) from LDBC Social Network Benchmark
Interactive v1 in Datalevin. It provides a correctness-gated, repeated
query-latency harness for optimization work.

This is not an official LDBC result. The official driver defines a scheduled
workload with update operations, dependent short reads, throughput/power
metrics, and auditing rules. This harness measures the implemented read
queries in one embedded Datalevin process; its EDN report records
`:official-ldbc-result false` to make that distinction machine-readable.

## What is covered

| Class | Queries | Examples |
|---|---:|---|
| Interactive Complex | IC1-IC14 | multi-hop traversal, aggregation, negation, shortest paths |
| Interactive Short | IS1-IS7 | person/message lookup and small traversals |

The Datalevin mapping is in
[`schema.clj`](src/ldbc_snb_bench/schema.clj), the loader is in
[`loader.clj`](src/ldbc_snb_bench/loader.clj), and query implementations are in
[`queries/`](src/ldbc_snb_bench/queries/).

## Prerequisites

- JDK 21+
- Clojure CLI tools
- Docker for the included Spark Datagen helper
- enough disk for the generated data and Datalevin database (about 10 GiB is
  a practical minimum for SF1)

Run commands below from `benchmarks/LDBC-SNB-bench`.

## Generate and load data

The included helper wraps the LDBC Spark Datagen Docker image:

```bash
git clone https://github.com/ldbc/ldbc_snb_datagen_spark.git \
  ../../../ldbc_snb_datagen_spark

./generate-data-docker.sh \
  --scale-factor 1 \
  --parallelism 4 \
  --memory 8g
```

The loader expects Spark Datagen's merged raw CSV tree below `data/`, including
paths such as:

```text
data/graphs/csv/raw/composite-merged-fk/
├── static/Place/part-*.csv
├── static/Organisation/part-*.csv
├── dynamic/Person/part-*.csv
├── dynamic/Person_knows_Person/part-*.csv
├── dynamic/Forum/part-*.csv
├── dynamic/Post/part-*.csv
└── dynamic/Comment/part-*.csv
```

Load and audit it:

```bash
clj -M -m ldbc-snb-bench.core load data/
clj -M -m ldbc-snb-bench.audit
```

Use an alternate database location for loading and benchmarking with
`--db PATH`:

```bash
clj -M -m ldbc-snb-bench.core load --db /tmp/ldbc-sf1 data/
clj -M:bench --db /tmp/ldbc-sf1 IS1
```

## Run the benchmark

The default command runs every query with the bundled validated SF1 parameter
for that query. There is one untimed correctness execution, one warmup, and
five measured repetitions:

```bash
clj -M:bench
```

Run a subset or change repetition counts:

```bash
clj -M:bench \
  --warmup 2 \
  --iterations 10 \
  IC1 IC3 IC5 IS1
```

The bundled suite contains one parameter row per query. Multi-parameter runs
use official substitution files or an EDN suite.

### Official IC substitution parameters

Interactive v1 Datagen writes pipe-delimited files named
`interactive_1_param.txt` through `interactive_14_param.txt`. Dates in those
files are epoch milliseconds. Pass the directory directly:

```bash
clj -M:bench \
  --parameters /path/to/substitution_parameters \
  --parameter-count 10 \
  --seed 42 \
  IC1 IC2 IC3 IC4 IC5 IC6 IC7 IC8 IC9 IC10 IC11 IC12 IC13 IC14
```

Up to `--parameter-count` rows are selected independently for each query using
a stable seeded shuffle. Selection is derived from the query's fixed ordinal,
so selecting only IC5 produces the same IC5 schedule as an all-query run. The
selected rows and schedule SHA-256 are stored in the report.

The v1 substitution directory contains parameters for the complex reads only.
When it is used in an all-query latency run, IS1-IS7 use the bundled SF1 rows.
Use EDN when you need multiple short-read parameters.

### EDN parameter suites

An EDN suite maps case-insensitive query names to one parameter map or a vector
of maps. It may contain any subset if the command selects only that subset:

```clojure
{:ic1 [{:person-id 100 :first-name "John"}
       {:person-id 200 :first-name "Jane"}]
 :ic2 [{:person-id 100 :max-date #inst "2012-07-01T00:00:00.000-00:00"}]
 :is1 [{:person-id 100}
       {:person-id 200}]
 :is4 [{:message-id 1099512606636}]}
```

Date parameters accept EDN instants, ISO-8601 strings, or epoch-millisecond
numbers/strings. IDs, months, durations, and years are normalized to longs.
Headers, required parameters, scalar types, missing files, and empty suites are
validated before the database is opened.

```bash
clj -M:bench --parameters parameters.edn --parameter-count 20 IC1 IS1 IS4
```

## Measurement contract

For every selected query/parameter pair, the harness performs:

1. an untimed correctness execution;
2. the configured untimed warmups;
3. the configured measured repetitions;
4. result digest checks after each warmup and measured run;
5. artifact writing only after measurement is complete.

The timer starts immediately before Datalevin query execution and stops after
query-specific post-processing and full result realization. Database open,
parameter conversion, the correctness baseline, warmups, console output, and
artifact writing are excluded.

Datalevin's query-result cache is disabled by default. This prevents repeated
parameters from measuring cached answers, while normal parse/plan and storage
caches can still warm. `--query-cache` explicitly measures the cached
application path and is recorded in the report; do not mix the two modes in a
comparison.

The default is a warm-cache, single-process latency workload. It is not a
cold-page-cache test, a concurrent throughput test, or the official operation
schedule.

## Correctness and failures

For the bundled SF1 parameters, the untimed result count is compared with the
validated per-query oracle before any timing is accepted. Every later result
must also have the exact same canonical SHA-256 digest as the untimed result.

For external parameters, no independent expected output is bundled. The
untimed result becomes a repeat-consistency oracle, and the report labels the
check `:consistent` rather than claiming an independent validation. Generate
or retain official driver validation outputs when stronger external-parameter
validation is required.

Any query exception, known-count mismatch, or repeat-digest mismatch marks the
run failed, stops timing that parameter, is written to the artifacts, and
causes a non-zero CLI exit. `--no-verify` exists for diagnosis only and should
not be used for published optimization numbers.

The test suite includes an SF1 semantic test for each of the 21 query
implementations, plus parameter parser, deterministic scheduling, latency
statistics, CLI validation, and failure-gate tests:

```bash
clj -M:test
```

## Artifacts

Defaults:

| Path | Contents |
|---|---|
| `results/report.edn` | host/JVM manifest, DB metadata, parameter schedule and digest, raw samples, summaries, result digests, correctness status |
| `results/perf.csv` | one latency-distribution row per query/parameter pair |
| `results/results.csv` | untimed correctness result rows, in report schedule order |

Override them with `--output`, `--perf`, and `--results`. The EDN report is the
source of truth; CSV files are conveniences. Retain the EDN report whenever a
number informs an optimization decision.

The database manifest hashes file names, sizes, and modification times. It does
not hash the multi-gigabyte LMDB contents, so also retain the data-generation
configuration and immutable dataset location for durable comparisons.

Example small smoke run:

```bash
clj -M:bench \
  --warmup 0 \
  --iterations 2 \
  --parameter-count 1 \
  --output /tmp/ldbc-smoke.edn \
  --perf /tmp/ldbc-smoke.csv \
  IS1
```

See every option with `clj -M:bench --help`.

## Comparing systems

The `neo4j/` directory contains the earlier Neo4j loader and one-shot query
runner. Those scripts are useful for semantic cross-checking, but their timing
boundary and repetition model are not yet equivalent to this harness. Do not
present their numbers as a same-harness comparison until the runner produces
the same parameter schedule, correctness gates, raw repetitions, cache policy,
and host manifest.

Likewise, historical single-pass numbers previously shown in this README are
not the benchmark baseline. Regenerate results with the current harness and
archive the EDN reports before drawing performance conclusions.

## References

1. [LDBC SNB benchmark and specifications](https://ldbcouncil.org/benchmarks/snb/)
2. [LDBC SNB Interactive v1 driver](https://github.com/ldbc/ldbc_snb_interactive_v1_driver)
3. [LDBC SNB Interactive v1 Hadoop Datagen](https://github.com/ldbc/ldbc_snb_datagen_hadoop)
4. [LDBC SNB Spark Datagen](https://github.com/ldbc/ldbc_snb_datagen_spark)
