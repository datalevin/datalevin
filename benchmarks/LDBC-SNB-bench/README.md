# LDBC SNB read-query benchmark

This project implements the 14 Interactive Complex reads (IC1-IC14) and seven
Interactive Short reads (IS1-IS7) from LDBC Social Network Benchmark
Interactive v1 in Datalevin and maintains a matching Cypher suite for Neo4j.
Both runners use the same correctness-gated query-latency harness so
optimization work and local cross-system comparisons share one measurement
contract. The reported comparison follows the JOB benchmark method: execute
one complete warmup pass, start a new runner/JVM process, then execute and
report one complete measurement pass. It does not repeatedly time each query
and summarize warmed executions, as that would be measuring caching behavior.

This is not an official LDBC result. The official driver defines a scheduled
workload with update operations, dependent short reads, throughput/power
metrics, and auditing rules. This harness measures the implemented read
queries in separate embedded Datalevin and Neo4j Community processes; every
EDN report records `:official-ldbc-result false` to make that distinction
machine-readable.

## What is covered

| Class | Queries | Examples |
|---|---:|---|
| Interactive Complex | IC1-IC14 | multi-hop traversal, aggregation, negation, shortest paths |
| Interactive Short | IS1-IS7 | person/message lookup and small traversals |

The Datalevin mapping is in
[`schema.clj`](src/ldbc_snb_bench/schema.clj), the loader is in
[`loader.clj`](src/ldbc_snb_bench/loader.clj), and query implementations are in
[`queries/`](src/ldbc_snb_bench/queries/). The Neo4j equivalents are in
[`neo4j/queries.cypher`](neo4j/queries.cypher), with import and schema setup in
[`neo4j/`](neo4j/).

## Prerequisites

- JDK 21+
- Clojure CLI tools
- Docker for the included Spark Datagen helper
- Homebrew on macOS for the optional Neo4j setup helper
- enough disk for generated data and both databases (about 15 GiB is a
  practical minimum for an SF1 comparison)

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

## Prepare Neo4j

The setup script installs Neo4j when absent and verifies that the installed
tools match the embedded artifact version pinned by this benchmark. It creates
a benchmark-local configuration and does not edit Homebrew's global
`neo4j.conf`:

```bash
./neo4j/install-neo4j.sh
```

Import the same Spark Datagen CSV tree used by the Datalevin loader. `--wipe`
removes only this benchmark's Neo4j database and import staging directory. With
`--start`, the script temporarily starts the server, waits for Bolt, and applies
[`neo4j/schema.cypher`](neo4j/schema.cypher):

```bash
./neo4j/bulk-import-native.sh --wipe --start
./neo4j/server.sh stop
```

The Neo4j schema adds only uniqueness constraints for the LDBC entity IDs. It
deliberately omits workload-specific secondary indexes. Datalevin maintains an
attribute-value-leading (AVE) index for every datom without user index
configuration; retaining that out-of-the-box behavior is part of the
comparison rather than something the Neo4j setup attempts to reproduce with
hand-selected indexes.

The benchmark itself embeds Neo4j Community in the runner JVM through the
`org.neo4j/neo4j` dependency. The embedded artifact and the `neo4j-admin`
version used for import must match; the expected version is recorded in
[`neo4j/version.txt`](neo4j/version.txt) and checked by the setup, import, and
test paths. The runner defaults to Cypher 25 and a 4 GiB Neo4j page cache. Set
`NEO4J_PAGECACHE` or pass `--page-cache` when the host requires a different
allocation. Every report records the actual embedded artifact version, access
mode, page-cache size, and live schema inventory.

## Run the benchmark

The default command runs every query with the bundled validated SF1 parameter
for that query. A command performs one complete measured pass with no
same-process warmup:

```bash
clj -M:bench
```

For a result comparable to the JOB numbers, invoke the command twice. These
are two independent runner/JVM processes; the first report is the warmup pass
and only the second report supplies latency numbers:

```bash
clj -M:bench \
  --run-role warmup \
  --output /tmp/ldbc-datalevin-warmup.edn \
  --perf /tmp/ldbc-datalevin-warmup-perf.csv \
  --results /tmp/ldbc-datalevin-warmup-results.csv

clj -M:bench \
  --run-role measurement \
  --output /tmp/ldbc-datalevin.edn \
  --perf /tmp/ldbc-datalevin-perf.csv \
  --results /tmp/ldbc-datalevin-results.csv
```

The Neo4j runner accepts the same harness options and query names. Each command
opens the database directly in its runner JVM and closes it afterward, so the
standalone server must remain stopped:

```bash
./neo4j/run-queries.sh \
  --run-role warmup \
  --output /tmp/ldbc-neo4j-warmup.edn \
  --perf /tmp/ldbc-neo4j-warmup-perf.csv \
  --results /tmp/ldbc-neo4j-warmup-results.csv

./neo4j/run-queries.sh \
  --run-role measurement \
  --output /tmp/ldbc-neo4j.edn \
  --perf /tmp/ldbc-neo4j-perf.csv \
  --results /tmp/ldbc-neo4j-results.csv
```

Embedded Neo4j options are `--database`, `--home`, `--page-cache`, and
`--cypher`. `--db` selects the Neo4j data directory, just as it selects the
Datalevin database for that runner.

`--warmup` and multiple `--iterations` remain available for diagnostics inside
one process, but those settings measure progressively warmed process state and
are not used for the published comparison. For example:

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

Within one invocation, the harness executes complete passes over the selected
query/parameter schedule. `--warmup` controls optional untimed, same-process
passes (default zero), and `--iterations` controls measured passes (default
one). The first successful execution is also the correctness baseline, so the
default does not add an extra untimed query execution.

The published JOB-style procedure is deliberately outside a single harness
process:

1. a runner/JVM process executes one complete pass labeled `warmup`, then exits;
2. a new runner/JVM process executes one complete pass labeled `measurement`;
3. only the second pass supplies the reported query times;
4. the comparator checks distinct JVM-instance IDs, matching configuration and
   parameter schedules, and exact result digests from both processes.

This avoids turning five or ten immediately repeated executions into a test of
query caches. The warmup pass is still useful for bringing database and
filesystem pages into the storage cache.

The timer starts immediately before query execution and stops after
query-specific post-processing and full result realization. For Datalevin this
is the embedded query call. For Neo4j it spans the embedded transaction, Cypher
execution, result iteration and conversion, and transaction completion.
Database open, parameter conversion, cache clearing, console output, and
artifact writing are excluded for both systems.

Datalevin's query-result cache is disabled. Before each measured query, the
runner clears Datalevin's parsed-query and plan caches outside the timer; parse,
planning, execution, post-processing, and realization then happen inside the
timed call. Neo4j receives `CYPHER cache=skip` on measured queries, so its timed
call also parses and plans instead of reusing a cached logical plan. This
benchmark is therefore aimed at query processing, not query-plan or final-result
cache lookup.

The Neo4j runner reads its live schema through the embedded schema API rather
than issuing an untimed metadata query. It therefore does not compile Cypher
before IC1; any lazy query-compiler initialization in the fresh measurement JVM
is included in that first timed query.

Both benchmark runner aliases request a fixed 2 GiB initial heap and enable
Clojure direct linking, matching the main Datalevin build. Fixing the initial
heap reduces run-to-run G1 heap-expansion variability. Direct linking and the
effective maximum heap are recorded in the host manifest, and the comparator
rejects reports produced with different compiler modes or maximum heaps.

Storage caching is intentionally retained. Both measurement processes reopen
their database after the independent warmup process exits, allowing operating-
system pages to remain warm. Neo4j's in-process page cache and both JVMs' JIT
state do not survive the process boundary. Database initialization remains
outside the query timer for both systems.

`--query-cache` explicitly measures Datalevin's cached application path and is
recorded in the report. Neo4j rejects that option because it has no equivalent
application result-cache mode; do not use it in a cross-system comparison.

The default is a warm-filesystem-cache, single-client latency workload. It is
not a cold-page-cache test, a concurrent throughput test, or the official
operation schedule. Both database engines and query APIs run embedded, without
network transport.

## Correctness and failures

For the bundled SF1 parameters, each process compares its result count with the
validated per-query oracle. If a process executes a query more than once, every
later result must also have the same canonical SHA-256 digest as its first
result. The four-report comparator additionally requires exact digest equality
between each system's independent warmup and measurement processes.

For external parameters, no independent expected output is bundled. The
first result becomes a repeat-consistency oracle. A single-process report labels
one execution `:single-execution`; comparing independent warmup and measurement
reports supplies a repeat check but does not claim independent semantic
validation. Generate or retain official driver validation outputs when stronger
external-parameter validation is required.

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

Datalevin defaults:

| Path | Contents |
|---|---|
| `results/report.edn` | host/JVM manifest, DB metadata, parameter schedule and digest, raw samples, summaries, result digests, correctness status |
| `results/perf.csv` | one latency-distribution row per query/parameter pair |
| `results/results.csv` | result rows used for correctness checks, in report schedule order |

Neo4j writes the same three artifact types below `neo4j/results/` by default.
Its EDN report also records the embedded artifact version, schema-file hash,
and exact live index inventory. The independent-pass validator requires that
inventory to match between warmup and measurement.

Override them with `--output`, `--perf`, and `--results`. The EDN report is the
source of truth; CSV files are conveniences. Retain the EDN report whenever a
number informs an optimization decision.

The database manifest hashes file names, sizes, and modification times. It does
not hash the multi-gigabyte LMDB contents, so also retain the data-generation
configuration and immutable dataset location for durable comparisons.

Compare two raw reports with the checked comparator:

```bash
clj -M:compare \
  /tmp/ldbc-datalevin.edn \
  /tmp/ldbc-neo4j.edn \
  --left-warmup /tmp/ldbc-datalevin-warmup.edn \
  --right-warmup /tmp/ldbc-neo4j-warmup.edn \
  --edn /tmp/ldbc-comparison.edn \
  --csv /tmp/ldbc-comparison.csv
```

It refuses reports with different hosts, parameter schedules, scale factors,
warmups, repetitions, cache modes, verification modes, timing boundaries, or
query/result-count sets. When given both warmup paths, it also validates each
system's independent-process replay before comparing systems. It prints
per-query Markdown plus summed-time and geometric-mean summaries; the EDN
output remains the machine-readable source.

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

## Current local Datalevin/Neo4j comparison

The following same-harness snapshot was captured sequentially on 2026-08-23
PDT. For each system, the warmup and measurement passes were separate
runner/JVM processes, and each process opened the database through its embedded
API. No server, network transport, or driver round trip was involved. Only the
single execution of each query in the second process is reported. This is not
an official LDBC result or a portable cross-machine baseline.

- SF1, using the same raw Spark Datagen dataset and one bundled validated
  parameter for each query
- one complete warmup pass in one process, followed by one complete measured
  pass in a new process; no same-process warmups and no repeated measurements
- query-result cache disabled, fresh parse and planning for each query, and
  result verification enabled
- identical parameter schedule SHA-256:
  `4d65cafce1512af2db43b6cd28b50fcf5de67a1e3bce88f4d9bb74f46c509d2d`
- Datalevin 1.0.2; Neo4j Community Embedded 2026.06.0 with Cypher 25 and a
  4 GiB page cache
- Clojure 1.12.5 and OpenJDK 21.0.11 with a 9 GiB runner maximum heap
- Clojure direct linking enabled for both benchmark runners
- Apple M3 Pro (12 cores, 36 GB), macOS 26.5.1, AArch64
- Neo4j import: 3,650,498 nodes and 20,630,969 relationships
- Neo4j schema: eight ID uniqueness constraints and its two automatic token
  lookup indexes; no workload-specific secondary indexes; schema SHA-256
  `ed0bb7c9957fd88cf72d0bd18413a424371e122d2f25bc9e6351d020d57db9e5`
- Datalevin measurement database manifest: 4,247,601,157 bytes, metadata
  SHA-256
  `56d0541dcf7b85cc86b671feaae8053aad3f6ddf202179d6c84885623715f5db`
- Neo4j measurement database manifest: 1,911,201,183 bytes, metadata SHA-256
  `c6355f5c3bc56c3cc1ef8f38c60d954893f4e495237a08b34fae3b5e9924cfcc`

All 21 queries passed their SF1 result-count oracle. The independent-process
validator matched all 21 exact result digests from warmup to measurement for
each system. Seventeen queries also produced byte-identical canonical results
across systems. The remaining four have known output-representation
differences: IC1 uses a Datalevin pull map versus flattened Cypher fields and
collection ordering; IC5 projects an extra forum ID in Datalevin; IC12's
tag-name collection order differs; and IC14 uses flat path IDs plus interaction
count and half-weight in Datalevin, versus a path vector and actual weight in
Neo4j (`actual = 2 * half-weight`).

`Neo4j / Datalevin` below is the ratio of the two single measured executions.
A value below 1 means Neo4j was lower in this pass; a value above 1 means
Datalevin was lower. These are observations, not latency-distribution estimates.

| Query | Datalevin measured (ms) | Neo4j Community Embedded measured (ms) | Neo4j Community Embedded / Datalevin | Lower time |
|---|---:|---:|---:|---|
| IC1 | 230.210 | 1,655.097 | 7.190x | Datalevin 7.190x |
| IC2 | 134.797 | 283.436 | 2.103x | Datalevin 2.103x |
| IC3 | 501.752 | 1,413.357 | 2.817x | Datalevin 2.817x |
| IC4 | 129.552 | 911.362 | 7.035x | Datalevin 7.035x |
| IC5 | 1,572.195 | 1,783.555 | 1.134x | Datalevin 1.134x |
| IC6 | 2.567 | 479.204 | 186.682x | Datalevin 186.682x |
| IC7 | 41.720 | 146.672 | 3.516x | Datalevin 3.516x |
| IC8 | 25.345 | 74.907 | 2.956x | Datalevin 2.956x |
| IC9 | 116.791 | 1,113.116 | 9.531x | Datalevin 9.531x |
| IC10 | 224.493 | 193.530 | 0.862x | Neo4j Community Embedded 1.160x |
| IC11 | 60.759 | 75.418 | 1.241x | Datalevin 1.241x |
| IC12 | 112.504 | 171.706 | 1.526x | Datalevin 1.526x |
| IC13 | 26.513 | 46.701 | 1.761x | Datalevin 1.761x |
| IC14 | 635.653 | 18,807.205 | 29.587x | Datalevin 29.587x |
| IS1 | 2.928 | 21.141 | 7.221x | Datalevin 7.221x |
| IS2 | 49.338 | 65.003 | 1.317x | Datalevin 1.317x |
| IS3 | 5.727 | 19.710 | 3.442x | Datalevin 3.442x |
| IS4 | 1.549 | 23.608 | 15.237x | Datalevin 15.237x |
| IS5 | 1.140 | 27.784 | 24.374x | Datalevin 24.374x |
| IS6 | 2.600 | 47.595 | 18.304x | Datalevin 18.304x |
| IS7 | 11.564 | 45.600 | 3.943x | Datalevin 3.943x |

| Query set | Datalevin sum of measured times (ms) | Neo4j Community Embedded sum of measured times (ms) | Neo4j Community Embedded / Datalevin sum | Geomean per-query ratio |
|---|---:|---:|---:|---:|
| All | 3,889.698 | 27,405.707 | 7.046x | 4.996x |
| IC1-IC14 | 3,814.851 | 27,155.265 | 7.118x | 4.202x |
| IS1-IS7 | 74.847 | 250.442 | 3.346x | 7.064x |

Datalevin was lower on 20 query types and Neo4j Community Embedded on one. The
summed all-query time was 7.046x higher for Neo4j, largely because of IC14; the
equal-query geometric mean favored Datalevin by 4.996x. Within the complex
reads, the sum and geometric mean favored Datalevin by 7.118x and 4.202x,
respectively. All seven short reads were lower for Datalevin; their sum and
geometric mean favored it by 3.346x and 7.064x. None of these aggregates is an
official throughput metric.

IC6 illustrates the indexing-policy difference directly: its tag-name input is
an automatic AVE lookup in Datalevin, while the untuned Neo4j schema must find
the matching tag without a user-created `Tag.name` index.

The four raw pass reports and generated comparison for this snapshot are:

```text
/private/tmp/ldbc-embedded-datalevin-warmup-20260823.edn
/private/tmp/ldbc-embedded-datalevin-20260823.edn
/private/tmp/ldbc-embedded-neo4j-warmup-20260823.edn
/private/tmp/ldbc-embedded-neo4j-20260823.edn
/private/tmp/ldbc-embedded-comparison-20260823.edn
/private/tmp/ldbc-embedded-comparison-20260823.csv
```

Rerun both independent-process pairs and retain all four EDN reports before
using a timing change for another optimization decision. With one measured
observation per query, large per-query ratios are profiling leads, not general
claims about either engine.

## References

1. [LDBC SNB benchmark and specifications](https://ldbcouncil.org/benchmarks/snb/)
2. [LDBC SNB Interactive v1 driver](https://github.com/ldbc/ldbc_snb_interactive_v1_driver)
3. [LDBC SNB Interactive v1 Hadoop Datagen](https://github.com/ldbc/ldbc_snb_datagen_hadoop)
4. [LDBC SNB Spark Datagen](https://github.com/ldbc/ldbc_snb_datagen_spark)
5. [Neo4j Deployment Center](https://neo4j.com/deployment-center/)
6. [Neo4j release notes](https://neo4j.com/release-notes/)
7. [Neo4j embedded Java setup](https://neo4j.com/docs/java-reference/current/java-embedded/setup/)
