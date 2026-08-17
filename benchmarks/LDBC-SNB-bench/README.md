# LDBC SNB read-query benchmark

This project implements the 14 Interactive Complex reads (IC1-IC14) and seven
Interactive Short reads (IS1-IS7) from LDBC Social Network Benchmark
Interactive v1 in Datalevin and maintains a matching Cypher suite for Neo4j.
Both runners use the same correctness-gated query-latency harness so
optimization work and local cross-system comparisons share one measurement
contract. The reported comparison follows the JOB benchmark method: execute
one complete warmup pass, start a new client/JVM process, then execute and
report one complete measurement pass. It does not repeatedly time each query
and summarize warmed executions.

This is not an official LDBC result. The official driver defines a scheduled
workload with update operations, dependent short reads, throughput/power
metrics, and auditing rules. This harness measures the implemented read
queries in either one embedded Datalevin process or one local Neo4j Community
server; every EDN report records `:official-ldbc-result false` to make that
distinction machine-readable.

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

The setup script installs Neo4j when absent or upgrades the Homebrew formula
to its latest stable version. It creates a benchmark-local configuration and
does not edit Homebrew's global `neo4j.conf`:

```bash
./neo4j/install-neo4j.sh
```

Import the same Spark Datagen CSV tree used by the Datalevin loader. `--wipe`
removes only this benchmark's Neo4j database and import staging directory. With
`--start`, the script starts the server, waits for Bolt, and applies
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

The local server defaults to Neo4j Community, Cypher 25, an 8 GiB heap, a
4 GiB page cache, Bolt on `localhost:7687`, and disabled HTTP connectors. Set
`NEO4J_HEAP`, `NEO4J_PAGECACHE`, or the other variables listed by
`./neo4j/server.sh --help` when the host requires a different allocation. The
actual server version and memory settings are queried at run time and embedded
in every Neo4j EDN report.

## Run the benchmark

The default command runs every query with the bundled validated SF1 parameter
for that query. A command performs one complete measured pass with no
same-process warmup:

```bash
clj -M:bench
```

For a result comparable to the JOB numbers, invoke the command twice. These
are two independent client/JVM processes; the first report is the warmup pass
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

The Neo4j runner accepts the same harness options and query names. Keep the
server alive, but use separate runner invocations for the two client passes:

```bash
./neo4j/server.sh start

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

./neo4j/server.sh stop
```

Neo4j connection options are `--address`, `--user`, `--password`, `--database`,
and `--cypher`. The defaults target the local setup above.

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

1. a client/JVM process executes one complete pass labeled `warmup`, then exits;
2. a new client/JVM process executes one complete pass labeled `measurement`;
3. only the second pass supplies the reported query times;
4. the comparator checks distinct JVM-instance IDs, matching configuration and
   parameter schedules, and exact result digests from both processes.

This avoids turning five or ten immediately repeated executions into a test of
query caches. The warmup pass is still useful for bringing database and
filesystem pages into the storage cache.

The timer starts immediately before query execution and stops after
query-specific post-processing and full result realization. For Datalevin this
is the embedded query call. For Neo4j it spans `Session.run`, Bolt transfer,
record conversion, and full `.list` realization. Database or driver/session
open, parameter conversion, cache clearing, console output, and artifact
writing are excluded.

Datalevin's query-result cache is disabled. Before each measured query, the
runner clears Datalevin's parsed-query and plan caches outside the timer; parse,
planning, execution, post-processing, and realization then happen inside the
timed call. Neo4j receives `CYPHER cache=skip` on measured queries, so its timed
call also parses and plans instead of reusing a cached logical plan. This
benchmark is therefore aimed at query processing, not query-plan or final-result
cache lookup.

Storage caching is intentionally retained. Datalevin reopens the same LMDB
database in the second process, allowing operating-system pages to remain warm.
Neo4j keeps its server and page cache alive while only the Bolt client/JVM is
restarted. That gives the server system an unavoidable lifecycle advantage,
but matches the JOB convention and avoids restarting a database server merely
because the embedded system starts a new client process.

`--query-cache` explicitly measures Datalevin's cached application path and is
recorded in the report. Neo4j rejects that option because it has no equivalent
application result-cache mode; do not use it in a cross-system comparison.

The default is a warm-cache, single-client latency workload. It is not a
cold-page-cache test, a concurrent throughput test, or the official operation
schedule. A Datalevin-versus-Neo4j comparison also necessarily compares an
embedded call with local Bolt; Neo4j's loopback transport and result decoding
are intentionally included in its end-to-end query latency.

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
Its EDN report also records the schema-file hash and the exact live index
inventory returned by the server. The independent-pass validator requires that
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

The following same-harness snapshot was captured sequentially on 2026-08-17
PDT. For each system, the warmup and measurement passes were separate
client/JVM processes. Neo4j's server stayed alive between its two client passes;
Datalevin reopened the same LMDB database. Only the single execution of each
query in the second process is reported. This is not an official LDBC result or
a portable cross-machine baseline.

- SF1, using the same raw Spark Datagen dataset and one bundled validated
  parameter for each query
- one complete warmup pass in one process, followed by one complete measured
  pass in a new process; no same-process warmups and no repeated measurements
- query-result cache disabled, fresh parse and planning for each query, and
  result verification enabled
- identical parameter schedule SHA-256:
  `4d65cafce1512af2db43b6cd28b50fcf5de67a1e3bce88f4d9bb74f46c509d2d`
- Datalevin 1.0.2; Neo4j Community 2026.06.0 with Java driver 6.2.0,
  Cypher 25, an 8 GiB heap, and a 4 GiB page cache
- Clojure 1.12.4 and OpenJDK 21.0.11 with a 9 GiB client maximum heap
- Apple M3 Pro (12 cores, 36 GB), macOS 26.5.1, AArch64
- Neo4j import: 3,650,498 nodes and 20,630,969 relationships
- Neo4j schema: eight ID uniqueness constraints and its two automatic token
  lookup indexes; no workload-specific secondary indexes; schema SHA-256
  `ed0bb7c9957fd88cf72d0bd18413a424371e122d2f25bc9e6351d020d57db9e5`
- Datalevin measurement database manifest: 4,247,601,157 bytes, metadata
  SHA-256
  `5676d5deb3150f7f98fdf147a387509ab30e980dd4615d94bb6abd56433e8698`
- Neo4j measurement database manifest: 1,911,189,908 bytes, metadata SHA-256
  `639fda714cbc4a7912d6965d85963910fa55a45e4b20bfb9cade180ce25e73fc`

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

| Query | Datalevin measured (ms) | Neo4j Community measured (ms) | Neo4j Community / Datalevin | Lower time |
|---|---:|---:|---:|---|
| IC1 | 587.998 | 92.818 | 0.158x | Neo4j Community 6.335x |
| IC2 | 661.245 | 57.989 | 0.088x | Neo4j Community 11.403x |
| IC3 | 690.553 | 837.922 | 1.213x | Datalevin 1.213x |
| IC4 | 228.961 | 772.807 | 3.375x | Datalevin 3.375x |
| IC5 | 3,615.858 | 1,486.024 | 0.411x | Neo4j Community 2.433x |
| IC6 | 15.759 | 436.938 | 27.726x | Datalevin 27.726x |
| IC7 | 93.481 | 60.536 | 0.648x | Neo4j Community 1.544x |
| IC8 | 29.980 | 34.768 | 1.160x | Datalevin 1.160x |
| IC9 | 131.047 | 981.807 | 7.492x | Datalevin 7.492x |
| IC10 | 1,115.216 | 108.751 | 0.098x | Neo4j Community 10.255x |
| IC11 | 92.727 | 44.669 | 0.482x | Neo4j Community 2.076x |
| IC12 | 264.865 | 86.693 | 0.327x | Neo4j Community 3.055x |
| IC13 | 1,168.985 | 20.358 | 0.017x | Neo4j Community 57.422x |
| IC14 | 832.416 | 18,308.867 | 21.995x | Datalevin 21.995x |
| IS1 | 7.962 | 18.641 | 2.341x | Datalevin 2.341x |
| IS2 | 60.042 | 45.491 | 0.758x | Neo4j Community 1.320x |
| IS3 | 21.813 | 17.579 | 0.806x | Neo4j Community 1.241x |
| IS4 | 4.909 | 18.443 | 3.757x | Datalevin 3.757x |
| IS5 | 20.841 | 21.425 | 1.028x | Datalevin 1.028x |
| IS6 | 4.281 | 35.627 | 8.323x | Datalevin 8.323x |
| IS7 | 23.396 | 34.257 | 1.464x | Datalevin 1.464x |

| Query set | Datalevin sum of measured times (ms) | Neo4j Community sum of measured times (ms) | Neo4j Community / Datalevin sum | Geomean per-query ratio |
|---|---:|---:|---:|---:|
| All | 9,672.335 | 23,522.408 | 2.432x | 1.018x |
| IC1-IC14 | 9,529.090 | 23,330.945 | 2.448x | 0.760x |
| IS1-IS7 | 143.244 | 191.462 | 1.337x | 1.824x |

The workload remains mixed: Datalevin was lower on 11 query types and Neo4j on
10. The summed all-query time was 2.432x higher for Neo4j, largely because of
IC14; the equal-query geometric mean was nearly even at 1.018x. Within the complex
reads, the geometric mean favored Neo4j even though IC14 made its sum larger.
Both short-read aggregates favored Datalevin in this pass. None of these
aggregates is an official throughput metric.

IC6 illustrates the indexing-policy difference directly: its tag-name input is
an automatic AVE lookup in Datalevin, while the untuned Neo4j schema must find
the matching tag without a user-created `Tag.name` index. The tracked January
Neo4j CSV is also not comparable to this table: its legacy runner launched and
wall-clocked a separate `cypher-shell` JVM for every query, adding roughly a
one-second client/tool floor that the query-processing timer intentionally
excludes here.

The four raw pass reports and generated comparison for this snapshot are:

```text
/private/tmp/ldbc-onepass-datalevin-warmup-20260817.edn
/private/tmp/ldbc-onepass-datalevin-20260817.edn
/private/tmp/ldbc-onepass-neo4j-warmup-20260817.edn
/private/tmp/ldbc-onepass-neo4j-20260817.edn
/private/tmp/ldbc-onepass-comparison-20260817.edn
/private/tmp/ldbc-onepass-comparison-20260817.csv
```

Rerun both independent-process pairs and retain all four EDN reports before
using a timing change for another optimization decision. With one measured
observation per query, extreme crossovers such as IC13 and IC14 are profiling
leads, not general claims about either engine.

## References

1. [LDBC SNB benchmark and specifications](https://ldbcouncil.org/benchmarks/snb/)
2. [LDBC SNB Interactive v1 driver](https://github.com/ldbc/ldbc_snb_interactive_v1_driver)
3. [LDBC SNB Interactive v1 Hadoop Datagen](https://github.com/ldbc/ldbc_snb_datagen_hadoop)
4. [LDBC SNB Spark Datagen](https://github.com/ldbc/ldbc_snb_datagen_spark)
5. [Neo4j Deployment Center](https://neo4j.com/deployment-center/)
6. [Neo4j release notes](https://neo4j.com/release-notes/)
7. [Neo4j Java Driver manual](https://neo4j.com/docs/java-manual/current/)
