# Indexed Document Benchmark

This benchmark tests the Indexed Document (idoc) feature of Datalevin. It runs
YCSB-style workloads (A/C/F) plus idoc queries to stress nested path lookups,
range predicates, wildcard paths, and array matching.

As a comparison, we also test the document database feature of MongoDB,
PostgreSQL and SQLite under the same workload.

## Benchmark Tasks

Each benchmark run performs the same high-level tasks:

1. **Load**: generate synthetic documents and bulk insert them.
2. **Index/Analyze**:
   - Datalevin runs `analyze` to refresh sampling stats.
   - Postgres/SQLite build expression indexes for the query mix and run `ANALYZE`.
   - MongoDB builds field indexes for the query mix.
3. **Verify**: compare reads, mutations, and idoc queries with an independent
   Clojure oracle.
4. **Within-pass warmup**: execute `--warmup` operations to stabilize the
   freshly built database before timing.
5. **Run**: execute `--ops` operations according to the workload mix.

Verification is mandatory by default. A mismatch or worker failure aborts the
run with a non-zero exit status, so an incomplete run cannot be mistaken for a
benchmark result.

## Measurement Contract

- The dataset, warmup operations, and measured operations are generated once
  from `--seed`. `--system all` reuses those exact schedules for every system.
- The report records SHA-256 digests for the dataset and both schedules.
- Every operation uses client-observed wall-clock time. Query latency includes
  planning, database execution, transfer, and complete realization of result
  ids for all four systems. Every backend projects only record ids for an idoc
  query; full documents are not fetched merely to discard them.
- Updates and read-modify-writes use the same per-record critical section on
  every backend. Time waiting on a contended record is included in latency.
- The default `strict` durability profile uses Datalevin strict WAL,
  PostgreSQL `synchronous_commit=on`, SQLite WAL `synchronous=FULL`, and
  MongoDB write concern `{w: 1, j: true}`.
- Database creation, loading, index creation, correctness checks, and warmup
  are outside the measured interval.
- The console reports mean, median, p95, and p99 latency by operation. Idoc
  latency is also broken down by query shape.
- Publishable results use two complete passes in independent JVM processes:
  one `--run-role warmup` pass followed by one `--run-role measurement` pass.
  Only the measurement pass is reported. Repeating a workload inside one JVM
  is deliberately unsupported because it primarily measures progressively
  warmer code and caches.
- Each artifact records its run role and JVM-instance ID. Use
  `idoc-bench.core/validate-pass-pair` to require distinct JVMs plus matching
  configuration, schedules, systems, host controls, and correctness gates.
- `--output` writes host/JVM metadata, configuration, correctness results,
  summaries, and every raw latency sample to one EDN artifact.

### Operation Types

- **read**: point read by document id.
- **update**: update an existing document with new `:stats` values.
- **rmw**: read-modify-write on `:stats` values.
- **idoc**: execute one of the idoc query shapes (nested, range, wildcards, array).

#### Idoc Query Mix

The benchmark includes queries that exercise:

- Nested path equality (e.g. profile language)
- Range predicates on numeric fields
- Wildcards `:?` (one segment) and `:*` (any depth)
- Array matching (tags inside events)

### Workloads

- **A**: 50% reads, 50% updates
- **C**: 100% reads
- **F**: 100% read-modify-write

The `--idoc` weight injects idoc queries into the workload. For example, with
`--workload C --idoc 30`, the run will mix point reads with idoc queries where
idoc queries are chosen with weight 30 relative to the base workload.

## Documents

Each document models a small record with these fields:

- `:profile` — `:age`, `:lang`, `:persona`
- `:stats` — `:score` (float), `:last_seen` (timestamp millis)
- `:facts` — string-keyed map with `"city"` and `"team"`
- `:memory` — `:topic`, `:source`, `:entities`
- `:tags` — vector of tag strings
- `:events` — vector of event maps, each with:
  - `:ts` (timestamp millis), `:kind`, `:tags`, `:entity {:name ...}`, `:score`

Example (abbreviated):

```clojure
{:profile {:age 34 :lang "en" :persona "developer"}
 :stats   {:score 0.62 :last_seen 1700001234567}
 :facts   {"city" "SF" "team" "blue"}
 :memory  {:topic "roadmap" :source "chat" :entities ["acme" "globex"]}
 :tags    ["urgent" "note"]
 :events  [{:ts 1700002345678
            :kind "chat"
            :tags ["feature" "todo"]
            :entity {:name "acme"}
            :score 0.41}]}
```

## Query Shapes

The `idoc` operation randomly picks one of these query shapes:

- **Nested equality**: match a profile language.
  - Idoc: `{:profile {:lang "en"}}`
  - SQL/Mongo: `profile.lang = 'en'`
- **Range predicate**: score strictly between two values.
  - Idoc: `(< 0.3 [:stats :score] 0.8)`
  - SQL/Mongo: `stats.score > 0.3 AND stats.score < 0.8`
- **Wildcard (one segment)**: match any `:facts` value.
  - Idoc: `{:facts {:? "SF"}}`
  - SQL/Mongo: match `"facts.city" = "SF"` or `"facts.team" = "SF"`
- **Wildcard (any depth)**: match nested events by entity name.
  - Idoc: `{:* {:entity {:name "acme"}}}`
  - SQL/Mongo: match any `events[].entity.name = "acme"`
- **Array match**: match tags inside events.
  - Idoc: `{:events {:tags "urgent"}}`
  - SQL/Mongo: match any `events[].tags` containing `"urgent"`

## Run Benchmarks

```bash
# Datalevin smoke run; the default role is measurement
clojure -M:bench --system datalevin \
  --output results/datalevin-c.edn

# Postgres (JSONB)
clojure -M:bench --system postgres \
  --pg-url jdbc:postgresql://localhost:5432/postgres

# SQLite (JSON1 extension)
clojure -M:bench --system sqlite --sqlite-path /tmp/idoc-bench.sqlite

# MongoDB
clojure -M:bench --system mongo --mongo-uri mongodb://localhost:27017

# Publishable two-pass run. These must be separate shell invocations/JVMs.
clojure -M:bench --system all --records 10000 --ops 10000 \
  --durability strict --run-role warmup \
  --output results/all-c-warmup.edn

clojure -M:bench --system all --records 10000 --ops 10000 \
  --durability strict --run-role measurement \
  --output results/all-c-measurement.edn
```

### Options

- `--system datalevin|postgres|sqlite|mongo|all`  Run target (default datalevin)
- `--workload A|C|F`  Workload type (default C)
- `--records N`       Number of documents to load (default 10000)
- `--ops N`           Number of operations to run (default 10000)
- `--warmup N`        Warmup ops before measurement (default 1000)
- `--threads N`       Number of worker threads (default 1)
- `--batch N`         Load batch size (default 1000)
- `--idoc N`          Weight for idoc queries (default 30)
- `--idoc-trace`      Record Datalevin candidate/verification trace metrics
- `--no-verify`       Skip correctness checks (not valid for published results)
- `--hotset P`        Hotset fraction for id selection (0-1, default 1.0)
- `--output PATH`     Write metadata, summaries, and raw samples as EDN
- `--dir PATH`        Datalevin DB directory (default /tmp/idoc-bench-<uuid>)
- `--dtlv-uri URI`    Datalevin server URI (e.g. dtlv://host:port)
- `--sqlite-path PATH` SQLite DB file path
- `--pg-url URL`      Postgres JDBC URL
- `--pg-user USER`    Postgres user
- `--pg-password PWD` Postgres password
- `--pg-table NAME`   Postgres table name (default idoc_bench_docs)
- `--mongo-uri URI`   MongoDB URI
- `--mongo-db NAME`   MongoDB database (default idoc_bench)
- `--mongo-coll NAME` MongoDB collection (default docs)
- `--seed N`          RNG seed (default 42)
- `--durability strict|relaxed` Acknowledgment profile (default strict)
- `--run-role warmup|measurement` Complete-pass role (default measurement)
- `--keep`            Keep DB artifacts after run

## Results

The completed study used 10,000 documents, 1,000 within-pass warmup operations,
10,000 measured operations, seed 42, and idoc weight 30. For each workload and
thread count, one complete `--system all --run-role warmup` JVM was immediately
followed by one complete `--system all --run-role measurement` JVM. Only the
measurement pass is reported below. All six pairs passed
`validate-pass-pair`: their configurations, schedules, systems, host controls,
and correctness results match, and every warmup/measurement pair has distinct
JVM-instance IDs.

The host was a 12-core Apple Silicon Mac running Java 21.0.11. Backend versions
were Datalevin 1.1.0 / DLMDB 1.0.0, PostgreSQL 18.4, SQLite 3.51.1, and MongoDB
7.0.35. `mediaanalysisd` was paused for all 12 passes and resumed afterward.

| Workload | Datalevin | PostgreSQL | SQLite | MongoDB | Leader |
|---|---:|---:|---:|---:|---:|
| A | 3,358/s | 2,270/s | 438/s | 309/s | Datalevin 1.480X |
| C | 11,454/s | 2,039/s | 437/s | 2,868/s | Datalevin 3.993X |
| F | 2,680/s | 1,789/s | 416/s | 192/s | Datalevin 1.497X |

#### Workload C query-shape p50

Each cell is the p50 latency from the retained measurement pass, in
milliseconds.

| Query shape | Datalevin | PostgreSQL | SQLite | MongoDB |
|---|---:|---:|---:|---:|
| Nested equality | 0.057 | 0.802 | 0.155 | 0.510 |
| Range | 0.328 | 1.552 | 0.789 | 1.814 |
| Wildcard, one segment | 0.051 | 0.635 | 0.136 | 0.501 |
| Wildcard, any depth | 0.147 | 2.767 | 22.719 | 1.112 |
| Array match | 0.207 | 3.615 | 23.924 | 1.459 |

SQLite is competitive on its expression-indexed scalar shapes, but JSON1
cannot index the nested array elements used by wildcard-depth and array-match;
those two shapes scan all documents and dominate its mixed throughput.
MongoDB's explicit journal acknowledgment primarily affects A/F, while C shows
its indexed read/query performance without write acknowledgment cost.

Use `--output` for any result intended to guide optimization or be published.
A publishable result is the measurement half of a validated two-process pair.
For before/after optimization, run one complete pair for each build and keep
the command, seed, machine, JVM, database versions, schedules, and service
configuration fixed.

## Tests and Smoke Checks

```bash
# Pure schedule, oracle, statistics, and validation tests
clojure -M:test

# Fast end-to-end checks for the two embedded systems
clojure -M:bench --system datalevin --records 100 --ops 40 --warmup 5
clojure -M:bench --system sqlite --records 100 --ops 40 --warmup 5
```
## Notes

- Postgres uses JSONB, SQLite uses JSON1 functions, and MongoDB stores the
  document fields at the top level with `_id` as the record id.
- Datalevin runs `analyze` after loading to refresh sampling statistics.
- Postgres/SQLite build expression indexes for the query mix and run ANALYZE
  after loading. MongoDB builds field indexes for the same queries.
- Wildcard-depth and array predicates in SQL and Mongo are implemented against
  the `events` array to match the generated documents.
- The benchmark drops Postgres tables / Mongo collections and removes SQLite
  files unless `--keep` is supplied.
- With multiple threads, throughput is measured over the concurrent wall-clock
  interval, while each raw latency sample is measured around its individual
  operation. Their durations therefore overlap as expected.
- SQLite limitation: SQLite cannot create indexes on nested array elements.
  Queries like `wildcard-depth` (searching `events[*].entity.name`) and `array`
  (searching `events[*].tags`) cannot use indexes in SQLite and require full
  table scans with `json_each`. This is a fundamental limitation of SQLite's
  JSON1 extension and affects performance for these query types.
