# TPC-H Benchmark

Runs 22 analytical queries against Datalevin, SQLite, and PostgreSQL using the
same generated data and query parameters.

This is a TPC-H-derived workload, not an audited TPC-H result. See the
[TPC fair-use rules](https://www.tpc.org/information/other/policy_fair_use5.asp).

## Setup

Run commands from this directory. Generate the data with the reference tools:

```bash
scripts/fetch-dbgen.sh
scripts/generate-data.sh 1
```

Scale factor 1 generates about 1 GB of data, including six million lineitems.
Use `0.01` for a quick check. All three databases load the same `.tbl` files
from `data/tbl/`. Rebuild databases when the storage format or benchmark schema
changes.

Queries use the default `qgen -d` parameters. SQL files are under
`queries/postgres/` and `queries/sqlite/`; Datalog translations are in
`src/datalevin_tpch/queries.clj`. Dates use ISO strings, and queries preserve
the required ordering and row limits.

Datalevin uses normalized entities linked by references. Following the JOB
loader, each table has a separate entity-ID range, and foreign keys become
target entity IDs before bulk loading. Ranges use actual source-key maxima,
including sparse order keys and zero-based region keys. Composite-key rows
use deterministic row ordinals within their own ranges; their unique tuples
contain references. Queries join entity IDs and project business keys where
the SQL result requires them.

## Run

Run each complete query set twice in separate processes. The first pass warms
the database; report the second pass. Do not repeat individual queries to
calculate averages or medians.

### Datalevin

```bash
clj -X:datalevin-db
clj -X:datalevin-bench '{:queries :all :out "datalevin_warmup.csv"}'
clj -X:datalevin-bench '{:queries :all :out "datalevin_pass.csv"}'
```

The loader starts with a 100,000 MiB map, matching JOB, so SF1 loading needs
no map growth. Override `:mapsize` in MiB for larger datasets.

### SQLite

```bash
clj -X:sqlite-db
clj -X:sqlite-bench '{:queries :all :out "sqlite_warmup.csv"}'
clj -X:sqlite-bench '{:queries :all :out "sqlite_pass.csv"}'
```

### PostgreSQL

```bash
clj -X:pg-db '{:url "jdbc:postgresql://localhost:5432/postgres"}'

# macOS with Homebrew PostgreSQL: reset caches before warmup.
sudo /usr/sbin/purge
brew services restart postgresql@18

clj -X:pg-bench '{:url "jdbc:postgresql://localhost:5432/postgres" :out "postgres_warmup.csv"}'
clj -X:pg-bench '{:url "jdbc:postgresql://localhost:5432/postgres" :out "postgres_pass.csv"}'
```

Use your server's service manager on other installations. Reset once before
warmup; keep PostgreSQL running and do not purge caches between the two passes.
Connection settings can also come from the `pg.url`, `pg.user`, and `pg.pass`
JVM properties.

## Verify results

After loading Datalevin and SQLite, compare all 22 query results:

```bash
clj -X:verify '{:queries :all :out "verification.edn"}'
```

Verification checks table counts, disjoint entity-ID ranges, reference targets,
result values, ordering, and limits. Old scalar-key databases require a rebuild.
Monetary aggregates use a relative tolerance of `1e-6`; identifiers, counts, and quantity
sums must match exactly. SQLite uses case-sensitive `LIKE` to match the queries.
The command exits nonzero on failure. Use `:dump? true` to print mismatches or
`:timeout-ms 120000` to set a per-query deadline.

## Measurement

- Use the same data, query order, machine, and database settings. Run without
  competing builds, tests, or benchmarks.
- Loading, indexing, verification, and warmup are excluded from reported times.
- Datalevin disables query-result caching. Each process starts with fresh plan
  caches; normal plan caching remains enabled within the pass.
- Datalevin and SQLite timings include consuming results. PostgreSQL reports
  server-side `EXPLAIN ANALYZE` planning and execution, excluding JDBC transfer.
- Datalevin's `:explain? true` option adds preparation and execution timings.
  Preparation can include nested-query execution.
- SQLite uses WAL and a 1 GiB read memory map. On macOS, the harness temporarily
  pauses the current user's media-analysis daemons during measurement.

## Results

Scale factor 1 (6,001,215 lineitems), measured on a 12-core Apple M3 Pro with
36 GiB RAM and macOS 26.6.2. All three engines were measured on 2026-11-05.
Versions: Datalevin 1.2.10
development checkout on Clojure 1.12.6 and Java 21.0.13.0.3 (8 GiB heap),
SQLite 3.51.1, and PostgreSQL 18.6.
PostgreSQL used 128 MiB shared buffers, 4 MiB work memory, and up to two parallel
workers per gather. Datalevin's parallel aggregate scans and grouped witness
checks use up to four participants, including the calling thread; existing join
execution uses its normal worker budget.

Times below are from the second complete pass, with warmup and measurement in
separate JVMs. The existing SF1 databases were reused, with matching row counts
for all eight tables. PostgreSQL stayed running before and between its passes;
neither its shared buffers nor the OS filesystem cache was reset for this run.

Units are milliseconds; lower is better. PostgreSQL times include planning and
execution; Datalevin and SQLite also include consuming results. All 22
Datalevin queries passed the SQLite correctness check.

| Query | Datalevin (ms) | SQLite (ms) | PostgreSQL (ms) |
|---|---:|---:|---:|
| Q1 | 1,115.9 | 3,790.7 | 902.5 |
| Q2 | 130.7 | 141.0 | 175.1 |
| Q3 | 451.9 | 542.6 | 204.2 |
| Q4 | 158.3 | 135.3 | 106.9 |
| Q5 | 354.4 | 339.7 | 325.5 |
| Q6 | 369.6 | 338.2 | 131.2 |
| Q7 | 271.4 | 3,991.3 | 366.8 |
| Q8 | 91.6 | 125.2 | 142.1 |
| Q9 | 1,136.8 | 1,465.5 | 1,070.6 |
| Q10 | 269.0 | 1,006.5 | 188.0 |
| Q11 | 74.2 | 36.7 | 75.3 |
| Q12 | 412.5 | 857.2 | 236.0 |
| Q13 | 718.8 | 1,022.9 | 299.1 |
| Q14 | 144.4 | 4,558.2 | 128.9 |
| Q15 | 151.0 | 344.5 | 176.2 |
| Q16 | 156.1 | 105.5 | 107.7 |
| Q17 | 48.4 | 51.0 | 482.2 |
| Q18 | 571.8 | 548.7 | 1,504.3 |
| Q19 | 54.5 | 28.7 | 28.1 |
| Q20 | 46.4 | 70.2 | 169.0 |
| Q21 | 516.1 | 1,450.9 | 268.4 |
| Q22 | 82.3 | 38.0 | 35.0 |
| **Total** | **7,326.1** | **20,988.5** | **7,123.2** |


## Tests

```bash
clj -M:test
```
