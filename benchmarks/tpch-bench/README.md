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
36 GiB RAM and macOS 26.6.2. Datalevin was measured on 2026-11-05, SQLite on
2026-11-04, and PostgreSQL on 2026-11-03. Versions: Datalevin 1.2.10
development checkout on Clojure 1.12.6 and Java 21.0.13.0.3 (8 GiB heap),
SQLite 3.51.1, and PostgreSQL 18.6.
PostgreSQL used 128 MiB shared buffers, 4 MiB work memory, and up to two parallel
workers per gather.

Times below are from the second complete pass, with warmup and measurement in
separate JVMs. Datalevin and SQLite were rebuilt from the same generated SF1
files before verification and warmup. PostgreSQL was restarted before its
warmup and kept running between passes; the OS filesystem cache was not purged.

Units are milliseconds; lower is better. PostgreSQL times include planning and
execution; Datalevin and SQLite also include consuming results. All 22
Datalevin queries passed the SQLite correctness check.

| Query | Datalevin (ms) | SQLite (ms) | PostgreSQL (ms) |
|---|---:|---:|---:|
| Q1 | 3,087.1 | 3,753.3 | 918.1 |
| Q2 | 124.2 | 144.5 | 179.0 |
| Q3 | 506.2 | 542.9 | 207.8 |
| Q4 | 186.7 | 134.1 | 110.9 |
| Q5 | 301.3 | 327.0 | 338.0 |
| Q6 | 361.0 | 337.9 | 129.7 |
| Q7 | 322.8 | 4,016.8 | 372.7 |
| Q8 | 76.4 | 125.7 | 146.3 |
| Q9 | 1,137.8 | 1,490.1 | 1,168.7 |
| Q10 | 303.7 | 1,019.7 | 193.6 |
| Q11 | 73.0 | 35.5 | 76.0 |
| Q12 | 375.3 | 876.5 | 236.6 |
| Q13 | 716.4 | 1,063.9 | 322.5 |
| Q14 | 142.7 | 4,767.9 | 133.3 |
| Q15 | 216.6 | 347.1 | 145.0 |
| Q16 | 246.0 | 105.1 | 110.0 |
| Q17 | 40.0 | 51.0 | 485.4 |
| Q18 | 1,402.9 | 554.7 | 1,499.3 |
| Q19 | 59.4 | 29.0 | 28.8 |
| Q20 | 47.1 | 69.3 | 166.9 |
| Q21 | 1,105.2 | 1,447.6 | 269.8 |
| Q22 | 94.8 | 38.9 | 34.7 |
| **Total** | **10,926.5** | **21,278.4** | **7,273.2** |


[Optimizer validation and comparison runs](../../doc/measurements/tpch-or-branches-2026-11-05/README.md)
include the Q19 branch plans and the complete correctness report.

## Tests

```bash
clj -M:test
```
