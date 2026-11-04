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

Verification checks table counts, result values, ordering, and limits. Monetary
aggregates use a relative tolerance of `1e-6`; identifiers, counts, and quantity
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

Scale factor 1 (6,001,215 lineitems), measured on 2026-11-03 on a 12-core Apple
M3 Pro with 36 GiB RAM and macOS 26.6.2. Versions: Datalevin 1.2.10 development
checkout on Java 21 (8 GiB heap), SQLite 3.51.1, and PostgreSQL 18.6.
PostgreSQL used 128 MiB shared buffers, 4 MiB work memory, and up to two parallel
workers per gather.

Times below are from the second complete pass, with warmup and measurement in
separate JVMs. PostgreSQL was restarted before warmup and kept running between
passes; the OS filesystem cache was not purged.

Units are milliseconds; lower is better. PostgreSQL times include planning and
execution; Datalevin and SQLite also include consuming results. All 22
Datalevin queries passed the SQLite correctness check.

| Query | Datalevin (ms) | SQLite (ms) | PostgreSQL (ms) |
|---|---:|---:|---:|
| Q1 | 3,169.6 | 3,812.3 | 918.1 |
| Q2 | 172.7 | 144.8 | 179.0 |
| Q3 | 483.6 | 543.9 | 207.8 |
| Q4 | 202.9 | 134.5 | 110.9 |
| Q5 | 504.4 | 346.7 | 338.0 |
| Q6 | 470.2 | 343.9 | 129.7 |
| Q7 | 384.9 | 4,094.6 | 372.7 |
| Q8 | 112.7 | 127.2 | 146.3 |
| Q9 | 1,279.9 | 1,550.3 | 1,168.7 |
| Q10 | 338.4 | 1,039.0 | 193.6 |
| Q11 | 94.8 | 37.2 | 76.0 |
| Q12 | 358.2 | 859.7 | 236.6 |
| Q13 | 749.7 | 1,035.5 | 322.5 |
| Q14 | 178.2 | 4,746.7 | 133.3 |
| Q15 | 177.4 | 345.6 | 145.0 |
| Q16 | 281.3 | 105.1 | 110.0 |
| Q17 | 49.9 | 50.7 | 485.4 |
| Q18 | 1,653.3 | 555.6 | 1,499.3 |
| Q19 | 100.8 | 28.8 | 28.8 |
| Q20 | 60.2 | 68.8 | 166.9 |
| Q21 | 840.1 | 1,448.5 | 269.8 |
| Q22 | 103.5 | 37.9 | 34.7 |
| **Total** | **11,766.9** | **21,457.2** | **7,273.2** |

## Tests

```bash
clj -M:test
```
