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
from `data/tbl/`. Rebuild databases when changing incompatible storage versions.

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

## Tests

```bash
clj -M:test
```
