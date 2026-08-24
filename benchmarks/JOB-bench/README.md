# Join Order Benchmark (JOB)

[JOB](https://github.com/gregrahn/join-order-benchmark) is a standard SQL
benchmark that stresses database query optimizers, as described in the
influential paper:

Viktor Leis, et al. "How Good Are Query Optimizers, Really?" PVLDB Volume 9, No.
3, 2015 [pdf](http://www.vldb.org/pvldb/vol9/p204-leis.pdf)

This benchmark uses real world data set, and is extremely challenging, compared
with other benchmarks, such as TPC series. We ported this benchmark to Datalog
to see how Datalevin handle complex queries.

## Data Set

The data set is originally from Internet Movie Database
[IMDB](https://developer.imdb.com/non-commercial-datasets/), downloaded in
May 2013. The exported CSV files of the data set can be downloaded from
https://event.cwi.nl/da/job/imdb.tgz

Unpack the downloaded `imdb.tgz` to obtain 21 CSV files, totaling 3.7 GiB. Each
CSV file is a table. The data is highly normalized, with many foreign key
references. The biggest table has over 36 million rows, while the smallest
has only 4 rows.

### PostgreSQL

Assume a PostgreSQL server is running on localhost:5432.

This program loads CSV data, creates schema, creats indexes, and runs ANALYZE:

```bash
clj -Xpg-db
```

### SQLite

Similarly, this program loads CSV data into a local `sqlite.db` file, creates
schema, creates indexes, and runs ANALYZE:

```bash
clj -Xsqlite-db
```

### Datalevin

We translated the SQL schema to equivalent Datalevin
[schema](src/datalevin_bench/core.clj), as shown in `datalevin-bench.core`
namespace. The attribute names follow Clojure convention.

This program loads the same set of CSV files into Datalevin, then runs analyze
function:

```bash
clj -Xdb
```

This loads 277,878,411 datoms into Datalevin. Datalevin creates indexes for
everything by default, so there is no separate indexing step.

On the benchmark machine described below, a clean build with the JVM's
automatically selected heap completed with these timings:

|Phase|Time (seconds)|
|---|---:|
|Load CSV data and build indexes|501.0|
|Analyze|11.4|
|End-to-end wall clock time|517.2|

The resulting database occupies 17 GB. The run used the default 2,097,152-datom
fill batch and did not specify `-Xmx`.

## Queries

The `queries` directory contains 113 SQL queries for this benchmark. These
queries all involve more than 5 tables and often have 10 or more where clauses,

We manually translated the SQL queries to equivalent Datalevin queries, and
manually verified that PostgreSQL and Datalevin produce exactly the same results
for the same query (Note 1).

For example, the query 1b of the benchmark:

```SQL
SELECT MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx,
     title AS t
WHERE ct.kind = 'production companies'
  AND it.info = 'bottom 10 rank'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND t.production_year BETWEEN 2005 AND 2010
  AND ct.id = mc.company_type_id
  AND t.id = mc.movie_id
  AND t.id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;
```
is translated into the equivalent Datalevin query:

```Clojure
'[:find (min ?mc.note) (min ?t.title) (min ?t.production-year)
  :where
  [?ct :company-type/kind "production companies"]
  [?it :info-type/info "bottom 10 rank"]
  [?mc :movie-companies/note ?mc.note]
  [(not-like ?mc.note "%(as Metro-Goldwyn-Mayer Pictures)%")]
  [?t :title/production-year ?t.production-year]
  [(<= 2005 ?t.production-year 2010)]
  [?mc :movie-companies/company-type ?ct]
  [?mc :movie-companies/movie ?t]
  [?mi :movie-info-idx/movie ?t]
  [?mi :movie-info-idx/info-type ?it]
  [?t :title/title ?t.title]]
```

Most queries in the benchmark are more complex than this example.

For Datalevin, both `lein` and `clj` build tools are needed, the former is for
building the main project, and the later for running tests and benchmarks.

Run tests to see results are correct.

```bash
clj -Xtest
```

## Run Benchmark

These software were tested on a MacBook Pro 16 inch Nov 2023, Apple M3 Pro chip,
6 performance cores and 6 efficiency cores, 36GB memory, 1TB SSD disk:

* Homebrew PostgreSQL@18
* SQLite (via sqlite-jdbc)
* Datalevin latest in this repository
* Clojure 1.12.5 on OpenJDK 21.0.11 for the current Datalevin run

All software were in default configuration without any tuning.

For PostgreSQL, run `clj -Xpg-bench` once to warm up, then run again. The
results of the second run were reported. The numbers were extracted from
PostgreSQL's own `EXPLAIN ANALYZE` results, and written into a CSV file
`postgres_onepass_time.csv`, in order to remove the impact of client/server
communication and other unrelated factors.

```bash
clj -Xpg-bench
clj -Xpg-bench
```

For SQLite, run `clj -Xsqlite-bench` once to warm up, then run again. Results
are written to `sqlite_onepass_time.csv`. Queries that exceed 60 seconds are
recorded as timeout.

```bash
clj -Xsqlite-bench
clj -Xsqlite-bench
```

Same as the above, we run `clj -Xbench` once to warm up. Then run it again to report the
results. The numbers were extracted from `explain` function results and written
into a CSV file `datalevin_onepass_time.csv`. The benchmark disables
intermediate cardinality counting in `explain` so per-tuple instrumentation does
not distort execution time.

```bash
lein run   # this runs tests to ensure this code base works
clj -Xbench
clj -Xbench
```

We did not run the same query repeatedly and then compute the median or average
for the query, because that would be mainly benchmarking caching behavior of the
databases, as they all have various caches. In this test, we are mainly
interested in the behavior of query optimizer.

## Results

The table below reports the second pass of each benchmark command described
above and is retained with the raw CSV files for reproducibility.

All three columns were regenerated on 2026-08-23. Each system ran one complete
warmup pass followed by one complete measurement pass in a separate runner/JVM
process. The Datalevin and JDBC runners used Clojure 1.12.5; Datalevin used
direct linking. The 113-query Datalevin correctness suite passed after the
runs. Before measurement, SQLite was rebuilt from the source CSV files and all
21 table row counts were verified to match PostgreSQL exactly. The complete
two-pass environment and artifacts are retained in the
[2026-08-23 run directory](results/baseline-postgres-sqlite-20260823/README.md).

We look at the timing results. The total query time can be divided into two
parts: query planning time and plan execution time. SQLite does not report
any timing on its own, so the benchmark collects only total query time. Raw data
files are the following:

* [PostgreSQL](postgres_onepass_time.csv)
* [SQLite](sqlite_onepass_time.csv)
* [Datalevin](datalevin_onepass_time.csv)

### Total query time

The table below sums the database-reported planning and execution times for all
113 queries. It excludes client startup and shutdown overhead:

|DB|Total Query Time (seconds)|
|---|---|
|PostgreSQL|129.4|
|SQLite|281.8 completed-query subtotal (9 timeouts)|
|Datalevin|40.4|

PostgreSQL's recorded total is 3.20X Datalevin's. SQLite's non-timeout subtotal
alone is 6.98X Datalevin's. Counting each timed-out query at only its 60-second
cutoff gives SQLite a lower bound of 821.8 seconds, or 20.35X Datalevin's; its
actual total is higher.

SQLite took extremely long to run some queries, so we had to put in a
one-minute timeout for each query. In the end, 9 queries timed out, meaning the
actual total time would be much higher.

Numbers below are in milliseconds. The raw data of query time is the following
table:

|Query|PostgreSQL (ms)|SQLite (ms)|Datalevin (ms)|
|---|---|---|---|
|1a|36.4|138.7|20.7|
|1b|30.3|134.5|17.1|
|1c|31.4|125.1|25.3|
|1d|30.1|141.2|9.4|
|2a|288.5|91.2|52.8|
|2b|274.6|89.1|19.3|
|2c|254.7|86.0|3.7|
|2d|385.0|113.2|24.3|
|3a|114.1|143.9|102.4|
|3b|62.3|116.2|13.5|
|3c|202.4|196.1|300.7|
|4a|68.5|232.2|383.3|
|4b|61.5|57.7|14.0|
|4c|67.2|392.0|865.0|
|5a|52.3|77.5|54.1|
|5b|49.0|76.6|326.8|
|5c|72.0|89.9|578.8|
|6a|12.4|277.4|15.1|
|6b|157.1|387.2|31.4|
|6c|3.7|274.5|7.4|
|6d|4712.5|1533.0|326.4|
|6e|7.9|280.2|10.6|
|6f|5176.2|2319.5|793.0|
|7a|667.6|460.9|102.3|
|7b|160.8|105.8|48.6|
|7c|2066.0|4482.2|825.6|
|8a|835.8|2273.5|47.6|
|8b|65.9|350.1|48.7|
|8c|1785.0|timeout|3876.4|
|8d|703.0|timeout|322.9|
|9a|109.6|6804.3|287.1|
|9b|105.1|2759.1|346.2|
|9c|179.7|38617.0|339.4|
|9d|2058.4|38763.4|735.2|
|10a|242.9|356.1|300.0|
|10b|106.2|119.6|77.2|
|10c|7266.9|timeout|2127.6|
|11a|39.1|58.3|63.8|
|11b|12.5|17.1|22.5|
|11c|604.7|114.7|362.3|
|11d|88.1|336.9|103.3|
|12a|126.3|1444.0|170.5|
|12b|35.1|1863.3|230.9|
|12c|352.3|1994.0|302.7|
|13a|567.1|408.0|924.9|
|13b|237.3|1246.4|315.7|
|13c|229.5|1216.9|745.7|
|13d|916.4|2822.2|1204.7|
|14a|227.8|203.9|311.3|
|14b|78.9|122.1|56.9|
|14c|467.0|373.8|214.5|
|15a|130.5|2616.3|468.9|
|15b|18.9|16.4|33.9|
|15c|235.7|timeout|534.3|
|15d|322.4|timeout|489.7|
|16a|126.2|47.8|65.9|
|16b|10618.1|8296.8|2388.1|
|16c|897.0|525.4|300.5|
|16d|728.3|409.0|197.7|
|17a|7463.7|2392.3|587.0|
|17b|6259.1|4310.4|384.1|
|17c|6225.4|4287.6|28.8|
|17d|6383.0|4432.7|246.0|
|17e|7883.5|4304.8|1985.2|
|17f|7784.9|5688.9|433.6|
|18a|2714.7|16852.7|686.0|
|18b|129.1|540.1|517.5|
|18c|3431.6|17992.1|532.5|
|19a|118.7|4472.9|571.5|
|19b|60.0|745.5|102.8|
|19c|931.7|19694.4|270.8|
|19d|2042.5|20813.3|1556.5|
|20a|2312.6|3596.2|325.2|
|20b|1782.8|935.9|116.0|
|20c|975.5|2267.9|107.4|
|21a|39.1|102.0|65.2|
|21b|26.1|88.0|38.0|
|21c|28.5|121.5|87.7|
|22a|302.1|1286.5|255.2|
|22b|270.2|1172.3|66.1|
|22c|1000.8|2175.0|336.4|
|22d|661.6|3077.4|109.1|
|23a|122.0|timeout|195.2|
|23b|45.0|timeout|47.6|
|23c|178.2|timeout|542.5|
|24a|314.0|6360.2|112.7|
|24b|43.9|23.8|65.3|
|25a|1680.2|1791.8|580.6|
|25b|185.3|806.8|46.3|
|25c|5984.3|6082.1|214.7|
|26a|696.2|2335.1|241.5|
|26b|162.2|1300.0|93.5|
|26c|1284.0|1417.1|111.6|
|27a|39.1|318.4|103.8|
|27b|33.1|209.1|89.6|
|27c|44.7|252.8|114.0|
|28a|480.2|1325.6|398.8|
|28b|276.9|552.5|197.9|
|28c|338.0|timeout|283.0|
|29a|59.3|9.3|870.9|
|29b|50.6|8.0|904.1|
|29c|119.8|422.2|1104.6|
|30a|4103.5|2450.7|595.0|
|30b|828.1|2396.3|109.0|
|30c|6052.8|2111.3|271.5|
|31a|704.7|1082.2|105.6|
|31b|256.4|1001.8|65.4|
|31c|709.0|1430.0|185.8|
|32a|5.1|60.6|5.7|
|32b|59.7|62.8|29.3|
|33a|53.3|27.7|106.5|
|33b|54.7|23.7|92.5|
|33c|80.8|38.8|111.2|

### Planning time

|DB|Mean|Min|Median|Max|
|---|---|---|---|---|
|PostgreSQL|9.7 |0.3 |2.4 |52.7 |
|Datalevin|68.7 |2.3 |30.3 |1079.8 |

Datalevin spent 7.761 seconds in planning, or 19.2% of its 40.392-second
reported total. Planning remains smaller than execution, but is material in
this run.

SQLite doesn't report any internal timing.

### Execution time

|DB|Mean|Min|Median|Max|
|---|---|---|---|---|
|PostgreSQL|1135.8 |3.3 |226.0 |10615.9 |
|SQLite|2710.1 |8.0 |493.1 |38763.4 |
|Datalevin|288.8 |0.2 |86.7 |3865.7 |

PostgreSQL's mean execution time is 3.9X Datalevin's, and SQLite's recorded
mean is 9.4X Datalevin's. Their medians are 2.6X and 5.7X Datalevin's,
respectively. The maximum Datalevin execution time is 3.866 seconds, versus
10.616 seconds for PostgreSQL and 38.763 seconds among SQLite's completed
queries.

## Remarks

For these complex queries, execution still accounts for 80.8% of Datalevin's
reported query time. The quality of the plans generated by the planner remains
the main determinant of the overall query-time differences, while planning
overhead is now large enough to report separately.

PostgreSQL's planning algorithm is based on statistics collected by separate
processes, so it is more expensive to maintain, at the same time, less
effective, due to its strong statistical assumptions that are almost never true
in real data.

SQLite's planner is more limited in its ability to handle complex multiple table
joins. It does exhaustive join order search only up to a limited number of
tables, and its statistics model is even weaker than PostgreSQL.

Datalevin's planning algorithm is based on a more realistic statistical
model and follows empirical Bayesian principles. While it is more
expensive to plan, the generated plans are of higher quality, resulting in
better overall query performance in handling complex queries. For more details
of Datalevin's planner, please see [documentation](../../doc/query.md).


## Notes

1. Manual verification is needed because PostgreSQL's `MIN` function uses
  locale-aware collation (e.g. `en_US.UTF-8`), which may differ from strict
  UTF-8 byte ordering used by Datalevin. For example, for query 1b, PostgreSQL
  `MIN(mc_note)` returns `"(as Grosvenor Park)"` under locale collation, but
  `"(Set Decoration Rentals)"` would be the correct answer based on strict UTF-8
  byte ordering. So we removed `MIN()` to obtain full results in order to verify
  that Datalevin produces exactly the same results as PostgreSQL before applying
  `MIN()`.
