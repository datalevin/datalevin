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

The PostgreSQL and SQLite columns were regenerated on 2026-08-23. The Datalevin
column was regenerated on 2026-09-01 at revision `e0152efe` after the query
engine and DLMDB changes. Each system ran one complete warmup pass followed by
one complete measurement pass in a separate runner/JVM process. The Datalevin
and JDBC runners used Clojure 1.12.5; Datalevin used direct linking.

Before the retained PostgreSQL pair, the macOS filesystem cache was purged and
PostgreSQL was restarted to clear `shared_buffers`; its first pass therefore
established the warm state used by the measurement pass. Before measurement,
SQLite was rebuilt from the source CSV files and all 21 table row counts were
verified to match PostgreSQL exactly.

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
|PostgreSQL|128.2|
|SQLite|281.8 completed-query subtotal (9 timeouts)|
|Datalevin|38.1|

PostgreSQL's recorded total is 3.37X Datalevin's. SQLite's non-timeout subtotal
alone is 7.40X Datalevin's. Counting each timed-out query at only its 60-second
cutoff gives SQLite a lower bound of 821.8 seconds, or 21.59X Datalevin's; its
actual total is higher.

SQLite took extremely long to run some queries, so we had to put in a
one-minute timeout for each query. In the end, 9 queries timed out, meaning the
actual total time would be much higher.

Numbers below are in milliseconds. The raw data of query time is the following
table:

|Query|PostgreSQL (ms)|SQLite (ms)|Datalevin (ms)|
|---|---|---|---|
|1a|36.8|138.7|11.9|
|1b|30.6|134.5|9.3|
|1c|30.2|125.1|7.8|
|1d|28.3|141.2|8.2|
|2a|279.1|91.2|49.2|
|2b|267.9|89.1|17.7|
|2c|252.9|86.0|3.8|
|2d|380.5|113.2|22.5|
|3a|111.4|143.9|88.3|
|3b|63.6|116.2|12.6|
|3c|197.2|196.1|214.5|
|4a|67.9|232.2|355.3|
|4b|62.5|57.7|13.6|
|4c|66.6|392.0|749.9|
|5a|52.8|77.5|54.0|
|5b|48.7|76.6|310.8|
|5c|71.6|89.9|637.8|
|6a|12.1|277.4|12.9|
|6b|157.1|387.2|23.1|
|6c|3.8|274.5|7.5|
|6d|4733.8|1533.0|307.1|
|6e|8.2|280.2|10.5|
|6f|5124.3|2319.5|826.2|
|7a|646.1|460.9|86.1|
|7b|157.7|105.8|36.4|
|7c|2007.6|4482.2|720.0|
|8a|822.4|2273.5|45.1|
|8b|66.1|350.1|48.9|
|8c|1796.0|timeout|3447.4|
|8d|671.2|timeout|313.6|
|9a|111.2|6804.3|371.2|
|9b|106.8|2759.1|231.0|
|9c|182.7|38617.0|302.8|
|9d|2121.7|38763.4|722.3|
|10a|240.9|356.1|308.0|
|10b|104.3|119.6|67.9|
|10c|6990.9|timeout|2006.2|
|11a|33.1|58.3|61.7|
|11b|12.3|17.1|23.4|
|11c|588.4|114.7|300.8|
|11d|85.3|336.9|138.1|
|12a|117.0|1444.0|150.9|
|12b|34.6|1863.3|229.3|
|12c|324.7|1994.0|310.2|
|13a|554.3|408.0|918.7|
|13b|233.3|1246.4|1039.7|
|13c|225.9|1216.9|579.3|
|13d|915.6|2822.2|1262.8|
|14a|226.8|203.9|239.3|
|14b|77.8|122.1|42.1|
|14c|461.0|373.8|163.4|
|15a|126.4|2616.3|525.7|
|15b|18.9|16.4|29.5|
|15c|236.0|timeout|484.8|
|15d|286.7|timeout|475.5|
|16a|123.9|47.8|55.5|
|16b|10838.4|8296.8|2328.1|
|16c|951.4|525.4|301.4|
|16d|760.9|409.0|184.0|
|17a|7460.0|2392.3|649.6|
|17b|6352.3|4310.4|346.4|
|17c|6188.4|4287.6|27.0|
|17d|6176.2|4432.7|234.8|
|17e|7847.0|4304.8|2115.8|
|17f|7423.6|5688.9|423.6|
|18a|2579.6|16852.7|474.2|
|18b|130.7|540.1|416.8|
|18c|3322.3|17992.1|312.5|
|19a|119.2|4472.9|406.2|
|19b|58.0|745.5|82.0|
|19c|906.5|19694.4|231.2|
|19d|2018.8|20813.3|1338.2|
|20a|2231.1|3596.2|247.4|
|20b|1794.5|935.9|109.9|
|20c|965.7|2267.9|119.6|
|21a|38.2|102.0|50.0|
|21b|25.8|88.0|36.3|
|21c|28.1|121.5|71.0|
|22a|300.8|1286.5|293.5|
|22b|271.0|1172.3|59.0|
|22c|983.2|2175.0|237.7|
|22d|696.2|3077.4|204.8|
|23a|120.9|timeout|197.8|
|23b|40.4|timeout|47.1|
|23c|200.9|timeout|464.7|
|24a|300.7|6360.2|83.0|
|24b|43.6|23.8|61.5|
|25a|1676.4|1791.8|508.6|
|25b|185.3|806.8|41.5|
|25c|5964.0|6082.1|229.6|
|26a|647.1|2335.1|274.6|
|26b|159.2|1300.0|87.4|
|26c|1203.2|1417.1|111.9|
|27a|38.7|318.4|90.3|
|27b|32.2|209.1|80.0|
|27c|43.3|252.8|104.3|
|28a|468.7|1325.6|330.9|
|28b|272.8|552.5|241.9|
|28c|331.5|timeout|165.5|
|29a|56.0|9.3|827.6|
|29b|48.1|8.0|809.4|
|29c|113.7|422.2|960.9|
|30a|4041.8|2450.7|506.3|
|30b|812.9|2396.3|101.8|
|30c|6106.4|2111.3|234.2|
|31a|803.3|1082.2|98.3|
|31b|267.6|1001.8|64.8|
|31c|733.6|1430.0|236.3|
|32a|4.4|60.6|5.6|
|32b|59.7|62.8|27.3|
|33a|53.8|27.7|97.2|
|33b|55.9|23.7|87.4|
|33c|81.2|38.8|102.4|

### Planning time

|DB|Mean|Min|Median|Max|
|---|---|---|---|---|
|PostgreSQL|9.4 |0.3 |2.5 |49.3 |
|Datalevin|59.2 |2.5 |27.1 |947.9 |

Datalevin spent 6.689 seconds in planning, or 17.6% of its 38.073-second
reported total. Planning remains smaller than execution, but is material in
this run.

SQLite doesn't report any internal timing.

### Execution time

|DB|Mean|Min|Median|Max|
|---|---|---|---|---|
|PostgreSQL|1125.4 |3.3 |224.8 |10836.2 |
|SQLite|2710.1 |8.0 |493.1 |38763.4 |
|Datalevin|277.7 |0.2 |91.8 |3437.3 |

PostgreSQL's mean execution time is 4.1X Datalevin's, and SQLite's recorded
mean is 9.8X Datalevin's. Their medians are 2.4X and 5.4X Datalevin's,
respectively. The maximum Datalevin execution time is 3.437 seconds, versus
10.836 seconds for PostgreSQL and 38.763 seconds among SQLite's completed
queries.

## Remarks

For these complex queries, execution accounts for 82.4% of Datalevin's
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
