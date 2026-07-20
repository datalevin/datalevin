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
|PostgreSQL|171|
|SQLite|295 (9 timeouts)|
|Datalevin|72|

Datalevin is about 2.4X faster than PostgreSQL and 4.1X faster than SQLite on
the recorded query time for these complex queries.

SQLite took extremely long to run some queries, so we had to put in a
one-minute timeout for each query. In the end, 9 queries timed out, meaning the
actual total time would be much higher.

Numbers below are in milliseconds. The raw data of query time is the following
table:

|Query|PostgreSQL (ms)|SQLite (ms)|Datalevin (ms)|
|---|---|---|---|
|1a|34.6|136.9|26.0|
|1b|29.7|137.0|9.1|
|1c|44.8|124.1|8.0|
|1d|29.8|141.2|8.4|
|2a|281.2|93.9|156.8|
|2b|267.5|86.6|123.0|
|2c|248.2|87.9|4.8|
|2d|377.3|117.3|113.6|
|3a|114.7|161.1|162.2|
|3b|65.0|116.5|13.7|
|3c|201.7|199.3|1136.2|
|4a|69.3|234.3|743.9|
|4b|62.8|57.0|27.8|
|4c|69.6|389.3|3073.2|
|5a|56.4|100.6|154.1|
|5b|52.1|74.5|328.7|
|5c|77.0|91.2|510.3|
|6a|13.2|283.8|12.9|
|6b|162.6|415.7|25.8|
|6c|4.2|277.1|7.1|
|6d|4819.4|1514.0|1149.9|
|6e|8.2|284.3|11.9|
|6f|4808.5|2323.3|1548.5|
|7a|670.8|915.6|163.9|
|7b|158.5|103.2|51.3|
|7c|1428.4|4898.8|1385.3|
|8a|864.8|2292.3|87.1|
|8b|68.8|361.2|152.8|
|8c|2947.2|timeout|5673.8|
|8d|1414.7|timeout|493.9|
|9a|112.2|6623.3|525.9|
|9b|106.5|2668.2|462.9|
|9c|259.7|37069.9|641.6|
|9d|1872.9|37808.4|1056.1|
|10a|239.9|6144.5|671.0|
|10b|105.2|120.7|136.9|
|10c|7343.5|timeout|3329.6|
|11a|36.5|430.3|67.1|
|11b|11.4|16.8|22.9|
|11c|575.9|209.5|430.3|
|11d|72.2|335.7|328.1|
|12a|123.7|3611.9|257.0|
|12b|32.4|1904.9|348.5|
|12c|356.2|4562.1|429.4|
|13a|553.6|1230.6|1322.3|
|13b|234.9|1233.5|358.4|
|13c|229.0|1186.3|981.6|
|13d|865.2|3804.8|2686.0|
|14a|231.7|372.1|469.6|
|14b|82.3|122.8|63.1|
|14c|477.0|473.3|383.1|
|15a|134.9|2762.0|454.4|
|15b|18.2|16.2|31.2|
|15c|269.6|timeout|592.5|
|15d|318.4|timeout|469.9|
|16a|130.7|108.8|169.5|
|16b|9768.6|10652.8|4407.6|
|16c|833.5|519.9|403.4|
|16d|678.2|423.1|321.4|
|17a|7469.3|2392.8|3412.8|
|17b|6326.7|4321.2|918.2|
|17c|6293.4|4238.8|58.2|
|17d|6376.2|4367.1|1591.2|
|17e|7469.8|4338.8|3500.4|
|17f|7533.8|5704.0|1672.3|
|18a|2661.6|16368.2|1094.5|
|18b|129.0|534.3|858.8|
|18c|3431.9|18014.1|630.9|
|19a|141.6|4439.3|635.4|
|19b|61.9|745.4|279.9|
|19c|963.2|19589.7|730.6|
|19d|2015.6|20814.6|2649.6|
|20a|2293.5|3622.3|769.0|
|20b|1832.4|2577.1|316.0|
|20c|986.9|2225.5|295.4|
|21a|38.0|101.5|75.4|
|21b|25.6|87.0|36.2|
|21c|26.9|119.8|108.3|
|22a|308.8|1464.5|447.6|
|22b|278.5|1171.3|121.9|
|22c|999.1|2138.9|413.2|
|22d|650.8|3041.8|197.7|
|23a|119.4|timeout|364.6|
|23b|44.1|timeout|67.7|
|23c|174.8|timeout|930.2|
|24a|310.9|5528.2|167.7|
|24b|41.8|23.4|52.6|
|25a|1732.1|1715.5|892.2|
|25b|188.2|798.3|26.1|
|25c|6123.7|5810.5|844.7|
|26a|11369.5|2271.8|364.7|
|26b|165.9|1235.1|117.2|
|26c|36105.7|1337.7|1235.5|
|27a|41.5|313.5|84.1|
|27b|31.7|204.0|179.8|
|27c|44.5|247.8|124.7|
|28a|492.0|1281.8|1139.4|
|28b|349.8|543.1|311.3|
|28c|550.0|timeout|283.1|
|29a|56.0|11.4|696.3|
|29b|48.3|8.1|601.5|
|29c|177.3|429.3|774.9|
|30a|2797.8|2429.6|945.6|
|30b|194.4|2317.5|98.9|
|30c|4427.0|2006.6|607.3|
|31a|704.0|1076.5|310.5|
|31b|260.4|999.2|54.9|
|31c|709.2|1392.7|242.5|
|32a|4.9|59.8|7.2|
|32b|60.3|57.2|48.9|
|33a|50.5|27.1|80.7|
|33b|47.2|22.7|69.3|
|33c|66.9|38.0|79.5|

### Planning time

|DB|Mean|Min|Median|Max|
|---|---|---|---|---|
|PostgreSQL|9.2 |0.2 |2.3 |48.8 |
|Datalevin|58.5 |3.7 |31.4 |719.9 |

Datalevin spent more time than PostgreSQL on query planning. However, the
planning time can be seen as rounding error compared with execution time.

SQLite doesn't report any internal timing.

### Execution time

|DB|Mean|Min|Median|Max|
|---|---|---|---|---|
|PostgreSQL|1507.0 |3.5 |227.1 |36075.3 |
|SQLite|2836.9 |8.1 |644.2 |37808.4 |
|Datalevin|577.0 |0.2 |258.7 |5657.4 |

On average, Datalevin is 2.6X faster than PostgreSQL and 4.9X faster than SQLite
in plan execution. The median times are similar between PostgreSQL and
Datalevin, but the differences are mainly in the extrema. The best plan in
Datalevin can be an order of magnitude faster, while the worst plans in
PostgreSQL and SQLite can be more than 5X slower than the worst plan in
Datalevin.

## Remarks

For these complex queries, planning time is insignificant compared with the long
execution time. The quality of the plans generated by planner determines the
overall query time differences.

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
