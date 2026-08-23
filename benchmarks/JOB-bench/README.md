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

The Datalevin column was regenerated on 2026-08-23 from two separate
runner/JVM processes: one complete warmup pass followed by one complete
measurement pass. Both used Clojure 1.12.5 with direct linking enabled. The
113-query correctness suite passed before measurement. The retained PostgreSQL
and SQLite columns were not rerun.

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
|PostgreSQL|171.3|
|SQLite|295.0 (9 timeouts)|
|Datalevin|40.4|

PostgreSQL's recorded total is 4.24X Datalevin's. SQLite's non-timeout subtotal
alone is 7.30X Datalevin's, and understates SQLite's actual total because nine
queries reached the one-minute limit.

SQLite took extremely long to run some queries, so we had to put in a
one-minute timeout for each query. In the end, 9 queries timed out, meaning the
actual total time would be much higher.

Numbers below are in milliseconds. The raw data of query time is the following
table:

|Query|PostgreSQL (ms)|SQLite (ms)|Datalevin (ms)|
|---|---|---|---|
|1a|34.6|136.9|20.7|
|1b|29.7|137.0|17.1|
|1c|44.8|124.1|25.3|
|1d|29.8|141.2|9.4|
|2a|281.2|93.9|52.8|
|2b|267.5|86.6|19.3|
|2c|248.2|87.9|3.7|
|2d|377.3|117.3|24.3|
|3a|114.7|161.1|102.4|
|3b|65.0|116.5|13.5|
|3c|201.7|199.3|300.7|
|4a|69.3|234.3|383.3|
|4b|62.8|57.0|14.0|
|4c|69.6|389.3|865.0|
|5a|56.4|100.6|54.1|
|5b|52.1|74.5|326.8|
|5c|77.0|91.2|578.8|
|6a|13.2|283.8|15.1|
|6b|162.6|415.7|31.4|
|6c|4.2|277.1|7.4|
|6d|4819.4|1514.0|326.4|
|6e|8.2|284.3|10.6|
|6f|4808.5|2323.3|793.0|
|7a|670.8|915.6|102.3|
|7b|158.5|103.2|48.6|
|7c|1428.4|4898.8|825.6|
|8a|864.8|2292.3|47.6|
|8b|68.8|361.2|48.7|
|8c|2947.2|timeout|3876.4|
|8d|1414.7|timeout|322.9|
|9a|112.2|6623.3|287.1|
|9b|106.5|2668.2|346.2|
|9c|259.7|37069.9|339.4|
|9d|1872.9|37808.4|735.2|
|10a|239.9|6144.5|300.0|
|10b|105.2|120.7|77.2|
|10c|7343.5|timeout|2127.6|
|11a|36.5|430.3|63.8|
|11b|11.4|16.8|22.5|
|11c|575.9|209.5|362.3|
|11d|72.2|335.7|103.3|
|12a|123.7|3611.9|170.5|
|12b|32.4|1904.9|230.9|
|12c|356.2|4562.1|302.7|
|13a|553.6|1230.6|924.9|
|13b|234.9|1233.5|315.7|
|13c|229.0|1186.3|745.7|
|13d|865.2|3804.8|1204.7|
|14a|231.7|372.1|311.3|
|14b|82.3|122.8|56.9|
|14c|477.0|473.3|214.5|
|15a|134.9|2762.0|468.9|
|15b|18.2|16.2|33.9|
|15c|269.6|timeout|534.3|
|15d|318.4|timeout|489.7|
|16a|130.7|108.8|65.9|
|16b|9768.6|10652.8|2388.1|
|16c|833.5|519.9|300.5|
|16d|678.2|423.1|197.7|
|17a|7469.3|2392.8|587.0|
|17b|6326.7|4321.2|384.1|
|17c|6293.4|4238.8|28.8|
|17d|6376.2|4367.1|246.0|
|17e|7469.8|4338.8|1985.2|
|17f|7533.8|5704.0|433.6|
|18a|2661.6|16368.2|686.0|
|18b|129.0|534.3|517.5|
|18c|3431.9|18014.1|532.5|
|19a|141.6|4439.3|571.5|
|19b|61.9|745.4|102.8|
|19c|963.2|19589.7|270.8|
|19d|2015.6|20814.6|1556.5|
|20a|2293.5|3622.3|325.2|
|20b|1832.4|2577.1|116.0|
|20c|986.9|2225.5|107.4|
|21a|38.0|101.5|65.2|
|21b|25.6|87.0|38.0|
|21c|26.9|119.8|87.7|
|22a|308.8|1464.5|255.2|
|22b|278.5|1171.3|66.1|
|22c|999.1|2138.9|336.4|
|22d|650.8|3041.8|109.1|
|23a|119.4|timeout|195.2|
|23b|44.1|timeout|47.6|
|23c|174.8|timeout|542.5|
|24a|310.9|5528.2|112.7|
|24b|41.8|23.4|65.3|
|25a|1732.1|1715.5|580.6|
|25b|188.2|798.3|46.3|
|25c|6123.7|5810.5|214.7|
|26a|11369.5|2271.8|241.5|
|26b|165.9|1235.1|93.5|
|26c|36105.7|1337.7|111.6|
|27a|41.5|313.5|103.8|
|27b|31.7|204.0|89.6|
|27c|44.5|247.8|114.0|
|28a|492.0|1281.8|398.8|
|28b|349.8|543.1|197.9|
|28c|550.0|timeout|283.0|
|29a|56.0|11.4|870.9|
|29b|48.3|8.1|904.1|
|29c|177.3|429.3|1104.6|
|30a|2797.8|2429.6|595.0|
|30b|194.4|2317.5|109.0|
|30c|4427.0|2006.6|271.5|
|31a|704.0|1076.5|105.6|
|31b|260.4|999.2|65.4|
|31c|709.2|1392.7|185.8|
|32a|4.9|59.8|5.7|
|32b|60.3|57.2|29.3|
|33a|50.5|27.1|106.5|
|33b|47.2|22.7|92.5|
|33c|66.9|38.0|111.2|

### Planning time

|DB|Mean|Min|Median|Max|
|---|---|---|---|---|
|PostgreSQL|9.2 |0.2 |2.3 |48.8 |
|Datalevin|68.7 |2.3 |30.3 |1079.8 |

Datalevin spent 7.761 seconds in planning, or 19.2% of its 40.392-second
reported total. Planning remains smaller than execution, but is material in
this run.

SQLite doesn't report any internal timing.

### Execution time

|DB|Mean|Min|Median|Max|
|---|---|---|---|---|
|PostgreSQL|1507.0 |3.5 |227.1 |36075.3 |
|SQLite|2836.9 |8.1 |644.2 |37808.4 |
|Datalevin|288.8 |0.2 |86.7 |3865.7 |

PostgreSQL's mean execution time is 5.2X Datalevin's, and SQLite's recorded
mean is 9.8X Datalevin's. Their medians are 2.6X and 7.4X Datalevin's,
respectively. The maximum Datalevin execution time is 3.866 seconds, versus
36.075 seconds for PostgreSQL and 37.808 seconds among SQLite's completed
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
