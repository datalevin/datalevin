# Change Log

## WIP

### Added
- [WAL] Write Ahead Log (WAL) mode for both KV and Datalog transactions. When
  enabled, transaction returns success after WAL is sync'ed to disk. Disk sync
  in DLMDB is done later at checkpoint. This increases write speed while keeping
  durability guarantees.  WAL will be the bases for replication, crash recovery
  and high availability.
- [KV] `:inmemory` mode, where the env is entirely in memory and all data is
  lost on close. It has faster write than `:nosync`.

## 0.10.5 (2026-02-08)

### Added
- [Server] When server starts, it checks a `DATALEVIN_DEFAULT_PASSWORD`
  environment variable to set the default password for the `datalevin` root user
  [#351](https://github.com/juji-io/datalevin/issues/351).
- [Datalog] `:db.fn/patchIdoc` transaction function to directly update values in
  idoc.

### Improved
- [Datalog] Selective cache invalidation, so we don't throw away whole cache on
  transaction.
- [Datalog] Rewrite unused variables so that the query engine does not waste
  resources to resolve them.
- [Datalog] Enhance speed of `idoc-match` by using bitmaps for doc id set
  operations.
- [Datalog] Optimize performance of `idoc-match`, `vec-neighbors` and `fulltext`
  query function by returning tuples that only contain needed values.

## 0.10.4 (2026-02-03)

### Added
- [Datalog] Document database feature (idoc) supports storage and query of EDN,
  JSON and Markdown documents
  [#169](https://github.com/juji-io/datalevin/issues/169). Details in
  [doc](https://github.com/datalevin/datalevin/blob/master/doc/idoc.md)

### Improved
- [Datalog] Batch and sort input tuples so index scans can take advantage of
  DLMDB's forward seek optimization, resulting in about 12% query speedup.
- [Datalog] Improve handling of cyclic rules.
- [Datalog] Cache bound values in rule resolution context.

## 0.10.3 (2026-01-27)

### Improved
- [Datalog] Faster hash join execution using bitmap based sideway information
  passing (SIP) for `:_ref` type of hash join.
- [Datalog] More robust link ratio calculation using empirical-Bayes shrinkage
  and skew‑aware upper‑bound correction.
- [Benchmark] Added SQLite to JOB benchmark.

## 0.10.2 (2026-01-24)

### Improved
- [Datalog] Planner cost model adjustment.
- [Datalog] More robust samplers.

## 0.10.1 (2026-01-21)

### Changed
- [KV] KV storage is now [DLMDB](https://github.com/huahaiy/dlmdb), which has
  additional features of counted DB
  [#337](https://github.com/juji-io/datalevin/issues/337), prefix compression,
  dupsort iterator optimizations and other improvements.
- [KV] Default Env flag is now `#{:nordahead}`
- [KV] Default DBI flag is now `#{:create :counted :prefix-compression}`
- [KV] Default `:max-readers` is now 1024.
- [Datalog] Rewritten rule engine. Implemented a bottom-up SNE approach with
  magic set rewrites that solves OOM problem with the old engine
  [#170](https://github.com/juji-io/datalevin/issues/170), and this can be
  orders of magnitude faster than Datomic in recursive rule resolution, see
  [benchmark](benchmarks/math-bench).
- [Datalog] Removed VAE index. This may reduce DB size by 30% and cut write time
  by 50% for datasets with heavy `:db.type/ref` presence (e.g. JOB benchmark
  DB). VAE was only used in entity retraction and the replacement solution is
  less than 10% slower.
- [Platform] Native dependencies are mostly statically compiled and bundled in the
  release jars (with exception of libc, as glibc is much faster than musl).
- [Platform] Drop support for Intel macOS.
- [Platform] Minimal Java version is now 21.

### Added
- [Platform] Automatically upgrade DB from version 0.9.27 onwards when opening
  the DB. The migration may take a while, and it needs Internet access to
  download old uberjar. [#276](https://github.com/juji-io/datalevin/issues/276)
- [KV] Random access and rank lookup functions in O(log n) time for
  `:counted` DBIs (default).
      - `get-rank`, return the rank (0-based) of the key.
      - `get-by-rank`, return the key/value at the given rank.
- [KV] Sampling function `sample-kv` in O(log n) time for `:counted` DBIs
  (default). [#325](https://github.com/juji-io/datalevin/issues/325)
- [Datalog] Add `:new-attributes` to transaction report.
  [#301](https://github.com/juji-io/datalevin/issues/301).
- [Datalog] `:order-by` now accepts column indices (0-based) in addition to
  variable names, e.g. `:order-by [2 :desc 1 :asc]`, useful for sorting by
  aggregation results in `:find`.
- [Datalog] `:find` now allows arithmetic expression over aggregations, e.g. `(+
  sum(?x) sum(?y))`.
- [Datalog] Add `:having` clauses, similar to SQL `HAVING`. This allows
  conditions to be applied to results of aggregations, e.g. `:having [(pos? (sum
  ?x))] [(< (sum ?y) 100)]`.
- [Datalog] Optimizer now includes `or-join` as potential links between
  entities.
- [Datalog] Optimizer now considers hash join as an alternative to `:_ref` and
  `:val-eq` joins when the number of input tuples is greater than
  `datalevin.constants/hash-join-min-input-size` (default 20000). This enhances
  query robustness, as hash join handles inaccuracy in cardinality estimation
  better.
- [Datalog] Add `apply` as a query function
  [#344](https://github.com/juji-io/datalevin/issues/344).
- [Benchmark] [LDBC-SNB-bench](benchmarks/LDBC-SNB-bench) that compares
  Datalevin with Neo4j on graph queries.

### Improved
- [Datalog] 40% less DB footprint due to VAE index removal and prefix
  compression.
- [Datalog] Cut query planning time in half due to faster range counting and
  sampling of DLMDB.
- [Datalog] Reduced query execution time due to optimizer improvements and
  faster DLMDB iterators.
- [Datalog] More robust query execution: added error handling, timeout and
  back pressure to query execution pipeline; Pipelining now uses its own thread
  pool.
- [Platform] Deps update. Upgrade to GraalVM 25.

### Fixed
- [KV] Enable virtual threads usage by not reusing read only transactions
  [#326](https://github.com/juji-io/datalevin/issues/326).
- [Server] Segfault due to sampler reading a closed DB.
  [#347](https://github.com/juji-io/datalevin/issues/347)
- [Server] Faster code path for `pull` and `pull-many` on server
  [#322](https://github.com/juji-io/datalevin/issues/322).
- [Datalog] Unify, not overwrite, in function binding
  [#232](https://github.com/juji-io/datalevin/issues/232).
- [Datalog] Support nested `q` as a query function
  [#323](https://github.com/juji-io/datalevin/issues/323).
- [Datalog] Run query in the invoking thread when in write tx
  [#331](https://github.com/juji-io/datalevin/issues/331).
- [Datalog] Handle lookup ref in type coercion
  [#338](https://github.com/juji-io/datalevin/issues/338).
- [Datalog] Handle idents in tuple refs
  [#340](https://github.com/juji-io/datalevin/issues/340).
- [Datalog] Do not unify `_`
  [#341](https://github.com/juji-io/datalevin/issues/341).
- [Vector] Handle `:display :refs+dists` in `vec-neighbors`
  [#327](https://github.com/juji-io/datalevin/issues/327).
- [Async] Run callback in worker thread
  [#329](https://github.com/juji-io/datalevin/issues/329).


## 0.9.27 (2025-11-19)

## Added
- [KV] Write a `VERSION` file, otherwise the same as 0.9.22. This is to allow
  automatic migration from 0.9.27 to 0.10.0 and onwards.

## 0.9.22 (2025-03-18)

### Changed
- [Platform] Update minimal version of Java to 17.

### Improved
- [Vector] Defer closing of vector indices until after async executor is
  shutdown to avoid segment fault due to trying to save a closed index.

## 0.9.21 (2025-03-18)

### Added
- [Vector] `new-vector-index` function creates an on-disk index for equal-length
  dense numeric vectors to allow similarity search, and related functions
  `close-vector-index`, `clear-vector-index` and `vector-index-info`. Vector
  indexing is implemented with [usearch](https://github.com/unum-cloud/usearch).
- [Vector] Corresponding `add-vec`, `remove-vec`, and `search-vec` functions to
  work with vector index. Similar to full-text search, vector search also
  support domain semantics to allow grouping of vectors into domains.
- [Doc] [Vector doc](doc/vector.md). **Note**, you may need to install some
  native dependencies on your system to load Datalevin: OpenMP and Vector Math.
- [Datalog] New data type `:db.type/vec`, for which a vector index is
  automatically created for them to allow a query function `vec-neighbors` to
  return the datoms with neighboring vector values.
  [#145](https://github.com/juji-io/datalevin/issues/145)

###  Fixed
- [Datalog] Handle pathological case of redundant clauses about the same
  attribute. [#319](https://github.com/juji-io/datalevin/issues/319)
- [Datalog] Correctly handle certain case of attribute without value.
  [#320](https://github.com/juji-io/datalevin/issues/320)

### Improved
- [Datalog] Limit the time for counting datoms during planning with a dynamic
  var `datalevin.constants/range-count-time-budget` in milliseconds, default
  is 10; another dynamic var `datalevin.constants/range-count-iteration-step`
  determines after how many loop iterations to take a time measure, default
  is 1000.
- [Datalog] Same for sampling with `sample-time-budget` and
  `sample-iteratioin-step`, defaults are 2 milliseconds and 20000, respectively.
- [Datalog] Reduce the exhaustive plan space to P(n, 2).
- [Datalog] Add `:parsing-time` for query parsing and `:building-time` for query
  graph building to `explain` results.
- [Search] Dump data to file when `re-index` to save memory.
- [KV] Periodically flush to disk, with an interval set by dynamic var
  `datalevin.constants/lmdb-sync-interval` in seconds, default is 300. This also
  cleans up dead readers.
- [KV] Remove the semaphore for preventing "Environment maxreaders limit
  reached" error, as it hurts read performance. User should be careful with
  functions such as `pmap` and `future`for parallel read operation, as they use
  unbounded thread pools. User can use a semaphore to limit the number of read
  threads in flight, or use a bounded thread pool. If needed, `:max-readers` KV
  option can also be set to increase the limit (default is now doubled to
  256).
- [Doc] Add `built-ins` namespace for query functions to cljdoc.

## 0.9.20 (2025-02-19)

### Improved
- [Datalog] Able to disable background sampling.

## 0.9.19 (2025-02-19)

### Added
- [Search] Boolean search expression.
  [#104](https://github.com/juji-io/datalevin/issues/104)
  [#310](https://github.com/juji-io/datalevin/issues/310)
- [Search] Phrase search. [#311](https://github.com/juji-io/datalevin/issues/311)
- [Datalog] `analyze` function to collect statistics that helps with query
  planner.
- [Datalog] `max-eid` function to return the current maximal entity id.
- [Benchmark] [write-bench](benchmarks/write-bench) that compares Datalevin with
  SQLite on transaction performance, and studies Datalevin KV write
  performance under various conditions.

### Fixed
- [Datalog] `and` in `or-join` exception
  [#304](https://github.com/juji-io/datalevin/issues/304)
- [Datalog] `and` join exception.
  [#305](https://github.com/juji-io/datalevin/issues/305)
- [Datalog] Validation of negative doubles
  [#308](https://github.com/juji-io/datalevin/issues/308)
- [Datalog] `seek-datom` and `rseek-datom` broken for `:eav` index

### Improved
- [Datalog] `fill-db` no longer creates a new DB, to reduce chance of user
  errors. [#306](https://github.com/juji-io/datalevin/issues/306)
- [Datalog] Added arity to `seek-datoms` and `rseek-datoms` to specify the
  number of datoms desired. [Thx @jeremy302]
  [#312](https://github.com/juji-io/datalevin/issues/312)
- [Datalog] `datoms` function takes an extra `n` argument as well.
- [Datalog] Faster attribute size count by selecting different counters based on
  cardinality.
- [Datalog] Sort by cardinality when picking minimal attribute.
- [KV] Prevent "MDB_READERS_FULL: Environment maxreaders limit reached" error
  when running hundreds of concurrent reading threads.
- [Async] Resepct individual callback of each async call.

## 0.9.18 (2025-01-20)

### Added
- [KV] `set-env-flags` and `get-env-flags`, so users may change the env flags
  after the DB is open. This is useful for adjusting transaction durability
  settings on the fly.

### Fixed
- [Platform] Clojure compiler adds `java.util.SequencedCollection` automatically
  when compiled on JDK 21 and above, which does not exist in earlier JDK.
  Compile library with JDK 17 instead.
- [KV] Type validation and coercion for `:bytes`.

### Improved
- [Feature] Streamline async transactions so it respects env flags.
- [Datalog] Reduce default sample change ratio to `0.05`.

## 0.9.17 (2025-01-17)

### Improved
- [Doc] Remove test namespaces from cljdoc.

## 0.9.16 (2025-01-17)

### Changed
- [Platform] Drop Java 8 support. Minimum JVM is now 11.

### Improved
- [Doc] Fix cljdoc build.
- [Package] Exclude tests, test data, Java source files from releases.

## 0.9.15 (2025-01-17)

### Added
- [KV] `transanct-kv-async` function to return a future and transact in batches
  to enhance write throughput (2.5X or more higher throughput in heavy write
  condition compared with `transact-kv`). Batch size is automatically adjusted
  to the write workload: the higher the load, the larger the batch. Howver,
  manual batching of transaction data is still helpful, as the effects of
  automatic batching and manual batching add up.
  [#256](https://github.com/juji-io/datalevin/issues/256)

### Fixed
- [Datalog] Upsert fails on entity with unique composite tuple
  [#299](https://github.com/juji-io/datalevin/issues/299)

### Improved
- [Datalog] Both `transact` and `transact-async` are also changed to use the
  same adaptive batching transaction mechanism to enhance write throughout.
- [Datalog] Reduce default `sample-processing-interval` to 10 seconds, so
  samples are more up to date. Each invocation will do less work, or no work
  , based on a change ratio, controlled by a dynamic var `sample-change-ratio`,
  default is `0.1`, i.e. resample if 10 percent of an attribute's values
  changed.
- [Datalog] Port applicable fixes from Datascript up to 1.7.4.
- [KV] Consolidated LMDB bindings to a single binding using JavaCPP. Removed
  both LMDBJava and Graalvm native image specific bindings. This eases
  maintenance, enhances performance, and makes it easier to add native
  dependencies. [#35](https://github.com/juji-io/datalevin/issues/35).
- [KV] Pushed all LMDB iterators, counters, sampler, and comparator
  implementation down to C code.
  [#279](https://github.com/juji-io/datalevin/issues/279).
- [KV] Now it is optional to add `--add-opens` JVM options to open modules for
  Java 11 and above. If these JVM options are not set, Datalevin will use a
  safer but slower default option instead of throwing exceptions. It is still
  recommended to add these JVM options to get optimal performance. For now,
  native image uses only the safer option.
- [Native] Datalevin library jar can now be used directly to compile GraalVM
  native image. There's no longer a need for a GraalVM specific Datalevin
  library, nor GraalVM version restriction.
- [Native] Upgrade to the latest version of GraalVM .
- [Server] Handle rare cases of exceptions in event loop.

## 0.9.14 (2024-11-25)

### Added
- [KV] `get-first-n` to return first `n` key-values in a key range.
- [KV] `list-range-first-n` to return first `n` key-values in a key-value range
  of a list DBI. [#298](https://github.com/juji-io/datalevin/issues/298)

### Fixed
- [Datalog] Exception in or-join. [#295](https://github.com/juji-io/datalevin/issues/295)

### Improved
- [Datalog] Faster `init-db`, `fill-db`, `re-index` and `load` by writing in
  `:nosync` mode and syncing only at the end.
- [Datalog] Push down nested predicates as well.
- [Datalog] Remove `c/plan-space-reduction-threshold`, always use `P(n, 3)`
  instead, as only initial 2 joins have accurate size estimation, so larger plan
  space for later steps is not really beneficial. This change improves JOB.
- [Datalog] Many small code optimizations to improve query speed about 10%.

## 0.9.13 (2024-11-09)

### Fixed
- [Datalog] Repeated cardinality many attributes for the same entity.
  [#284](https://github.com/juji-io/datalevin/issues/284)
- [Datalog] Apply type coercion according to scheam to ensure correct storage of
  values. [#285](https://github.com/juji-io/datalevin/issues/285)
- [Datalog] Correct query caching when referenced content may have changed.
  [#288](https://github.com/juji-io/datalevin/issues/288)
  [Thx @andersmurphy]
- [Pod] Typos in search code.
  [#291](https://github.com/juji-io/datalevin/issues/291)

### Improved
- [Pod] Consistent `entity` behavior in pod as in JVM.
  [#283](https://github.com/juji-io/datalevin/issues/283)
- [Datalog] Allow `:offset 0`.
- [Datalog] Implement `empty` on Datom so it can be walked.
  [#286](https://github.com/juji-io/datalevin/issues/286)
- [Datalog] Query functions resolve their arguments recursively.
  [#287](https://github.com/juji-io/datalevin/issues/287)
- [Native] Remove `:aot` to avoid potential dependency conflict.

## 0.9.12 (2024-10-08)

### Added
- [Datalog] `:offset` and `:limit` support,
  [#126](https://github.com/juji-io/datalevin/issues/126),
  [#117](https://github.com/juji-io/datalevin/issues/117)
- [Datalog] `:order-by` support,
  [#116](https://github.com/juji-io/datalevin/issues/116)
- [Datalog] `count-datoms` function to return the number of datoms of a pattern
- [Datalog] `cardinality` function to return the number of unique values of an
  attribute

### Improved
- [KV] Added version number to kv-info, in preparation for auto-migration.
- [Datalog] Cache query results by default, can use dynamic var `q/*cache?*` to
  turn it off.

## 0.9.11 (2024-10-04)

### Fixed
- [Datalog] Empty results after querying empty database before transact. [#269](https://github.com/juji-io/datalevin/issues/269)
- [Datalog] Handle multiple variables assigned to the same cardinality many
  attribute. [#272](https://github.com/juji-io/datalevin/issues/272)
- [Datalog] Return maps regression. [#273](https://github.com/juji-io/datalevin/issues/273)
- [Datalog] Regression in dealing with non-existent attributes. [#274](https://github.com/juji-io/datalevin/issues/274)

### Changed
- [KV] Change default write option to be the safest, the same as LMDB
  defaults, i.e. synchronous flush to disk when commit.

### Added
- [KV] Expose `sync` function to force a synchronous flush to disk, useful
  when non-default flags for writes are used.
- [Pod] added `clear` to bb pod.

### Improved
- [JVM] Support Java 8 in uberjar, following Clojure supported Java version.
- [Pod] Added missing arity in `get-conn` [Thx @aldebogdanov]

## 0.9.10 (2024-08-11)
### Fixed
- [Native] compile native image with UTF-8 encoding on Arm64 Linux and Windows.

## 0.9.9 (2024-08-11)
### Added
- [Platform] native image on Arm64 Linux. [Thx @aldebogdanov]
- [Benchmark] ported Join Order Benchmark (JOB) from SQL

### Fixed
- [Datalog] Planner: nested logic predicates.
- [Datalog] Planner: multiple predicates turned ranges.
- [Datalog] Planner: missing range turned predicates.
- [Datalog] Planner: need to first try target var to find index for :ref plan.
- [Datalog] Planner: fail to unify with existing vars in certain cases. [#263](https://github.com/juji-io/datalevin/issues/263)
- [Datalog] Planner: skip initial attribute when it does not have a var.
- [Datalog] Planner: target var may be already bound for link step.
- [Datalog] Planner: missing bound var in merge scan.
- [Datalog] `like` function failed to match in certain cases.
- [Datalog] `clear` function also clear the meta DBI

### Improved
- [Datalog] Planner: execute initial step if result size is small during
  planning, controlled by dynamic var `init-exec-size-threshold` (default 1000),
  above which, the same number of samples are collected instead. These
  significantly improved subsequent join size estimation, as these initial steps
  hugely impact the final plan.
- [Datalog] Planner: search full plan space initially, until the number of plans
  considered in a step reaches `plan-space-reduction-threshold` (default 990),
  then greedy search is performed in later stages, as these later ones have less
  impact on performance. This provides a good balance between planning time and
  plan quality, while avoiding potential out of memory issue during planning.
- [Datalog] Planner: do parallel processing whenever appropriate during planning
  and execution (regular JVM only).
- [LMDB] Lock env when creating a read only txn to have safer concurrent reads.

### Changed
- [Datalog] maintain an estimated total size and a representative sample of
  entity ids for each attribute, processed periodically according to
  `sample-processing-interval`(default 3600 seconds).
- [Datalog] reduce default `*fill-db-batch-size*` to 1 million datoms.
- [KV] throw exception when transacting `nil`, [#267](https://github.com/juji-io/datalevin/issues/267)

## 0.9.8 (2024-06-29)

### Fixed
- [Datalog] Planner: column attributes should be a set of equivalent attribute
  and variable.
- [Datalog] Planner: convert ranges back to correct predicates.
- [Datalog] Handle `like`, `in` within complex logic expressions.

### Improved
- [Datalog] Optimize `not`, `and` and `or` logic functions that involve only
  one variable.

## 0.9.7 (2024-06-23)

### Fixed
- [Datalog] Handle bounded entity IDs in reverse reference and value equality
  scans, [#260](https://github.com/juji-io/datalevin/issues/260)
### Improved
- [Datalog] Added `:result` to `explain` result map.

## 0.9.6 (2024-06-21)

### Added
- [Datalog] `like` function similar to LIKE operator in SQL: `(like input
  pattern)` or `(like input pattern opts)`. Match pattern accepts wildcards `%`
  and `_`. `opts` map has key `:escape` that takes an escape character, default
  is `\!`. Pattern is compiled into a finite state machine that does non-greedy
  (lazy) matching, as oppose to the default in Clojure/Java regex. This function
  is further optimized by rewritten into index scan range boundary for patterns
  that have non-wildcard prefix. Similarly, `not-like` function is provided.
- [Datalog] `in` function that is similar to IN operator in SQL: `(in input
  coll)` which is optimized as index scan boundaries. Similarly, `not-in`.
- [Datalog] `fill-db` function to bulk-load a collection of trusted datoms,
  `*fill-db-batch-size*` dynamic var to control the batch size (default 4
  million datoms). The same var also control `init-db` batch size.
- `read-csv` function, a drop-in replacement for `clojure.data.csv/read-csv`.
  This CSV parser is about 1.5X faster and is more robust in handling quoted
  content.
- Same `write-csv` for completeness.

### Fixed
- [Datalog] Wrong variable may be returned when bounded variables are
  involved. [#259](https://github.com/juji-io/datalevin/issues/259)

### Changed
- [KV] Change default initial DB size to 1 GiB.

### Improved
- [Platform] Use local LMDB library on FreeBSD if available [thx
@markusalbertgraf].
- [Datalog] `min` and `max` query predicates handle all comparable data.
- [Datalog] Port applicable fixes from Datascript up to 1.7.1.
- update deps.


## 0.9.5 (2024-04-17)

### Fixed
- [Datalog] planner generates incorrect step when bound variable is involved in
  certain cases.
- [Datalog] `explain` throws when zero result is determined prior to actual
  planning. [Thx @aldebogdanov]
- [Datalog] regression in staged entity transactions for refs, [#244](https://github.com/juji-io/datalevin/issues/244), [Thx @den1k]
### Improved
- [Datalog] added query graph to `explain` result map.

## 0.9.4 (2024-04-02)

### Added
- [Datalog] `explain` function to show query plan.
- [Platform] Embedded library for Linux on Aarch64, which is crossed compiled
  using zig.
### Fixed
- [KV] Broken embedded library on Windows.
### Improved
- [Datalog] more robust concurrent writes on server.
- [KV] Flags are now sets instead of vectors.
- Update deps

## 0.9.3 (2024-03-13)

[DB Upgrade](https://github.com/juji-io/datalevin/blob/master/doc/upgrade.md) is required.

### Added
- [Datalog] Query optimizer to improve query performance, particularly for
  complex queries. See [details](doc/query.md). [#11](https://github.com/juji-io/datalevin/issues/11)
- [Datalog] More space efficient storage format, leveraging LMDB's
  dupsort feature, resulting in about 20% space reduction and faster counting of
  data entries.
- [Datalog] `search-datoms` function to lookup datoms without having to specify
  an index.
- [KV] Expose LMDB dupsort feature, i.e. B+ trees of B+ trees, [#181](https://github.com/juji-io/datalevin/issues/181), as the
  following functions that work only for dbi opened with `open-list-dbi`:
    * `put-list-items`
    * `del-list-items`
    * `visit-list`
    * `get-list`
    * `list-count`
    * `key-range-list-count`
    * `in-list?`
    * `list-range`
    * `list-range-count`
    * `list-range-filter`
    * `list-range-first`
    * `list-range-some`
    * `list-range-keep`
    * `list-range-filter-count`
    * `visit-list-range`
    * `operate-list-val-range`
- [KV] `key-range` function that returns a range of keys only.
- [KV] `key-range-count` function that returns the number of keys in a range.
- [KV] `visit-key-range` function that visit keys in a range for side effects.
- [KV] `range-some` function that is similar to `some` for a given range.
- [KV] `range-keep` function that is similar to `keep` for a given range.

### Changed
- [Datalog] Change VEA index back to VAE.
- [Datalog] `:eavt`, `:avet` and `:vaet` are no longer accepted as index names,
  use `:eav`, `:ave` and `:vae` instead. Otherwise, it's misleading, as we don't
  store tx id.
- [KV] Change default write setting from `:mapasync` to `:nometasync`, so
  that the database is more crash resilient. In case of system crash, only the
  last transaction might be lost, but the database will not be corrupted. [#228](https://github.com/juji-io/datalevin/issues/228)
- [KV] Upgrade LMDB to the latest version, now tracking mdb.master branch,
  as it includes important fixes for dupsort, such as
  https://bugs.openldap.org/show_bug.cgi?id=9723
- [KV] `datalevin/kv-info` dbi to keep meta information about the databases, as
  well as information about each dbi, as flags, key-size, etc. [#184](https://github.com/juji-io/datalevin/issues/184)
- [KV] Functions that take a predicate have a new argument `raw-pred?` to
  indicate whether the predicate takes a raw KV object (default), or a pair of
  decoded values of k and v (more convenient).

### Improved
- [Datalog] Query results is now spillable to disk. [#166](https://github.com/juji-io/datalevin/issues/166)
- [Search] Functions in `search-utils` namespace are now compiled instead of
  being interpreted to improve performance.

## 0.8.29 (2024-02-23)
### Improved
- Support older Clojure version.
- [Server] Recover options after automatic reconnect. [#241](https://github.com/juji-io/datalevin/issues/241)

## 0.8.28 (2024-02-23)
### Fixed
- [Datalog] Concurrent writes of large data values.
### Added
- [Datalog] `:closed-schema?` option to allow declared attributes only, default
  is `false`. [Thx @andersmurphy]
### Improved
- [Datalog] ported applicable improvements from Datascript up to 1.6.3

## 0.8.26 (2024-02-09)
### Fixed
- [Datalog] `:validate-data? true` not working for some data types. [Thx @andersmurphy]
- [Datalog] ported applicable fixes from Datascript up to 1.6.1
### Improved
- bump deps

## 0.8.25 (2023-12-14)
### Added
- [Datalog] Add `:db.fulltext/autoDomain` boolean property to attribute schema,
  default is `false`. When `true`, a search domain
  specific for this attribute will be created, with a domain name same as
  attribute name, e.g. "my/attribute". This enables the same `fulltext` function syntax as
  Datomic, i.e. `(fulltext $ :my/attribute ?search)`.
- [Search] Add `:search-opts` option to `new-search-engine` option argument,
  specifying default options passed to `search` function.

## 0.8.24 (2023-12-12)
### Added
- [Datalog] Add `:db.fulltext/domains` property to attribute schema, [#176](https://github.com/juji-io/datalevin/issues/176)
- [Datalog] Add `:search-domains` to connection option map, a map from domain
  names to search engine option maps.
- [Datalog] Add `:domains` option to `fulltext` built-in function option map
### Fixed
- [Datalog] Removed problematic caching in pull api implementation
### Improved
- [Datalog] Create search engines on-demand. [#206](https://github.com/juji-io/datalevin/issues/206)


## 0.8.23 (2023-12-06)
### Fixed
- deps conflict

## 0.8.22 (2023-12-06)
### Improved
- [Datalog] `<`, `>`, `<=`, `>=` built-in functions handle any comparable data, not just numbers.
- [Datalog] Better fix for [#224](https://github.com/juji-io/datalevin/issues/224) [Thx @dvingo]
- bump deps

## 0.8.21 (2023-10-31)
### Fixed
- [All] Do not interfere with the default print-methods of regular expression, byte
  array and big integer. [#230](https://github.com/juji-io/datalevin/issues/230)


## 0.8.20 (2023-10-02)
### Fixed
- [Datalog] `:xform` in pull expression not called for `:cardinality/one` ref
  attributes, [#224](https://github.com/juji-io/datalevin/issues/224). [Thx @dvingo]
- [Datalog] `:validate-data?` does not recognize homogeneous tuple data type, [#227](https://github.com/juji-io/datalevin/issues/227).
- [All] BigDec decoding out of range error for some values in JVM 8. [#225](https://github.com/juji-io/datalevin/issues/225).
### Improved
- [KV] Add JVM shutdown hook to close DB. per [#228](https://github.com/juji-io/datalevin/issues/228)
- [Datalog] TxReport prints differently from the actual value, [#223](https://github.com/juji-io/datalevin/issues/223).
- [Server] re-open server search engine automatically, [#229](https://github.com/juji-io/datalevin/issues/229)


## 0.8.19 (2023-08-16)
### Improved
- [Datalog] Handle refs in heterogeneous and homogeneous tuples, [#218](https://github.com/juji-io/datalevin/issues/218). [Thx @garret-hopper]
- [All] Remove some clojure.core redefinition warnings. [Thx @vxe]

## 0.8.18 (2023-07-02)
### Improved
- [Test] Fix windows tests.

## 0.8.17 (2023-07-02)
### Added
- [main] Added an `--nippy` option to dump/load database in nippy binary
  format, which handles some data anomalies, e.g. keywords with space in
  them, non-printable data, etc., and produces smaller dump file, [#216](https://github.com/juji-io/datalevin/issues/216)
### Improved
- [KV] More robust bigdec data type encoding on more platforms
- [All] Create a backup db directory `dtlv-re-index-<unix-timestamp>` inside the
  system temp directory when `re-index`, [#213](https://github.com/juji-io/datalevin/issues/213)
- [Search] Graceful avoidance of proximity scoring when positions are not indexed

## 0.8.16 (2023-05-10)
### Improved
- Remove Clojure 1.11 features to accommodate older Clojure

## 0.8.15 (2023-05-08)
### Added
- [Search] Consider term proximity in relevance when `:index-position?` search
  engine option is `true`. [#203](https://github.com/juji-io/datalevin/issues/203)
- [Search] `:proximity-expansion` search option (default `2`) can be used to
  adjust the search quality vs. time trade-off: the bigger the number, the
  higher is the quality, but the longer is the search time.
- [Search] `:proximity-max-dist` search option (default `45`) can be used to
  control the maximal distance between terms that would still be considered as
  belonging to the same span.
- [Search] `create-stemming-token-filter` function to create stemmers, which
  uses Snowball stemming library that supports many languages. [#209](https://github.com/juji-io/datalevin/issues/209)
- [Search] `create-stop-words-token-filter` function to take a customized stop
  words predicate.
- [KV, Datalog, Search] `re-index` function that dump and load data with new
  settings. Should only be called when no other threads or programs are
  accessing the database. [#179](https://github.com/juji-io/datalevin/issues/179)
### Fixed
- [KV] More strict type check for transaction data, throw when transacting
  un-thawable data. [#208](https://github.com/juji-io/datalevin/issues/208)
### Changed
- [Main] Remove `*datalevin-data-readers*` dynamic var, use Clojure's
  `*data-readers*` instead.

## 0.8.14 (2023-04-28)
### Fixed
- [Native] Rollback GraalVM to 22.3.1, as 22.3.2 is missing apple silicon.

## 0.8.13 (2023-04-28)
### Fixed
- [Datalog] Unexpected heap growth due to caching error. [#204](https://github.com/juji-io/datalevin/issues/204)
- [Datalog] More cases of map size reached errors during transaction. [#196](https://github.com/juji-io/datalevin/issues/196)
- [Datalog] Existing datoms still appear in `:tx-data` when unchanged. [#207](https://github.com/juji-io/datalevin/issues/207)

### Improved
- [Datalog] Disable cache during transaction, save memory and avoid disrupting
  concurrent write processes.
- [Native] upgrade GraalVM to 22.3.2
- [Lib] update deps.

## 0.8.12 (2023-04-03)
### Fixed
- [KV] When `open-kv`, don't grow `:mapsize` when it is the same as the current
  size.
- [Server] automatically reopen DBs for a client that is previously removed from
  the server.


## 0.8.11 (2023-04-02)
### Added
- [Search] `:include-text?` option to store original text. [#178](https://github.com/juji-io/datalevin/issues/178).
- [Search] `:texts` and `:texts+offsets` keys to `:display` option of `search`
  function, to return original text in search results.
### Improved
- [Main] more robust `dump` and `load` of Datalog DB on Windows.

## 0.8.10 (2023-04-01)
### Added
- [KV] `:max-readers` option to specify the maximal number of concurrent readers
  allowed for the db file. Default is 126.
- [KV] `max-dbs` option to specify the maximal number of sub-databases (DBI)
  allowed for the db file. Default is 128. It may induce slowness if too big a
  number of DBIs are created, as a linear scan is used to look up a DBI.
### Fixed
- [Datalog] `clear` after db is resized.

## 0.8.9 (2023-03-28)
### Fixed
- [KV] transacting data more than one order of magnitude larger than the
  initial map size in one transaction. [#196](https://github.com/juji-io/datalevin/issues/196)

## 0.8.8 (2023-03-21)
### Improved
- [Pod] serialize TxReport to regular map. [#190](https://github.com/juji-io/datalevin/issues/190)
### Fixed
- [Server] migrate old sessions that do not have `:last-active`.

## 0.8.7 (2023-03-21)
### Added
- [Datalog] `datalog-index-cache-limit` function to get/set the limit of Datalog
  index cache. Helpful to disable cache when bulk transacting data. [#195](https://github.com/juji-io/datalevin/issues/195)
- [Server] `:idle-timeout` option when creating the server, in ms, default is 24
  hours. [#122](https://github.com/juji-io/datalevin/issues/122)
### Fixed
- [Datalog] error when Clojure collections are used as lookup refs. [#194](https://github.com/juji-io/datalevin/issues/194)

## 0.8.6 (2023-03-10)
### Fixed
- [Datalog] correctly handle retracting then transacting the same datom in the
  same transaction. [#192](https://github.com/juji-io/datalevin/issues/192)
- [Datalog] error deleting entities that were previously transacted as part of
  some EDN data. [#191](https://github.com/juji-io/datalevin/issues/191).
### Improved
- [Lib] update deps.

## 0.8.5 (2023-02-13)
### Added
- [KV] added tuple data type that accepts a vector of scalar values. This
  supports range queries, i.e. having expected ordering by first element, then
  second element, and so on. This is useful, for example, as path keys for
  indexing content inside documents. When used in keys, the same 511 bytes
  limitation applies.
- [Datalog] added heterogeneous tuple `:db/tupleTypes` and homogeneous tuples
  `:db/tupleType` type. Unlike Datomic, the number of elements in a tuple are
  not limited to 8, as long as they fit inside a 496 bytes buffer. In addition,
  instead of using `nil` to indicate minimal value like in Datomic, one can use
  `:db.value/sysMin` or `:db.value/sysMax` to indicate minimal or maximal
  values, useful for range queries. [#167](https://github.com/juji-io/datalevin/issues/167)
 - [Main] dynamic var `*datalevin-data-readers*` to support loading custom tag
   literals. (thx @respatialized)
### Fixed
- [Main] dump and load big integers.
### Improved
- [Datalog] avoid unnecessary caching, improve transaction speed up to 25% for
large transactions.
- [Native] upgrade Graalvm to 22.3.1
- [Native] static build on Linux. [#185](https://github.com/juji-io/datalevin/issues/185)
- [Lib] update deps.

## 0.8.4 (2023-01-20)
### Fixed
- [Datalog] error when large `:db/fulltext` value is added then removed in the
  same transaction.

## 0.8.2 (2023-01-19)
### Fixed
- [Search] `search-utils/create-ngram-token-filter` now works. [#164](https://github.com/juji-io/datalevin/issues/164)
- [Datalog] large datom value may throw off full-text indexing. [#177](https://github.com/juji-io/datalevin/issues/177)

## 0.8.1 (2023-01-19)
### Fixed
- [Datalog] intermittent `:db/fulltext` values transaction error. [#177](https://github.com/juji-io/datalevin/issues/177)

## 0.8.0 (2023-01-19)

[DB Upgrade](https://github.com/juji-io/datalevin/blob/master/doc/upgrade.md) is required.

### Changed
- [Search] **Breaking** search index storage format change. Data re-indexing is
  necessary.
### Improved
- [Search] significant indexing speed and space usage improvement: for default
  setting, 5X faster bulk load speed; 2 orders of magnitude faster
  `remove-doc` and 10X disk space reduction; when term positions and offsets are
  indexed: 3X faster bulk load and 40 percent space reduction.
- [Search] added caching for term and document index access, resulting in 5
  percent query speed improvement on average, 35 percent improvement at median.
### Added
- [Search] `:index-position?` option to indicate whether to record term
  positions inside documents, default `false`.
- [Search] `:check-exist?` argument to `add-doc`indicate whether to check the
  existence of the document in the index, default `true`. Set it to `false` when
  importing data to improve ingestion speed.
### Fixed
- [Datalog] increasing indexing time problem for `:db/fulltext` values. [#151](https://github.com/juji-io/datalevin/issues/151)
- [Search] error when indexing huge documents.
- [KV] spillable results exception in certain cases.
### Removed
- [Search] `doc-refs` function.
- [Search] `search-index-writer` as well as related `write` and
  `commit`functions for client/server, as it makes little sense to bulk load
  documents across network.

## 0.7.12 (2023-01-11)
### Fixed
- [Native] allow native compilation on apple silicon
- [Datalog] db print-method. (thx @den1k)

## 0.7.11 (2023-01-09)
### Fixed
- [Datalog] intermittent concurrent transaction problems

## 0.7.10 (2023-01-09)
### Fixed
- [CI] adjust CI workflow for the latest Graalvm

## 0.7.9 (2023-01-09)
### Improved
- [Datalog] moved entity and transaction ids from 32 bits to 64 bits integers, supporting much larger DB. [#144](https://github.com/juji-io/datalevin/issues/144)
- [Datalog] wrapped `transact!` inside `with-transaction` to ensure ACID and improved performance
- [Native] updated to the latest Graalvm 22.3.0. [#174](https://github.com/juji-io/datalevin/issues/174)

## 0.7.8 (2023-01-04)
### Fixed
- [KV] `get-range` regression when results are used in `sequence`. [#172](https://github.com/juji-io/datalevin/issues/172)
### Improved
- [Datalog] Ported all applicable Datascript improvements since 0.8.13 up to now
  (1.4.0). Notably, added composite tuples feature, new pull implementation,
  many bug fixes and performance improvements. [#3](https://github.com/juji-io/datalevin/issues/3), [#57](https://github.com/juji-io/datalevin/issues/57), [#168](https://github.com/juji-io/datalevin/issues/168)
- bump deps

## 0.7.6 (2022-12-16)
### Fixed
- [Server] error when granting permission to a db created by `create-database`
  instead of being created by opening a connection URI
### Improved
- [Datalog] avoid printing all datoms when print a db
- [Doc] clarify that `db-name` is unique on the server. (thx @dvingo)

## 0.7.5 (2022-12-16)
### Improved
- avoid `(random-uuid)`, since not every one is on Clojure 1.11 yet.

## 0.7.4 (2022-12-15)
### Fixed
- typo prevent build on CI

## 0.7.3 (2022-12-15)
### Fixed
- [KV] spill test that prevents tests on MacOS CI from succeeding.

## 0.7.2 (2022-12-15)
### Fixed
- [KV] broken deleteOnExit for temporary files

## 0.7.1 (2022-12-15)
### Improved
- [KV] clean up spill files

## 0.7.0

[DB Upgrade](https://github.com/juji-io/datalevin/blob/master/doc/upgrade.md) is required.

### Added
- [Platform] embedded library support for Apple Silicon.
- [KV] A new range function `range-seq` that has similar signature as
  `get-range`, but returns a `Seqable`, which lazily reads data items into
  memory in batches (controlled by `:batch-size` option). It should be used
  inside `with-open` for proper cleanup. [#108](https://github.com/juji-io/datalevin/issues/108)
- [KV] The existent eager range functions, `get-range` and `range-filter`, now
  automatically spill to disk when memory pressure is high. The results, though
  mutable, still implement `IPersistentVector`, so there is no API level
  change. The spill-to-disk behavior is controlled by `spill-opts` option map
  when opening the db, allowing `:spill-threshold` and `:spill-root` options.
### Improved
- [KV] write performance improvement
### Changed
- [KV] Upgrade LMDB to 0.9.29

## 0.6.29
### Added
- [Client] `:client-opts` option map that is passed to the client when opening remote databases.
### Fixed
- [KV] `with-transaction-kv` does not drop prior data when DB is resizing.
- [Datalog] `with-transaction` does not drop prior data when DB is resizing.

## 0.6.28
### Improved
- [Native] Add github action runner image ubuntu-20.04 to avoid using too new a
  glibc version (2.32) that does not exist on most people's machines.

## 0.6.27
### Fixed
- [KV] `with-transaction-kv` does not crash when DB is resizing.

## 0.6.26
### Added
- [KV] `with-transaction-kv` macro to expose explicit transactions for KV
  database. This allows arbitrary code within a transaction to achieve
  atomicity, e.g. to implement compare-and-swap semantics, etc, [#110](https://github.com/juji-io/datalevin/issues/110)
- [Datalog] `with-transaction` macro, the same as the above for Datalog database
- [KV]`abort-transact-kv` function to rollback writes from within an explicit KV transaction.
- [Datalog] `abort-transact` function, same for Datalog transaction.
- [Pod] Missing `visit` function
### Improved
- [Server] Smaller memory footprint
- bump deps

## 0.6.22
### Added
- [Datalog] `fulltext-datoms` function that return datoms found by full
  text search query, [#157](https://github.com/juji-io/datalevin/issues/157)
### Fixed
- [Search] Don't throw for blank search query, return `nil` instead, [#158](https://github.com/juji-io/datalevin/issues/158)
- [Datalog] Correctly handle transacting empty string as a full text value, [#159](https://github.com/juji-io/datalevin/issues/159)
### Improved
- Datalevin is now usable in deps.edn, [#98](https://github.com/juji-io/datalevin/issues/98) (thx @ieugen)

## 0.6.21
### Fixed
- [Datalog] Caching issue introduced in 0.6.20 (thx @cgrand)

## 0.6.20
### Added
- [Pod] `entity` and `touch` function to babashka pod, these return regular
  maps, as the `Entity` type does not exist in a babashka script. [#148](https://github.com/juji-io/datalevin/issues/148) (thx
  @ngrunwald)
- [Datalog] `:timeout` option to terminate on deadline for query/pull. [#150](https://github.com/juji-io/datalevin/issues/150) (thx
  @cgrand).

## 0.6.19
### Changed
- [Datalog] Entity equality requires DB identity in order to better support
  reactive applications, [#146](https://github.com/juji-io/datalevin/issues/146) (thx @den1k)
### Improved
- bump deps

## 0.6.18
### Fixed
- [Search] corner case of search in document collection containing only one term, [#143](https://github.com/juji-io/datalevin/issues/143)
- [Datalog] entity IDs has smaller than expected range, now they cover full 32 bit integer range, [#140](https://github.com/juji-io/datalevin/issues/140)
### Added
- [Datalog] Persistent `max-tx`, [#142](https://github.com/juji-io/datalevin/issues/142)

## 0.6.17
### Added
- [Datalog] `tx-data->simulated-report` to obtain a transaction report without actually persisting the changes. (thx @TheExGenesis)
- [KV] Support `:bigint` and `:bigdec` data types, corresponding to
  `java.math.BigInteger` and `java.math.BigDecimal`, respectively.
- [Datalog] Support `:db.type/bigdec` and `:db.type/bigint`, correspondingly, [#138](https://github.com/juji-io/datalevin/issues/138).
### Improved
 - Better documentation so that cljdoc can build successfully. (thx @lread)

## 0.6.16
### Added
- [Datalog] Additional arity to `update-schema` to allow renaming attributes. [#131](https://github.com/juji-io/datalevin/issues/131)
- [Search] `clear-docs` function to wipe out search index, as it might be faster
  to rebuild search index than updating individual documents sometimes. [#132](https://github.com/juji-io/datalevin/issues/132)
- `datalevin.constants/*data-serializable-classes*` dynamic var, which can be
  used for `binding` if additional Java classes are to be serialized as part of
  the default `:data` data type. [#134](https://github.com/juji-io/datalevin/issues/134)
### Improved
- [Datalog] Allow passing option map as `:kv-opts` to underlying KV store when `create-conn`
- bump deps
### Fixed
- [Datalog] `clear` function on server. [#133](https://github.com/juji-io/datalevin/issues/133)
### Changed
- [Datalog] Changed `:search-engine` option map key to `:search-opts` for consistency [**Breaking**]

## 0.6.15
### Improved
- [Search] Handle empty documents
- [Datalog] Handle safe schema migration, [#1](https://github.com/juji-io/datalevin/issues/1), [#128](https://github.com/juji-io/datalevin/issues/128)
- bump deps
### Added
- [KV] visitor function for `visit` can return a special value `:datalevin/terminate-visit` to stop the visit.

## 0.6.14
### Fixed
- Fixed adding created-at schema item for upgrading Datalog DB from prior 0.6.4 (thx @jdf-id-au)
### Changed
- [**breaking**] Simplified `open-dbi` signature to take an option map instead
### Added
- `:validate-data?` option for `open-dbi`, `create-conn` etc., [#121](https://github.com/juji-io/datalevin/issues/121)

## 0.6.13
### Fixed
- Schema update regression. [#124](https://github.com/juji-io/datalevin/issues/124)
### Added
- `:domain` option to `new-search-engine`, so multiple search engines can
  coexist in the same `dir`, each with its own domain, a string. [#112](https://github.com/juji-io/datalevin/issues/112)

## 0.6.12
### Fixed
- Server failure to update max-eid regression, [#123](https://github.com/juji-io/datalevin/issues/123)
### Added
- Added an arity to `update-schema` to allow removal of attributes if they are
  not associated with any datoms, [#99](https://github.com/juji-io/datalevin/issues/99)

## 0.6.11
### Fixed
- Search add-doc error when alter existing docs

## 0.6.10
### Improved
- Persistent server session that survives restarts without affecting clients, [#119](https://github.com/juji-io/datalevin/issues/119)
- More robust server error handling

## 0.6.9
### Fixed
- Query cache memory leak, [#118](https://github.com/juji-io/datalevin/issues/118) (thx @panterarocks49)
- Entity retraction not removing `:db/updated-at` datom, [#113](https://github.com/juji-io/datalevin/issues/113)
### Added
- `datalevin.search-utils` namespace with some utility functions to customize
  search, [#105](https://github.com/juji-io/datalevin/issues/105) (thx @ngrunwald)

## 0.6.8
## Fixed
- Add `visit` KV function to `core` name space
- Handle concurrent `add-doc` for the same doc ref
## Improved
- Bump deps

## 0.6.7
## Fixed
- Handle Datalog float data type, [#88](https://github.com/juji-io/datalevin/issues/88)
### Improved
- Allow to use all classes in Babashka pods

## 0.6.6
### Improved
- Dump and load Datalog option map
- Dump and load `inter-fn`
- Dump and load regex
- Pass search engine option map to Datalog store

## 0.6.5
### Fixed
- Make configurable analyzer available to client/server

## 0.6.4
### Fixed
- Dot form Java interop regression in query, [#103](https://github.com/juji-io/datalevin/issues/103)
### Added
- Option to pass an analyzer to search engine, [#102](https://github.com/juji-io/datalevin/issues/102)
- `:auto-entity-time?` Datalog DB creation option, so entities can optionally have
  `:db/created-at` and `:db/updated-at` values added and maintained
  automatically by the system during transaction, [#86](https://github.com/juji-io/datalevin/issues/86)
### Improved
- Dependency bump

## 0.6.3
### Added
- `doc-count` function returns the number of documents in the search index
- `doc-refs` function returns a seq of `doc-ref` in the search index
### Improved
- `datalevin.core/copy` function can copy Datalog database directly.

## 0.6.1
### Added
- `doc-indexed?` function
### Improved
- `add-doc` can update existing doc
- `open-kv` function allows LMDB flags, [#100](https://github.com/juji-io/datalevin/issues/100)

## 0.6.0

[DB Upgrade](https://github.com/juji-io/datalevin/blob/master/doc/upgrade.md) is required.

### Added
- Built-in full-text search engine, [#27](https://github.com/juji-io/datalevin/issues/27)
- Key-value database `visit` function to do arbitrary things upon seeing a
  value in a range

### Fixed
- [**breaking**]`:instant` handles dates before 1970 correctly, [#94](https://github.com/juji-io/datalevin/issues/94). The storage
  format of `:instant` type has been changed. For existing Datalog DB containing
  `:db.type/instant`, dumping as a Datalog DB using the old version of dtlv, then
  loading the data is required; For existing key-value DB containing `:instant`
  type, specify `:instant-pre-06` instead to read the data back in, then write
  them out as `:instant` to upgrade to the current format.

### Improved
- Improve read performance by adding a cursor pool and switch to a more
  lightweight transaction pool
- Dependency bump

## 0.5.31
### Fixed
- Create pod client side `defpodfn` so it works in non-JVM.
### Added
- `load-edn` for dtlv, useful for e.g. loading schema from a file, [#101](https://github.com/juji-io/datalevin/issues/101)

## 0.5.30
###  Fixed
- Serialized writes for concurrent transactions, [#83](https://github.com/juji-io/datalevin/issues/83)
### Added
- `defpodfn` macro to define a query function that can be used in babashka pod, [#85](https://github.com/juji-io/datalevin/issues/85)

## 0.5.29
### Fixed
- Update `max-aid` after schema update (thx @den1k)

## 0.5.28
### Improved
- Updated dependencies, particularly fixed sci version (thx @borkdude)

## 0.5.27
### Fixed
- occasional LMDB crash during multiple threads write or abrupt exit

## 0.5.26
### Improved
- Update graalvm version
### Fixed
- Exception handling in copy

## 0.5.24
### Fixed
- Handle scalar result in remote queries
### Improved
- Server asks client to reconnect if the server is restarted and client
  reconnects automatically when doing next request

## 0.5.23
### Improved
- Bump versions of all dependency

## 0.5.22
### Improved
- More robust handling of abrupt network disconnections
- Automatically maintain the required number of open connections, [#68](https://github.com/juji-io/datalevin/issues/68)
### Added
- Options to specify the number of connections in the client connection pool
  and to set the time-out for server requests

## 0.4.44
### Fixed
- Backport the dump/load fix from 0.5.20

## 0.5.20
### Fixed
- Dumping/loading Datalog store handles raw bytes correctly

## 0.5.19
### Improved
- Remove client immediately when `disconnect` message is received, clean up
  resources afterwards, so a logically correct number of clients can be obtained
  in the next API call on slow machines.

## 0.5.18
### Fixed
- Occasional server message write corruptions in busy network traffic on Linux.

## 0.5.17
### Added
- JVM uberjar release for download
### Changed
- JVM library is now Java 8 compatible, [#69](https://github.com/juji-io/datalevin/issues/69)
### Improved
- Auto switch to local transaction preparation if something is wrong with remote
  preparation (e.g. problem with serialization)

## 0.5.16
### Improved
- Do most of transaction data preparation remotely to reduce traffic
### Fixed
- Handle entity serialization, fix [#66](https://github.com/juji-io/datalevin/issues/66)

## 0.5.15
### Changed
- Allow a single client to have multiple open databases at the same time
- Client does not open db implicitly, user needs to open db explicitly
### Fixed
- New `create-conn` should override the old, fix [#65](https://github.com/juji-io/datalevin/issues/65)

## 0.5.14
### Added
- `DTLV_LIB_EXTRACT_DIR` environment variable to allow customization of native
  libraries extraction location.
### Improved
- Use clj-easy/graal-build-time, in anticipation of GraalVM 22.

## 0.5.13

### Improved
- Better robust jar layout for `org.clojars.huahaiy/datalevin-native`

## 0.5.10
### Added
- Release artifact `org.clojars.huahaiy/datalevin-native` on clojars, for
  depending on Datalevin while compiling GraalVM native image. User
  no longer needs to manually compile Datalevin C libraries.

## 0.5.9
### Improved
- Only check to refersh db cache at user facing namespaces, so internal db
  calls work with a consistent db view
- Replace unnecessary expensive calls such as `db/-search` or `db/-datoms` with
  cheaper calls to improve remote store access speed.
- documentation

## 0.5.8

### Improved
- More robust build

## 0.5.5

### Improved
- Wrap all LMDB flags as keywords

## 0.5.4
### Fixed
- Don't do AOT in library, to avoid deps error due to exclusion of graal
### Improved
- Expose all LMDB flags in JVM version of kv store

## 0.5.3

[DB Upgrade](https://github.com/juji-io/datalevin/blob/master/doc/upgrade.md) is required.

### Added
- Transparent networked client/server mode with role based access control. [#46](https://github.com/juji-io/datalevin/issues/46)
  and [#61](https://github.com/juji-io/datalevin/issues/61)
- `dtlv exec` takes input from stdin when no argument is given.
### Improved
- When open db, throw exception when lacking proper file permission

## 0.4.40
### Added
- Transactable entity [Thanks @den1k, [#48](https://github.com/juji-io/datalevin/issues/48)]
- `clear` function to clear Datalog db

## 0.4.35
### Fixed
- Native uses the same version of LMDB as JVM, [#58](https://github.com/juji-io/datalevin/issues/58)

## 0.4.32
### Improved
- Remove GraalVM and dtlv specific deps from JVM library jar
- Update deps

## 0.4.31
### Improved
- More robust dependency management
### Fixed
- Replacing giant values, this requires Java 11 [#56]

## 0.4.30
### Fixed
- Transaction of multiple instances of bytes [#52, Thanks @den1k]
- More reflection config in dtlv
- Benchmark deps

## 0.4.29
### Fixed
- Correct handling of rule clauses in dtlv
- Documentation clarification that we do not support "db as a value"

## 0.4.28
### Added
- Datafy/nav for entity [Thanks @den1k]
- Some datom convenience functions, e.g. `datom-eav`, `datom-e`, etc.
### Changed
- Talk to Babashka pods client in transit+json

## 0.4.27
### Added
- Exposed more functions to Babashka pod

## 0.4.26
### Added
- Native Datalevin can now work as a Babashka pod

## 0.4.23
### Added
- Compile to native on Windows and handle Windows path correctly
- `close-db` convenience function to close a Datalog db

## 0.4.21
### Changed
- Compile to Java 8 bytecode instead of 11 to have wider compatibility
- Use UTF-8 throughout for character encoding

## 0.4.20
### Improved
- Improve dtlv REPL (doc f) display

## 0.4.19
### Improved
- Provide Datalevin C source as a zip to help compiling native Datalevin dependency

## 0.4.17
### Improved
- Minor improvement on the command line tool

## 0.4.16
### Changed
- Native image now bundles LMDB

## 0.4.13
### Fixed
- Handle list form in query properly in command line shell [#42]

## 0.4.4
### Changed
- Consolidated all user facing functions to `datalevin.core`, so users don't have to understand and require different namespaces in order to use all features.

## 0.4.0

[DB Upgrade](https://github.com/juji-io/datalevin/blob/master/doc/upgrade.md) is required.

### Changed
- [**Breaking**] Removed AEV index, as it is not used in query. This reduces storage
  and improves write speed.
- [**Breaking**] Change VAE index to VEA, in preparation for new query engine. Now
  all indices have the same order, just rotated, so merge join is more likely.
- [**Breaking**] Change `open-lmdb` and `close-lmdb` to `open-kv` and `close-kv`,
  `lmdb/transact` to `lmdb/transact-kv`, so they are consistent, easier to
  remember, and distinct from functions in `datalevin.core`.

### Added
- GraalVM native image specific LMDB wrapper. This wrapper allocates buffer
  memory in C and uses our own C comparator instead of doing these work in Java,
  so it is faster.
- Native command line shell, `dtlv`

## 0.3.17
### Changed
- Improve Java interop call performance

## 0.3.16
### Changed
- Allow Java interop calls in where clauses, e.g. `[(.getTime ?date) ?timestamp]`, `[(.after ?date1 ?date2)]`, where the date variables are `:db.type/instance`. [#32]

## 0.3.15
### Changed
- Changed default LMDB write behavior to use writable memory map and
  asynchronous msync, significantly improved write speed for small transactions
  (240X improvement for writing one datom at a time).

## 0.3.14
### Fixed
- Read `:db.type/instant` value as `java.util.Date`, not as `long` [#30]

## 0.3.13
### Fixed
- Fixed error when transacting different data types for an untyped attribute [#28, thx @den1k]

## 0.3.12
### Fixed
- proper exception handling in `lmdb/open-lmdb`

## 0.3.11
### Fixed
- Fixed schema update when reloading data from disk

## 0.3.10
### Fixed
- Fixed `core/get-conn` schema update

## 0.3.9
### Changed
- Remove unnecessary locks in read transaction
- Improved error message and documentation for managing LMDB connection
### Added
- `core/get-conn` and `core/with-conn`

## 0.3.8
### Fixed
-  Correctly handle `init-max-eid` for large values as well.

## 0.3.7
### Fixed
- Fixed regression introduced by 0.3.6, where :ignore value was not considered [#25]

## 0.3.6
### Fixed
- Add headers to key-value store keys, so that range queries work in mixed data tables

## 0.3.5
### Changed
- Expose all data types to key-value store API [#24]

## 0.3.4
### Fixed
- thaw error for large values of `:data` type. [#23]
### Changed
- portable temporary directory. [#20, thx @joinr]

## 0.3.3
### Fixed
- Properly initialize max-eid in `core/empty-db`

## 0.3.2
### Changed
- Add value type for `:db/ident` in implicit schema

## 0.3.1

### Changed
- [**Breaking**] Change argument order of `core/create-conn`, `db/empty-db`
  etc., and put `dir` in front, since it is more likely to be specified than
  `schema` in real use, so users don't have to put `nil` for `schema`.

## 0.2.19
### Fixed
- correct `core/update-schema`

## 0.2.18

### Fixed
- correctly handle `false` value as `:data`
- always clear buffer before putting data in

## 0.2.17

### Fixed
- thaw exception when fetching large values

## 0.2.16

### Changed
- clearer error messages for byte buffer overflow

## 0.2.15

### Fixed
- correct schema update

## 0.2.14

### Added
- `core/schema` and `core/update-schema`

## 0.2.13
### Added
- `core/closed?`

## 0.2.12
### Fixed
- `db/entid` allows 0 as eid

## 0.2.11
### Fixed
- fix test

## 0.2.10

### Fixed
- correct results when there are more than 8 clauses
- correct query result size

## 0.2.9
### Changed
- automatically re-order simple where clauses according to the sizes of result sets
- change system dbi names to avoid potential collisions

### Fixed
- miss function keywords in cache keys

## 0.2.8
### Added
- hash-join optimization [submitted PR [#362](https://github.com/juji-io/datalevin/issues/362) to Datascript](https://github.com/tonsky/datascript/pull/362)
- caching DB query results, significant query speed improvement

## 0.2.7
### Fixed
- fix invalid reuse of reader locktable slot [#7](https://github.com/juji-io/datalevin/issues/7)
### Changed
- remove MDB_NOTLS flag to gain significant small writes speed

## 0.2.6
### Fixed
- update existing schema instead of creating new ones

## 0.2.5
### Fixed
- Reset transaction after getting entries
- Only use 24 reader slots

## 0.2.4
### Fixed
- avoid locking primitive [#5](https://github.com/juji-io/datalevin/issues/5)
- create all parent directories if necessary

## 0.2.3
### Fixed
- long out of range error during native compile

## 0.2.2
### Changed
- apply [query/join-tuples optimization](https://github.com/tonsky/datascript/pull/203)
- use array get wherenever we can in query, saw significant improvement in some queries.
- use `db/-first` instead of `(first (db/-datom ..))`, `db/-populated?` instead of `(not-empty (db/-datoms ..)`, as they do not realize the results hence faster.
- storage test improvements

## 0.2.1
### Changed
- use only half of the reader slots, so other processes may read

### Added
- add an arity for `bits/read-buffer` and `bits/put-buffer`
- add `lmdb/closed?`, `lmdb/clear-dbi`, and `lmdb/drop-dbi`

## 0.2.0
### Added
- code samples
- API doc
- `core/close`

## 0.1.0
### Added
- Port datascript 0.18.13
