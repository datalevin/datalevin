# Benchmarks

The current benchmark suite includes:

* [Write](write-bench) compares Datalevin's Datalog and SQLite write paths in
  pure, concurrent, and mixed read/write workloads. Durability-matched Datalog
  comparisons pair strict Datalevin WAL with SQLite WAL `FULL`; relaxed modes
  are reported separately. It also studies Datalevin's KV writes in various
  settings.
* [Join Order Benchmark](JOB-bench) compares Datalevin, PostgreSQL, and SQLite
  on all 113 queries in the standard IMDB workload. Its complex multiway joins
  stress query optimization; the publication protocol uses one complete warmup
  pass followed by one retained measurement pass.
* [LDBC-SNB Benchmark](LDBC-SNB-bench) compares Datalevin and Neo4j on an
  industry-standard graph workload containing interactive short reads and
  complex graph queries over a synthetic social-network data set.
* [OpenRuleBench](openrulebench) compares Datalevin with six alternative
  rule/SQL engines on portable transitive closure, same generation, and Join1
  tasks, important for recursive rule resolutions.
* [iDOC](idoc-bench) compares Datalevin, PostgreSQL, SQLite, and MongoDB on
  YCSB-style A/C/F workloads plus document-query mixes covering nested paths,
  ranges, wildcards, and arrays.
* [YCSB-style](ycsb-bench) runs A–F workloads against Datalevin KV and Datalog
  in embedded and local-server modes, with seeded records, concurrent clients,
  per-operation latency, throughput, and record validation. Datalog comparisons
  pair embedded mode with SQLite and remote mode with PostgreSQL.
* [Wikipedia Full-text Search](search-bench) compares Lucene and Datalevin on
  full-text search performance using a Wikipedia data set and realistic Web
  queries.
* [Math Genealogy](math-bench)  compares Datascript, Datomic and Datalevin on
  Datalog rule processing using a realistic Math Genealogy data set.
* [Datascript](datascript-bench) is the benchmark inherited from Datascript,
  that compares Datascript, Datomic and Datalevin on Datalog transaction and
  queries, as well as rule processing using a synthetic data set.
* [Access Path](access-path-bench) compares identical fulltext and approximate
  vector queries with access paths enabled and disabled, reporting latency and
  residual candidate work.

## Maintaining benchmark work

Keep reusable harness code in each benchmark's `src/` and `test/` directories.
The [host helper](host-control) is a local dependency of YCSB, TPC-H, and TPC-C;
include its sources and `deps.edn` when committing changes that depend on it.
Likewise, include newly required source and test namespaces with their callers
so a fresh checkout can run the harness.

Keep retained measurements under `results/<date>-<purpose>/`, with a short
README linking the configuration, raw output, validation status, and conclusion.
Record the engine and harness revisions, including a source hash or patch for
uncommitted changes. Mark interrupted or contaminated runs explicitly. Promote
current usage and design guidance into the benchmark README or the relevant
`doc/` page; dated experiment notes describe the code measured at that time.

Store disposable databases and build products outside retained result bundles,
preferably in a temporary directory. Generated database directories, local
dependency caches, and Python bytecode are ignored; experiment scripts and
result summaries remain visible for review. Archive large datasets and source
snapshots deliberately, preserving anything needed to reproduce a published
result.

Run performance measurements without concurrent builds, tests, or other
benchmarks. Reverse comparison order across trials and retain per-trial results.
The host helper only pauses macOS media-analysis daemons; it does not isolate
the machine from unrelated JVMs or other CPU and I/O activity. Check that
activity before treating small timing changes as regressions or improvements.
