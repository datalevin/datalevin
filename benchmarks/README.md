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
