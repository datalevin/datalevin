# Benchmarks

The following benchmarks are included:

* [Join Order Benchmark](JOB-bench) compares Datalevin, SQLite and Postgresql on
  JOB benchmark. This is a standard benchmark for SQL databases that consists of
  realistic data set (IMDB) and complex queries, and puts a high demand on the
  quality of query optimizer.
* [LDBC-SNB Benchmark](LDBC-SNB-bench) compares Datalevin and Neo4j on an
  industry standard graph database workload. It contains some interactive short
  reads and complex graph queries on a synthetic graph data set.
* [Math Genealogy](math-bench)  compares Datascript, Datomic and Datalevin on
  Datalog rule processing using a realistic Math Genealogy data set.
* [Datascript](datascript-bench) is the benchmark inherited from Datascript,
  that compares Datascript, Datomic and Datalevin on Datalog transaction and
  queries, as well as rule processing using a synthetic data set.
* [Wikipedia Full-text Search](search-bench) compares Lucene and Datalevin on
  full-text search performance using Wikipedia data set and realistic Web
  queries.
* [Access Path](access-path-bench) compares identical fulltext and approximate
  vector queries with access paths enabled and disabled, reporting latency and
  residual candidate work.
* [Idoc](idoc-bench) runs YCSB-style A/C/F workloads plus idoc query mixes
  (nested paths, ranges, wildcards, arrays) to stress document queries.
* [Write](write-bench) studies writing synthetic data to measure Datalevin's write
  performance in various conditions, and compares with SQLite.
