<p align="center"><img src="logo.png" alt="datalevin logo"
height="140"></img></p>
<h1 align="center">Datalevin</h1>
<p align="center"> 🧘 Simple, fast and versatile Datalog database for everyone
💽 </p>
<p align="center">
<a href="https://central.sonatype.com/artifact/org.datalevin/datalevin-java"><img
src="https://img.shields.io/maven-central/v/org.datalevin/datalevin-java.svg?color=success"
alt="datalevin-java on maven central"></img></a>
<a href="https://www.npmjs.com/package/datalevin-node"><img
src="https://img.shields.io/npm/v/datalevin-node.svg?color=success"
alt="datalevin-node on npm"></img></a>
<a href="https://pypi.org/project/datalevin/"><img
src="https://img.shields.io/pypi/v/datalevin.svg?color=success"
alt="datalevin on pypi"></img></a>
<a href="https://clojars.org/datalevin"><img
src="https://img.shields.io/clojars/v/datalevin.svg?color=success"
alt="datalevin on clojars"></img></a>
<a
href="https://github.com/datalevin/datalevin/blob/master/doc/install.md#babashka-pod"><img
src="https://raw.githubusercontent.com/babashka/babashka/master/logo/badge.svg"
alt="bb compatible"></img></a>
</p>
<p align="center">
<a href="https://javadoc.io/doc/org.datalevin/datalevin-java/latest/"><img
src="https://javadoc.io/badge2/org.datalevin/datalevin-java/javadoc.svg"
alt="datalevin-java javadocs"></img></a>
<a href="https://cljdoc.org/d/datalevin/datalevin"><img
src="https://cljdoc.org/badge/datalevin/datalevin" alt="datalevin on
cljdoc"></img></a>
</p>

> I love Datalog, why hasn't everyone used this already?

**Datalevin** (/ˈdadə ˈlevən/, "levin" means "lightning") is a simple durable
[Datalog](https://en.wikipedia.org/wiki/Datalog) database. Here's what a Datalog
query looks like in Datalevin:

```Clojure
(d/q '[:find  ?name ?total
       :in    $ ?year
       :where [?sales :sales/year ?year]
              [?sales :sales/total ?total]
              [?sales :sales/customer ?customer]
              [?customer :customers/name ?name]]
      (d/db conn) 2026)
```

## :question: Why

The rationale is to have a simple, fast, versatile and open source Datalog query
engine running on durable storage.

It is our observation that many developers prefer
the flavor of Datalog popularized by [Datomic®](https://www.datomic.com) over
any flavor of SQL, once they get to use it. Perhaps it is because Datalog is
more declarative and composable than SQL, e.g. the automatic implicit joins seem
to be its killer feature. In addition, the recursive rules feature of Datalog
makes it suitable for [graph queries](benchmarks/LDBC-SNB-bench) and
[deductive reasoning](benchmarks/openrulebench).

The feature set of Datomic® may not be a good fit for some use cases. One thing
that may [confuse some
users](https://vvvvalvalval.github.io/posts/2017-07-08-Datomic-this-is-not-the-history-youre-looking-for.html)
is its [temporal
features](https://docs.datomic.com/cloud/whatis/data-model.html#time-model). To
keep things simple and familiar, Datalevin behaves the same way as most other
databases: when data are deleted, they are gone. Datalevin also follows the
widely accepted principles of ACID, instead of introducing [unusual
semantics](https://jepsen.io/analyses/datomic-pro-1.0.7075).

In addition to support Datomic® flavor of Datalog query language, Datalevin has
a [novel cost-based query optimizer](doc/query.md) with a much better query
performance, which is competitive with SQL RDBMS such as
[PostgreSQL and SQLite](benchmarks/JOB-bench) and graph databases such as
[Neo4j](benchmarks/LDBC-SNB-bench).

Datalevin provides robust ACID transaction features on the basis of [our
fork](https://github.com/huahaiy/dlmdb) of
[LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database), known
for its high read performance. With built-in support for WAL and
asynchronous transaction, Datalevin can also handle [write intensive
workload](benchmarks/write-bench).

Datalevin can store large document (< 2 GiB) and automatically build index by
paths for JSON, EDN and Markdown [documents](doc/idoc.md), so it can be used as
a document database, similar to MongoDB or PostgreSQL JSONB column.

Datalevin supports [vector database](doc/vector.md) features by integrating an
efficient SIMD accelerated vector indexing and search
[library](https://github.com/unum-cloud/usearch). Datalevin has a [novel full-text search
engine](doc/search.md) that has [competitive](benchmarks/search-bench) search
performance.

Datalevin is also AI-native. It ships with a built-in local [MCP
server](doc/mcp.md). Datalevin supports in-DB embedding and text generation with
built-in [llama.cpp](https://github.com/ggml-org/llama.cpp).

Datalevin can be used as a fast key-value store for
[EDN](https://en.wikipedia.org/wiki/Extensible_Data_Notation) data. The native
EDN data capability of Datalevin should be beneficial for Clojure programs.

Datalevin can be used as a library, embedded in applications to manage state,
e.g. used like SQLite; or it can run in a networked
[client/server](https://github.com/datalevin/datalevin/blob/master/doc/server.md)
mode (default port is 8898) with a Raft consensus based high availability
cluster configuration with full-fledged role-based access control (RBAC); or it
can be used as a [babashka
pod](https://github.com/babashka/pod-registry/blob/master/examples/datalevin.clj)
for shell scripting.

For embedded usage, [Java](examples/java/README.md),
[Python](bindings/python/README.md), [Node.js](bindings/javascript/README.md)
and [Clojure](https://cljdoc.org/d/datalevin/datalevin) are currently supported.

More information about our vision and design decisions can be found in these
resources in chronicle order:

* Interview [Clojure Corner Interview with Huahai Yang](https://www.youtube.com/watch?v=1XMU5mdDj7I)
* **Book [Datalevin: the Definite Guide to Logical and Intelligent Databases](https://datalevin.org/docs)**
* Post [Triple Store, Triple Progress: Datalevin Posited for the Future](https://yyhh.org/blog/2026/01/triple-store-triple-progress-datalevin-posited-for-the-future/)
* Post [Achieving High Throughput and Low Latency through Adaptive Asynchronous Transaction](https://yyhh.org/blog/2025/02/achieving-high-throughput-and-low-latency-through-adaptive-asynchronous-transaction/)
* Post [Competing for the JOB with a Triplestore](https://yyhh.org/blog/2024/09/competing-for-the-job-with-a-triplestore/)
* Post [If I had to Pick One: Datalevin](https://vimsical.notion.site/If-I-Had-To-Pick-One-Datalevin-be5c4b62cda342278a10a5e5cdc2206d)
* Post [T-Wand: Beat Lucene in Less Than 600 Lines of Code](https://yyhh.org/blog/2021/11/t-wand-beat-lucene-in-less-than-600-lines-of-code/)
* Presentation [2020 London Clojurians Meetup](https://youtu.be/-5SrIUK6k5g)

## :truck: [Installation](doc/install.md)

Datalevin is simple to add as a dependency to your project written in Java,
Python, node.js or Clojure. There are also several other installation options.
Please see details in [Installation Documentation](doc/install.md)

## :green_book: Documentation

The searchable [online user guide](https://datalevin.org) accepts
user-submitted examples so practical usage patterns can be shared with the
community.

The [complete book is available in print and ebook
formats](https://www.amazon.com/dp/B0H8X1QF2Q/). It includes five additional
chapters on AI memory that are not part of the online guide.

Please refer to the [Clojure API
documentation](https://cljdoc.org/d/datalevin/datalevin) and
[JavaDoc](https://javadoc.io/doc/org.datalevin/datalevin-java/latest/datalevin/package-summary.html)
for API details.

## :tada: Quick Examples

Datalevin is aimed to be a versatile database.

### Use as a Datalog store

Here is a simple Clojure code example using Datalevin:

```clojure
(require '[datalevin.core :as d])

;; Define an optional schema.
;; Note that pre-defined schema is optional, as Datalevin does schema-on-write.
;; However, attributes requiring special handling need to be defined in schema,
;; e.g. range query, many cardinality, uniqueness, reference type, etc.
;; Similar to Datascript, Datalevin schemas differ from Datomic®:
;; - The schema must be a map of maps, not a vector of maps.
;; - It is not `transact`ed into the db but passed when acquiring connections.
;; - Use `update-schema` to update the schema of an open connection to a DB.
(def schema {:aka  {:db/cardinality :db.cardinality/many}
             ;; :db/valueType is optional, if unspecified, the attribute will be
             ;; treated as EDN blobs, and may not be optimal for range queries
             :name {:db/valueType :db.type/string
                    :db/unique    :db.unique/identity}})

;; Create DB on disk and connect to it, assume write permission to create the dir
(def conn (d/get-conn "/tmp/datalevin/mydb" schema))
;; or if you have a Datalevin server running on myhost with default port 8898
;; (def conn (d/get-conn "dtlv://myname:mypasswd@myhost/mydb" schema))

;; Schema input patches existing attribute definitions: omitted properties are
;; preserved. Use :db/retract as a property value to remove that property.
;; Internal :db/aid values returned by d/schema are ignored in schema input.
;; Property patches, value migrations, deletions, and renames commit atomically;
;; repeating a completed deletion or rename is safe.
;; (d/update-schema conn {:name {:db/unique :db/retract}})

;; Transact some data
;; `:nation` is not defined in schema, so it will be treated as an EDN blob
(d/transact! conn
            [{:name "Frege", :db/id -1, :nation "France", :aka ["foo" "fred"]}
             {:name "Peirce", :db/id -2, :nation "france"}
             {:name "De Morgan", :db/id -3, :nation "English"}])

;; Query the data
(d/q '[:find ?nation
       :in $ ?alias
       :where
       [?e :aka ?alias]
       [?e :nation ?nation]]
     (d/db conn)
     "fred")
;; => #{["France"]}

;; Retract the name attribute of an entity
(d/transact! conn [[:db/retract 1 :name "Frege"]])

;; Pull the entity, now the name is gone
(d/q '[:find (pull ?e [*])
       :in $ ?alias
       :where
       [?e :aka ?alias]]
     (d/db conn)
     "fred")
;; => ([{:db/id 1, :aka ["foo" "fred"], :nation "France"}])

;; Close DB connection
(d/close conn)
```

### Use as a key-value store

Datalevin packages the underlying LMDB database as a convenient key-value store
for EDN data.

```clojure
(require '[datalevin.core :as d])
(import '[java.util Date])

;; Open a key value DB on disk and get the DB handle
(def db (d/open-kv "/tmp/datalevin/mykvdb"))
;; or if you have a Datalevin server running on myhost with default port 8898
;; (def db (d/open-kv "dtlv://myname:mypasswd@myhost/mykvdb" schema))

;; Define some table (called "dbi", or sub-databases in LMDB) names
(def misc-table "misc-test-table")
(def date-table "date-test-table")

;; Open the tables
(d/open-dbi db misc-table)
(d/open-dbi db date-table)

;; Transact some data, a transaction can put data into multiple tables
;; Optionally, data type can be specified to help with range query
(d/transact-kv
  db
  [[:put misc-table :datalevin "Hello, world!"]
   [:put misc-table 42 {:saying "So Long, and thanks for all the fish"
                        :source "The Hitchhiker's Guide to the Galaxy"}]
   [:put date-table #inst "1991-12-25" "USSR broke apart" :instant]
   [:put date-table #inst "1989-11-09" "The fall of the Berlin Wall" :instant]])

;; Get the value with the key
(d/get-value db misc-table :datalevin)
;; => "Hello, world!"
(d/get-value db misc-table 42)
;; => {:saying "So Long, and thanks for all the fish",
;;     :source "The Hitchhiker's Guide to the Galaxy"}


;; Range query, from unix epoch time to now
(d/get-range db date-table [:closed (Date. 0) (Date.)] :instant)
;; => [[#inst "1989-11-09T00:00:00.000-00:00" "The fall of the Berlin Wall"]
;;     [#inst "1991-12-25T00:00:00.000-00:00" "USSR broke apart"]]

;; This returns a PersistentVector - e.g. reads all data in JVM memory
(d/get-range db misc-table [:all])
;; => [[42 {:saying "So Long, and thanks for all the fish",
;;          :source "The Hitchhiker's Guide to the Galaxy"}]
;;     [:datalevin "Hello, world!"]]

;; This allows you to iterate over all DB keys inside a transaction.
;; You can perform writes inside the transaction.
;; Avoid long-lived transactions. Read transactions prevent reuse of pages freed
;; by newer write transactions, thus the database can grow quickly.
;; Write transactions prevent other write transactions, since writes are serialized.
(d/visit db misc-table
            (fn [kv]
               (let [k (d/read-buffer (d/k kv) :data)]
                  (when (= k 42)
                    (d/transact-kv db [[:put misc-table 42 "Don't panic"]]))))
              [:all])

(d/get-range db misc-table [:all])
;; => [[42 "Don't panic"] [:datalevin "Hello, world!"]]

;; Delete some data
(d/transact-kv db [[:del misc-table 42]])

;; Now it's gone
(d/get-value db misc-table 42)
;; => nil

;; Close key value db
(d/close-kv db)

```

## :bar_chart: Benchmarks

This repository contains several [benchmarks](benchmarks) that compare
performance of Datalevin with other systems.

All these benchmarks were conducted on a 2023 MacBook Pro, Apple M3 Pro chip
with 12 cores, 36 GB memory and 1TB SSD drive.

### Write

The [write benchmark](benchmarks/write-bench) compares Datalevin Datalog
transactions with SQLite under explicitly verified durability settings.

In pure write, the one-million-person-record matrix covers batches 1, 10, 100,
and 1000. Datalevin led at default sync mode in batches 1 and 1000, and default
WAL mode in batches 10, 100 and 1000; SQLite led the other rows. Datalevin's
async write modes are much faster, as expected.

For concurrent writes in default WAL mode, Datalevin led in all batches in both
2 and 4 concurrent threads.

For 50/50 mixed read/write, Datalevin held slight lead.

### Read

Datalevin support multiple database paradigms.

#### Relational

We compared Datalevin with PostgreSQL and SQLite in handling complex relational
queries, using [Join Order Benchmark](benchmarks/JOB-bench). The chart below
plots query latency for all 113 queries in the benchmark.

<p align="center">
<img src="benchmarks/JOB-bench/job_benchmark_log_bars.svg" alt="JOB benchmark" height="300"></img>
</p>

In the current second-pass snapshot, Datalevin completed the 113 queries in
38.1 seconds. PostgreSQL's retained 128.2-second total is 3.37X Datalevin's,
while SQLite's 281.8-second completed-query subtotal is 7.40X Datalevin's, with
another nine queries reaching the one-minute timeout. The gain is mainly due to
shorter query execution time as Datalevin's query optimizer generates better
plans.

#### Graph

We compare Datalevin with Neo4j Community Embedded using the 21 read
queries in the [LDBC SNB benchmark](benchmarks/LDBC-SNB-bench). Datalevin's
summed measured query time was 3.890 seconds versus 27.406 seconds for Neo4j.
Neo4j's sum was 7.046X Datalevin's, the equal-query Neo4j/Datalevin
geometric-mean ratio was 4.996X, and Datalevin had the lower time on 20 of 21
queries.

#### Logic

Datalevin's [rule engine](doc/rules.md) is evaluated with a portable
[OpenRuleBench-derived suite](benchmarks/openrulebench). It covers recursive
transitive closure (TC), nonlinear recursive same-generation fixed points
(SG), and nonrecursive Join1 rule DAGs. The benchmark compares Datalevin with 6
alternative systems.

In the current representative comparison, Datalevin had the lowest latency in
all ten selected cells. `FF` means both query arguments are free, `BF` fixes the
first argument, and `FB` fixes the second. The last column divides the fastest
alternative's latency by Datalevin's latency.

| Task | Result rows | Datalevin (ms) | Fastest alternative (ms) | Alternative / Datalevin |
|---|---:|---:|---:|---:|
| TC 50K cyclic FF | 1,000,000 | 67.88 | Soufflé 1,030.36 | 15.18X |
| TC 50K acyclic FF | 473,807 | 45.20 | Soufflé 371.93 | 8.23X |
| TC 50K cyclic BF | 1,000 | 4.71 | SQLite 7.04 | 1.49X |
| SG 6K cyclic FF | 869,923 | 70.23 | Soufflé 865.82 | 12.33X |
| SG 6K acyclic FF | 215,263 | 18.91 | Soufflé 89.43 | 4.73X |
| SG 6K acyclic BF | 533 | 6.09 | PostgreSQL 7.28 | 1.20X |
| SG 6K acyclic FB | 541 | 4.66 | PostgreSQL 6.81 | 1.46X |
| Join1 50K b1 FF | 1,000,000 | 86.65 | XSB 1,795.00 | 20.72X |
| Join1 50K b2 FF | 917,680 | 124.85 | XSB 174.00 | 1.39X |
| Join1 50K a BF | 1,000 | 12.01 | XSB 120.00 | 9.99X |

## :rocket: Status

Datalevin is extensively tested with property-based testing. It was used
in production at Juji (acquired by Accenture) for several years, among other
companies.

If your project is using Datalevin in production, drop a comment in [this
issue](https://github.com/datalevin/datalevin/issues/383).

## :earth_americas: Roadmap

The goal of Datalevin is to simplify data storage and access. We aim to support
diverse workloads and use cases. Below are the tentative goals that we try to
reach as soon as we can. We may adjust the priorities based on feedback.

* 0.4.0 ~~Native image and native command line tool.~~ [Done 2021/02/27]
* 0.5.0 ~~Networked server mode with role based access control.~~ [Done 2021/09/06]
* 0.6.0 ~~As a search engine: full-text search across database.~~ [Done 2022/03/10]
* 0.7.0 ~~Explicit transactions, lazy results loading, and results spill to disk
  when memory is low.~~ [Done 2022/12/15]
* 0.8.0 ~~Long ids; composite tuples; enhanced search engine ingestion speed.~~
  [Done 2023/01/19]
* 0.9.0 ~~New Datalog query engine with improved performance.~~ [Done 2024/03/09]
* 0.10.0 ~~Async transaction; boolean search expression and phrase search; as a
  vector database; counted and prefix compressed KV storage; auto upgrade
  migration; new rule engine.~~[Done 2026/01/22]
* 1.0.0 ~~As a document database with automatic path indexing; WAL mode;
  transaction log access API;  read-only replicas; high availability; JSON API;
  library for Java, Python, and JavaScript; Book.~~[Done 2027/07/20]
* 1.1.0 AVE storage optimization; transaction and query performance enhancement.
* 1.2.0 data compression; KV TTL;
* 1.3.0 extensible storage/query for arbitrary data; entity-level security.
* 2.0.0 direct analytics and virtual relationships.
* 2.1.0 incremental view maintenance core.
* 3.0.0 aggregation in rule heads.
* 3.1.0 provenance annotation.
* 4.0.0 local sharding.
* 4.1.0 data distribution.

## :arrows_clockwise: Contact

Datalevin will remain open source for the foreseeable future. We appreciate and
welcome your contributions or suggestions. Please feel free to file issues or
pull requests.

You can join the discussions in the `#datalevin`
channel on [Clojurians Slack](http://clojurians.net/) or use the Github
Discussions feature above.

If commercial support is needed, reach out to https://agenwiz.com

## License

Copyright © 2020-2026 [Huahai Yang](https://huahaiy.github.io/) and contributors.

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
