# Datascript Benchmark

This directory contains the original benchmarks from
[Datascript](https://github.com/tonsky/datascript), with code for Datalevin
added, and related software updated to the latest versions.

The data consists of 20K entities of random person information, each entity has
5 attributes, totaling 100K datoms. The benchmarks load and query the data in a
in-memory setting.

## Run benchmarks

To run the benchmarks, you need to have both [leiningen](https://leiningen.org/) and [Clojure CLI](https://clojure.org/guides/deps_and_cli) installed on your system already. Then run these commands in the project root directory.

```
lein javac
cd bench
./bench.clj
```

For more comparisons with other alternatives, you may also consult [this fork](https://github.com/joinr/datalevinbench).

## Results

We ran this benchmark on an Apple M3 Pro with 36GB RAM, running macOS 26.2 and
OpenJDK 21.0.9, with Clojure 1.12.4.

Datomic peer binary (licensed under Apache 2.0) 1.0.7277, Datascript 1.7.4, and
Datalevin 0.10.5 all ran in in-memory mode.

Clojure code for benchmarked tasks are given below in Datalevin syntax.

### Write

Several write conditions are tested.

#### Init

This task loads the pre-prepared 100K `datoms `directly into the databases,
without going through the transaction process. Datomic does not have this
option.

```Clojure
      (d/init-db datoms nil schema {:kv-opts {:inmemory? true}})
```

|DB|Init Latency (ms)|
|---|---|
|Datomic|N/A|
|Datascript|13.1|
|Datalevin|164.7|

Datalevin loads datoms into an in-memory DB slower than Datascript. The
difference ratio is over 12X, because Datalevin still uses its LMDB-backed
index structures even in memory mode, while Datascript uses a simple in-memory
sorted set.

#### Add-1

This transacts one datom at a time.

```Clojure
    (reduce
      (fn [db p]
        (-> db
            (d/db-with [[:db/add (:db/id p) :name      (:name p)]])
            (d/db-with [[:db/add (:db/id p) :last-name (:last-name p)]])
            (d/db-with [[:db/add (:db/id p) :sex       (:sex p)]])
            (d/db-with [[:db/add (:db/id p) :age       (:age p)]])
            (d/db-with [[:db/add (:db/id p) :salary    (:salary p)]])))
      (d/empty-db nil schema {:kv-opts {:inmemory? true}})
      core/people20k)
```

|DB|Add-1 Latency (ms)|
|---|---|
|Datomic|1187.2|
|Datascript|500.4|
|Datalevin|878.3|

Compared with Init, transacting data is an order of magnitude more expensive,
since the transaction logic involves a great many number of reads and checks.

Datascript is the fastest and Datomic is the slowest.

Datalevin is between the two, about 1.75X slower than Datascript.

#### Add-5

This transacts one entity (5 datoms) at a time.

```Clojure
          (reduce (fn [db p] (d/db-with db [p]))
            (d/empty-db nil schema {:kv-opts {:inmemory? true}})
            core/people20k)
```

|DB|Add-5 Latency (ms)|
|---|---|
|Datomic|388.5|
|Datascript|479.9|
|Datalevin|531.1|

Datomic does better in this condition, more than halves its transaction time
compared with Add-1, now actually becomes the fastest.

Datascript improves modestly over Add-1. Datalevin is close to Datascript.

#### Add-all

This transacts all 100K datoms in one go.

```Clojure
    (d/db-with
      (d/empty-db nil schema {:kv-opts {:inmemory? true}})
      core/people20k)
```

|DB|Add-all Latency (ms)|
|---|---|
|Datomic|168.2|
|Datascript|469.1|
|Datalevin|448.5|

Datomic again improves greatly.

Datalevin is now slightly faster than Datascript, both within a few percent of
each other.

#### Retract-5

This retracts one entity at a time.

```Clojure
(reduce (fn [db eid] (d/db-with db [[:db.fn/retractEntity eid]])) db eids)
```

|DB|Retract-5 Latency (ms)|
|---|---|
|Datomic|698.1|
|Datascript|230.4|
|Datalevin|146.4|

Datalevin is the fastest in retracting data, about 1.6X faster than Datascript
and almost 5X faster than Datomic.

### Read

This benchmark only involves a few simple Datalog queries.

In order to have a fair comparison, all caching is disabled in Datalevin.

#### q1

This is the simplest query: find entity IDs of a bound attribute value,
returning about 4K IDs.

```Clojure
(d/q '[:find ?e
       :where [?e :name "Ivan"]]
      db100k)
```
|DB|Q1 Latency (ms)|
|---|---|
|Datomic|1.0|
|Datascript|0.25|
|Datalevin|0.22|

Datalevin edges out Datascript for this query. Datomic is several times slower.

#### q2

This adds an unbound attribute to the results.

```Clojure
    (d/q '[:find ?e ?a
           :where [?e :name "Ivan"]
                  [?e :age ?a]]
      db100k)
```
|DB|Q2 Latency (ms)|
|---|---|
|Datomic|2.0|
|Datascript|1.1|
|Datalevin|0.25|

Datalevin is over 4X faster than Datascript. The reason is that Datalevin
performs a merge scan on the index instead of a hash join to get the values of
`:age`, so it processes far fewer intermediate results.

Datomic lags further behind Datalevin for this query.

#### q2-switch

This is the same query as q2, just switched the order of the two where clauses.

```Clojure
    (d/q '[:find ?e ?a
           :where [?e :age ?a]
                  [?e :name "Ivan"]]
      db100k)
```
|DB|Q2-switch Latency (ms)|
|---|---|
|Datomic|9.6|
|Datascript|2.2|
|Datalevin|0.24|

Datalevin performs identically to q2 at 0.24ms. The query optimizer generates
the same plan regardless of clause order.

Datascript slows down 2X compared with q2. Datomic is even worse, slowing down
close to 5X. These databases do not have a query optimizer, so they blindly join
one clause at a time. If the first clause has a large result set, the query
slows down significantly.

#### q3

This adds a bound attribute, but its selectivity is low (0.5), so it merely cuts
the number of tuples in half.

```Clojure
    (d/q '[:find ?e ?a
           :where
           [?e :name "Ivan"]
           [?e :age ?a]
           [?e :sex :male]]
         db100k)
```

|DB|Q3 Latency (ms)|
|---|---|
|Datomic|2.7|
|Datascript|1.7|
|Datalevin|0.13|

Datalevin is over 13X faster than Datascript and over 20X faster than Datomic
for this query.

#### q4

This adds one more unbound attribute.

```Clojure
    (d/q '[:find ?e ?l ?a
           :where
           [?e :name "Ivan"]
           [?e :last-name ?l]
           [?e :age ?a]
           [?e :sex :male]]
         db100k)
```
|DB|Q4 Latency (ms)|
|---|---|
|Datomic|3.7|
|Datascript|2.5|
|Datalevin|0.14|

Datalevin is now nearly 18X faster than Datascript and over 26X faster than
Datomic.

An additional unbound attribute adds negligible overhead in Datalevin, while it
costs more for Datomic and Datascript.

#### qpred1

This is a query with a predicate that limits the values of an attribute to about
half of the value range, returning about 10K tuples.

```Clojure
    (d/q '[:find ?e ?s
           :where [?e :salary ?s]
                  [(> ?s 50000)]]
      db100k)
```
|DB|QPred1 Latency (ms)|
|---|---|
|Datomic|5.4|
|Datascript|3.7|
|Datalevin|1.0|

Datalevin is over 3.7X faster than Datascript, and about 5.4X faster than Datomic
for this query. The reason is that the Datalevin optimizer rewrites the
predicate into a range boundary for range scan on `:salary`. This query is
essentially turned into a single range scan in the `:ave` index, so even though
half of all entity IDs are returned, the speed is still fast.

#### qpred2

This is essentially the same query as qpred1, but the lower limit of `:salary`
is passed in as a parameter.

```Clojure
    (d/q '[:find ?e ?s
           :in   $ ?min_s
           :where [?e :salary ?s]
                  [(> ?s ?min_s)]]
      db100k 50000)
```
|DB|QPred2 Latency (ms)|
|---|---|
|Datomic|6.6|
|Datascript|6.1|
|Datalevin|0.99|

Datalevin performs the same as qpred1 for this one. The reason is because the
optimizer plugs the input parameter into the query directly, so it becomes
identical to qpred1.

Datascript performs over 1.6X worse in this one than in qpred1, because
Datascript treats each input parameter as an additional relation to be joined,
often performing a Cartesian product due to a lack of shared attributes. Datomic
also slows slightly.

## Conclusion

Using this benchmark, Datalevin query engine is found to be faster than Datomic
and Datascript with a relatively large margin.

However, the queries in this benchmark are fairly simple. To see Datalevin's
ability to handle complex queries and a much larger data size, see
[JOB Benchmark](../JOB-bench). For Datalevin's durable write performance, see
[write benchmark](../write-bench).
