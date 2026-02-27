# Transactions in Datalevin

Datalevin offers a rich set of transaction configurations to support
diverse use cases and needs. A description of the performance characteristics of
these different configurations can be found in [write
benchmark](../benchmarks/write-bench).

The foundation of all these transaction modes is a LMDB transaction.

## LMDB Transaction

Datalevin relies on the transaction mechanism of the underlying key-value store,
LMDB (Lightening Memory-Mapped Database), to achieve ACID. Datalevin transaction
and LMDB transaction have an one-to-one correspondence.

LMDB implements multiversion concurrency control (MVCC), so read and write are
independent and do not block each other. Read requires a read transaction. Write
requires a read-write transaction. These are normally two different
transactions.

Writes are serialized in LMDB, only one thread can write at a time. When
multiple threads write concurrently to the same key, whoever writes later wins
eventually, because writes are serialized. The first write succeeds, but the
value will then be overwritten by the second write.

Reads can be concurrent. Basically each reader thread reads its own view of the
data created at the moment the read transaction starts. When a read is concurrent
with a write, the newly written values are invisible to the reader, because read
transaction sees a view of the database that is consistent and up to the time
when the read transaction starts, which is before the write transaction commits.

LMDB suggests that:

> Avoid long-lived transactions. Read transactions prevent reuse of pages freed
> by newer write transactions, thus the database can grow quickly. Write
> transactions prevent other write transactions, since writes are serialized.

## Additional Transaction Mechanisms

By default, each write transaction in LMDB flushes to disk when it commits,
which is an expensive operation even with today's SSD disks. Datalevin builds
two layers of additional transaction mechanisms on top to achieve higher level
of write throughput: WAL mode and asynchronous transaction functions.

### WAL mode

WAL mode enables Datalevin to escape the single writer limitation of LMDB. It
uses a sync queue to amortize the cost of expensive disk sync operations.
Details of the WAL mode are documented [here](wal.md).

### Asynchronous Transaction

Asynchronous transaction functions automatically batch transactions together to
reduce the number of expensive commit calls: `transact-kv-async` for KV
store, `transact-async` for Datalog store. Both return a `future`, that is only
realized after the data is flushed to disk, and they optionally take a callback
function, that will only be called after the data is flushed to disk.

`transact` function is a blocked version of `transact-async`, that will block
until the future is realized. One can call a sequence of `transact-async`,
followed by a `transact` to achieve good batching effect and deterministic commit
at the same time, for the asynchronous transactions are still committed in
order, so the last realized future indicates all the prior calls are
already committed. Or one can `deref` the future of the last asynchronous calls
manually, or put in a callback for the last call.

The batching of asynchronous transactions is adaptive to work load. The higher
the load, the bigger the batch size. Asynchronous transactions is recommended
for heavy write workload as they can increase throughput orders of magnitude
while providing low latency. See [blog
post](https://yyhh.org/blog/2025/02/achieving-high-throughput-and-low-latency-through-adaptive-asynchronous-transaction/).

It is still useful to manually batch transaction data in user code, as the
effect of auto batching and manual batching compounds. The compound batching
effect in KV transaction is more pronounced than in Datalog transaction.

## Non-durable LMDB Environment Flags

Datalevin write transactions by default are guaranteed to be durable, i.e. no
risk of data loss or DB corruption in case of system crash. As mentioned above,
this fully safe durable write condition has some performance implications since
syncing to disk is expensive.

LMDB supports some faster, albeit less durable write conditions. By passing
in some environment flags when opening the DB, or calling `set-env-flags`
function, significant write speed up can be achieved, with some caveats. The
follwing table lists these flags and their implications.

| Flags | Meaning | Speedup in Mixed Read/Write | Implications |
|----|----|----|---|
| `:nometasync` | Only sync data pages when commit, do not sync meta pages | up to 5X | Last transaction may be lost at untimely system crashes, but integrity of DB is retained |
| `:nosync` | Don't msync when commit | up to 20X | OS is responsible for syncing the data. Untimely system crash may render the DB corrupted. |
| `:writemap` + `:mapasync` | Use writable memory map and asynchronous commit | up to 25X | Untimely system crash may render the DB corrupted; Buggy external code may accidentally overwrite DB memory; Some OS fully preallocates the disk to the specified map size. |

Here are some examples of passing the env flags:

```Clojure
(require '[datalevin.core :as d])
(require '[datalevin.constants :as c])

;; Pass :nosync to my-kvdb KV store
(def kv-db (d/open-kv "/tmp/my-kvdb" {:flags (conj c/default-env-flags :nosync))))})

;; Turn off :nosync
(d/set-env-flags kv-db [:nosync] false)

;; Set :temp? true for a KV store automaticaly adds :nosync,
;; this DB will also be deleted on graceful JVM exit.
(d/open-kv "/tmp/tmp-kvdb" {:temp? true))})

;; Pass :writemap + :mapasync to testdb Datalog store
(d/get-conn "/tmp/testdb" {}
            {:kv-opts {:flags (-> c/default-env-flags
                                  (conj :writemap)
                                  (conj :mapasync))}})

```

Setting these flags improves write speed signficantly, users can then manually
call `sync` function at appropriate time to force flusing to disk in application
code. Timely backups may also mitigate some potential data loss. Combining these
techniques may achieve desirable write speed and durability trade-off.

## Explicit Synchronous Transaction

In addition to `:db/cas` or `:db.fn/cas` transaction functions, to obtain
features such as compare-and-swap semantics, that is, a group of reads and
writes are treated as a single atomic action, Datalevin exposes explicit
synchronous transaction as another mechanism.

For key-value API, `with-transaction-kv` macro is used for explicit transaction.
`with-transaction` macro is similarly used for Datalog API. Basically, all the
code in the body of the macros run inside a single read/write transaction with a
single thread. These work the same in all modes of Datalevin: embedded,
client/server, or babashka pod. For usage examples, see tests in
`datalevin.withtxn-test` or `datalevin.remote-withtxnkv-test`.

Rollback from within the transaction can be done with `abort-transact-kv` and
`abort-transact`.

Datalog functions such as `transact!` use `with-transaction` internally.

## Transaction Functions in Datalog Store

As mentioned above, in addition to `with-transaction`, transaction functions can
be used in Datalog store for atomic actions. In addition to `:db/cas`, two types
of customized transaction functions can be written.

`:db/fn` allows stored transaction functions. Such functions need to be defined
using `inter-fn` or `definterfn`.  This is necessary in order to support
de-serialization of functions without calling Clojure `eval`. This requirement
is needed to accommodate GraalVM native image. `eval` generates classes at
runtime, so it cannot be used in native image, which has a closed world
assumption.  This way of defining a function is also necessary when a function
needs to be passed over the wire to servers or babashka. The source code of the
function will be interpreted by [sci](https://github.com/babashka/sci) instead,
so there's currently some limitations, e.g. except for built-in ones, normal
Clojure vars are not accessible. We will address these limitations in the
future.

`:db.fn/call` is another way to call a transaction function, which does not
store the function in the database, so this is usable in embedded mode, where
that function is available in user code to call and that function can be a
regular Clojure function.

For usage examples, see tests in `datalevin.test.transact`.

## Bulk Load Data into Datalog Store

### By Transaction

The most straightforward method of transacting data at a time using `transact!`
works quite well for many cases, especially in WAL mode. To have a much higher
throughput, use `transact-async` instead.

In non-WAL mode, a single write thread is effective at a time, parallel
transactions actually slow writes down due to mutex contention and
thread switching overhead. Use WAL mode (default in Datalog store) if
concurrent writers are needed.

Transacting Datalog data involves a great number of data transformation and
integrity checks. When initializing a DB with data, it may not be necessary to
pay the price of this transaction overhead.

### By `init-db` and `fill-db`

If it is possible, a much faster way of bulk loading data into an empty DB is to
directly load a collection of prepared datoms using `init-db` function. However,
it is the caller's responsibility to ensure these datoms are correct because data
integrity checks and temporary entity ID resolution are not performed.

Similarly, `fill-db` can be used to bulk load additional collections of prepared
datoms into a DB that is not empty. The same caution on datoms preparation need
to apply.

The manual datoms prepared process is mainly about making up correct entity
IDs, which would not be too difficult if the numbers of entities to load is
known ahead of time. See JOB benchmark to see [an
example](../benchmarks/JOB-bench/src/datalevin_bench/core.clj).

## Transactable Entities in Datalog store

In other Datalog DBs (Datomic®, DataScript, and Datahike) `d/entity` returns a type
that errors on associative updates. This makes sense because Entity represents
a snapshot state of a DB Entity and `d/transact` demarcates transactions.
However, this API leads to a cumbersome developer experience, especially
for the removal of fields where vectors of `[:db/retract <eid> <attr> <optional eid>]`
must be used in transactions because `nil` values are not allowed.

Datalevin ships with a special Entity type that allows for associative updates
while remaining immutable until expanded during transaction time (`d/transact`).
This type works the same in either local or remote mode.

Below are some examples. Look for the `:<STAGED>` keyword in the printed entities

```clojure
(require '[datalevin.core :as d])

(def db
  (-> (d/empty-db nil {:user/handle  #:db{:valueType :db.type/string
                                          :unique    :db.unique/identity}
                       :user/friends #:db{:valueType   :db.type/ref
                                          :cardinality :db.cardinality/many}})
      (d/db-with [{:user/handle  "ava"
                   :user/friends [{:user/handle "fred"}
                                  {:user/handle "jane"}]}])))

(def ava (d/entity db [:user/handle "ava"]))

(d/touch ava)
; => {:user/handle ava, :user/friends #{#:db{:id 3} #:db{:id 2}}, :db/id 1}
(assoc ava :user/age 42)
; => {:user/handle  ava
;     :user/friends #{#:db{:id 3} #:db{:id 2}},
;     :db/id        1,
;     :<STAGED>     #:user{:age [{:op :assoc} 42]}} <-- staged transaction!

(d/touch (d/entity db [:user/handle "ava"]))
; => {:user/handle ava, :user/friends #{#:db{:id 3} #:db{:id 2}}, :db/id 1}
; immutable! – db entity remains unchanged

(def db2 (d/db-with db [(assoc ava :user/age 42)]))

(def ava-with-age (d/entity db2 [:user/handle "ava"]))

(d/touch ava-with-age)
;=> {:user/handle "ava",
;    :user/friends #{#:db{:id 3} #:db{:id 2}},
;    :user/age 42, <-- age was transacted!
;    :db/id 1}

(def db3
  (d/db-with db2 [(-> ava-with-age
                      (update :user/age inc)
                      (d/add :user/friends {:user/handle "eve"}))]))

;; eve exists
(d/touch (d/entity db3 [:user/handle "eve"]))
;; => {:user/handle "eve", :db/id 4}

; eve is a friend of ada
(d/touch (d/entity db3 [:user/handle "ava"]))
;=> {:user/handle "ava",
;    :user/friends #{#:db{:id 4} <-- that's eve!
;                    #:db{:id 3}
;                    #:db{:id 2}},
;    :user/age 43,
;    :db/id 1}

; Oh no! That was a short-lived friendship.
; eve and ava got into an argument 😔

(def db4
  (d/db-with
    db3
    [(d/retract (d/entity db3 [:user/handle "ava"]) :user/friends [{:db/id 4}])]))

(d/touch (d/entity db4 [:user/handle "ava"]))
;=> {:user/handle "ava",
;    :user/friends #{#:db{:id 3} #:db{:id 2}}, ; <-- eve is not a friend anymore
;    :user/age 43,
;    :db/id 1}
```

For more examples have a look at the
[tests](https://github.com/juji-io/datalevin/blob/master/test/datalevin/test/entity.clj#L45-L116).

This Entity API is new and can be improved. For example, it does not currently
resolve lookup refs like `[:user/handle "eve"]`. If you'd like to help, feel
free to reach out to @den1k.
