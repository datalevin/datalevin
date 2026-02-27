# WAL Mode in Datalevin

Datalevin supports write-ahead logging (WAL) for both KV and Datalog writes.
WAL improves write throughput while keeping crash recovery explicit and
operationally manageable.

Concurrent writers receive tangible throughput benefits in WAL mode. The
scaling is sub-linear, e.g. 4 concurrent write threads may produce 2X throughout
increase.

## Enable WAL

### Datalog

For Datalog stores, WAL is enabled by default for new databases.
You can set WAL options when creating/opening a connection:

```clojure
(require '[datalevin.core :as d])

(def conn
  (d/create-conn
    "/tmp/my-datalog-db"
    {}
    {:wal? true
     :wal-durability-profile :strict}))
```

### KV

For direct KV usage (`open-kv`), WAL is off by default and must be enabled:

```clojure
(require '[datalevin.core :as d])

(def kv
  (d/open-kv
    "/tmp/my-kv-db"
    {:wal? true
     :wal-durability-profile :strict}))
```

## Durability Profiles

WAL supports two profiles:

* `:strict`: each transaction waits for durable WAL acknowledgment.
* `:relaxed`: transactions can return before durability is forced for every
  single write, using batched syncs for much higher throughput.

In `:relaxed`, an untimely crash can lose a recent tail of transactions that were
appended but not yet durably synced to disk.

### Durability Risk Window Controls

The `:relaxed` crash-risk window is bounded by two thresholds:

* count threshold: `:wal-group-commit` (max writes per durability batch)
* time threshold: `:wal-group-commit-ms` (max milliseconds per durability batch)

Batch durability is triggered when either threshold is reached first.

Defaults come from dynamic vars:

* `datalevin.constants/*wal-group-commit*` (default `100`)
* `datalevin.constants/*wal-group-commit-ms*` (default `100`)

You can set them per database via options:

```clojure
{:wal? true
 :wal-durability-profile :relaxed
 :wal-group-commit 200
 :wal-group-commit-ms 50}
```

Or by dynamic binding (for process defaults):

```clojure
(require '[datalevin.constants :as c]
         '[datalevin.core :as d])

(binding [c/*wal-group-commit* 200
          c/*wal-group-commit-ms* 50]
  (d/open-kv "/tmp/my-kv-db" {:wal? true
                              :wal-durability-profile :relaxed}))
```

For Datalog, top-level WAL options passed to `create-conn`/`get-conn` are
propagated to the underlying KV WAL configuration.
If both top-level options and `:kv-opts` provide the same WAL key, `:kv-opts`
takes precedence.

## How WAL Works

WAL works by tracking a Log Sequence Number (LSN).

In Datalevin WAL:

* Each committed WAL record has an `:lsn`.
* LSNs are positive, strictly increasing, and contiguous (`1, 2, 3, ...`).
* `open-tx-log` reads records ordered by LSN.

You can think of LSN as the canonical commit position in WAL. It is the unit used
for replay, lag tracking, retention floors, and GC safety decisions.

When reading `txlog-watermarks`, these fields are especially important:

* `:last-committed-lsn`: latest LSN accepted into WAL commit stream.
* `:last-durable-lsn`: latest LSN confirmed durable on disk.
* `:last-applied-lsn`: latest LSN known applied/covered by recovery marker state.

At a high level:

1. A write transaction is encoded as a WAL record with a new LSN. Meanwhile, the
   transaction data is also write in LMDB (the DB) in `:nosync` mode to avoid
   expensive `msync`, and the DB serves queries as usual.
2. The WAL record is appended to the active WAL segment file.
3. Durability acknowledgment depends on profile:
   * `:strict`: transaction waits until required sync makes that LSN durable.
   * `:relaxed`: transaction may return before that LSN is durable; sync is batched.
4. Periodically, the DB is synced to disk, and the snapshots of DB are also taken.
5. Periodic WAL segment GC is performed to bound resource usage.
6. On restart/recovery, Datalevin scans WAL segments, validates records, ignores/truncates
   incomplete tail, and replays committed records from the recovered position.

WAL files are segmented and rolled by size/age limits; snapshot and floor metadata
determine which older segments are safe to delete.

## Public WAL API

The public WAL API is intentionally small:

* `txlog-watermarks`
* `open-tx-log`
* `create-snapshot!`
* `list-snapshots`
* `gc-txlog-segments!`

These APIs operate on a KV handle opened with `open-kv`.

### `txlog-watermarks`

Returns runtime WAL watermarks/state for the DB (for example, committed vs
durable progress).

### `open-tx-log`

Reads committed WAL records in LSN order.
Each record is a map including at least `:lsn`, `:tx-time`, `:rows`, and
`:tx-kind`.

```clojure
(d/open-tx-log kv 1)         ; from LSN 1
(d/open-tx-log kv 1 1000)    ; range [1, 1000]
```

### `create-snapshot!`

Creates/rotates LMDB snapshots (`current`/`previous`) and advances snapshot floor
bookkeeping used by retention safety.

### `list-snapshots`

Lists available snapshots and metadata.

### `gc-txlog-segments!`

Deletes old WAL segments that are safe to remove.

```clojure
(d/gc-txlog-segments! kv)        ; normal GC
(d/gc-txlog-segments! kv 5000)   ; keep LSN >= 5000
```
Default WAL retention policy is:

  - :wal-retention-bytes = 8 GiB
  - :wal-retention-ms = 7 days

And enforcement is effectively OR (bytes exceeded or age exceeded) during GC.
Also, retention floors/safety can delay deletion, so actual retained data can
temporarily exceed either cap.

## Typical Operational Loop

For long-running WAL-enabled KV services:

1. Periodically call `create-snapshot!`.
2. Periodically call `gc-txlog-segments!`.
3. Use `txlog-watermarks` and `open-tx-log` for monitoring/replay consumers.
