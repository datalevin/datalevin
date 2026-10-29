# WAL Mode in Datalevin

Datalevin supports write-ahead logging (WAL) for both KV and Datalog writes.
WAL improves write throughput while keeping crash recovery explicit and
operationally manageable.

Concurrent writers receive tangible throughput benefits in WAL mode. The scaling
is sub-linear, e.g. 2 concurrent write threads may produce close to 2X
throughput increase compared with a single write thread, while 4 threads produce
a little over 3X increases.

## Enable WAL

### Datalog

For local Datalog stores, WAL is off by default for new databases. Enable it
explicitly when you want WAL write concurrency and throughput behavior:

```clojure
(require '[datalevin.core :as d])

(def conn
  (d/create-conn
    "/tmp/my-datalog-db"
    {}
    {:wal? true}))
```

When a local Datalog or KV store enables WAL and omits
`:wal-durability-profile`, the default profile is `:strict`. Specify
`:relaxed` only when accepting its bounded crash-loss window for higher
throughput, or `:extra` for stronger-than-default OS sync behavior. The default
can also be written explicitly:

```clojure
(d/create-conn
  "/tmp/my-datalog-db"
  {}
  {:wal? true
   :wal-durability-profile :strict})
```

Existing databases retain their persisted WAL durability profile on reopen.
To move an older `:relaxed` database to the new default, reopen it explicitly
with `:wal-durability-profile :strict`.

Consensus-lease HA databases require WAL and default to `:strict`.

### KV

For direct KV usage (`open-kv`), WAL is also off by default and must be enabled:

```clojure
(require '[datalevin.core :as d])

(def kv
  (d/open-kv
    "/tmp/my-kv-db"
    {:wal? true}))
```

One Datalevin process owns each WAL directory. Multi-process sharing of a WAL
directory is unsupported. HA nodes use separate local WAL directories.

### LMDB flags

Persistent WAL environments default to `:writemap` when opening without an
explicit `:flags` option. This applies to KV and Datalog, embedded and remote,
including reopening a database whose persisted settings enable WAL. WAL also
enables LMDB's `:nosync`; durability follows the selected WAL profile, and
checkpoints explicitly sync LMDB.

To override the writable-map default, supply the complete flag set on open:

```clojure
(d/open-kv "/tmp/my-kv-db"
  {:wal? true :flags #{:nordahead :notls}})

(d/create-conn "/tmp/my-datalog-db" {}
  {:wal? true :kv-opts {:flags #{:nordahead :notls}}})
```

Stores without WAL keep the ordinary LMDB defaults.

## Durability Profiles

WAL supports three durability profiles in WAL:

* `:strict`: each transaction waits for durable WAL acknowledgment, i.e,
  `fsync` success for the WAL segment file. This is the default whenever WAL
  is enabled, including consensus-lease HA.
* `:relaxed`: transactions can return before durability is forced for every
  single write, using batched syncs for a higher throughput. This is an
  explicit opt-in.
* `:extra`: each transaction waits for stricter durability than `:strict`,
  on macOS, that is `fcntl(F_FULLSYNC)`.

Embedded Datalog writes use the same admission collector as embedded KV writes,
for both WAL and durable non-WAL stores. Idle writes take the direct fast path;
under contention, a submitting thread executes the next group. Synchronous
Datalog writes do not hand off to a separate executor or use a queue cooldown.
WAL sync policy remains independent of request collection: `:strict` and
`:extra` wait for their required sync; `:relaxed` retains deferred syncing.

Standalone private-WAL `:strict` and `:relaxed` KV writes and remote KV/Datalog transactions
also collect requests waiting for the writer. The next submitting thread
executes up to `:wal-group-commit` requests in one native transaction and WAL
record. Every caller waits for successful native commit; strict callers also
wait for durable WAL. Relaxed callers retain the configured deferred-sync policy.
Readers cannot see a partially executed group. There is no deliberate batching
delay by default, and `:wal-group-commit-ms` does not delay admission to either group.

Embedded KV and Datalog writes also collect concurrent requests when `:wal?`
is false and LMDB uses its normal durable commit settings. This avoids opening
and committing a native transaction for every waiting request. Temporary,
in-memory, and explicitly asynchronous LMDB environments keep their existing
write path.

For bursts arriving at an idle embedded writer, set `:write-batch-delay-us` to
a short collection window, for example `50`. Its default is `0`, with no
intentional delay. Collection happens before acquiring the native writer and
ends when `:write-batch-size` requests arrive or the window expires. The size
defaults to `:wal-group-commit` (normally 128); both options can be passed to
`open-kv` or as top-level `create-conn` options. The delay accepts 0–1,000,000
microseconds and trades isolated-write latency for sharing commit costs across
nearby requests. A single synchronous producer still needs one durable commit
per call. Actual wakeup time depends on the OS scheduler.

Production embedded WAL collection bounds request counts, not payload bytes.
At most 4,096 requests are admitted at once, and `:write-batch-size` limits the requests
in one batch, capped at 4,096. A request can contain many rows or large values;
its encoded rows, preparation buffers, WAL body and result do not count toward
a payload-byte budget. Existing accepted transaction sizes are preserved.

`:wal-pending-max-bytes`, `:write-batch-max-bytes` and `:wal-rmw-max-bytes` are
internal prototype accounting options. The production opener validates their
values, but they do not impose payload-byte limits on embedded KV or Datalog
writes. Request-count limits therefore do not provide a total memory bound.
Applications needing a memory bound must limit transaction sizes and concurrent
submissions before calling the write APIs, including output from transaction
functions and explicit transaction bodies.

Remote `init-db` and `fill-db` inside an explicit WAL transaction encode each
bulk batch into a spillable buffer. Up to 8 MiB of encoded row bytes stays in
memory per transaction; crossing that limit moves the buffered prefix to a
temporary file and sends subsequent batches directly to that file. Pending row
objects and their encoding arenas can be released between batches. Small imports
create no scratch file. Commit consumes the memory buffer or maps the file and
appends one ordinary WAL record, preserving atomicity and replay; abort discards
the buffer. Scratch files are deleted when their channels close, including on
process exit. Input batches and encoding workspaces still consume memory, and
the WAL record size limit still applies.

The idle collection window works with private WAL `:strict`/`:relaxed`,
`:extra` Datalog writes, and durable non-WAL stores. Native commit and, in strict
or extra WAL mode, WAL sync finish before any caller or transaction listener
receives success. Relaxed WAL retains
its explicitly selected durability policy. Embedded Datalog stores with
synchronous secondary indexes retain their existing path so a failed batch
cannot leave an index ahead of its LMDB data.

This shares commit costs while retaining LMDB's single writer. Explicit
transaction bodies, shared-WAL stores, and writes with HA/commit hooks retain
their existing paths. A body failure aborts the group and retries requests
individually; commit or durability failures are propagated without that retry.
Update functions must remain free of side effects and may execute on another
submitting thread, with their Clojure dynamic bindings preserved.

Exceptions from user transaction bodies, KV update functions and Datalog
transaction functions, including `RuntimeException` and `IllegalStateException`,
abort the uncommitted batch without fencing the environment. Recorded native
failures still fence it even if user code catches or wraps them. JVM errors,
owner interruption and terminal runtime/deadline errors retain their existing
failure handling.

Local Datalog WAL specializes common single-form writes before using the
general transaction planner. A simple cardinality-one `:db.fn/patchIdoc` with
a numeric entity id can be stamped against the active LMDB write snapshot;
homogeneous queued patches are prepared in logical order and committed in one
physical WAL transaction. These batch specializations run through the shared
collector when callers have matching dynamic bindings; other groups execute
each bound operation through the general runner. Each request still receives
its own transaction id and report. Lookup refs, cardinality-many or unique IDoc attributes, attribute
predicates, automatic entity timestamps, and mixed transaction forms retain
the general path and its full validation semantics.

In `:strict`, the durability guarantee follows the OS's `fsync` guidance, which
is not the same for different OS. On macOS, `fsync` is not full durable. Getting
full durability on MacOS requires `:extra`. On Linux and Windows, `:strict` and
`:extra` are mostly the same.

In `:relaxed`, an untimely crash can lose a recent tail of transactions that were
appended but not yet durably synced to disk. The `:relaxed` crash-risk window is
bounded by two thresholds:

* count threshold: `:wal-group-commit` (logical requests per durability batch
  for automatically grouped private-WAL writes)
* time threshold: `:wal-group-commit-ms` (max milliseconds per durability batch)

Batch durability is triggered when either threshold is reached first.

A group of N requests consumes N units of the count threshold, even though it
produces one WAL record. This also applies to the local Datalog request queue.
An explicitly submitted transaction or shared-WAL append retains its existing
one-record count. Group failures that retry requests individually count only
the records actually appended. Recovery and replication still use physical
LSNs: `:pending-count` reports pending WAL records, while `:unsynced-count` in
sync statistics includes the logical request weights. Finishing a sync removes
only the weights covered by its durable LSN.

Defaults come from dynamic vars:

* `datalevin.constants/*wal-group-commit*` (default `128`)
* `datalevin.constants/*wal-group-commit-ms*` (default `10`)

You can set them per database via options:

```clojure
{:wal? true
 :wal-durability-profile :relaxed
 :wal-group-commit 2048
 :wal-group-commit-ms 50}
```

Or by dynamic binding (for process defaults):

```clojure
(require '[datalevin.constants :as c]
         '[datalevin.core :as d])

(binding [c/*wal-group-commit* 2048
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
   * `:extra`: transaction waits for stricter sync semantics than `:strict`.
4. Periodically, the DB is synced to disk, and the snapshots of DB are also taken.
5. Periodic WAL segment GC is performed to bound resource usage.
6. On restart/recovery, Datalevin scans WAL segments, validates records, ignores/truncates
   incomplete tail, and replays committed records from the recovered position.

WAL files are segmented and rolled by size/age limits; snapshot and floor metadata
determine which older segments are safe to delete.

Ordinary Datalog datom adds and retractions use a compact WAL operation: the
entity ID and encoded attribute/value/giant pointer are stored once. Replay
reconstructs both AVE and EAV mutations. Giant-value bodies, schema, metadata,
custom data, and secondary-index jobs remain ordinary KV operations in the same
transaction. The LMDB indexes and strict durability acknowledgment are unchanged.

Compact datom operations use commit-payload format 2 (`DLTX`). The reader also
accepts format 1, and transactions containing only ordinary KV operations still
use format 1. Older binaries cannot read compact format 2 WAL; use an upgraded
reader when recovering or transferring these WAL files. Physical replay rows
remain available through the existing replay interface.

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

HA followers and read replicas use an internal encoded batch request when the
source supports it. The source sends WAL payload bytes without decoding rows;
the receiver verifies the record checksums and decodes the rows for replay.
Older sources fall back to the decoded row request.

Each open WAL runtime reuses matching encoded batches across followers, with
default limits of 32 batches and a 16 MiB budget for payloads and estimated index
key storage. Concurrent fetches of the same window share one payload load.
Current record windows and their sequence boundaries are validated before cache
lookup, so retention and truncation still affect subsequent fetches. The cache
has its own short monitor; payload reads and waits do not hold it or a writer
lock. This changes the replication transport, not the on-disk WAL format or
the public `open-tx-log` result.

### `create-snapshot!`

Creates/rotates LMDB snapshots (`current`/`previous`) and advances snapshot floor
bookkeeping used by retention safety.

### `list-snapshots`

Lists available snapshots and metadata.

### `gc-txlog-segments!`

Deletes old WAL segments that are safe to remove.

```clojure
(d/gc-txlog-segments! kv)        ; explicit GC of every floor-safe segment
(d/gc-txlog-segments! kv 5000)   ; keep LSN >= 5000
(d/gc-txlog-segments! kv {:dry-run? true}) ; plan without deleting
```
Default WAL retention policy is:

  - :wal-retention-bytes = 8 GiB
  - :wal-retention-ms = 7 days

The retention report uses these caps to describe pressure. Explicit
`gc-txlog-segments!` ignores the age/byte caps and deletes every segment permitted
by the snapshot, vector, replica, backup-pin, operator, and applied-LSN floors.
Those floors can keep retained data above either cap.

Dry run returns `:targets`, `:would-delete-count`, and `:would-delete-bytes`
without unlinking files. Apply returns a `:segment-results` entry for each
attempted segment, actual deletion counts/bytes, aggregate `:errors`, and
`:durability`. Missing files are successful already-deleted no-ops. Failures on
one file do not discard other deletions, and a final-report failure retains
those outcomes with `:after nil`.

Selection, deletion, directory sync, and final reporting share the snapshot
maintenance lock. A busy lock returns a zero-deletion `:maintenance-busy`
failure. Directory sync follows actual deletions even when another unlink
failed; no-deletion apply and dry run skip sync. Sync failure or unsupported
directory sync (including Windows) returns `:ok? false` while retaining any
completed deletions. Callers must inspect `:ok?` and the native error list.

For remote administration, use `dtlv admin wal-gc --database NAME --dry-run`
with the URI in `DATALEVIN_URI`. The CLI checks server capabilities and catalog
identity, may open a closed target, and leaves it open. It prints native partial
results and exits `1` on failure. Only WAL-disabled and rollback skips exit `0`.

## Typical Operational Loop

For long-running WAL-enabled KV services:

1. Periodically call `create-snapshot!`.
2. Periodically call `gc-txlog-segments!`.
3. Use `txlog-watermarks` and `open-tx-log` for monitoring/replay consumers.

## Bulk Load Exception (`init-db` / `fill-db`)

`init-db` and `fill-db` are treated as bulk-load paths that do not have
transaction log. When WAL is enabled, Datalevin temporarily switches these
operations to direct LMDB writes (rollback rollout mode) for throughput. As a
result, these writes are not appended to the WAL and will not appear in
`open-tx-log` replay/history.
