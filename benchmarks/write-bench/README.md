# Write Benchmark

This benchmark measures end-to-end write throughput and request latency for
Datalevin's Datalog and key-value APIs. It compares Datalog with SQLite through
embedded JDBC where the workloads are equivalent. Transaction parsing, value
encoding, data generation, and result decoding are included.

## Workloads

All workloads use deterministic person records containing a 36-byte UUID
string identity, first name, last name, and age from 18 through 90.

| System | Person representation |
|---|---|
| Datalog | `:person/id`, `:person/first-name`, `:person/last-name`, and `:person/age`; ID is `:db.unique/identity` |
| SQLite | `person_id TEXT PRIMARY KEY NOT NULL`, two `TEXT` name columns, and `age INTEGER`, with separate indexes on `first_name`, `last_name`, and `age` |
| Datalevin KV | UUID string key and a data map containing first name, last name, and age |

SQLite uses an ordinary rowid table. It assigns a signed 64-bit rowid and
maintains a separate unique index for the string primary key; it does not use
the `AUTOINCREMENT` keyword.

### Pure write

Pure write inserts one million people into a fresh database. A batch is one API
transaction containing 1, 10, 100, or 1000 records. The initial records use
even logical key slots mapped deterministically to UUID strings.

SQLite uses one connection-scoped prepared `INSERT`. Each request binds its
records with JDBC `addBatch`, calls `executeBatch`, and commits once with
auto-commit disabled.

### Concurrent

Concurrent mode runs the person-record write workload through multiple callers.
Datalog concurrent writes require WAL. SQLite gives each caller a separate JDBC
connection with a 10-second busy timeout.

### Mixed

Mixed mode starts with one million people and performs one million read/write
pairs. Each pair reads a person by UUID and then upserts a complete person
record containing first name, last name, and age.

Read and write slots are selected independently from 1 through two million,
giving an initial 50% hit/upsert probability. Sync tasks complete each write
before starting the next read. Async Datalevin tasks keep up to 1000 submitted
writes outstanding; subsequent reads use the latest available connection
snapshot without a read-your-write barrier. SQLite uses connection-scoped
prepared lookup and `INSERT ... ON CONFLICT DO UPDATE` statements, matching
Datalevin identity upsert semantics.

The Datalog lookup uses a relation find specification for first name, last
name, and age; it does not use the single-tuple find form.

### KV

KV uses the same generated person identities and values. It has no equivalent
row-store task in this benchmark, so it is compared only across its own
durability conditions and is not part of the Datalog/SQLite ranking.

## Running

Run commands from this directory. Use a disk-backed location and a fresh target
for each measurement.

### Pure write

| Task | API |
|---|---|
| `dl-sync` | Datalog `transact!` |
| `dl-async` | Datalog `transact-async` |
| `sql-tx` | SQLite prepared JDBC batch with one commit per request while maintaining the three explicit value indexes |

Append `-wal` to enable WAL, for example `dl-sync-wal`, `dl-async-wal`,
or `sql-tx-wal`.

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-write/dl-sync-b1"' \
  :batch 1 \
  :f dl-sync \
  > dl-sync-b1.csv
```

For a named WAL durability profile:

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-write/dl-async-wal-b10"' \
  :batch 10 \
  :f dl-async-wal \
  :durability-profile :strict \
  > dl-async-wal-b10.csv
```

### Concurrent

Set `:threads` to the number of callers:

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-write/dl-sync-wal-b1-t8"' \
  :batch 1 \
  :f dl-sync-wal \
  :durability-profile :strict \
  :threads 8
```

Strict-mode commit timeouts invalidate the run.

### Mixed

Mixed mode runs against the database target created by a completed pure-write
measurement:

```bash
clj -X:mixed \
  :dir '"/private/tmp/dtlv-write/dl-sync-wal-b1000/dl-sync-wal-1000-relaxed"' \
  :f dl-sync-wal \
  :durability-profile :relaxed
```

A mixed run mutates its input database. Prepare an independent identical
database for each measurement. Published mixed results use clones of the
batch-1000 initial targets.

### KV

Use `kv-sync`, `kv-async`, or their `-wal` variants with the write entry point:

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-write/kv-sync-b1"' \
  :batch 1 \
  :f kv-sync
```

### Smoke tests and output

Use a smaller total only for smoke testing:

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-write-smoke"' \
  :batch 5 \
  :f dl-async \
  :total 20 \
  :report 0
```

Progress and the manifest path go to stderr; the measurement CSV goes to
stdout. `clj -X:suite` is a compatibility alias for the one-pass protocol. It
accepts `:measurement-writes` as an alias for `:total` and rejects the old
`:warmup-writes` option.

## Methodology

### Measurement protocol

Publishable results use one measurement pass. Pure write starts with a fresh
empty database; mixed starts with an independent clone of its defined
preloaded image. There is no discarded warmup pass: database growth, page
allocation, and sync costs are part of the workload. Use the same dataset,
seed, caller count, and durability settings for every comparison. Run on an
otherwise idle machine.

Data generation is included in end-to-end throughput but excluded from the API
call-latency sample. The default seed is 42. Initial and final record counts are
verified exactly.

The `write` entry point rejects existing targets. With `:base-dir`, it writes a
`results.edn` manifest containing the protocol, database path, durability and
storage configuration, software versions, platform information, and metrics.

Do not use a memory-backed `/tmp` filesystem. On macOS, `/private/tmp` is
normally disk-backed; verify the mount on the benchmark host.

### Durability

| Condition | Datalevin | SQLite |
|---|---|---|
| Default | LMDB default sync | rollback journal, `synchronous=FULL` |
| WAL `:strict` | WAL `fsync` acknowledgment | WAL, `synchronous=FULL` |
| WAL `:relaxed` | bounded group flushing | WAL, `synchronous=NORMAL` |
| WAL `:extra` | extra full sync on supported platforms | WAL, `synchronous=EXTRA`, `fullfsync=ON` |

WAL tasks default to `:relaxed`. Set `:durability-profile` to `:strict`,
`:relaxed`, or `:extra` when a named profile is required. Durability profiles
are rejected for non-WAL tasks.

The benchmark verifies SQLite's journal, synchronous, and full-sync settings
before measuring. On macOS, `:extra` is a separately labeled stronger
durability condition. SQLite documents `synchronous=EXTRA` and
[`fullfsync`](https://sqlite.org/pragma.html#pragma_fullfsync) separately; this
profile enables both.

### Async batching

Datalevin's async APIs adaptively combine queued API requests into physical
transactions. The request count is therefore not a physical commit count.

The benchmark caps outstanding work at 1000 API requests and 100,000 records.
The effective request cap is 1000 for batches 1, 10, and 100, and 100 for batch
1000. Override these with `:in-flight` and `:in-flight-writes`; `results.edn`
records both limits and the effective cap.

### Physical storage work

Datalevin materializes EAV and AVE entries for all four person attributes.
SQLite writes the row table and string-primary-key index and also maintains
separate indexes on `first_name`, `last_name`, and `age`. These provide
comparable single-column value lookup coverage, but must be declared explicitly
by the developer. The comparison aligns logical records and API transaction
boundaries, not byte-for-byte physical index work.

### Metrics and validation

The measurement CSV reports:

- `Writes` and `Requests`: completed records or mixed pairs, and API
  transactions.
- `Time`: elapsed time through the last completion callback.
- `Throughput`: completed records divided by elapsed time.
- `Call Mean`: time spent in the transaction API call; for async tasks this is
  primarily submission time.
- `Completion Mean/P50/P95/P99`: submission-to-callback latency after commit
  acknowledgment.

Completion latency is per API request. Every async request participates in a
completion barrier. A transaction error, timeout, missing callback, SQLite
configuration mismatch, or final count mismatch fails the run.

Run the correctness suite with:

```bash
clj -M:test
```

The shared async executor is tested in the main project with:

```bash
lein test datalevin.async-test
```

## Pure write

Results show throughput: records written per second.

Datalevin rows were rerun on 2026-08-30 with Datalevin 1.1.0 and native
library 0.19.4. SQLite comparator rows are the existing matching runs measured
on 2026-08-29; every SQLite run includes the three explicit value indexes.

`Datalog sync` and SQLite are blocking APIs and are compared in the
`Sync comparison` column. Datalog async is reported in the last column.

### Default durability

| Batch | Datalog sync | SQLite | Sync comparison | Datalog async |
|---:|---:|---:|---:|---:|
| 1 | 2,380/s | 1,814/s | Datalog 1.312X | 37,357/s |
| 10 | 5,157/s | 6,233/s | SQLite 1.209X | 114,960/s |
| 100 | 12,546/s | 14,171/s | SQLite 1.129X | 282,470/s |
| 1000 | 49,436/s | 30,722/s | Datalog 1.609X | 284,146/s |

### Relaxed WAL

Datalevin uses its `:relaxed` WAL profile. SQLite uses WAL with
`synchronous=NORMAL`.

| Batch | Datalog sync | SQLite | Sync comparison | Datalog async |
|---:|---:|---:|---:|---:|
| 1 | 13,094/s | 15,574/s | SQLite 1.189X | 110,755/s |
| 10 | 35,889/s | 19,083/s | Datalog 1.881X | 211,238/s |
| 100 | 63,440/s | 25,757/s | Datalog 2.463X | 275,859/s |
| 1000 | 129,227/s | 33,146/s | Datalog 3.899X | 276,797/s |

## Concurrent

Each row writes one million people through `dl-sync-wal` and `sql-tx-wal` on a
fresh database with no warmup pass, with either 2 or 4 concurrent threads.

| Batch | Threads | Datalog sync | SQLite | Sync comparison |
|---:|---:|---:|---:|---:|
| 1 | 2 | 15,660/s | 22,056/s | SQLite 1.408X |
| 1 | 4 | 22,834/s | 21,997/s | Datalog 1.038X |
| 10 | 2 | 38,088/s | 32,160/s | Datalog 1.184X |
| 10 | 4 | 45,405/s | 30,671/s | Datalog 1.480X |
| 100 | 2 | 73,733/s | 39,402/s | Datalog 1.871X |
| 100 | 4 | 82,273/s | 34,425/s | Datalog 2.390X |
| 1000 | 2 | 123,316/s | 60,981/s | Datalog 2.022X |
| 1000 | 4 | 143,021/s | 61,764/s | Datalog 2.316X |

## Mixed

Results show completed read/write pairs per second. Each engine was populated
once through its batch-1000 pure-write path. Every condition starts from an
independent clone of that engine's one-million-person image, then runs one
million pairs with seed 42, no warmup pass, and one measurement pass. Sync and
SQLite runs are closed-loop; Datalog async keeps its bounded pipeline without a
read-your-write barrier. SQLite maintains the three explicit value indexes.
Corrected Datalevin runs were measured on 2026-08-30; SQLite comparator rows
are retained from the existing matching runs.

`Datalog sync` and SQLite are blocking APIs and are compared in the
`Sync comparison` column. Datalog async is reported separately and keeps a
bounded pipeline of outstanding writes, allowing cross-request adaptive
batching while reads continue against the latest available snapshot.

| Durability | Datalog sync | SQLite | Sync comparison | Datalog async |
|---|---:|---:|---:|---:|
| Default | 1,552/s | 1,515/s | Datalog 1.025X | 7,791/s |
| Relaxed WAL | 5,398/s | 6,731/s | SQLite 1.247X | 20,437/s |

## KV

Publishable KV results have not yet been collected. KV will be reported only
across its own durability conditions.
