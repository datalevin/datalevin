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
| SQLite | `person_id TEXT PRIMARY KEY NOT NULL`, two `TEXT` name columns, and `age INTEGER` |
| SQLite indexed control | The same row plus separate indexes on `first_name`, `last_name`, and `age` |
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

Mixed mode starts with one million people and performs one million closed-loop
read/write pairs. Each pair reads a person by UUID and then upserts a complete
person record containing first name, last name, and age.

Read and write slots are selected independently from 1 through two million,
giving an initial 50% hit/upsert probability. Every pair completes before the
next begins, including when an async Datalevin API is selected. SQLite uses
`INSERT ... ON CONFLICT DO UPDATE`, matching Datalevin identity upsert
semantics.

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
| `sql-tx` | SQLite prepared JDBC batch with one commit per request |
| `sql-tx-indexed` | The same SQLite batch while maintaining the three explicit value indexes |

Append `-wal` to enable WAL, for example `dl-sync-wal`, `dl-async-wal`,
`sql-tx-wal`, or `sql-tx-indexed-wal`.

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
  :dir '"/private/tmp/dtlv-write/dl-sync-wal-b1/dl-sync-wal-1-relaxed"' \
  :f dl-sync-wal \
  :durability-profile :relaxed
```

A mixed run mutates its input database. Prepare an independent identical
database for each measurement.

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

Publishable results use one measurement pass against a fresh database. There
is no discarded warmup pass: database growth, page allocation, and sync costs
are part of the workload. Use the same one-million-record dataset, seed, caller
count, and durability settings for every comparison. Run on an otherwise idle
machine.

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
The base SQLite task writes the row table and the string-primary-key index; its
three non-identity fields have no secondary indexes. The indexed SQLite control
also maintains separate indexes on `first_name`, `last_name`, and `age`, which
provides comparable single-column value lookup coverage while requiring those
indexes to be declared explicitly. The comparison aligns logical records and
API transaction boundaries, not byte-for-byte physical index work.

### Metrics and validation

The measurement CSV reports:

- `Writes` and `Requests`: completed records and API transactions.
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

`Datalog sync` and SQLite are blocking APIs and are compared in the
`Sync comparison` column. Datalog async is reported in the last column.

### Default durability

| Batch | Datalog sync | SQLite | Sync comparison | Datalog async |
|---:|---:|---:|---:|---:|
| 1 | 2,311/s | 2,130/s | Datalog 1.085X | 34,483/s |
| 10 | 4,914/s | 12,093/s | SQLite 2.461X | 97,763/s |
| 100 | 11,482/s | 26,879/s | SQLite 2.341X | 192,474/s |
| 1000 | 44,757/s | 42,361/s | Datalog 1.057X | 190,214/s |

### Relaxed WAL

Datalevin uses its `:relaxed` WAL profile. SQLite uses WAL with
`synchronous=NORMAL`. Neither requests the separately labeled extra macOS
full-sync policy.

| Batch | Datalog sync | SQLite | Sync comparison | Datalog async |
|---:|---:|---:|---:|---:|
| 1 | 10,415/s | 31,348/s | SQLite 3.010X | 91,729/s |
| 10 | 30,650/s | 42,850/s | SQLite 1.398X | 158,650/s |
| 100 | 52,354/s | 47,146/s | Datalog 1.110X | 185,677/s |
| 1000 | 103,386/s | 45,716/s | Datalog 2.261X | 183,090/s |

### SQLite with explicit value indexes

This control creates indexes on `first_name`, `last_name`, and `age` before the
measurement pass. Together with the string-primary-key index, SQLite maintains
value indexes for all four fields in the person record. This is what Datalevin
does by default, so the comparison columns are included.

| Batch | SQLite indexed, default | Default comparison | SQLite indexed, relaxed WAL | Relaxed WAL comparison |
|---:|---:|---:|---:|---:|
| 1 | 1,814/s | Datalog 1.274X | 15,574/s | SQLite 1.495X |
| 10 | 6,233/s | SQLite 1.268X | 19,083/s | Datalog 1.606X |
| 100 | 14,171/s | SQLite 1.234X | 25,757/s | Datalog 2.033X |
| 1000 | 30,722/s | Datalog 1.457X | 33,146/s | Datalog 3.119X |

Both systems commit once per request, but their physical work differs. SQLite
writes a table row and its string-primary-key index, so moving from batch 1 to
10 quickly amortizes its fixed transaction cost. Datalog materializes four
datoms per person in both EAV and AVE, leaving more per-record work after that
amortization. AVE is the automatically maintained attribute-value index that
lets Datalevin query attributes by value without separate index declarations.
Comparable lookup support in SQLite would require developers to create and
maintain additional secondary indexes, as the indexed SQLite control does.

The difference is therefore partly an ergonomics/performance tradeoff: Datalevin
places greater priority on query ergonomics and pays the associated write cost
automatically. With larger batches, Datalevin sorts the EAV and AVE writes into
their respective index orders and benefits increasingly from ordered cursor
writes, so its marginal write cost falls enough to overtake indexed SQLite at
batch 1000 under both reported durability conditions.

## Concurrent

This experiment uses batches 1, 10, 100, and 1000 with relaxed WAL. Each row
writes one million people through `dl-sync-wal` and `sql-tx-wal` on a fresh
database with no warmup pass. Results were measured on 2026-08-29.

| Batch | Threads | Datalog sync | SQLite | Sync comparison |
|---:|---:|---:|---:|---:|
| 1 | 2 | 13,819/s | 43,564/s | SQLite 3.152X |
| 1 | 4 | 18,721/s | 43,753/s | SQLite 2.337X |
| 10 | 2 | 31,926/s | 71,635/s | SQLite 2.244X |
| 10 | 4 | 36,438/s | 74,442/s | SQLite 2.043X |
| 100 | 2 | 63,239/s | 74,050/s | SQLite 1.171X |
| 100 | 4 | 64,869/s | 76,741/s | SQLite 1.183X |
| 1000 | 2 | 106,763/s | 93,391/s | Datalog 1.143X |
| 1000 | 4 | 119,506/s | 88,769/s | Datalog 1.346X |

## Mixed

Publishable mixed read/write results have not yet been collected.

## KV

Publishable KV results have not yet been collected. KV will be reported only
across its own durability conditions.

The PNG files in this directory are legacy results and are not evidence for
the current implementation.
