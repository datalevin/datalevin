# Write Benchmark

This benchmark measures end-to-end write throughput and request latency for
Datalevin's Datalog and key-value APIs. It also compares Datalog writes with
SQLite through JDBC. The comparison intentionally includes transaction parsing,
value encoding, and result decoding on both sides; it is an embedded API
benchmark, not a raw storage-engine microbenchmark.

The current person-record matrix covers Datalevin Datalog and SQLite in default
and relaxed-WAL modes. KV is intentionally a separate workload: it has no
equivalent row-store task in this benchmark and should be compared only across
its own durability conditions. The PNG files in this directory are legacy
results from an older implementation and are not evidence for the current code.

## Pure-write workload

Each write inserts a deterministic small person record: a 36-byte UUID string
identity, first name, last name, and age from 18 through 90.

- Datalevin Datalog stores `:person/id`, `:person/first-name`,
  `:person/last-name`, and `:person/age`. The string ID is declared
  `:db.unique/identity`, and age uses `:db.type/long`.
- Datalevin KV stores the UUID string as the key and a data map containing first
  name, last name, and age as the value.
- SQLite stores `person_id TEXT PRIMARY KEY NOT NULL`, two `TEXT` name columns,
  and `age INTEGER`. This is an ordinary rowid table, so SQLite auto-assigns a
  signed 64-bit rowid and maintains a separate unique index for the string
  primary key. It deliberately does not use the `AUTOINCREMENT` keyword, whose
  extra sequence bookkeeping is not needed for this equivalence.

SQLite uses one connection-scoped, single-row prepared `INSERT`. Each benchmark
request binds its records with JDBC `addBatch`, calls `executeBatch`, and then
commits once. Auto-commit is disabled. The SQL statement shape and preparation
count are therefore constant across batch sizes; a batch of N differs only in
the number of bound parameter sets executed in its transaction.

The record semantics are now aligned, but the physical index work is still
native to each data model. Datalevin materializes EAV and AVE entries for all
four attributes. SQLite writes the row table and the string-primary-key index;
the three non-identity fields have no secondary indexes. The comparison is
therefore an end-to-end API workload, not a storage-engine test with matched
index counts.

The initial records use even logical key slots; each slot is mapped
deterministically to a UUID string. A batch is one API transaction containing 1,
10, 100, or 1000 records. A final partial batch is supported, so the requested
total need not be divisible by the batch size.

Data generation is included in end-to-end throughput but excluded from the API
call-latency sample. UUIDs, person fields, and mixed-workload key streams are
deterministic for the configured `:seed`, which defaults to 42.

### Person-record results

One million records were written per result on the macOS arm64 benchmark host
with one thread, a fresh database, one measurement pass, and no warmup pass.
All rows were measured on 2026-08-29. The blocking Datalog and SQLite columns
use one API request at a time. Datalog async is reported as an additional API
condition with bounded outstanding work; it is not treated as a blocking-API
storage-engine ratio.

#### Default

| Batch | Datalog sync | SQLite | Sync comparison | Datalog async |
|---:|---:|---:|---:|---:|
| 1 | 2,311/s | 2,130/s | Datalog 1.085X | 34,483/s |
| 10 | 4,914/s | 12,093/s | SQLite 2.461X | 97,763/s |
| 100 | 11,482/s | 26,879/s | SQLite 2.341X | 192,474/s |
| 1000 | 44,757/s | 42,361/s | Datalog 1.057X | 190,214/s |

The blocking throughput scaling between adjacent batch sizes makes the two
crossovers easier to see:

| Batch transition | Datalog sync scaling | SQLite scaling |
|---:|---:|---:|
| 1 to 10 | 2.13X | 5.68X |
| 10 to 100 | 2.34X | 2.22X |
| 100 to 1000 | 3.90X | 1.58X |

Across these adjacent tenfold batch increases, Datalevin's throughput speedup
rises from 2.13X to 2.34X to 3.90X, while SQLite's falls from 5.68X to 2.22X to
1.58X. The measurements do not establish the cause of either progression.

This is a crossover result, not a single overall ranking. For independent
durable writes at batch 1, Datalevin led by 8.5%. SQLite led at the intermediate
batch sizes of 10 and 100 by 2.46X and 2.34X. For large bulk transactions at
batch 1000, Datalevin led by 5.7%. The relevant row is therefore the one closest
to an application's transaction size; averaging the four ratios would not be
meaningful.

#### Relaxed WAL

Datalevin uses its `:relaxed` WAL profile. SQLite uses WAL with
`synchronous=NORMAL`; neither side requests the separately labeled extra macOS
full-sync policy.

| Batch | Datalog sync | SQLite | Sync comparison | Datalog async |
|---:|---:|---:|---:|---:|
| 1 | 10,415/s | 31,348/s | SQLite 3.010X | 91,729/s |
| 10 | 30,650/s | 42,850/s | SQLite 1.398X | 158,650/s |
| 100 | 52,354/s | 47,146/s | Datalog 1.110X | 185,677/s |
| 1000 | 103,386/s | 45,716/s | Datalog 2.261X | 183,090/s |

For the blocking APIs, SQLite led at batches 1 and 10, while Datalog led at
batches 100 and 1000. The Datalog async condition had the highest measured
throughput at every batch size. These rows report the observed outcomes and do
not assign a cause to their batch-size progressions.

The crossover should not be interpreted as a general storage-engine ranking.
This is an end-to-end API workload, and the physical index work differs:
Datalevin materializes all four attributes in EAV and AVE, while SQLite writes
a row and its string-primary-key index. The measurements show the outcome at
each batch size, but do not isolate every component of the crossover.

SQLite 3.48.0 reported rollback journal mode with `synchronous=FULL` for the
default rows and WAL with `synchronous=NORMAL` for relaxed WAL. Both reported
`fullfsync=OFF`, `checkpoint_fullfsync=OFF`, and a 4 KiB page size. Its schema
inspection showed one million distinct string IDs, an auto-assigned rowid, and
the separate `sqlite_autoindex_person_1` primary-key index. At default batch
1000, the final Datalevin database was 321.1 MiB and SQLite database was 112.0
MiB. Datalevin still writes substantially more physical index data because all
four attributes are present in both EAV and AVE.

All 24 manifests use the same one-pass protocol and recorded environment. The
current Datalog/SQLite matrix is complete for default and relaxed-WAL modes.
Strict and extra WAL are different durability experiments, not missing rows in
these tables.

### Person-record comparison tasks

| Task | API |
|---|---|
| `dl-sync` | Datalog `transact!` |
| `dl-async` | Datalog `transact-async` with bounded outstanding requests and writes |
| `sql-tx` | SQLite reusable prepared `INSERT`, JDBC batch, one commit/request |

Append `-wal` to any task to enable WAL mode, for example `dl-sync-wal` or
`sql-tx-wal`.

### Separate KV workload

| Task | API |
|---|---|
| `kv-sync` | KV `transact-kv` |
| `kv-async` | KV `transact-kv-async` with bounded outstanding requests |

KV tasks also accept the `-wal` suffix. Report KV independently across its
durability conditions; do not place its results in the Datalog/SQLite ranking.

Datalevin's async APIs adaptively combine queued API requests into physical
transactions. The reported request count is therefore not a count of physical
commits. The benchmark caps outstanding work at both 1000 API requests and
100,000 writes. The effective request cap is the smaller of those limits: it is
1000 for batches 1, 10, and 100, and 100 for batch 1000. Override the limits
with `:in-flight` and `:in-flight-writes`; `results.edn` records both configured
limits and the effective request cap.

## Durability conditions

The publishable default comparison respects the operating system's ordinary
sync policy. It does not request an additional macOS full sync:

| Condition | Datalevin | SQLite |
|---|---|---|
| Default | LMDB default sync | rollback journal, `synchronous=FULL` |
| WAL `:strict` | WAL `fsync` acknowledgment | WAL, `synchronous=FULL` |
| WAL `:relaxed` | bounded group flushing | WAL, `synchronous=NORMAL` |
| WAL `:extra` | extra full sync on supported platforms | WAL, `synchronous=EXTRA`, `fullfsync=ON` |

WAL tasks default to `:relaxed`, matching explicit local WAL use in the
Datalevin API; the paired SQLite setting is WAL with `synchronous=NORMAL`. Set
`:durability-profile` explicitly to `:strict`, `:relaxed`, or `:extra` when a
named profile is important. Use explicit `:strict` for a durability-matched
comparison in which each transaction waits for WAL `fsync` acknowledgment.
Durability profiles are rejected for non-WAL tasks.

The benchmark queries SQLite after configuration and fails unless the requested
journal, synchronous, and full-sync settings took effect. On macOS,
`:extra` is a separately labeled stronger-durability experiment; it is not part
of the ordinary-fsync default comparison. SQLite documents `synchronous=EXTRA`
and [`fullfsync`](https://sqlite.org/pragma.html#pragma_fullfsync) as separate
settings; the benchmark configures both for this profile.

## Metrics

The measurement CSV contains one final row with:

- `Writes` and `Requests`: successfully completed writes and API transactions.
- `Time`: elapsed time from the start of workload generation through the last
  completion callback.
- `Throughput`: completed writes divided by that elapsed time.
- `Call Mean`: mean time spent in the transaction API call. For async APIs this
  is primarily submission time.
- `Completion Mean/P50/P95/P99`: time from submission until that particular API
  request's success callback runs after its configured commit acknowledgment.

Completion latency is per API request. It is not derived by dividing a grouped
commit by the number of callbacks.

Progress is written to stderr every 10,000 completed writes by default, keeping
stdout valid CSV. Use `:report 0` to disable progress output.

Every async request is accounted for with a completion barrier. Both successful
and failed async operations release their benchmark permits. Any transaction
error, timeout, missing callback, SQLite configuration mismatch, or final count
mismatch fails the process instead of producing a usable result row.

## Required protocol

Publishable write results use one measurement pass against a fresh database.
There is no discarded warmup pass: database growth, page allocation, and sync
costs are part of the workload rather than JVM steady-state effects to warm
away. Use the same one-million-write dataset, seed, thread count, and durability
settings for every system and batch size. Run on an otherwise idle machine and
a disk-backed benchmark directory.

The `write` entry point rejects existing database targets rather than silently
measuring overwrites. When `:base-dir` is supplied it also writes `results.edn`,
recording the one-pass protocol, exact database path, effective durability,
storage configuration, software versions, platform information, and metrics.

Do not use a memory-backed `/tmp` filesystem. On macOS, `/private/tmp` is
normally disk-backed; verify the mount on the machine being measured.

## Running a measurement

Run from this directory. Give each task/batch combination a new base directory:

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-write-20260828/dl-sync-b1"' \
  :batch 1 \
  :f dl-sync \
  > dl-sync-b1.csv
```

An explicit strict async WAL example:

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-write-20260828/dl-async-wal-b10"' \
  :batch 10 \
  :f dl-async-wal \
  :durability-profile :strict \
  > dl-async-wal-b10.csv
```

SQLite WAL uses the same protocol:

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-write-20260828/sqlite-wal-b10"' \
  :batch 10 \
  :f sql-tx-wal \
  :durability-profile :strict \
  > sqlite-wal-b10.csv
```

Progress and the manifest location go to stderr; the measurement row goes to
stdout. Smaller write counts are useful only for smoke testing:

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-write-smoke"' \
  :batch 5 \
  :f dl-async \
  :total 20 \
  :report 0
```

`clj -X:suite` remains as a compatibility alias for the same one-pass protocol
and accepts legacy `:measurement-writes` as an alias for `:total`. It rejects
`:warmup-writes` so a two-pass run cannot be mistaken for the reported method.

## Multi-thread write ingress

Set `:threads` to use multiple callers. Datalog multi-thread runs require WAL;
KV and SQLite permit default or WAL modes. SQLite gives each caller its own JDBC
connection with a 10-second busy timeout.

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-write-20260828/dl-sync-wal-b1-t8"' \
  :batch 1 \
  :f dl-sync-wal \
  :durability-profile :strict \
  :threads 8
```

Strict-mode commit timeouts are not skipped. A timeout invalidates the run.

## Mixed read/write workload

Mixed mode starts with a pure-write database containing one million people,
then performs one million read/write pairs. Read and write logical key slots are
in the range 1 through two million and are deterministically mapped to UUID
strings, giving an initial 50% hit/upsert probability. The exact key streams are
seeded. An upsert replaces first name, last name, and age.

Mixed mode is deliberately closed-loop: every read/write pair commits before
the next pair begins. Selecting an async Datalevin API still awaits its returned
future. This preserves the same read-your-writes semantics as SQLite instead of
letting Datalevin reads race ahead of queued writes.

SQLite uses `INSERT ... ON CONFLICT DO UPDATE`, matching Datalevin identity
upsert semantics rather than SQLite's delete-and-reinsert `OR REPLACE` behavior.
The initial and final exact entity counts are verified.

Run mixed mode against the measurement database recorded in `results.edn`:

```bash
clj -X:mixed \
  :dir '"/private/tmp/dtlv-write-20260828/dl-sync-wal-b1/dl-sync-wal-1-strict"' \
  :f dl-sync-wal \
  :durability-profile :strict
```

A mixed run mutates its input database. Prepare an independent identical
database for every reported measurement; never reuse a database from an earlier
mixed run.

## Bulk initialization

`dl-init` loads the same deterministic person dataset through
`init-db`/`fill-db`. It uses the same string unique-identity schema and rejects
an existing target:

```bash
clj -X:dl-init :dir '"/private/tmp/dtlv-init"'
```

Bulk initialization is a separate workload and must not be placed in the same
table as transactional API throughput.

## Tests

Run the benchmark's correctness suite from this directory:

```bash
clj -M:test
```

The suite includes generated batch-accounting cases; timeout, callback, metric,
CSV, and validation checks; SQLite PRAGMA verification; and small temporary
database runs through every KV, Datalog, SQLite, async, default-relaxed-WAL, and
explicit-strict-WAL adapter. It also exercises multi-caller SQLite, mixed mode,
bulk initialization, fresh target rejection, and the one-pass manifest. These
are correctness probes with only a few writes, not performance measurements.
Temporary databases are removed after each test.

The shared asynchronous executor contract is tested with the main project:

```bash
lein test datalevin.async-test
```
