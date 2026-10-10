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
maintains a separate unique index for the string primary key, and main index for
other three columns.

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
record containing first name, last name, and age. Read/write operations are 50/50
proportion.

Read and write slots are selected independently from 1 through two million,
giving an initial 50% hit/upsert probability. Sync tasks complete each write
before starting the next read. Async Datalevin tasks keep up to 1000 submitted
writes outstanding; subsequent reads use the latest available connection
snapshot without a read-your-write barrier. SQLite uses connection-scoped
prepared lookup and `INSERT ... ON CONFLICT DO UPDATE` statements, matching
Datalevin identity upsert semantics.

### KV

KV uses the same generated person identities and values. It has no equivalent
row-store task in this benchmark, so it is compared only across its own
durability conditions and is not part of the Datalog/SQLite comparison.

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

The cache-oriented non-durable study uses only non-WAL `kv-sync`. Select one
of the three named LMDB flag policies with `:kv-non-durable-profile`:

```bash
clj -X:write \
  :base-dir '"/private/tmp/dtlv-kv-nosync-b100"' \
  :batch 100 \
  :f kv-sync \
  :kv-non-durable-profile :nosync
```

Accepted values are `:nometasync`, `:nosync`, and `:writemap-mapasync`.
The benchmark rejects this option for WAL, async, Datalog, and SQLite tasks and
records both the selected profile and effective LMDB flags in `results.edn`.

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

WAL tasks default to `:strict`. Set `:durability-profile` to `:relaxed` for
batched durability or `:extra` for stronger-than-default OS sync behavior.
Durability profiles are rejected for non-WAL tasks.

The separate non-durable KV study uses the documented non-WAL LMDB policies:

- `:nometasync` omits synchronous metadata-page flushing; the last transaction
  can be lost after a system crash while database integrity is retained.
- `:nosync` leaves data-page flushing to the operating system and can corrupt
  the database after an untimely system crash.
- `:writemap-mapasync` enables a writable memory map with asynchronous map
  flushing and has the additional writable-map safety and allocation caveats.

These profiles are intended to represent cache-like uses, so the benchmark
deliberately measures them only through synchronous, non-WAL KV writes.

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

## October 10, 2026 regression investigation

The published tables below are the historical reference. The comparison here
uses those numbers, with the same one-million-person workload, seed 42, one
caller, batch sizes, and async limits. Each current case uses a fresh database,
no warmup, Java 21.0.12.1, a 4 GB initial/maximum heap, and native artifacts
1.1.6. [All 16 final Datalog WAL cases](results/2026-10-10-fused-ordered/comparison.csv)
include throughput before fusion, the final throughput, ratio to the published
reference, and completion P99. Commands, manifests, and source hashes are
retained alongside the CSV. Historical heap settings and host activity were
not captured, so the remaining difference is not attributed entirely to source
changes.

Profiling identified unnecessary sorted resolver indexes on simple collector
writes. Tree-map frames occurred in 216 of 515 sampled async-worker Java stacks;
custom-schema comparison frames occurred in 143, overlapping those samples.
The fixes use ordinary comparators for ordinary schemas, build pending E/A and
unique AV hash indexes only when a later request needs them, and materialize
sorted indexes before general resolution. Unconditional storage encoding can
be deferred; native add-only index passes already supply their ordering.
The [initial index/encoding fixes](results/2026-10-10-investigation/comparison.csv)
recovered much of the first regression.

Eligible ordinary inserts now also enforce uniqueness during the AVE put,
as the archived pre-refactor grouped writer did. Unique keys are inserted in
AVE order before other rows. A collision removes only the keys inserted by that attempt, allowing
identity-upsert resolution within the same writer. Transaction IDs and resolver
state advance after success, and rejected rows never enter frozen replay or
WAL. Earlier transaction functions are not re-executed. Auxiliary/custom index
paths retain preflight validation.

| Profile | API | Batch | Published | Before fixes | After fixes | After / published |
|---|---|---:|---:|---:|---:|---:|
| Strict | Sync | 1000 | 114,739/s | 78,295/s | 118,875/s | 1.036X |
| Strict | Async | 1 | 102,133/s | 71,555/s | 101,892/s | 0.998X |
| Strict | Async | 10 | 195,976/s | 91,529/s | 206,374/s | 1.053X |
| Strict | Async | 100 | 245,485/s | 86,249/s | 250,401/s | 1.020X |
| Strict | Async | 1000 | 238,974/s | 81,139/s | 234,532/s | 0.981X |
| Relaxed | Sync | 1000 | 129,227/s | 78,599/s | 118,421/s | 0.916X |
| Relaxed | Async | 1 | 110,755/s | 71,394/s | 104,238/s | 0.941X |
| Relaxed | Async | 10 | 211,238/s | 92,580/s | 205,868/s | 0.975X |
| Relaxed | Async | 100 | 275,859/s | 83,068/s | 242,606/s | 0.879X |
| Relaxed | Async | 1000 | 276,797/s | 81,535/s | 238,189/s | 0.861X |

Batch-1000 async improves by 2.89X strict and 2.92X relaxed over the regressed
implementation. In this run, every strict sync case exceeds its published
reference; strict async is within 2% of its reference or above it. Relaxed async
still falls 2.5–13.9% below its reference, and batch-1000 relaxed sync is 8.4%
below. The published relaxed throughput has not been fully recovered.

[Diagnostic controls](results/2026-10-10-investigation/diagnostics.csv) did not
close the gap by reducing the async form cap to 10,000/25,000 or using the
JVM default heap. Before restoring fusion, batch-1000 default non-WAL async
reached 202,741/s versus its published 284,146/s, indicating that the initial
remaining cost was not mainly WAL fsync. Native sampling found 32 of 100 worker
samples in `mdb_get`, used by identity preflight. An isolated unsafe bypass
reached 250,404/s strict and 247,040/s relaxed; those results measured current
probe cost and did not establish its contribution to the historical regression.
The older code also contained preflight checks, but skipped them for eligible
fused inserts.

The [first safe fused matrix](results/2026-10-10-fused-uniqueness/comparison.csv)
used entity-order reservations. Moving them to AVE order reached 226,136/s and
240,765/s in strict batch-1000 diagnostic trials, with an entity-order control
at 216,430/s; relaxed reached 237,177/s. Those commands and hashes are under
[the ordering diagnostics](results/2026-10-10-fused-uniqueness/diagnostics/).
The final matrix above uses the tested AVE-order implementation. Compared with
the initial index/encoding fixes, it improves async throughput by 6–23% while
retaining uniqueness validation.

Validation: 112 affected Datalog, custom-schema, batch, recovery, and WAL tests
passed with 1,629 assertions; the benchmark harness passed 49 tests with 418
assertions. New tests cover partial uniqueness rollback, identity-upsert
collisions with persisted and preceding batch writes, value-unique rejection,
transaction functions executing once, and map growth after a rejected attempt.
The collector test helper now preserves existing phase observers so the resize
test verifies that growth actually occurred. Full `clj-kondo --lint src test`
reported zero errors and 56 existing warnings. Every reported benchmark
verified its final record count before closing the database.

A [focused async follow-up](results/2026-10-10-async-investigation/findings.md)
found that over 94% of measured native-batch time preceded WAL append. Eligible
fused inserts execute before dispatch so collisions can fall back safely; their
large native writes consequently do not overlap WAL. The strict and relaxed
traces used 20 and 21 physical transactions for 1,000 API requests, with different
adaptive batch-size distributions. Strict append, including synchronous-write
durability I/O, took 83 ms in aggregate versus 46 ms relaxed. Relaxed policy
completion did not wait for fsync. Repeated profile throughput varied enough
that the small current strict/relaxed difference is not a reliable durability
penalty estimate.

The follow-up removes repeated region-boundary binary searches from native
preparation by scanning `RowRegions` sequentially. Two untraced default-cap
trials averaged 246,139/s strict and 243,550/s relaxed; the relaxed improvement
over paired controls was 2.5%, close to trial variation. With this fix, a
diagnostic 25,000-form cap reached 259,350/s and 255,675/s relaxed, averaging
0.930X its published batch-1000 reference. A 10,000-form cap was slower. The
production cap remains 100,000; these focused trials do not replace the complete
matrix above. Before the non-WAL fusion follow-up below, its grouped path still
used preflight and the control reached 186,929/s. The retained region
change passed 125 affected tests with 1,787 assertions; lint reported zero
errors and the same 56 warnings. Commands, phase traces, profiles, and manifests
are linked in the follow-up.

Eligible non-WAL grouped inserts now also use safe fused uniqueness checks,
skipping separate identity preflight and identity-AV collection. Collision
rollback permits upsert fallback within the same native writer. The
[non-WAL follow-up](results/2026-10-10-non-wal-fusion/findings.md) uses the same
fresh one-million-person workload and unchanged async limits:

| Async batch | Published README | With fusion | Fusion / published |
|---|---:|---:|---:|
| 1 | 37,357/s | 32,247/s | 0.863X |
| 10 | 114,960/s | 105,203/s | 0.915X |
| 100 | 282,470/s | 225,213/s | 0.797X |
| 1000 | 284,146/s | 227,646/s | 0.801X |

Batch 1000 averages two trials and improves 18.0% over paired controls averaging
192,951/s. Other batch sizes have one trial each. The historical deficit remains;
the published tables below are preserved. Source hashes, commands, manifests,
completion latency, and exact ratios are retained in the linked follow-up.
Validation passed 128 affected tests with 1,828 assertions; lint reported zero
errors and the same 56 existing warnings.

A [deeper non-WAL investigation](results/2026-10-10-non-wal-deep/findings.md)
reproduced the deficit: three unchanged-code controls averaged 226,470/s,
20.3% below the published batch-1000 reference. Instrumentation measured
995 ms in EAV insertion, 715 ms in conditional unique AVE insertion, 411 ms in
storage preparation, 402 ms in AVE sorting and 473 ms in native commit.
Restoring archived index ordering was slower. Encoding-cache and raw-buffer
experiments produced only small gains; serial sorting was slower. A tenfold
larger pipeline reached 241,732/s but changed the protocol and raised completion
P99 to four seconds. No production changes were retained from these experiments;
the historical deficit remains unresolved.
The preparation timing is corrected to remove double-counted nested arity
delegation in the original profiler.
A [matched write-stage comparison](results/2026-10-10-write-stage-comparison/findings.md)
found identical native put primitives and per-datom storage encoders. With ten
fixed physical transactions and conditional uniqueness enabled in both variants,
EAV plus AVE insertion totaled 1,803 ms current versus 1,809 ms with archived
index ordering. The native executor differed by less than 0.4%; these stages
do not establish the cause of the remaining historical throughput gap.

[Further elapsed-time accounting](results/2026-10-10-gap-accounting/findings.md)
identified repeated schema-dependent attribute support and fusion eligibility
checks. Caching those decisions per preparation call reduces blind preparation
from 598 ms to 286 ms per million entities while retaining value validation and
all fusion guards. Indexed traversal of ordinary lists reduces the trailing
metadata scan from 63 ms to 32 ms; region collections retain sequential traversal.
Two uninstrumented candidate trials average **253,862/s**, versus **231,106/s**
for two interleaved controls, a **9.8% gain** with unchanged async limits.
Candidate throughput is **0.893X** the published 284,146/s, leaving a **10.7%**
deficit. Native insertion comparisons and the roughly 25 ms outside transaction
bodies and commits do not account for the remaining historical difference.
These focused trials do not replace the batch-size matrices above.
The retained changes pass 130 affected tests with 1,823 assertions; lint reports
zero errors and the same 56 existing warnings.

## Pure write

Results show throughput: records written per second.

### Default sync

`Datalog ` and `SQLite `use the default blocking APIs of respective systems and
are compared in the `Sync comparison` column.

Datalog async condition is reported in the last column.

These are all fully durable writes.

| Batch | Datalog | SQLite | Sync comparison | Datalog async |
|---:|---:|---:|---:|---:|
| 1 | 2,380/s | 1,814/s | Datalog 1.312X | 37,357/s |
| 10 | 5,157/s | 6,233/s | SQLite 1.209X | 114,960/s |
| 100 | 12,546/s | 14,171/s | SQLite 1.129X | 282,470/s |
| 1000 | 49,436/s | 30,722/s | Datalog 1.609X | 284,146/s |

### WAL

These are write ahead log (WAL) write modes.

#### Strict durability

Datalevin uses its `:strict` WAL profile and SQLite uses WAL with
`synchronous=FULL`.

Datalog WAL strict async condition is reported in the last column.

These are fully durable write.

| Batch | Datalog strict WAL | SQLite FULL WAL | Strict WAL comparison | Datalog WAL strict async |
|---:|---:|---:|---:|---:|
| 1 | 8,186/s | 8,822/s | SQLite 1.078X | 102,133/s |
| 10 | 26,519/s | 17,609/s | Datalog 1.506X | 195,976/s |
| 100 | 44,747/s | 22,840/s | Datalog 1.959X | 245,485/s |
| 1000 | 114,739/s | 32,105/s | Datalog 3.574X | 238,974/s |

#### Relaxed durability

Datalevin uses its `:relaxed` WAL profile and SQLite uses WAL with
`synchronous=NORMAL`.

Datalog WAL relaxed async condition is reported in the last column.

These modes can lose recently acknowledged transactions after an OS crash or
power loss, so they are kept separate from the strict durability comparison.

| Batch | Datalog relaxed sync | SQLite NORMAL WAL | Relaxed sync comparison | Datalog relaxed async |
|---:|---:|---:|---:|---:|
| 1 | 13,094/s | 15,574/s | SQLite 1.189X | 110,755/s |
| 10 | 35,889/s | 19,083/s | Datalog 1.881X | 211,238/s |
| 100 | 63,440/s | 25,757/s | Datalog 2.463X | 275,859/s |
| 1000 | 129,227/s | 33,146/s | Datalog 3.899X | 276,797/s |

Relative to relaxed WAL, strict synchronous acknowledgment costs most at small
batches. The async pipeline combines queued API requests into physical
transactions, reducing the strict penalty to 7.2% through 13.7%.

## Concurrent

Both Datalevin and SQLite are single writer in the default synchronous mode, so
only WAL mode has any benefit in concurrent writes. The measurements use either
2 or 4 concurrent caller threads.

### Strict durability

Datalevin uses its `:strict` WAL profile and SQLite uses WAL with
`synchronous=FULL`.

| Batch | Threads | Datalog strict WAL | SQLite FULL WAL | Strict WAL comparison |
|---:|---:|---:|---:|---:|
| 1 | 2 | 13,317/s | 8,175/s | Datalog 1.629X |
| 1 | 4 | 19,003/s | 7,557/s | Datalog 2.514X |
| 10 | 2 | 35,168/s | 23,314/s | Datalog 1.508X |
| 10 | 4 | 42,827/s | 25,003/s | Datalog 1.713X |
| 100 | 2 | 71,594/s | 39,689/s | Datalog 1.804X |
| 100 | 4 | 81,618/s | 43,305/s | Datalog 1.885X |
| 1000 | 2 | 127,638/s | 45,996/s | Datalog 2.775X |
| 1000 | 4 | 151,622/s | 50,735/s | Datalog 2.988X |

### Relaxed durability (not strictly durable)

Datalevin uses its `:relaxed` WAL profile and SQLite uses WAL with
`synchronous=NORMAL`.

| Batch | Threads | Datalog relaxed WAL | SQLite NORMAL WAL | Relaxed WAL comparison |
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

`Datalog sync` and SQLite are blocking APIs and are compared in the
`Sync comparison` column. Datalog async is reported separately and keeps a
bounded pipeline of outstanding writes, allowing cross-request adaptive
batching while reads continue against the latest available snapshot.

### Strictly durable paths

The default row compares Datalevin's default LMDB path with SQLite's rollback
journal and `synchronous=FULL`. The strict-WAL row compares Datalevin `:strict`
with SQLite WAL and `synchronous=FULL`.

| Durability | Datalog sync | SQLite sync | Sync comparison | Datalog async |
|---|---:|---:|---:|---:|
| Default | 1,552/s | 1,515/s | Datalog 1.025X | 7,791/s |
| Strict WAL | 5,616/s | 5,041/s | Datalog 1.114X | 16,996/s |

### Relaxed durability (not strictly durable)

| Durability | Datalog sync | SQLite sync | Sync comparison | Datalog async |
|---|---:|---:|---:|---:|
| Relaxed WAL | 6,965/s | 6,731/s | Datalog 1.035X | 20,437/s |

## KV

Results show throughput: records written per second. Every row writes one
million records to a fresh database with seed 42, no warmup pass, and one
measurement pass. Measurements were collected on 2026-08-31 with Datalevin
1.1.0, Java 21.0.11, and a 12-core Apple Silicon macOS host. Background media
analysis was paused during each measurement phase and resumed immediately
afterward to keep host load stable.

KV has no equivalent SQLite task in this benchmark, so these tables compare
only Datalevin KV API and durability conditions.

### Strictly durable paths

The default LMDB path and the `:strict` WAL profile both preserve acknowledged
commits across an OS crash or power loss. Async tasks use the bounded pipeline
described in [Async batching](#async-batching).

| Batch | Default sync | Strict WAL sync | Default async | Strict WAL async |
|---:|---:|---:|---:|---:|
| 1 | 8,204/s | 16,289/s | 53,705/s | 148,167/s |
| 10 | 18,684/s | 79,044/s | 110,259/s | 215,406/s |
| 100 | 40,043/s | 143,404/s | 359,055/s | 294,525/s |
| 1000 | 57,990/s | 171,421/s | 336,221/s | 300,193/s |

Strict WAL is still 1.99X through 4.23X faster than default synchronous LMDB
durability.

### Relaxed WAL (not strictly durable)

| Batch | Relaxed WAL sync | Relaxed WAL async |
|---:|---:|---:|
| 1 | 30,719/s | 166,730/s |
| 10 | 106,721/s | 233,724/s |
| 100 | 164,165/s | 330,084/s |
| 1000 | 184,117/s | 334,558/s |

Relaxed WAL improves synchronous throughput by 3.18X through 5.71X over
default LMDB durability. For async writes, its advantage is concentrated at
batches 1 and 10; the default and relaxed-WAL paths converge at batches 100
and 1000 as encoding and storage work dominate. Its synchronous gap to strict
WAL falls sharply as the batch amortizes each acknowledgment. Async grouping
keeps the strict penalty between 7.8% and 11.1% across all tested batch sizes.

### Mixed strict WAL

| Durability | KV sync | KV async | Async speedup |
|---|---:|---:|---:|
| Strict WAL | 12,690/s | 112,114/s | 8.835X |

The synchronous path completes each lookup and strictly acknowledged upsert
before starting the next pair. The async path uses the same bounded
1,000-request pipeline as the Datalog mixed study.

### Cache-oriented non-durable paths

These are synchronous, non-WAL `kv-sync` measurements. Default durability is
repeated as the baseline. The safety tradeoffs of the other columns are
described in [Durability](#durability); these conditions are not durability
equivalents.

| Batch | Default | `nometasync` | `nosync` | `writemap` + `mapasync` |
|---:|---:|---:|---:|---:|
| 1 | 8,204/s | 8,400/s | 56,771/s | 93,938/s |
| 10 | 18,684/s | 16,279/s | 147,563/s | 209,732/s |
| 100 | 40,043/s | 40,095/s | 202,438/s | 254,537/s |
| 1000 | 57,990/s | 58,079/s | 223,205/s | 271,192/s |

`nometasync` provides no consistent throughput improvement in this one-pass
study. `writemap` + `mapasync` is the fastest synchronous condition at every
batch size, reaching 271,192 writes/s at batch 1000 and exceeding `nosync` by
1.22X through 1.65X.
