# YCSB-style KV and Datalog benchmark

Runs the A–F operation mixes against both Datalevin APIs, either embedded or
through a managed loopback server. It also compares embedded Datalog with
SQLite and remote Datalog with PostgreSQL. This is a standalone Clojure harness using
the checkout at `../..`; it adds no benchmark code or dependencies to production.
It is inspired by the [YCSB core workloads](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads),
not an official YCSB binding or a directly comparable YCSB score.

## Run

From this directory, using the Clojure CLI and the repository's supported JDK:

```sh
clojure -M:jvm:test

# Smoke-test all 24 combinations with the default ten 100-byte fields.
clojure -M:jvm:bench --api all --mode all --workload all \
  --records 100 --ops 200 --warmup 50 --threads 3 \
  --output /tmp/datalevin-ycsb-smoke.edn

# Larger comparison of KV and Datalog, embedded and remote, for workload A.
clojure -J-Xms4g -J-Xmx4g -M:jvm:bench \
  --api all --mode all --workload A --records 100000 \
  --ops 100000 --warmup 20000 --threads 8 --seed 17 \
  --output /tmp/datalevin-ycsb-a.edn

# Read-only Datalog over TCP, with an explicit connection pool size.
clojure -M:jvm:bench --api datalog --mode remote --workload C \
  --threads 8 --pool-size 8 --distribution uniform

clojure -M:jvm:bench --help
```

On macOS, the harness uses the shared `benchmarks/host-control` helper to pause
the current user's `mediaanalysisd` and `photoanalysisd` before running the
selected cases. It resumes the processes it stopped after the cases finish,
including when a case throws. Processes already stopped are left stopped.
This covers load, warmup, measurement, and validation for every selected engine;
other platforms perform no process control.

## SQL comparisons

`--system all` runs each selected Datalog case followed by its SQL counterpart:

| Mode | Pair |
| --- | --- |
| Embedded | Datalevin Datalog and SQLite |
| Remote | Datalevin Datalog and PostgreSQL |

```sh
# Embedded Datalog versus SQLite, all six workloads.
clojure -M:jvm:bench --system all --api datalog --mode embedded \
  --workload all --records 100000 --ops 100000 --warmup 20000 --threads 8 \
  --output /tmp/datalevin-ycsb-sqlite.edn

# Remote Datalog versus PostgreSQL; use a disposable PostgreSQL database.
export YCSB_PG_URL=jdbc:postgresql://127.0.0.1:5432/ycsb
export YCSB_PG_USER=benchmark
# Set YCSB_PG_PASSWORD separately if required by the server.
clojure -M:jvm:bench --system all --api datalog --mode remote \
  --workload all --records 100000 --ops 100000 --warmup 20000 --threads 8 \
  --output /tmp/datalevin-ycsb-postgres.edn

# Exercise both comparison pairs with small inputs: 24 cases.
clojure -M:jvm:bench --system all --api datalog --mode all --workload all \
  --records 100 --ops 200 --warmup 50 --threads 3 \
  --output /tmp/datalevin-ycsb-comparison-smoke.edn
```

The default `--system datalevin` preserves Datalevin-only runs. `--system sqlite`
and `--system postgres` run just the corresponding SQL cases, allowing separate
process invocations and alternate engine ordering. KV cases remain Datalevin
only; `--system all --api all --mode all --workload all` runs 36 cases.

PostgreSQL must already be running. `--pg-url` and `--pg-user` override
`YCSB_PG_URL` and `YCSB_PG_USER`; the URL defaults to
`jdbc:postgresql://127.0.0.1:5432/postgres`. Passwords come from
`YCSB_PG_PASSWORD`. The role needs permission to create a schema in the target
database. Connectivity and durability are checked before the comparison starts.
Each PostgreSQL case creates a unique `ycsb_<uuid>` schema, uses ordinary logged
tables, and drops only that schema on completion or failure. `--keep-db` retains
it and records its name. JDBC URLs and usernames are omitted from reports.
SQLite uses a fresh temporary file with the same retention rules as Datalevin.

Both SQL adapters store one row per record: an integer primary key and one
`TEXT NOT NULL` column per field. Only the primary key is indexed. This compares
the same logical operations; it includes Datalog's additional index maintenance
costs rather than giving SQL unused secondary indexes. Point reads select all
fields, updates change one column, and scans use the same bounded ID interval
with `ORDER BY id`. Initial batches and measured inserts are transactional.
F performs a full-record read followed by the same character change inside one
transaction: SQLite uses `BEGIN IMMEDIATE`, and PostgreSQL uses
`SELECT ... FOR UPDATE` at `READ COMMITTED` isolation.

SQL connections and prepared statements are created before load/warmup and
reused across phases. `--pool-size` (default `--threads`) bounds each SQL pool;
pool waiting is included in latency. PostgreSQL can write different rows
concurrently, while SQLite and shared Datalog handles serialize writes according
to their normal transaction behavior. The harness adds no shared row lock.

Datalevin prepares its Datalog scan query with `prepare-q` when opening each
database handle, before load and warmup. The field projection is fixed in the
query; executions supply only the lower and upper ID bounds. Remote handles
register lazily on each connection and are reused across phases. With zero
warmup, initial registration is included in measured latency. Reports identify
this choice with `:storage :scan-api :prepare-q`.
See the [prepared-scan validation and workload E comparison](results/2026-09-15-prepared-scans/README.md).

| Durability profile | Datalevin | SQLite | PostgreSQL |
| --- | --- | --- | --- |
| `strict` | Strict WAL | WAL, `synchronous=FULL` | `synchronous_commit=on` |
| `relaxed` | Relaxed WAL | WAL, `synchronous=NORMAL` | `synchronous_commit=off` |

The harness verifies these SQL settings on every connection. PostgreSQL also
requires `fsync=on` and `full_page_writes=on` for both profiles. Engine/driver
versions and effective SQL settings appear in each result's `:storage` map.
SQLite's `fullfsync` setting is reported without overriding its default. These
profiles align commit policies, not every OS/device guarantee; see the
[SQLite synchronous documentation](https://www.sqlite.org/pragma.html#pragma_synchronous)
and [PostgreSQL WAL settings](https://www.postgresql.org/docs/current/runtime-config-wal.html).

Keep PostgreSQL on the same host for a loopback comparison, with sufficient
connections and no unrelated load. PostgreSQL runs in its own process; the
managed Datalevin server also runs in a separate JVM by default. Both servers
use the host's CPU and memory; neither has a CPU quota. The Datalevin server's
heap and background limits stay fixed as client counts change. The harness
records PostgreSQL's buffer, checkpoint, autovacuum, and parallel-query settings
without changing them. Equal commit policies do not imply identical indexes,
transaction isolation, memory management, or storage architecture.

For a remote Datalevin concurrency sweep:

```sh
clojure -J-Xms4g -J-Xmx4g -M:jvm:bench \
  --system datalevin --api all --mode remote --workload c \
  --client-counts 1,2,4,8 --datalog-handles shared --repetitions 1 \
  --records 100000 --warmup-ms 10000 --measurement-ms 30000 \
  --server-workers 16 --server-heap-mb 4096 --durability strict \
  --output /tmp/datalevin-ycsb-comparison.edn
```

Use `--api datalog --threads 8 --datalog-handles both` without `--client-counts`
to compare shared and independent handles for writes: workload A mixes reads
and updates, and F includes atomic read-modify-write. Use `--system all --api
datalog` when a new PostgreSQL baseline is needed. KV has a different physical
layout.

`--client-counts` expands the cases with matching worker and pool sizes.
`--datalog-handles shared` gives workers one Datalog handle with a pooled read
client and the normal dedicated transaction connection. `independent` creates
one handle and one authenticated, single-connection client per worker; that
connection also handles its transactions. Workers keep their assigned handles
throughout each phase. `both` runs and labels both arrangements, with one SQL
baseline per client count. Independent mode requires pool size to equal worker
count. This option applies to remote Datalevin Datalog only.

One trial is the default. Optional repetitions reload fresh data and start a
fresh owned Datalevin server each time.
Even-numbered repetitions reverse the full case order. Results retain every
trial and summarize median/min/max throughput separately for each topology.
Timed phases run for the requested duration, then finish in-flight operations;
`--warmup-ms` and `--measurement-ms` override their respective operation counts.
Every latency sample is retained, and percentile calculation is outside phase
timing. Timed inserts grow the Zipf table as necessary, with growth included in
timing. The phase timeout must exceed the requested duration.

The default Datalevin server uses a fixed 4096 MiB heap, one thread per connection,
a limit of 16 explicit transactions, 4 background threads, and a 1000 ms writer
slot timeout. KV and Datalog handles with pooled clients have one additional
connection for explicit transactions. Legacy `--server-workers` and
`--server-queue-size` options remain accepted, but requests no longer use those
pools or queues. Corresponding `--server-*` options appear in
`--help`. `--server-mode in-process` is available for diagnostic controls and
is explicitly labeled in reports. Child startup, readiness, and shutdown are
outside phase timing. A startup failure or timeout terminates the child;
closing the parent's control pipe also stops it. Effective WAL state is read
from Datalevin and checked before the case starts. Server PID, Java version,
heap, execution model, background limits, client topology, and final WAL watermarks are
included in the storage report. Format version 3 adds run options and trial
summaries to the complete EDN report.

`clojure -M:jvm:test` always tests SQLite. Set `YCSB_PG_URL` (plus credentials
if needed) to include PostgreSQL integration checks. They check field
preservation, concurrent atomic modifications, failed-batch rollback, and both
durability profiles. Without the URL, the test runner prints an explicit skip.

## Database lifecycle and defaults

Each Datalevin case loads a **fresh database** into a generated `datalevin-ycsb-*`
directory under the JVM temporary directory. Files are removed after closing
stores and stopping the server, including on failure. `--keep-db` retains those
directories and includes their paths in successful results. Existing databases
are never used. Set `-J-Djava.io.tmpdir=/path/to/benchmark-disk` to choose a disk;
keep database files outside the repository.

Without arguments, the harness runs workload A for both APIs in both modes:
10,000 initial records, 1,000 warmup operations, 10,000 measured operations,
one worker, seed 17. Operation counts are **totals across workers**, not counts
per worker. `--workload all` selects A–F; workload names are case-insensitive.
The load batch size defaults to 100 records; measured writes each commit one
logical record operation.

## Workloads

| Workload | Operation mix | Default key distribution |
| --- | --- | --- |
| A | 50% read, 50% update | Zipfian |
| B | 95% read, 5% update | Zipfian |
| C | 100% read | Zipfian |
| D | 95% read, 5% insert | Latest |
| E | 95% scan, 5% insert | Zipfian |
| F | 50% read, 50% read-modify-write | Zipfian |

Mixes are sampled probabilistically; the report includes actual counts.
All reads fetch every field. Updates replace one randomly chosen field with
an ASCII string of the same length. Inserts write all fields atomically.
Scans choose a uniform length from 1 through `--scan-length` (default 100),
clipped at the current end of the committed keyspace, and return ordered,
fully materialized records. There are no deletes.

F reads the entire record and advances the selected field's first character
cyclically through `a`–`z` inside an explicit write transaction. The replacement
depends on the read value. This **atomic** read-modify-write is stronger than
the upstream workload's separate read and update calls, and includes Datalevin's
transaction setup/commit costs. Shared-handle transaction locking is included
in latency and throughput.

## Data and execution choices

The logical schema is identical for both APIs: a sequential numeric record ID
and `--field-count` strings of `--field-length` ASCII bytes. Defaults are ten
fields of 100 bytes, excluding keys and database overhead.

* **KV:** one `:long` key per field, `record-id * field-count + field-index`,
  with a `:string` value in the `records` DBI. A logical read is one bounded
  range read, and a blind single-field update is one put. This avoids requiring
  a read to preserve other fields. There are ten physical entries per default
  logical record, rather than a single serialized map.
* **Datalog:** one entity per record, a unique long `:ycsb/id`, and string
  attributes `:ycsb/field0` through `:ycsb/field9` by default. Point reads use
  `pull`; updates use `transact!`. Scans execute a prepared bounded ID-range
  Datalog query with pull and sort its results by ID. Datalog's indexes and entity overhead
  are part of the measurement. Background sampling is disabled.
* **Durability:** WAL is enabled with `--durability strict` by default for both
  APIs and modes. `--durability relaxed` selects a separately labeled profile.
  The initial LMDB map size is 4096 MiB; normal automatic growth remains enabled.
* **Remote:** a new server process binds to `127.0.0.1` on an OS-assigned port for
  each case. Workers share one remote handle by default; independent Datalog
  handles can be selected explicitly. The connection pool's default size equals
  `--threads`. Datalog's shared handle also uses its standard dedicated
  transaction client when the pool has more than one connection. Server worker
  counts stay fixed independently of client concurrency. Debug
  request logging is disabled. The server uses its normal initial credentials,
  honoring `DATALEVIN_DEFAULT_PASSWORD`. Credentials are not included in reports.
  This measures TCP, encoding, pooling, and server dispatch with separate
  client/server heaps and GC. The loopback transport does not model network
  latency between different machines.

Initial field values and per-worker random streams are seeded. Concurrency
changes commit interleaving, so a seed does not reproduce one global request
order. Insert IDs become eligible for reads only after commit; out-of-order
commits wait behind earlier reserved IDs before entering the readable prefix.

The Zipf generator uses finite rank weights with exponent 0.99. It favors low
numeric IDs; latest reverses ranks to favor the newest committed IDs. Unlike
upstream YCSB's scrambled Zipfian generator and hashed record keys, keys here
are ordered and hot ranks are contiguous. `--distribution uniform` overrides
the default for any workload. The generator is initially built outside measured phases;
its cumulative weights use eight bytes per possible record, conservatively
bounded by `records + warmup + ops` for count-based phases. Timed insert phases
grow that table if they exceed this initial capacity.

## Measurements and interpretation

Load, warmup, and measurement are separate phases. Warmup changes the database
but contributes no measured samples. Each measured operation's latency includes
request generation, waiting for client/transaction locks, database calls,
complete result materialization, and read/scan shape and ASCII checks. A shared
start gate releases workers together. Overall throughput divides successfully
completed logical operations by wall-clock phase duration; scans and RMW each
count as one operation, independent of their number of rows or wire requests.

This is a **closed-loop** workload: each worker waits for one operation before
issuing the next. It has no target arrival rate or coordinated-omission
correction. Latencies describe completed requests at the chosen concurrency,
not response times under an independently offered request rate.

The EDN report contains normalized configuration including the selected system,
Datalevin/JVM/OS information,
storage layout, load throughput, warmup and measured counts, aggregate and
per-operation mean/p50/p95/p99/max latency in **microseconds**, and final
validation results. Percentiles are exact nearest-rank percentiles; sorting is
outside the timed interval. Sample storage uses nine bytes per operation,
plus up to eight bytes per operation while calculating per-operation statistics.
Choose heap size with those arrays, generators, and database caches in mind.

Every successful run checks the final record count, every record's ID and field
shape, and that all reserved insert IDs committed. These are structural checks,
not a linearizability oracle or crash-durability test. Missing reads, malformed
scans, worker failures, and phase timeouts fail the run; failed batches do not
produce a success report. `--timeout-ms` controls remote request timeout.
For SQL it also controls pool waits, statement timeouts,
and database lock waits (JDBC timeout resolution is whole seconds).
`--phase-timeout-ms` triggers cancellation of an overdue warmup/measured phase.
Cleanup waits for every worker to exit before closing stores or removing files
and schemas. An operation that ignores interruption can delay this wait beyond
the phase timeout; an operation that never returns will keep the run blocked.
Once cancellation starts, workers do not start another operation, even if the
current operation clears its interrupt flag. The original phase failure is
reported after workers have stopped.

Smoke results establish that the harness works, not a performance ranking.
For comparisons, use identical data, durability, heap, disk, and concurrency;
increase warmup until results stabilize, repeat runs, and alternate case order
using separate CLI invocations to reduce JVM/cache/order effects. Loading and
warmup both touch the data, so this is not a cold-cache benchmark.
