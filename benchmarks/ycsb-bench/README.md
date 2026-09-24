# YCSB-style KV and Datalog benchmark

Runs the A–F operation mixes against both Datalevin APIs, either embedded or
through a managed loopback server. It also compares both embedded APIs with
SQLite and both remote APIs with PostgreSQL. This is a standalone Clojure harness using
the checkout at `../..`; it adds no benchmark code or dependencies to production.
It is inspired by the [YCSB core workloads](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads),
not an official YCSB binding or a directly comparable YCSB score.

F follows upstream's read-then-update sequence: generate an independent
replacement for one field, read all fields, then issue an ordinary field update.
The pair is not wrapped in a transaction. Remote Datalevin and PostgreSQL each
perform a read request followed by an update request. Embedded Datalevin and
SQLite use the same two-call sequence locally.

The individual KV update uses `update-kv` to preserve other fields in the stored
vector; embedded calls use a compiled Clojure callback, and remote updates run
the replacement callback on the server. Datalog updates use `transact!`, and
SQL updates use an autocommit prepared UPDATE. These are the same update paths
used by A/B. Reports identify F with `:workload-model :ycsb-read-update-v1`,
`:rmw-execution :client-read-update`, and `:atomic-rmw? false`. Earlier atomic F
results require fresh baselines for both Datalevin and SQL.

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

`--system all` runs each selected Datalevin case followed by its SQL counterpart:

| Mode | Pair |
| --- | --- |
| Embedded | Datalevin KV or Datalog and SQLite |
| Remote | Datalevin KV or Datalog and PostgreSQL |

The default `--sql-indexes matched` aligns SQL value indexes with the selected
Datalevin API:

| API | SQL value-index condition | Capabilities compared |
| --- | --- | --- |
| KV | `none` | Primary-key reads, writes, and scans |
| Datalog | `all` | Primary-key access plus indexed lookup on every value field |

The primary key remains indexed in both conditions. `--sql-indexes none` or
`all` overrides the match; `--sql-indexes both` runs both SQL conditions for each
selected API without duplicating Datalevin runs. Each SQL condition loads a fresh database.
Console output, per-case configuration, storage settings, and trial summaries
label the condition; repetitions never combine indexed and unindexed results.
For SQL results, `:api` identifies the Datalevin API used for the comparison.

```sh
# Embedded Datalog versus SQLite, all six workloads.
clojure -M:jvm:bench --system all --api datalog --mode embedded \
  --workload all --records 100000 --ops 100000 --warmup 20000 --threads 8 \
  --output /tmp/datalevin-ycsb-sqlite.edn

# Compare both APIs with their matching SQLite index conditions.
clojure -M:jvm:bench --system all --api all --mode embedded \
  --workload all --records 100000 --ops 100000 --warmup 20000 --threads 8 \
  --output /tmp/datalevin-ycsb-sqlite-matched.edn

# Run SQLite alone with and without value indexes.
clojure -M:jvm:bench --system sqlite --api datalog --mode embedded \
  --workload all --sql-indexes both \
  --output /tmp/datalevin-ycsb-sqlite-indexes.edn

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
process invocations and alternate engine ordering.
`--system all --api all --mode all --workload all` runs 48 cases with the default
matched index conditions.

PostgreSQL must already be running. `--pg-url` and `--pg-user` override
`YCSB_PG_URL` and `YCSB_PG_USER`; the URL defaults to
`jdbc:postgresql://127.0.0.1:5432/postgres`. Passwords come from
`YCSB_PG_PASSWORD`. The role needs permission to create a schema in the target
database. Connectivity and durability are checked before the comparison starts.
Each PostgreSQL case creates a unique `ycsb_<uuid>` schema, uses ordinary logged
tables, and drops only that schema on completion or failure. `--keep-db` retains
it and records its name. JDBC URLs and usernames are omitted from reports.
SQLite uses a fresh temporary file with the same retention rules as Datalevin.

For A/B/C/D/F, both SQL adapters store one row per record: an integer primary key and one
`TEXT NOT NULL` column per field. Inserts explicitly supply the benchmark's
numeric record key. In the `all` condition, both SQLite and PostgreSQL maintain
a separate non-unique index on every value column (`f0` through `f9` by default),
providing the same single-field value lookup capability as Datalog's attribute
indexes. These indexes are created before loading, so load, warmup, and measured
writes all include their maintenance. The `none` condition has only the primary
key index and matches KV's access capabilities. Reports record the resolved
condition under `:configuration :sql-indexes` and
`:storage :configuration :sql-indexes`, with index names and columns under
`:storage :configuration :secondary-indexes` (empty for `none`). The comparison
aligns lookup capabilities and logical operations; physical index layouts still
differ. Published results predating these conditions indexed only the SQL
primary key, including the earlier Datalog comparisons.

Point reads select all fields and updates change one column. Known-record
validation reads run outside measurement. Single-record inserts use one
autocommit prepared INSERT, matching the default
[upstream JDBC insert path](https://github.com/brianfrankcooper/YCSB/blob/master/jdbc/src/main/java/site/ycsb/db/JdbcDBClient.java).
Multi-record load batches use an explicit transaction.
F issues an ordinary full-record SELECT and then an autocommit UPDATE. There is
no `SELECT ... FOR UPDATE`, explicit BEGIN, or transaction spanning the pair.
SQLite commits explicit insert batches by restoring JDBC autocommit directly; calling `commit()` first
would make the Xerial driver start another empty `BEGIN IMMEDIATE` transaction.

SQL connections and prepared statements are created before load/warmup and
reused across phases. `--pool-size` (default `--threads`) bounds each SQL pool;
pool waiting is included in latency. PostgreSQL can write different rows
concurrently, while SQLite and shared Datalog handles serialize writes according
to their normal transaction behavior. The harness adds no shared row lock.

E uses the application-key schema and query below. A/B/C/D/F validate known
hashed numeric keys with prepared point reads after measurement.

### E: ordered ranges on an application key

E follows the [upstream short-range operation](https://github.com/brianfrankcooper/YCSB/blob/master/workloads/workloade):
start at a record key and return the next N records in key order, including all
payload fields. The key is separate from the ten default payload fields.
Scans count returned records across gaps and stop at the end of the keyspace.

The application key is the upstream default string `user` followed by the
decimal FNV64 hash of the insert ordinal. The ordinal is generator bookkeeping;
key order differs from insertion order. A scan chooses a committed ordinal
using the configured request distribution and converts it to that string key.

* **KV:** the `records` DBI uses a `:string` key and a payload vector encoded as
  `:data`. `get-first-n` starts at `[:at-least start]` and returns at most N entries.
* **Datalog:** `:ycsb/key` is a string attribute with `:db.unique/identity`.
  Internal entity IDs are assigned by Datalevin. The prepared query filters
  `:ycsb/key >= start`, orders by that attribute, limits the result, and pulls
  the payload fields. One query per supported page size is prepared before
  loading and reused because `:limit` requires a literal. Remote E executes
  the query and pulls in one request.
* **SQL:** `record_key TEXT` is the primary key. SQLite uses `COLLATE BINARY`
  and `WITHOUT ROWID`; PostgreSQL uses `COLLATE "C"`. These order the generated
  ASCII keys identically. The `none` condition has no secondary indexes;
  `all` indexes every payload column (ten by default). Single-record inserts
  use an autocommit prepared INSERT; load batches use an explicit transaction.

For a page of ten records, the prepared Datalog query is:

```clojure
{:find [?key (pull ?entity [:ycsb/field0 :ycsb/field1 ; ...all configured fields
                            :ycsb/field9])]
 :in [$ ?start]
 :where [[?entity :ycsb/key ?key]
         [(>= ?key ?start)]]
 :order-by [?key]
 :limit 10}
```

The SQL equivalent is:

```sql
SELECT record_key, f0, f1, f2, f3, f4, f5, f6, f7, f8, f9
FROM records
WHERE record_key >= ?
ORDER BY record_key
LIMIT ?;
```

All adapters return `[key values]` rows. Timed checks verify the existing start
key, strict ordering, the requested limit, and payload shape. Concurrent inserts
may add keys anywhere within a page. Final validation sorts all generated keys
and checks exact page membership and every payload character outside timing.

Reports mark E with `:workload-model :application-key-range-v1`. Historical E
scores using entity-ID intervals or conversation keys need fresh baselines.
The current planner probe uses ordered AVE access and ten pulls for a limit of
ten, with 500 eligible keys. The benchmark submits the declarative query above.
See the [plan and distribution probes](../../doc/ycsb-application-key-2026-09-24/README.md).

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
and updates, and F includes a read followed by an independent update. Use `--system all --api
datalog` when a new PostgreSQL baseline is needed. KV has a different physical
layout.

`--client-counts` expands the cases with matching worker and pool sizes.
`--datalog-handles shared` gives workers one Datalog handle with a pooled read
client and the normal dedicated transaction connection. `independent` creates
one handle and one authenticated, single-connection client per worker; that
connection also handles its transactions. Workers keep their assigned handles
throughout each phase. `both` runs and labels both arrangements, with one SQL
baseline per client count and selected SQL index condition. Independent mode requires pool size to equal worker
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
if needed) to include PostgreSQL integration checks. They check actual index
definitions, field preservation, interleaving between F's read and update, failed-batch
rollback, both value-index conditions, and both durability profiles. Without
the URL, the test runner prints an explicit skip.

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
E scans choose a uniform length from 1 through `--scan-length` (default 100),
returning ordered, fully materialized records up to that limit or the keyspace's
end. There are no deletes.

F has 50% ordinary reads and 50% read-modify-write operations. Following
[CoreWorkload.doTransactionReadModifyWrite](https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/workloads/CoreWorkload.java),
each RMW generates a fresh value for a random field, reads the complete record,
then updates that field on the same key. The replacement does not depend on the
read value, and another writer may commit between these calls. No stored
transaction function or SQL row lock spans the pair. Each individual field
update remains atomic and preserves unrelated fields.

The read and update together count as one logical `:rmw` operation, and its
latency includes both calls. The common harness timing policy also includes
request generation and materialized-read shape checks. A read failure invalidates
the run before sending the update.

## Data and execution choices

For A/B/C/D/F, the logical schema is identical for both APIs: a hashed numeric record key
and `--field-count` strings of `--field-length` ASCII bytes. Defaults are ten
fields of 100 bytes, excluding keys and database overhead.

* **KV:** one `:id` key per record, equal to the nonnegative numeric record ID,
  with all field strings stored together as a vector encoded with `:data` in the
  `records` DBI. Point reads reuse a `prepare-get-value` created when each store
  opens and call `execute-prepared` with the key. Field updates use `update-kv`
  in both modes. An update reads the current
  vector, replaces that field, and writes the whole vector inside one write
  transaction, preserving concurrent changes to other fields. F first performs
  a separate prepared point read, then uses this ordinary update with an
  independently generated replacement. Each physical entry counts as one logical record.
  The `:id` encoding uses eight bytes without a type header.
  Reports identify this layout as `:record-value` and its key encoding as `:id`;
  earlier single-vector results used `:long` keys, which include a type header.
  Published results labeled `:field-keys` used the earlier per-field layout.
* **Datalog:** one entity per record with an explicitly assigned `:db/id` equal
  to the benchmark's numeric FNV64 key, and string attributes
  `:ycsb/field0` through `:ycsb/field9` by default. There is no separate
  `:ycsb/id` attribute or identity lookup. Point reads reuse a `prepare-pull`
  created when each store opens and call `execute-prepared` with the entity ID;
  local executions pass the current connection DB to retain preparation across
  writes. Updates use `transact!`. F performs that prepared read followed by an
  ordinary `transact!` update; no encompassing transaction is opened.
  Untimed validation reuses prepared reads for known hashed IDs and arranges
  each record's fields in benchmark order. Record counts use `count-datoms` on
  the mandatory first field. Reports identify the key with
  `:storage :record-key :db/id`. Datalog's field indexes and entity overhead
  are part of the measurement. Index-result caching is bypassed with
  `:cache-limit 0` in both embedded and remote modes; reports include the
  effective limit. Pull-pattern and query-plan caches remain enabled.
  Background sampling is disabled. Published
  results predating this change used the separate `:ycsb/id` identity attribute.
* **Durability:** WAL is enabled with `--durability strict` by default for both
  APIs and modes. `--durability relaxed` selects a separately labeled profile.
  The adapters inherit the WAL-backed LMDB defaults, including `:writemap`
  and `:nosync`; reports record the effective flags under `:storage :env-flags`.
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

The default Zipfian selection follows upstream
[ScrambledZipfianGenerator](https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/generator/ScrambledZipfianGenerator.java):
theta 0.99, the Gray inverse approximation over the inclusive `[0, 10^10]`
source range, its precomputed zeta, then FNV64 modulo a fixed destination
keyspace. Uncommitted ordinals are rejected and redrawn. Popular records remain
scattered without moving when inserts commit. The default fixed keyspace is
`records + floor(2 * ops * insert-proportion) + 1` for measurement. Warmup uses
its own prediction from `warmup` operations. `--zipfian-keyspace N` overrides
both. Changing warmup duration or count does not change the measured modulus.
For duration-based runs, set the same sufficiently large keyspace on all engines
if new inserts should remain eligible; ordinals beyond the fixed keyspace are
not selected. The report records the actual modulus. No growing CDF is needed
for scrambled Zipfian selection.

Stored keys are hashed separately using upstream `Utils.fnvhash64`. A/B/C/D/F
keep numeric keys in both Datalevin and SQL; E uses upstream's `user` + decimal
hash string, with the default zero-padding of one. The numeric representation
is a harness choice and sorts differently from strings, but insertion locality
is no longer sequential. `:key-generator`, `:insert-order`, and
`:request-generator` distinguish these results from old contiguous-key runs.

`--distribution uniform` samples the committed ordinal prefix. `latest` retains
the harness's finite inverse-CDF Zipf weights over that prefix, reversing ranks
to favor new commits; it is not scrambled. This latest sampler remains a harness
variant of upstream's approximation. Its CDF is built outside timing with eight
bytes per possible record and can grow during timed insert phases. Every
distribution uses hashed storage keys.

## Measurements and interpretation

Warmup runs against its own freshly loaded database. After its workers finish,
the harness validates and closes that database, then loads a **fresh measured
database** with exactly `--records` records and the original seeded values.
Warmup inserts, updates, key allocation, sampler growth, and audit histories do
not carry into measurement. Both counted and timed warmups use this isolation.
The Datalevin server process stays alive across phases, retaining JVM/JIT warmup;
client handles and prepared statements are recreated for the new database.
SQLite gets a new file, and PostgreSQL gets a new schema on the same server.
The measured load warms the new dataset; this is not a cold-cache experiment.
A zero-length warmup skips its database entirely.

Reports record `:warmup-isolation :separate-database` and each phase's verified
`:starting-records`. Top-level load and validation describe the measured
database. Warmup includes its own load, storage, and validation results. Final
measured counts include only measured inserts. With `--keep-db`, both datasets
are retained and their locations/names appear in the report. Older results
that carried warmup mutations into measurement need fresh baselines; summaries
keep the two warmup models separate.

Each measured operation's latency includes
request generation, waiting for client/transaction locks, database calls,
complete result materialization, and read/scan field-count, string-type, string-length,
and scan-ID checks. Character-by-character ASCII validation runs only in the
final full-database validation, outside both latency and throughput timing.
This timing policy is shared by Datalevin, SQLite, and PostgreSQL. A shared
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
shape, every field's ASCII content, and that all reserved insert IDs committed.
The default validation report records `:scope :structure`,
`:value-checks :not-performed`, and `:character-checks :post-measurement`.
These checks do **not** establish value correctness: returning a different
record's well-formed payload or silently dropping updates can pass them.
Adapter tests provide separate correctness coverage. Results
from older harness versions that timed ASCII checks need to be rerun before
comparing throughput or latency with this version. These are structural checks,
not a linearizability oracle or crash-durability test. Missing reads, malformed
scans, worker failures, and phase timeouts fail the run; failed batches do not
produce a success report. `--timeout-ms` controls remote request timeout.
For SQL it also controls pool waits, statement timeouts,
and database lock waits (JDBC timeout resolution is whole seconds).
`--phase-timeout-ms` triggers cancellation of an overdue warmup/measured phase.

Use `--value-audit` for a correctness investigation with the same workload and
adapters. Each phase retains read/scan payloads and write arguments with invocation
and completion times in per-thread logs. After that phase's workers stop, it
compares every observed field and every final record against
the deterministic initial values and recorded inserts/updates for that key and
field. F's read and update remain two separate calls. Wrong-record payloads,
unwritten values, and values superseded by a completed, nonoverlapping write
fail the audit. Overlapping writes may leave either value; response order does
not establish commit order.

Audit reports use `:scope :structure-and-observed-values` and detail the checked
reads, scan rows, writes, and final records under `:value-checks`. This checks
necessary per-field ordering constraints, **not full linearizability** or a
consistent multi-field snapshot. A dropped write overwritten before any read
cannot be detected from its final value. Equal timestamp boundaries are treated
conservatively as overlapping. Crash durability is not checked.

Value comparisons stay outside measurement, but capturing histories adds
allocation, timestamps, and retention to timed operations. Audit timings include
that overhead and must not be used as performance baselines. Trial summaries
separate audit and ordinary runs. Start with small operation counts: retained
payload memory grows with a phase's reads, scanned rows, and writes; audit
indexes also retain the initial dataset. Warmup history is released before the
measured dataset is loaded. Ordinary runs allocate
no audit histories and retain the existing timing path.
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

The [September 15 comparison](results/2026-09-15-current/README.md) contains
the earlier 36-case A–F matrix and sustained remote read controls, with frozen
sources, validation results, and historical comparisons. It predates explicit
Datalog entity IDs and the matched SQL value-index conditions.
