# TPC-C-Derived Benchmark

This benchmark compares two deployment modes separately:

- **Embedded:** SQLite WAL and Datalevin WAL.
- **Client/server:** PostgreSQL and Datalevin WAL over loopback TCP, each with a
  separate database server process.

All use the same generated population, transaction inputs, scalar types, terminal
lifecycle, and synchronous WAL commit contract. The workload has nine normalized
tables and five short transactions with hot-row contention.

> **Derived, not audited.** This is a TPC-C-derived implementation. It follows
> the published schema and transaction definitions but is not an audited TPC-C
> result and must not be presented as official. See the [TPC fair-use
> rules](https://www.tpc.org/information/other/policy_fair_use5.asp).

The generator, loaders, transaction implementations, and drivers are available
for Datalevin, SQLite, and PostgreSQL.

## Schema and population

The nine TPC-C tables are `warehouse`, `district`, `customer`, `history`,
`orders`, `new_order`, `order_line`, `item`, and `stock`. Monetary values are
stored as IEEE-754 doubles in every backend: Datalevin `:db.type/double`, SQLite
`REAL`, and PostgreSQL `double precision`. IDs and counters use signed 64-bit
integers (`:db.type/long`, `INTEGER`, `bigint`); strings use Datalevin strings and
SQL `TEXT`. Dates are ISO-8601 strings. Floating-point money is an intentional
deviation from the official specification, applied equally to all drivers. Use PostgreSQL `LC_COLLATE=C` to match SQLite
`BINARY` and Datalevin ordering of the generated ASCII customer names.

Population follows the specification's cardinalities for `w` warehouses:

| Table | Rows per warehouse |
|---|---:|
| item | 100,000 (global) |
| stock | 100,000 |
| district | 10 |
| customer | 30,000 |
| history | 30,000 |
| orders | 30,000 |
| new_order | 9,000 |
| order_line | ~300,000 |

`C_LAST` follows TPC-C §4.3.3.1: within each district, customers 1-1,000 iterate
the 1,000 standard names in order, and customers 1,001-3,000 draw from the same
pool with `NURand(255,0,999)` using a constant drawn from the population seed.
All drivers select the run constant so its absolute difference from the load
constant is 65-119, excluding 96 and 112, as required by
[TPC-C §2.1.6.1](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf#page=22).
The run constants are shared by every terminal and stay fixed through warmup
and measurement. That surname draw is deliberately non-uniform, so
name-based Payment and Order-Status see the specification's surname match sizes
rather than a uniform one-per-name or unmatched random string.

Each district's 3,000 orders map bijectively to its 3,000 customers, the first
2,100 orders are delivered, and orders 2,101-3,000 are the open new orders.
Generation is deterministic per table from a seed, so reloads are reproducible.

Datalevin represents each row as an entity and each foreign key as a
`:db.type/ref`. Composite SQL foreign keys become a single reference to the
target entity. The scalar foreign-key columns are not duplicated:

| Entity | References |
|---|---|
| district | warehouse |
| customer | district |
| stock | warehouse, item |
| orders | district, customer |
| new_order | order |
| order_line | order, supplying stock |
| history | customer, paying district |

An order line's item and supplying warehouse follow its stock reference. History
keeps the paying district separate from the customer's district for remote
payments. All nine SQL rows can be reconstructed by following these relationships.

The loader computes entity IDs from disjoint table ranges and complete business
keys, following JOB's approach. It constructs references arithmetically without
an entity lookup map. Runtime inserts use normal transaction tempids; New-Order
reuses the returned order ID and retrieved stock IDs for its line references.

Unique composite keys contain references plus local IDs: district
`[warehouse id]`, customer `[district id]`, stock `[warehouse item]`, order
`[district id]`, and line `[order number]`. Customer surnames and orders by
customer have secondary tuple indexes. A new-order entity has a unique order
reference and an explicit `[district order-number]` queue index for oldest-first
Delivery. Other composite attributes are maintained automatically. References,
lookup keys, and these secondary indexes have value indexes; remaining scalar
fields use `:db/noindex true`.

Order-Status and Delivery query lines through their order references.
Stock-Level selects the last 20 order IDs with a bounded index range, then queries
their lines and follows supplying stock to item. It checks that item's **home
warehouse** stock quantity, including when a line was supplied remotely, and
counts distinct items in the database.

Reload databases created with older schemas before running this version. The
Datalevin driver rejects databases with legacy scalar foreign keys or incompatible
indexes. Both SQL schemas index orders by warehouse, district, customer, and
order ID.

## Transactions

| Transaction | Mix | Writes |
|---|---:|---|
| New-Order | 45% | district counter, stock, order, new_order, order_line |
| Payment | 43% | warehouse, district, customer, history |
| Order-Status | 4% | none |
| Delivery | 4% | new_order, orders, order_line, customer |
| Stock-Level | 4% | none |

All drivers select Payment customers by last name 60% of the time and by
customer ID 40% of the time. Each terminal keeps a fixed home warehouse and
Stock-Level district. Terminals are assigned round-robin across warehouses,
then across their ten districts. With multiple warehouses, 1% of New-Order lines
use another supplying warehouse, and 15% of Payments select a customer in another
warehouse and a random district. One-warehouse runs use local customers and stock.

All drivers share New-Order line generation: 5-15 lines per order, with one
random 1-in-100 rollback decision per order. A rollback order has only its
final item id replaced with an unused value, as specified in
[TPC-C §2.4.1.3-5](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf#page=28).

New-Order reads the warehouse tax, the district counter and tax, and the customer
discount, last name, and credit, then advances the counter and inserts the order
and new-order headers.
It processes each line in input order, updating stock and inserting the line
before looking up the next item. All backends return the successful order's
`:amount` as `sum(OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)`
(TPC-C §2.4.2.2); stored line amounts remain quantity times item price. Results also
include the customer details, taxes, entry date, and line count. Each line returns
its item name, price, retrieved stock quantity, and brand flag (`B` when both item
and stock data contain `ORIGINAL`, otherwise `G`). Invalid-item results retain the
customer ID, name, credit, attempted order ID, and error message.
An invalid item rolls back the header and all preceding valid-line writes, exercising the rollback workload required by
TPC-C §2.4.2.3. Payment updates the warehouse and district year-to-date
totals, the customer balance/payment count, and appends a history row. Delivery
removes the oldest new-order in each district and delivers its lines.

Order-Status retrieves the selected customer's ID, balance, and full name; the
latest order's ID, entry date, and carrier; and every line's item, supplying
warehouse, quantity, amount, and delivery date, as required by
[TPC-C §2.6.2.2](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/tpc-c_v5.11.0.pdf#page=38).
All three backends return these fields in `:customer`, `:order`, and
`:order-lines`, with lines ordered by `:number`. Undelivered carriers and dates
are `nil`. The `:lines` count remains available. All dependent reads execute
inside a single transaction.

New-Order copies `S_DIST_xx` from each supplying stock row into `OL_DIST_INFO`,
where `xx` is the order's district. For bad-credit customers, Payment prepends
`C_ID`, `C_D_ID`, `C_W_ID`, `D_ID`, `W_ID`, and the amount to `C_DATA`, preserving
the first 500 characters of the combined text. Good-credit `C_DATA` is unchanged.
Payment returns warehouse and district addresses, the customer's name, address,
phone, registration date, credit details, discount, and updated balance and payment
counters. Only the selected bad-credit customer's `C_DATA` is read; its first 200
characters are returned. `H_DATA` contains the payment warehouse and district names
separated by four spaces. Remote payments update the home warehouse/district and
the customer's own warehouse/district separately. These payloads follow
[TPC-C §§2.4.2.2 and 2.5.2.2](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf).

## Concurrency and correctness

Every terminal owns a separate connection and worker thread, including embedded
and remote Datalevin. One client JVM runs all terminals; in client/server mode,
the database runs in a separate process. The same worker and connection survive
warmup and measurement. Terminals synchronize at a common measurement barrier.
Connection opening, warmup, baseline reads, verification, and closing are outside
the throughput clock. Transaction latency excludes input generation; total
throughput includes it. There is no process per terminal in either comparison.

Transaction coordination follows each engine's native model:

- **Datalevin** performs New-Order, Payment, Order-Status, and Delivery inside
  `d/with-transaction`, using the connection bound by that macro for all reads
  and writes. Concurrent writers wait before reading values they will update.
  `d/db` alone returns a database reference and does not establish a transaction
  snapshot. Stock-Level uses committed reads with the specification's relaxed
  isolation. Transaction errors propagate and abort the run.
- **SQLite** has a single writer and no row locking. Terminals use independent
  connections with `IMMEDIATE` transactions and a 60 s native busy timeout.
  There is no JVM lock around the transaction mix. WAL is enabled before terminal
  connections open. Order-Status also opens a transaction for a consistent
  snapshot; Stock-Level uses relaxed committed reads.
- **PostgreSQL** supports concurrent writers. Read-then-write statements use
  `SELECT ... FOR UPDATE` (warehouse, district, customer, stock, and the oldest
  new-order row), and the driver retries transactions that fail with a
  serialization or deadlock SQLSTATE (`40001`/`40P01`). Order-Status uses a
  serializable transaction and restores the connection's previous isolation level.

All backends share the stock calculation from
[TPC-C §2.4.2](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/tpc-c_v5.11.0.pdf#page=30):
subtract the ordered quantity when stock is at least that quantity plus 10;
otherwise add 91 after subtracting it. Repeated lines for the same supplying
warehouse and item read and update the stock row in input order within the same
transaction. This preserves multiple replenishments and increments the order
and remote counters for each applicable line.

All drivers check two New-Order invariants after every run:

1. Each district's `next_o_id` advanced by exactly the number of New-Order
   transactions that district committed.
2. Each district's order count grew by exactly that same number.

All drivers also check Payment accounting:

3. Each warehouse's and district's year-to-date total increased by the sum of
   successful Payments to it.
4. Each district's history amount and row count increased by the successful
   Payment amount and count, respectively.
5. Each customer district's year-to-date payments and payment counts increased
   by the Payments received by its customers, including remote Payments. Delivery
   does not change these fields.

All drivers capture baselines after warmup and use only committed measured
transactions for these invariants. Every configured district is checked,
including districts with zero committed New-Orders. Counts must match exactly;
Monetary deltas allow less than half a cent of floating point error.
Checks run after all terminals finish, outside the timed interval. Any mismatch throws with an
`:errors` vector containing the invariant, affected key, expected value, and
actual value, before throughput or latency metrics are reported. Successful
runs return `:invariants :ok`.

Throughput counts completed measured New-Orders: both commits and the required
invalid-item rollbacks, as specified in TPC-C §5.1.2. Warmup, other transaction
types, and unexpected failures do not contribute. All three drivers calculate
`tpmC = 60 × completed New-Orders / elapsed seconds` using the same helper.
The console and result map report the completed total (`:new-orders`) and
separate counts for `:committed-new-orders` and `:rolled-back-new-orders`.

PostgreSQL transaction payloads, customer selection, Order-Status, remote Payments,
and stock updates are covered by integration tests against PostgreSQL 18.6.
The earlier SQL transaction-mix results are retained in the measurement report linked below.

## Host control

All three drivers wrap the measured interval in the shared host-control helper.
On macOS it pauses the current user's `mediaanalysisd` and `photoanalysisd` with
`SIGSTOP` and resumes exactly those processes with `SIGCONT` afterward; a daemon
that was already stopped before the run is left untouched. On other platforms,
or where the signal is not permitted, it is a no-op and the driver still runs.

## Running

Run commands from `benchmarks/tpcc-bench`. Use fresh seed copies for every measured
run; the workload changes the database. `TPCC_DIR` selects the data directory
(default: this directory). Keep the supplied `data/schema-*.sql` files there when
loading SQL databases.

The Datalevin loader starts with a 100,000 MiB map, matching JOB. Pass
`:mapsize` in MiB when a larger initial reservation is needed.

```bash
# Embedded pair: identical population and terminal options
clj -X:datalevin-db '{:warehouses 1 :seed 42 :dir "db-w1"}'
clj -J-Xmx8g -X:verify '{:warehouses 1 :seed 42 :dir "db-w1"}'
clj -X:sqlite-db    '{:warehouses 1 :seed 42 :path "sqlite-w1.db"}'
clj -J-Xmx8g -X:datalevin-bench '{:dir "db-w1" :warehouses 1 :threads 4 :warmup 1000 :txns 100000 :seed 42 :load-seed 42}'
clj -J-Xmx8g -X:sqlite-bench '{:path "sqlite-w1.db" :warehouses 1 :threads 4 :warmup 1000 :txns 100000 :seed 42 :load-seed 42}'

# Client/server pair: PostgreSQL must use the settings below.
# Use C collation so generated ASCII customer names sort identically.
createdb --host=127.0.0.1 --port=5432 --template=template0 --locale=C tpcc_w1
clj -X:pg-db '{:url "jdbc:postgresql://127.0.0.1:5432/tpcc_w1" :warehouses 1 :seed 42}'
clj -J-Xmx8g -X:pg-bench '{:url "jdbc:postgresql://127.0.0.1:5432/tpcc_w1" :warehouses 1 :threads 4 :warmup 1000 :txns 100000 :seed 42 :load-seed 42}'
clj -J-Xmx8g -X:datalevin-bench '{:uri "dtlv://datalevin:datalevin@127.0.0.1:18898/tpcc" :warehouses 1 :threads 4 :warmup 1000 :txns 100000 :seed 42 :load-seed 42}'
```

The Datalevin server example uses the default credentials for an isolated local
benchmark. Start a separate server process with a populated database and a 60 s
transaction lock timeout. The [reproduction scripts](../../doc/measurements/tpcc-stock-query-2026-11-04/README.md)
show how to clone a closed embedded seed into a fresh server root, start the
server, run the client, and stop the server.

Run `:verify` on a fresh Datalevin seed before warmup. It checks reference
targets and the pending-order queue index, reconstructs all nine tables, and
compares row counts and order-independent SHA-256 fingerprints against the shared
generator. Pass `:out "verification.edn"` to save its report.

`:warmup` is **per terminal** (default 1,000); `:txns` is the **total measured
count** (default 100,000), divided across terminals. `:threads` defaults to 1.
Each terminal gets its own seed-derived input stream; measurement restarts that
stream after warmup. Every backend consumes identical inputs for a given set of
options. `:load-seed` must match the population seed (default 42); it is supplied
by the caller, not inferred from database metadata.

Run the generator, transaction, connection-lifecycle, and remote concurrency tests:

```bash
clojure -M:test
TPCC_TEST_PG_URL=jdbc:postgresql://localhost:5432/postgres clojure -M:test
```

The PostgreSQL tests create and remove unique schemas. `TPCC_TEST_PG_USER` and
`TPCC_TEST_PG_PASS` optionally supply test credentials. Tests cover transaction
payloads, rollback of staged writes, concurrent accounting, and a remote abort
whose wrapper closes after another terminal acquires the writer.

## Durability and comparison boundaries

Datalevin throughput runs always enable `:wal? true`,
`:wal-durability-profile :strict`, and `:wal-sync-mode :fsync`; the driver checks
the effective options. SQLite uses WAL, `synchronous=FULL`, and `fullfsync=OFF`.
PostgreSQL requires `fsync=on`, `synchronous_commit=on`, `wal_sync_method=fsync`,
and `full_page_writes=on`; the driver checks these settings on each connection.
These comparisons use synchronous operating-system WAL flushes. On macOS they
do not request the additional `F_FULLFSYNC` hardware-cache flush.

Data representation and business operations are matched, while native storage
and concurrency remain engine-specific. Datalevin stores entities, datoms, and
composite indexes; SQL engines store rows and B-tree indexes. Datalevin and SQLite
serialize writers; PostgreSQL uses row locks and retries. Each engine retains
its native cache and checkpoint mechanisms. Selected settings are recorded with
the results.

Client/server transaction logic runs in the client for both databases, using
native APIs. No driver replaces a transaction with a server-side procedure.
Both keep a server transaction open across client requests until commit or abort.
PostgreSQL holds the affected row locks; Datalevin holds its single writer.
Within `with-transaction`, all `transact!` calls join the outer commit. Explicit
Datalevin bodies currently have separate native commit outcomes to keep an abort
from cancelling another terminal's writes.
SQL statements and Datalevin query/pull/transaction requests have different sizes and
round-trip counts, so the client/server figures measure those complete API paths.
Remote Datalevin uses an ordinary request connection and a dedicated transaction
connection internally for each application connection; PostgreSQL uses one JDBC
socket per terminal. Terminals otherwise have the same lifecycle and threading.

## Measured results

The current Datalevin reference model was measured November 6, 2026 on an Apple
M3 Pro (36 GiB RAM), with 8 GiB client/server heaps. Both fresh one- and
two-warehouse seeds passed complete row-count and SHA-256 fingerprint checks
against the shared generator, reconstructed through references.

The table below compares the previous scalar model with the current reference
model under strict WAL/fsync. Each entry is one sequential run from a fresh seed
copy: 1,000 warmup transactions per terminal and **10,000 measured transactions**.
Throughput is completed New-Orders per minute, including required rollbacks;
higher is better. These short runs are controls for the model change, not repeated
estimates of steady-state capacity.

| Mode | Warehouses | Terminals | Scalar model tpmC | Reference model tpmC | Change |
|---|---:|---:|---:|---:|---:|
| embedded | 1 | 1 | 34,791.3 | 29,193.0 | -16.1% |
| remote | 1 | 1 | 11,325.6 | 10,490.0 | -7.4% |
| embedded | 2 | 4 | 37,300.2 | 33,826.7 | -9.3% |
| remote | 2 | 4 | 12,616.2 | 11,784.4 | -6.6% |

Overall throughput is lower in these controls. Embedded W1/T1 New-Order mean
latency is nearly unchanged (1.034 → 1.045 ms); the larger latency increases
occur in Payment, Order-Status, Delivery, and Stock-Level.

All runs passed New-Order and Payment accounting, with identical transaction
counts in each paired configuration. The final runs also passed post-workload
reference-target and pending-order queue checks. Full PostgreSQL, SQLite,
embedded Datalevin, and remote Datalevin tests passed: 65 tests and 12,140
assertions.

See the [reference-model measurement report](../../doc/measurements/tpcc-references-2026-11-05/README.md)
for per-transaction latencies, raw results, seed verification, and reproduction.
The [earlier SQL comparison](../../doc/measurements/tpcc-stock-query-2026-11-04/README.md)
used the scalar Datalevin model and 100,000 measured transactions; its throughput
figures are historical and should not be combined with these shorter runs.
