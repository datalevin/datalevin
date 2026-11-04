# TPC-C-Derived Benchmark

This benchmark runs a TPC-C-derived transactional workload against Datalevin.
TPC-C is the standard OLTP benchmark: nine normalized tables, five short
read/write transactions, and a strict ACID contract with hot-row contention.

> **Derived, not audited.** This is a TPC-C-derived implementation. It follows
> the published schema and transaction definitions but is not an audited TPC-C
> result and must not be presented as official. See the [TPC fair-use
> rules](https://www.tpc.org/information/other/policy_fair_use5.asp).

The generator, loaders, transaction implementations, and drivers are available
for Datalevin, SQLite, and PostgreSQL.

## Schema and population

The nine TPC-C tables are `warehouse`, `district`, `customer`, `history`,
`orders`, `new_order`, `order_line`, `item`, and `stock`. Monetary values are
stored as doubles/REAL in Datalevin and SQLite and fixed-precision numerics in
PostgreSQL. Dates are ISO-8601 strings; returned monetary fields are normalized
to doubles.

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

Datalevin uses unique item and warehouse IDs and unique composite business keys
for districts, customers, stock, orders, new orders, and order lines. Secondary
composites index customer surnames and orders by customer. Point reads use lookup
refs and pulls; Delivery and Order-Status seek the first or last matching index
entry, and line reads use bounded key ranges. Descriptive strings are stored
without value indexes. Both bulk loads and transactions maintain the composite
attributes automatically.

Reload Datalevin databases created with the previous schema before running this
version. The driver rejects databases missing the required keys.

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

SQLite and PostgreSQL terminals own separate JDBC connections, so one terminal's
rollback cannot affect another terminal's transaction. Datalevin terminals share
a connection and use its explicit transaction boundary:

- **Datalevin** performs New-Order, Payment, Order-Status, and Delivery inside
  `d/with-transaction`, using the connection bound by that macro for all reads
  and writes. Concurrent writers wait before reading values they will update.
  `d/db` alone returns a database reference and does not establish a transaction
  snapshot. Stock-Level uses committed reads with the specification's relaxed
  isolation. Transaction errors propagate and abort the run.
- **SQLite** has a single writer and no row locking. Terminals use independent
  connections, and every transaction is serialized on one JVM lock so the
  read-modify-write sequences are atomic. WAL is enabled once before terminals
  start; connections use a 30 s busy timeout. Order-Status also opens a database
  transaction so separate JDBC callers get a consistent snapshot.
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

The Datalevin driver also checks Payment accounting:

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
Datalevin's monetary deltas allow less than half a cent of floating point error.
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
The full transaction mix also passed the one- and two-warehouse runs below.

## Host control

All three drivers wrap the measured interval in the shared host-control helper.
On macOS it pauses the current user's `mediaanalysisd` and `photoanalysisd` with
`SIGSTOP` and resumes exactly those processes with `SIGCONT` afterward; a daemon
that was already stopped before the run is left untouched. On other platforms,
or where the signal is not permitted, it is a no-op and the driver still runs.

## Running

```bash
# Load a one-warehouse database
clj -X:datalevin-db '{:warehouses 1 :seed 42 :dir "db-w1"}'
clj -X:sqlite-db    '{:warehouses 1 :seed 42 :path "sqlite-tpcc.db"}'

# Run the transaction mix (same options for either system)
clj -X:datalevin-bench '{:dir "db-w1" :warehouses 1 :txns 10000 :threads 4 :warmup 1000 :seed 42}'
clj -X:sqlite-bench    '{:path "sqlite-tpcc.db" :warehouses 1 :txns 10000 :warmup 1000 :seed 42}'
```

Options: `:warehouses`, `:txns`, `:threads`, `:warmup`, and `:seed` for both.
The Datalevin runner takes `:dir`; the SQLite runner takes `:path`.

All three runners also accept `:load-seed` (default 42). Set it to the `:seed`
used when loading the database so the surname constant is checked against the
actual population. The run's `:seed` controls transaction inputs independently;
for example, a database loaded with `:seed 123` can be benchmarked with
`{:load-seed 123 :seed 42 ...}`. The load seed is supplied by the caller; it is
not read from database metadata.

Run generator and transaction regression tests from this directory:

```bash
clojure -M:test
```

Transaction tests use temporary databases and cover concurrent Payments to the
same and different customers/districts, Payment with Delivery, concurrent
New-Order and Delivery, rollback, and detection of lost Payment totals. Payment
concurrency is checked with WAL disabled and with strict and relaxed WAL.
Stock tests cover the threshold, repeated items, multiple replenishments, local
and remote supply, and rollback using Datalevin and SQLite. Rollback tests observe
the pending header, counter, stock, and line writes and verify that every row is
restored afterward. Enable the same
integration cases for PostgreSQL with a test database URL:

```bash
TPCC_TEST_PG_URL=jdbc:postgresql://localhost:5432/postgres \
  clojure -M:test
```

`TPCC_TEST_PG_USER` and `TPCC_TEST_PG_PASS` optionally supply credentials. The
PostgreSQL test creates and removes its own unique schema; the database user
needs permission to create schemas.

Datalevin's native and supporting dependency versions come from the local root
project. Databases created with the former native 0.19.4 override need to be
regenerated in a fresh directory for the current native storage format.

## Durability

The driver runs against whatever durability configuration the database was
created with. Datalevin's default LMDB path, WAL `:strict`, and WAL `:relaxed`
map to the same strict/relaxed split used by the `write-bench`. A publishable
durability comparison should create one database per condition and report them
separately; the runner does not yet switch modes.

## Measured results

Recorded November 4, 2026 on an Apple M3 Pro (36 GiB RAM), OpenJDK 21.0.13,
Clojure 1.12.5, native 1.2.2, and an 8 GiB JVM heap. Each run starts from a fresh
copy of the seed-42 population, with 1,000 warmup and 10,000 measured transactions.
Runs execute sequentially without a profiler. These are single-pass local
measurements, not audited TPC-C results or steady-state capacity estimates.

Throughput is completed New-Orders per minute, including required invalid-item
rollbacks. Higher is better.

| Warehouses | Terminals | Datalevin default LMDB | Datalevin strict WAL | SQLite | PostgreSQL |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 9,573.5 | 35,351.3 | 62,073.8 | 26,822.1 |
| 1 | 4 | 9,989.1 | 39,035.9 | 62,136.7 | 77,734.3 |
| 2 | 4 | 8,236.0 | 35,081.6 | 60,926.6 | 81,829.8 |

Default Datalevin uses synchronous LMDB commits with WAL disabled. The separate
WAL runs enable `:wal? true` with `:wal-durability-profile :strict`. SQLite 3.51.1
uses WAL and `synchronous=FULL`; PostgreSQL 18.6 uses `fsync=on`,
`synchronous_commit=on`, and `full_page_writes=on`. These are different storage
and synchronization implementations; settings and connection costs are recorded
in the measurement notes.

All six Datalevin runs passed New-Order invariants and independent Payment
accounting over warmup plus measurement. Concurrent WAL runs include the required
invalid-item rollbacks; each explicit transaction has an independent outcome.
SQL figures are from the preceding same-day comparison, with the same population
and inputs. Transaction and rollback counts match across backends.

Datalevin still shows limited throughput scaling with terminals. Order-Status
shares the writer boundary to preserve its snapshot. These short runs also vary
with commit timing; see the supporting repeated measurements.

See the [measurement report](../../doc/measurements/tpcc-fixes-2026-11-04/README.md)
for before/after results, latency, regression checks, and profiles, and the
[preceding comparison](../../doc/measurements/tpcc-gaps-2026-11-04/README.md)
for SQL settings and results.

## Notes

- Datalevin's explicit write transactions hold the writer for their reads as
  well as their writes. Order-Status uses this boundary too. Reported latency
  includes waiting for the transaction.
- TPC-C naming and cardinalities are reproduced, but dictionary text is
  generated rather than taken from the specification's appendix.
