# TPC-C-Derived Benchmark

This benchmark runs a TPC-C-derived transactional workload against Datalevin.
TPC-C is the standard OLTP benchmark: nine normalized tables, five short
read/write transactions, and a strict ACID contract with hot-row contention.

> **Derived, not audited.** This is a TPC-C-derived implementation. It follows
> the published schema and transaction definitions but is not an audited TPC-C
> result and must not be presented as official. See the [TPC fair-use
> rules](https://www.tpc.org/information/other/policy_fair_use5.asp).

## Status

- Deterministic population generator: complete.
- Datalevin loader: complete.
- Datalevin transactions (New-Order, Payment, Order-Status, Delivery,
  Stock-Level): implemented; write transactions use explicit transactions.
- SQLite loader and transactions: complete.
- Driver with tpmC and per-transaction latency: complete for Datalevin and
  SQLite.
- PostgreSQL implementation: pending.

## Schema and population

The nine TPC-C tables are `warehouse`, `district`, `customer`, `history`,
`orders`, `new_order`, `order_line`, `item`, and `stock`. Monetary values are
stored as doubles/REAL and dates as ISO-8601 strings in every system so results
compare directly.

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

Each district's 3,000 orders map bijectively to its 3,000 customers, the first
2,100 orders are delivered, and orders 2,101-3,000 are the open new orders.
Generation is deterministic per table from a seed, so reloads are reproducible.

## Transactions

| Transaction | Mix | Writes |
|---|---:|---|
| New-Order | 45% | district counter, stock, order, new_order, order_line |
| Payment | 43% | warehouse, district, customer, history |
| Order-Status | 4% | none |
| Delivery | 4% | new_order, orders, order_line, customer |
| Stock-Level | 4% | none |

All drivers share New-Order line generation: 5-15 lines per order, with one
random 1-in-100 rollback decision per order. A rollback order has only its
final item id replaced with an unused value, as specified in
[TPC-C §2.4.1.3-5](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf#page=28).

New-Order reads the warehouse tax, the district counter and tax, the customer
discount, and each item and stock row; it then updates stock and inserts the
order, the new-order row, and the order lines. Any invalid item rolls the whole
transaction back. Payment updates the warehouse and district year-to-date
totals, the customer balance/payment count, and appends a history row. Delivery
removes the oldest new-order in each district and delivers its lines.

## Concurrency and correctness

SQLite and PostgreSQL terminals own separate JDBC connections, so one terminal's
rollback cannot affect another terminal's transaction. Datalevin terminals share
a connection and use its explicit transaction boundary:

- **Datalevin** performs every New-Order, Payment, and Delivery inside
  `d/with-transaction`, using the connection bound by that macro for all reads
  and writes. Concurrent writers wait before reading values they will update.
  `d/db` alone returns a database reference and does not establish a transaction
  snapshot. Transaction errors propagate and abort the run.
- **SQLite** has a single writer and no row locking. Terminals use independent
  connections, and every transaction is serialized on one JVM lock so the
  read-modify-write sequences are atomic. WAL is enabled once before terminals
  start; connections use a 30 s busy timeout.
- **PostgreSQL** supports concurrent writers. Read-then-write statements use
  `SELECT ... FOR UPDATE` (warehouse, district, customer, stock, and the oldest
  new-order row), and the driver retries transactions that fail with a
  serialization or deadlock SQLSTATE (`40001`/`40P01`).

All backends share the stock calculation from
[TPC-C §2.4.2](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/tpc-c_v5.11.0.pdf#page=30):
subtract the ordered quantity when stock is at least that quantity plus 10;
otherwise add 91 after subtracting it. Repeated lines for the same supplying
warehouse and item are processed in input order before writing the final stock
row. This preserves multiple replenishments and increments the order and remote
counters for each applicable line.

All drivers check two New-Order invariants after every run:

1. Each district's `next_o_id` advanced by exactly the number of New-Order
   transactions that district committed.
2. Each district's order count grew by exactly that same number.

The Datalevin driver also checks Payment accounting:

3. Each warehouse's and district's year-to-date total increased by the sum of
   successful Payments to it.
4. Each district's history amount and row count increased by the successful
   Payment amount and count, respectively.
5. Each district's customer year-to-date payments and payment counts increased
   by those same amounts and counts. Delivery does not change these fields.

Datalevin checks use baselines captured after warmup and count only successful
measured transactions. Counts must match exactly; monetary deltas allow less
than half a cent of floating point error. Checks run after all terminals finish,
outside the timed interval. Any mismatch throws with details instead of
reporting a successful benchmark result.

PostgreSQL New-Order stock updates are covered by integration tests against
PostgreSQL 18.6. A full PostgreSQL transaction-mix run has not been validated.

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

Run generator and transaction regression tests from this directory:

```bash
clojure -M:test
```

Transaction tests use temporary databases and cover concurrent Payments to the
same and different customers/districts, Payment with Delivery, concurrent
New-Order and Delivery, rollback, and detection of lost Payment totals. Payment
concurrency is checked with WAL disabled and with strict and relaxed WAL.
Stock tests cover the threshold, repeated items, multiple replenishments, local
and remote supply, and rollback using Datalevin and SQLite. Enable the same
integration cases for PostgreSQL with a test database URL:

```bash
TPCC_TEST_PG_URL=jdbc:postgresql://localhost:5432/postgres \
  clojure -M:test -n datalevin-tpcc.stock-test
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

## Smoke results (not performance claims)

Current Datalevin smoke runs use fresh one-warehouse databases, seed 42, 100
warmup transactions, and native 1.1.1 with default durability:

| Terminals | Measured txns | New-Orders | tpmC | New-Order and Payment invariants |
|---|---:|---:|---:|---|
| 1 | 400 | 171 | ~2,961 | OK |
| 4 | 400 | 172 | ~3,323 | OK |

Stock quantities remained within [10, 100] in both runs.

Historical results, single warehouse, seed 42, on the development host:

| System | Terminals | Txns | New-Orders | tpmC | Invariants |
|---|---|---:|---:|---:|---|
| Datalevin (old implementation) | 1 | 300 | 120 | ~1,780 | New-Order only |
| Datalevin (old implementation) | 4 | 400 | 180 | ~4,750 | New-Order only |
| SQLite | 1 | 200 | 93 | ~11,900 | OK |
| SQLite | 1 | 100 | 37 | ~20,200 | OK |

The old Datalevin results predate the Payment concurrency fix and do not
establish Payment correctness or describe the current transaction strategy.
Per-transaction latency is printed by each driver. Use a fresh database, a fixed
seed, and the same durability setting for any comparison. SQLite serializes
writers, so its multi-terminal comparison is not meaningful without a
shared-cache or WAL configuration.

## Notes

- Datalevin's explicit write transactions hold the writer for their reads as
  well as their writes. Reported latency includes waiting for that transaction.
- The workload is single-warehouse; remote warehouses (`ol_supply_w_id` other
  than the home warehouse) are part of the schema but not yet generated.
- TPC-C naming and cardinalities are reproduced, but dictionary text is
  generated rather than taken from the specification's appendix.
