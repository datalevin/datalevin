# Write Benchmark

The purpose of this benchmark is to study the write throughput and latency under
various conditions in Datalevin. Hopefully, this gives users some reference data
points to help choosing the right transaction function, the right data batch
size and environment flags for specific use cases.

We also compare Datalevin's Datalog transaction speed with SQLite, as it is the
most widely deployed embedded database that has a reputation for being fast.

## Setup

The benchmark is conducted on a 2016 Intel Core i7-6850K CPU @ 3.60GHz with 6
cores, 64GB RAM, Samsung 860 EVO 1TB SSD.

The OS is x86_64 GNU/Linux 6.1.0-31-amd64 #1 SMP PREEMPT_DYNAMIC Debian
6.1.128-1 (2025-02-07), running OpenJDK version "17.0.14" 2025-01-21, and
Clojure version is 1.12.0. Datalevin is version 0.9.19. SQLite is from SQLite
JDBC driver 3.48.0.0, using next.jdbc Clojure library 1.3.981.

### Tasks

There are two tasks that are done sequentially.

#### Pure Write

The first task writes a 8 bytes integer as key and a 36 bytes random UUID string
as value. In Datalevin, this means an entitiy of two attributes, one is a long,
marked as `:db.unique/identity`, and the other a string. In SQLite, this is a
row of two fields, one is an integer `PRIMARY KEY`, and another a `TEXT`.

The pure write task is to do 1 million such writes to an empty DB. The integers
are all even numbers between 1 and 2 millions, so the next task can have 50%
chance of hitting existing data initially.

The writes can be batched to improve throughput as it reduces the number of disk
flush calls. We vary the batch sizes in this task: 1, 10, 100, and 1000, to
test the batching speed up effect.

#### Mixed Read/Write

With 1 million entities in DB, we then do 2 million additional operations, with
1 million reads and 1 million writes. Read and write are interleaved. These
reads/writes are individual operations, not batched.

The read/write integers are random number between 1 and 2 millions. So initally
write has a 50% chance of being an append and 50% chance of being an
overwrite. The chance of being an overwrite increases as more items are
written.

### Metrics

For every 10K write requests, a set of metrics are recorded:

* Throughput (writes/second), average throughput at the moment.
* Call Latency (milliseconds), average latency of write function calls.
* Commit Latency (milliseconds), average latency of transaction commits.

The results are written into a CSV file.

For asynchronous transactions, the measures are taken in the callback function,
which is only called after the commit is finished, so its commit latency includes
the time for flushing to disk, so as to ensure all comparisons are fair.

For consistency and to avoid exhausting system resources, the number of
asynchronous write requests in flight is capped at 1000 using a Semaphore.

At the end of the benchmark, the total number of data items on disk is also
queried to verify that all data are written, otherwise the benckmark will report
an error.

### Run

Clojure command line is needed to run the benchmarks.

For example, the command below runs pure write benchmark for `transact-async`
with batch size 10, and save the results in `dl-async-10.csv`:

```bash
time clj -Xwrite :base-dir \"/tmp/dl/\" :batch 10 :f dl-async > dl-10-async.csv
```

WAL mode is available for Datalevin tasks by appending `-wal` to task names
(for example `kv-sync-wal`, `kv-async-wal`, `dl-sync-wal`, `dl-async-wal`).
You can also set `:durability-profile` to `:strict` (default) or `:relaxed`.

For synchronous KV writers, you can run multiple calling threads to test
concurrent write ingress in WAL mode, e.g.:

```bash
time clj -Xwrite :base-dir \"/tmp/dl/\" :batch 1 :f kv-sync-wal :durability-profile :strict :threads 8 > kv-sync-wal-strict-1-t8.csv
```

This command runs mixed read/write benchmark following the pure write task
above:

```bash
time clj -Xmixed :dir \"/tmp/dl/dl-async-10\" :f dl-async > dl-10-async-mixed.csv

# WAL example:
time clj -Xmixed :dir \"/tmp/dl/dl-async-wal-10\" :f dl-async-wal > dl-10-async-wal-mixed.csv
```

The command below runs pure write benchmark for Sqlite `INSERT`  with batch size
1, and save the results in `sqlite-1.csv`

```bash
time clj -Xwrite :base-dir \"/tmp/sql/\" :batch 1 :f sql-tx > sqlite-1.csv
```

This command runs the read/write mixed task following the pure write above:

```bash
time clj -Xmixed :dir \"/tmp/sql/sqlite-1\" :f sql-tx > sqlite-1-mixed.csv
```

The total wall clock time, system time and user time are also recorded.

## Durable Datalog Transaction vs. SQLite

This is the write conditions that matter for an OLTP store.

### Write Conditions

Datalevin has two Datalog transaction functions:

* `transact!`
* `transact-async`

Both are durable by default. In the case of `transact-async`, the returned
`future` is only realized after the data are flushed to disk. Both are tested.

`transact` is just the blocked version of `transact-async` so it is not tested.
There are two faster `init-db` and `fill-db` functions that directly load
prepared datoms to bypass the expensive process of verifying integrity of
everything. These are not formally tested in this benchmark, as we are mainly
interested in transactions of raw data.

SQLite has two durable transaction mode: default and WAL in FULL sync mode.

### Results

We will first focus on durable writes. We presents throughput and commit
latency. The call latencies are close to commit latencies in synchronous writes,
while close to zero for asynchronous writes. So call latencies are omitted from
this presentation.

#### Pure Write Task

We plotted the accumulated throughput for each batch size. The Y axis (writes
per second) is on the log scale. The X axis is every 10000 writes per tick.

##### Batch size 1 throughput

<p align="center">
<img src="throughput-1.png" alt="throughput batch size 1" height="300"></img>
</p>

When data are not batched, Datalevin default writes are much faster than
SQLite's default, and Datalevin async writes are much faster than SQLite's WAL
mode.

##### Batch size 10 throughput

<p align="center">
<img src="throughput-10.png" alt="throughput batch size 10" height="300"></img>
</p>

When data are batched at size 10, the same relative positions remain, but the
gaps narrow.

##### Batch size 100 throughput

<p align="center">
<img src="throughput-100.png" alt="throughput batch size 100" height="300"></img>
</p>

At batch size 100, SQLite's default mode writes are now faster than Datalevin's
default writes, and WAL mode is faster than async writes.

Notice that Datalevin's async writes at batch 100 are not faster than
that of batch size 10.

##### Batch size 1000 throughput

<p align="center">
<img src="throughput-1000.png" alt="throughput batch size 1000" height="300"></img>
</p>

At batch size 1000, SQLite's default writes are now faster than Datalevin's
Async writes in all cases.

##### Average commit latency

The average latency of commits (milliseconds) for all conditions are plotted:

<p align="center">
<img src="commit-latency.png" alt="Commit Latency" height="300"></img>
</p>

Datalevin's commit latency goes up as the batch size goes up. The same pattern
is with WAL mode. On the other hand, SQLite's default has a sweet spot at batch
size 10.

None of SQLite's commit latency goes under 1 milliseconds, while Datalevin's
Async transaction can go way below 1 milliseconds at batch size 1 and 10.

#### Mixed Read/Write Task

The wallclock time to finish the 2 millions mixed reads/writes is plotted on
the left, and the CPU times are plotted at right:

<p align="center">
<img src="wallclock-time.png" alt="Mixed Read/Write Wallclock Time" height="300"></img>
<img src="cpu-time.png" alt="Mixed Read/Write CPU Time" height="300"></img>
</p>

For mixed read/write task, Datalevin default is much faster than SQLite default,
Datalevn Async is much faster than SQLite WAL, while SQLite WAL is faster than
Datalevin default.

As to CPU time, the differences among different conditions are not big. We can
see that Aysnc in Datalevin and WAL mode in SQLite do save CPU time compared
with respective default conditions.

Notice that most of the time is spent on waiting for I/O in the three
synchronous conditions, where CPU times are relatively small compared with the
wallclock time. The exception is Datalevin Async, where the total CPU
time (227.89 seconds) is actually greater than wallclock time (111.04 seconds),
indicating an effective utilization of multicore and the apparent hiding of I/O
wait time.

#### Remark

In general, Datalevin's Datalog transaction speed is more stable and less
sensitive to batch size variations. Particularly, the throughput of Async
transaction hovers around 15k to 40k per second regardless the batch sizes.
SQLite is very good at batch transaction, but individual transaction fails
short.

It should be mentioned that for bulk loading of large amount of data,  Datalevin
has the option to use `init-db` and `fill-db` functions. For example, it took 5.3
seconds to load this data set using `init-db` and `fill-db`, slightly better
than the fastest SQLite's 6.7 seconds at WAL batch size 1000.

Finally, Datalevin builds index at data load time so it is maintenance free,
whereas SQLite does not build index at transaction time so users have to manage
indices.

## Non-durable Datalog Transaction vs. SQLite

These are generally faster than durable writes, and can be used when durability
is not a top priority.

We are interested in these non-durable write conditions, because there are many
good use cases for a fast non-durable store, such as caching, session
management, temporary data storage, real-time analytics, message queues,
configuration, leaderboards, and so on.

### Write Conditions

Datalevin supports faster, albeit less durable writes, by setting some
environment flags (either when openning the DB, or call `set-env-flags`).
These are the conditions:

* dl-sync-nometa: `transact!`, with `:nometasync` flag
* dl-sync-nosync: `transact!`, with `:nosync` flag
* dl-sync-wm: `transact!`, with `:writemap` and `:mapasync` flags
* dl-async-nometa: `transact-async`, with `:nometasync` flag
* dl-async-nosync: `transact-async`, with `:nosync` flag
* dl-async-wm: `transact-async`, with `:writemap` and `:mapasync` flags

SQLite also has several non-durable write modes. We test these combinations:

* sql-default-normal: default journal mode, and sync mode normal
* sql-default-off: default journal mode, and sync mode off
* sql-wal-normal: WAL journal mode, and sync mode normal
* sql-wal-off: WAL journal mode, and sync mode off

### Results

We list the throughput and latency for all conditions. Throughput is in writes
per second and latency in milliseconds.

#### Pure Write Task

Batch size 1:

|Condition |Throughput|Latency|
|---|---|---|
| dl-sync-nometa| 291.39| 	3.62|
| dl-sync-nosync| 15453.56|	0.07|
| dl-sync-wm| 22460.30|	0.05|
| dl-async-nometa| 16917.77|	0.07|
| dl-async-nosync| 37697.74|	0.02|
| dl-async-wm| 41457.29|	0.02|
| sql-default-normal|83.62|	12.16|
| sql-default-off|9163.72	|0.11|
| sql-wal-normal| 18353.00|	0.05|
| sql-wal-off| 24986.88|	0.04|

Batch size 10:

|Condition |Throughput|Latency|
|---|---|---|
| dl-sync-nometa|1965.79|	5.09|
| dl-sync-nosync|41523.07|	0.24|
| dl-sync-wm|52222.05|	0.19|
| dl-async-nometa|30974.74|	0.41|
| dl-async-nosync|50495.61|	0.19|
| dl-async-wm|54961.35|	0.17|
| sql-default-normal|1040.87|	12.06|
| sql-default-off|67222.37	|0.14|
| sql-wal-normal|107158.17|	0.08|
| sql-wal-off|134934.56|	0.08|

Batch size 100:

|Condition |Throughput|Latency|
|---|---|---|
| dl-sync-nometa|9120.43|	17.57|
| dl-sync-nosync|53256.64	|1.83|
| dl-sync-wm|61682.7	|1.52|
| dl-async-nometa|27368.42	|3.82|
| dl-async-nosync|44844.06	|2.16|
| dl-async-wm|50626.12	|1.67|
| sql-default-normal|7865.47|	12.72|
| sql-default-off|243961.94	|0.4|
| sql-wal-normal|248942.00|	0.31|
| sql-wal-off|294724.43|	0.28|

Batch size 1000:

|Condition |Throughput|Latency|
|---|---|---|
| dl-sync-nometa|15622.31|	|69.30|
| dl-sync-nosync|55242.51	|18.10|
| dl-sync-wm|61774.15	|15.80|
| dl-async-nometa|39931.57|	71.00|
| dl-async-nosync|41811.15	|25.00|
| dl-async-wm|44209.15	|21.69|
| sql-default-normal|65044.88|	15.20|
| sql-default-off|371747.21	|2.4|
| sql-wal-normal|338409.48|	2.30|
| sql-wal-off|376364.32|	2.30|

#### Mixed Read/Write Task

|Condition |Throughput|Latency|
|---|---|---|
| dl-sync-nometa|269.14|	3.85|
| dl-sync-nosync|5968.12|	0.17|
| dl-sync-wm|6955.55|	0.14|
| dl-async-nometa|8545.21|	0.13|
| dl-async-nosync|11424.13|	0.09|
| dl-async-wm|11339.99|	0.09|
| sql-default-normal|82.48|	12.15|
| sql-default-off|7715.45	|0.13|
| sql-wal-normal|8290.71|0.16|
| sql-wal-off|19112.05|	0.05|

### Remark

Patterns similar to durable writes seems to apply to non-durable writes:
Datalevin is less sensitive to batching and is better at individual
transactions, whereas SQLite is better at batch transactions.

For mixed read/write task, Datalevin is slightly better overall, except that
sql-wal-off performs the best. The most commonly used SQLite write mode,
sql-wal-normal, performs slightly worse than the equivalent Datalevin write
mode, dl-async-nometa, where both behave similarly: the last transaction may
be lost at an untimely system crash but the database is saved from corruption.

## Key Value Transaction

Datalevin wraps LMDB to offer KV store feature. Here we do not compare Datalevin
with other KV stores, as there are plenty of such comparison between LMDB and
others KV stores already.

We study the throughput and latency of KV writes under different combination of
transact function and env flags, using the same tasks as above. For pure write
task, we write 1 million pairs of key and value; 2 millions of reads/writes for
mixed read/write task, the same as Datalog transaction.

### Write Conditions

All combinations of batch size, transaction function and environment flags are
tested:

* sync-default: `transact-kv`, with default flags
* sync-nometa: `transact-kv`, with `:nometasync` flag
* sync-nosync: `transact-kv`, with `:nosync` flag
* sync-wm: `transact-kv`, with `:writemap` and `:mapasync` flags
* async-default: `transact-kv-async`, with default flags
* async-nometa: `transact-kv-async`, with `:nometasync` flag
* async-nosync: `transact-kv-async`, with `:nosync` flag
* async-wm: `transact-kv-async`, with `:writemap` and `:mapasync` flags

### Results

Throughput is in writes per second and latency in milliseconds.

#### Pure Write Task

Batch size 1:

| |sync-default|sync-nometa|sync-nosync|sync-wm|async-default|async-nometa|async-nosync|async-wm|
|---|---|---|---|---|---|---|---|---|
|Throughput|232.27	|1013.77	|109134.56	|230149.6	|109973.73	|173688.4	|179371.47	|174347.78|
|Latency	|5.8	|0.99	|0.01	|0	|0.01	|0.01	|0.01	|0.01|

Batch size 10:

| |sync-default|sync-nometa|sync-nosync|sync-wm|async-default|async-nometa|async-nosync|async-wm|
|---|---|---|---|---|---|---|---|---|
|Throughput| 5147.9	| 6738.41	| 520833.33	| 743494.42	| 390767.41	| 570534.48	| 759197.86	| 767131.84|
|Latency|1.96	|3.45	|0.02	|0.01	|0.02	|0.02	|0.01	|0.01|

Batch size 100:

| |sync-default|sync-nometa|sync-nosync|sync-wm|async-default|async-nometa|async-nosync|async-wm|
|---|---|---|---|---|---|---|---|---|
|Throughput|32795.49|	45132.46|	913242.01|	1012145.75|	400225.73|	462968.75|	1246203.35|	1256516.13|
|Latency|2.95|	2.16|	0.1|	0.08|	0.21|	0.19|	0.05|	0.06|

Batch size 1000:

| |sync-default|sync-nometa|sync-nosync|sync-wm|async-default|async-nometa|async-nosync|async-wm|
|---|---|---|---|---|---|---|---|---|
|Throughput|223914.02|	251762.34|	1109877.91|	1131221.72|	366345.71|	467817.9|	619602.47|	652868.55|
|Latency|5.2|	3.7|	0.8|	0.8|	2.33|	1.78|	1|	1.86|

#### Mixed Read/Write Task

| |sync-default|sync-nometa|sync-nosync|sync-wm|async-default|async-nometa|async-nosync|async-wm|
|---|---|---|---|---|---|---|---|---|
|Throughput|216.01|	991.07|	67078.08|	106112.05|	25952.69|	27631.68|	80696.16|	84061.15|
|Latency|5.83|	1|	0.01|	0.01|	0.05|	0.04|	0.01|	0.01|

### Remark

It is interesting to see that for durable writes, asynchronous writes can
usually perform about half as fast as the fastest non-durable writes.

For non-durable writes, synchronous transaction with `:writemap` and `:mapasync`
flags is the best performing method, while asynchronous writes can actually
be little behind sometimes, perhaps due to the overhead of asynchronous processing.

Unlike Datalog store, batching in KV store has more impact, and the growth seem
to be linear with batch size increases.
