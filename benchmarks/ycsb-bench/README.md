# YCSB-style KV and Datalog benchmark

Benchmarks Datalevin's KV and Datalog APIs in embedded and remote modes,
with SQLite and PostgreSQL comparisons. Uses YCSB-style A–F workloads.

## Run

From this directory, using the Clojure CLI and the repository's supported JDK:

```sh
# Quick check of all Datalevin workloads and modes.
clojure -M:jvm:bench --api all --mode all --workload all \
  --records 100 --ops 200 --warmup 50 --threads 3

# Run the one- and eight-worker matrices shown below.
clojure -J-Xms4g -J-Xmx4g -M:jvm:bench \
  --system datalevin --api all --mode all --workload all \
  --records 10000 --ops 100000 --warmup 20000 --client-counts 1,8 \
  --seed 17 --durability strict --output /tmp/datalevin-ycsb.edn

clojure -M:jvm:bench --help
```

Select individual cases with `--api kv|datalog`, `--mode embedded|remote`, and
`--workload A|B|C|D|E|F`. Use `--threads N` for a single worker count or
`--repetitions N` for repeated trials. Operation counts are totals across workers.

Each case uses a fresh temporary database, removed afterward. `--keep-db`
retains it. Remote Datalevin runs its server in a separate JVM over loopback TCP.
On macOS, the harness pauses media-analysis processes during the run and resumes
them afterward.

## SQL comparisons

SQLite is paired with embedded Datalevin; PostgreSQL with remote Datalevin.
Use `--system all` to run both members of each pair, or select SQL alone:

```sh
clojure -J-Xms4g -J-Xmx4g -M:jvm:bench \
  --system sqlite --api all --mode embedded --workload all \
  --records 10000 --ops 100000 --warmup 20000 --client-counts 1,8 \
  --seed 17 --durability strict --output /tmp/datalevin-ycsb-sqlite.edn

export YCSB_PG_URL=jdbc:postgresql://127.0.0.1:5432/ycsb
export YCSB_PG_USER=benchmark
# Set YCSB_PG_PASSWORD if needed.
clojure -J-Xms4g -J-Xmx4g -M:jvm:bench \
  --system postgres --api all --mode remote --workload all \
  --records 10000 --ops 100000 --warmup 20000 --client-counts 1,8 \
  --seed 17 --durability strict --output /tmp/datalevin-ycsb-postgres.edn
```

PostgreSQL must already be running, and the role must be able to create schemas.
The harness creates and removes a separate schema for each case.

## Workloads

| Workload | Operation mix | Key distribution |
|---|---|---|
| A | 50% read, 50% update | Zipfian |
| B | 95% read, 5% update | Zipfian |
| C | 100% read | Zipfian |
| D | 95% read, 5% insert | Latest |
| E | 95% scan, 5% insert | Zipfian |
| F | 50% read, 50% read-modify-write | Zipfian |

Records have ten 100-byte fields by default. Reads return all fields; updates
replace one field. E scans return 1–100 records in key order. F performs a read
followed by a separate update, counted together as one operation. All stores
index the application key and leave payload fields unindexed.

## Results

Host: macOS arm64, 12 available processors, Java 21.0.12.1. Each case used
10,000 initial records, 20,000 warmup operations, 100,000 measured operations,
seed 17, and one trial. Client and Datalevin server heaps were 4 GiB.

All results use strict durability: Datalevin strict WAL, SQLite WAL with
`synchronous=FULL`, and PostgreSQL with `synchronous_commit=on`, `fsync=on`, and
`full_page_writes=on`. SQLite JDBC was 3.51.1.0; PostgreSQL was 18.6.

Datalevin was measured on October 10, 2026; SQLite and PostgreSQL on October 9
with the same benchmark settings. All cases passed validation. These are warmed
runs; performance varies with hardware, cache state, and workload size.

### Throughput

Values are **thousands of operations per second**. Ratios are Datalevin / SQL;
above 1× means Datalevin is faster. Ratios use unrounded measurements.

**1 worker**

| Workload | API | Datalevin embedded | SQLite | Ratio | Datalevin remote | PostgreSQL | Ratio |
|---|---|---:|---:|---:|---:|---:|---:|
| A | KV | 37.7 | 24.4 | 1.55× | 15.5 | 15.6 | 1.00× |
| A | Datalog | 29.1 | 24.6 | 1.18× | 13.1 | 15.5 | 0.84× |
| B | KV | 242.9 | 88.4 | 2.75× | 35.7 | 28.6 | 1.25× |
| B | Datalog | 110.8 | 93.9 | 1.18× | 28.1 | 28.8 | 0.98× |
| C | KV | 680.3 | 120.3 | 5.66× | 33.3 | 32.7 | 1.02× |
| C | Datalog | 187.1 | 120.3 | 1.56× | 26.4 | 32.2 | 0.82× |
| D | KV | 158.3 | 79.6 | 1.99× | 35.7 | 26.9 | 1.33× |
| D | Datalog | 90.0 | 71.8 | 1.25× | 27.8 | 27.1 | 1.03× |
| E | KV | 22.4 | 4.9 | 4.53× | 8.0 | 6.7 | 1.20× |
| E | Datalog | 6.7 | 5.0 | 1.34× | 4.7 | 6.9 | 0.68× |
| F | KV | 39.9 | 22.5 | 1.78× | 13.0 | 11.6 | 1.12× |
| F | Datalog | 26.4 | 21.5 | 1.23× | 11.2 | 12.4 | 0.90× |

**8 workers**

| Workload | API | Datalevin embedded | SQLite | Ratio | Datalevin remote | PostgreSQL | Ratio |
|---|---|---:|---:|---:|---:|---:|---:|
| A | KV | 131.3 | 27.8 | 4.73× | 46.1 | 71.7 | 0.64× |
| A | Datalog | 120.3 | 24.2 | 4.97× | 36.3 | 71.6 | 0.51× |
| B | KV | 878.5 | 156.5 | 5.61× | 106.8 | 132.0 | 0.81× |
| B | Datalog | 528.3 | 138.8 | 3.81× | 89.4 | 134.5 | 0.66× |
| C | KV | 3,356.3 | 595.4 | 5.64× | 128.0 | 145.2 | 0.88× |
| C | Datalog | 1,005.7 | 577.6 | 1.74× | 115.6 | 140.3 | 0.82× |
| D | KV | 645.2 | 125.7 | 5.13× | 122.0 | 125.0 | 0.98× |
| D | Datalog | 416.9 | 120.2 | 3.47× | 93.5 | 124.6 | 0.75× |
| E | KV | 88.3 | 20.9 | 4.22× | 35.8 | 27.0 | 1.33× |
| E | Datalog | 36.7 | 20.6 | 1.78× | 27.3 | 26.8 | 1.02× |
| F | KV | 150.1 | 24.1 | 6.23× | 38.0 | 59.2 | 0.64× |
| F | Datalog | 116.2 | 16.5 | 7.05× | 37.4 | 59.4 | 0.63× |

### Datalevin p99 latency

Values are **microseconds**.

**1 worker**

| Workload | KV embedded | KV remote | Datalog embedded | Datalog remote |
|---|---:|---:|---:|---:|
| A | 58.8 | 126.8 | 69.4 | 155.9 |
| B | 50.5 | 135.4 | 65.5 | 196.5 |
| C | 1.9 | 39.0 | 6.6 | 82.3 |
| D | 98.8 | 153.8 | 115.3 | 212.4 |
| E | 120.5 | 246.0 | 293.9 | 636.8 |
| F | 56.9 | 157.5 | 84.5 | 181.3 |

**8 workers**

| Workload | KV embedded | KV remote | Datalog embedded | Datalog remote |
|---|---:|---:|---:|---:|
| A | 214.1 | 549.6 | 250.2 | 740.2 |
| B | 159.6 | 484.8 | 198.8 | 608.2 |
| C | 4.6 | 142.2 | 15.5 | 135.9 |
| D | 214.7 | 356.9 | 297.4 | 596.0 |
| E | 390.8 | 616.2 | 593.9 | 770.5 |
| F | 132.1 | 618.5 | 205.1 | 742.5 |
