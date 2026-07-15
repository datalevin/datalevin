# LDBC SNB Benchmark

This benchmark implements the Graph Data Council's [LDBC Social Network
Benchmark (SNB)](https://ldbcouncil.org/benchmarks/snb/) Interactive Workload.

## Overview

LDBC SNB is an industry-standard benchmark for graph databases that
simulates a social network workload with:

- **8 entity types**: Person, Post, Comment, Forum, Place, Organization, Tag,
  TagClass
- **7 short queries (IS1-IS7)**: Point lookups and small traversals
- **14 complex queries (IC1-IC14)**: Multi-hop traversals, aggregations, path
  finding

We unofficially implemented the benchmark specification [1] in Datalevin. We
also include an implementation in Neo4j for comparison.

## Schema

The LDBC SNB data model is mapped to the attribute centered Datalevin
[schema](src/ldbc_snb_bench/schema.clj):

```clojure
;; Entity attributes
:person/id, :person/firstName, :person/lastName, ...
:message/id, :message/content, :message/hasCreator, ...
:message/isContainedIn, :message/replyOf, ...
...

;; Relationship edges attributes
:knows/person1, :knows/person2, :knows/creationDate
:workAt/person, :workAt/organization, :workAt/workFrom
...
```

## Query

Two classes of queries are included in the benchmark.

### Interactive [Short](src/ldbc_snb_bench/queries/short.clj) Queries (IS1-IS7)

| Query | Description |
|-------|-------------|
| IS1 | Profile of a Person |
| IS2 | Recent messages of a Person |
| IS3 | Friends of a Person |
| IS4 | Content of a message |
| IS5 | Creator of a message |
| IS6 | Forum of a message |
| IS7 | Replies to a message |

### [Interactive](src/ldbc_snb_bench/queries/interactive.clj) Complex Queries
(IC1-IC14)

| Query | Description | Key Features |
|-------|-------------|--------------|
| IC1 | Friends with given first name | 3-hop traversal with recursive rules |
| IC2 | Recent messages by friends | Join + temporal filter |
| IC3 | Friends in countries X and Y | Geographic filtering |
| IC4 | New topics | Aggregation + negation |
| IC5 | New groups | Forum membership |
| IC6 | Tag co-occurrence | Tag joins |
| IC7 | Recent likes | Like relationship |
| IC8 | Recent replies | Comment chains |
| IC9 | Recent posts by friends-of-friends | 2-hop traversal |
| IC10 | Friend recommendation | Common interests |
| IC11 | Job referral | Work relationships |
| IC12 | Expert search | TagClass hierarchy (recursive) |
| IC13 | Shortest path | Recursive path finding |
| IC14 | Trusted connection paths | Weighted paths |

## Prerequisites

- JDK 21+ (for running the benchmark)
- Clojure CLI tools
- Docker (for data generation)
- ~10GB disk space for SF1 data and the Datalevin database

## Data Generation

Generate LDBC SNB data using the LDBC SNB Spark Datagen [2] via Docker:

```bash
# Clone Datagen (one time, sibling to this repo)
git clone https://github.com/ldbc/ldbc_snb_datagen_spark.git ../ldbc_snb_datagen_spark

# Generate SF1 data (scale factor 1)
./generate-data-docker.sh \
  --scale-factor 1 \
  --parallelism 4 \
  --memory 8g
```

The `data/` directory should have this structure:
```
data/
└── graphs/csv/raw/composite-merged-fk/
    ├── static/Place/part-*.csv
    ├── static/Organisation/part-*.csv
    ├── static/TagClass/part-*.csv
    ├── static/Tag/part-*.csv
    ├── dynamic/Person/part-*.csv
    ├── dynamic/Person_knows_Person/part-*.csv
    ├── dynamic/Forum/part-*.csv
    ├── dynamic/Post/part-*.csv
    ├── dynamic/Comment/part-*.csv
    └── ...
```

## Running the Benchmark

### 1. Load Data

```bash
# Load LDBC SNB data into Datalevin
clj -M -m ldbc-snb-bench.core load data/
```

This creates a 4.0 GiB Datalevin database in `db/ldbc-snb` with the SNB data.

#### Load Audit

To sanity-check that the loader ingested all entity/edge rows:

```bash
clj -M -m ldbc-snb-bench.audit
```

### 2. Run Benchmark

```bash
# Run all benchmark queries
clj -M -m ldbc-snb-bench.core bench
```

Results are by default written to `results/results.csv` (query outputs) and
`results/perf.csv` (timings).

### 3. Run Tests

To verify query results are correct:

```bash
clj -M:test
```

This runs 21 tests (one per query) with 97 assertions that validate result
counts and specific field values against expected outputs.

## Neo4j Comparison

To compare query results and performance against Neo4j:

```bash
# Install Neo4j (macOS via Homebrew)
./neo4j/install-neo4j.sh

# Import data into Neo4j (use --wipe for clean reimport)
./neo4j/bulk-import-native.sh

# Start Neo4j
neo4j start

# Run Neo4j queries (default password: neo4jtest)
./neo4j/run-queries.sh
```

Compare `results/results.csv` with `neo4j/results/results.csv` to validate outputs.
Use `results/perf.csv` and `neo4j/results/perf.csv` to compare timings.

## Results

We reran the full benchmark on July 15, 2026, on an Apple M3 Pro with 12 cores
and 36 GiB RAM, running macOS 26.5.1 and OpenJDK 21.0.11, with Clojure 1.12.4.

The dataset is LDBC SNB SF1 (scale factor 1), which contains approximately 3.2M
entities and 17.3M edges. Datalevin was run twice in separate JVMs. The first
full pass warmed the operating-system page cache and was discarded; the tables
report the second full pass. A separate JVM prevents the in-process query result
cache from serving the measured pass. Timings start immediately before query
execution and end after post-processing and realization of the result rows;
database loading, migration, and analysis are outside the timing scope. The
Neo4j numbers are retained from the previous benchmark run and are therefore a
reference rather than a same-machine comparison.

### Interactive Short Queries (IS1-IS7)

Run IS queries with:

```bash
clj -M -m ldbc-snb-bench.core bench -o results/is-results.csv -p results/is-perf.csv IS1 IS2 IS3 IS4 IS5 IS6 IS7
```

| Query | Neo4j (ms) | Datalevin (ms) |
|-------|------------|----------------|
| IS1   | 1168.9     | 4.8            |
| IS2   | 1173.2     | 68.7           |
| IS3   | 1166.2     | 12.5           |
| IS4   | 1484.7     | 3.3            |
| IS5   | 1445.9     | 11.0           |
| IS6   | 1494.5     | 2.5            |
| IS7   | 5424.6     | 11.6           |
| **Avg** | **1908.3** | **16.3**     |

Datalevin is significantly faster across all short queries, with roughly two
orders of magnitude difference in the averages shown here.

### Interactive Complex Queries (IC1-IC14)

Run IC queries with:

```bash
clj -M -m ldbc-snb-bench.core bench -o results/ic-results.csv -p results/ic-perf.csv IC1 IC2 IC3 IC4 IC5 IC6 IC7 IC8 IC9 IC10 IC11 IC12 IC13 IC14
```

| Query | Neo4j (ms) | Datalevin (ms) |
|-------|------------|----------------|
| IC1   | 3434.3     | 452.4          |
| IC2   | 1133.4     | 1120.3         |
| IC3   | 1961.7     | 4561.0         |
| IC4   | 1799.9     | 266.6          |
| IC5   | 2509.2     | 9188.1         |
| IC6   | 1561.8     | 8.5            |
| IC7   | 1157.9     | 173.6          |
| IC8   | 1215.9     | 19.6           |
| IC9   | 2052.3     | 33.8           |
| IC10  | 1169.9     | 1226.7         |
| IC11  | 1161.9     | 77.0           |
| IC12  | 4361.2     | 254.7          |
| IC13  | 1150.4     | 1059.2         |
| IC14  | 19354.1    | 3952.0         |
| **Avg** | **3144.6** | **1599.5**   |

Datalevin performs better on 11 of the 14 complex queries. IC6, IC8, IC9, and
IC11 show especially large differences, while Datalevin remains slower on IC3,
IC5, and IC10.

## Remark

Considering Neo4j is on the Graph Data Council as one of the authors of this
benchmark, it is remarkable that Datalevin performs so favorably without any
tuning or customization.

## Extending the Benchmark

### Adding Query Parameters

Edit `sample-params` in `core.clj` to use different parameter values:

```clojure
(def sample-params
  {:ic1 {:person-id 12345
         :first-name "Alice"}
   ...})
```

### Loading Parameters from LDBC Files

LDBC Datagen generates `substitution_parameters/` with parameter files.
These can be loaded to run the official benchmark parameters.


## References

1. [LDBC SNB Specification](https://ldbcouncil.org/ldbc_snb_docs/ldbc-snb-specification.pdf)
2. [LDBC SNB Datagen](https://github.com/ldbc/ldbc_snb_datagen_spark)
