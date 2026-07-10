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
:forum/containerOf, :message/replyOf, ...
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
- ~10GB disk space for SF1 data

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

This creates a 5.3 GiB Datalevin database in `db/ldbc-snb` with the SNB data.

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

We ran this benchmark on a 2023 Apple M2 Max with 12 cores, 32GB RAM, 1TB SSD,
running macOS 15.2 and OpenJDK 21, with Clojure 1.12.4.

The dataset is LDBC SNB SF1 (scale factor 1), which contains approximately 3.2M
entities and 17.3M edges. For both Datalevin and Neo4j, we ran queries twice,
Warm up in the first, and report results of the second run.

### Interactive Short Queries (IS1-IS7)

Run IS queries with:

```bash
clj -M -m ldbc-snb-bench.core bench -o results/is-results.csv -p results/is-perf.csv IS1 IS2 IS3 IS4 IS5 IS6 IS7
```

| Query | Neo4j (ms) | Datalevin (ms) |
|-------|------------|----------------|
| IS1   | 1168.9     | 3.6           |
| IS2   | 1173.2     | 62.0          |
| IS3   | 1166.2     | 11.4           |
| IS4   | 1484.7     | 0.9            |
| IS5   | 1445.9     | 1.2            |
| IS6   | 1494.5     | 1.9            |
| IS7   | 5424.6     | 6.8           |
| **Avg** | **1908.3** | **12.6**     |

Datalevin is significantly faster across all short queries, and the speedup is
several orders of magnitude.

### Interactive Complex Queries (IC1-IC14)

Run IC queries with:

```bash
clj -M -m ldbc-snb-bench.core bench -o results/ic-results.csv -p results/ic-perf.csv IC1 IC2 IC3 IC4 IC5 IC6 IC7 IC8 IC9 IC10 IC11 IC12 IC13 IC14
```

| Query | Neo4j (ms) | Datalevin (ms) |
|-------|------------|----------------|
| IC1   | 3434.3     | 413.9          |
| IC2   | 1133.4     | 221.9          |
| IC3   | 1961.7     | 4295.6         |
| IC4   | 1799.9     | 287.3          |
| IC5   | 2509.2     | 9140.5         |
| IC6   | 1561.8     | 10.0           |
| IC7   | 1157.9     | 191.3          |
| IC8   | 1215.9     | 18.5           |
| IC9   | 2052.3     | 16955.7        |
| IC10  | 1169.9     | 1294.7         |
| IC11  | 1161.9     | 77.6           |
| IC12  | 4361.2     | 232.6          |
| IC13  | 1150.4     | 1163.2         |
| IC14  | 19354.1    | 3826.7         |
| **Avg** | **3144.6** | **2759.2**   |

Datalevin is about 13% faster overall in these complex graph queries.

Datalevin performs better on the majority of the queries, with some, IC6,
IC8, IC11, are even orders of magnitude better than Neo4j, while lags behind in
IC3, IC5, and IC9.

Datalevin performs the worst in IC9, which "is one of the most complex queries",
according to page 67 of the specification [1]. Neo4j performs surprisingly well
on this one.

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
