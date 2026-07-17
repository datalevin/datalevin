# OpenRuleBench for Datalevin (WIP)

Benchmarks for comparing Datalevin's Datalog engine with rule engines,
recursive SQL systems, and deductive systems. The suite follows the
[OpenRuleBench](https://www3.cs.stonybrook.edu/~kifer/TechReports/OpenRuleBench09.pdf)
workloads where practical, and also includes a non-standard `tiny` TC/SG scale
for local development runs when the standard `small` case is too large.

## Benchmarks

| Benchmark | Type | Description | Sizes |
|-----------|------|-------------|-------|
| **TC** | Synthetic | Transitive closure on random graphs | 1K, 50K, 125K, 250K, 500K, 1M edges |
| **SG** | Synthetic | Same generation over random `par`/`sib` relations | 1K, 6K, 24K, 48K facts |
| **Join1** | Synthetic | 5-way join with intermediate results | 10K, 50K, 250K tuples |
| **DBLP** | Real-world | Publication data (4-way self-join) | 2K, 8K, 64K papers |
| **LUBM** | Semantic | University domain (type inference) | 1, 10, 50 universities |

## Quick Start

```bash
cd benchmarks/openrulebench

# Run the configured default benchmarks
./bench.clj

# Run a tiny local comparison between Datalevin and Clara
./bench.clj --systems datalevin,clara tc:tiny sg:tiny

# Run selected OpenRuleBench standard cases
./bench.clj --systems datalevin,clara tc:small sg:small

# Run all OpenRuleBench benchmarks
./bench.clj all

# Run stress tests
./bench.clj stress
```

The Datalevin runner uses the in-memory KV store for generated benchmark
databases. Datalevin, Clara, and O'Doyle are run with `-J-Xmx8g`.

### Timing Method

The in-process TC and SG runners use the same timing boundary. Data generation,
engine or session creation, base-fact loading, index creation, and statistics
collection are setup and are not timed. After setup and a full GC, the timed
region performs recursive rule evaluation and fully materializes the result:
`d/q` for Datalevin, `fire-rules` plus `query` for Clara, `fire-session` plus
`query-all` for O'Doyle, and the recursive query plus row materialization for
SQLite and PostgreSQL. Timeout bookkeeping is outside O'Doyle's reported time.
XSB and Souffle use external process runners and retain their documented
process-level timings.

## Current Results

Environment for these runs: Clojure `1.12.5`, Clara Rules `0.24.0`,
O'Doyle Rules `1.3.1`, OpenJDK `21.0.11`, macOS arm64, `-J-Xmx8g`.

### Tiny TC/SG

These are non-standard local development instances. `tc:tiny` uses 100 nodes
and 1,000 edges; `sg:tiny` uses 100 nodes, 500 `par` facts, and 500 `sib` facts.
These results use the common timing boundary above. Datalevin and Clara
produced the same result counts.

| Benchmark | Datalevin | Clara | O'Doyle | Result count |
|-----------|-----------|-------|---------|--------------|
| `tc:tiny` | 57.46 ms | 352.2 ms | T/O at 60s | 10,000 for Datalevin/Clara |
| `sg:tiny` | 27.37 ms | 404.5 ms | T/O at 60s | 10,000 for Datalevin/Clara |

The Datalevin values are one-shot fresh-process runs. Repeated runs would
produce far shorter times.

### Small Runs

`tc:small` is the OpenRuleBench TC small graph: 1,000 nodes and 50,000 edges.
`sg:small` is the OpenRuleBench SG small shape: 1,000 nodes, 3,000 `par` facts,
and 3,000 `sib` facts. These results use the common timing boundary above.

| Benchmark | Datalevin | Clara | O'Doyle | Result count |
|-----------|-----------|-------|---------|--------------|
| `tc:small` | 1,048.8 ms | OOM after about 6m45s | setup >5m; stopped | 1,000,000 for Datalevin |
| `sg:small` | 1,470.0 ms | 43,757.1 ms | T/O at 60s | 869,923 for Datalevin/Clara |

The Datalevin values are medians of five runs. The observed ranges were
897.9-1,145.4 ms for `tc:small` and 1,311.5-1,560.2 ms for `sg:small`.

Correctness was checked for both Datalevin and Clara TC/SG rules against
independent fixed-point references. The corrected SG reference uses the
OpenRuleBench rule shape: `sg(X,Y) :- sib(X,Y)` and
`sg(X,Y) :- par(X,Z), sg(Z,Z1), par(Y,Z1)`. Clara's current benchmark
implementation uses private process-wide seen sets, so it is intended for the
sequential benchmark harness.

O'Doyle requires tuple ids such as `[::sg x y]` to represent many-valued binary
relations; a simpler `[x ::sg y]` encoding overwrites values for the same `x`.
With the corrected tuple representation, a small custom SG sanity case passes.
Under the common timing boundary, both O'Doyle tiny cases still time out during
rule evaluation. O'Doyle `sg:small` also times out during rule evaluation;
`tc:small` did not finish the excluded session-construction phase within five
minutes, so no timed result is reported.

## ORE 2015 OWL-RL Benchmark (Realistic Reasoning)

This benchmark uses a stratified 50-ontology subset of the ORE 2015 corpus
and runs OWL-RL materialization plus a fixed set of generic queries.
The subset is selected by axiom count and cached in a manifest file for
reproducibility.

Download ORE 2015:
```bash
./scripts/download-ore2015.sh
```

Run the OWL-RL benchmark:
```bash
clojure -M:ore-rl -m openrulebench.ore-rl
```

Optional flags:
```bash
clojure -M:ore-rl -m openrulebench.ore-rl --limit 10 --refresh --out out/ore-rl-small.csv
```

Notes:
- The OWL-RL rule set implemented is an OWL-RL core (RDFS + property/class
  hierarchy, sameAs, inverse, symmetric, transitive, functional).
- Materialization is performed by enumerating all inferred triples and
  storing them in a new Datalevin database; queries run on that closure.

## Benchmark Details

### Transitive Closure (TC)

Compute reachability in a directed graph:
```
tc(A, B) :- edge(A, B).
tc(A, B) :- edge(A, X), tc(X, B).
```

| Instance | Nodes | Edges |
|----------|-------|-------|
| tiny | 100 | 1,000 |
| small | 1,000 | 50,000 |
| medium | 1,000 | 125,000 |
| large | 2,000 | 250,000 |
| xlarge | 2,000 | 500,000 |
| xxlarge | 2,000 | 1,000,000 |

`tiny` is a non-standard development scale for local comparisons when the
OpenRuleBench `small` instance is too large for a system or heap size.

### Same Generation (SG)

Find same-generation pairs from base `sib` and `par` relations:
```
sg(X, Y) :- sib(X, Y).
sg(X, Y) :- par(X, Z), sg(Z, Z1), par(Y, Z1).
```

| Instance | Nodes | `par` facts | `sib` facts | Total facts |
|----------|-------|-------------|-------------|-------------|
| tiny | 100 | 500 | 500 | 1,000 |
| small | 1,000 | 3,000 | 3,000 | 6,000 |
| medium | 1,000 | 12,000 | 12,000 | 24,000 |
| large | 1,000 | 24,000 | 24,000 | 48,000 |

`tiny` and `large` are local development extensions; the OpenRuleBench paper
reports SG sizes of 6,000 and 24,000 base facts.

### Join1 (5-Way Join)

Tests intermediate result elimination with cascading joins:
```
c1(X, Y) :- d1(X, Z), d2(Z, Y).
b2(X, Y) :- c3(X, Z), c4(Z, Y).
b1(X, Y) :- c1(X, Z), c2(Z, Y).
a(X, Y)  :- b1(X, Z), b2(Z, Y).
```

| Instance | Tuples per relation |
|----------|---------------------|
| small | 10,000 |
| medium | 50,000 |
| large | 250,000 |

### DBLP (Real-World Data)

4-way self-join query on publication metadata in EAV format:
```clojure
[:find ?id ?title ?year ?author ?month
 :where
 [?e1 :db/id ?id] [?e1 :att/attribute :title] [?e1 :att/value ?title]
 [?e2 :db/id ?id] [?e2 :att/attribute :year] [?e2 :att/value ?year]
 [?e3 :db/id ?id] [?e3 :att/attribute :author] [?e3 :att/value ?author]
 [?e4 :db/id ?id] [?e4 :att/attribute :month] [?e4 :att/value ?month]]
```

| Instance | Papers |
|----------|--------|
| small | 2,000 |
| medium | 8,000 |
| large | 64,000 |

To use real DBLP data (optional):
```bash
./scripts/download-dblp.sh
```

### LUBM (Semantic Web)

Tests type inference with university domain ontology:
```
;; Type hierarchy inference
[(is-a ?x :Student) [?x :type :GraduateStudent]]
[(is-a ?x :Person) (is-a ?x :Student)]
```

| Instance | Universities | Triples |
|----------|--------------|---------|
| lubm-1 | 1 | ~100K |
| lubm-10 | 10 | ~1M |
| lubm-50 | 50 | ~5M |

## Systems Compared

| System | Category | Description |
|--------|----------|-------------|
| **Datalevin** | Deductive (Datalog) | Bottom-up with tabling |
| **Clara Rules** | Production rule engine | Rete-style forward chaining |
| **O'Doyle Rules** | Production rule engine | EAV-style forward chaining |
| **SQLite** | SQL | Recursive CTE baseline |
| **PostgreSQL** | SQL | Recursive CTE (optional) |
| **XSB** | Deductive (Tabled Prolog) | Reference implementation |
| **Soufflé** | Compiled Datalog | Reference (compiles to C++) |

## Requirements

### Required
- Clojure 1.12+; benchmark aliases currently pin Clojure `1.12.5`
- Java 17+

### Optional External Systems

**SQLite** (usually pre-installed):
```bash
sqlite3 --version
```

**PostgreSQL**:
```bash
brew install postgresql  # macOS
# or
apt install postgresql   # Linux
```

**XSB Prolog**:
```bash
brew install xsb  # macOS
# or set XSB_PATH=/path/to/xsb
```

**Soufflé**:
```bash
brew install souffle  # macOS
```

## File Structure

```
openrulebench/
├── bench.clj                 # Main benchmark runner
├── README.md                 # This file
├── src/openrulebench/
│   ├── core.clj              # Timing utilities
│   ├── data.clj              # Data generation
│   ├── datalevin.clj         # Datalevin benchmarks
│   ├── dblp.clj              # DBLP data loader
│   ├── lubm.clj              # LUBM data generator
│   ├── sqlite.clj            # SQLite benchmarks
│   └── ...                   # Other systems
├── scripts/
│   ├── download-dblp.sh      # Download real DBLP data
│   └── generate-lubm.sh      # Generate LUBM data
├── external/
│   ├── xsb/                  # XSB Prolog programs
│   ├── sql/                  # SQL scripts
│   └── souffle/              # Soufflé Datalog
└── data/
    ├── dblp/                 # DBLP XML (downloaded)
    └── lubm/                 # LUBM OWL (generated)
```


## References

- [OpenRuleBench Paper](https://www3.cs.stonybrook.edu/~kifer/TechReports/OpenRuleBench09.pdf)
- [LUBM Benchmark](http://swat.cse.lehigh.edu/projects/lubm/)
- [DBLP](https://dblp.uni-trier.de/)
- [XSB Prolog](https://xsb.sourceforge.net/)
- [Soufflé](https://souffle-lang.github.io/)
