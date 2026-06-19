# OpenRuleBench for Datalevin

Benchmarks comparing Datalevin's Datalog engine against other rule engines and deductive systems.

This benchmark suite implements tests from [OpenRuleBench](https://www3.cs.stonybrook.edu/~kifer/TechReports/OpenRuleBench09.pdf), the standard benchmark for comparing rule engines.

## Benchmarks

| Benchmark | Type | Description | Sizes |
|-----------|------|-------------|-------|
| **TC** | Synthetic | Transitive closure on random graphs | 50K, 125K, 250K, 500K, 1M edges |
| **SG** | Synthetic | Same generation on random graphs | Same as TC |
| **Join1** | Synthetic | 5-way join with intermediate results | 10K, 50K, 250K tuples |
| **DBLP** | Real-world | Publication data (4-way self-join) | 2K, 8K, 64K papers |
| **LUBM** | Semantic | University domain (type inference) | 1, 10, 50 universities |

## Quick Start

```bash
cd benchmarks/openrulebench

# Run default benchmarks (quick)
./bench.clj

# Run specific benchmarks
./bench.clj tc:small tc:medium sg:small

# Run all OpenRuleBench benchmarks
./bench.clj all

# Run stress tests (requires >8GB heap)
./bench.clj stress
```

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
| small | 1,000 | 50,000 |
| medium | 1,000 | 125,000 |
| large | 2,000 | 250,000 |
| xlarge | 2,000 | 500,000 |
| xxlarge | 2,000 | 1,000,000 |

### Same Generation (SG)

Find nodes at the same depth in a tree:
```
sg(X, X) :- parent(_, X).
sg(X, Y) :- parent(PX, X), parent(PY, Y), sg(PX, PY).
```

Uses the same instances as TC.

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
| **SQLite** | SQL | Recursive CTE baseline |
| **PostgreSQL** | SQL | Recursive CTE (optional) |
| **XSB** | Deductive (Tabled Prolog) | Reference implementation |
| **Soufflé** | Compiled Datalog | Reference (compiles to C++) |

## Requirements

### Required
- Clojure 1.12+
- Java 11+

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

## Expected Results

Deductive systems (Datalevin, XSB) significantly outperform production rule engines (Clara, Drools) on recursive queries:

| Benchmark | Datalevin | SQLite | XSB |
|-----------|-----------|--------|-----|
| TC small | ~50ms | ~200ms | ~20ms |
| TC medium | ~200ms | ~1s | ~100ms |
| SG small | ~100ms | ~500ms | ~50ms |

**Why?**
- Deductive systems use tabling/memoization to avoid redundant computation
- Production rule engines use forward-chaining without query optimization
- For recursive Datalog, deductive systems are 10-100x faster

## References

- [OpenRuleBench Paper](https://www3.cs.stonybrook.edu/~kifer/TechReports/OpenRuleBench09.pdf)
- [LUBM Benchmark](http://swat.cse.lehigh.edu/projects/lubm/)
- [DBLP](https://dblp.uni-trier.de/)
- [XSB Prolog](https://xsb.sourceforge.net/)
- [Soufflé](https://souffle-lang.github.io/)
