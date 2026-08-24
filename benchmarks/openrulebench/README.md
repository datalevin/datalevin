# Portable OpenRuleBench tasks

This directory contains a correctness-gated, cross-system subset derived from
[OpenRuleBench](https://www3.cs.stonybrook.edu/~kifer/TechReports/OpenRuleBench09.pdf).
The comparison suite deliberately includes only tasks for which we can state the
same logical semantics, input relations, query bindings, result contract, and
timing boundary for every participating backend.

It is not a reproduction of the 2009 result tables. The original anonymous CVS
repository is no longer available, so the generated relation files are not
claimed to be byte-for-byte copies of the historical inputs. The rules, fact
counts, cyclic/acyclic variants, and query-binding matrix follow the paper; the
generators in this directory are fixed, deterministic benchmark inputs. Results
from this harness are comparable with one another, not directly with the paper's
reported times.

## Defensible task set

| Family | Published scales | Variants | What it exercises |
|---|---:|---|---|
| TC | 50K, 500K facts | cyclic/acyclic; FF/BF/FB | recursive transitive closure and bound-query propagation |
| SG | 6K, 24K facts | cyclic/acyclic; FF/BF/FB | nonlinear recursive same-generation fixed points |
| Join1 | 50K, 250K facts per base relation | `a`/`b1`/`b2`; FF/BF/FB | nonrecursive trees of joins and intermediate-result control |

`FF` means both query arguments are free, `BF` fixes the first argument to
`1`, and `FB` fixes the second argument to `1`. TC and SG use both a cyclic
random-relation profile and a DAG profile. At each SG scale, half of the facts
are in `par` and half are in `sib`. Join1 has five set-valued base relations
over a 1,000-value domain. Every generator emits exactly the requested number
of unique tuples in a stable order, using fixed seeds.

The rules are:

```prolog
tc(X,Y) :- edge(X,Y).
tc(X,Y) :- edge(X,Z), tc(Z,Y).

sg(X,Y) :- sib(X,Y).
sg(X,Y) :- par(X,Z), sg(Z,Z1), par(Y,Z1).

c1(X,Y) :- d1(X,Z), d2(Z,Y).
b2(X,Y) :- c3(X,Z), c4(Z,Y).
b1(X,Y) :- c1(X,Z), c2(Z,Y).
a(X,Y)  :- b1(X,Z), b2(Z,Y).
```

The non-published `tiny` profile exists only for fast smoke and differential
checks. Never present `tiny` results as OpenRuleBench results.

Join1 `a` is intentionally much harder than `b1` and `b2`: it joins two large
derived relations. A timeout or OOM at the published scales is a benchmark
result, not a reason for the harness to substitute a smaller relation or omit
the cell. Join1 is therefore outside the default group but remains in `joins`,
`stress`, and `all`.

## Excluded workloads

The source tree still contains exploratory DBLP, LUBM, and ORE code, but these
are not accepted by the comparison runner:

- DBLP requires the benchmark's frozen roughly 2.5-million-fact bibliography
  snapshot. Downloading today's DBLP XML and truncating it to invented sizes is
  not a reproducible equivalent.
- The existing LUBM namespace uses a custom generator and a reduced type rule,
  not the official 10- and 50-university data plus OpenRuleBench Query1, Query2,
  and Query9 translations.
- ORE 2015 is a useful separate ontology experiment, but it is not an
  OpenRuleBench task and currently has only a Datalevin implementation.
- Join2, Mondial, WordNet, Wine, and negation tests are omitted until their
  frozen data and portable semantic contracts can be supplied.

This is preferable to reporting Datalevin-only placeholders as cross-system
benchmarks.

## Systems and timing classes

| System | TC/SG FF | TC/SG bound | Join1 | Timing class |
|---|---:|---:|---:|---|
| Datalevin | yes | yes | yes | query + full result materialization |
| SQLite | yes | yes | yes | recursive/nonrecursive SQL + JDBC row materialization |
| PostgreSQL | yes | yes | yes | recursive/nonrecursive SQL + JDBC row materialization |
| XSB | yes | yes | yes | external process: load + evaluate + materialize |
| Souffle | yes | yes | yes | external process: compile/evaluate + output writing |
| Clara Rules | yes | no | no | forward-chain + query materialization |
| O'Doyle Rules | yes | no | no | forward-chain + query materialization |

Datalevin, SQLite, PostgreSQL, Clara, and O'Doyle exclude data generation,
database/session construction, fact loading, index construction, and statistics
collection from the timed region. Each timed region evaluates the rules/query
and fully materializes its answer.

XSB and Souffle cannot use that same embedded boundary in the current runners.
Their results are labeled `:external-process-load-evaluate-materialize` (XSB)
or `:external-process-compile-load-evaluate-materialize` (Souffle) in the EDN
artifact and must not be placed in the same latency ranking as the
embedded/JDBC results. They remain useful as correctness and process-level
scalability references.

Clara and O'Doyle are materialize-all production-rule systems. Bound queries
would execute the same full closure and filter it afterward, so the runner marks
those task/system pairs `N/A` instead of implying bound-query optimization.

## Correctness and measurement contract

- Every warmup and measured sample gets newly generated and newly loaded base
  relations. Generation is deterministic.
- Base relations and derived answers have set semantics in every backend. SQL
  plans use duplicate elimination at Datalog relation boundaries.
- The default is one warmup and five measured iterations. The displayed value
  is the median; the EDN artifact retains every sample and min/mean/p95/p99.
- Every measured answer count must be stable across samples and equal an
  independent reference: BitSet transitive closure for TC, a delta work queue
  for SG, and BitSet relation composition for Join1.
- Every sample records a SHA-256 digest of its named base relations. Digests
  must agree across samples and systems before the combined report is accepted.
- Incorrect counts, errors, timeouts, out-of-memory results, missing child
  reports, and failed child processes make the top-level command exit nonzero.
- `--no-verify` is for diagnosis only and is unsuitable for published results.
- Child processes isolate JVM state and heap behavior between systems. Artifacts
  record the parsed task, fact count, timing class, Clojure/JVM/OS/CPU metadata,
  engine version where discoverable, latency distribution, and raw samples.
- O'Doyle runs each task in a separate child JVM because its rule-firing loop
  does not honor thread cancellation. A timed-out worker therefore exits with
  its task process and cannot overlap the next measurement.

The orchestrator requests an 8 GiB maximum heap for every Clojure wrapper.
Every child records its effective maximum heap. PostgreSQL-server, XSB, and
Souffle resource limits are external and must be reported separately.

Setup is excluded because the benchmark is about inference/query execution.
The separately labeled XSB/Souffle boundary is the only exception.

## Running the suite

Requirements are Java 17+ and Clojure 1.12.5. SQLite is brought in through
JDBC. PostgreSQL, XSB, and Souffle are optional external installations.

```bash
cd benchmarks/openrulebench

# Four published-size FF recursive tasks on the default systems
./bench.clj --output results/default.edn

# Fast non-published smoke check
./bench.clj --systems datalevin,sqlite --warmup 0 --iterations 1 smoke

# All 21 tiny variants; useful after changing a backend implementation
./bench.clj --systems datalevin,sqlite --warmup 0 --iterations 1 \
  --output results/differential.edn differential

# Complete published recursive or Join1 matrices
./bench.clj --systems datalevin,sqlite recursive
./bench.clj --systems datalevin,sqlite joins

# One explicit task
./bench.clj --systems datalevin,sqlite tc:50k-acyclic-bf

# All 42 published tasks supported by each selected system
./bench.clj --systems datalevin,sqlite,postgresql all

# Unit tests for task parsing, generators, references, and failure gates
clojure -M:test
```

Available groups are `default`, `smoke`, `differential`, `recursive`, `joins`,
`stress`, and `all`. Run `./bench.clj --help` for the canonical task syntax and
CLI options. Unsupported pairs are printed as `N/A`; they are distinct from a
failed implementation.

For PostgreSQL, the runner currently connects to
`jdbc:postgresql://localhost:5432/postgres`. Set `-Dpg.user=...` and
`-Dpg.pass=...` as JVM properties when needed. Each sample uses connection-local
temporary tables, so it does not replace application tables.

## Publishing results

A defensible result report should include:

1. the raw EDN artifact produced without `--no-verify`;
2. the exact task list and timing class;
3. engine, JVM, OS, CPU, memory limit, and external-server configuration;
4. median and raw samples, not a single best run;
5. explicit `N/A`, timeout, and OOM cells; and
6. a statement that these deterministic inputs are OpenRuleBench-derived and
   are not the lost historical relation files.

Do not combine external-process and embedded/JDBC numbers into a single
speedup. Do not compare `tiny` results with published scales. Do not silently
drop failed or unsupported cells.

## Layout

```text
openrulebench/
├── bench.clj
├── deps.edn
├── src/openrulebench/
│   ├── core.clj          # task contract, references, statistics, artifacts
│   ├── data.clj          # deterministic set-valued generators
│   ├── runner.clj        # capability-aware process orchestrator
│   ├── datalevin.clj
│   ├── sqlite.clj
│   ├── postgresql.clj
│   ├── xsb.clj
│   ├── souffle.clj
│   ├── clara.clj
│   └── odoyle.clj
└── test/openrulebench/
```

The primary benchmark definition is the
[OpenRuleBench paper](https://www3.cs.stonybrook.edu/~kifer/TechReports/OpenRuleBench09.pdf).
