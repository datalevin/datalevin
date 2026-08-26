# Portable OpenRuleBench tasks

This directory contains subset of benchmark tasks derived from
[OpenRuleBench](https://www3.cs.stonybrook.edu/~kifer/TechReports/OpenRuleBench09.pdf),
testing rule resolution performance.

## Task set

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

The rules, in Prolog syntax, are the following:

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

Join1 `a` is intentionally much harder than `b1` and `b2`: it joins two large
derived relations. A timeout or OOM at the published scales is a benchmark
result. Join1 is therefore outside the default group but remains in `joins`,
`stress`, and `all`.

The non-published `tiny` profile exists only for development, smoke test and
differential checks.

### Excluded workloads

The comparison suite deliberately includes only tasks for which we can state the
same logical semantics for every participating backend. It is not a reproduction
of the 2009 result tables. The original anonymous CVS repository is no longer
available, so the generated relation files are not claimed to be byte-for-byte
copies of the historical inputs. The rules, fact counts, cyclic/acyclic
variants, and query-binding matrix follow the paper; the generators in this
directory are fixed, deterministic benchmark inputs. Results from this harness
are comparable with one another, not directly with the paper's reported times.

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

## Systems and timing classes

The following systems are tested:

| System | TC/SG FF | TC/SG bound | Join1 | Timing class |
|---|---:|---:|---:|---|
| [Datalevin](https://datalevin.org/) | yes | yes | yes | query + full result materialization |
| [SQLite](https://www.sqlite.org/lang_with.html) | yes | yes | yes | recursive/nonrecursive SQL + JDBC row materialization |
| [PostgreSQL](https://www.postgresql.org/docs/18/queries-with.html) | yes | yes | yes | recursive/nonrecursive SQL + JDBC row materialization |
| [XSB](https://doi.org/10.1017/S1471068411000500) | yes | yes | yes | tabled evaluation + answer-list materialization |
| [Souffle](https://souffle-lang.github.io/cav-paper) | yes | yes | yes | compiled evaluation + in-memory result relation |
| [Clara Rules](https://www.clara-rules.org/) | yes | no | no | forward-chain + query materialization |
| [O'Doyle Rules](https://github.com/oakes/odoyle-rules) | yes | no | no | forward-chain + query materialization |

### System references

1. [Datalevin documentation](https://datalevin.org/).
2. [SQLite `WITH` clause documentation](https://www.sqlite.org/lang_with.html),
   including recursive common table expressions.
3. [PostgreSQL 18 `WITH` queries documentation](https://www.postgresql.org/docs/18/queries-with.html),
   including recursive query evaluation.
4. T. Swift and D. S. Warren. 2012. ["XSB: Extending Prolog with Tabled Logic
   Programming"](https://doi.org/10.1017/S1471068411000500). *Theory and
   Practice of Logic Programming* 12(1–2): 157–187.
5. H. Jordan, B. Scholz, and P. Subotić. 2016. ["Soufflé: On Synthesis of
   Program Analyzers"](https://souffle-lang.github.io/cav-paper). In *Computer
   Aided Verification (CAV 2016)*.
6. [Clara Rules documentation](https://www.clara-rules.org/) and
   [source repository](https://github.com/oracle-samples/clara-rules).
7. [O'Doyle Rules documentation and source](https://github.com/oakes/odoyle-rules).

### Timing

Timing for every backend excludes data generation, database/session
construction, fact loading, index construction, and statistics collection from
the timed region. Each timed region evaluates the rules/query and fully
materializes its answer.

SQLite and PostgreSQL use the same end-to-end JDBC consumption contract. Their
timers include statement creation, query execution, reading every projected
column through JDBC, and constructing the complete in-memory result. The
PostgreSQL values therefore include localhost client/server communication,
result transfer, and JDBC decoding; no estimated loopback cost is subtracted.
The SQLite values include its embedded JDBC/native boundary and the same column
decoding and row materialization.

XSB and Souffle generate Datalog query programs and compile them into native
code. XSB consults its generated program and facts before reading XSB's internal
wall clock. The interval ends after `findall` has materialized every projected
answer; list counting and process startup/shutdown are outside it. In each pass
process, Souffle generates and compiles C++ once per distinct query program.
Each task pass creates a fresh embedded program instance and calls `loadAll()`
before the clock, then times `runAll()` with I/O disabled through completion of
its in-memory `result` relation. Program compilation, process startup, input
loading, and CSV output are therefore excluded. Bound Souffle programs enable
its magic-set transform for `result`. All five portable backends consequently
report `:timing-scope :query-and-materialization` and may be compared under this
boundary.

Clara and O'Doyle are materialize-all production-rule systems. Bound queries
would execute the same full closure and filter it afterward, so the runner marks
those task/system pairs `N/A` instead of implying bound-query optimization.

## Correctness and measurement contract

- The default publication protocol follows JOB-bench: one complete warmup pass
  over the selected tasks, followed by one complete measurement pass. The
  orchestrator runs both passes in the same child JVM so the warmup applies to
  the retained measurement.
- Every task pass gets newly generated and newly loaded base relations.
  Generation is deterministic.
- Base relations and derived answers have set semantics in every backend. SQL
  plans use duplicate elimination at relation boundaries.
- Each displayed value is the single retained measurement, not a median or
  average of repeated executions. The EDN artifact records it as `:time-ms`, as
  the sole value in `:samples-ms`, and with `:reported-statistic
  :single-measurement`.
- Every measured answer count must equal an independent reference: BitSet
  transitive closure for TC, a delta work queue for SG, and BitSet relation
  composition for Join1.
- Every measured task records a SHA-256 digest of its named base relations.
  Digests must agree across systems before the combined report is accepted.
- Incorrect counts, errors, timeouts, out-of-memory results, missing child
  reports, and failed child processes make the top-level command exit nonzero.
- `--no-verify` is for diagnosis only and is unsuitable for published results.
- `--iterations` values greater than `1` retain the old median-producing mode
  for diagnosis only. They can measure cache effects and are unsuitable for
  published results.
- Child processes isolate JVM state and heap behavior between systems. Artifacts
  record the parsed task, fact count, timing class, Clojure/JVM/OS/CPU metadata,
  engine version where discoverable, and the raw measurement.
- O'Doyle runs each task in a separate child JVM because its rule-firing loop
  does not honor thread cancellation. A timed-out worker therefore exits with
  its task process and cannot overlap the next measurement.

The orchestrator requests an 8 GiB maximum heap for every Clojure wrapper.
Every child records its effective maximum heap. PostgreSQL-server, XSB, and
Souffle resource limits are external and are reported separately.

Setup is excluded because the benchmark is about inference/query execution.

## Representative results (2026-08-26)

This published-scale matrix covers all three portable families without the
explicitly designated Join1 `a` free/free stress cell. It contains the four
canonical free/free recursive tasks, cyclic TC bound-first, both bound
orientations of acyclic SG, free/free Join1 `b1` and `b2`, and the top-level
Join1 `a` bound-first query. It is not a substitute for the complete 42-task
matrix.

The run used a 12-core Apple M3 Pro MacBook Pro (6 performance and 6 efficiency
cores), 36 GiB RAM, macOS 26.6.2, OpenJDK 21.0.11, Clojure 1.12.5, and an 8 GiB
heap limit for each Clojure wrapper process. Engine versions were Datalevin
1.0.2, SQLite 3.51.1, PostgreSQL 18.4 (Homebrew), XSB 5.0.0, Souffle 2.5, Clara
Rules 0.24.0, and O'Doyle Rules 1.3.1 (configured dependency).

All 53 completed measurements passed their independent count oracle, and the
cross-system input-digest check passed.

The harness ran one complete warmup pass and then one complete measurement pass
in the same child JVM. Each displayed number is that single measured latency in
milliseconds.

Clara completed three of its four supported tasks; cyclic TC exhausted its 8 GiB
wrapper heap during warmup. O'Doyle's four supported tasks exceeded the
60-second rule-firing timeout during warmup. A warmup failure has no retained
measurement, so those cells report `OOM` or `T/O`; unsupported system/task pairs
report `N/A`. The command exits nonzero when any selected task fails, while
preserving the diagnostic artifact. The [raw EDN
artifact](results/2026-08-26-representative.edn) contains every result and the
full environment metadata.

### Query evaluation and result materialization

Data generation, loading, index construction, and statistics collection are
outside these timed regions. XSB program consultation and Souffle C++
generation/compilation are outside as described above. Among completed results,
Datalevin has the lowest latency in nine cells and Souffle in one. Datalevin
leads all three selected TC comparisons, including cyclic TC bound-first at
4.76 ms versus 6.62 ms for the fastest alternative, PostgreSQL. Across the six
selected TC and Join1 comparisons, Datalevin leads five; the remaining Join1
`b2` free/free cell is 211.71 ms for Datalevin versus 168.56 ms for Souffle.

| Task | Result rows | Datalevin (ms) | SQLite (ms) | PostgreSQL (ms) | XSB (ms) | Souffle (ms) | Clara Rules (ms) | O'Doyle Rules (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| TC 50K cyclic FF | 1,000,000 | 501.13 | 41,827.70 | 6,423.95 | 3,634.00 | 1,035.65 | OOM | T/O |
| TC 50K acyclic FF | 473,807 | 64.65 | 8,899.15 | 1,703.13 | 950.00 | 373.22 | 50,461.65 | T/O |
| TC 50K cyclic BF | 1,000 | 4.76 | 7.28 | 6.62 | 1,805.00 | 1,008.25 | N/A | N/A |
| SG 6K cyclic FF | 869,923 | 347.51 | 4,678.69 | 1,807.57 | 122,323.00 | 811.48 | 41,489.81 | T/O |
| SG 6K acyclic FF | 215,263 | 33.82 | 471.29 | 184.53 | 14,559.00 | 88.44 | 3,132.56 | T/O |
| SG 6K acyclic BF | 533 | 6.03 | 23.75 | 7.31 | 264.00 | 83.83 | N/A | N/A |
| SG 6K acyclic FB | 541 | 4.63 | 21.23 | 6.73 | 5,627.00 | 117.15 | N/A | N/A |
| Join1 50K b1 FF | 1,000,000 | 183.21 | 20,288.17 | 9,449.25 | 1,856.00 | 2,350.56 | N/A | N/A |
| Join1 50K b2 FF | 917,680 | 211.71 | 663.79 | 788.52 | 173.00 | 168.56 | N/A | N/A |
| Join1 50K a BF | 1,000 | 12.36 | 875.29 | 399.79 | 125.00 | 36,869.99 | N/A | N/A |

## Running the suite

Requirements are Java 17+ and Clojure 1.12.5. SQLite is brought in through
JDBC. PostgreSQL, XSB, and Souffle are external installations. The
Souffle timing harness also requires `souffle-compile.py`, a C++17 compiler,
and the installed Souffle headers; set `SOUFFLE_INCLUDE_DIR` if the headers are
outside the installation prefix or standard include locations.

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
`-Dpg.pass=...` as JVM properties when needed. Each task pass uses
connection-local temporary tables, so it does not replace application tables.

## Publishing results

A defensible result report should include:

1. the raw EDN artifact produced without `--no-verify`;
2. the exact task list and timing class;
3. engine, JVM, OS, CPU, memory limit, and external-server configuration;
4. the single retained measurement and its raw value, not a median or average
   of repeated executions;
5. explicit `N/A`, timeout, and OOM cells; and
6. a statement that these deterministic inputs are OpenRuleBench-derived and
   are not the lost historical relation files.

Do not combine results with different `:timing-scope` values into a single
speedup. In particular, do not substitute an external process wall time for the
internal query-and-materialization interval. Do not compare `tiny` results with
published scales or silently drop failed or unsupported cells.
