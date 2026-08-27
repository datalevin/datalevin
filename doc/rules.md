# Datalevin Rule Engine

Datalevin has an innovative rule engine that implements an efficient rules
evaluation strategy that leverages the cost based query optimizer.

## Motivation

The power of Datalog compared to a relational query language lies in the
expressiveness in handling logical rules that can be recursively defined.
However, the logic syntax of traditional Datalog may feel alien and hard to
grasp for some developers. Datomic flavor of Datalog is beneficial, not just in
term of its Clojuristic syntax, but more so in its marrying the traditional
SQL-like structure with traditional Datalog. By using the SQL-like syntax as the
basis and treating traditional Datalog syntax as special rule clauses, users are
gradually introduced to this logic view of Datalog, a win in ergonomics.

The previous implementation of rule clause evaluation algorithm inherited from
Datascript uses a top-down evaluation strategy, which can be less efficient than
a bottom-up strategy [4]. For example, it is prone to run out of memory due to
explosion of tuples from recursive rules. More importantly, this top-down method
cannot take advantage of our cost based query optimizer. To address this
deficiency, we developed a new rules evaluation engine using the latest research
advances in bottom-up Datalog evaluation strategy, with some innovation of our
own.

## Rule Evaluation Algorithm

The new rule engine uses a bottom-up Datalog evaluation strategy. It handles
recursive rules more efficiently.

### Semi-naive fix-point evaluation

The rule engine employs the well known semi-naive evaluation (SNE) strategy [1]
[2]. The engine generates tuples from the rule sets until a fix-point is
reached, i.e. when no new tuples are produced. The evaluation is stratified,
where rules run in their strongly connected components (stratum) in the correct
topological sort order.

### Magic set rewrite

We implements the well known magic sets rule rewrite algorithm [1] [2] that add
magic rules to leverage bound variables to avoid generating unnecessary
intermediate results in SNE. The rewrite is enabled only when it is effective.

### Seeding tuples (new)

Compared with a standalone SNE engine, Datalevin rule engine is part of the
query engine, so it does not work off a blank slate, but base the work on a warm
start of a set of already produced tuples from outer query clauses. These
seeding tuples are often produced more efficiently than SNE, as they benefit
from indices and the cost based query optimizer. These seeds effectively act as
filters to prevent the generation of unnecessary tuples during SNE.

### Selectively inline non-recursive rule clauses (new)

As an innovation, we identify clauses that are not involved in recursion and,
when safe, pull them into the regular query clauses so the cost-based query
optimizer can work on them. SNE therefore only evaluates rules involved in
recursion, while ordinary index-based joins handle simple non-recursive rules.

Inlining is deliberately selective. A Datalog predicate is a set, so its rule
boundary removes duplicate proofs of the same head tuple. Inlining through a
rule that projects away body-local variables, or that has multiple branches,
can lose that boundary and retain every proof in downstream joins. The number
of proofs can be orders of magnitude larger than the number of predicate
tuples. The rewriter consequently keeps these set-valued boundaries in nested
or derived-rule DAGs, while continuing to inline single-branch rules whose body
does not project away variables.

### Set-valued rule boundaries and fused distinct joins

Every retained non-recursive rule branch is projected to its head variables and
deduplicated before it is combined with other branches. Branch unions are also
deduplicated. This applies Datalog set semantics at the earliest useful
predicate boundary instead of carrying duplicate derivations through the rest
of the rule DAG.

When a rule branch ends in another rule call, the engine uses a fused hash join,
head projection, and distinct sink. It emits only previously unseen head tuples
without first materializing the complete proof relation. The same sink can be
pushed into a terminal EAV lookup, so duplicate scan and join results are
discarded as they are produced. For relation-composition shapes whose projected
variables are split across the two inputs, the operator tracks the projected
value domain for each output group and stops probing a group once that domain
is complete. Dense binary compositions receive a bounded reuse sample followed
by exact domain and proof-pair costing. If the projected domains are compact,
the proof-pair fanout is high enough, and a bounded memory estimate passes,
join-key adjacency and per-group results are represented as bitsets. The join
then unions complete value sets instead of performing one hash-set insertion per
proof pair; sparse or wide-domain cases retain the ordinary fused hash join.

The same physical composition is selected directly at a terminal two-clause
EAV boundary when the join key is hidden by the final distinct projection. The
compiler chooses between a full sequential AVE scan and scans of only the
distinct bound join keys, then feeds the resulting binary relation to the
composition operator instead of constructing the complete three-column proof
relation. Small compact projected domains use dense machine-word bitmaps;
larger domains switch to Roaring bitmaps so sparse value sets remain compact.

Memory use is therefore governed by the distinct predicate relations, join hash
tables, and final result rather than by the potentially much larger number of
proof paths. The optimization does not imply constant memory: a genuinely large
distinct result must still be represented.

### Eager materialization of proven-unique results

Some terminal rule operators produce a relation that is already exactly
projected to the query's find variables and proven unique. Re-inserting every
tuple into a general-purpose set would repeat duplicate detection after that
proof. This is especially expensive for large binary results because it adds a
hash-table probe and possible vector equality checks for every public result
row.

For one proven-unique terminal batch, the collector adopts each immutable tuple
array as a persistent vector and builds a Clojure-`hasheq`-based open-addressed
membership index without comparing tuples for duplicates again. Both the
vectors and the entire index are built before the query returns, so this remains
full result materialization rather than a lazy result or benchmark-only counting
path. The returned value retains normal Clojure set equality, lookup, sequence,
metadata, and serialization behavior; operations that change it fall back to a
regular persistent set.

The shortcut is disabled as soon as ordinary sink input is mixed with a direct
batch or more than one batch is received, because independently distinct
batches need not be disjoint. Results above two million rows, results whose
index estimate exceeds available heap headroom, and paths without an exact
uniqueness proof use the existing spillable set collector.

In the 2026-08-26 OpenRuleBench representative protocol (one complete warmup
pass followed by one complete measurement pass), Join1 `b2` free/free produced
917,680 verified rows in 124.85 ms, down from the previously retained 211.71 ms.
In the same fresh seven-system comparison, Soufflé took 174.95 ms, so
Datalevin's latency is 28.6% lower while retaining the same
query-and-full-materialization timing scope. The [comparison artifact](../benchmarks/openrulebench/results/2026-08-26-representative.edn)
records the protocol run and XSB launcher repair provenance.

Bound rule calls benefit as well. If a lookup proves and elides a bound head
variable, the evaluator safely reattaches a singleton seed before projecting
the rule result. When several rule clauses are otherwise ready, the late-clause
scheduler starts with the rule call having the most bound arguments, preserving
source order for ties. This lets bound-first and bound-last forms enter the same
rule DAG from its more selective side.

### Temporal elimination

For certain applicable recursive rules that meet the criteria of
T-stratification [3], we implements temporal elimination, an optimization that
saves only the results of the last iteration of recursion, so that the recursive
process can be optimized to avoid storing intermediate results.

### Linear recursive EAV fast path

For linear recursive EAV branches, the rule engine streams recursive candidates
directly into the seen set, caches static EAV adjacency maps for the recursive
stratum, precompiles output and lookup metadata into primitive arrays, and
reduces temporary tuple allocation in the seen-set path. This avoids
materialization of large duplicate join products and repeated store lookups in
TC/SG-style rules. Seen sets also use a mixed per-element tuple hash so dense
numeric tuples do not collapse into heavily contended hash buckets. The linear
fast path fills each output array and computes that hash in one pass, then
reuses the hash for both the seen-set probe and the stored tuple wrapper.

When a linear branch introduces one EAV value, the engine indexes seen values
by the remaining, invariant head values. Duplicate candidates then require only
a scalar set probe instead of construction and hashing of the complete output
tuple. Each group also tracks the exact value domain from the base relation and
cached EAV adjacency. Once that domain is exhausted, later deltas for the group
are skipped. Rule calls that only rename a complete result relation reuse its
tuple arrays rather than materializing an identical projection.

### Indexed bound transitive closure

A binary transitive-closure rule with one singleton-bound endpoint does not
need to materialize the complete recursive relation. The rule engine recognizes
the canonical two-branch form: a base branch containing one ref-valued EAV
clause, plus a left- or right-linear recursive branch that composes the same EAV
relation with the predicate. Both physical EAV orientations are supported.

For this shape, evaluation performs a work-queue traversal from the bound
endpoint. Each expansion is an indexed EA or AV lookup, depending on the
direction, and a seen set provides Datalog set semantics and cycle termination.
The start value is returned only when a non-empty cycle reaches it, matching
ordinary transitive rather than reflexive-transitive closure. Literal bounds
and query variables whose current domain contains exactly one value are
eligible. Committed databases use compact tuple probes; a `db-with` value with
a pending transaction overlay uses transaction-aware datom probes. Other rule
shapes, multi-valued bounds, non-ref attributes, and calls against an already
materialized rule relation continue through the general semi-naive evaluator.

The committed-database traversal is adaptive for dense reachable components.
It starts with indexed probes and measures their fanout. When the known pending
frontier predicts work comparable to scanning the attribute, the evaluator
performs one full attribute scan, builds a primitive long-to-long adjacency
map, and finishes the existing work queue from that map. A minimum observation
and frontier size keeps sparse or short traversals on point probes; pending
transaction overlays always remain on the transaction-aware indexed path.

#### Exact output-domain saturation

Forward traversal on a committed database has an additional exact stopping
condition. For a rule whose recursive edge is `[?from :edge ?to]`, every answer
for the free `?to` argument must be one of the distinct stored values of
`:edge`. Datalevin obtains that value-domain size from the AVE index. The work
queue already emits each reachable value at most once, so when the number of
emitted answers equals the complete value-domain size, the two sets must be
equal. Expanding the remaining queue cannot add another answer and evaluation
stops immediately.

The test uses the number of emitted answers, not the traversal's seen set. The
bound start node is placed in the seen set to terminate cycles, but ordinary
transitive closure may return it only after a non-empty path reaches it again.
Consequently the saturation check preserves the distinction between transitive
and reflexive-transitive closure.

The optimization is deliberately limited to committed, forward EAV traversal:

- In reverse traversal the answers are entities, while AVE cardinality counts
  distinct attribute values, so it is not an answer-domain bound.
- A pending `db-with` overlay may contain values not reflected in the stored
  AVE cardinality, so transaction-aware traversal runs to queue exhaustion.
- Other recursive shapes and non-ref attributes continue through their existing
  specialized or general evaluators.

This is an exact completion proof rather than a selectivity estimate. It is
most effective for a dense strongly connected component that reaches the whole
attribute value domain; sparse traversals normally finish before saturation and
retain their indexed-probe behavior.

#### Singleton-bound result elision

The bound endpoint is already represented by a literal or by a singleton input
relation. The specialized traversal therefore materializes only the newly
reached free value instead of repeating the bound value in every two-column
tuple. It marks that one-column relation as unique, allowing terminal result
collection to consume it directly. If the query projects the bound variable as
well, its existing singleton input relation still supplies the value. A
symbol-disjoint input relation may be skipped during set-valued projection only
after it is known to be nonempty, since an empty input must still make the
result empty.

### Demand-driven synchronized closure

A singleton-bound binary predicate can also avoid whole-relation evaluation for
the synchronized recursive form

```clojure
[(p ?x ?y) [?x :base ?y]]
[(p ?x ?y) [?x :left ?z] (p ?z ?z1) [?y :right ?z1]]
```

The physical EAV direction of each clause may be reversed. The evaluator first
discovers only the recursive bound values reachable from the demand. It then
seeds those subproblems from the base relation and propagates each new result
once through a delta work queue to its dependent callers. Per-demand result
sets provide Datalog set semantics and terminate cyclic inputs without
materializing the complete binary predicate. Both bound-first and bound-last
calls use the same mechanism by exchanging the demand and output sides.

This specialization requires exactly one singleton-bound argument, two distinct
binary head variables, one ref-valued EAV base branch, and one recursive branch
with two ref-valued EAV links that preserve argument position. Other shapes and
multi-valued demands retain the general semi-naive rule evaluator.

### Dense full synchronized closure

The same synchronized recursive form has a separate free/free execution path
when the entity domain is compact. The evaluator assigns the ref-valued EAV
domain dense ordinals and stores each binary-relation row as a `BitSet`. A
semi-naive round first unions the right-link rows reached by every new recursive
row, then propagates that complete row through the left-link callers. New rows
are obtained with `andNot` against the accumulated result. Thus duplicate proof
paths are collapsed by machine-word operations instead of allocating, hashing,
and comparing one tuple per proof.

Ordinary result tuples are allocated only after the fixed point is complete.
The compiler uses this path only for the exact two-branch, two-argument
synchronized EAV shape described above, with both call arguments free and all
three attributes ref-valued. A conservative 64 MiB upper bound covers the
result, current and next delta, and both link matrices. Wider domains fall back
to the general semi-naive evaluator. Pending transaction overlays are safe:
the dense domain and matrices are built from transaction-aware EAV tuple scans.

## Benchmarks

### Math Genealogy Benchmark

A benchmark comparing this rule engine with that of Datomic and Datascript can
be found [here](../benchmarks/math-bench). The short summary is that this rule
engine is significantly faster. For recursive rules in particular, the speedup
can be several orders of magnitude.

### LDBC SNB Benchmark

This industry standard benchmark for graph databases also contains some queries
that leverage rules. Datalevin is compared favorably with neo4j
[here](../benchmarks/LDBC-SNB-bench).

### OpenRuleBench

The portable [OpenRuleBench suite](../benchmarks/openrulebench) includes
recursive rule task TC and SG, as well as Join1, a non-recursive rule DAG
designed to expose intermediate-result growth. Its published timing boundary
includes construction of the complete public result set, including the eager
membership index described above.

## References

[1] T. J. Green, S. Huang, B. T. Loo, W. Zhou. Datalog and Recursive Query
Processing. Foundations and Trends in Databases, vol. 5, no. 2, pp. 105–195, 2012.

[2] Maier, David, et al. "Datalog: concepts, history, and outlook." in
Declarative Logic Programming: Theory, Systems, and Applications. 2018. 3-100.

[3] Shaikhha, Amir, et al. "Optimizing Nested Recursive Queries." Proceedings of
ACM SIGMOD, 2(1). 2024: 1-27.

[4] Ullman, J. D. "Bottom-up beats top-down for datalog." Proceedings of the
eighth ACM SIGACT-SIGMOD-SIGART symposium on Principles of Database
Systems. 1989.
