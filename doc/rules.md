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

### Inline non-recursive rule clauses (new)

As an innovation, we identify the clauses that are not involved in recursions,
pull them out and add them to the regular query clauses to allow the cost-based
query optimizer to work on them. That is to say, SNE only works on the rules
that involved in recursions. This increases efficiency as index based joins are
faster than SNE.

### Temporal elimination

For certain applicable recursive rules that meet the criteria of
T-stratification [3], we implements temporal elimination, an optimization that
saves only the results of the last iteration of recursion, so that the recursive
process can be optimized to avoid storing intermediate results.

## Benchmarks

### Math Genealogy Benchmark

A benchmark comparing this rule engine with that of Datomic and Datascript can
be found [here](../benchmarks/math-bench). The short summary is that this rule
engine is significantly faster. For recursive rules in particular, the speedup
can be several orders of magnitude.

### LDBC SNB Benchmark

This industry standard benchmark for graph databases also contains some queries
that leverage rules. Datalevin is compared favorably with neo4j
[here](../benchmarks/LDBC-SNB-bench), particularly those queries that use rules.

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
