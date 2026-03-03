# Datalevin Query Engine

Datalevin has an innovative query engine that handles complex Datalog queries on
large data sets with ease. It is an optimizing compiler that compiles query
logic into an efficient sequence of index walks similar to a hand-written
data access program.

## Motivation

One of the main reasons for people to use Datomic flavored Datalog stores is to
use their declarative and composible query language. In the era of generative
AI, this query language is also found to be an ideal target language for natural
language query translation.

The simple and elegant query language is often backed by a flexible triple
store. However, it is a well-know problem that querying a triple store is much
slower than querying RDBMS that stores data in rows (or columns). [1]

Datalevin solves the problem by developing an advanced query engine based on
the latest research findings and some innovations of our own. We leverage some
unique properties of Datomic-like triple stores to achieve the goal of bringing
triple store query performance to a level competitive with RDBMS.

## Difference from RDF Stores

Although Datomic-like stores are heavily inspired by RDF stores, there are
some differences that impact query engine design. Rather than trying to be
maximally general and open, Datomic-like stores are closer to traditional
databases in its design choices.

RDF stores often have a limited number of properties even for huge datasets,
whereas Datomic-like stores normally have many attributes, and they are
often specialized to a class of entities.  Therefore, filtering by
attribute values can be very efficient.

In Datomic-like stores, entities have explicit integer IDs, which makes the
filtering by entity IDs efficient. The entity vs. entity relationship is also
explicitly marked by `:db.type/ref` value type. The cost of resolving entity
relationship becomes lower.

Conversely, in Datomic-like stores, the data values are stored as they are,
rather than being represented by integer IDs, therefore, pushing selection
predicates down to index scan methods brings more benefits.

Datalevin query engine exploits these design choices to maximize query
performance.

## Nested Triple Storage

Literal representation of triples in an index introduces significant redundant
storage. For example, in `:eav` index, there are many repeated values of `e`,
and in `:ave` index, there are many repeated values of `a` and `v`. These
repetitions of head elements increase not just the storage size, but also
processing overhead during query.

Taking advantage of LMDB's DUPSORT capability (i.e. a key can be mapped to a
list of values, and this list of values are also sorted, essentially it is a two
level nested B+ trees of B+ trees), we store the head elements only once, by
treating them as keys. The values are the remaining two elements of the
triple as a list of values mapped to by a key. This nested triple storage
results in about 20% space reduction. In addition, the [underlying KV
storage](https://github.com/huahaiy/dlmdb) implements page based prefix
compression to achieve an additional 10% space reduction.

The main advantage of this list based triple storage is to facilitate counting
of elements, which is the most critical input for query planning. Some list
counts can be immediately read from the index in O(1), without performing actual
range scan to count them. For example, in our storage schema, the number of
datoms matching `[?e :an-attr "bound value"]` pattern can be obtained from the
`:ave` index in constant time, without maintaining specialized statistics
collecting facilities and storage. Other range counts take advantage of the
underlying KV storage's order statistics meta data to have O(log n) counting
time.

## Query Optimizations

Datalevin query engine employs multiple optimization strategies.

### Predicates push-down

As mentioned above, we take advantage of the opportunities to push selection
predicates down to index scan in order to minimize unnecessary intermediate
results. Currently, two types of predicates are pushed down: 1. inequality
predicates involving one variable and one or two constants are converted to
range boundary in range scan; 2. other predicates involving one attributes are
pushed down to operations that scan the attribute values.

These predicates push-downs are implemented as query rewrites. We also plug the
constant query parameters into the query itself in order to avoid expensive
joins with these bindings turned relations. More query rewrite cases will be
considered in the future.

### Merge scan

For star-like attributes, we utilize an idea similar to pivot scan [2], which
returns multiple attribute values with a single index scan using `:eav` index.
This single scan takes a list of entity IDs, an ordered list of attributes
needed, and corresponding predicates for each attribute, to produce a relation.
This avoids performing joins within the same entity class to obtain the same
relation. The bulk of query execution time is spent on this operation.

The input list of entity IDs may come from a search on `:ave` index that returns
an entity ID list, a set of linking references from a relation produced
in the previous step, or the reverse references from the previous step, and so
on.

### Query graph simplification

Since star-like attributes are already handled by merge scan, the optimizer
works mainly on the simplified graph that consists of stars and the links
between them [4] [11] [13], this significantly reduces the size of optimizer
search space.

### Cost based query optimizer

We built a Selinger style cost-based query optimizer that uses dynamic
programming for query planning [14]. Instead of considering all possible
combinations of join orders, the plan enumeration is based on connected
components of the query graph. Each connected component has its own plan and its
own execution sequence. Multiple connected components are processed
concurrently. The resulting relations are joined afterwards, and the order of
which is based on result size.

### Left-deep join tree

Our planner generates left-deep join trees, which may not be optimal, but
work well for our simplified query graph, since stars are already turned into
meta nodes and mostly chains remain. This also reduces the cost of cost
estimation, which dominates the cost of planning. The impact of the loss of
search space is relatively small, compared with the impact of inaccuracy in
cardinality estimation. [7]

We do not consider bushy join trees, as our join methods are mainly based on
scanning indices, so a base relation is needed for each join. Since we
also count in base relations, the size estimation obtained there is quite
accurate, so we want to leverage that accuracy by keeping at least one base
relation in each join.

### Join methods (new)

Currently, we consider five join methods. For two sets of where clauses
involving two classes of entities respectively, e.g. `?e` and `?f`, we currently
consider the following cases.

#### Forward references `:ref`

If there is a reference attribute in the clauses that connects these two classes
of entities e.g. `[?e :a/ref ?f]`, forward reference method will be considered.
The forward `:ref` method takes the list of `f?` in the left relation, then
merge scan values of `?f` entities.

#### Reverse references `:_ref`

Reverse reference method has an extra step, it starts with `?f` in left relation
and scan `:ave` index to obtain corresponding list of `?e`.

#### Value equality join `:val-eq`

The third case is the value equality case, where `e` and `f` are linked due to
unification of attribute values, then `:ave` index is scanned to find the
target's entity IDs.

The above two methods are essentially nested loop joins using `AVE` index. They
scan for a list of entity IDs, and other attribute values then need to be merge
scanned to obtain a full relation.

#### Hash join `:hash-join`

An alternative to reverse references and value equality joins is hash join. Our
hash join operator chooses build side vs. probe side based on actual input
relation sizes, so it is more flexible and handles size estimation inaccuracy
more robustly.

For reverse reference type of hash join, we implement a form of sideway
information passing (SIP) using a bitmap [15] to pre-filter target relation.

#### Or-join `:or-join`

When an `or-join` clause connects a bound variable to a free variable, one or
more join links can be created: the free variable may be a value
in some triple patterns, the entities of these patterns can now be joined with
the entity of the bound variable. We first perform the `or-join` operation
to get a relation, then join with these pattern relations. This join also
benefits from a form of SIP by passing in bound values to `or-join` operation.

The choice of these join methods in the query plan and their ordering is
determined by the optimizer based on its cost estimation.

#### Not-join `:not-join` (easy cases)

For a restricted class of `not-join` clauses, we can plan an anti-join step
instead of always deferring evaluation to the late clause stage. The currently
optimized shape is conservative: explicit `not-join` join variables, pattern
bodies only, and a single source. When those join variables become bound in a
single plan component, the planner inserts an anti-join filter step directly in
that component.

This allows negative filtering to happen earlier than full late-clause
evaluation in common cases and can reduce intermediate relation sizes before
subsequent work. Clauses outside these conditions (e.g. nested complex clauses,
cross-source shapes, or ambiguous binding points) still fall back to late
resolution.

### Directional join result size estimation (new)

The traditional join result size estimation formula used in RDBMS like PostgreSQL
is based on a very simplistic statistical assumption: the attributes are
considered statistically independent from one another. Data in the real world
almost never meet this idealized assumptions. One major consequence of such
simplification is that the join size estimation formula is un-directional, the
same outcome is predicted regardless the side of the joins. In Datalevin, the
`:ref` and `:_ref` join methods described above are directional, hence the size
estimation should also be directional. No attribute independence assumption is
made in our size estimation, as it is based entirely on counting and sampling.
Data correlations are encoded naturally by these methods.

### Direct counting for result size estimation (new)

As mentioned, the main advantage of our system is having more accurate
result size estimation. Instead of relying on statistics based estimations
using histograms and the like, we count elements directly, because counts in our
list based triple storage are cheap to obtain. As our B+tree KV storage maintains
order statics on the branch nodes, the range count operations have O(log n)
time complexity. Compared with statistics based estimation, counting is simple,
accurate and always up to date.

### Query specific sampling (new)

We use sampling to estimate join result size. To ensure representative samples
that are specific to the query and data distribution, we perform sampling by
execution under actual query conditions. Online sampling is performed during
query using reservoir sampling methods. Similar to counting, online sampling
takes advantage of O(log n) rank operation of the counted feature of our KV
storage.

A sample of base entity IDs are collected first, then merge scans are performed
to obtain base selectivity ratios. After that, the selectivity of all possible
two way joins are obtained by counting the number of linked entity ids based on
these samples. The estimated selectivity is then calculated using
empirical-Bayes shrinkage with a prior [6]. To combat extreme skew, we implements
skew‑aware upper‑bound correction inspired by skew‑robust statistics [5]. Later
joins use these selectivity ratios to estimate result sizes. We have found our
method is more effective than sampling more than 2-way joins (e.g. [9]).

### Recency based link choice (new)

During planning, when multiple links are possible to reach the same next node in
the graph, we choose the link whose source node is most recently resolved. The
reason is the following: as the query execution progresses, the data
distribution shifts significantly. The source node with more recent resolution
represents the data distribution more accurately, while the distribution of
older nodes may be very different from current distribution. To do that, the
optimizer tracks recency of each step and prefers the most recent linkage, and
only use cost as a tie breaker.

### Dynamic plan search policy (new)

The plan search space initially include all possible join orders as our joins
are directional.  When the number of plans considered reaches `P(n, 2)`, the
planner turns the search policy from an exhaustive search to a greedy one. The
reason is that size estimation of only the initial two joins are absolutely
accurate, so a planning space larger than that afterwards does not need to be
exhaustive to be useful. The shrinkage of plan search space in the later stages
of planning has relatively little impact on quality of the final plan, while
results in significant savings in memory consumption and planning time for
complex queries.

### Parallel processing

Our counting and sampling based query planning method does more work than
traditional statistics based methods at query time. Fortunately, these work are
amicable for parallel processing, so it is done whenever appropriate.

Pipelining is used for plan execution, so multiple tuples in different execution
steps are in flight at the same time. A tuple generated from one step becomes input
of next step. Each step is processed by a dedicated thread.

### Multiple stage clause resolution

In addition to patterns and single variable predicates, Datalevin supports
complex clauses, such as `and`, `or`, `not`, `not-join`, multi-variable
predicates, function bindings, as well as rules. We are gradually expanding the
coverage of the optimizer to handle more clause types. Right now, `or-join`
and easy-case `not-join` are optimized. Other complex clauses are deferred
until the
indices access clauses have produced intermediate result. Heuristics and
variable dependencies are considered to reorder these complex clauses to
optimize performance. Rules are executed last, see [rules](rules.md) for details
of the rule engine.

## Benchmarks

We conducted several benchmarks to test Datalevin query engine.

### Datascript Benchmark

A benchmark developed in Datascript is performed. The speedup compared
with the original Datascript engine is substantial. The details can be found
[here](../benchmarks/datascript-bench). Queries in this benchmarks are simple
and often do not involve more than one relation.

### Join Order Benchmark (JOB)

The join order benchmark (JOB) [8] for SQL contains 113 complex queries that
stresses the optimizer. We ported these queries to Datalog and compared with
PostgreSQL and SQLite [here](../benchmarks/JOB-bench). The query execution time
of Datalevin are more consistent and much better (2X and more) on average than
PostgreSQL and SQLite, due to better query plans produced in Datalevin.

### LDBC SNB Benchmark

[LDBC SNB](../LDBC-SNB-bench) is an industry standard benchmark for graph
databases [3], where Datalevin compares favorably with neo4j. For Short
Interactive queries, Datalevin is orders of magnitude faster, while often faster
in Complex Interactive queries, with a couple of exceptions.

### Math Genealogy Benchmark

This [Datalog benchmark](../benchmarks/math-bench) [10] tests Datalog rules
evaluation performance. We compared with Datascript and Datomic, where Datalevin
is much faster. For recursive rules in particular, Datalevin is several orders
of magnitude faster, due to start of the art Datalog [rule engine
implementation](rule.md).

## Remark

The more granular and redundant storage format of triple stores brings some
challenges to query processing due to its greater demand on storage access, but
it also offer some opportunities to help with query processing.

We found that the opportunities lie precisely in the "Achilles Heel" of RDBMS
optimizer: cardinality estimation [6]. It is hard to have good cardinality
estimation in RDBMS because the data are stored in rows, so it becomes rather
expensive and complicated trying to unpack them to get attribute value
counts or to sample by rows [4]. On the other hand, it is cheap and
straightforward to count or sample elements directly in the already unpacked
indices of triple stores.

## Conclusion

Datalevin query engine stands on the shoulder of a half century of database
research to bring a new hope to triple stores. We have chosen to implement
simple and effective techniques that are consists with our goal of simplifying
data access, and we are also open for the future, e.g. explore learning based
techniques.

## Reference

[1] Aluç, G., Hartig, O., Özsu, M. T. and Daudjee, K. "Diversified Stress
Testing of RDF Data Management Systems". ISWC. 2014.

[2] Brodt, A., Schiller, O. and Mitschang, B. "Efficient resource attribute
retrieval in RDF triple stores." CIKM. 2011.

[3] Erling, O., et al. The LDBC Social Network Benchmark: Interactive Workload.
SIGMOD, 2015.

[4] Gubichev, A., and Neumann, T. "Exploiting the query structure for efficient
join ordering in SPARQL queries." EDBT. Vol. 14. 2014.

[5] Haas, P., and Swami, A. N. "Sampling-based selectivity estimation for joins
using augmented frequent value statistics." ICDE, 1995.

[6] Heimel, M., Markl V., and Murthy, K.. "A bayesian approach to estimating the
selectivity of conjunctive predicates." DBIS. 2009.

[7] Lan, H., Bao, Z. and Peng, Y.. "A survey on advancing the DBMS query
optimizer: cardinality estimation, cost model, and plan enumeration." Data
Science and Engineering, 2021

[8] Leis, V., et al. "How good are query optimizers, really?." VLDB Endowment
2015.

[9] Leis, V., et al. "Cardinality Estimation Done Right: Index-Based Join
Sampling." CIDR. 2017.

[10] D. Maier, et al. "Datalog: concepts, history, and outlook." In Declarative
Logic Programming: Theory, Systems, and Applications. 2018. 3-100.

[11] Meimaris, M., et al. "Extended characteristic sets: graph indexing for
SPARQL query optimization." ICDE. 2017.

[12] Moerkotte, G., and Neumann, T. "Dynamic programming strikes back."
SIGMOD. 2008.

[13] Neumann, T., and Moerkotte, G. "Characteristic sets: Accurate cardinality
estimation for RDF queries with multiple joins." ICDE. 2011.

[14] Selinger, P. Griffiths, et al. "Access path selection in a relational
database management system." SIGMOD. 1979.

[15] Zhao, H., et al. "I Can’t Believe It’s Not Yannakakis: Pragmatic Bitmap
Filters in Microsoft SQL Server.", CIDR, 2026.
