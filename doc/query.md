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

## Nested Queries and Derived Relations

The built-in `q` function runs a nested Datalog query. When its result is bound
with a relation binding, the result becomes a derived relation whose columns
can be joined and aggregated by the containing query:

```clojure
(d/q '[:find (sum ?w) .
       :where
       [(q [:find ?key (count ?left-row)
            :where [?left-row :left/key ?key]]
           $)
        [[?key ?wl]]]
       [(q [:find ?key (count ?right-row)
            :where [?right-row :right/key ?key]]
           $)
        [[?key ?wr]]]
       [(* ?wl ?wr) ?w]]
     db)
```

Variables inside a nested query are locally scoped. The output binding maps
the nested query's result columns positionally to variables in the containing
query. An uncorrelated nested query, whose inputs contain no outer logic
variables, is evaluated once and materialized before it is joined with other
relations. A nested query that receives outer logic variables is correlated and
may be evaluated once per input tuple.

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
results. A predicate is eligible when it contains exactly one free variable,
that variable is an attribute value in the query graph, and the predicate does
not contain a dynamic source form.

Comparisons (`<`, `<=`, `>`, and `>=`) involving constants become open or
closed AVE range boundaries. Equality becomes an exact range, while `in` and
`not-in` become one or more exact or complementary ranges. `like` and
`not-like` derive a prefix range when the pattern permits it and retain the
value predicate for the final check; a wildcard-free `like` becomes an exact
bound value. Multiple range predicates on the same value are intersected, so a
contradictory intersection can be recognized without scanning. Big-decimal
inequalities and other single-variable predicates that cannot safely become
index bounds are still attached to the value scan instead of being evaluated
after materialization.

These push-downs are implemented while building the query graph. Related input
specialization and early materialization rewrites run before graph planning.

### Input and dead-binding specialization

Datalevin substitutes a safe scalar query input directly into its where
clauses and removes the corresponding one-row input relation. This avoids a
relation join for values that are not returned and do not need to remain as
variables for an `or-join`. Sequential scalar inputs remain intact so query
functions can consume them lazily. An unprojected singleton collection input
used exactly once as a pattern value can likewise become a literal pattern
value.

Repeated patterns with the same source, entity, and attribute receive
schema-aware handling when one pattern contains a constant value. For a
cardinality-one attribute, the constant determines the duplicate variable and
the extra lookup is replaced by a one-value binding. For a cardinality-many or
schema-unknown attribute, that equivalence would be invalid, so Datalevin
materializes the repeated group with its constant pattern first and retains all
matching variable values.

After input, rule, and disjunction rewrites, the optimizer also replaces a
variable that occurs only once and is not needed by `:find`, `:with`, `:in`, an
input relation, or a nested clause with an internal placeholder. For a pattern
such as `[?e :item/name ?unused]`, this preserves the existence test but avoids
reading and carrying an unused value column through scans and joins. Variables
used by predicates, functions, rules, `or`, or `not` forms are protected from
this rewrite.

### Pre-planning pattern materialization

After resolving query inputs, Datalevin materializes a database pattern before
graph planning when exactly one of its entity or value variables is already
represented by a small relation. The pattern must have a constant keyword
attribute and use a searchable database source. Its normal costed lookup is
joined into the current relations, the pattern is removed from the remaining
where clauses, and the process repeats while newly bound variables make another
pattern eligible.

This propagation avoids planning an unconstrained scan when an input relation
already provides the relevant entity IDs or attribute values. The safety limit
also prevents a large input collection from turning pre-planning into an
unbounded series of point lookups.

A related case handles multiple unique constant anchors. Consider a path query
whose two endpoint IDs are projected or otherwise must remain bound:

```clojure
[:find ?start ?middle ?end
 :where
 [?start :node/id 1]
 [?end :node/code "three"]
 [?edge1 :edge/from ?start]
 [?edge1 :edge/to ?middle]
 [?edge2 :edge/from ?middle]
 [?edge2 :edge/to ?end]]
```

If `:node/id` and `:node/code` are declared `:db.unique/identity` or
`:db.unique/value`, and the anchors belong to the same connected query
component, Datalevin resolves both before planning. Their entity relations then
constrain propagation from both ends of the path. A missing unique value
produces an empty relation and short-circuits the query.

The optimizer intentionally leaves a single unique literal with the normal
planner because it is already an ideal selective root. It also leaves unique
anchors in disconnected components alone, avoiding eager work that cannot
constrain the same join component. This special handling applies only when at
least two connected unique entity variables must be preserved by `:find`,
`:with`, or an `or-join`.

Pre-materialization is not limited to attributes declared unique. After the
other pre-planning rewrites, a constant AVE lookup whose entity variable joins
the rest of the query is considered as a possible entity relation, regardless
of the attribute's value type. The first cost gate compares its exact AVE
fanout and one-column materialization cost with the marginal cost of the normal
plan step that first introduces that entity. It also requires that delayed step
to dominate the remaining plan, so borderline alternatives do not materialize
candidate tuples.

For a candidate that passes the gate, Datalevin resolves every entity matching
the value, including all matches of a non-unique attribute, and propagates the
new bindings through the same bounded-pattern machinery described above. It
first performs a tuple-free preflight of the complete propagation chain, using
bounded or sampled cardinality probes for each step. If that projection fits
within the delayed-step cost budget, actual propagation begins and is checked
against the same budget after every lookup. Datalevin charges the lookup
outputs, relation allocation, hash joins, and the estimated residual plan. The
rewrite is accepted only when that complete cost is lower than the unchanged
plan. Consequently, a two-entity value can be pre-materialized when it prevents
a much larger intermediate, while even a one-entity value remains in the graph
when its downstream expansion would make eager propagation more expensive.

With `explain`, evaluated candidates appear in
`:pre-materialization-decisions`. Each decision reports AVE fanout, lookup and
delayed-step costs, materialization and residual costs, both complete plan
costs, any cost-budget guardrail that stopped a trial, and the selected
strategy (`:pre-materialized-value-lookup` or `:planner-value-lookup`).

### Equality-disjunction pattern push-down

A value filter expressed as an `or-join` may otherwise be applied only after a
large attribute relation has been materialized. For example:

```clojure
[?message :message/isLocatedIn ?loc]
(or-join [?loc ?country-x ?country-y ?x-inc ?y-inc]
  (and [(= ?loc ?country-x)]
       [(ground 1) ?x-inc]
       [(ground 0) ?y-inc])
  (and [(= ?loc ?country-y)]
       [(ground 0) ?x-inc]
       [(ground 1) ?y-inc]))
```

When `?loc` is only an internal filter variable, the optimizer distributes the
pattern into the equality branches:

```clojure
(or-join [?message ?country-x ?country-y ?x-inc ?y-inc]
  (and [?message :message/isLocatedIn ?country-x]
       [(ground 1) ?x-inc]
       [(ground 0) ?y-inc])
  (and [?message :message/isLocatedIn ?country-y]
       [(ground 0) ?x-inc]
       [(ground 1) ?y-inc]))
```

This is the relational rewrite `P(e, v) AND OR_i(v = t_i AND B_i)` to
`OR_i(P(e, t_i) AND B_i)`. It exposes each selectively bound target value to
the normal pattern lookup machinery. Runtime lookup costing can then choose
AVE point probes instead of an attribute scan, while retaining entity-bound or
full lookup when either is estimated to be cheaper.

The rewrite is deliberately conservative. It requires:

* a three-element EAV pattern, after an optional source, with variable entity
  and value positions and a constant keyword attribute on a searchable source;
* a flat, explicit `or-join` variable list with at least two branches;
* exactly one equality in every branch between the pattern's value variable and
  a branch target, with no other use of that value variable in the branch;
* branch targets declared by the `or-join` and selectively bound outside it by
  an already materialized relation or a constant-constrained pattern;
* no use of the eliminated value variable by another where clause, `:find`,
  `:with`, `:in`, `:having`, or `:order-by`; and
* no already materialized or selective constant binding for the entity
  variable, since the original pattern can already use a cheap entity probe in
  that case.

If any condition is not met, the query is left unchanged and follows normal
planning and late-clause resolution. The rewrite runs after scalar inputs and
non-recursive rules have been expanded, and before graph construction, so the
rewritten patterns participate in the existing lookup cost decisions.

### Costed bound-pattern lookup

When clause resolution reaches a database pattern, a single bound entity or
value becomes an ordinary EAV or AVE point lookup. If a relation supplies
multiple distinct entities or values, Datalevin compares two complete costs:
performing one indexed probe per bound key and joining the returned tuples, or
scanning the matching attribute range and hash-joining it with the bindings.
The comparison includes probe, retrieval, scan, and join work and uses the
current index count. A large-key safety limit remains a guardrail, but it is not
the normal decision boundary.

Entity-bound patterns whose value is `_` or an optimizer-generated placeholder
take a narrower path. They use an EAV `populated?` probe and emit the entity at
most once instead of retrieving every matching value. This is especially
useful for cardinality-many attributes used only as existence conditions. A
concrete value also emits only the entity binding once after a successful
probe, while a required value variable retains normal multiplicity.

When both the entity and value variables have multi-value bindings, the lookup
intersects them while reading the cheaper physical side. It compares batched
EAV probes, batched AVE probes, and an attribute scan using the current index
counts. Tuples whose opposite endpoint is not in the other bound set are
discarded during the read, before a relation and its hash joins are
materialized.

### Bound-value expansion with presence filtering

A bound-value pattern that produces a new entity can be fused with contiguous
presence-only checks on that entity. For example, given bound `?person` rows,
the sequence `[?post :message/hasCreator ?person]` followed by
`[?post :message/isContainedIn _]` first performs the normal costed AVE
expansion in an isolated relation. It applies the presence check to that compact
producer and joins the surviving posts with the wider outer payload only once.
This avoids carrying payload columns through rows that the presence check will
discard.

The specialization accepts one or more immediately following wildcard checks
with the same entity and database source. Each check still uses the ordinary
cost choice between indexed EAV presence probes and an attribute scan; the
rewrite does not create an index or force a particular storage operation.
Default and explicit sources, singleton and multi-value bindings, and duplicate
outer rows preserve their normal semantics. An intervening clause, a different
source or entity, a value-producing EAV pattern, or an already bound entity
falls back to ordinary clause resolution.

### Singleton-owned runtime domains

Two indexed patterns can share a value that is not yet logically bound while
both of their entity variables are already bound. If exactly one entity has a
single distinct value, its pattern can cheaply produce a semi-known runtime
domain for the shared value. For example, given many bound `?post` values but
one bound `?start`, these clauses need not materialize every tag on every post
before discovering the start person's small interest set:

```clojure
[?post :message/hasTag ?tag]
[?start :person/hasInterest ?tag]
```

Datalevin resolves the singleton-owned pattern in isolation and turns its
distinct values into an immutable membership predicate on the other pattern's
EAV scan. Large bound-entity inputs retain the storage layer's CPU-aware
parallel chunk scan because the predicate is safe to share between workers;
smaller inputs use point probes. Only matching entity/value pairs are
materialized and joined with the wider outer payload.

This is an exact join rewrite, not existential join elimination: the owner,
consumer entity, and shared value remain in the compact result, so later uses
of the shared value and duplicate outer payload rows preserve normal query
semantics. The rewrite is independent of `or-join` and is available in both
normal conjunction resolution and late-clause execution. It requires the same
searchable source, keyword attributes, one singleton owner, and an actually
small domain relative to a non-trivial consumer entity set. A structural
prepass lets conjunctions without a compatible shared value bypass domain
planning, while a minimum consumer-size guard avoids turning short property
chains into optimization work. Multiple owners, an already bound shared value,
incompatible sources, oversized domains, or insufficient runtime selectivity
fall back to ordinary clause resolution.

### Costed late indexed-producer scheduling

Some dependencies expressed by rules or disjunctions are deliberately left
outside the graph plan and resolved after its planned components. At that
boundary, Datalevin can choose among a bound database pattern and compatible
indexed producers later in the clause sequence. The supported alternatives are
an indexed `or-join` or a bounded scalar AVE scan that produces the pattern's
entity variable. For the rewritten country example above, this means comparing
three ways to obtain candidate messages: expand the messages for the currently
bound people, form the union of message IDs in the two bound countries, or scan
the requested creation-date interval.

The decision uses exact index counts for the values present in the runtime
relations. It charges an indexed union for its probes and retrievals and its
isolated projection. A range alternative is charged for the AVE scan and the
one-column relation it materializes; open endpoints are removed from the
inclusive range count. Both alternatives also include the subsequent two-sided
pattern lookup. The lowest complete switch cost wins only when it is lower than
the original bound-pattern cost. This avoids relying on a fixed cardinality
ratio and allows the same query shape to choose a different order for different
input parameters.

Union eligibility is intentionally narrow: a flat explicit `or-join` must have
at least two branches, each branch must contain one pattern with the same
source and attribute, the entity must be declared by the `or-join`, and every
currently bound declared variable must have exactly one value. The remaining
branch clauses may only be constant `ground` bindings. The union is evaluated
in an isolated context and projected to its newly produced variables before it
is joined with the outer relations. This prevents unrelated singleton seed
columns from creating a Cartesian product during materialization.

Range eligibility is similarly conservative. The attribute must be
cardinality-one, its AVE ordering must implement the Datalog inequality exactly,
and exactly one simple scalar lower bound and one simple scalar upper bound must
constrain its value variable. This covers the scalar types accepted by normal
inequality range pushdown; BigDecimal remains excluded because its index prefix
uses an inexact double approximation. A bound operand may come from a runtime
relation only when it has one distinct value. The value variable may occur only
in the attribute pattern and those two inequalities, and it cannot be used by
`:find`, `:with`, or `:having`. Datalevin then consumes all three clauses
together and materializes only their entity IDs. Otherwise the clauses retain
their normal order and semantics.

With `{:run? true}`, `explain` reports these runtime choices in
`:late-clause-decisions`, including the bound-pattern fanout and cost, the
winning alternative's fanout and producer cost, its complete switch cost, and
the selected strategy (`:bound-pattern-first`, `:indexed-union-first`, or
`:indexed-range-first`).

### Ordered limit push-down

An ordered query with a small `:limit` should not have to materialize and sort
every matching tuple. For all finite ordered relation queries, Datalevin uses a
bounded priority queue of `:offset + :limit` tuples. This reduces sorting space
from the full result size to the size of the requested window even when index
push-down is not possible.

Datalevin can avoid producing the full result for conservative access shapes.
For an AVE path, the leading order term must be the value variable of a simple
default-source EAV pattern, and an inequality predicate must provide a scalar
or constant range boundary in the scan direction. A single-domain `fulltext`
clause using `:display :refs+scores` can provide the same descending property
when the score variable is the leading order term. A single-domain
`vec-neighbors` or `embedding-neighbors` clause using
`:display :refs+dists` similarly provides ascending distance order. Aggregates,
pull expressions, `:with`, `:having`, result maps, and other unsafe batching
shapes use complete or normal execution.

The optimized path walks the AVE index, fixed fulltext result stream, or fixed
approximate vector result stream in candidate batches. It replaces the covered
source clause with a relation of candidate tuples, then executes the rest of the
original query against that relation. Distinct result tuples are accumulated
until the scan has passed the primary order value of the last tuple in the
requested window. All candidates at the boundary value are evaluated before
stopping, so secondary order terms are handled correctly.

The adaptive controller derives a maximum candidate budget from
`:offset + :limit` and the sampled or estimated yield of the residual joins and
filters, capped by the available access range. A retained planning-sample
prefix counts toward that budget, and execution resumes from its opaque access
frontier instead of reading the prefix again. If the budget is exhausted before
the requested window is proven complete, execution uses the already planned
conventional root. The older 32-batch limit remains only as a safety guard for
an access path without an explicit candidate budget. `explain` reports the
access and conventional alternatives, the candidate budget, and sample reuse;
with `{:run? true}` it executes the selected root. When an access root is
selected, `:plan` reports its access method, mode, physical operators, actual
candidate and fragment counts, and the residual plan for each batch. If the
adaptive controller falls back, the same plan also reports the attempted
access work and the conventional fallback reason and plan.

The query result cache stores the exact ordered `:offset`/`:limit` window. Its
key includes the complete parsed query, including the ordering and window, so
different pages are cached separately. Transaction invalidation remains based
on the attributes used by the query. Unbounded queries continue to cache their
full result, and the plan cache remains independent of result-window caching.

### Post-top-k property enrichment

An ordered limit can still do unnecessary work when a late function fetches
properties used only by the final projection. Datalevin can defer a total
`get-some-else` enrichment until after the result window is selected. It first
collects and orders the candidate keys, applies `:offset` and `:limit`, and then
reads the optional properties only for the selected rows. The fallback argument
to `get-some-else` makes the operation cardinality preserving even when none of
the requested properties exists.

This rewrite requires a proof that moving the function cannot affect filtering,
ordering, or distinctness. Every enrichment output must be introduced only by
that clause and used only by `:find`; it cannot occur in `:in`, `:with`,
`:having`, `:order-by`, or another where clause. A retained projected entity ID
or unique attribute value must also provide a stable distinct key. Queries with
aggregates, pull expressions, result maps, or other non-relational result
shapes retain their original evaluation order.

`explain` reports an accepted rewrite under `:post-top-k-enrichment`. With
`{:run? true}`, it includes both the number of candidates and the number of
rows enriched, together with the cardinality-preserving, projection-only, and
stable-distinct-key proof obligations.

### Adaptive unordered limits

A finite unordered relation query can also avoid producing every source match
when its driving operation exposes a resumable access path, such as an
optimizer-selected `idoc-match`, `fulltext`, `vec-neighbors`, or
`embedding-neighbors`. Unlike ordered limit push-down, this execution mode does
not require or provide an ordering property.

The root query retains `:offset + :limit` as its required output count while
the access source is read in bounded batches. The remaining joins and filters
run on each batch, and offset is applied only after enough distinct final
tuples survive. The optimizer estimates how many source candidates those
clauses may discard and can resume the source to fetch more.

This path competes with the conventional plan in the cost model. Queries
without a finite limit and query shapes that cannot safely preserve their
semantics under batching continue to use conventional execution. Adaptive
execution also retains the conventional plan as a fallback if candidate work
reaches its safety budget.

The general access-method contract, property propagation rules, correlated
access scheduling, and bounded alternative search are described in
[Property-Aware Access Planning](access-planning.md).

### Bounded access-path sampling

Sampling an access alternative must not cost more than the plan choice could
save. When the conventional root has a complete cost estimate, that cost is a
shared planning-sample budget across all access alternatives. Before opening an
access cursor, the optimizer projects the work of the reachable indexed joins
from catalog counts. It rejects an over-budget alternative without reading its
speculative tuples. During sampling, the same budget is checked before each
materialized expansion; rejected work is reported as
`:unavailable-reason :sample-work-budget` and under
`:estimate :sampling-abort` in `explain`.

A terminal EAV expansion sometimes needs no sample relation at all. If the
entity and value are both projected, their pair proves distinct result rows,
there are no later indexed joins, and the remaining residual predicates are
safe to sample, Datalevin sums the actual sample's indexed fanouts instead of
materializing the expanded values. Duplicate input sample rows retain their
weight, and the count is capped by the remaining work budget. The corresponding
join stage appears in `explain` with `:sampling :counted`.

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

The implementation sorts the input entity IDs and requested attributes, then
uses one EAV cursor to obtain all requested values for each entity. Attribute
prefix setup, cursor positioning, and predicate dispatch are shared by the
fused operation. Consequently, the cost model charges the first output value at
full price and, by default, each additional output value at only 15 percent of
that price. Residual value predicates and equality checks against columns
already in the input tuple are evaluated during the scan, before rejected rows
are materialized.

Presence-only EAV checks and exact AVE membership checks have narrower native
loops. Their invariant key portion is encoded once per chunk, and repeated
adjacent entity IDs, values, or value/entity pairs reuse the preceding probe
result. These paths preserve every matching input tuple, including duplicate
payload rows, while avoiding retrieval of values that the query does not need.

### Provenance-aware EAV gathering

An indexed expansion can already prove one of the facts that a following merge
scan would otherwise read again. For example, after expanding memberships by a
bound person through AVE, gathering the membership's other properties does not
need to verify the person attribute a second time. Reverse-reference and
value-equality link steps, as well as compatible fused `or-join` expansions,
therefore attach provenance that identifies the produced entity column, the
proven attribute, and the input value column.

When the immediately following base scan is fused into that expansion, the
planner removes the redundant EAV attribute only if all three parts of the
proof match. An attribute with a residual predicate is never removed. The
proof is deliberately local to the preceding operator, so it cannot survive an
intervening operation that might change either column. If every attribute in a
degenerate duplicate-pattern scan would be removed, one physical check is
retained because a merge scan must still have an attribute to scan.

The same physical rewrite avoids carrying values needed only by a local range
or predicate. The planner first determines which variables must survive for
the result, a downstream join or clause, an entity or link binding, or another
occurrence of the variable. A value outside that set is still read and tested
inside the fused EAV scan, but it is not appended to every output tuple. Base
samples continue to materialize all logical values, and the cost model retains
the original attributes and variable count. Consequently, this narrower
runtime tuple does not perturb sampling, join enumeration, or existing plan
selection.

Removing a tuple-dependent equality check could otherwise enable the
repeated-entity output cache. The planner explicitly keeps that cache disabled,
preserving the previous no-cache execution mode. Duplicate entity IDs are
safely rescanned rather than incorrectly reusing a result produced for a
different input tuple. This optimization uses the existing AVE expansion and
EAV cursor; it neither creates nor requires a composite index or schema change.

### Deferred EAV attribute groups

Fusing attributes is usually cheaper than issuing independent EAV scans, but it
can be wasteful when a large entity relation is about to be reduced by a hash
join. The optimizer therefore builds a guarded alternative in which eligible
attributes become dependency nodes in the component plan. It groups them by
owning entity and by role: projection-only attributes form one group and local
filters form another. An attribute is movable only when it is not
cardinality-many, has a non-placeholder binding variable, has no literal
value, and that variable is not a connection key between entity nodes.

The eager plan is built first. Deferred placement is considered only when that
plan contains a hash join, and attributes already placed after a hash join are
removed from consideration. To keep the additional state space bounded, the
alternative preserves the eager plan's entity join order and searches only the
legal positions of at most four attribute groups in a component of at most ten
entity-plus-group nodes. Thus the search answers a global question--whether the
smaller downstream scan repays the loss of fusion--without reopening the much
larger join-order search.

Before expanding those states, Datalevin computes an optimistic upper bound on
savings by pretending that every deferred scan is free. If even that bound is
less than five percent of the complete eager-plan cost, the eager plan wins
without further enumeration. Otherwise, the complete eager and deferred costs
are compared, and a deferred plan is selected only when a group actually lands
after a hash join and clears the same improvement margin.

`explain` exposes the decision under `:attribute-group-planning`, including
each group's owner, role, attributes, variables, estimated selectivity, the
evaluated alternative costs, and the selected strategy. A search rejected by
the early guard instead reports `:reason :insufficient-potential-savings` and
its optimistic savings bound.

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

Currently, we consider seven join methods. For two sets of where clauses
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

The graph planner uses a minimum-input guard before considering a hash join,
then compares it with the indexed-link alternative by cost. Hash-join costing
uses the larger of its input-work estimate and an output-materialization
estimate based on predicted result cardinality and tuple width. This makes a
high-fanout join expensive when allocating and copying its output dominates,
without double-charging ordinary output already covered by the historical
input coefficient.

For access-source fragments, the property memo can retain both an ordered
index-join alternative and an unordered hash-join alternative for the same
logical subset. Index joins preserve outer order when their operator contract
says so; hash joins discard ordering and resumability. An alternative is
removed only when another has no greater cost and size while providing a
superset of its physical properties.

For reverse reference type of hash join, we implement a form of sideway
information passing (SIP) using a bitmap [15] to pre-filter target relation.

##### Reusable-domain SIP

The ordinary reverse-reference SIP bitmap is built from the hash join's full
left input. That may be too late when an earlier `or-join` produced a small
domain and an intervening AVE expansion multiplied it into a much larger
relation. For a later value-equality hash join, the optimizer can connect that
earlier domain directly to the target scan instead.

The producer must be a fused `or-join` whose first free variable is a reference
value used by the later hash-join input. The target must scan a reference
attribute for the same value, and both hash-join inputs must clear the normal
minimum-size guard. The `or-join` records the distinct integer domain in a
bitmap before beginning its AVE expansion. This allows the target scan to start
with the domain filter while the expanded input is still flowing through the
pipeline.

At runtime, SIP is used only when the actual domain is non-empty and is
sufficiently smaller than both estimated hash-join inputs. A small domain
on the target's initial scan becomes exact single-value index ranges; a larger
one becomes a bounded range plus a bitmap predicate. If the target attribute is
already part of a fused merge scan, the bitmap predicate is attached directly
to that attribute. If the runtime guard fails, execution uses the normal hash
join. One captured domain is assigned to at most one later consumer. `explain`
identifies the annotated operators as `reusable-domain SIP` and reports the
domain variable, primary join variable, target attribute, actual domain size,
input sizes, and decision under `:reusable-sip-domains`.

#### Indexed semi-join `:semi-join`

When the target of an indexed join is used only to test existence, producing
all of its bindings can multiply the left relation without changing the query
result. For example:

```clojure
[:find ?title
 :where
 [?t :title/name ?title]
 [?credit :credit/title ?t]
 [?credit :credit/role :actor]]
```

If `?credit` has no other graph connection and is not otherwise required, the
last two patterns mean "a matching credit exists." An indexed semi-join emits
each matching input tuple once instead of emitting one tuple per credit.

The optimizer recognizes this conservatively. The target must be a degree-one
leaf connected by `:ref`, `:_ref`, or `:val-eq`, with exactly one incoming graph
link. None of the columns introduced by the target may be required by `:find`,
`:with`, input relations, late clauses, planned `not-join` clauses, or another
database source. The estimated ordinary join result must also be larger than
the input, otherwise avoiding fanout is not expected to pay for the seen-set
work. Projecting `?credit` in the example therefore selects a normal join.

Execution runs the normal indexed link and merge-scan steps into a set of
matching left prefixes, then filters the original left tuples through that set.
The right-side bindings and duplicate matches are never materialized as output.
`explain` identifies this step as `Semi-join by indexed link scan.` The variables
required outside each source graph are included in the plan-cache key, so a
plan for an existence-only query cannot be reused for a query that projects the
same target.

This optimization intentionally does not cover general last-use nodes or dead
subgraphs. Restricting it to a leaf keeps eligibility cheap and prevents the
smaller semi-join cardinality from changing earlier join-order decisions.

#### Or-join `:or-join`

When an `or-join` clause connects a bound variable to a free variable, one or
more join links can be created: the free variable may be a value
in some triple patterns, the entities of these patterns can now be joined with
the entity of the bound variable. We first perform the `or-join` operation
to get a relation, then join with these pattern relations. This join also
benefits from a form of SIP by passing in bound values to `or-join` operation.
Value-filtering shapes may first undergo the
[equality-disjunction pattern push-down](#equality-disjunction-pattern-push-down),
which exposes branch target values directly to indexed pattern lookup.

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

Both planned and late `not-join` execution physically project the outer input
to the declared join keys and deduplicate those keys before resolving the
negative body. The negative result is projected and deduplicated the same way
before subtraction. This avoids carrying hidden tuple columns or repeated keys
through the anti-join while preserving the original outer tuples in the final
result.

##### Reusable indexed prefix

A correlated `not-join` may declare several join variables even though the
first part of its body depends on only a much smaller anchor key. Resolving the
whole negative body for every distinct full key then repeats the same indexed
walk. Datalevin can decorrelate a contiguous prefix of at least two connected
EAV patterns, evaluate it once per distinct anchor, and join the prefix result
back to the complete outer keys before resolving the residual clauses.

Eligibility is conservative. The prefix must start from a non-empty strict
subset of the declared join variables, produce at least one other declared
join variable, and leave a residual clause whose semantics still depend on the
complete key. It requires the searchable implicit source and is disabled for
incremental delta evaluation. The specialization runs only when there are at
least 16 distinct full outer keys per distinct anchor key; otherwise ordinary
`not-join` resolution is cheaper and remains the fallback. Both paths finally
project and deduplicate the declared keys before subtracting them from the
original outer relation.

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

When two join inputs share more equality variables than the graph link chosen
to connect them, estimating from that primary link alone can greatly
overestimate the result. Datalevin recognizes the additional common keys from
the two input schemas and reduces the estimate for each one. It deliberately
uses the square root of the key attribute's observed domain cardinality rather
than the full independence formula: graph attributes are often correlated, so
the damped correction captures the extra equality constraint without assuming
independent distributions. The corrected output estimate is also used when
comparing an indexed link with its hash-join alternative.

### Direct counting for result size estimation (new)

As mentioned, the main advantage of our system is having more accurate
result size estimation. Instead of relying on statistics based estimations
using histograms and the like, we count elements directly, because counts in our
list based triple storage are cheap to obtain. As our B+tree KV storage maintains
order statistics on the branch nodes, the range count operations have O(log n)
time complexity. Compared with statistics based estimation, counting is simple,
direct, and transactionally maintained. Because a zero count is also used as a
correctness decision, it receives the additional verification described below.

For an `or-join` link sample, the planner resolves the `or-join` relation but
does not materialize the final AVE join just to measure its size. It sums the
exact target-attribute fanout for each matched free-variable value instead,
caching fanout within the estimate. This preserves the multiplicity of both
input and `or-join` tuples while avoiding potentially large estimate-only tuple
products. A planning component also reuses the resolved `or-join` build across
outgoing target attributes. The short-lived cache uses input-list identity, not
the mutable list's content hash, together with an immutable link description.

A zero returned by counted-index metadata is verified against the actual EAV
or AVE range before it is allowed to short-circuit the query. If the range is
populated, the optimizer substitutes a conservative non-empty size and stays on
the sampled planning path. This keeps a fast but stale metadata zero from being
treated as a correctness proof that the result is empty.

### Query specific sampling (new)

We use sampling to estimate join result size. To ensure samples are specific to
the query and data distribution, sampling executes the same base scans,
predicates, and directional links being considered by the planner. Similar to
counting, online sampling takes advantage of rank operations on the counted KV
indexes.

A sample of base entity IDs is collected first, then merge scans obtain base
selectivity ratios. Two-way link selectivity is measured by counting linked
entity IDs from these samples. The active mainline policy applies a conservative
dominating envelope: it uses at least the observed sample mean, the
storage-derived default fanout, and the semantic fallback floor. The selected
ratio is cached within the planning component and used to estimate later joins.

Empirical-Bayes shrinkage and optional tail corrections are alternative
policies evaluated by the CIDR work; they are not the estimator used by this
branch. See the
[CIDR 2027 estimator study](cidr2027.md#24-managing-sampling-uncertainty) for
the distinction and experimental results.

Sampling itself is also subject to dominance checks. If a connected component
already contains an exact one-row anchor, an unbound, cardinality-one node whose
attributes are used only for projection cannot provide a better root. Datalevin
therefore defers that node's speculative global value sample while retaining
its catalog count and its normal place in join enumeration. An attribute used
by a predicate, join, or late clause is not eligible, because its sample can
still change selectivity or ordering decisions. Deferred roots are listed by
source under `:deferred-base-samples` in `explain`.

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
traditional statistics based methods at query time. Independent component
counts, samples, and plans are evaluated concurrently when the database is
read-only. Plan execution also uses a bounded tuple pipeline, so different
steps can have tuples in flight at the same time.

The most expensive list-based storage operations additionally parallelize
inside an individual scan batch. This currently covers fused EAV merge scans,
EAV presence filtering, AVE filtering by a bound entity, and AVE filtering by
a value/entity pair. Inputs are divided into contiguous chunks and outputs are
concatenated in chunk order, preserving the serial result order. The calling
thread performs one chunk while the remaining chunks use the common fork-join
pool.

Participation is adaptive rather than a fixed worker count. It is bounded by
the number of available processors, currently available executor slots, and
the amount of useful work at a target of roughly 4,000 probes per participant.
Nested calls from a fork-join worker discount the occupied pool slot. A
single-core machine, a small input, an exhausted executor, or a write
transaction therefore remains serial.

A predicate used by a parallel EAV scan must also be safe to invoke
concurrently. Generated predicates carry a factory from which each chunk gets
an independent instance; immutable predicates may explicitly be marked
shareable. Predicate composition retains these factories when every child is
forkable. An opaque user predicate has no such proof, so its scan remains
serial. This makes storage-level parallelism available to normal Datalog plans
without introducing shared mutable predicate state.

### Multiple stage clause resolution

In addition to patterns and single variable predicates, Datalevin supports
complex clauses, such as `and`, `or`, `not`, `not-join`, multi-variable
predicates, function bindings, as well as rules. We are gradually expanding the
coverage of the optimizer to handle more clause types. Right now, `or-join`
and easy-case `not-join` are optimized into the query plan. Other complex
clauses are deferred as late clauses until the index access plan has produced
intermediate results.

Late clauses are ordered during planning by variable dependencies. The planner
starts with variables bound by query inputs and by the planned index access
components, then repeatedly chooses the earliest late clause whose required
variables are available. Deferred patterns and function bindings can make new
variables available to later late clauses, which avoids running predicates or
dependent clauses before their inputs are bound. The ordering is stable among
clauses that are ready at the same time, and clauses whose dependencies cannot
be satisfied are left in their original order so normal validation errors remain
clear. The planned order is visible in `explain` under `:late-clauses`.

Direct `and` branches of a late `or-join` receive an additional dynamic ordering
step. Within each contiguous run of ready, fixed-attribute data patterns, the
resolver chooses the cheapest indexed pattern using the bindings produced so
far. Very small bound sets use exact index fanout counts; larger sets use a
bounded probe-count estimate so planning does not repeat the scan's work.
Predicates, functions, and other clause forms remain ordering barriers. When a
branch pattern moves ahead of source order, `explain` records the alternatives
and selected pattern under `:late-or-join-branch-decisions`.

Late work also participates in physical-plan costing. Predicates and function
bindings are charged per estimated input row. Patterns, `or` clauses, and rules
may expand cardinality rather than merely filter it; when a non-correlated
access path has a real planning sample with positive residual output, Datalevin
extrapolates that observed yield to the complete access range. The conventional
alternative is then costed with the larger of its own planned cardinality and
the sampled expansion. Heuristic estimates and zero-output samples are not
used for this upward correction. `explain` reports the correction as a
`:sampled-late-expansion` stage in the conventional alternative's cost
breakdown.

Rules are resolved by the rule engine when their rule clauses are reached; see
[rules](rules.md) for details of the rule engine.

## Runtime UDFs in Queries

Datalevin query supports a `udf` function for runtime-resolved user-defined
functions. Query UDFs are resolved against the runtime registry attached to the
current DB value.

`udf` accepts:

* an installed ident whose entity stores `:db/udf`
* an inline descriptor map
* a registered id keyword when that id is unambiguous in the registry

Query UDF descriptors use `:udf/kind :query-fn`. Predicate UDFs use
`:udf/kind :predicate`. Descriptor maps support only `:udf/lang`, `:udf/kind`,
`:udf/id`, and optional scalar `:udf/version`; unsupported keys are rejected.

```clojure
(require '[datalevin.core :as d]
         '[datalevin.udf :as udf])

(def descriptor {:udf/lang :java
                 :udf/kind :query-fn
                 :udf/id   :normalize-email})

(def registry
  (doto (udf/create-registry)
    (udf/register! descriptor clojure.string/lower-case)))

(def conn
  (d/create-conn
    "/tmp/query-udf"
    {:user/email {:db/valueType :db.type/string}}
    {:runtime-opts {:udf-registry registry}}))

(d/transact! conn [{:db/ident :normalize-email
                    :db/udf   descriptor}
                   {:db/id 1 :user/email "A@B.COM"}])

;; installed ident
(d/q '[:find ?email .
       :in $
       :where
       [?e :user/email ?raw]
       [(udf :normalize-email ?raw) ?email]]
     @conn)

;; inline descriptor
(d/q '[:find ?email .
       :in $ ?descriptor
       :where
       [?e :user/email ?raw]
       [(udf ?descriptor ?raw) ?email]]
     @conn
     descriptor)
```

The runtime registry is transient; only `:db/udf` descriptors are persisted.
Query result caching tracks the registry generation, so updating a registered
UDF takes effect on subsequent queries.

In client/server mode, remote `:q` and `:explain` requests use a server-safe
resolver. They can call built-in query functions, registered/installed UDFs, and
sandboxed `inter-fn` values supplied as query inputs, but they do not resolve
arbitrary fully qualified Clojure symbols, reflective dot forms, or raw function
values supplied by the client.

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
