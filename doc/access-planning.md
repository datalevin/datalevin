# Property-Aware Access Planning

## Goal

Treat ordered AVE scans, full-text search, vector search, and indexed document
search as physical implementations of logical query expressions. Access-aware
and conventional alternatives must:

- implement the same logical expression;
- be costed with the same residual joins and filters they execute;
- advertise only properties they actually provide; and
- execute through the normal plan contract.

Queries with no applicable access expression must stay on the existing planner
path without creating a property memo, sampling an access method, or expanding
the dynamic-programming search space.

## Invariants

### Logical equivalence

Every alternative stored under one memo key covers the same logical
expressions and produces the same required variables. An access alternative
records:

- stable identities for all expressions it covers, rather than a positional
  clause index;
- variables required before it can run;
- its output schema;
- residual expressions that remain to be planned; and
- any recheck needed to turn index candidates into logical matches.

Executing a plan normally must produce its complete logical result. Sampling
may return a bounded prefix. Demand-aware execution may stop before exhaustion
only when the plan supplies a proof that the unseen suffix cannot affect the
requested result.

### Physical properties

Properties describe operator output, not query demand:

- actual output ordering, including whether it is a full order or a prefix;
- whether the source is resumable;
- the kind of certified bound available for unseen candidates;
- duplicate and tie semantics;
- logical completeness and required rechecks; and
- result quality relative to the logical expression.

Properties are propagated through residual operators. Filters preserve
ordering; joins preserve it only when their implementation guarantees that
the outer order is retained; sorting and top-k operators create ordering.

There is no special rule allowing an incomplete plan merely because its
ordering equals the requested ordering.

### Demand and work controls

Root query demand is separate from access work controls.

Root demand includes required output variables, ordering, offset, limit, and
quality. Per-path execution controls include sample size, batch size, maximum
work, and continuation state. A cost estimate's candidate count is not used as
an implicit batch size.

Continuation and frontier data are opaque to the generic executor. Each access
method either supplies a certified comparison operation for a root demand or
declares that early stopping is unsupported.

### Cost correspondence

The cost attached to an alternative describes the plan that will execute:

- access startup and candidate production;
- candidate rechecks;
- residual predicates, functions, and joins;
- duplicate elimination and projection;
- ordering or top-k enforcement; and
- expected adaptive batches.

Sampling executes the same residual plan used by the alternative. It has a
hard work bound even when an order-key tie is larger than the sample. Sampling
confidence is explicit, and low-confidence estimates cannot win solely on an
optimistic point estimate.

Adaptive execution stops trying an access path when its observed work reaches
a candidate budget derived from result demand and sampled or estimated
residual yield. Planning samples have a separate shared budget derived from a
complete conventional alternative. Falling back must not discard an unbounded
amount of work.

## Planner shape

For a query with applicable access expressions:

1. Build the conventional plan once.
2. Derive root demand independently of any access method.
3. Discover access expressions and cheap, unsampled paths.
4. Seed access paths as alternatives for the logical expressions they cover.
5. Plan residual operators with the existing estimators and join machinery.
6. Retain non-dominated alternatives per logical key and relevant physical
   property signature.
7. Apply root enforcers such as sort/top-k.
8. Perform strictly bounded sampling only where it can change the choice.
9. Select and execute the chosen complete plan through the normal executor.

The access-aware branch may initially use a separate property-indexed table,
provided it reuses the existing base-plan, join, and cost functions. Ordinary
queries continue to use the existing tables unchanged.

## Bounded plan space

At each logical key retain only useful representatives:

- the cheapest complete unordered alternative;
- the cheapest alternative for each ordering prefix required by the query;
- resumable alternatives with a compatible certified bound; and
- distinct quality levels only when the query can accept them.

An alternative is removed when another has no greater cost and provides a
superset of its required physical properties.

## Current implementation

The first implementation establishes the common contract and migrates ordered
AVE limit/offset queries onto it:

- root demand, path properties, work controls, estimates, batches, and
  frontiers are separate values;
- `AccessStep` is a complete logical source during normal execution and a
  hard-bounded source during sampling;
- continuation and frontier certificates are opaque to the controller, with
  AVE owning its resume and bound comparison semantics;
- one root property memo compares executable conventional and access root
  plans, including root ordering/top-k enforcement;
- the access root carries its residual query and the already-planned
  conventional root used by adaptive fallback;
- conventional selection and adaptive fallback reuse that planned root rather
  than invoking the planner again;
- candidate work is bounded independently of batch size, using the required
  result count and residual yield rather than a fixed number of batches;
- access join subsets retain a Pareto frontier of ordered index-join and
  unordered hash-join variants, with explicit property transfer and dominance
  pruning; the bounded access-only subset DP compares different reachable join
  orders under the same logical-subset key;
- an access expression with `requires` is scheduled after an outer dependency
  closure produces those variables, and a correlated-capable method is opened
  once for each outer binding;
- bounded access samples retain their raw candidate prefix and opaque resume
  point, so selected execution evaluates the prefix and resumes after it
  instead of scanning it again; the access-production portion of marginal
  execution cost is charged only for unseen candidates; and
- explain distinguishes the recommended alternative from the alternative
  actually executed;
- when the conventional root provides a sound cost boundary, all access
  alternatives share that planning-sample budget; a catalog-count preflight
  rejects an indexed expansion that cannot fit before its cursor is opened;
- a terminal EAV expansion whose projected entity/value pair proves distinct
  output can be sampled by summing indexed fanouts instead of materializing its
  values, while preserving the weight of duplicate sample rows; and
- finite unordered relation queries use a separate adaptive-limit mode: the
  root retains offset plus limit as its required output count, residual
  filters and joins run per batch, and offset is applied only after enough
  distinct final rows survive;
- unsampled adaptive limits derive conservative residual yield from indexed
  join cardinalities, retain a bounded safety budget, and fall back to the
  already-planned conventional root when that budget is exhausted; and
- idoc access prepares candidate bitmaps lazily at execution, pages document
  references and rechecks across domains, and exposes opaque continuation
  state without claiming an ordering property.

The property-indexed subset table is created only for access-capable queries.
It carries alternative access fragments through the logical subsets covered by
their joins, while the ordinary Selinger tables remain unchanged. The root
memo still compares the resulting access roots against the conventional root,
and residual batches continue to use the conventional executor.

Planning samples the same residual logical expressions with the same primitive
scan/link adjustment formulas, but uses a bounded sampling evaluator instead
of retaining and replaying a residual physical plan. Prefix reuse therefore
reuses access candidates and continuation state; residual predicates are
evaluated normally during execution. Before materializing a reachable indexed
join, a preflight projects its output and cost from catalog counts. A terminal
join may instead count its output directly when projected keys prove that the
count represents distinct result rows. Correlated access currently uses
complete execution after its outer subset. A method must implement the
correlated-open contract before the planner will schedule it; adaptive
correlated top-k is not advertised.

## Migration stages

### 1. Contract tests

Add tests for complete `AccessStep` execution, hard-bounded sampling,
capability-safe demand satisfaction, residual-predicate costing, and
cost-bounded fallback.

### 2. Normalize the access model

Separate root demand, physical path properties, estimates, and work controls.
Replace `clause-idx`, `:entity`/`:order`, and AVE-shaped frontier fields with
stable logical coverage, an explicit output schema, and opaque continuation
and bound values.

### 3. Make alternatives executable

Make `AccessStep` drain to logical completion during normal execution. Build
each access alternative from the access source plus its actual residual steps
and enforcers. Remove the payload side channel from alternative selection.

### 4. Propagate properties

Preserve alternatives by logical key and property signature through the
access-aware join search. Encode property preservation on each physical
operator and use dominance pruning to bound the search.

### 5. Align estimation and execution

Use the residual physical plan for sampling and costing. Bound planning work,
reuse sampled prefixes when practical, and replace the fixed batch-count
fallback with a demand-and-yield-derived candidate budget. Bound speculative
planning separately by the complete conventional plan's cost.

### 6. Migrate AVE

Run ordered limit/offset through the generic access plan and adaptive
controller. Remove `ranked-batch-query`, AVE-specific frontier comparison, and
the separate top-k execution branch.

### 7. Add concrete methods

Add full-text, vector, and idoc access methods only after the common contract
passes:

- full-text exposes relevance ordering and its paging bound;
- vector declares ANN quality and whether increasing search effort is
  resumable or requires restart;
- idoc exposes candidate/recheck coverage and normally provides no ordering.

## Explain requirements

Explain output reports:

- logical coverage and residual expressions;
- actual provided properties;
- sample budget, observed work, and confidence;
- full alternative cost breakdown;
- recommended alternative; and
- executed alternative.

When explain deliberately executes the conventional plan for instrumentation,
it must not label an access recommendation as the executed plan.
