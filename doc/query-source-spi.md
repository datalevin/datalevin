# Query-source SPI

Status: initial F0–F3 workflow signed off; native performance fixes implemented,
with residual regression investigation open (2026-10-25)

Public `q`/`explain` and the companion mapping compiler/local adapter now use the
v2 contract in §10, with no version fallback. The earlier envelopes in §§2–9
are historical. See [current migration status](../spi-v2-cleanup.md#implementation-status)
for the original migration checks and the
[final signoff report](measurements/federation-signoff-2026-10-24.md) for the
current advertised-path scope, validation and native-performance verdict.
The [query regression sweep](measurements/query-regression-sweep-2026-10-24.md)
finds release-to-current regressions on larger workloads beyond the original
64-row gate. The [regression fixes and validation](measurements/query-regression-fixes-2026-10-25.md)
record the optimizations, complete follow-up matrix, final-build checks, and
remaining performance limits. TPC-H is excluded pending query validation.
Later preparation notes record the stages preceding activation.

Release placement: [roadmap source milestones](../roadmap.md#source-milestones); embedded JVM execution  
Public namespace: `datalevin.query.source`  
Implementation guide: [developing federation adapters](../../federation/doc/adapter-development.md)  
Optional contracts: [extensions appendix](query-source-extensions.md)  
Related plans: [companion federation design](../../federation/federation.md),
[analytics](../analytics.md)

This document specifies the boundary between Datalevin's query engine and a
separately developed source implementation. It makes the minimum contracts
concrete for the current external execution path. The
[four-shape revision target](#11-four-shape-revision-target) extends the common
model to native Datalevin, SQL, files/exports, and APIs before commitment.
[Section 10](#10-spi-v2-revised-four-shape-contract) specifies the revised v2
envelopes and negotiation; §§2–9 describe the earlier executable prototype.
This SPI is unreleased exploration. V1 and v2 name design iterations, not
supported releases: the revision replaces the prototype in place, with no v1
compatibility path, dual-version providers, or compatibility sign-off.
Core, companion adapters, and fixtures migrate together. F1 gates establish
correctness and performance; they do not freeze this exploratory API.

This is the authoritative core source contract. The companion plan specifies
provider behavior and delivery; it cannot add baseline capabilities here.
The separate [schema definitions contract](schema-definitions.md) specifies
metadata entries, schema snapshots, conditional updates, and fingerprint
vocabulary. [Derived-fact views](../analytics.md#7-views-as-derived-facts) have
their own contract and gate; direct mapped EAV queries do not require views.

All sources participate in the existing Datalog query/rule engine. The SPI adds
source-specific access paths, required bindings, capabilities, costs, and read
guarantees to its planner/executor. Core owns query meaning, join planning,
residual evaluation, rules, and result semantics. Adapters implement admitted
accesses and exact delegated fragments; they do not introduce a parallel
federation query engine. This boundary applies to every optimization phase,
including future probes and broader plan search. The prototype source-specific
execution branch in Sections 2–9 is an implementation baseline to integrate
into that architecture, not a second engine to evolve independently.

<a id="architectural-layers"></a>

The plans share these architectural layers:

| Layer | Responsibility |
| --- | --- |
| Logical facts | Entities, attributes, identity, types, cardinality, references, and absence. Native, mapped, and derived facts share this model. |
| Datalog and rules | Existing query syntax, joins, rule composition, aggregation, and result semantics. |
| Existing planner and executor | Preserve logical boundaries; select accesses; execute joins, residual checks, and supported rule evaluation. |
| Access contract | Required bindings, capabilities, costs, batches, read guarantees, completion, and resource limits. |
| Implementations | Native indexes, SQL, APIs, file readers, and optional delegated engines. |

**Governing invariant:** An access capability determines how an admitted EAV
operation can execute; it does not define a second query language or change the
operation's meaning.

This layering preserves the specialized native-only execution path under the
[native execution isolation contract](#13-native-execution-isolation-and-performance).

## 1. Project boundary

Datalevin proper owns:

- the public SPI and validation of its envelopes;
- Datalog semantics, source-clause collection, plan selection, native joins,
  residual evaluation, and supported rule expansion in the existing engine;
- query-owned sessions, cancellation, incremental consumption, resource
  accounting, and cleanup;
- external-query result-cache bypass and redacted explanation; and
- the independent [schema definitions contract](schema-definitions.md).

The sibling directory `../federation` hosts `datalevin-federation`, a separate
project with its own build, dependencies, tests, and releases. It owns every
production external source adapter, including the first application-owned local
relation adapter, APIs, files/exports, PostgreSQL, CSV/dataset integrations, and
optional Parquet/DataFusion. The companion also owns discovery,
mapping syntax and compilation, external entity-reference values,
SQL generation, connections, credentials, and provider-specific conversions.
Native Datalevin remains a built-in core source.

The dependency direction is `datalevin-federation -> datalevin`. Core neither
loads the companion automatically nor depends on its namespaces, drivers,
Arrow, or engine libraries. Core tests use a deterministic fake source in test
code; that fake is not a production adapter or a second federation project.
Provider integrations and their eager-materialization oracles live in the
separate project. Broader integration matrices may also use `../dtlvtest`.

The SPI is an in-process trusted extension boundary, not a provider sandbox or
a wire protocol. Its first implementation supports embedded JVM queries.
Remote server execution of adapters, transport of live sources, native-image
execution, and language-binding entry points are outside v1 scope and require
separate conformance gates. Embedded `d/q` and `d/explain` reject a live source
combined with a remote DB before query dispatch or provider acquisition; input
order does not select a different execution mode. This client-side guard is not
a server-side adapter or serialization contract. No external write or
distributed transaction API is introduced.

Only an explicit `:in` source binding implementing `IQuerySource` enters this
path. Existing Datalevin and collection sources keep their current contracts;
they are not automatically wrapped or reinterpreted as companion adapters.

### 1.1 Four-shape revision target

This section records the scope of the revision implemented in SPI v2.
Sections 2–9 preserve the earlier unreleased prototype for historical context.
Its unparameterized alternatives, query-wide consistency request and replay-based
one-shot restrictions have been replaced. The active envelopes and negotiation
rules are specified in [Section 10](#10-spi-v2-revised-four-shape-contract).

The federation interface is mapped entities and attributes, queried through
source-qualified EAV patterns. Native Datalevin supplies its existing entities
and attributes; companion adapters map external identity, fields, references,
and missing-value semantics into that interface. Core receives the accepted
attribute types, cardinality, entity/reference domains, and absence semantics
through the [v2 logical schema](#logical-schema-guarantees). Physical columns,
endpoint paths, conversion implementations, and supporting proofs remain
provider-owned. Each mapped attribute denotes a typed entity/value relation
with executable access paths. Keep physical
source identity and access scope, accepted mapping metadata, alternative access
paths, and the query-owned read context distinct. Accepted scope and conversion
semantics participate in the definition fingerprint; query input values remain
execution data.

Arbitrary tuple and collection inputs remain existing Datalog facilities.
Planning alternatives and cursor batches use tuples of variable bindings as
their execution representation, including wide results of fused EAV patterns.
Neither introduces a separate tuple-source federation model. V2 requires an
EAV pattern contract as specified in Section 10.1; the tuple source contract
in Sections 2–9 documents the earlier executable v1 baseline.
Providers describe required inputs, returned columns, exact clause coverage,
safe superset filters, and the cost and execution properties of each path.

| Shape | Access-path examples | Provider-specific implementation |
| --- | --- | --- |
| Native Datalevin | Indexed EAV lookup, range access, scan, native fragment | Existing native planner, indexes, read context, and full native query semantics. |
| SQL | Bound lookup, scan, filter/project/join fragment | Dialect translation, driver, transaction, and buffers. |
| Files/exports | Sequential read, supported partition/column pruning | Format reader, object version/manifest, decoding, and one-shot input ownership. |
| API | ID lookup, scoped listing, supported search or batch lookup | Endpoint/query translation, credentials, pagination, rate limits, and retries. |

No provider must support every access kind. Scans are optional; lookup-only
sources are valid. Pushdown capabilities are demonstrated by alternatives,
not inferred from a shape or advertised as mandatory remote relational work.
Physical discovery is optional companion I/O; declared schemas are valid.

Delivery starts with accepted EAV mappings, logical schema, and runtime entity
identity, then bounded provider access and native joins. Safe fusion and cost
improvements follow that working path; views and further improvements to the
same optimizer have their own gates. The [roadmap sequence](../roadmap.md#source-milestones)
and [optimization gate](../roadmap.md#federation-optimization-follow-up) separate
initial execution requirements from later improvements.

Required initial execution and conformance work under Section 10:

1. **Typed input dependencies and binding batches.** Promote parameterized
   access from the deferred bulk-binding proposal into the common baseline.
   Providers may still return only input-free alternatives. Core orders
   dependencies, rejects unsatisfied inputs and unseeded dependency cycles,
   deduplicates typed input keys without losing outer multiplicity, and
   associates returned rows with input IDs. Implement bounded cursor-open
   binding batches and request splitting;
   no provider batch endpoint is assumed. Unknown costs remain unknown, and
   per-key HTTP work is explicit and charged rather than hidden as one batch.
2. **Independent read guarantees.** Negotiate requirements per source and
   report actual consistency without silently weakening another source.
   Retain snapshot, statement, and best-effort distinctions; stable replay and
   exact pinned data versions are separate guarantees. Reissuing an API
   request is not identical replay. Default to one consumption group per session;
   use explicit groups only for independent resources needing separate open
   budgets or concurrency permissions. Reject reuse or spool under quotas.
   Acquisition may establish an authorized weak context without claiming a
   stable provider snapshot. Memoized observations do not create one.
3. **Scope and completion.** Accepted relation metadata fixes tenant,
   repository, partition, or other configured scope. For supplied bindings,
   successful exhaustion means completion of that accepted access under the
   reported consistency, not a global point-in-time snapshot. Pagination and
   continuation are provider-owned. Known result caps, omitted pages, and
   request-budget exhaustion fail ordinary Datalog execution; they cannot be
   reported as successful exhaustion. A capped/search-defined view may be a
   separately declared relation, never a silently incomplete base relation.
   Superset filters must preserve every matching row; residual checks cannot
   repair missing rows. Weak reads disable shortcuts requiring stable replay.
4. **Native participation.** The existing native planner and executor are
   the specialized implementation of the common relation/access contract.
   Activate the native planner bridge when composing mixed queries with
   external sources. Native-only queries retain their direct execution path,
   full syntax, indexes, caches, prepared paths, and representations, without
   federation planning or allocation of federation runtime state. Section 1.3 defines isolation
   and the mandatory native performance gate.
5. **Four-shape conformance.** Before F1 closes, compare scan and dependent
   access with eager evaluation across native, SQL, file/export, and API
   fixtures. Include empty/duplicate bindings, batch association and request
   splitting, absent required inputs, mixed consistency, independent one-shot
   paths, API truncation/errors, and cross-shape joins. Use real native
   execution and deterministic test providers; fixtures do not certify a real
   SQL driver or API integration. Existing local and fake suites remain gates.
6. **EAV access planning.** Integrate entity lookup, attribute enumeration,
   and optional value lookup with the existing planner. Start with single-clause
   accesses and deterministic structural selection, then join results in core.
   Keep logical attribute terms independent of fixed-attribute access paths:
   core specializes bound attribute variables and lowers feasible schema-domain
   enumeration under [§10.6](#attribute-term-lowering).
   Return advisory estimate envelopes under Section 1.2; all-unknown estimates
   are valid. Preserve required bindings, bounded batches, and hard limits.
   Safe fusion and comparable-cost ranking follow the bounded execution gate;
   broader search and repeated estimate rounds extend the existing planner
   under separate gates.
7. **Basic rule integration.** F1 requires native rules alongside external
   clauses and positive nonrecursive rules over mapped attributes, composed
   through the existing rule engine using ordinary `%` inputs. Preserve explicit
   source bindings, argument bindings, variable hygiene, and rule-result set
   semantics before caller joins/aggregates. Retain those boundaries as
   [logical planning regions](#logical-planning-regions), issuing a separate
   request for each source/region pair rather than collecting by source alone.
   Native rule outputs may feed
   dependent external access, and external bindings may constrain native rule
   calls. The [F1 rule checks](../roadmap.md#f1-execution-checks) require both
   core fake and companion local adapter evidence; direct conjunction coverage
   alone is insufficient. Rule-head aggregation and recursion over external
   sources have separate gates.

Logical types/equality, exact coverage and residual rules, metadata identity,
authorization, bounded results/work, cancellation, cleanup, and redacted
explanation remain shared. Request/concurrency limits and metrics already
exist in the baseline; reuse their ledger for API pages, retries, and splitting.
SQL ASTs, file formats, endpoint schemas, and discovery stay outside core.
Ordering, aggregate delegation, probes, and borrowed batches remain separate
capabilities. Production API and export delivery precedes PostgreSQL; release
placement and both owners' acceptance gates remain in the
[roadmap](../roadmap.md#source-milestones).

### 1.2 Capability and cost planning target

The first planning revision integrates mapped EAV accesses with Datalevin's
existing planner. Section 5 remains the executable v1 envelope;
[Section 10.6](#106-conditioned-planning-and-deterministic-selection) specifies
the planned v2 access and estimate fields. Native-only queries retain their
current optimizer and execution path. Mixed queries add external access choices
at the existing planning boundaries, without a second general optimizer.
Requests are scoped to [logical planning regions](#logical-planning-regions);
rule/branch predicates and coverage cannot leak across those boundaries.
The first useful release uses separate EAV accesses and structural selection.
The fusion and comparable-cost rules below specify improvements beyond those
F0–F3 exit requirements. Core now admits same-entity fusion under
`:eav-fusion-v1`; adapters must establish equivalence before advertising it.
Comparable-cost selection remains follow-up work.

| EAV choice | Required meaning |
|---|---|
| Entity lookup | Given an entity constant or required entity binding, return the matching facts for a declared attribute. Many-valued attributes may return several values. |
| Attribute enumeration | Enumerate the declared attribute's entity/value facts within the accepted scope. A scoped API may still require tenant or parent bindings. |
| Optional value lookup | Given a typed value constant or required value binding, find matching entities for a declared attribute. No inverse index or endpoint is assumed. |
| Safe same-entity fusion | Satisfy several clauses sharing an entity through one of those accesses when it is equivalent to their EAV join. Preserve identity, cardinality, reference types, and attribute existence. |

These are low-level access choices for fixed attributes, not restrictions on
logical EAV attribute terms. Core handles bound attribute variables and feasible
schema-domain enumeration under [§10.6](#attribute-term-lowering).
A provider may support only enumeration or only dependent lookup. The planner
validates typed required bindings, exact coverage/safe prefilters, accepted
scope, actual read guarantees, replay/consumption, completion, and hard limits
before considering cost. An access kind is a logical claim, not proof of an
index or a physical row layout. Fusion relies on accepted provider proofs under
the [companion fusion rules](../../federation/federation.md#61-fusion-prerequisites).
Single-valuedness alone does not prove co-location; missing attributes, `_`,
many-valued join products, and `:with` must retain their meaning.

Each alternative carries a small advisory estimate in the same planning
response; every numeric field may be unknown. The first release validates and
reports this envelope without requiring estimate-driven selection. The cost
follow-up applies it to the binding counts and batches considered by the
existing planner, including key deduplication and outer fan-out. Comparable
units are nanoseconds, logical rows/bytes, external requests, and transfer bytes.
Include provider request splitting and pagination in the estimate; unknown
work stays unknown. Confidence and provenance accompany estimates, and stale
statistics cannot become exact guarantees. There are no separate estimate
callbacks, repeated provider estimate rounds, or mandatory whole-plan latency
search in this baseline.

The existing planner owns join order, native access costing, residual work, and
materialization decisions. Initial external selection is structural. In the
cost follow-up, integrate external costs through a common-unit
conversion where supported; never add provider nanoseconds to unconverted
native optimizer scores. Missing conversions or estimates use the deterministic
EAV fallback in Section 10.6. Runtime quotas remain authoritative. An estimate
cannot prove emptiness, uniqueness, completeness, or memory fit, and a cheap
alternative cannot legalize unsafe fusion or an unseeded dependency cycle.

Planning uses accepted metadata and known scalar constants without source I/O
or consuming outer relations. Live counts, samples, and source EXPLAIN are
separately budgeted probes. General cross-entity delegation, richer conditioned
cost models, repeated estimate rounds, and broader plan search are optional
later extensions of the existing planner under the
[extensions appendix](query-source-extensions.md).
Native, SQL, file/export, and API are access shapes; OLTP/OLAP/hybrid labels
neither grant capabilities nor change a choice with identical evidence.

Initial conformance covers entity lookup, attribute enumeration, optional value
lookup, bounded native joins, all-unknown estimates, and deterministic structural
choices after response reordering. The follow-up adds fused/separate equivalence,
cost crossover cases, and unknown/stale/wrong-cost fallback. Explain distinguishes
rejected capabilities from the implemented selection policy and keeps actual
work separate from estimates.


### 1.3 Native execution isolation and performance

Native Datalevin is a specialized implementation of the shared semantics and
access contract. The contract does not require a common general-purpose
executor or public protocol calls inside native hot loops. Native-only queries,
including existing collection inputs and native-only rule expansion,
keep the native optimizer, executor, index operations, caches, prepared-query
specializations, and internal value representations.

Specialized access and execution paths belong to the same query/rule engine;
source adapters do not define independent join, rule, or aggregation semantics.
Select the access/execution specialization at the query or compiled-fragment
boundary.
Native-only work must not construct federation descriptors or estimate maps,
validate external envelopes, enumerate source alternatives, open extension
sessions/cursors, initialize federation ledgers, or copy rows into external
owned-batch representations. Shared correctness rules may reuse existing
specialized native code; they do not require new per-row/per-datom protocol
dispatch or accounting on the native-only path.

The entry-point guard gives native DB values precedence over `IQuerySource`,
including protocol extensions to the DB class. Adapters load the public
`datalevin.query.source` namespace when implementing its protocols; this installs
source detection without loading the executor. Core-only applications leave
that optional detection inactive. Direct implementations and late protocol
extensions are both supported. The executor resolves on the first external
invocation; native-only calls retain their route before and after that load.
The mixed planner uses the same native precedence. No adapter registration API
or per-row dispatch is added. Retain routing regression checks alongside the
existing supported-query and cache behavior checks.

For mixed queries, integrate mapped external EAV choices at the existing access
planning boundaries and invoke native operations directly within selected fragments.
Federation coordination, conversions, and required resource accounting belong
to that mixed-query execution and its boundaries. This does not exempt mixed
native work from the shared limits or promise zero coordination overhead.

Native-only performance is a release constraint. Any reproducible slowdown or
additional allocation attributable to federation blocks integration until
eliminated; faster external queries do not offset it. Benchmark the existing
input guards, prepared paths, cache hits, and startup as well as the proposed
bridge. Keep a pre-federation native control so an earlier dispatch cost is
not silently accepted as the baseline. Establish measurement noise and retain
raw results; passing semantic conformance alone is not performance evidence.
The [roadmap native performance gate](../roadmap.md#native-execution-performance-gate)
defines the required workloads and sign-off. These are implementation and
measurement requirements, not a claim of measured parity from this document.

## 2. Public protocols

The proposed minimum protocols are:

```clojure
(defprotocol IQuerySource
  (-source-info [source])
  (-open-session [source request])
  (-plan-source [source session request]))

(defprotocol IQuerySourceSession
  (-session-info [session])
  (-open-cursor [session request])
  (-cancel-session! [session reason])
  (-close-session! [session]))

(defprotocol IQuerySourceCursor
  (-next-batch! [cursor request])
  (-cursor-metrics [cursor])
  (-close-cursor! [cursor]))
```

The source is caller-owned; a query never closes it. The provider may expose
its own `AutoCloseable` source/profile API. Sessions and cursors are opaque,
query-owned runtime objects implementing these public protocols. Their
implementations must not require core optimizer records or namespaces marked
`^:no-doc`.

Except for these runtime objects, control/reservation tokens, and explicit plan
handles, envelopes contain
ordinary maps, vectors, sets, symbols, keywords, and portable logical values.
Descriptors, session information, and alternative metadata must be immutable,
including namespaced extension fields and Clojure metadata on their values.
Mutable leaves belong in owned row values, not these metadata envelopes.
Core never evaluates provider-supplied Clojure forms or examines a plan handle.
Core must not print runtime objects, handles, raw source clauses, bindings, or
rows in automatic diagnostics.

### Version negotiation and source information

`-source-info` is metadata-only: no discovery, source-row consumption, external
request, session acquisition, or authorization side effect. Its required fields
are illustrated below. All identifiers are non-secret portable values.

```clojure
{:spi-versions #{1}
 :source-instance-id #uuid "11111111-1111-4111-8111-111111111111"
 :compute-context-id "sales/read-profile-1"
 :definition-id :sales/orders
 :definition-fingerprint "federation-definition:v1:sha256:<hex>"
 :features #{}
 :required-features #{}
 :pattern-contract
 {:kind :tuple
  :columns [{:type :long :nullable? false}
            {:type :long :nullable? false}]}}
```

The definition identifies this logical view, including its accepted metadata;
the source-instance and compute-context IDs identify its physical read domain.
Aliases, mapping versions, and connection handles are not physical identity.
Repointing at an unrelated system changes the source-instance ID. Context IDs
must distinguish incompatible catalogs, authorization profiles, and snapshot
requirements without embedding credentials.

The earlier executable v1 baseline supports two pattern contracts:

- `:tuple`: positional columns; a pattern has exactly the declared arity.
- `:eav`: exactly three terms with a constant advertised attribute, as specified
  in the [EAV descriptor contract](#eav-descriptor-contract) below.

The revised federation interface requires `:eav` under
[Section 10.1](#101-revision-and-descriptor). V1's positional source
contract is retained here as historical design evidence. Ordinary Datalog
tuple inputs and the binding tuples exchanged during execution keep their own
roles under the revised contract.

Core selects the highest mutually supported SPI version and the supported
intersection of `:features` before opening a session. With no common version,
it raises `:query-source/version`. `:required-features` must be a subset of
that intersection or acquisition fails. Unknown advertised optional features
are ignored; unknown required features are rejected. Namespaced annotation
keys may be ignored and must never grant a capability. An absent optional
feature has no effect. Baseline v1 has no planning probes, borrowed batches,
aggregate delegation, ordering guarantee, or correlated execution.

Baseline sources advertise `:features #{}` and `:required-features #{}`.
Logical type names, `:tuple`/`:eav`, and baseline scan/filter/projection/join
support are not feature flags; types and pattern kinds appear in descriptors,
and exact operator support appears in planning alternatives. Optional feature
keywords and contracts are defined in the [extensions appendix](query-source-extensions.md);
additional keywords need a specified contract before use.

Protocol methods and envelopes may change together during exploration. Update
core, companion adapters, and fixtures for each change; an adapter for the
superseded prototype is not required. Any published compatibility policy will
be decided at release time.

`-source-info` returns a coherent immutable descriptor. Core captures it once
per source object for an invocation. Planning and execution must use that
descriptor's metadata or fail with `:query-source/stale-definition`; a provider
cannot silently replace it after refresh. Source-data changes between queries
are independent of metadata changes.

### Definition fingerprint

The provider computes `:definition-fingerprint` under the
[shared fingerprint contract](schema-definitions.md#3-revisions-fingerprints-and-data-versions),
covering the advertised `:pattern-contract`, normalized mapping/query or
equivalent definition, and its resolved dependencies. Ephemeral sources also
supply a definition fingerprint; object identity or a random token cannot replace it.

The [companion computation](../../federation/federation.md#44-fingerprint-computation)
produces the value used in its stored mapping and this envelope; there is no
separate SPI hash. Other providers own their versioned digest schemes. The
SPI requires a nonempty, non-secret string, compared as a whole for equality;
core does not hash opaque provider definitions or interpret digest internals.
Examples use `<hex>` as a placeholder for the actual digest.

Core passes the captured value unchanged to cursor opens. The provider checks
it against the definition used to plan and execute, failing with
`:query-source/stale-definition` on a mismatch. A successful refresh creates a
new accepted descriptor; it cannot update a running query's descriptor or make
an old handle valid under new metadata.

### EAV descriptor contract

An EAV source supplies the same required top-level `-source-info` fields as a
tuple source, with this `:pattern-contract`:

| Required field | Contract |
| --- | --- |
| `:kind` | Exactly `:eav`. |
| `:attributes` | A nonempty finite map keyed by every queryable attribute keyword, including any exposed identity attribute. |
| `:attributes A :entity-type` | The logical type descriptor of the entity term for attribute `A`. |
| `:attributes A :value-type` | The logical type descriptor of the value term for attribute `A`. |

Both types use [Section 3](#3-logical-values-and-clause-envelopes); they are type
descriptors, not column maps or physical type names. These are the only baseline
EAV fields in the earlier executable v1 contract; namespaced annotations may
be ignored. V1 does not advertise cardinality or reference classification beyond
the types, so core cannot infer them. The required
[v2 logical schema](#logical-schema-guarantees) supplies accepted cardinality,
entity/reference domains, and absence semantics alongside these types. Physical
expressions, uniqueness proofs, and conversion implementations stay with the
provider in both versions.

This example extends the companion's [normalized orders mapping](../../federation/federation.md#43-compilation-to-the-spi)
with a `:order/customer` reference. Its target is the same source's `:customer`
domain at epoch 0, with one `:long` key component. The `let` only constructs
ordinary data; `-source-info` returns the map with expanded type descriptors.

```clojure
(let [order-type
      {:kind :custom
       :name :datalevin.federation/entity-ref
       :version 1
       :fingerprint "entity-ref-v1-definition-hash"
       :parameters
       {:identity-domain-id [#uuid "11111111-1111-4111-8111-111111111111"
                             :order 0]
        :key-types [:long]}}
      customer-type
      {:kind :custom
       :name :datalevin.federation/entity-ref
       :version 1
       :fingerprint "entity-ref-v1-definition-hash"
       :parameters
       {:identity-domain-id [#uuid "11111111-1111-4111-8111-111111111111"
                             :customer 0]
        :key-types [:long]}}]
  {:spi-versions #{1}
   :source-instance-id #uuid "11111111-1111-4111-8111-111111111111"
   :compute-context-id "sales/read-profile-1"
   :definition-id :sales/orders
   :definition-fingerprint "federation-definition:v1:sha256:<hex>"
   :features #{}
   :required-features #{}
   :pattern-contract
   {:kind :eav
    :attributes {:order/id {:entity-type order-type :value-type :long}
                 :order/amount {:entity-type order-type :value-type :bigdec}
                 :order/customer {:entity-type order-type
                                  :value-type customer-type}}}})
```

`:order/customer` relates an order reference to a customer reference. Their
`:long` keys share a scalar type, but their nominal entity domains differ.

The custom type's name and parameter layout belong to the companion; the
fingerprint shown stands for its registered type-definition fingerprint, not a
mapping hash. Core validates the generic type envelope and compares type/domain
descriptors without interpreting those parameters. A domain keyword such as
`:order` alone is not an entity type descriptor.

Before session acquisition, core rejects malformed descriptors (including
missing type fields or invalid attribute keys) with `:query-source/invalid`.
An EAV pattern has exactly three terms. The earlier executable v1 prototype
requires the middle term to be `[:const A]` for an advertised attribute and
rejects variable/wildcard or unadvertised attributes with
`:query-source/unsupported` before acquisition. SPI v2 replaces that restriction with ordinary
[logical attribute terms](#attribute-terms-and-access-capabilities) and
[core access lowering](#attribute-term-lowering); a fixed-attribute provider
envelope does not restrict query syntax. Entity and value terms may be variables,
constants, or `[:ignore]`. Provider alternatives must declare the corresponding
entity/value types and `:nullable? false` for returned pattern variables; a
scalar natural key cannot replace the entity reference type.

## 3. Logical values and clause envelopes

The initial scalar type names are `:boolean`, `:long`, `:bigint`, `:double`,
`:float`, `:bigdec`, `:string`, `:keyword`, `:symbol`, `:uuid`, `:instant`, and
`:bytes`. `:data` admits portable values without a narrower type guarantee.
Use existing Datalevin value representations and equality/hash behavior, not
provider collation, numeric coercion, array identity chosen by an adapter, or
a new SPI comparison implementation. The shared core
[`QueryKey`](../src/java/datalevin/utl/QueryKey.java) implements key equality/hash
and predicate equivalence for both execution paths. Byte arrays compare by
content, recursively through supported collections; metadata does not affect
keys. Results still contain ordinary byte arrays. Custom records and opaque
values retain their own equality. Lossy conversion is an error. Precision,
timezone, or provider types without an exact representation require an
explicit companion conversion or rejection.

`:instant` uses `java.util.Date`. Immutable `java.time.Instant`, `Duration`, and
`Period` values use `:data`, preserving nanoseconds and period components without
conversion to dates or normalized periods. They also work inside supported
collections and row metadata; descriptor/session metadata remains immutable EDN.

Each column has `:type` and `:nullable?`. A fixed tuple type is
`{:kind :tuple :elements [...]}`, where `:elements` is an ordered vector of
logical type descriptors. A registered custom type is described by
`{:kind :custom :name :app/type :version 1 :fingerprint "..."}` with optional
plain-data `:parameters` refining its logical domain. The version and
fingerprint identify the existing custom-type definition; they do not replace
the database registry or redefine equality through an order key. Custom values
must satisfy the existing portable value contract. Core compares the declared
type/domain descriptors without interpreting companion-specific parameters.

Custom-type compatibility uses the complete accepted descriptor under Clojure
map equality (`=`): `:kind`, `:name`, `:version`, `:fingerprint`, `:parameters`,
and any accepted namespaced extra fields participate. Changing the version or
type-definition fingerprint is a different type even with unchanged parameters.
Unknown unqualified fields at the descriptor's top level are invalid.
Namespaced extras and parameter entries
are preserved and compared, never stripped as annotations; put descriptive
annotations outside the logical type map. Map entry order and Clojure metadata
do not affect equality. Descriptor hashing must agree with this equality;
Java map `hashCode` can distinguish equal numeric representations.

Mapped external entity references use the companion's registered custom value
and [nominal domain/key contract](../../federation/federation.md#51-entity-identity).
They are never native `:db.type/ref` values or numeric Datalevin entity IDs.
Joining an external natural key with a native attribute remains ordinary
scalar equality. A custom value used in native storage must be registered in
that database; a source cannot silently alter the registry during planning.
The reference contract and runtime values belong to the
[F0 foundation](../roadmap.md#f0-companion) and are used by the first revised
mapped provider in F1. Keyless exports may use
the companion's [snapshot-scoped row identities](../../federation/federation.md#snapshot-scoped-row-identity):
the typed key includes the export namespace and row locator. Reusing a row
offset in a different export never produces an equal reference. Domain/type
compatibility alone does not establish entity continuity between exports.

For tuple sources, nullable columns may contain `nil`, which is an explicit
value. EAV facts have non-`nil` entity and value terms; an absent/SQL-null
attribute contributes no tuple, including when its pattern uses `_` or projects
away the value. There is no SPI missing-value sentinel. Row identity is explicit
data: a mapped entity reference distinguishes records with equal field values;
earlier tuple sources may expose an identity column. Core does not invent stable
IDs or deduplicate away `:with` variables. Existing Datalog projection and
aggregation boundaries determine observable duplicate semantics.

Ordinary rules and computed bindings preserve their existing value/nullability
semantics. Derived views use the [fact contract](../analytics.md#72-types-nullability-and-absence):
internal binding descriptors track type and nullability, while published EAV
facts require non-null entity/value terms, accepted identity domains, attribute
types, and cardinality. Query outputs do not become attributes by their arity.
View definitions explicitly derive EAV facts and retain their validation
dependencies in fingerprints. This does not add a source model or gate direct
mapped EAV execution.

Core normalizes the admitted query and assigns query-local clause IDs before
source planning through the existing normalization/planning path. Supported
rule normalization retains invocation, branch, and projection boundaries during
clause collection. V2 uses [planning regions](#logical-planning-regions) to keep
each request within those boundaries, with IDs for distinct clause occurrences.
The IDs are opaque to providers and stable for that invocation. Terms use tagged vectors
so constants cannot be confused with variables or wildcards:

```clojure
{:id 0
 :kind :pattern
 :terms [[:var ?order-id] [:var ?amount]]}

{:id 1
 :kind :predicate
 :operator :>
 :semantics-version 1
 :args [[:var ?amount] [:const 50]]}
```

These are data examples, not evaluated forms. The pattern uses the declared
two-column tuple source above. `[:ignore]` represents `_`. For the EAV source,
`[$sales ?order :order/amount ?amount]` becomes:

```clojure
{:id 0
 :kind :pattern
 :terms [[:var ?order] [:const :order/amount] [:var ?amount]]}
```

Its output variables have the advertised entity-reference and `:bigdec` types,
respectively, both non-nullable even when the physical amount column is nullable.

Initially, eligible predicates are deterministic scalar `:=`, `:not=`, `:<`,
`:<=`, `:>`, and `:>=`, with their current Datalevin semantics. The semantics
version identifies the contract, not permission to use SQL's similarly named
operator. Exact support is per input type. Unsupported functions, recursion,
negation, or branch structures are not flattened into a positive conjunction.
They remain supported residual core work only when its resource and replay
requirements can be met; otherwise core rejects the query before source reads.

## 4. Query context and session lifecycle

Core establishes one query runtime containing the authorization context,
deadline, cancellation state, native read context, and shared resource ledger.
It validates syntax, definitions, source descriptors, types available from
those descriptors, and execution-mode support before acquiring source views.
Authorization and provider-specific validation occur again during session
acquisition and source planning; a reusable definition grants no access.

The common request context has:

```clojure
{:spi-version 1
 :features #{}
 :query-id #uuid "22222222-2222-4222-8222-222222222222"
 :authorization {:principal-id :application :policy-token "policy-v1"}
 :consistency :snapshot
 :control control
 :limits limits}
```

`control` is a core-owned runtime object. The public helpers in
`datalevin.query.source` are:

| Helper | Result/behavior |
| --- | --- |
| `(cancelled? control)` | Boolean cancellation state |
| `(remaining-time-ms control)` | Nonnegative time remaining, or `nil` for no deadline |
| `(check-active! control)` | `nil`, or throws cancellation/existing query-timeout error |
| `(register-cancel! control cancel-fn)` | Register a thread-safe cancellation signal function and return an opaque registration |
| `(unregister-cancel! registration)` | Idempotently remove a registration; return `nil` |
| `(reserve! control charges)` | Atomically charge a map of resource names to nonnegative amounts; return an opaque reservation token, or throw without charging any category |
| `(release! reservation)` | Idempotently release that token's live-resource charges; return `nil` |
| `(batch-charge-bytes columns rows)` | Conservative nonnegative retained-value/container charge, or an unsupported-value error |

Control, cancellation registrations, and reservation tokens are query-scoped
runtime objects, not portable logical values; they are never persisted or
rendered in diagnostics. Time
remaining uses the query's effective deadline, including inherited nested
timeouts. Reserve before allocation/work. Resource names are the limit keys in
Section 7, excluding per-batch caps checked on each batch. `:external-requests`
is cumulative and is not refunded by release; the other ledger categories
measure live use. A provider returning a batch transfers its reservation to
core through an opaque `:reservation` batch field; core releases it when that
batch is consumed or its retained state is independently accounted. Failed
operations release their own reservations before throwing.

Register cancellation before a blocking acquisition or operation, including
connection checkout and snapshot acquisition before a session exists. The
callback receives the redacted `{:error ... :phase ...}` cancellation reason
and signals backend cancellation without waiting for the blocked operation's
execution lock. Core dispatches callbacks from another execution context.
Concurrent registration and cancellation cannot lose a signal; registering
after cancellation dispatches immediately. Unregistration prevents future
dispatch but may race an already dispatched callback, so callbacks are
idempotent and safe during cleanup. Unregister on completion or acquisition
failure. Callbacks return promptly; failures do not replace the primary query
failure. Core clears remaining registrations during query cleanup. A provider
must reject acquisition that it cannot make deadline/cancellation-aware.

`-open-session` receives this context plus `:source-descriptors`, the complete
set of captured logical views that will share the session. It must authorize
their access, acquire a stable read view, and return an `IQuerySourceSession`.
If acquisition fails, the provider closes everything it acquired before
throwing. Core cannot close a session it never received.

Acquiring a one-shot input reserves its read view and ownership without
traversing it. If this cannot be guaranteed, reject snapshot mode. A selected
query-local spool may consume that input only after planning succeeds.
Snapshot copies of replayable mutable inputs are acquisition work and must
obey the same memory/spill limits.

`-session-info` is metadata-only and reports:

```clojure
{:spi-version 1
 :features #{}
 :session-id #uuid "33333333-3333-4333-8333-333333333333"
 :source-instance-id #uuid "11111111-1111-4111-8111-111111111111"
 :compute-context-id "sales/read-profile-1"
 :authorization-token "effective-policy-v1"
 :consistency :snapshot
 :acquired-at-ms 0
 :data-version nil}
```

The effective authorization token is a non-secret identity/fingerprint, not a
credential. Core shares a session only for matching source-instance,
compute-context, effective principal/policy, and consistency requirements, with
compatible declared logical views. Repeated source aliases do not acquire
independent snapshots. If the provider cannot serve the group in one read view,
the query fails instead of opening unrelated snapshots. Core may serialize
cursor work within that session; concurrent cursor support is not required.

`:snapshot` is the default. It covers every fragment, repeated scan, and bound
request for that source. Mutable local input needs a provider-guaranteed
snapshot or quota-bound copy/spool; retaining its object reference is
insufficient. The native Datalevin input must also have a query-consistent
read context. This does not promise a global snapshot across independent
systems. A lost view fails the query; retries cannot switch views midway.

Explicit `:statement` or `:best-effort` requests may be accepted and must be
reported as such. No silent downgrade is permitted. Weak views do not permit
correctness shortcuts based on previous reads or definitions requiring stable
replay. `:data-version`, if present, identifies the exact pinned data view within
its source/compute context under the provider's version contract. Use `nil` when
the provider cannot supply such a token, including weak modes without a pinned
view. Core assumes no ordering or cross-provider comparability. It is not a
schema/definition fingerprint, session ID, maintenance log position, or promise
of result-cache reuse.

Non-nil data-version tokens are non-secret portable identifiers suitable for
diagnostics, like definition IDs. Their semantic values may appear in explain;
private annotations belong outside those values. Clojure metadata on an
identifier is not part of its diagnostic representation.

Core completes acquisition and session metadata validation for every source
before planning any of them. The normal lifecycle is:

```text
capture descriptors -> validate -> acquire/authorize sessions
  -> plan each source -> validate/select plan -> open selected cursors
  -> consume owned batches incrementally -> close cursors -> close sessions
```

On failure, core signals cancellation, then closes every acquired cursor and
session in reverse ownership order. Closing a session also releases any
unclosed child cursors as a defensive fallback. Cleanup is idempotent; cleanup
failures are suppressed onto the primary failure. Session close releases its
read transaction and query state, never a caller-owned pool or source profile.
Cancellation may race with a blocking operation and must be thread-safe.
Other calls on a session/cursor are serialized by core; thread affinity is not
promised, and providers marshal calls internally when their backend needs it.
Core must observe deadlines/cancellation while a provider call is blocked and
signal registered callbacks and acquired sessions from another execution
context. Close/cancel methods return `nil`; repeated calls are harmless. Work
attempted after close raises `:query-source/invalid` with `:state :closed`.
Metadata and final cursor metrics
remain readable after close.

## 5. Planning request and alternatives

In the earlier flat-conjunction v1 baseline, core calls `-plan-source` once per
explicit source variable with all of its eligible clauses, using that source's
captured descriptor and shared session. V2 replaces this source-wide collection
with one request per source symbol and logical planning region under
[§10.6](#logical-planning-regions); §5's flat request is not a rule/branch contract.
Different variables may share a session while retaining separate planning
calls and clause identities. No per-pattern planning call or hidden planning
scan is permitted. Planning consumes accepted metadata and request constants;
baseline v1 cannot issue counts, samples, source `EXPLAIN`, or discovery I/O.

The request extends the common context:

```clojure
{:source-symbol $sales
 :source-descriptor source-info
 :clauses [pattern-clause predicate-clause]
 :bindings {?minimum 50}
 :required-vars #{?order-id ?amount}
 :demand {:quality :exact :ordering [] :offset 0 :limit nil}
 :max-alternatives 32}
```

`:bindings` contains known scalar values only, not source handles or an
unbounded outer relation. `:required-vars` includes join keys, `:with` inputs,
and residual dependencies, not just final output columns. Demand describes
the final query; it does not authorize a source-local limit or offset.

The response is `{:alternatives [...]}`. An alternative has:

```clojure
{:id :scan-1
 :plan opaque-plan-handle
 :covers #{0}
 :prefilters #{1}
 :requires []
 :columns [{:variable ?order-id :type :long :nullable? false}
           {:variable ?amount :type :long :nullable? false}]
 :properties {:replay :reopen
              :ordering []
              :blocking-state :none}
 :estimate {:rows 1000 :row-bytes 32 :startup-ms 1 :per-row-ns 100
            :requests 0 :confidence :low}
 :explain {:label "declared relation scan"}}
```

`:covers` contains clauses implemented exactly. `:prefilters` contains
additional predicates applied only as proven superset filters; these remain
core residual checks. The two sets are disjoint. Partial coverage of a pattern
does not count as exact coverage. A provider cannot remove a predicate merely
because an estimate suggests all rows satisfy it.

Core validates known clause IDs, unique alternative IDs and column variables,
types, supplied/required bindings, residual columns, finite nonnegative cost
values, capability claims, and disjoint coverage in the chosen plan. Unknown
cardinality, width, or cost components are `nil` with `:confidence :unknown`,
never optimistic zero. Estimates are advisory, including an estimated zero.
Provider claims are trusted code claims tested by conformance; structural
validation cannot prove a SQL translation sound. The revised baseline's EAV
access estimates, comparable units, and existing-planner integration are specified as work
in [Section 1.2](#12-capability-and-cost-planning-target); this existing envelope
does not establish that implementation.

A variable repeated in covered patterns has one output column with the common
logical type. Its `:nullable?` is true only when every covered occurrence permits
`nil`; column order cannot weaken that constraint.

Every required external pattern must be exactly covered once by selected
alternatives. Core may combine alternatives with disjoint coverage and local
joins when that plan is bounded. It never sends an uncovered source pattern
through collection lookup, drops it, or treats no alternatives as no rows. No
complete admissible plan raises `:query-source/unsupported` before opening an
execution cursor or consuming a one-shot input.

Rechecks run before distinct, limit, aggregation, or other lossy operations.
V1 does not delegate those operations or promise source ordering. Providers
preserve every required row/identity and residual column. In the earlier executable
baseline, `:requires` is empty; core chooses a bounded scan/local join or rejects the plan, never one
remote cursor per outer row. SPI v2 defines required inputs and cursor-open
binding batches in [Section 10.5](#105-dependent-access-and-binding-batches),
superseding the earlier [bulk-binding proposal](query-source-extensions.md#1-bulk-bindings).
This documentation change does not implement it; §§2–9 remain the earlier
baseline.

All v1 source alternatives and opaque handles are query-scoped. They must not
be persisted or retained in shared native plan caches, and cannot own open
cursors or resources requiring individual disposal. Query resources belong to
the session. Reusable provider plan descriptions and planning probes require
separately negotiated extensions; neither is necessary for v1 execution.
Core admits authorization, descriptors, session information, and every planning
envelope against the shared retained-memory quota before recursive validation.
Sizing and validation observe cancellation inside nested metadata. Charges include
extension fields, Clojure metadata, and hidden vector backing; explicit `:plan`
handles remain opaque. Core releases session-information charges after validation
and response-header/unselected-alternative charges after selection. Selected
envelopes remain charged through execution or explain. Providers account for the
state behind handles through their session.

## 6. Cursor requests and owned batches

`-open-cursor` receives exactly the seven [common context](#4-query-context-and-session-lifecycle)
fields (`:spi-version`, `:features`, `:query-id`, `:authorization`, `:consistency`,
`:control`, `:limits`) plus these four fields in baseline v1:

```clojure
{:alternative-id :scan-1
 :plan opaque-plan-handle
 :source-symbol $sales
 :definition-fingerprint "<captured fingerprint>"}
```

Core forwards the captured context unchanged and the selected handle by identity.
The fingerprint is the selected source's captured definition fingerprint.
Acquisition/planning fields (`:source-descriptors`, `:source-descriptor`,
`:clauses`, `:bindings`, `:required-vars`, `:demand`, `:max-alternatives`) are
absent. During planning the provider associates required clauses, bindings,
and other execution state with the session-owned handle; opening it does not
replan or supply new bindings. Additional execution inputs require a specified,
negotiated extension.

Open uses the session's existing view and returns an `IQuerySourceCursor`.
Failed open cleans up partially acquired cursor resources. Opening unselected
candidates for speculative execution is not part of v1.

`-next-batch!` receives `{:control control :max-rows n :max-bytes b}`. These are
the remaining permitted batch sizes, not hints. Calls pull one batch at a time;
there is no background unbounded queue. The response is:

```clojure
{:columns [{:variable ?order-id :type :long :nullable? false}
           {:variable ?amount :type :long :nullable? false}]
 :rows [[101 75] [102 90]]
 :row-count 2
 :byte-count batch-byte-charge
 :reservation batch-reservation
 :exhausted? false}
```

The columns exactly match the selected alternative, in order. Rows are owned,
fixed-arity vectors of logical values. Counts must match the rows and obey the
requested limits. `batch-byte-charge` is the charge computed for these rows;
`batch-reservation` is a live token covering at least that many retained bytes
and one in-flight batch on this query's ledger. Core validates the token and
accepts its ownership once. A provider never releases a transferred token from
cursor/session close. For an empty final batch the token may be omitted when
its computed charge is zero; row metadata or backing storage still needs a
reservation.
The final batch may contain rows with `:exhausted? true`.
An empty non-final batch is invalid; the provider continues its bounded read
internally or throws on cancellation/limit. `nil` is not an exhaustion marker.
After exhaustion, core closes the cursor and makes no further batch call.

V1 transfers ownership of batch values to core. They remain valid after the
next advance and cursor/session close. Providers copy reusable buffers and
mutable byte arrays before returning them and do not mutate returned values.
Arrow memory, JDBC buffers, lazy sequences, and foreign pointers never cross
as logical values. Borrowed batches are deferred to a negotiated extension;
there is no release-batch protocol in v1.

`:byte-count` is a conservative charge for retained logical values and batch
containers, not wire bytes or a claim about total JVM heap usage. Core supplies
a public `batch-charge-bytes` helper and validates the charge; unknown/custom
value sizes require an explicit conservative charge or rejection. Core checks
row and advertised byte limits before traversing values, then recomputes the
charge incrementally under the byte cap with cancellation checks, including
nested values and metadata. An understated count cannot bypass the cap; the
reported `:attempted` charge may be the first subtotal exceeding it. The accounting
must include retained backing storage, not just a slice's visible length.
The shared helper charges a Clojure `subvec`'s entire parent, including hidden
elements and parent metadata, wherever the view occurs in rows or columns.
Shared backing is conservatively counted per occurrence. `owned-copy` detaches
vector views; its `copy-charge` companion sizes that detached result before
allocation. Providers report the helper's charge for the actual handed-off
values, even if they reserved more before copying. Core input and constant
capture use the detached charge; retained join and result values keep their
backing charge.
Oversized individual values fail with `:query-source/limit` rather than bypass
the batch cap. Final defaults and type-size accounting must be fixed in the F1
implementation and tested with large strings, arrays, and custom values.

`:replay :reopen` permits a new cursor for the same plan in the same session
with identical logical rows; ordering is still unspecified. For the baseline,
selecting `:replay :once` permits only one source cursor for that session across
all alternatives and aliases. Core rejects a plan requiring additional reads,
or explicitly spools once under quotas and reports that spool. A provider that
supports more independent reads can expose a replayable session snapshot.
SPI v2 keeps one session-wide consumption group by default and permits explicit
[consumption groups](#104-consumption-groups-and-replay) only for independent
resources needing separate consumption permissions, as specified in Section 10.4.
No implicit sampling or replay is allowed in either version.

`-cursor-metrics` is non-blocking and returns cumulative `:batches`, `:rows`,
`:charged-bytes`, `:read-rows`, `:read-bytes`, `:external-requests`, and
`:elapsed-ns`, including final counts after close. Unknown physical read work
is `nil`, not zero. Session setup is charged to the query ledger separately
and is not attributed to each cursor. Metrics are observations, not a second
result channel.

## 7. Resource limits, early stop, and cancellation

For queries using this SPI, the proposed query-map option is `:query-source`:

```clojure
{:query-source
 {:consistency :snapshot
  :limits {:batch-rows 4096
           :batch-bytes 1048576
           :in-flight-batches 2
           :retained-bytes 67108864
           :spill-bytes 1073741824
           :result-rows 1000000
           :result-bytes 67108864
           :external-requests 1024
           :concurrent-requests 4}}}
```

These numbers are proposed initial defaults, subject to F1 measurements; the
field meanings and enforcement are the contract. All limits are finite
nonnegative integers, except positive batch/concurrency sizes. Zero spill means
spilling is disabled. `:timeout` retains its existing query meaning and is not
a source-local timer. Native-only query behavior is unchanged.

Limits apply to the complete expanded query, including native join state and
residual operators on the external path, not separately per provider,
alternative, cursor, or nested query. A nested query shares the ledger and may
tighten limits but cannot reset spent work or extend its parent's deadline.
Source-owned in-flight work, batches retained by core, blocking state, spools,
and final-result materialization are accounted separately without silently
omitting one category. Allocations transfer reservations as ownership moves;
the same live batch is not charged twice to one category.

An external request is a provider/backend operation such as executing a
statement or fetching another result page, including session setup I/O.
Providers reserve request/concurrency allowance before issuing it and report
actual work. Independent lower backend limits may split requests, consuming
the shared allowance. Physical rows examined by an external engine may be
unknown and are not confused with rows transferred or retained memory.

Core implements incremental consumers for non-blocking operations. Blocking
hash builds, distinct, grouping, and sorting reserve state and spill under the
disk quota where supported, or fail explicitly. A cursor does not make existing
full-result collectors bounded. Unsupported operator/replay shapes are rejected
before consuming a one-shot cursor. Bad estimates cannot disable runtime limits.

`d/q` completes query-source execution and materializes its result before
returning. In the initial F1 executor, a relation find without a return map
returns a vector of row vectors when it contains an aggregate or a nonempty
`:with`, including `[]` when no result rows remain. These vectors preserve
native result values and multiplicities under `=`, but differ from native
`d/q`'s lazy sequences for those queries; concrete collection-type parity is
not guaranteed. Plain relation finds return a spillable set, and relation
finds with `:keys`, `:strs`, or `:syms` return a set of maps, including when
combined with aggregates or `:with`.

A result limit is an overflow error, never silent truncation. A query `:limit`
is a semantic operator applied after required joins and residual checks;
reaching a safe final limit stops source reads and closes cursors promptly.
A result-size quota alone cannot authorize early success.

Providers check `control` before opening/fetching and during long decoding or
computation. `-cancel-session!` signals all outstanding work and must unblock
pending I/O using the backend's cancellation/close mechanism. It is safe to
repeat, including after close. It must not wait for the cancelled operation's
own execution lock. Its `reason` is a redacted map containing `:error` and
`:phase`, never the raw primary exception. Cancellation, quota overflow,
snapshot loss, timeout, and provider errors fail the whole query; already
received rows are never returned as an unmarked partial success.
Core keeps a session's cancellation registration active until its close call
finishes, then unregisters it even if close throws. Closing may wait for pending
provider work, so cancellation must also unblock that wait. Completed core
cleanup entries drop their resource and runtime references; repeated close
calls remain idempotent and cleanup errors preserve the primary failure.

## 8. Caching, errors, and explanation

Any expanded dependency on an `IQuerySource` bypasses query-result caching
before lookup and insertion, including rules, derived views, and nested
queries. Source object identity, mapping revisions, metadata fingerprints, and
session IDs are not data-freshness tokens. Source authorization is performed
on every invocation. Initially, shared caching of external plans, rows, and
planning evidence is also disabled; native-only caches retain their current
behavior.

Errors use `ex-info` with a stable `:error` category and redacted structural
context: `:query-source/version`, `:query-source/unsupported`,
`:query-source/invalid`, `:query-source/unauthorized`,
`:query-source/stale-definition`, `:query-source/snapshot-lost`,
`:query-source/limit`, `:query-source/cancelled`, and `:query-source/provider`.
Timeout preserves the existing `:query/timeout` classification. Add `:phase`,
`:query-id`, `:source-symbol`, and applicable `:clause-ids` and `:alternative-id`.
The embedded executor uses zero-based `:where` clause indices. Context comes
from the query and accepted plan, never from provider exception data; an
alternative ID is included once an alternative is selected. Shared-session
acquisition and cleanup identify the alias used to acquire the session.
Limit errors also name the category, maximum, and attempted charge. Native provider
exceptions and SQL can contain secrets; automatic diagnostics must not render
their messages/causes without provider redaction. Cleanup errors do not replace
the original category.

`explain` without execution may acquire/authorize a session and plan, and must
close it before returning. It opens no execution cursor; session-acquisition
work is identified separately. The source executor honors `:run?` (default
`false`) and `:intermediate-counts?` (default `true`). With execution enabled,
the latter controls per-row counting and the `:streamed-rows` field; disabling
it retains final result sizes and provider cursor metrics. Plan-only explain
and ordinary `q` calls do not collect intermediate counts. Nil explain options
mean the default plan-only request. Unknown source explain options reject with
`:query-source/unsupported` in phase `:explain` before contacting a provider.
Execution explanation reports selected clause
coverage, residual checks, metadata versions, actual consistency, blocking
operators/spools, estimates, actual rows/bytes/batches/requests, and limit
failures. An allowlisted provider `:label` may be shown; opaque plans, SQL,
credentials, raw bindings, rows, and source values are never printed. No-query-
source queries allocate no extension sessions, ledgers, or planning state.

Each `:sources` entry reports the validated session's `:consistency`. Its
`:selected` entries report exact `:covers` and `:prefilters` as clause-ID sets
and include a public `:alternative-id` and the validated `:estimate` envelope
from §5, preserving unknown values
and confidence separately from actual `:cursors` metrics. Selected-plan display
containers are quota-accounted and omit provider extension fields and Clojure
metadata, including when metadata printing is enabled.

The earlier embedded baseline reports the following execution details:

- `:acquisition {:sessions n}` counts acquired sessions separately from execution
  cursors, including in plan-only explain. Shared aliases count as one session.
  Session setup work is not inferred from or added to cursor metrics.
- `:operators` describes planned core placement: relational `:inputs` by input
  index, ordered `:readers` with source/clause/alternative IDs and `:retained?`,
  shared-variable join indexes, distinct on find/with variables, optional
  grouping, and retained results. Its `:storage :memory`, `:spill-policy :reject`,
  and `:replay-spools :none` describe the current executor policy. Selected
  provider blocking/replay properties remain under `:sources`.
- Executed output adds `:actual-operators`: input row counts, retained-reader
  row counts in planned order (a prefix when an empty table stops execution),
  indexes and cross products actually built, distinct rows admitted, and
  actual `:spools` and `:spill-bytes` (both zero in this implementation).
  An empty stream builds no join indexes. Plan-only output omits these counters.
- Each `:cursors` entry identifies its `:source`, `:alternative-id`, and
  `:clause-ids`, and contains the seven allowlisted metrics from §6. Unknown
  metrics remain `nil`; unknown fields and Clojure metadata are omitted.
  Rows/bytes count batches delivered by that cursor, including unused rows in
  the last batch after an early limit. They are not final result sizes.
- `:result-rows` counts retained output tuples after aggregation and the final
  window; `:result-bytes` is their conservative result-ledger charge, including
  container overhead, not serialized bytes. Both remain available with
  intermediate counts disabled. Plan-only output omits result sizes and cursor
  metrics; the existing `:native-scans 0` records that no native scans occurred.

Core display containers and copied identifiers are admitted against the query
budget. A failed execution throws the redacted error, including applicable
limit details; it does not return a partial successful explanation. Primary
and suppressed cleanup errors strip provider messages, causes, annotations,
and extension fields while preserving public structural context.

The same entry identifies the captured logical view with `:definition-id` and
`:definition-fingerprint`, and separately reports the acquired session's
`:data-version` (`nil` when unavailable). These values come from the accepted
descriptor and session metadata, never a later provider refresh. Displayed
identifiers are copied under the query budget, detached from collection backing,
and stripped of Clojure metadata recursively. Schema revisions and definition
update counters are not baseline SPI fields and are not inferred from these
values or copied from provider annotations; their roles follow the
[shared version vocabulary](schema-definitions.md#3-revisions-fingerprints-and-data-versions).

The top-level `:residual-checks` vector describes the predicates core evaluates
on joined rows, in query clause order:

```clojure
{:clause-id 3 :operator :> :source-symbols [$orders $limits]
 :stage :before-projection}
```

`:source-symbols` lists the symbols of sources whose patterns bind the predicate's
variables, including native sources, sorted by symbol. It is empty when no source
pattern binds the predicate's variables, for example predicates over only query
inputs or constants. Core currently rechecks every predicate, including exact
provider coverage and superset prefilters. `:before-projection` places
these checks before core projection, aggregation, and final limit/offset.
The same structural description appears in plan-only and executed explain;
it contains no operands, bindings, or predicate values.

## 9. Source conformance

Acceptance follows the [milestone checklist](../roadmap.md#source-milestones);
its [F1/core row](../roadmap.md#f1-core) defines the shared execution suite.

The [public namespace](../src/datalevin/query/source.clj) implements the three
protocols, descriptor validation, and control/accounting helpers. The
[test-only fake and driver](../test-src/datalevin/query/source/) exercise these
without loading the query engine or a provider. Run the isolated F0 suite with:

```sh
clojure -M:test -m datalevin.query.source.runner
```

The F0 driver eagerly retains small fixtures. The separate
[public source driver](../src/datalevin/query/source/driver.clj) now accepts explicit
source inputs in `d/q` and `d/explain`, bypassing native result caches. Run its
focused integration suite with:

```sh
cd ../dtlvtest
lein run -m datalevin.query.source.execution-runner
```

The same runner includes the v2
[mapped migration matrix](../test-src/datalevin/query/source/mapped_conformance.clj)
and [EAV fixture](../test-src/datalevin/query/source/fake.clj). This matrix
uses the real internal `source.driver/run` and an adapter constructor, with eager
EAV `d/q` as its oracle; it supplies no alternate engine or resolver. It covers
scan/entity/value paths, thirteen scalar families, absent mapped facts, ordinary
input tuples, correlated keys/IDs and fan-out, many-valued joins, native/mapped
rules, branch scopes, result shapes/maps, pinned versions and owned mutable
leaves after close. Structural explanation checks reverse provider candidate
order and separate provider metrics from optional engine counts. Failure checks
include malformed IDs/completion, one-shot reuse and oversized single lookup
keys. The fixture's modeled setup/page requests charge the shared backend ledger.
The active fake, companion local adapter and public route use these v2 checks.
The companion also runs the deterministic SQLite/file/HTTP transport matrix in
[`transport-conformance`](../test-src/datalevin/query/source/transport_conformance.clj).
Production provider gates and native performance sign-off remain separate.

Within v2 compilation, an independently ready native/local caller producer can
seed a mapped rule or union body. A charged core-only dependency walk includes
ready scalar steps and observes cancellation before each transition. Scope
steps then require those actual caller columns, so clause ordering cannot lose
a legal lookup dependency. Possible mapped outputs and sibling/private scope
exports are excluded from this proof. No provider read or estimate supplies a
missing seed, and cyclic/self-supplied dependencies still reject before open.

Run [native isolation checks](../test-src/datalevin/query/native_isolation_runner.clj)
in separate JVMs; they deliberately install protocol extensions and check both
public-SPI/core load orders:

```sh
clojure -M:test -m datalevin.query.native-isolation-runner
clojure -M:test -e '(require (quote datalevin.query.source)) (require (quote datalevin.query.native-isolation-runner)) (datalevin.query.native-isolation-runner/-main)'
```

These checks cover direct/extended sources, native DB precedence, native syntax,
caches and prepared reads, lazy executor loading, and rejection of native-image
source execution before lazy resolution. The last check uses the JVM execution
mode property; it does not certify a native-image build. The separate
[measurement script](../script/query_source_native_gate.clj) compares native
entry points with a reconstructed control that removes source routing. It
records raw timings, thread allocations, loading, GC, and the control source;
it supplements the [performance gate](../roadmap.md#native-execution-performance-gate)
and does not replace its historical control or complete workload requirements.

The [shared execution matrix](../test-src/datalevin/query/source/conformance.clj)
is test support, separate from the runtime SPI. It accepts an adapter constructor
and a row-replacement function. Core runs it against the fake; the companion
runs the same checks against its local adapter with `clojure -M:test:local`.
Deterministic seeds and batch sizes exercise all thirteen baseline scalar types,
tuple/EAV queries, aliases, native joins, input bindings, grouping, `:with`,
nulls, bytes, and numeric keys.
Comparisons preserve key distinctions and multiplicities instead of using
Clojure set equality. Explicit expected results also check the eager oracle.
Repeated tuple variables exercise every nullability combination across two or
three columns. Test nominal types cover scalar/composite references, reference
attributes, missing facts, domain/epoch mismatches, and natural-key joins to
native data through both fused and separate source fragments.
Reference variants also pin version, fingerprint, name, and accepted extra-field
identity, equivalent reordered/metadata-decorated descriptors, hash-based
distinctness, and rejection before acquisition. These JVM fixtures do not establish
the runtime reference ordering or wire encoding required by the revised
[F1 companion gate](../roadmap.md#f1-companion).
Public protocol wrappers check pinned views under replacement, acquisition of
all views before planning, and snapshot/cancellation isolation between concurrent
invocations. Cursor-open checks pin the exact baseline request keys, captured
context, and selected handle identity through query and explain, including
aliases and scalar bindings; the isolated F0 harness checks the same envelope.
They also cover plan-only explain, acquisition/planning/open/fetch
faults, limits, early stop, and resource release. Blocking wrappers exercise
cancellation before and after session/cursor/batch handoff, after partial fetch
progress, and during cursor/session close. Explicit cancellation and real nested
deadlines run through query, executed explain, and applicable plan-only paths,
with and without aliases. They check callback dispatch off the query thread,
error location, close order, released reservations, and a successful fresh query
using the same source. Adapter-specific fault and capability tests remain necessary.

The matrix's `explanation!` checks plan-only and executed output for estimates
and actual consistency. It covers fused/separate plans and shared aliases,
unknown/partial/numeric estimates, zero estimates with real rows and quota
failures, invalid estimates, mismatched or unsupported consistency, snapshot
loss, fresh reuse, and redaction of private annotations under metadata printing.
Its `coverage-explanation!` checks selected coverage and core residuals across
fused/separate plans, cross-source and native joins, input-only predicates, and
deliberately permissive prefilters. Eager comparisons and explicit results pin
filtering before aggregation and limit/offset; plan-only output opens no cursor,
and predicate constants, bindings, and private metadata stay out of diagnostics.
The `metadata-explanation!` checks publish row-only, definition, and counter-only
refreshes during acquisition, planning, and fetching. Real fake/local sessions
keep their captured versions across aliases and fragments; later invocations
see the new versions. They also cover unavailable data tokens, nested private
metadata, retained version charges after releasing the session envelope, and
oversized-version rejection before planning.

`execution-explanation!` reconciles reported rows, batches, and charged bytes
with real adapter events across aliases/fragments, empty inputs/results, joins,
grouping, and early limits. It checks planned versus allocated operators and
final sizes with intermediate counting disabled. `diagnostic-boundary!` checks
batch, retained-state, result, and request limits; unknown/invalid metrics;
and primary/suppressed failures. It injects private metadata, labels, opaque
handles, row values, and exception causes and checks metadata-aware redaction
through both adapters. These checks run as part of the same lifecycle suite.

The [integrated failure regressions](../../dtlvtest/test/datalevin/query/source_failure_test.clj)
run with `lein test datalevin.query.source-failure-test` from `../dtlvtest`.
They exercise invalid descriptors/plans/batches, one-shot selection, partial
opens, lost views, late cancellation, and cleanup error preservation through
`d/q` and `d/explain`, checking that query resources are released.
They also reject mutable descriptor/session/plan metadata before the next
provider operation and check metadata quotas across acquisition, planning, and
aliases, including plan-only explain. Oversized authorization, session envelopes,
unselected alternatives, and hidden backing reject before later values are walked;
transient charges are released before execution. Explicit plan handles stay opaque.
Batch validation
regressions cover early envelope rejection, understated byte counts, cancellation
inside nested values and metadata, and cleanup after partial progress.
Core row-sizing regressions also interrupt scratch, distinct/grouping, and output
charges inside nested values, and reject insufficient scratch space before an
unneeded traversal. The [runtime tests](../../dtlvtest/test/datalevin/query/source_runtime_test.clj)
check exact scaled charges, result quotas, and arithmetic overflow.
Validation cases cancel inside captured input values and constant metadata,
including native type checks and planning requests/responses. Cancellation keeps
its category and originating source/clause. Executable fake alternatives exercise
backtracking, overlapping/incomplete coverage, and one-shot restrictions through
both query and explain. Cases include equivalent failed paths before a valid
fallback, predicate-sensitive coverage, residual checks before aggregation/windows,
shared aliases, and later-fragment failure/cancellation. Search workspace is
released before cursor execution; every case checks query cleanup.
Close-phase cases keep cancellation active through session shutdown, including
shared aliases, plan-only explain, and nested deadlines. The
[core cleanup tests](../../dtlvtest/test/datalevin/query/source_runtime_test.clj) check
reference release, concurrent/reentrant close, cleanup order, and redaction.

The [explain-option regressions](../../dtlvtest/test/datalevin/query/source_explain_test.clj)
run with `lein test datalevin.query.source-explain-test` from `../dtlvtest`.
They cover counting defaults and opt-out, plan-only execution, invalid options,
and native/source option parity.

The [multi-row batch regressions](../../dtlvtest/test/datalevin/query/source_batch_test.clj)
run with `lein test datalevin.query.source-batch-test` from `../dtlvtest`.
They use the F0 fake's multi-row cursor to check row/byte boundaries, reservation
transfer and release, early stop, and rejection of oversized batches,
independently of the companion local adapter.

The companion's [batch and memory regressions](../../federation/local/test/datalevin/federation/local_batch_test.clj)
check snapshot charges including outer-vector metadata, early rejection during
nested sizing, and cancellation while sizing snapshots/plans/cursor scratch
and copying batch values. Sizing uses the request's byte cap; the final atomic
reservation enforces the shared ledger. A partial batch leaves its next row
unconsumed, and a failed copy releases its reservation. The adapter retains one
reservation per output batch.

The [input-binding regressions](../../dtlvtest/test/datalevin/query/source_input_test.clj)
compare tuple, collection, and relation inputs with eager queries, including
duplicates/`:with`, nulls, native joins, mutable iterators, quotas, cancellation,
and cleanup before provider acquisition.

The [constant-ownership regressions](../../dtlvtest/test/datalevin/query/source_constant_test.clj)
mutate caller-owned dates and nested value metadata during input iteration and
provider callbacks. They check fused/residual predicates, native joins, aliases,
fresh invocations, quota rejection before provider calls, redacted plan-only
explain, and cleanup.

The [capture regressions](../../dtlvtest/test/datalevin/query/source_capture_test.clj)
exercise incremental sizing and cancellation inside constants, flat inputs,
and nested metadata. They check early rejection before oversized container
children, cancellation during copying, shared-ledger changes, size overflow,
and release of partial progress before provider callbacks, including plan-only
explain.

The failure and input-binding suites also cover retained vector views: hidden
parents cannot bypass batch/result limits, backing traversal observes
cancellation, and copied constants/inputs neither inspect nor retain hidden
parent values. Empty row vectors with metadata or backing storage need a live
reservation even when they contribute no rows.

The [join-index regressions](../../dtlvtest/test/datalevin/query/source_join_test.clj)
run with `lein test datalevin.query.source-join-test` from `../dtlvtest`.
They cover shared and composite keys, collisions, duplicates, nulls, cross
products, native joins, and cleanup; equality-call counts detect quadratic
probes without timing assertions. The focused core suite also checks index
reservation failures and cancellation during construction.
The [native key-equality regressions](../../dtlvtest/test/datalevin/query_key_equality_test.clj)
check decimal scale and tuple keys across bounded entity/value/full scans,
domain filtering, reference aliases, and eager input relations.

The [aggregate regressions](../../dtlvtest/test/datalevin/query/source_aggregate_test.clj)
compare native reductions and check cancellation, arithmetic workspace, and
result quotas. They cover decimal scale/precision expansion, wide integers,
ratios, independent groups, native/source joins, and cleanup on every exit.

The [EAV execution regressions](../../dtlvtest/test/datalevin/query/source_eav_test.clj)
compare fused plans and separate core-joined fragments with eager facts through
`d/q`. They cover nominal entity domains and epochs, reference-valued attributes,
scalar/composite natural-key joins to native data, typed inputs, missing values,
duplicates/`:with`, and pinned aliases. Small graphs exhaust reference/name
presence combinations. Invalid domains, attributes, batches, and nullable EAV
columns check rejection and cleanup; native type errors retain their source
symbol and clause IDs.

The [empty-join and window regressions](../../dtlvtest/test/datalevin/query/source_short_circuit_test.clj)
verify that an empty required join table stops subsequent native scans and
source cursors, including giant-value decoding and shared-session fragments.
They check empty result shapes, cleanup, planning/cancellation failures, result
overflow, and dynamic limit/offset validation against eager native evaluation.

The [aggregate cancellation regressions](../../dtlvtest/test/datalevin/query/source_aggregate_test.clj)
check cancellation, interruption, and deadlines after cursor exhaustion,
during grouping and aggregate reduction, including multiple aggregate passes.
They also check cleanup, result quotas, and eager parity for numeric aggregates,
null grouping keys, `:with`, and result windows.

The [decimal projection regressions](../../dtlvtest/test/datalevin/decimal_projection_test.clj)
compare native, eager, and source results for scalar and tuple keys, aggregates,
`:with`, and windows. They also check memory/disk result-set iteration, key
collisions, mutations, and the [spill codec's compatible payloads](nippy.md#values-and-existing-extensions).

The [native tuple regressions](../../dtlvtest/test/datalevin/query/source_tuple_test.clj)
run with `lein test datalevin.query.source-tuple-test` from `../dtlvtest`.
They compare mixed joins and aggregates with eager results for inline/giant
tuples, inferred composite keys, native reference components, and missing
components, and check decoding reservations on success and failure.

The [native existence regressions](../../dtlvtest/test/datalevin/query/source_existence_test.clj)
check explicit value wildcards against byte, custom, and generic attributes,
including cardinality-many facts, missing attributes, bound entities, and
`:with`. They verify small-budget scans over giant values, skipped custom
callbacks, pinned views across concurrent writes, early stop, and cleanup.

The [native entity regressions](../../dtlvtest/test/datalevin/query/source_entity_test.clj)
check literal and scalar/tuple/collection/relation-bound IDs under small memory
budgets, unindexed attributes, cardinality-many facts, missing entities/attributes,
pinned aliases, explain, and cleanup. They also cover source-provided IDs,
reference chains, null candidates, duplicate/correlated rows, and early stop
between entity ranges. Native list and streaming readers
also return empty for unknown attributes and refresh after schema creation.

The [attribute-scan regressions](../../dtlvtest/test/datalevin/query/source_attribute_test.clj)
count real native cursor entries to verify selective AVE scans and bounded EAV
duplicates. They cover unindexed attributes, missing attributes, giant prefix
collisions, attribute-ID boundaries, early stop, cancellation, and cleanup.
Value-seek cases also check scalar constant/input equality, signed zero, NaN,
decimal/tuple fallbacks, and reservation before bound encoding.

The [native custom payload regressions](../../dtlvtest/test/datalevin/query/source_custom_test.clj)
cover registered types using portable Nippy data: colliding order keys, typed
inputs, unindexed/many-valued attributes, compressed and cached payloads, quota
failures, pinned aliases, ownership, and cleanup. They also check reads after
reopening without the order runtime and rejection without calling custom codecs.

The [variable tuple regressions](../../dtlvtest/test/datalevin/query/source_variable_tuple_test.clj)
run with `lein test datalevin.query.source-variable-tuple-test` from `../dtlvtest`.
They cover varying arities, inline unsigned counts/lengths, compressed giants,
native references, owned mutable components, and reservation cleanup.

The [native data regressions](../../dtlvtest/test/datalevin/query/source_data_test.clj)
run with `lein test datalevin.query.source-data-test` from `../dtlvtest`.
They cover structured values and untyped composite keys, compressed giant
values, cached expansion, and cleanup after quota or unsupported-value errors.
The [portable data regressions](../../dtlvtest/test/datalevin/query/source_portable_data_test.clj)
add characters, ratios, smaller JVM integers, and default sorted collections,
including eager join/aggregate parity and malformed-value cleanup.
The [Java time regressions](../../dtlvtest/test/datalevin/query/source_time_test.clj)
cover nanosecond precision, extreme values, constant/flat inputs, predicates,
aggregates, compressed nested data, quotas, and cleanup after malformed values.

The [legacy compression regressions](../../dtlvtest/test/datalevin/query/source_compression_test.clj)
run with `lein test datalevin.query.source-compression-test` from `../dtlvtest`.
They cover LZ4, Snappy, Zstd, and LZMA2 inline and giant payloads, nested storage
compression, expansion/dictionary limits, cached collections, and cleanup after
partial scans.

The [typed metadata regressions](../../dtlvtest/test/datalevin/query/source_metadata_test.clj)
run with `lein test datalevin.query.source-metadata-test` from `../dtlvtest`.
They cover symbol/tuple metadata, eager join and aggregate parity, legacy
compression, cached expansion limits, partial scans, and reservation cleanup.

The initial path supports positive patterns, the baseline scalar predicates,
scalar and flat tuple/collection/relation input bindings, projection, `:with`,
count/sum/avg/min/max, and final limit/offset. It pins native views and groups
aliases into one source session.
Earlier fragments become quota-bound join tables; the final fragment streams
through joins and residual predicates. On its first row, core builds hash
indexes for shared variables using the native engine's key wrappers. Index
containers and growth are reserved before allocation; keys borrow already
charged table values. Tables without shared variables remain cross products.
An empty required table stops construction of later tables and skips the final
stream. Complete source planning still precedes execution.
Probes clear their scratch references before releasing the streamed batch,
and query cleanup clears indexes. Blocking state fails at its memory
limit; this implementation does not spill, even with a positive disk allowance.
Core row scratch, distinct/grouping state, and result retention check cancellation
and remaining quota incrementally while sizing, including any charge multiplier.
Output checks result-row limits before traversal and result-byte limits during
it; the final reservation atomically checks all applicable ledger categories.
Explain reports the structural and quantitative diagnostics specified in
[§8](#8-caching-errors-and-explanation), including captured public version
identifiers, without provider labels, handles, bindings, or rows.

For example, `:in $s [?id ...]` and `:in $s [[?id ?label]]` become owned,
quota-bound local join tables. Core checks every input value against the native
and external pattern types before provider acquisition. Only scalar values,
including values destructured from a single tuple, enter SPI `:bindings`;
collection/relation inputs do not request a cursor per input row. Capture
checks cancellation within nested values as well as between rows. Sizing checks
the live shared retained-memory budget incrementally before visiting container
children; a final atomic reservation covers the charge before copying mutable
leaves. Copying also checks cancellation, including inside metadata.
Subsequent type and planning validation also checks cancellation within these
values and tuple components; cancellation is not converted into a type error.
The planner validates each response once, then checks cancellation while
searching for complete, nonoverlapping coverage under the replay constraints.
It memoizes failed coverage states, including covered predicates, under the
retained-memory quota and releases that workspace after selection. Equivalent
failed paths do not consume additional search attempts. The current embedded
planner permits 4096 distinct states before rejecting excessive complexity with
`:query-source/unsupported` in `:planning`.
Pattern and predicate constants are also captured as owned values, including
metadata, with their retained charge reserved before copying. This capture
precedes input iteration and all provider callbacks; each invocation captures
the caller's current values afresh.
Empty input relations open no execution cursors. Flat tuples accept `_` and
ignore trailing values, matching native bindings; nested destructuring remains
unsupported.
Dynamic `:limit` and `:offset` variables must be direct scalar `:in` bindings,
with positive and nonnegative integer values respectively, bounded by
`Long/MAX_VALUE`. The literal negative-limit convention remains unlimited;
negative dynamic limits reject, matching native execution.

Custom values implement
[`IQuerySourceValue`](../src/datalevin/query/source/value.clj) to supply their
complete nominal descriptor and conservative retained size. This opt-in does
not install a storage codec or alter equality.
The shared `owned-copy` helper in that namespace preserves collection/symbol
metadata and default sorted collection behavior, and detaches mutable leaves,
including leaves inside metadata. Custom comparators require the immutable
custom-value contract; ordinary collection copying does not retain comparator
closures. The fake source and local adapter use it after reserving their batch
charges.

Source joins and projection distinctness use the shared native query keys;
nonbinary values retain Java equality and scalar predicates use the native
engine's built-ins. Decimal scale is significant for
keys, including scalar and tuple input bindings. Native result sets preserve
those keys during iteration and spilling, so projected `1.0M` and `1.00M`
remain distinct inputs to aggregation on both execution paths.
Return maps (`:keys`, `:strs`, `:syms`) preserve the same distinctions.
Native indexed input probes compare decoded stored values before dropping
bound columns; storage normalization, such as floating zero, does not replace
the join's key comparison. Native/eager comparisons use the values actually
stored when a storage encoding normalizes its input.
`sum` and `avg` preflight each group's numeric values and reserve conservative
workspace for wide arithmetic before invoking the native reducer. The bound
includes decimal scale alignment (including the initial integer zero), requested
`*math-context*` precision, and ratio denominator growth. It uses stored numeric
sizes, without expanding decimal powers or strings during preflight. Scratch is
released after each reducer; wide results receive a retained charge before that
release and remain charged through query completion. Cancellation is checked
during preflight and again between reduction inputs. Ordinary numeric semantics
and errors are preserved within the budget; `count`, `min`, and `max` do not
reserve sum/average workspace.

Byte content equality applies to scalar/tuple keys and nested portable data,
including map keys and set elements. Unordered values retain entry/element
multiplicity when distinct host array objects contain the same bytes. Query
key indexes borrow already owned values instead of making canonical copies.
Temporary indexes used to compare unordered values reserve workspace before
allocation, check cancellation during traversal, and release it on every exit.
Native inline and Nippy giant decoding accept bytes under the existing bounds,
including compressed payloads and metadata. Public predicate equality preserves
Clojure numeric equivalence; key equality preserves decimal scale.
Native generic values containing bytes use attribute-bounded content scans
(or entity/attribute scans when the entity is bound), because serialization
order cannot safely bound logical equality. Typed scalar indexes remain usable.
Source tuple descriptors may include byte components; native tuple schemas keep
their existing component restrictions.

The [byte execution regressions](../../dtlvtest/test/datalevin/query/source_bytes_test.clj)
compare eager and source execution across copies, batch boundaries, flat inputs,
constants, native joins, EAV facts, aggregation, and spilled result membership.
The [shared key tests](../test/datalevin/query_key_test.clj) cover hash collisions,
unordered multiplicity, numeric/custom equality, scratch quotas, cancellation,
and restoration of the enclosing query budget. The companion local adapter
uses the same shared comparisons and tests owned byte query results.

In the earlier v1 baseline, unsupported execution shapes reject explicitly:
prepared execution, function/rule/branch/negation clauses, ordering, pull, and
nested input destructuring. V2 replaces that rule rejection for the
[required basic rule cases](../roadmap.md#f1-execution-checks), including positive
nonrecursive mapped rules and native/local recursive rule participation.
Baseline mixed native queries require platform threads, a local DB with
neither a pending transaction overlay nor an active write transaction, and
supported scalar, typed tuple, generic data, or portable registered Nippy values
wherever the query binds or compares a value.

The [execution-context regressions](../../dtlvtest/test/datalevin/query/source_context_test.clj)
cover remote DBs against a live server, pending native overlays, active writers,
and (on JDK 21+) virtual threads through `d/q`, plan-only explain, and executed
explain, with both source input orders. Each rejects with
`:query-source/unsupported`, phase `:native-view`, and the native source/clause
location before acquiring a provider session. Ordinary native operations remain
usable after rejection. Source-only embedded queries can run on virtual threads.
The native-image `:execution-mode` guard is not an end-to-end native-image
conformance claim; that mode remains outside v1 as specified in §1.

An explicit native value wildcard such as `[?e :document/body _]` uses a
quota-bound scan of entity/attribute pairs without reading giant payloads,
decoding values, or invoking custom codecs. It can therefore test existence on
byte, custom, and otherwise unsupported generic attributes. Each matching
fact contributes a pair, preserving cardinality-many behavior until ordinary
projection/aggregation. A named value variable still requires the supported
decoding contract, even when that variable is not returned.

Native scans with an unbound entity use the indexed attribute's AVE prefix,
visiting only its datoms. Bound entities use EAV with both the entity key and
attribute duplicate range restricted. A `:db/noindex` pattern requires a bound
entity, including when it appears inside a rule or union. A bound value or value
predicate cannot make it a starting access. If no clause or query input supplies
the entity, planning rejects the query before opening providers. Missing
attributes open no cursors. Prefix bounds use the pinned schema's attribute ID
without invoking value codecs, and include all giant/custom variants.

Native constants and scalar/flat-tuple input bindings also restrict value
candidates for booleans, longs/references, floats/doubles except NaN, UUIDs,
instants, strings, keywords, and symbols. Bounds use AVE keys or EAV duplicates,
with a reservation before encoding. Unrelated values never enter the decoder;
giants sharing the selected truncated prefix still do. Core retains logical
equality checks for those collisions and Java input-key semantics such as signed
zero. Other value types keep the attribute/entity scan; no user codec or generic
serialization is invoked to construct a bound.

Native entity constants and IDs supplied by scalar, flat tuple, collection, or
relation `:in` bindings restrict EAV cursor ranges before payload reads or
decoding. Collection/relation inputs use a quota-bound set of distinct IDs;
their original rows still join locally, preserving duplicates and correlations
with other columns. This also applies to value wildcards and unindexed
attributes. Unrelated entities cannot consume the decoding budget or trigger
unsupported-value errors; selected values retain their normal type checks and
quotas. Early stop prevents opening subsequent ranges, and executed explain
counts each opened range in `:native-scans`.

When a source fragment supplies an otherwise unbound native entity variable
and the native value has no constant or scalar-input constraint,
the executor retains the source rows under quota before reading that native
pattern. It propagates this order through native reference chains. Each such
native reader borrows candidate IDs from a retained relation, deduplicates them
under quota, and visits only those entity ranges in the pinned view. Null and
non-Long candidates cannot match native entity keys and open no ranges. The
original rows still participate in core joins, preserving correlations and
`:with`; other join predicates remain core checks. Explicit entity bindings and
constant/scalar value constraints keep their earlier native reads, so an empty
native filter can still skip source execution. This is bounded local join
ordering, with no correlated provider calls or new SPI capability. The source
build side must fit the retained-state budget; the final native reader streams
to the result window and stops before later ranges once it is satisfied.

Fixed tuple attributes use `:db/valueType :db.type/tuple` with `:db/tupleTypes`,
including types inferred for composite attributes. Their 2–255 scalar components
map positionally to the SPI tuple descriptor; native reference components map
to `:long`. Missing native components remain `nil`; external rows must still
satisfy their advertised element types.

Homogeneous tuples declared with `:db/tupleType` support 1–255 non-null scalar
components and use the SPI `:data` descriptor, since their arity varies. Joins
require an external `:data` column; the native schema still validates component
types and input bindings. Native reference components remain ordinary entity
IDs represented as `:long`. This adds no variable-arity SPI tuple descriptor.

Untyped attributes and untyped composites use the SPI `:data` descriptor; joins
require an external `:data` column. Generic data supports JVM integers (including
`Byte` and `Short`), characters, ratios, the scalar representations above,
the Java time values from [Section 3](#3-logical-values-and-clause-envelopes),
and nested vectors, maps/sets with hashing or the default sorted
comparator, and lists, including bounded portable metadata. Preflight limits
nesting to 64 levels and caches to 256 entries; repeated cached subtrees count
toward the decoded-value charge at every occurrence. Ratio component lengths
and sorted collection counts are checked before allocation. Malformed values,
including zero ratio denominators or incomparable sorted keys, report
`:query-source/invalid`. Other JVM objects and custom readers remain unsupported.

The supported Java time values use fixed-size Nippy layouts. Preflight validates
their payload lengths;
constructor range/overflow failures report `:query-source/invalid`. Native and
source execution compare the decoded logical values. Native inequality scans
on generic data retain predicates because serialized order does not implement
logical comparison, including for negative instants and durations.

Registered native types with the default `:payload :nippy` use that same portable
data contract and advertise the logical value as `:data`; their order-key type
does not determine the payload's logical type. Joins therefore require an
external `:data` column, and equality compares payload values, including when
their order keys collide. The pinned view supplies both accepted type metadata
and payloads. Reads validate stored references without copying order keys, then
reserve buffers, decompression, and layout-derived decoding space through row
handoff. They do not invoke registered order functions or user codecs. Types
with user-defined payload deserializers reject before source acquisition;
Nippy custom readers and nonportable payload values reject before decoding.

Inline and giant values reserve decoding memory before allocation, including
tuple containers. Giant reads bound stored bytes and Zstd expansion, then
validate tags, collection counts, tuple arity, and bounded caches before
invoking readers. Generic collections and giant homogeneous tuples receive an
additional layout-derived reservation before decoding, held through row handoff.
Stored metadata on symbols, fixed tuples, homogeneous tuples, and their symbol
components uses the same bounded preflight and additional reservation, including
cached occurrences. Metadata does not relax declared component types or permit
custom readers. Nippy giants preserve metadata; typed inline indexes continue
to omit it, matching existing native storage behavior.
Preflight checks cancellation during large traversals. Raw Nippy and legacy
uncompressed, LZ4, Snappy, Zstd, and LZMA2 envelopes are supported. Legacy
expansion and decoder scratch are reserved before copying or decompressing the
payload, including when nested inside the storage Zstd envelope; the expansion
reservation lasts through row handoff. Zstd accepts one frame with a declared
output size and no external dictionary. LZMA2 accepts one XZ stream; its
dictionary and decoder arrays are charged before allocation and released when
returned, including after partial construction failures. Concatenated frames or
streams and trailing bytes reject.
Reading arbitrary custom payloads, encryption, and custom envelopes still
rejects before value decoding; explicit value wildcards avoid
that read.
Combining one-shot fragments within a session rejects unless a complete
reopenable alternative is available.

The earlier v1 baseline is superseded. The revised v2
[F1 gates](../roadmap.md#f1-execution-checks), four-shape fixture matrix and
native routing gate passed in the [coordinated cleanup](../spi-v2-cleanup.md#implementation-status).
The roadmap records subsequent provider and integrated-workflow signoff.
Control construction and plan/batch validation remain internal implementation
details.

## 10. SPI v2: revised four-shape contract

Status: active public core/local contract. The [cleanup status](../spi-v2-cleanup.md#implementation-status)
and [resource audit](query-source-resource-audit.md) record implementation and
regression evidence; the roadmap owns performance sign-off. This section defines
SPI revision `2`, replacing the unreleased
prototype documented in Sections 2–9. It reuses the protocol method signatures,
logical values, clause coverage, ownership, authorization, cancellation, quota ledger, and
error/redaction rules unless explicitly replaced below. Unknown unqualified
fields are invalid; namespaced annotations grant no capability. Native-only
queries retain §1.3's direct execution and performance requirements.

### 10.1 Revision and descriptor

`-source-info` keeps the required top-level fields from §2. Its
`:pattern-contract` must be `{:kind :eav :attributes {...}}` under the
EAV descriptor contract: every exposed attribute declares its entity and value
types, including nominal reference types where applicable. The accepted mapping
defines entity identity, attribute names, conversions, and missing-value
semantics. The required top-level `:logical-schema` exposes the accepted
logical guarantees defined below. Core consumes that compiled schema; mapping
compilation and the physical evidence supporting its guarantees remain
companion-owned.

Revised federation planning admits only EAV source descriptors. A tuple source
must acquire an accepted entity/attribute mapping before joining this model;
a `:tuple` pattern contract raises `:query-source/unsupported` before acquisition. Existing Datalog
tuple/collection inputs remain valid without an `IQuerySource` wrapper. Logical
tuple types within keys or attribute values, required-input tuples, and returned
binding tuples remain supported; they do not change the source pattern contract.

A revised source uses one descriptor with these additional immutable fields:

```clojure
{:spi-versions #{2}
 ;; Other required §2 identity and fingerprint fields remain.
 ;; :pattern-contract is the compiled EAV descriptor described above.
 :features #{}
 :required-features #{}
 :logical-schema logical-schema
 :read-capabilities #{:snapshot :statement :best-effort}
 :scope {:id "scope:tenant-a" :kind :restricted
         :parameters {:tenant :tenant-a}}
 :completion {:domain :finite :extent :all-matches}}
```

All fields shown are required. There is one top-level pair of feature sets.
Dependent access and the initial execution contracts in this section are
required, not optional feature flags. `:bulk-bindings` is obsolete. Section
10.6 also specifies follow-up fusion and cost improvements; §10.7 separates
their gates from initial v2 conformance.

The revised core and all providers use revision `2`: descriptors advertise
`:spi-versions #{2}`, and sessions and planning/open contexts carry
`:spi-version 2`. This marker detects mismatched implementations; it does not
select a compatibility mode. There is no per-query version selector, mixed-version
execution, fallback to v1, or translation of old envelopes. Replace the core
implementation, companion adapters, and shared fixtures together.

Core captures descriptors and forms physical session groups (§10.3). Negotiated
features are the intersection of core support and every member's feature set;
every member's required features must be a subset. Validate all aliases before
acquisition: an unsupported revision raises `:query-source/version`, a missing
or malformed required field raises `:query-source/invalid`, and an unsupported
required feature raises `:query-source/unsupported`. Sessions report the exact
revision and negotiated features from their captured context.

A scan-only provider may use empty `:requires`; a lookup-only provider need not
offer a scan. Both implement the same descriptor, read guarantees, access
properties, and cost contract. Existing semantic and resource tests should be
ported to this contract; tests that only preserve obsolete envelopes are removed.

#### Attribute terms and access capabilities

The logical EAV pattern is `[entity attribute value]`. Its attribute term may
be a constant, a bound or unbound variable, or `_`, under ordinary Datalog
binding and equality rules. The captured `:attributes` map is the complete
exposed attribute domain for this logical source, including exposed identity
attributes. It describes facts and their types, not which access operations the
provider must implement. Unmapped physical fields are outside this domain.

Core uses that finite domain and valid region-local constraints to plan an
attribute term. A keyword absent from the captured domain has no matching
facts; it is not a declared attribute with missing access capability. An empty
candidate domain yields an empty pattern result. These proofs require no source
data read or physical discovery, but do not bypass normal source authorization
and validation. Malformed descriptor keys/type metadata still fail validation.

A declared attribute without a complete usable access path is different: core
must find another legal plan or report `:query-source/unsupported`. It cannot
assume that attribute has no facts. A provider may offer only fixed-attribute
lookups or scans; core performs [attribute-term lowering](#attribute-term-lowering)
when those operations suffice. This replaces the prototype's syntactic rejection
of variable/wildcard attributes without requiring providers to accept those
terms in their low-level access envelope.

#### Logical schema guarantees

`:logical-schema` is a required immutable top-level descriptor map with
exactly the unqualified keys `:domains` and `:attributes`. The pattern contract
continues to supply entity/value types; this envelope adds logical guarantees
without requiring core to parse a provider's mapping language. For the orders
example in §2, `order-type` and `customer-type` are its complete custom type
descriptors, and `order-domain` and `customer-domain` are their accepted nominal
domain IDs:

```clojure
{:domains {order-domain {:entity-type order-type}
           customer-domain {:entity-type customer-type}}
 :attributes
 {:order/id {:entity-domain order-domain
             :cardinality :one :absence :no-fact :reference-domain nil}
  :order/amount {:entity-domain order-domain
                 :cardinality :one :absence :no-fact :reference-domain nil}
  :order/customer {:entity-domain order-domain
                   :cardinality :one :absence :no-fact
                   :reference-domain customer-domain}}}
```

Every field shown for a domain or attribute is required. Namespaced annotations
remain inert. `:domains` is a nonempty finite map keyed by non-nil, non-secret,
immutable portable IDs; core compares those IDs without interpreting their
provider-specific structure. Each value supplies a complete logical
`:entity-type`. IDs must distinguish unrelated identity domains; sharing an ID
across sources requires an explicitly accepted common identity contract. The
companion may use its accepted `[source-instance-id domain identity-epoch]` IDs.
Domain entries may include a
reference target whose attributes this source does not expose.

`:attributes` has exactly the same keys as the EAV pattern contract:

| Field | Accepted logical guarantee |
|---|---|
| `:entity-domain` | An ID in `:domains` identifying the attribute's entity domain. That domain's `:entity-type` must equal the pattern's `:entity-type`. |
| `:cardinality` | `:one`, `:many`, or `:unknown`. `:one` permits zero or one distinct logical value per entity; `:many` permits zero or more; `:unknown` supplies no bound. Distinctness uses query-key equality. |
| `:absence` | Exactly `:no-fact` in v2: an absent value contributes no EAV fact, including for `_` and unprojected values. Returned entity/value terms remain non-nullable. |
| `:reference-domain` | `nil` for an attribute without a declared reference guarantee, otherwise an ID in `:domains`. Its `:entity-type` must equal the attribute's `:value-type`; it identifies the target domain of returned references. |

Domain compatibility uses the opaque ID together with its complete entity type,
including the type's version, fingerprint, and parameters under §3. Equal IDs
with incompatible types do not license shared-domain reasoning. Matching scalar
key types or attribute names cannot establish a shared domain. A declared
reference does not promise target existence, inverse uniqueness, or referential
integrity. Ordinary scalar-key joins keep their existing equality semantics.

These guarantees apply to the captured logical definition and accepted scope
under the acquired read context. `:one` is an at-most-one value guarantee; it
does not imply attribute presence, value-to-entity uniqueness, a physical row
count, or permission to change `:with` multiplicity. Aliases or mappings with
matching domains do not automatically share the same population or read view.
A combined mapped source must establish guarantees for its combined attribute
relations: two individually single-valued mappings can introduce conflicting
values for the same entity, so their union cannot simply inherit `:one`.

Basic scalar/reference `:many` mappings are part of the first revised mapped
provider. Their [mapping semantics](../../federation/federation.md#52-references-and-cardinality)
expose distinct non-null EAV facts: repeated collection elements do not create
extra facts, while different entity references remain distinct even with equal
attribute values. Empty or absent collections contribute no facts. Two independent
many-valued patterns on one entity form the ordinary EAV join product, including
two patterns of the same attribute; shared variables and predicates constrain
that product normally. Fusion must preserve this behavior and `:with` semantics.
Array order, occurrence positions, and nested-object identity require additional
mapping rules and are not inferred from `:many`.

Core may use the accepted guarantees for reference-domain propagation,
single-valued access/fan-out reasoning, join planning, and validation of
semantic rewrites. It must retain an attribute's existence test when no separate
exact proof establishes presence. Such a proof may come from validated clause
coverage; cardinality, estimates, and reference declarations cannot supply it.
Rewrites comparing multiple observations must satisfy the read/replay contract;
one value per observation does not make separate weak reads identical.
Failed or incomplete requests cannot be interpreted as absent attributes.

The native bridge projects native attribute types/cardinality, native reference
domains, and no-fact absence semantics into the same planning vocabulary for
mixed queries. Native domains are qualified by database identity; this metadata
does not change native entity-ID representations or equality. Native-only
queries retain §1.3's direct path without allocating federation descriptors.

Core validates complete attribute coverage, enum values, domain references, and
type agreement before acquisition; violations raise `:query-source/invalid`.
It checks structure and compatibility, while providers establish and enforce
the claimed guarantees using accepted constraints, declared policies, or
validation. Physical columns, endpoint paths, conversion code, and proof
artifacts stay provider-owned. A known violated guarantee fails execution with
`:query-source/invalid`; it cannot silently become an estimate or be weakened
mid-query. Metadata validation does not prove the provider's physical data sound.

The complete logical schema participates in the definition fingerprint alongside
the EAV pattern contract and accepted scope. Changing cardinality, domains,
reference classification, or absence semantics requires a new accepted
descriptor and dependent-plan revalidation. Statistics and measured costs do
not change these guarantees. Plan-only explain may show their bounded logical
fields under §8's redaction and accounting rules; it must not expose supporting
physical metadata or proofs.

### 10.2 Accepted scope and completion

The v2 source descriptor fixes these contracts before planning:

| Field | Required meaning |
|---|---|
| `:scope :id` | Nonempty non-secret string identifying this accepted scope under the provider's versioned definition scheme. |
| `:scope :kind` | `:full` or `:restricted`, relative to the declared logical relation. Neither grants access beyond authorization. |
| `:scope :parameters` | Immutable portable map of accepted tenant, repository, partition, window, or other scope parameters; `{}` for an unrestricted scope. Credentials and runtime handles are forbidden. |
| `:completion :domain` | Exactly `:finite` in v2. Unbounded subscriptions/streams require another contract. |
| `:completion :extent` | `:all-matches` for the complete accepted relation, or `:declared-view` for an explicitly defined bounded/search/capped view. |

The complete scope and completion maps participate in the definition fingerprint,
along with the normalized mapping/query and conversions. The same scope ID must
not denote different accepted parameters. A `:declared-view` includes its exact
membership rule, cap, ordering/tie policy where relevant, and search/window
semantics in the accepted definition. It is a distinct logical relation, never
a way to advertise an incomplete scan of a broader one. Request bindings narrow
an access within that scope; they do not mutate it or enter its fingerprint.
Any scope change requires a new accepted descriptor and revalidation.

Each alternative names the captured `:scope-id` and promises
`:completion :all-bindings`. For dependent access, successful cursor exhaustion
means that every supplied binding has received all matching rows for that
alternative's exact coverage, including bindings with no matches. For input-free
access (`:requires []`), exhaustion means all matching rows within the accepted
scope and planned constants have been returned; no runtime bindings are needed.
The final `:exhausted? true` batch establishes completion for the whole access;
no separate per-binding completion list is needed. Safe superset rows remain
subject to residual checks. Completion does not imply a snapshot across pages
or opens; the actual read guarantee in §10.3 applies.

Pagination tokens and retries remain provider-owned. A known result cap,
unavailable continuation, omitted page, or partial backend response must fail
with `:query-source/incomplete`, unless the narrower result is the accepted
`:declared-view` itself. Budget/deadline/cancellation failures retain their
existing categories. None may be returned as successful exhaustion. Reaching
the query's own early-stop condition closes the cursor without claiming source
completion; partial observations cannot back a complete replay spool.

### 10.3 Per-source read requirements and actual guarantees

Query options retain `:consistency` as a default and add alias overrides:

```clojure
{:query-source
 {:consistency :snapshot
  :sources {'$native {:consistency :snapshot}
            '$crm {:consistency :best-effort}}}}
```

The default is `:snapshot`. `:sources` keys must name actual source variables
in the expanded invocation, including native inputs in mixed queries; unknown
aliases/options are invalid. Each override has exactly `:consistency` (plus
inert namespaced annotations). These are minimum guarantees, ordered
`:snapshot > :statement > :best-effort`. A stronger actual guarantee is allowed
and reported; a weaker one is rejected. Native-only calls keep the native path.

Core groups aliases by source-instance ID, compute-context ID, and effective
principal/policy requirements, with compatible logical views. It takes the
strongest requirement across the group; it never opens separate weaker views
to bypass a conflict. The descriptors' `:read-capabilities` are nonempty sets
of the three guarantees; advertising a stronger guarantee also supports weaker
minima. If any member lacks a guarantee at least as strong as the effective
minimum, fail with `:query-source/unsupported` before acquisition. Different
physical groups keep independent requirements. Incompatible authorization or scope bindings
fail rather than being silently broadened or substituted.
Alias names or different requested consistency levels cannot by themselves
justify different compute-context IDs to evade this grouping rule.

The common v2 context retains the seven fields in §4; `:consistency` is now the
group's effective minimum. Acquisition adds both `:source-descriptors` and
`:read-requirements`, a map from every alias in the group to its requested
minimum. Core forwards the selected common context unchanged to that group's
planning and opens. `-session-info` reports the original identity/authorization
fields, `:spi-version 2`, the negotiated features, and actual `:consistency`:

| Actual guarantee | Promise |
|---|---|
| `:snapshot` | All fragments, binding batches, pages, and permitted repeated reads in the group observe one pinned logical view. Loss of that view fails the query. |
| `:statement` | One cursor invocation, including all of its bindings, internal requests, and pages, observes one logical view. Different opens may observe different views. |
| `:best-effort` | The provider completes the declared access but cannot promise one stable view even within a cursor. |

A backend snapshot per HTTP request does not establish `:statement` for a
multi-page cursor. Acquisition may establish authorization and scope for a weak
session without acquiring a snapshot. All reported guarantees must satisfy the
effective minimum and be supported by every member's advertised capabilities
(including their implied weaker guarantees); otherwise close the session
and fail with `:query-source/unsupported`. Query explain records requested and
actual guarantees separately for every alias and the effective group minimum.

Session `:data-version` retains §4's meaning: an exact pinned group view token,
or `nil` if unavailable. Statement/best-effort sessions report `nil`; this
version introduces no per-cursor version-token envelope. Snapshot sessions may
also report `nil` when no external token exists. Read consistency, identical
replay, and exact version tokens remain independent declarations. A materialized
observation stabilizes those rows, not the backend history; it cannot upgrade
actual consistency or enable cross-read emptiness/uniqueness shortcuts.

The internal [`source.session`](../src/datalevin/query/source/session.clj)
lifecycle implements this grouping/acquisition behavior as preparation. Its
`prepare!` consumes captured descriptors after full schema/type validation and
checks every alias and group before provider acquisition. `acquire!` opens all
prepared groups eagerly, once, and validates the actual guarantee against every
captured descriptor, even when aliases describe the same logical view. It owns
returned sessions before observing late cancellation, retains cancellation
registration through close, and unwinds partially acquired sessions and
snapshots before rethrowing. Its explanation copies distinguish alias minimums,
group minimums, and actual guarantees without authorization or provider-private
metadata. Providers still admit setup I/O through their session-wide request
limiter; core validation creates no duplicate limiter or synthetic request
charge. Native read contexts remain owned by the existing engine. The public v2 driver uses these helpers for both `q` and `explain`.

### 10.4 Consumption groups and replay

V2 defaults to one consumption group for the whole session. Session information
declares its permissions once in `:consumption`, alongside session-wide request
constraints:

```clojure
{:consumption {:opens :many :max-open-cursors 1}
 :request-constraints
 {:max-concurrent-requests 2
  :rate-limit {:requests 10 :per-ms 1000}}}
```

All alternatives, aliases, and binding batches share those permissions. They
omit `:consumption-group` from alternative properties and cursor-open contexts;
the session identifies the group, so no synthetic group ID is needed. The
`:consumption` map requires exactly `:opens` and `:max-open-cursors` (plus inert
namespaced annotations). Permissions must be declared, not inferred from replay
or consistency. A one-shot session uses `:opens :once` instead of `:many`.

Use explicit groups only when independent resources in one session need separate
open budgets or concurrency permissions. For example, an orders stream may allow
one open while customer lookups allow repeated opens; two independent one-shot
streams also need separate permissions if each must open once. Resources that
can safely share one budget keep the session default. Explicit groups replace
`:consumption` with `:consumption-groups`:

```clojure
{:consumption-groups
 [{:id :orders-stream :opens :once :max-open-cursors 1}
  {:id :customer-lookups :opens :many :max-open-cursors 1}]
 :request-constraints
 {:max-concurrent-requests 2
  :rate-limit {:requests 10 :per-ms 1000}}}
```

Session information contains exactly one of `:consumption` and
`:consumption-groups`. The latter is a vector of at least two declarations, each
with exactly `:id`, `:opens`, and `:max-open-cursors` (plus inert namespaced
annotations). Group IDs are unique non-secret keywords/strings within a session
and last until session close; their identity is `(session-id, group-id)`.
In this explicit form, every alternative names one existing group in
`:properties :consumption-group`, and core forwards that same ID as
`:consumption-group` on cursor open. There is no implicit fallback group.
Missing/unknown IDs, an open ID differing from its alternative, or explicit IDs
in the default session form are invalid. Both forms use the same enforcement.

All paths/aliases consuming the same underlying one-shot resource must share
one permission budget. An alias, alternative, attribute, or cursor is not an
independent resource and cannot justify a separate group. Explicit groups do
not create separate sessions or read views, or weaken §10.3's alias grouping.
Provider-private resources shared across queries require provider-side ownership
enforcement as well; new query IDs cannot restore a consumed stream.

In either form, `:opens :once` permits one cursor-open attempt across all group
alternatives, aliases, and binding batches during the session, with
`:max-open-cursors 1`.
`:opens :many` permits repeated opens up to the positive concurrent-open limit.
Core reserves the permission before calling open. A failed open spends a
one-shot permission; cleanup never replenishes it. Metadata/planning consumes
no permission. Internal page fetches/retries use the same cursor but consume
actual request budgets. No additional cursor is opened speculatively.

Each alternative's `:properties :replay` is `:identical` or `:unstable`.
`:identical` guarantees the same logical row multiset for the same alternative,
scope, and typed binding values in that session, allowing binding-ID renaming;
order remains unspecified. `:unstable` makes no such promise. This property
does not grant another open, override the group's limit, or upgrade consistency.
Repeated live API calls normally have unstable replay. Changed bindings describe
a different access and are not a replay of previous results.

A plan requiring repeated logical use must either have permitted identical
replay or explicitly materialize one complete observation under memory/spill
quotas. Spool identity includes the session (and group ID only for explicit
groups), captured definition/scope, exact coverage, and typed binding keys.
Share it only when its complete rows are sufficient for the other use through
exact local evaluation. Closing an
incomplete cursor, observing a prefix, or recording one key does not complete
another key or relation. Spooling never grants a second one-shot open.

One-shot dependent access therefore needs all required keys in one legal input
batch, or a different complete access that core can spool and index. A plan
needing multiple opens of that group is rejected before execution. Unknown key
counts may be collected under quotas before its single open; exceeding the
admitted batch fails rather than truncating or reopening. A chosen full scan
may feed multiple local lookups only after its required complete spool exists.
Identical-replay reuse is forbidden for unstable accesses unless the plan
explicitly shares a captured complete observation; explain exposes that choice.

Session request constraints apply across all groups and aliases.
`:max-concurrent-requests` is positive; `:rate-limit` is either `nil` (no
additional advertised rate limit) or positive `:requests` and `:per-ms`, meaning
at most that many request starts in any rolling interval. Providers enforce
these alongside query concurrency limits and any limits shared across queries.
Every page, retry, and split request charges the cumulative request ledger;
waiting for a permit is cancellable and observes the query deadline.

The public helpers `datalevin.query.source/request-limiter` and
`datalevin.query.source/acquire-request!` implement shared request admission.
Create one Closeable limiter during session setup with the invocation's control
and the session's request-constraints map, and share it across all aliases,
cursors, and explicit groups. Every actual backend operation, including setup,
pages, retries, and splits, acquires its own Closeable permit:

```clojure
(with-open [permit (source/acquire-request! session-request-limiter)]
  (fetch-page!))
```

A permit marks one request start and owns concurrency until that operation
finishes, including on failure. Closing it releases live charges without
refunding a request start or its rolling rate history. Waiting holds no request
start or concurrency slot; its retained state is quota-accounted and observes
cancellation, interruption, deadlines, and limiter/control closure. Close the
limiter during session cleanup. Metadata needed by outstanding permits remains
accounted until they close. Backend constraints shared across invocations remain
provider-owned.

Core's internal consumption helper separately implements default/explicit open
permissions, reserving attempts before provider calls and retaining spent
one-shot attempts after failures. The public session lifecycle enforces these
v2 declarations independently of replay guarantees.

### 10.5 Dependent access and binding batches

V2 alternatives retain `:id`, `:plan`, `:covers`, `:prefilters`, `:columns`, and
`:explain`. Their IDs are stable nonempty keywords/strings for an unchanged
structural planning request and unique within that source/region response.
The request's `:region-id` scopes coverage and the selected handle under §10.6.
Alternatives additionally require the following fields;
`:access` and the revised inline `:estimate` are specified in §10.6. This example
uses the default session-wide consumption permissions:

```clojure
{:requires [{:variable ?customer :type :long :nullable? false}]
 :scope-id "scope:tenant-a"
 :completion :all-bindings
 :properties {:replay :unstable :ordering [] :blocking-state :none}
 :constraints {:max-binding-rows 100
               :max-binding-bytes 65536
               :max-request-binding-rows 20}}
```

`:requires` is an ordered vector of unique variable/type/nullability columns;
`[]` means input-free access. The selected alternative's `:requires` fixes the
open and batch shapes; no additional mode flag or synthetic binding is needed:

| Access | Cursor-open additions | Returned batch shape |
|---|---|---|
| Input-free (`:requires []`) | `:scope-id` and `:region-id`; omit `:bindings`. | Keep §6's fields; omit `:binding-ids`, including on the final batch. |
| Dependent (nonempty `:requires`) | `:scope-id`, `:region-id`, and a nonempty `:bindings` vector. | Keep §6's fields and require per-row `:binding-ids`, including an empty vector for an empty final batch. |

Both access shapes omit `:consumption-group` under the session default. Only
sessions declaring explicit groups require that field in alternative properties
and cursor-open contexts, as specified in §10.4.

Known planning constants remain in the selected handle. An entity/value lookup
with all inputs fixed by those constants can therefore also be input-free.
Conversely, a scoped enumeration with runtime required inputs is dependent.

Every required variable must occur in the region's accepted clause/input
envelope and be supplied by legal query inputs or upstream/local operators
before open. A sibling branch or a hidden variable beyond a rule/projection
boundary is not a supplier. No hidden correlation or provider callback into
the outer query is allowed. `:columns` declares returned variables; together they must supply the
variables needed to implement covered clauses and downstream demand. A required
variable may also be returned, but each returned value must equal its associated
input under `QueryKey.equals`. Repeated-variable types/nullability follow §5.

Core constructs a dependency DAG and rejects absent inputs, self-supplying
alternatives, and unseeded cycles before opening execution cursors. A constant
or a finite query/native/source input can seed a path. The admitted input domain
must satisfy every required column's type and nullability. Core cannot silently
drop unsupported keys: it must prove they cannot match the logical relation,
choose another complete path, or reject the plan. Runtime violations are errors.

For dependent access, core deduplicates the ordered input tuples with shared
`QueryKey.equals`/`hash`, owns their mutable values, and retains the association
to every outer tuple and its multiplicity under quotas. It does not use
raw-tuple Clojure map equality.
Deduplication is within the chosen logical access; unstable reads do not permit
an unannounced cache across separate access occurrences. Matches fan back out
before projection, `:with`, distinct, or aggregation. An empty dependent input
relation produces no matches and opens no cursor. This does not suppress an
input-free scan, which opens normally when required by the query plan.
Evaluate each distinct key once per logical access: either collect its outer
associations before lookup or retain its matches until all matching outer rows
have been processed. Exceeding retention quotas fails; it cannot silently turn
deduplication into repeated unstable reads.

V2 query limits add positive `:binding-rows` (default `256`) and `:binding-bytes`
(default `262144`). These cap each dependent open's input batch, independently
of returned `:batch-rows`/`:batch-bytes`. Alternative constraint values are
positive integers.
The effective cap is the minimum of provider and query row/byte caps. Core
splits by both; a single oversized key fails `:query-source/limit`. The provider
may split a legal cursor input further at `:max-request-binding-rows`; `1`
supports endpoints without bulk lookup. Pages and retries are additional actual
requests, never hidden in one logical-batch charge. Binding caps do not apply
to input-free opens; returned batch and query resource limits still apply.

`-open-cursor` receives the seven common context fields, the four fields from
§6, plus `:scope-id` and `:region-id`. It adds `:consumption-group` only for a
session declaring explicit groups. Dependent access additionally requires `:bindings`, as in
this default-session example (besides inert namespaced annotations):

```clojure
{:scope-id "scope:tenant-a"
 :region-id 4
 :bindings [{:binding-id 7 :values [42]}
            {:binding-id 8 :values [99]}]}
```

IDs are core-assigned query-local nonnegative signed-64-bit integers, opaque to
providers. An ID is never reused for a different logical access or key within
the query; exact replay may reuse its IDs. Each dependent open has unique IDs
and unique input tuples under query-key equality. IDs persist through the access's
replay/spool and are not reassigned to different values. Values
are owned fixed-arity vectors in `:requires` order. An empty binding vector
is not a legal dependent open. Input-free opens omit `:bindings` entirely;
core does not allocate binding IDs or input-association state for them.
Providers borrow dependent input values until cursor close and copy them if
retained longer, with retained-state accounting.

Before any open, core validates its shape, scope/fingerprint, source/region
and handle/session membership, and available consumption permission; explicit
groups additionally require group membership validation. Dependent opens additionally validate
IDs, arity, types/nullability, and binding caps.
The provider verifies the same captured execution contract. The handle contains
the immutable clauses and known planning constants; runtime bindings are exactly
the declared required inputs, not permission to replan or change coverage.

Dependent returned batches keep §6's fields and add per-row `:binding-ids`:

```clojure
{:binding-ids [8 7 7]
 :exhausted? false}
```

`:binding-ids` is parallel to `:rows`, with one requested ID per row; response
order and row values cannot substitute for it. Multiple rows for one ID and
identical rows for different IDs are legal. Unknown IDs, wrong lengths, or
incompatible returned input variables are invalid. Provider rows retain their
logical multiplicity; core does not deduplicate them merely by binding ID.
Input-free batches use §6's shape directly, with no `:binding-ids` field or
per-row ID allocation. Core validates the shape against the selected alternative;
IDs on an input-free batch or missing IDs on a dependent batch are invalid.

Successful final `:exhausted? true` establishes completion under §10.2 for
either shape. Final rows are allowed. A dependent final batch retains per-row
IDs; an empty one has `:rows []` and `:binding-ids []`, including when every
requested binding has no matches. Zero-match bindings need no placeholder rows
or IDs. An empty input-free final batch has `:rows []` and no ID field.
Until successful exhaustion, an ID's absence from returned rows does not prove
that it has no matches. There is no partial-success result in ordinary `d/q`.
Empty non-final batches and `nil` exhaustion remain invalid. No subsequent
advance is allowed after the validated final batch.

V2 adds `(binding-charge-bytes requires bindings)` and the public
`(batch-charge-bytes columns rows binding-ids)` arity for dependent access.
Input-free batches retain `(batch-charge-bytes columns rows)` and allocate or
charge no binding-ID vectors. Both paths use §6's conservative, cancellation-aware
accounting for owned rows and retained backing. Dependent accounting also covers
input IDs and per-row output IDs. Input batches are charged to
retained memory until their users release them; returned
`:byte-count`/`:reservation` cover the extended batch. Headers, dedup indexes,
and outer associations remain accounted for even when zero rows are returned.

The public v2 execution path uses the accounting helpers and binding limits. The dependent helper validates required columns, IDs,
typed input tuples, and duplicate keys under query-key equality. Both output
charge shapes include metadata and retained backing; an empty dependent final
batch still charges its ID vector. The two-argument input-free path keeps its
existing charge and allocates no ID state. Dependent cursors use the revised descriptor and session contract.

The internal [`source.binding`](../src/datalevin/query/source/binding.clj)
primitive supplies the public dependent execution path. Its caller creates one ID space
for the invocation's dependent accesses and collects all outer associations for
each chosen logical access before admitting any of its input batches. It owns
keys and witnesses, applies query/provider row and byte caps, and rejects a
one-shot access that would require multiple opens. Each response is checked
against the IDs of its particular input batch, including returned required
variables; rows fan out synchronously to every associated witness. Only final
exhaustion marks a batch complete. Keys, indexes, witnesses, batch containers,
and temporary sizing state are quota-accounted and cleared on failure/close.
Batch assembly sizes each key once and matches `binding-charge-bytes` exactly
for its generated vectors. The cursor adapter still owns envelope admission,
permission reservation, and cursor/batch lifetime; downstream shared operators
must own and charge values they retain. Input-free paths allocate none of this
association state.

The internal [`source.cursor`](../src/datalevin/query/source/cursor.clj) adapter
now admits both shapes for captured v2 session/access plans. It validates the
captured source/region/scope/session boundary, reserves the open attempt before
the provider call, owns late-returned cursors before checking cancellation, and
keeps batch reservations live through synchronous delivery. It validates IDs
and echoed inputs for the complete batch before emitting rows. Missing final
exhaustion or a known truncated response fails with `:query-source/incomplete`;
it cannot return a successful prefix. Normal and exceptional closure release
cursor concurrency, input associations, batch envelopes, and transferred
reservations, preserving primary errors and redacting suppressed close errors.

Shared `AccessStep` execution, including its pipe hook, has an explicit batched
method for this adapter. The method collects all outer witnesses before any
dependent open, evaluates each distinct key once for that logical access, and
preserves returned multiplicity during fan-out. Overlapping variables use
`QueryKey.equals`; retained output is copied and charged separately from borrowed
provider batches through invocation cleanup. Input-free execution creates no ID
space or binding index. Input-free accesses with upstream keys use the shared
bounded hash join. These hooks execute public v2 compiler regions; the
[resource audit](query-source-resource-audit.md) records their admission and
ownership boundaries.

<a id="106-conditioned-planning-and-deterministic-selection"></a>

### 10.6 EAV access planning and deterministic selection

The baseline is EAV access selection within the existing planner. V2 makes one
`-plan-source` call per `[source-symbol region-id]` pair, returning that region's
legal access alternatives and advisory estimates together. Source grouping alone
cannot preserve rule, branch, or projection semantics. F1 implements
single-clause accesses, dependency scheduling, structural selection, and bounded
native joins. Estimates
may all be unknown. Core also schedules structurally valid same-entity fused
alternatives; providers retain responsibility for their physical equivalence.
Comparable-cost ranking remains part of the
[optimization follow-up](../roadmap.md#federation-optimization-follow-up).
Separate estimate phases and broader search remain later extensions of the
existing planner. Planning performs no source I/O or cursor opens;
metadata remains bounded and charged as in §5. The common context uses the
acquired group's effective request and validated actual guarantees.

#### Attribute-term lowering

Normalize logical attribute terms through the existing planner before assigning
provider clause IDs/regions. The captured exposed schema gives a finite
candidate domain; only exact constraints valid at this logical location may
narrow it. Attribute-domain enumeration is metadata work, distinct from the
`:attribute-enumeration` access kind, which reads facts for one fixed attribute.

| Logical attribute term | Core lowering |
|---|---|
| Declared constant, or variable with a known scalar attribute binding | Select that attribute's accesses and preserve the variable's original binding when it is observable. |
| Variable supplied at execution by query inputs, a rule argument, or another operator | Prepare a dispatch over every still-possible declared attribute. Route each outer binding to its matching preplanned branch; preserve all associated outer bindings and multiplicity. |
| Unbound variable | Plan one branch per still-possible declared attribute and combine their matching facts; bind the attribute variable to that branch's keyword. |
| `_` | Plan the same candidate accesses, retaining ordinary wildcard projection semantics without introducing an observable attribute variable. |

Specializing to attribute `A` produces an ordinary fixed-attribute EAV access
plus core binding/unification of the attribute term with `A`. That unification
also constrains repeated variables in entity/value positions, joins, rule
arguments, and predicates; it is not an independent replacement of the middle
term. Provider output need not repeat a constant attribute: core restores the
attribute binding before its consumers. The lowered request's live-variable
requirements exclude bindings core reconstructs from that branch's constant;
validation checks the restored variables on the parent plan edge. Runtime
attribute dispatch still needs a legal upstream supplier. No new provider
method or feature flag is needed for this lowering.

The dispatcher/union is part of the existing plan. Split the access node into
candidate branches without cloning the whole query body. A known single
attribute stays in its existing region. Each generated dispatch/union branch
has its own planning region and clause-occurrence IDs, with a private core map
back to the original pattern. Assign branches in canonical keyword order before
the stable region traversal. Providers cover only lowered occurrences; one
child alternative cannot claim the entire original variable-attribute pattern.
The parent pattern is complete only when every possible branch has a complete
plan or an exact proof of no matches. Existing rule, branch, projection, and
prefilter boundaries remain in force; source equality cannot justify combining
clauses across generated branches.

Known attributes can select one branch during planning. Attributes supplied at
execution choose among preplanned branches without calling `-plan-source` again
or reading outer rows during planning. Values outside the exposed domain match
no branch under ordinary term validation/matching rules and open no access for
that binding. Even empty branches retain normal query-wide authorization and
validation. A possibly selected declared attribute cannot be omitted because
its estimate is high or no alternative was returned.

Use available entity/value bindings to choose the existing access kinds within
each branch. Enumerating attributes of a bound entity can use entity lookups
across the schema and does not require full-source scans. For an unbound entity,
value lookup or per-attribute fact enumeration may suffice. Required tenant,
parent, or other scoped inputs still need a legal supplier. If any potentially
matching branch has no complete legal path after valid domain narrowing, the
query is unsupported before cursor opens. Expansion and retained plans obey the
shared planning limits; exhausting a bound raises `:query-source/limit`, never
partial success. Shared one-shot permissions and read guarantees apply across
all branches. A partial attribute spool is not a complete observation of the
other attributes.

The internal [`source.lowering`](../src/datalevin/query/source/lowering.clj)
implements fixed specialization, canonical attribute branches,
runtime dispatch metadata, branch-specific types/guarantees, restored attribute
bindings and repeated-term unification. It retains caller multiplicity while
visiting compatible branches, observes cancellation and reentrant closure, and
admits retained planning state before expansion. Undeclared keywords and proven
disjoint repeated-variable types can establish no matches. Different descriptors
whose value domains may overlap, including `:data`, cannot establish that proof;
an unsupported repeated-variable type contract is rejected rather than erased.
Internal acquired-session/shared-step tests compare its lowered reads with eager
EAV facts, including heterogeneous attributes, wildcard and repeated terms,
zero-column reads, self-joins, aggregates and `:with`. The public v2 compiler
selects this pass.

Each branch keeps its declared entity/value types, reference domains, and
non-null fact terms; the restored attribute binding has type `:keyword` and is
non-null. Different attributes may yield heterogeneous values or entity domains
in the combined result. Use the existing engine's binding representation and
retain branch type knowledge for validation; do not coerce values to one scalar
type, erase nominal identity, or reject the logical pattern merely because its
branches differ. Downstream typed access and function checks use their ordinary
contracts; incompatible bindings may be discarded only with an exact no-match
proof, not to repair an unsupported operation.

Preserve EAV fact identity, repeated-variable unification, caller multiplicity,
rule-head set boundaries, and ordinary projection/`:with` semantics. In
particular, equal `[entity value]` pairs under two attributes are still distinct
facts. A query retaining the attribute, including via `:with`, must distinguish
them; `_` follows the existing engine's wildcard behavior. Do not deduplicate
away the attribute before the corresponding logical projection permits it.

#### Logical planning regions

Core retains the existing normalized query/rule graph and partitions its access
work into planning regions. A region is a conjunctive block whose clauses can
be planned together without crossing a rule invocation/branch, rule-head set
projection, union branch, or other projection/distinct/aggregation boundary.
For every admitted operator, preserve its input/output scope and semantics;
this contract does not enable otherwise unsupported operators. Regions are
internal annotations on the existing plan, not another query model or engine.

Each rule call site in the normalized plan has distinct argument/local-variable
scope; runtime invocations reuse its compiled plan with explicit bindings. Its
alternative branches have separate regions, followed by the existing rule-head
projection and set union. Callers consume that result through explicit plan
edges. Nested calls retain their own boundaries; separate call sites cannot
share local variables merely because names or rule bodies match. Likewise,
union branches and the inputs/outputs of lossy operations stay distinct. A
plain conjunction can remain one region containing several source accesses and
core operations; it does not require one region per pattern.

The internal [`source.region`](../src/datalevin/query/source/region.clj)
preparation annotates engine-owned parser query/rule nodes without replacing
their evaluation. It records explicit head/argument mappings and set boundaries,
separate call-site and union scopes, generated attribute branches, stable IDs,
and parent reachability. Native rule nodes remain in the shared engine; recursive
external calls and external negation remain unsupported by this initial pass.
It builds flat planning requests from captured descriptors and explicit typed
exports supplied by the shared planner. Requests contain only that source's
lowered occurrences; core residuals and private/enclosing/sibling nodes remain
private. Statically empty ancestors eliminate descendant access work while
leaving query-wide validation/authorization requirements intact. Planning
reservations unwind on failure, and closed or foreign captures cannot form new
requests. The active shared planner derives typed exports and emits these
annotations through its rule/union execution paths.

The internal [`source.planning`](../src/datalevin/query/source/planning.clj)
lifecycle now connects owned v2 descriptor capture, grouped acquisition,
region-local provider planning, shared access selection, and whole-query
consumption validation. It captures a provider object's descriptor once across
aliases and validates all requests before acquisition. The shared compiler must
publish each live region's actual typed inputs and captured operators, with
explicit coverage of every private clause/site; missing core work rejects
before acquisition. [`source.descriptor`](../src/datalevin/query/source/descriptor.clj)
validates the complete revised EAV schema and scope, and
[`source.envelope`](../src/datalevin/query/source/envelope.clj) validates every
returned candidate, including unused envelopes and hidden vector backing.
Both revised estimate shapes retain unknowns and zero without native cost
conversion. Owned selected envelopes retain their reservations; discarded
candidates release theirs. Failure closes all compilation resources immediately.
Focused tests run selected flat-query paths through shared `AccessStep` execution,
including native-seeded dependent lookups. The query entry point still needs
the coordinated native/compiler/adapter switch.

The internal [`source.scope`](../src/datalevin/query/source/scope.clj) pass now
captures those typed boundary interfaces from complete shared branch schemas.
It maps actual parent inputs only through explicit head arguments/exports,
keeps the full rule-head set key before wildcard or repeated-argument mapping,
and retains each branch's complete type contract. Heterogeneous combined outputs
use `:data` without converting values; equality to a typed fact can subsequently
narrow that general contract. Its completion adapter accepts already completed
branch relations, owns the declared head/export values, and delegates projection,
union and argument unification to shared operators. Attribute expansion uses
bag union and restores the captured keyword. Retaining containers are admitted
before allocation, partial admissions unwind on error, and temporary list views
observe cancellation/closure inside shared loops without escaping in results.
This adapter does not evaluate branches, join caller witnesses, or authorize
replay. Producers must already enforce decoding, arithmetic and intermediate
limits. The active driver still needs to select these interfaces and connect
region evaluation with the remaining resource audit and coordinated switch.

The internal [`source.program`](../src/datalevin/query/source/program.clj)
compiler now emits actual scoped execution steps from those annotations and
typed inputs. Shared `IStepFactory` instances bind concrete tuple positions
after scheduling, preserving carried columns when external accesses change the
layout. Scoped calls collect distinct input keys before executing child regions
once, complete their shared projection/set boundary, and join results back to
all original outer witnesses. The shared bounded hash join admits counting and
execution indexes and exact output multiplicity before materialization; guarded
input lists observe cancellation inside its existing loops. Input-free accesses
retain zero-column outer multiplicity and exhaust normally on empty outer input.
The same compiler runs terminal projection, `:with`, baseline aggregates,
windows and return shapes through the shared result processor with explicit
workspace/cancellation hooks. Results are fully realized and independently
owned before cleanup; failed execution/collection closes compilation resources
immediately. Typed boundary no-match proofs propagate upward through all-empty
sites and downward through empty ancestors before provider requests. Exact
literal/scalar mismatches also eliminate impossible EAV facts; numeric/custom
or sequential representation differences do not establish no-match proofs.
Literal rule arguments now constrain facts by ordinary constant equality while
retaining their original representation in scoped seeds, scalar functions and
the complete rule head. This includes numeric literals whose representation
differs from the declared fact type. Function outputs constrained by literals
use shared function evaluation and predicate equality with an admitted private
temporary column. Their disjoint type proofs still prune whole nested regions.
Captured scalar inputs retain typed key equality; some representation-mismatched
scalar input edges still reject as unsupported.

The internal [`source.native-plan`](../src/datalevin/query/source/native_plan.clj)
bridge now executes native fixed-attribute initializations, entity-bound
merge scans and equal-value link steps. Entities may be variables, fixed integer
constants or wildcards; values may be variables, constants or wildcards.
Unindexed attributes require a bound entity. The native planner's pure emitters
read no mutable store
metadata, statistics or native caches and invoke no installed access methods.
Their explicit controlled interfaces use shared AVE/EAV scan drivers. Entity
keys are admitted and deduplicated before reads; non-Long runtime entity keys
open no ranges, while integer literals use ordinary constant equality. Shared joins
restore all original outer witnesses. A value column already present in the
selected input layout supplies distinct value keys; combined entity/value
lookups preserve complete key correlations. Bound encoding has a separate
scratch reservation held through the shared cursor visit. Eligible scalar
and byte equality bounds restrict AVE keys or EAV duplicate ranges before
decoding, while logical comparisons reject complete giant-prefix collisions.
Integer literals equivalent to native Long values can use those bounds;
runtime inputs retain query-key equality, including signed zero and decimal
scale. NaN never enters a native index encoder. Decimal/bigint encodings,
tuples, generic data and custom payloads retain bounded prefix scans and
logical filtering where an exact bound has not been established.

Constant scalar predicates reuse native range lowering and intersection for
Long/reference, floating-point and date ordering. Candidate bounds include
complete endpoint buckets, with the shared original predicates enforcing strictness before owned
retention and again as ordinary residuals. String and other unsupported index
orderings remain residual. Symbol-valued constant arguments and variables
shadowed by host Vars retain shared resolver evaluation. Incompatible literal
types can complete without decoding a payload. Quota failures release range
scratch and compilation state. Controlled steps admit rows before allocation,
decode through the bounded raw-value visitor,
own values before advancing the cursor, and join through the shared bounded
join. Wildcards skip payload decoding. The invocation pins native snapshots
across provider planning and execution; views cannot escape or move threads.
Portable registered Nippy payloads use the same bounded raw decoder without
invoking custom order functions. Controlled reads bypass native tuple/result
caches. Other compiler core work
includes scalar predicates and identity/ground bindings. Controlled unindexed
attribute enumeration and repeated entity/value terms are also admitted. Native
reference producers can bound later payload reads, including when the provider
offers both scans and dependent lookups. The
[operator/decode audit](query-source-resource-audit.md) covers the admitted paths;
unsupported shapes fail before execution cursors open.

The internal `source.program/query!` bridge now parses ordinary query syntax and
`:in` arguments into that compiler. Source aliases and `%` rules are extracted
before capture. Scalar, tuple, collection, relation and nested bindings become
owned shared Relations; bounded hash joins admit their products before output
allocation and preserve correlations and duplicate witnesses. Only complete
owned inputs establish a homogeneous type/nullability; heterogeneous and empty
columns remain `:data`. Reusable Java rows and tuple leaves are detached before
the producer advances. Ignored inputs and unused tuple tails remain unread;
arity validation pulls at most one extra argument. Direct scalar limit/offset
parameters resolve before provider callbacks. Input preparation observes query
timeouts, cancellation and quotas, and all invocation resources close before
returning independently owned results. Public `q`/`explain` select this compiler.
Unsupported resolver/input shapes still reject explicitly rather than bypassing
the controlled execution boundary.

Local collection sources now also enter the revised compiler. Ordinary input
preparation and direct capture detach every local source before external
descriptor callbacks. Aliases of one producer share a complete capture. Unlike
a tuple binding's declared prefix, source facts have no declared row width and
are owned completely; unbounded sources or fact tails stop at the invocation
quota/deadline. Shared controlled collection matching preserves constant and
repeated-variable equivalence, nullable facts, arbitrary tuple widths and
zero-column bags. Matching emits only declared columns, admitting rows before
allocation, and complete local results supply conservative types to dependent
source planning. Collection/native/mapped clauses use the same bounded joins.
Exhausted local matches establish exact no-fact proofs, propagated through
rule/union scopes before provider planning. Authorization remains query-wide,
and live union alternatives remain scheduled.
Native read views pin DB inputs only.

Positive acyclic native/local rules over admitted operators now use the same
scoped completion as mapped rules, including nested calls, required inputs,
literal values and complete rule-head set semantics. Recursive native programs
retain core nodes for controlled body emission. Ordinary native `q` continues
to select its direct execution path.

The native evaluator and internal controlled recursion adapter now share the
same delta-iteration driver. The controlled adapter accepts completed compiled
body relations, runs serially on the pinned-view thread, and uses shared typed
keys and relation union. It projects complete heads before seen-set insertion,
copies borrowed values before reading another tuple, and reserves indexes,
containers and owned payloads before retaining answers. Body workspaces release
after synchronous result consumption; superseded total/delta lists release
after each iteration. Seen keys and their values remain owned until the result
handle closes. Cleanup does not accumulate one parent entry per iteration.
This is an internal operator boundary, not a clause interpreter.
Native public routing and clause evaluation continue through the existing
engine, using its native operations with the shared iteration driver.

The revised compiler now emits positive native/local recursive programs through
that boundary. It captures a finite source-qualified rule graph and compiles
each reachable body with the existing region compiler. Private body contexts
are owned and validated; they replace eligible rule calls with shared relation
steps, not source descriptors. Patterns, native read steps, predicates, constant
functions and scoped unions retain their existing operators. Initial bodies
run with empty rule tables and synchronously hand off their answers before
cleanup. Subsequent affected bodies read complete current totals, so nonlinear
and mutually recursive positive programs converge to complete head sets.
Call matching and shared distinct projection preserve literal, repeated and
wildcard argument semantics; shared joins restore outer witness multiplicity.

Producer-type propagation starts with native schemas, complete owned local fact
types and constant producers, then follows rule-head/argument edges to a fixed
point. Missing producers are unknown, while a real `:data` producer remains
`:data`; filters never invent narrower types. The resulting conservative types
and nullability can seed dependent mapped accesses before any recursive body
executes. Every body compiles before provider acquisition. No native index scan
or external cursor is opened to derive these interfaces. Captured operator
runtimes attach new execution resources to the iteration workspace; static
compilation resources remain invocation-owned. Long-chain quota regressions
cover both local joins and repeated native scans, including decoded values.
Native body execution stays within the invocation's pinned views.

Declared required-head prefixes now add correlated demand predicates to that
same fixpoint. The outer relation contributes complete required tuples, never
independent per-column value sets. Compiled rule-reference steps register further
demands before reading the current answer tables. Demand and answer predicates
converge together, including mutual recursion, calls from free rules and empty
required prefixes. Newly discovered demand keys are owned before their body
workspace closes; staging releases immediately after solving, while the kernel
owns completed answer keys through invocation cleanup. Both demand and answer
keys use shared typed equality.

Required literals and repeated required positions select their exact demand
partition after ordinary argument matching; repeated arguments retain their
required input position for the subsequent typed join. Variable-argument
identity/ground functions admit required head inputs and locally produced values.
A scope-aware observability analysis identifies optional head positions that can
affect such functions, including functions inside negation. These positions need
not form a contiguous prefix. Minimum required bindings propagate through call
dependencies and public union exports; a positive local producer can satisfy a
callee without requiring that caller head as an input. Private union
and not-join names do not observe caller heads outside their declared interface.
The shared typed scheduler requires the minimum inputs before the call runs.
The step's actual input columns and constant arguments select additional bound
observable positions from the already compiled variants; wildcards stay unbound.
Body seeds, demand keys and call matching use the same ordered positions, so
sparse correlations, renamed heads, typed caller representations and repeated
argument partitions survive recursion. Inference uses short-lived workspaces.
After whole-graph validation, each body context captures only its direct
reference headers and definitions. Reference bodies compile separately; native
reference classification does not walk those definitions again from negation.
Each source/rule/input-position adornment has independent demand and full-head
answer relations. Unbound observable heads enumerate through compiled positive
producers, including mutual/nonlinear recursion and lower negative strata.
Functions without those producers still require caller seeds. All adornments
compile before provider acquisition. Expansion reserves storage before growing
and stops at 4096 variants per prepared graph or the invocation quota. Compiled
references publish their selected positions for dependency and type analysis.
Optional scope inputs retain actual caller types instead of requiring a future
body output type; required inputs may wait for preceding facts or explicit null
filters. Exported types merge conservatively across possible adornments.
Literal numeric comparisons against other producers require a representation-safe
proof; seed-only values retain their original numeric representation. Existing
captured scalar key semantics and their remaining representation edges apply.
Missing required inputs and local self-supply cycles reject before provider
acquisition, even when a provider could otherwise be planned.

Native/local `not` and `not-join` bodies now compile through this region compiler.
An implicit `not` correlates with positively supplied outer variables; explicit
`not-join` requires all declared inputs and keeps other names local, even when
they share spellings with outer columns. A complete distinct input relation
seeds the compiled child once. Shared bounded relation subtraction removes its
matching outer witnesses, preserving all surviving duplicates and typed keys.
The child and subtraction index release before the output container's lease
transfers to the enclosing workspace. Empty negative children retain every
outer witness and do not create parent no-match proofs.

The owned source-qualified rule graph tracks dependency polarity through nested
negations. A finite level computation rejects cycles through negative edges
before acquisition. Same-level positive dependencies use the bounded demand
fixpoint. A lower-level reference completes its own kernel for the supplied
required tuples before returning any answer relation. Upper negation therefore
never decides absence from an incomplete lower relation or an undrained demand.
Temporary lower kernels release with the caller's body workspace; native reads
stay in the invocation's pinned views. This synchronous implementation caps
stratum descent at 32 and reports `:query-source/limit` before body compilation
when that bound is exceeded.

Negative facts participate in representation-safety constraints but never
establish outer producer types or nullability. Comparison constraints also flow
back through identity/ground copies. Mapped negative bodies and mapped recursion
remain unsupported in the initial mapped workflow. Public v2 execution retains
these capability gates.

Internal logical value walkers bound nesting at 128, including metadata,
retained vector backing and nested logical tuples. Structural envelopes count
toward that bound. Excess depth is `:query-source/limit`, observed before copying
or hashing a borrowed recursive key can exhaust the JVM stack. Cancellation and
deadlines retain precedence; a depth/resource failure during type validation
cannot become a false mismatch or an empty proof. Detached vector copies need
only traverse visible contents, while retained-value accounting also checks
their hidden backing.

Core assigns query-local nonnegative signed-64-bit `:region-id` values in a
stable traversal of the normalized plan, independent of provider response
order. Clause IDs identify occurrences, not text: repeated patterns or predicates
in distinct branches/calls have distinct query-local IDs. Hygienic variable renaming and
explicit input/output mappings preserve lexical scope. Providers treat these
IDs and scoped variables as opaque and cannot infer equivalence from them.

For each source symbol with eligible work in a region, call `-plan-source`
once with that region's clauses. Multiple regions or aliases may use the same
captured descriptor and session, but have separate request identities
`[source-symbol region-id]`. This is structural partitioning, not repeated
estimate rounds: do not replan per input row, binding batch, or cursor open.
All regions are planned and the complete graph validated before opening
execution cursors. Statically eliminated work needs no provider request.

Core retains rule/branch/projection nodes and their dependencies privately.
Providers receive a flat positive clause envelope only within the identified
region, not the enclosing rule graph or a source-wide union of its clauses.
There is no provider feature flag for interpreting Datalog rules. Native-only
queries retain §1.3's direct path without constructing SPI region requests.

#### Planning request and EAV alternatives

V2 adds required `:region-id`, captured `:scope-id`, and `:available-inputs` to
§5's request, and narrows its existing fields to that region:

- `:clauses` contains only this source's EAV pattern occurrences and eligible
  predicates valid in this region. A predicate over another branch, a caller's
  result, or values after projection/aggregation is not eligible merely because
  the same source or variable spelling appears there. Predicates requiring
  another source's unavailable values remain core operations.
- `:bindings` contains only known scalar values valid in this scope, including
  constants passed as rule arguments. It cannot import another branch's
  conditions or inspect an outer result to discover keys/selectivity.
- `:available-inputs` is an ordered vector of unique typed variables that query
  inputs or legal upstream/local operators can supply at this region. Crossing
  a rule/projection boundary exposes only its exported variables, with explicit
  argument mappings; hidden body variables are unavailable. Availability still
  requires a legal dependency schedule and establishes no actual key values.
- `:required-vars` includes the live variables needed by this region's joins,
  residuals, outgoing bindings, rule-head projection, and `:with`/aggregate
  boundaries. It is not just the final query's output. Providers must return
  enough columns for core to perform those operations at their original scope.
  Attribute-term lowering accounts for core-restored constants separately, as
  specified above; their absence from provider columns is not missing coverage.
- `:demand` is region-local. Initial v2 uses
  `{:quality :exact :ordering [] :offset 0 :limit nil}`: final-query demand does
  not authorize truncating a branch or an input to rule projection/aggregation.
  Any later ordering/limit delegation needs its separately proven contract.

A minimal request addition is:

```clojure
{:region-id 4
 :scope-id "scope:tenant-a"
 :available-inputs [{:variable ?customer :type :long :nullable? false}]}
```

The example omits §5's retained fields and common context. `:region-id` is a
logical planning identity, distinct from the source's accepted data `:scope-id`
and from session/consumption-group identity.

Core may move a predicate or binding across a boundary only through an existing
optimizer rewrite proven to preserve that operator's semantics, including rule
head/set boundaries, variable scope, and null/empty-group behavior. The rewritten
plan determines region membership before requests are issued. Provider costing,
source equality, or a superset-prefilter claim is not such a proof. Initial
integration can leave these predicates in core; no new cross-boundary rewrite
is required to implement basic rules.

#### Region coverage and execution identity

Return `{:alternatives [...]}` for the request. `:covers`, `:prefilters`, and
`:access :anchor-clause` may name only clause IDs in that exact request.
Selected alternatives cover each required EAV occurrence in each live region
exactly once. Coverage in another region never satisfies this obligation.
Initial single-pattern access and later safe fusion both obey this boundary;
same-source/same-entity patterns in different regions cannot be fused.

A predicate can enter `:covers` only with exact semantics in its own region;
a `:prefilters` claim must preserve every matching row there and retain an exact
core residual in that region before its projection, set union, or other lossy
boundary. Filtering one branch cannot constrain another branch's input or
result. Rechecking later cannot restore rows removed from the wrong branch.
If only a subset of predicate inputs is available, do not treat it as an
eligible filter without an independently established safe rewrite.

Alternative IDs are unique within one response, not across all regions. Core
identifies a selected alternative by `[source-symbol region-id alternative-id]`
and retains its captured request and handle. Opens carry the same `:region-id`
as specified in §10.5. Reject a foreign clause ID, crossed source/region handle,
missing live variable, or invalid correlation as `:query-source/invalid` before
execution; a well-formed region with no complete admissible plan remains
`:query-source/unsupported`. Core validates scope and coverage membership;
provider conformance still establishes semantic exactness/superset safety.

Multiple requests do not create new read views, authorizations, cursor quotas,
or consumption permissions. All opens across regions and aliases obey the
shared session/group guarantees and limits. A branch-filtered spool cannot
serve an unfiltered or differently filtered branch. Reuse across regions needs
an independently proven complete compatible observation under the existing
read/replay contract, with each region's residuals and set boundaries retained;
otherwise use legal separate accesses or reject the plan. Region partitioning
is not permission to reopen a one-shot source or duplicate its budget.

#### Access alternatives

Alternatives have §10.5's fields and required `:access` and `:estimate` fields.
The following example is dependent; input-free estimates use the per-invocation
shape defined below.

```clojure
{:access {:kind :entity-lookup :anchor-clause 0}
 :estimate {:startup-ns 50000
            :per-binding-ns 200000
            :rows-per-binding 2
            :row-bytes 64
            :requests-per-open 0
            :requests-per-binding 1
            :transfer-bytes-per-binding 128
            :peak-retained-bytes 4096
            :confidence :medium
            :provenance {:kind :heuristic :id "customer-lookup-model-v1"
                         :observed-at-ms nil :valid-for-ms nil}}}
```

`:access` has exactly the unqualified keys `:kind` and `:anchor-clause`. The kind is
`:entity-lookup`, `:attribute-enumeration`, or `:value-lookup`. The anchor is an
exactly covered, lowered EAV pattern with a declared constant attribute. This
condition applies after core's attribute-term lowering; it is not a restriction
on the original query's attribute term. Entity lookup
requires the anchor entity to be a constant, a known value in `:bindings`, or a
declared required binding; value lookup similarly requires its value. Attribute
enumeration requires neither, although accepted scope inputs may still be required. All other
correlation is explicit in `:requires`; access kinds do not weaken §10.5.

External accesses may cover one EAV pattern or fuse several patterns. Core
schedules the complete coverage set as one access and joins its wide output
with the remaining work. Existing native plans retain native optimization.
One alternative may exactly cover more EAV
patterns only when they share the anchor's entity variable or the same typed
entity constant, in the same source, planning region,
and accepted read scope. This is same-entity fusion, not a fourth source kind.
Distinct variables in self-joins and separate `_` entity positions do not imply
shared identity. Predicates may be covered or advertised as safe prefilters
within that region's clause envelope. Cross-entity joins remain core work in
this baseline; sharing a source or entity cannot authorize cross-region fusion.

Fusion must be equivalent to joining the attribute relations. A one-row
projection requires accepted identity/functional-dependency proofs; many-valued
attributes retain their independent products and repeated-pattern semantics.
Each matched attribute must exist even when its value is unprojected or `_`.
Keep required identity, reference, residual, and `:with` bindings. Structural
validation checks the envelope; providers establish physical equivalence under
the [fusion contract](../../federation/federation.md#61-fusion-prerequisites).
An unsupported fusion must fall back to separate legal EAV accesses or reject;
low cost is never evidence of equivalence.
Each covered pattern is scheduled exactly once. Overlapping alternatives are
competing choices, not extra reads to finish partial coverage. A fused access
uses one open permission per invocation; dependent batching may still need
multiple invocations. Do not split a one-shot permission or assume a partial
spool covers other attributes to make a plan feasible. Residual predicates
remain core operators and require their input columns from the selected plan.
Core does not synthesize fused provider plans: an adapter must advertise legal
wide alternatives with complete coverage, typed outputs, and explicit inputs.

#### Comparable access estimates

The estimate shape follows `:requires`, just like the execution shape. For
dependent access every field shown above is required. Input-free access instead
uses the following per-invocation fields, all required:

```clojure
{:startup-ns 50000 :run-ns 2000000
 :rows 1000 :row-bytes 64 :requests-per-open 2
 :transfer-bytes 64000 :peak-retained-bytes 4096
 :confidence :medium
 :provenance {:kind :heuristic :id "attribute-scan-model-v1"
              :observed-at-ms nil :valid-for-ms nil}}
```

Numeric fields are finite nonnegative numbers or `nil` for unknown. Validation
and unknown-value preservation are initial requirements; evaluating these cost
models for selection is follow-up work. Both shapes are conditioned on the
alternative's region-local predicates, constants, available input contract,
accepted data scope, types, and coverage; dependent estimates also account for
published binding/request caps. Binding-cardinality estimates refer to the
supplying operator's output after any rule-head projection/deduplication, not
its hidden derivation witnesses. Attribute dispatch conditions each branch on
its routed inputs; do not charge every branch for the entire outer stream or
assume a partition's unknown cardinality is zero. Complete-plan costs include
all required branches and core dispatch/union work. Input-free estimates omit
per-binding fields. Their `:run-ns` is elapsed work after startup
through complete exhaustion, including fetches, pages, decoding/copying, retries,
and waiting; `:rows` and `:transfer-bytes` are totals for that invocation.
The shared and dependent fields mean:

| Field | Unit and meaning |
|---|---|
| `:startup-ns` | Fixed elapsed work per cursor open, excluding session acquisition. |
| `:per-binding-ns` | Additional elapsed work per distinct input tuple, including expected fetches, pages, request splitting, decoding/copying, retries, and rate-limit waits. |
| `:rows-per-binding` | Expected returned binding rows per input tuple, before core fan-out and residual checks. |
| `:row-bytes` | Mean owned logical row/container charge, excluding binding-ID envelopes and wire bytes. |
| `:requests-per-open`, `:requests-per-binding` | Fixed and incremental expected backend operations/pages/retries; zero is valid for wholly local work. |
| `:transfer-bytes-per-binding` | Expected received wire bytes per input tuple; zero for wholly local access. |
| `:peak-retained-bytes` | Expected peak provider-owned live state per invocation, excluding returned batches and core state; not a reservation. |

Baseline time estimates assume one active cursor and serial external requests;
they take no credit for overlap across requests or accesses. Execution may use
permitted concurrency, with actual work still governed by the shared limits.

For an input-free invocation, estimated provider time is `startup-ns + run-ns`;
requests, rows, and transfer use the per-invocation totals directly. Repeated
legal opens incur that work each time. No binding count or ID is invented for
costing or execution.

For a dependent invocation with `b` input tuples, estimated provider time is
`startup-ns + b * per-binding-ns`, requests are
`requests-per-open + b * requests-per-binding`, and output/transfer estimates
scale by `b`. Scoped enumeration with runtime inputs uses one binding per
supplied scope tuple. Zero dependent inputs open no cursor and have no provider
work. A known zero multiplier contributes zero; an unknown operand
needed for any other calculation leaves that result unknown.

The dependent batch model is an estimate, not an assertion that backend
batching is linear. Providers must include expected splitting/pagination under
their advertised caps, declare appropriate confidence, and leave coefficients
unknown when this model cannot represent their work usefully. A real bulk
endpoint can amortize fixed request work; a per-key endpoint must retain its
per-key request cost. Step changes, skew, and variable-size batches can make
estimates inaccurate but cannot change completion or runtime request charges.

For `k` distinct dependent keys in `n` planned legal opens, estimated provider
time is
`n * startup-ns + k * per-binding-ns`; charge full and tail batches through that
same model. Derive `n` under row and byte caps, not just a provider's bulk limit.
Unknown key counts or byte fit leave the dependent totals unknown. Key
deduplication does not erase outer multiplicity; core accounts for fan-out and
joins through its existing planner. Count a fused physical access once, and
shared work once only when the read/replay contract permits reuse. Session
acquisition is common setup, not a per-alternative charge. Do not add request,
transfer, or provider decoding time again when it is already included above.

`:confidence` is `:high`, `:medium`, `:low`, or `:unknown`; an entirely unknown
estimate uses `:unknown` and all numeric fields `nil`. `:provenance` has exactly
the shown fields. Its `:kind` is `:exact`, `:statistics`, `:heuristic`, or
`:unknown`; `:id` is a non-secret string or `nil`. Timestamps/durations are
nonnegative integer milliseconds or `nil`. Statistics with missing/future
timestamps or missing/expired validity are stale. Static exact/heuristic models
may use `nil` timestamps. No raw keys, samples, SQL, or secrets belong in
provenance.
Estimated zero is never proof of emptiness or zero runtime resource use.

#### Integration, fallback, and bounds

Feed structural choices into the existing
[access scheduling](../src/datalevin/query/optimizer/access_plan.clj) and
[costing](../src/datalevin/query/optimizer/plan_cost.clj) boundaries. The active
bridge uses shared access plans and dependency scheduling; comparable-cost
conversion and fused-candidate ranking remain optimization follow-ups.
The existing planner owns viable join orders, native operations, local joins,
residuals, and necessary materialization. Keep rule/branch/projection nodes and
validate coverage and input/export edges region by region; a globally complete
set of clause IDs is insufficient. Bindings and complete coverage must remain
satisfiable, with no unseeded cycles or illegal one-shot reuse. Validate
the chosen complete query plan before opening any execution cursor. If no valid
plan is available, raise `:query-source/unsupported`; never execute partial coverage.

The active [`source.access`](../src/datalevin/query/source/access.clj)
bridge constructs shared `AccessPlan` records from captured, validated
alternatives. It schedules one logical region from explicit typed input/export
columns and checks complete, nonoverlapping pattern coverage without native cardinality
or cost conversion. Its dependency search uses the shared
[`access.dependency`](../src/datalevin/query/access/dependency.clj) scheduler.
Captured native/core `IStep` interfaces contribute typed outputs only after
their own dependencies and explicit ordering barriers are satisfied. Visible
potential exports for planning requests come from lowered pattern schemas and
those interfaces; potential exports cannot seed themselves or hide a cycle
through a local operator. Scoped operators publish only their exported columns,
keeping body variables private. The search remains bounded at 4096 states and
uses an explicit stack. Its query-wide validation checks selected regions together
for one-shot resource reuse. It performs no provider callbacks and admits no
implicit replay/spool. The shared sampling hook enforces the path's
`:none`/`:execution` policy before opening a cursor. Public `q`/`explain` use this
planner with revised descriptor/session/envelope validation, scoped interfaces
and controlled native participation. Discarded envelope and bridge reservations
release after complete-query validation. The
[resource audit](query-source-resource-audit.md) records the active operators.

Regions containing only single-pattern candidates use `:eav-structural-v1`.
Among legal, dependency-ready choices
for the same work, prefer bound-entity lookup, then bound-value lookup, then
attribute enumeration, breaking ties by the canonical access key below.
Estimates remain advisory metadata and do not affect this policy, even when
known. Existing native costing remains in place; provider cost calibration and
conversion into native units are not initial-release requirements.

Regions containing fused candidates use `:eav-fusion-v1`. Choices are ordered
by source, region, and lowest covered pattern ID. For the same first work item,
prefer entity lookup, then value lookup, then attribute enumeration; within the
same kind, prefer more covered patterns, then the canonical access key. This
preserves single-pattern ordering and does not depend on provider response
order or advisory estimates. The bounded dependency search backtracks when a
preferred choice cannot complete the region, including overlapping coverage,
missing typed inputs, and one-shot conflicts. It selects the first feasible
complete schedule, not a globally minimum number of opens. Explain reports
this policy and the full pattern coverage of each selected access. Cost-based
selection remains a separate follow-up.

After the cost gate passes, compare choices covering the same required work,
including fused access versus equivalent separate accesses within one region.
Different branch predicates or projection boundaries do not describe the same
work. Use lower estimated cost only when the competing choices have comparable,
sufficiently confident, fresh evidence. Provider nanoseconds and native internal scores require an
explicit common-unit conversion; never add them directly. Retain existing core
costing where applicable. If differing core work or unknown binding counts
cannot be costed comparably, use fallback for that decision. Neither stage requires
a whole-query latency estimate nor calibration of every core operator.

The improved access policy is `:eav-access-v1`. With at least medium confidence
and fresh comparable costs for all choices at a decision, minimize estimated
time. Otherwise use this deterministic fallback among legal, dependency-ready
choices within the existing planner: prefer bound-entity lookup, then bound-value
lookup, then attribute enumeration. For equal costs or equal fallback kind,
prefer fewer physical accesses for the same covered clauses when fusion/reuse
is proven safe, then the canonical access key. This is a local access heuristic,
not a promise of optimality or permission to change the query's read semantics.
Unknown evidence at one decision does not invalidate unrelated cost comparisons.

The canonical key contains source symbol, region ID, anchor/covered clause IDs,
access kind, ordered required variables, and stable alternative IDs. Region IDs
come from core's stable normalized-plan traversal, not provider enumeration.
Compare numeric IDs numerically, symbols by qualified name, keywords by
namespace/name, and strings by Unicode code-point order, with type tags. Exclude handles, session/query UUIDs, map iteration order,
provider labels, and OLTP/OLAP labels. Identical metadata and planning inputs
at the same captured planning timestamp produce the same choice regardless of
provider response order. Estimates cannot establish uniqueness, existence,
completeness, or feasibility; hard quotas and all §10.5 constraints still apply.

The only additional planning options are:

```clojure
{:query-source {:planning {:max-alternatives 32
                          :estimate-max-age-ms 300000}}}
```

The alternative cap is positive and applies per source/region call; maximum age
is nonnegative. The region graph and retained requests/responses share the query's
existing planning memory, time, and metadata limits. Regions do not reset those
budgets; limit exhaustion fails instead of omitting a branch or its clauses.
Statistics freshness uses the smaller of provider validity and this age at
core's captured planning timestamp. Existing metadata limits,
cancellation, and planner bounds still apply. There are no estimate-context,
estimate-round, or federation plan-state budgets in the baseline. If the
existing planner cannot produce a complete admitted plan within its bounds,
fail before execution; bound exhaustion must not masquerade as empty data.

V2 explain includes `:region-id` with each selected alternative and cursor so
identical alternative IDs in separate regions remain distinguishable. Coverage
and residual placement refer to their owning region; enclosing rule/branch/set
operations remain visible in the existing engine's plan explanation. These
allowlisted IDs expose no raw bindings or provider plans.

Initial explain records `:policy :eav-structural-v1`, `:selection :structural`,
`:access`, the validated advisory estimate, and actual work separately. It uses
`:estimated-access-ns nil` and `:fallback-reasons []`: structural selection does
not claim to have compared costs. Report known binding/batch caps and
`:planning-time-ms` without computing unavailable cardinality estimates.
These policy markers describe the implemented planner; they add no caller mode
flag or provider protocol negotiation.

The internal `datalevin.query.source.program/explain!` entry point accepts
`[runtime query arguments explain-options]`, with an optional final source
options map. It uses the same input ownership, compilation, selection and
cleanup as the internal v2 query bridge. `:run?` defaults to false and
`:intermediate-counts?` to true. Plan-only calls authorize sessions and select
complete accesses without opening provider execution cursors or requesting
cursor metrics. Public `q`/`explain` route through this bridge.

The internal `datalevin.query.source.driver/run` accepts
`[query arguments explain-options]`, matching the public executor's call shape.
Nil explanation options execute the query; a map selects the explanation bridge.
It uses no native parse/result cache. Both program entry points read embedded
`:query-source` options from map or sequential query syntax and apply the query
timeout. An explicit final source-options map overrides the embedded map;
inherited caps and deadlines still bound the invocation.

Header extraction is a charged, shallow top-level walk, without visiting nested
query constants before local limits are known. The child control applies before
query/input ownership. Context and declared alias overrides validate before
argument consumption and descriptor callbacks. Shared key budgets, nested native
and source queries, and cleanup callbacks inherit the effective child control
and earliest deadline. Budget/deadline bindings restore on every exit; a deadline
cannot extend when cleanup reenters the context. A timeout or cancellation takes
precedence over a new untrusted input exception, with private exception data
redacted. Public `q`/`explain` select this driver lazily only for external inputs.

Its `:sources` distinguish definition fingerprints, scope IDs, data versions
and requested/effective/actual consistency. `:regions` retain the structural
selection fields above and shared operator placement; `:program-id` distinguishes
separately compiled native rule/negative bodies with their own region IDs.
Selected steps include coverage, prefilters, required/output variables, replay,
blocking state, binding caps and query batch caps. Provider labels, plan handles,
authorization, query/binding values and envelope annotations stay private.

Executed output adds `:result-rows` and attributable `:cursors`, qualified by
source, region and alternative. Each cursor retains validated provider
`:metrics`, including unknown nil values. Optional `:actual` counters report
validated batch rows/bytes and engine operator input/output rows, calls and
inclusive elapsed time. Nested operator times are not additive. Disabling
intermediate counts omits those counters and actual cursor input counts while
preserving final result size and provider metrics. Plan-only output invents no
execution counts. Diagnostic storage remains admitted under the invocation
ledger across recursive workspaces and is detached before cleanup.

An ordinary invocation's complete owned empty input is a semantic empty proof:
its regions are pruned before typed access selection even when an empty column
has no narrow type. Descriptors and source authorization still run. Compiled
body templates use placeholder inputs and are excluded from this proof.

The cost follow-up records each access decision's `:policy :eav-access-v1`, `:access`,
`:selection` (`:estimated` or `:structural-fallback`), `:estimated-access-ns`
(`nil` when unknown), conditioning counts/caps, confidence/provenance, and
`:planning-time-ms`. `:fallback-reasons` is a sorted vector drawn from
`:unknown-cost`, `:unknown-inputs`, `:low-confidence`, `:stale-statistics`, and
`:incomparable-costs`, empty for estimated selection. Show fusion rejection or
required-binding failures separately and keep actual metrics separate from
estimates. Explain need not invent a whole-plan elapsed score. All diagnostics
remain bounded and redacted under §8.


### 10.7 Revision conformance and failure gates

Initial v2 use requires core and companion tests covering the revised mapping/
identity foundation, bounded access envelopes, native joins, basic rule
integration, and four shapes:

- native rules alongside external clauses and positive nonrecursive rules over
  mapped attributes execute through the existing rule engine. Compare ordinary
  `%` calls with the same query/rules over eager facts, including bound/unbound
  arguments, source bindings, repeated/composed calls, and empty results.
  Duplicate witnesses and alternative branches collapse at the rule-head set
  boundary before downstream joins/counts; exposed witness variables and caller
  `:with` retain their ordinary meaning. Core fake and companion local suites
  must pass the [required rule cases](../roadmap.md#f1-execution-checks);
- actual delivered planning envelopes isolate sibling rule/union branches,
  nested/repeated calls, and projection/aggregate boundaries. Same-source
  branches with incompatible predicates retain both valid branch results;
  bound inputs and hidden variables obey region scope. Assert `:covers`,
  `:prefilters`, and anchors name only request-local IDs; foreign-region IDs,
  cross-region fusion, and mismatched open handles reject before execution.
  Exercise same alternative IDs in different regions, shared sessions/one-shot
  permissions, quotas, and region-labelled explanation under the
  [F1 region cases](../roadmap.md#f1-execution-checks);
- mapped native/SQL/file/API entities and attributes share source-qualified
  EAV query semantics, including nominal identity, references, and missing
  attributes; tuple source descriptors reject before acquisition, while ordinary
  Datalog tuple inputs and binding/result batches remain valid;
- bound attribute variables, unbound variables, and wildcard attributes agree
  with eager EAV evaluation after core lowering to fixed-attribute accesses.
  Run the [F1 attribute cases](../roadmap.md#f1-execution-checks), including
  runtime dispatch, heterogeneous types/domains, repeated-variable unification,
  attribute projection/`:with`, and lookup-only access with bound entities.
  Validate generated branch coverage and request-local IDs, with no per-row
  replanning. Undeclared keywords match no facts; a declared attribute without
  a complete legal path cannot be silently skipped. Shared one-shot permissions
  and planning limits apply across all generated branches;
- logical-schema validation rejects missing required schema entries, unresolved
  domains, incompatible entity/reference types, and invalid cardinality/absence
  policies before acquisition; querying an undeclared attribute is a separate
  empty-match case;
- scalar/reference many-valued mappings agree with native EAV semantics for
  repeated elements, empty/missing collections, omitted null elements, invalid
  typed elements, reference domains, repeated patterns, independent attribute
  products, `_`, and `:with` through separate accesses and core joins;
- snapshot-scoped export references repeat within the same pinned export and
  retain distinct identical records, but differ across exports even when row
  offsets and field values match; snapshot loss fails, and matching domain/type
  descriptors cannot justify cross-export continuity or maintenance checkpoints;
- native and external fixtures expose equivalent accepted logical guarantees;
  absent single-valued attributes, dangling references, shared-domain mappings
  with different populations, combined mappings with conflicting values, and
  weak reads cannot justify unsafe existence, uniqueness, or replay rewrites;
- logical-guarantee changes invalidate fingerprints and dependent plans while
  statistics changes do not; provider proof artifacts remain outside public
  descriptors and diagnostics;
- all providers and aliases use the revised descriptor and contexts; stale
  revisions, malformed envelopes, and unsupported required features fail before
  acquisition, without a compatibility fallback;
- required-input DAGs, empty dependent inputs, unseeded cycles, binary/decimal
  key identity, duplicate outer multiplicities, nullable inputs, arbitrary
  dependent response order, zero-match IDs, malformed IDs, and row/byte splitting;
- input-free scans and constant-bound lookups omit binding/ID fields and
  association allocations, retain the two-argument batch sizing helper, and
  complete through ordinary exhaustion. Scoped enumeration with runtime inputs
  retains dependent IDs; shape mismatches reject. Scan estimates use complete
  per-invocation work, while empty dependent inputs open no cursor;
- bulk and per-key endpoints charge every request/page/retry, respect shared
  concurrency/rate limits, and clean up after failure between binding batches;
- independent source guarantees coexist, shared aliases use the strongest
  minimum, unsupported conflicts fail, and multi-page weak reads never claim
  statement or snapshot consistency;
- default sessions declare permissions once, omit `:consumption-groups`, and
  use no group IDs in alternatives or opens; all aliases, alternatives, and
  binding batches share the declared session open/concurrency permissions.
  Explicit groups isolate independent resources'
  permissions, including one open each for independent one-shot streams;
  missing/unknown/mismatched IDs and conflicting session forms reject. Aliases
  of the same stream cannot reopen it under either form, failed opens consume
  permission, and bounded complete spools support only the bindings/coverage
  actually captured;
- successful final exhaustion of dependent access completes every requested
  binding, including mixed and all-zero-match requests; final rows retain per-row
  IDs, while empty final batches have empty row/ID vectors. Caps, missing pages, budget exhaustion,
  and early stops never masquerade as complete broader relations or reusable
  complete spools;
- one call per source/region returns lowered EAV accesses and inline estimates;
  entity lookup, attribute enumeration, and optional value lookup integrate with
  the existing planner, retaining typed requirements, batching, and complete
  coverage of every still-possible attribute branch;
- single-clause alternatives and structural selection produce the complete
  supported workflow without fusion or cost ranking. All-unknown estimates
  remain valid; malformed envelopes reject, known estimates cannot change the
  structural choice, and unsafe or infeasible fused-only plans reject before execution;
- ties, reordered provider responses, planner bound exhaustion, and OLTP/OLAP
  relabeling preserve deterministic selection; estimates cannot change result
  correctness, completeness, or quota enforcement;
- revised execution retains native-only routing and the mandatory native
  performance gate. No v2 bridge is activated merely to validate native queries.

The optimization follow-up adds gates before enabling its capabilities:

- fused, separate, and eager execution agree on identity, cardinality-many
  products, missing attributes, references, `_`, and `:with`; distinct entity
  variables/wildcards cannot justify fusion. Cheap invalid fusion is rejected;
- selectivity, distinct keys, full/tail batches, request splitting, and latency
  affect comparable access costs; unknown/stale/missing or incompatible units
  trigger deterministic fallback. Native scores require explicit unit conversion;
- cost crossovers, ties, provider response order, and workload labels preserve
  the specified selection policy; explain reports fusion and cost/fallback
  decisions accurately, and native-only performance remains unchanged.

These checks do not gate the initial separate-access workflow. Live probes,
repeated estimate rounds, and broader search extend the same optimizer under
independent contracts and delivery gates; no parallel planner/executor is planned.

Malformed envelopes use `:query-source/invalid`; no legal capability/dependency
schedule uses `:query-source/unsupported`; known incomplete access uses
`:query-source/incomplete`. Existing authorization, stale-definition, lost-view,
limit, cancellation, timeout, provider-failure, and cleanup categories retain
their meanings. All new errors and explain data follow §8's redaction rules.
