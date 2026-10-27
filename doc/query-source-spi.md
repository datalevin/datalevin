# Query-source SPI: adapter reference

Implement this interface to query an external data source with Datalevin's
embedded JVM `d/q` and `d/explain`. An adapter exposes typed entity–attribute–value
(EAV) facts and access paths for retrieving them. Datalevin handles joins,
residual predicates, supported rules, aggregation, and query results.

This reference targets **Datalevin 1.2.9** and **SPI revision 2**. The SPI was
introduced in Datalevin 1.2.8; version 1.2.9 adds same-entity fusion and fixes
boxed-integer interoperability. Revision 2 is the only accepted revision. Cost
estimates are advisory; they do not select access paths.

The public namespace is [`datalevin.query.source`](../src/datalevin/query/source.clj).
For mapping compilation, provider packaging, and implementation examples, see
[Developing federation adapters](../../federation/doc/adapter-development.md)
in the `datalevin-federation` project. Links to that project assume sibling
checkouts. Historical envelopes and core development reports are preserved in
[design and implementation notes](query-source-design-notes.md).

- [Version compatibility](#version-compatibility)
- [Implementation order](#implementation-order)
- [Protocols and ownership](#protocols-and-ownership)
- [Source descriptor](#source-descriptor)
- [Logical values and identity](#logical-values-and-identity)
- [Query options](#query-options)
- [Session acquisition and read guarantees](#session-acquisition-and-read-guarantees)
- [Planning requests and alternatives](#planning-requests-and-alternatives)
- [Dependent access](#dependent-access)
- [Cursor batches](#cursor-batches)
- [Resource limits and cancellation](#resource-limits-and-cancellation)
- [Errors, caching, and explanation](#errors-caching-and-explanation)
- [Testing an adapter](#testing-an-adapter)

Map examples below describe data envelopes. Symbols such as `?item`, `$items`,
`control`, and `plan-handle` represent query variables or runtime placeholders;
quote variable symbols when constructing these maps in Clojure code. Fields
shown in a complete envelope are required unless stated otherwise. Unknown
unqualified fields are invalid. Namespaced envelope annotations are allowed
but do not grant capabilities. Logical type parameters and namespaced fields
inside a type descriptor are part of that type's identity.

## Version compatibility

The [1.2.8 release](../CHANGELOG.md#128-2026-10-26) includes the external-source
protocols, EAV scans and dependent lookups, shared resource controls, and
embedded `d/q`/`d/explain` entry points. Adapter artifacts have their own versions
in the federation project.

| Capability | Datalevin 1.2.8 | Datalevin 1.2.9 |
|---|---|---|
| SPI v2 descriptors, single-pattern accesses, dependent bindings | Available | Available |
| Same-entity multi-pattern fusion and `:eav-fusion-v1` selection | Unavailable | Available |
| `Integer`, `Short`, and `Byte` values in `:long` columns and native joins | Affected by the integer coercion regression; use `Long` values | Fixed, with shared integer key equality and hashing |
| Cost-based ranking of external accesses | Unavailable | Unavailable; estimates remain advisory |

Adapters using fusion or boxed-integer interoperability require Datalevin
**1.2.9**. A provided dependency on `1.2.8` does not supply these changes. The
SPI revision remains `2`, so checking `:spi-versions` alone does not establish
compatibility with capabilities added in 1.2.9. Declare and test the minimum
Datalevin version with your adapter. The
[PostgreSQL adapter](../../federation/postgres/README.md) uses same-entity fusion.
See the [changelog](../CHANGELOG.md) for the release changes.

<a id="1-project-boundary"></a>
<a id="11-four-shape-revision-target"></a>
<a id="10-spi-v2-revised-four-shape-contract"></a>

## Implementation order

1. Define the logical facts: stable entity identity, exposed attributes, types,
   cardinality, references, absence, and accepted data scope.
2. Implement a descriptor and one complete access path. An input-free scan is
   useful when the backend permits one; a lookup-only adapter is also valid.
3. Acquire an authorized session with an honest read guarantee. Implement
   resource accounting, cancellation, and cleanup before adding backend I/O.
4. Return validated alternatives from planning without reading source data.
   Open only the selected handle and return bounded, owned batches.
5. Compare queries against complete eager EAV fixtures. Add dependent lookups,
   predicate translation, and fusion only after validating their semantics.

Applications bind the source explicitly:

```clojure
(require '[datalevin.core :as d]
         '[datalevin.query.source :as source])

;; items-source is your IQuerySource implementation.
(d/q '[:find ?item ?name
       :in $items
       :where [$items ?item :item/name ?name]]
     items-source)

(d/explain {:run? false}
           '[:find ?item ?name
             :in $items
             :where [$items ?item :item/name ?name]]
           items-source)
```

<a id="architectural-layers"></a>

| Adapter owns | Datalevin owns |
|---|---|
| Accepted mapping, external identity, conversions, and schema discovery | Query parsing, logical semantics, and typed validation |
| Drivers, credentials, connection pools, and external read contexts | Session grouping and query lifecycle |
| Access translation, pagination, decoding, and backend cancellation | Dependency scheduling, joins, residual evaluation, and result semantics |
| Accurate capability, completion, and resource declarations | Enforcement of shared query limits and redacted diagnostics |

Production adapters and their provider dependencies belong in
`datalevin-federation` or another adapter project. Core does not load them
automatically. The SPI is a trusted in-process interface, not a wire protocol
or provider sandbox. Remote-server adapter execution, transport of live source
objects, native-image execution of adapters, and language-binding entry points
are outside this contract. Combining a live source with a remote database is
rejected before provider acquisition. There is no external write or distributed
transaction interface.

<a id="13-native-execution-isolation-and-performance"></a>

Existing native databases and collection inputs keep their own interfaces;
do not wrap them in `IQuerySource`. Queries containing no external source keep
the native optimizer, execution path, and caches. External sources can join
with local native data through ordinary query source bindings.

<a id="2-public-protocols"></a>

## Protocols and ownership

Implement these protocols from `datalevin.query.source`:

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

| Method | Required behavior |
|---|---|
| `-source-info` | Return immutable metadata without provider I/O. |
| `-open-session` | Authorize every grouped logical view and acquire the requested read context. Return a session; clean up partial acquisition on failure. |
| `-session-info` | Return metadata describing the acquired session without reading source data. |
| `-plan-source` | Return alternatives for the supplied source/region using captured metadata and constants. No scans, counts, samples, backend `EXPLAIN`, or discovery I/O. |
| `-open-cursor` | Validate and open a selected session-owned handle. Clean up partial resources if open fails. |
| `-next-batch!` | Return one bounded, owned batch, or throw. |
| `-cursor-metrics` | Return non-blocking cumulative metrics, including after close. |
| `-cancel-session!` | Promptly signal all pending work, including blocked I/O. Safe to call concurrently and repeatedly; return `nil`. |
| Close methods | Release owned resources idempotently and return `nil`. Session close also closes any remaining child cursors. |

The caller owns the source and any reusable pool. Datalevin owns each returned
session and cursor for one query invocation. Closing a session must not close
a caller-owned source or pool. Calls on an individual session/cursor are
serialized except cancellation; thread affinity is not promised. Marshal calls
internally if the backend requires it.

```text
capture descriptors -> validate -> acquire and authorize sessions
  -> plan each source/region -> validate complete plan
  -> open selected cursors -> consume batches -> close cursors -> close sessions
```

All source sessions are acquired and validated before any source is planned.
All regions are planned before any execution cursor opens. On failure,
Datalevin signals cancellation and closes acquired resources. Cleanup failures
do not replace the primary error. Providers must clean up acquisitions that
throw before returning a session or cursor. Work after close is invalid;
session metadata and final cursor metrics remain readable.

<a id="version-negotiation-and-source-information"></a>
<a id="101-revision-and-descriptor"></a>
<a id="eav-descriptor-contract"></a>

## Source descriptor

`-source-info` returns a complete descriptor such as:

```edn
{:spi-versions #{2}
 :features #{}
 :required-features #{}
 :source-instance-id :example/catalogue
 :compute-context-id :catalogue/read-profile
 :definition-id :catalogue/items
 :definition-fingerprint "sha256:<definition-digest>"
 :pattern-contract
 {:kind :eav
  :attributes {:item/name {:entity-type :long :value-type :string}
               :item/tag  {:entity-type :long :value-type :string}}}
 :logical-schema
 {:domains {:catalogue/item {:entity-type :long}}
  :attributes
  {:item/name {:entity-domain :catalogue/item :cardinality :one
               :absence :no-fact :reference-domain nil}
   :item/tag  {:entity-domain :catalogue/item :cardinality :many
               :absence :no-fact :reference-domain nil}}}
 :read-capabilities #{:snapshot}
 :scope {:id "catalogue:all-items" :kind :full :parameters {}}
 :completion {:domain :finite :extent :all-matches}}
```

This small example uses scalar entity keys. Production mappings often need
nominal external entity-reference types to distinguish unrelated identity
domains; see [logical values and identity](#logical-values-and-identity).

Use `(source/validate-source-info! descriptor)` during adapter development.
Validation returns the descriptor unchanged or throws. Every top-level field
shown above is required.

| Field | Meaning |
|---|---|
| `:spi-versions` | Exactly `#{2}`. Other revisions fail with `:query-source/version`. |
| `:features`, `:required-features` | Sets of keywords; required features must be advertised. Core currently negotiates no optional features, so use empty sets for this contract. An unsupported required feature fails before acquisition. |
| `:source-instance-id` | Non-secret portable identity of the physical data source, stable across logical aliases that share it. |
| `:compute-context-id` | Non-secret portable identity of the execution/read profile used with that source. Together with source identity, determines session grouping. |
| `:definition-id` | Non-secret portable identity of the accepted logical definition. |
| `:definition-fingerprint` | Nonempty string identifying the accepted definition's semantics, not its current data. |
| `:pattern-contract` | `:eav` and the complete finite map of exposed keyword attributes with their entity/value types. |
| `:logical-schema` | Accepted domain, cardinality, reference, and absence guarantees. |
| `:read-capabilities` | Nonempty subset of `#{:snapshot :statement :best-effort}`. A stronger guarantee can satisfy a weaker requested minimum. |
| `:scope`, `:completion` | Accepted data population and completion contract, described below. |

Descriptors are immutable portable metadata, not holders for connections,
functions, credentials, or runtime buffers. Core captures a descriptor once per
source object per invocation. Planning and execution must use that accepted
metadata or fail with `:query-source/stale-definition`. Refreshes affect future
invocations and cannot change the meaning of an existing handle.

### Definition fingerprint

Compute the fingerprint from the accepted mapping/query and dependencies,
complete pattern and logical schemas, identity/conversion rules, scope, and
completion semantics. Cardinality, reference-domain, or absence changes require
a new fingerprint. Statistics and ordinary source-data changes do not.
Object identity, session IDs, and random tokens are not substitutes.

Use the [shared fingerprint contract](schema-definitions.md#3-revisions-fingerprints-and-data-versions)
and, for companion mappings, its
[fingerprint computation](../../federation/federation.md#44-fingerprint-computation).
Other adapters own their versioned digest scheme. Core compares the supplied
string as a whole and forwards it unchanged on open; the provider must check it
against the definition that owns the selected plan.

### Logical schema guarantees

`:logical-schema` has exactly `:domains` and `:attributes`, apart from inert
namespaced annotations. `:domains` is a nonempty map of non-nil portable IDs to
`{:entity-type type}`. It may include reference targets whose attributes are not
exposed by this source. The attribute map must contain exactly the attributes
in `:pattern-contract`.

Each attribute declares all four fields:

| Field | Guarantee |
|---|---|
| `:entity-domain` | Existing domain ID whose entity type equals the pattern's entity type. |
| `:cardinality` | `:one`, `:many`, or `:unknown`. `:one` means at most one distinct logical value per entity, not guaranteed presence. |
| `:absence` | Exactly `:no-fact`: missing or null values contribute no fact. |
| `:reference-domain` | `nil`, or an existing domain whose entity type equals the attribute's value type. It identifies the reference target domain. |

Domain compatibility requires both the ID and the complete entity type.
Matching scalar types or attribute names alone do not establish a shared
identity domain. Reference declarations do not promise target existence or
inverse uniqueness. Cardinality does not prove attribute presence, physical
row count, or permission to change query multiplicity. Combining mappings
requires guarantees valid for their combined facts.

Core validates structure and type agreement. The adapter must establish the
claimed guarantees from accepted constraints, policies, or data validation.
A known violation fails execution with `:query-source/invalid`; do not silently
weaken a guarantee during a query. Physical proofs and provider schema details
stay in the adapter.

<a id="102-accepted-scope-and-completion"></a>

### Accepted scope and completion

| Field | Contract |
|---|---|
| `:scope :id` | Nonempty, non-secret string identifying this accepted scope. One ID cannot denote conflicting accepted parameters. |
| `:scope :kind` | `:full` or `:restricted`, relative to the declared logical relation. Neither bypasses authorization. |
| `:scope :parameters` | Immutable portable map of accepted tenant, repository, partition, or other scope parameters. Must be `{}` for `:full`. |
| `:completion :domain` | Exactly `:finite`. Unbounded subscriptions are outside this contract. |
| `:completion :extent` | `:all-matches`, or `:declared-view` for an explicitly defined bounded/search/capped relation. |

A declared view includes its membership rule, cap, and relevant ordering/tie or
window semantics in the accepted definition and fingerprint. It cannot disguise
an incomplete read of a broader relation. Runtime bindings narrow an access
within its accepted scope; they do not redefine that scope.

Every alternative promises `:completion :all-bindings`. Successful exhaustion
means all matching rows for its exact coverage and supplied bindings have been
returned, including bindings with no matches. Input-free access completes all
matches within the scope and planning constants. Read consistency is a separate
promise; completion alone does not imply a snapshot.

Pagination and retries are provider-owned. A known backend result cap, missing
continuation, omitted page, or partial response fails with
`:query-source/incomplete`, unless the narrower relation itself is the accepted
declared view. Budget, deadline, and cancellation failures retain their own
categories. Never report these conditions as successful exhaustion.

<a id="3-logical-values-and-clause-envelopes"></a>

## Logical values and identity

Use these representations for values returned in typed columns:

| Logical type | Accepted JVM representation |
|---|---|
| `:boolean` | `Boolean` |
| `:long` | `Byte`, `Short`, `Integer`, or `Long`; see the version table above |
| `:bigint` | `java.math.BigInteger` or `clojure.lang.BigInt` |
| `:double`, `:float` | `Double` and `Float`, respectively |
| `:bigdec` | `java.math.BigDecimal` |
| `:string`, `:keyword`, `:symbol` | String, Clojure keyword, and Clojure symbol, respectively |
| `:uuid` | `java.util.UUID` |
| `:instant` | Exactly `java.util.Date`, with millisecond precision; subclasses such as `java.sql.Timestamp` are not accepted |
| `:bytes` | JVM byte array (`byte[]`) |
| `:data` | Supported portable values without a narrower type promise, including immutable `java.time.Instant`, `Duration`, and `Period` |

`:long` follows native `:db.type/long` validation. It preserves the boxed
representation; it does not convert every value to `Long`. An arbitrary-precision
integer is a `:bigint`, even when its value fits in a long. Unsupported or lossy
conversions must be rejected or defined explicitly in the accepted mapping.

Columns have a variable, type, and Boolean nullability:

```edn
{:variable ?item :type :long :nullable? false}
```

A fixed tuple type is `{:kind :tuple :elements [type ...]}` with a nonempty
ordered vector of element types. A custom type is:

```edn
{:kind :custom :name :example/entity-ref :version 1
 :fingerprint "sha256:<type-definition-digest>"
 :parameters {:domain :catalogue/item}}
```

`:parameters` is optional. The complete custom descriptor participates in type
compatibility, including version, fingerprint, parameters, and namespaced
fields. Descriptive annotations should live outside the type map. Validate
types with `(source/validate-type! type)`.

Custom values implement `datalevin.query.source.value/IQuerySourceValue`:

```clojure
(-logical-type [value])   ; complete custom type descriptor
(-retained-bytes [value]) ; conservative positive retained-memory charge
```

Both methods are pure and perform no I/O. The value must be immutable and its
charge must include retained backing storage. This interface does not register
a storage codec or change equality. A value stored in a native database also
needs the database's custom-type registration; planning cannot install one.

Use shared query equality, not SQL collation or an adapter-specific key
comparison. [`QueryKey`](../src/java/datalevin/utl/QueryKey.java) supplies binding
key equality/hash and predicate equivalence. Byte arrays compare by content,
including inside supported collections; metadata does not change query keys.
Fixed-width boxed integers compare by their long value and share the same
key hash, including for negative values. Arbitrary-precision integers and
decimals retain their separate binding-key representations.
Pattern constants are predicate operands: a different numeric representation
can compare equal to a fact even when it is not a valid representation for a
returned typed column. Do not infer no matches merely from that mismatch.

For example, Datalevin 1.2.9 distinguishes binding-key identity from numeric
predicate equivalence:

```clojure
(import '[datalevin.utl QueryKey])

(QueryKey/equals (int -1) (long -1))                 ; true
(= (QueryKey/hash (int -1)) (QueryKey/hash (long -1))) ; true
(QueryKey/equals 1 1N)                              ; false
(QueryKey/equiv 1 1N)                               ; true
(QueryKey/equals 1.0M 1.00M)                         ; false
(QueryKey/equiv 1.0M 1.00M)                          ; true
```

EAV entities and values are non-null. A missing attribute contributes no fact,
even for `_` or an unprojected value. There is no missing-value sentinel.
Many-valued mappings expose distinct non-null facts: duplicate collection
elements do not create extra facts, and empty collections create none.
Independent many-valued patterns form their ordinary join product, including
two occurrences of the same attribute. Shared variables constrain that product.
Keep identity and `:with` bindings needed to preserve observable multiplicity.

External entity references follow the companion's
[nominal identity contract](../../federation/federation.md#51-entity-identity).
They are not native database entity IDs. Scalar natural-key joins remain
ordinary scalar joins. If an export has no stable key, an accepted snapshot
identity can include the export namespace and row locator; reusing an offset in
a different export must not imply the same entity.

## Query options

The query-map `:query-source` value accepts these options:

| Option | Meaning |
|---|---|
| `:authorization` | Invocation-specific authorization context; defaults to `{:principal-id :application}`. |
| `:consistency` | Default requested read minimum; defaults to `:snapshot`. |
| `:sources` | Source-symbol map of `{:consistency ...}` overrides for declared external aliases. |
| `:limits` | Overrides for the shared resource limits shown below. |
| `:planning` | `:max-alternatives` (default `32`) and `:estimate-max-age-ms` (default `300000`). Estimate age remains advisory. |

For example, put limits inside `:limits`, rather than directly under
`:query-source`:

```clojure
(d/q '{:find [?item ?name]
       :in [$items]
       :where [[$items ?item :item/name ?name]]
       :query-source {:limits {:batch-rows 256 :external-requests 32}
                      :planning {:max-alternatives 16}}}
     items-source)
```

The ordinary query `:timeout` supplies the deadline; it is not a
`:query-source` option. Unknown unqualified options are rejected.

<a id="4-query-context-and-session-lifecycle"></a>
<a id="103-per-source-read-requirements-and-actual-guarantees"></a>

## Session acquisition and read guarantees

The common context appears unchanged in acquisition, planning, and cursor-open
requests for a group:

```edn
{:spi-version 2
 :features #{}
 :query-id #uuid "22222222-2222-4222-8222-222222222222"
 :authorization {:principal-id :application}
 :consistency :snapshot
 :control control
 :limits limits}
```

`control` is a core-owned runtime object. `:limits` contains the effective query
limits. `:consistency` is the group's requested minimum, not necessarily its
actual guarantee. Authorization is invocation-specific; a descriptor or saved
mapping never grants access by itself.

Acquisition adds `:source-descriptors`, a vector of all captured logical views
sharing the session, and `:read-requirements`, a map of source aliases to their
requested minima. Authorize every view and scope. Core groups aliases with the
same `:source-instance-id` and `:compute-context-id` and requests their strongest
minimum. Do not invent different identities to evade this grouping or open
separate weaker views for aliases that should share one session.

Callers can request different minima for independent sources:

```clojure
(d/q '{:find [?item ?name]
       :in [$items]
       :where [[$items ?item :item/name ?name]]
       :query-source {:consistency :snapshot
                      :sources {$items {:consistency :best-effort}}}}
     items-source)
```

The default minimum is `:snapshot`. The ordering is
`:best-effort < :statement < :snapshot`. Unsupported group requirements fail
before acquisition. Actual session guarantees must meet the effective minimum
and be supported by every grouped descriptor.

| Actual guarantee | Adapter promise |
|---|---|
| `:snapshot` | All accesses, bindings, pages, and permitted repeated reads observe one pinned logical view. Losing it fails the query. |
| `:statement` | One cursor invocation, including every binding, request, and page, observes one logical view. Separate opens may differ. |
| `:best-effort` | The complete declared access is returned without a stable-view promise, even within one cursor. |

A snapshot for each HTTP request does not establish statement consistency for
a paginated cursor. Caching observed rows does not upgrade the backend's read
guarantee. Independent sources do not acquire a global distributed snapshot.

A complete session-information response using default consumption permissions:

```edn
{:spi-version 2
 :features #{}
 :session-id #uuid "33333333-3333-4333-8333-333333333333"
 :source-instance-id :example/catalogue
 :compute-context-id :catalogue/read-profile
 :authorization-token :catalogue/authorized-policy
 :consistency :snapshot
 :acquired-at-ms 0
 :data-version nil
 :consumption {:opens :many :max-open-cursors 1}
 :request-constraints {:max-concurrent-requests 2 :rate-limit nil}}
```

Report the exact negotiated revision/features and matching source/compute
identities. `:acquired-at-ms` is a nonnegative timestamp. The authorization token
is a non-secret identity of the effective policy, never a credential.
`:data-version` is an exact pinned group-view token when available; otherwise
use `nil`. Statement/best-effort sessions must use `nil`. Snapshot sessions may
also use `nil` when no external version token exists. Non-nil tokens are portable,
non-secret identifiers suitable for explanation, not ordering or cache hints.

<a id="104-consumption-groups-and-replay"></a>

### Consumption permissions and replay

The default `:consumption` applies to all alternatives, aliases, and binding
batches in the session. `:opens :many` allows repeated opens up to the positive
`:max-open-cursors` limit. `:opens :once` allows one open attempt and requires
`:max-open-cursors 1`. A failed open spends that permission; closing does not
restore it. Planning consumes no open permission.

Declare independent groups only for genuinely independent resources. Replace
`:consumption` with a vector of at least two groups:

```edn
{:consumption-groups
 [{:id :items-stream :opens :once :max-open-cursors 1}
  {:id :tag-lookups :opens :many :max-open-cursors 1}]}
```

Session information contains exactly one of these two forms. Explicit group IDs
are unique nonempty keywords/strings. Every alternative then names an existing
ID in `:properties :consumption-group`; core forwards that ID on open. Default
sessions omit this field entirely. An attribute, alias, or alternative over the
same underlying stream cannot claim an independent one-shot budget. Resources
shared across queries also need provider-side ownership enforcement.

Replay is a separate alternative property:

- `:identical`: the same alternative, scope, and typed binding values return the
  same logical row multiset in the session, allowing binding-ID renaming.
- `:unstable`: repeated reads have no such guarantee.

Neither grants another open or upgrades consistency. Rows have no guaranteed
order. Core does not insert an implicit replay spool. One-shot dependent access
must fit all required keys in one legal binding batch or execution fails; do
not truncate or reopen. A prefix or one completed key does not establish a
complete observation for another access.

### Backend request admission

Session `:request-constraints` contains positive `:max-concurrent-requests` and
`:rate-limit`, either `nil` or `{:requests positive-integer :per-ms positive-integer}`.
The rate bounds request starts in a rolling interval. These constraints apply
across every alias, cursor, and consumption group, together with query limits.

Create one Closeable limiter during session setup with
`(source/request-limiter control request-constraints)`, retain it in the
session, and use it for each backend operation:

```clojure
(with-open [permit (source/acquire-request! session-request-limiter)]
  (fetch-page!))
```

Acquire a permit for every actual backend operation, including setup, each
page, retry, and split request. Closing releases live concurrency but does not
refund cumulative request counts or rate history. Waiting is cancellable and
observes the deadline. Close the limiter during session cleanup, and clean it
up if acquisition fails. Limits shared across queries remain provider-owned.

<a id="5-planning-request-and-alternatives"></a>
<a id="12-capability-and-cost-planning-target"></a>
<a id="106-eav-access-planning-and-deterministic-selection"></a>
<a id="106-conditioned-planning-and-deterministic-selection"></a>

## Planning requests and alternatives

Core calls `-plan-source` once for each source alias and logical planning region.
Use only the captured descriptor, session metadata, and supplied clause/input
envelope. Planning must not read data or issue provider calls to measure cost.
Return `{:alternatives [...]}` with no more than `:max-alternatives` entries.
An empty vector means no offered access; it does not mean the relation is empty.
Every returned alternative is validated, including candidates not selected.

### Logical planning regions

A region keeps coverage and predicates inside the relevant conjunction, rule
invocation/branch, and projection boundary. Source equality or matching variable
names do not permit crossing a boundary. IDs identify clause occurrences, so
repeated patterns have separate IDs. Treat source symbols, region IDs, and
clause IDs as query-local identities and use exactly those received.

Positive nonrecursive rules over external attributes and supported native rule
participation are handled by core. Adapters do not flatten rule bodies or query
hidden caller values. Recursive external rules, external negation, and broader
delegation require capabilities beyond this contract. Unsupported queries fail
rather than being approximated.

<a id="attribute-terms-and-access-capabilities"></a>

### Attribute-term lowering

Query-level attributes may be constants, variables, or `_`. Core uses the finite
exposed schema to specialize or dispatch them before asking for accesses.
Provider pattern clauses always have a declared constant keyword attribute.
Core restores any logically bound attribute variable itself.

An undeclared attribute has no facts. A declared attribute without a usable
access path is unsupported, not empty. Lookup-only adapters need not implement
scans; they must decline requests for which no complete access is available.

### Planning request

The request includes all common context fields plus:

```edn
{:source-symbol $items
 :source-descriptor captured-descriptor
 :region-id 4
 :scope-id "catalogue:all-items"
 :clauses [{:id 0 :kind :pattern
            :terms [[:var ?item] [:const :item/name] [:var ?name]]}]
 :bindings {}
 :available-inputs [{:variable ?item :type :long :nullable? false}]
 :required-vars #{?item ?name}
 :demand {:quality :exact :ordering [] :offset 0 :limit nil}
 :max-alternatives 32}
```

| Field | How to use it |
|---|---|
| `:source-descriptor` | The immutable captured definition for this alias. |
| `:region-id`, `:scope-id` | Planning boundary and accepted data scope; they are distinct identities. |
| `:clauses` | This source's lowered patterns and eligible predicates within the region. |
| `:bindings` | Known scalar values valid in this region. Retain them with the plan; do not seek more by inspecting outer rows. |
| `:available-inputs` | Potential typed inputs from query bindings or legal upstream operators. Availability does not prove a schedule exists or supply actual values. |
| `:required-vars` | Live variables needed for joins, residuals, rule outputs, `:with`, and aggregation, not just final projection. |
| `:demand` | Exactly the shown unbounded, unordered, exact demand. A final query limit cannot justify truncating this access. |
| `:max-alternatives` | Positive response cap; the default is 32. |

Terms are `[:var symbol]`, `[:const value]`, and `[:ignore]` for `_`. A pattern
has three terms. Eligible predicate envelopes have this shape:

```edn
{:id 1 :kind :predicate :operator :not= :semantics-version 1
 :args [[:var ?name] [:const ""]]}
```

Eligible operators are `:=`, `:not=`, `:<`, `:<=`, `:>`, and `:>=`, with two
arguments and semantics version `1`. Exact support depends on operand types.
A similarly named SQL operator is not automatically equivalent. Unsupported
predicates remain core work; never discard rows needed by a residual.

### Access alternatives

A complete input-free alternative for the name pattern:

```edn
{:id :item-name/scan
 :plan plan-handle
 :covers #{0}
 :prefilters #{}
 :requires []
 :columns [{:variable ?item :type :long :nullable? false}
           {:variable ?name :type :string :nullable? false}]
 :scope-id "catalogue:all-items"
 :completion :all-bindings
 :access {:kind :attribute-enumeration :anchor-clause 0}
 :properties {:replay :identical :ordering [] :blocking-state :none}
 :constraints {:max-binding-rows 256
               :max-binding-bytes 262144
               :max-request-binding-rows 32}
 :estimate {:startup-ns nil :run-ns nil :rows nil :row-bytes nil
            :requests-per-open nil :transfer-bytes nil :peak-retained-bytes nil
            :confidence :unknown
            :provenance {:kind :unknown :id nil
                         :observed-at-ms nil :valid-for-ms nil}}
 :explain {:label "Item names"}}
```

`:id` is a nonempty keyword/string, unique within the source/region response
and stable for an unchanged structural request. Keep IDs non-secret.
`:plan` is a non-nil opaque handle; it is the one field that may retain private
provider implementation state. Associate it with the session, captured
source/region/scope, clauses, known constants, and declared outputs. Account
for retained state and release it on session close. Do not cache handles
across invocations or allocate backend execution resources while planning.

Current alternatives require `:ordering []` and `:blocking-state :none`.
They do not advertise delegated ordering or an unbounded materialization step.
The required `:explain :label` is a nonempty public string; keep it bounded and
free of backend text or credentials.

`:requires` and `:columns` are ordered vectors of unique variable/type/nullability
columns. Outputs must agree with the covered patterns' declared types or an
explicit required input. Use `[]` when no inputs or outputs are needed; even a
zero-column access must enforce fact existence. Constraints are positive
integers and are required for both input-free and dependent alternatives.

| Access kind | Required anchor |
|---|---|
| `:entity-lookup` | Entity is a constant, a known planning binding, or a declared required input. |
| `:value-lookup` | Value is a constant, a known planning binding, or a declared required input. |
| `:attribute-enumeration` | Neither entity nor value must be bound; explicit scope inputs may still be required. |

`:anchor-clause` identifies an exactly covered pattern. Access kinds do not
imply backend indexes or grant hidden correlation. An entity lookup with a
planning constant may be input-free; enumeration requiring a runtime scope key
is dependent.

### Region coverage and execution identity

`:covers` is a set of request clause IDs implemented exactly. It must include
at least one pattern and the anchor. `:prefilters` is a disjoint set of eligible
predicate IDs implemented as safe superset filters. It cannot contain patterns.
Neither may refer to another request or region. False positives can be removed
by core residuals; false negatives cannot be repaired.

Bindings, required inputs, and returned columns together must supply every live
covered variable needed by the rest of the region. Keep all variables needed
by a covered or prefiltered predicate, even when the predicate is claimed exact.
Core retains residual evaluation. A provider cannot drop hidden identity,
reference, or `:with` variables merely because they are absent from final output.

Core validates a complete dependency schedule before opening any cursor. Each
pattern is covered exactly once; overlapping alternatives compete. Inputs must
be legally supplied before an access runs. Self-supplying inputs and unseeded
cycles are rejected. All handles stay bound to their captured source, region,
scope, definition, and session; opening cannot substitute another plan.

### Same-entity fusion

An alternative may cover several patterns when they share the anchor's entity
variable or the same entity constant under the same declared entity type. They
must remain within one source, region, and accepted scope. Distinct entity
variables and separate `_` entity positions do not imply shared identity.
Cross-entity join pushdown is outside this contract.

For example, a request containing these two patterns on `?item` can use one
wide row shape. The request fragment replaces `:clauses` and `:required-vars`
in the earlier planning request:

```edn
{:clauses [{:id 0 :kind :pattern
            :terms [[:var ?item] [:const :item/name] [:var ?name]]}
           {:id 1 :kind :pattern
            :terms [[:var ?item] [:const :item/tag] [:var ?tag]]}]
 :required-vars #{?item ?name ?tag}}
```

The corresponding fused alternative includes:

```edn
{:covers #{0 1}
 :access {:kind :attribute-enumeration :anchor-clause 0}
 :columns [{:variable ?item :type :long :nullable? false}
           {:variable ?name :type :string :nullable? false}
           {:variable ?tag :type :string :nullable? false}]}
```

This is only a fragment of an alternative; supply every other required field.
The adapter must prove equivalence to the EAV join before advertising it:

- Enforce existence of every matched attribute, including `_` and unprojected
  values. Selecting nullable physical columns alone does not implement a join
  requiring those attributes.
- Preserve independent cardinality-many products, repeated patterns, shared
  variables, references, and bindings used by residuals or `:with`.
- A one-row physical projection needs accepted identity/functional-dependency
  guarantees. A cardinality declaration is not a physical uniqueness proof.
- Preserve the accepted scope and actual read guarantee across all work.

Core checks structural safety, not the adapter's SQL, mapping, or physical
proof. Compare fused, separate, and eager results as described under
[testing](#testing-an-adapter) and the companion's
[fusion prerequisites](../../federation/federation.md#61-fusion-prerequisites).
If fusion cannot be established, offer separate legal accesses or decline it.
One fused invocation uses one open permission; dependent batching may still
require several invocations.

### Estimates and selection

All estimate fields are required, even when their values are unknown. Numeric
fields accept finite nonnegative numbers or `nil`; do not replace unknown with
zero. Input-free access uses the estimate shape shown above. Dependent access
uses this shape instead:

```edn
{:startup-ns nil :per-binding-ns nil :rows-per-binding nil :row-bytes nil
 :requests-per-open nil :requests-per-binding nil
 :transfer-bytes-per-binding nil :peak-retained-bytes nil
 :confidence :unknown
 :provenance {:kind :unknown :id nil :observed-at-ms nil :valid-for-ms nil}}
```

| Field | Meaning |
|---|---|
| `:startup-ns` | Fixed elapsed work per cursor open, excluding session acquisition. |
| `:run-ns` | Input-free elapsed work after startup through exhaustion. |
| `:per-binding-ns` | Additional work per distinct input tuple, including splitting, pages, retries, decoding, and waits. |
| `:rows`, `:rows-per-binding` | Returned rows before core residuals and fan-out, per invocation or input tuple. |
| `:row-bytes` | Mean owned row/container charge, excluding binding-ID envelopes and wire bytes. |
| `:requests-per-open`, `:requests-per-binding` | Fixed and per-input backend request counts. Local work may use zero. |
| `:transfer-bytes`, `:transfer-bytes-per-binding` | Received wire bytes per invocation or input tuple. |
| `:peak-retained-bytes` | Peak provider-owned live state, excluding returned batches and core state. This is not a reservation. |

Confidence is `:unknown`, `:low`, `:medium`, or `:high`; all-nil numeric fields
require `:unknown`. Provenance kind is `:exact`, `:statistics`, `:heuristic`, or
`:unknown`; its ID is a string or `nil`, and both timestamps/durations are
nonnegative integers or `nil`. Condition estimates on the actual scope,
coverage, constants, predicates, types, and binding/request caps. Do not issue
planning I/O to obtain them or count on speculative concurrency.

Current selection is structural and ignores estimate values:

- Regions with only single-pattern candidates report `:eav-structural-v1`.
- Regions containing fused candidates report `:eav-fusion-v1`.

Choices are ordered by source, region, and lowest covered pattern ID. For the
same first work item, entity lookup precedes value lookup, then attribute
enumeration. Within the same kind, wider pattern coverage precedes narrower
coverage; canonical structural identifiers break ties. Provider response order,
opaque handles, labels, and costs do not determine the choice. The dependency
search backtracks to find complete nonoverlapping coverage within its 4096-state
bound. Failure to find a feasible complete plan is `:query-source/unsupported`.
This does not promise minimum latency or a globally minimum number of opens.

<a id="105-dependent-access-and-binding-batches"></a>

## Dependent access

An alternative is dependent exactly when `:requires` is nonempty. Declare only
variables supplied by the request's typed `:available-inputs`, then wait for
core to provide their actual values at open. A required variable may also be
returned, but each returned value must equal its associated input under shared
query-key equality.

For an entity lookup, change the scan example's required inputs and access:

```edn
{:requires [{:variable ?item :type :long :nullable? false}]
 :access {:kind :entity-lookup :anchor-clause 0}}
```

Use a distinct alternative ID and the dependent estimate shape. All remaining
fields are still required. The plan must not assume an input exists just
because it could potentially be supplied.

Core deduplicates ordered input tuples with shared query-key equality/hash,
retains their association to all outer rows, and fans matches back out before
projection, `:with`, or aggregation. An empty dependent input opens no cursor.
Do not use a hidden callback into the outer query or silently drop unsupported
keys. Exact no-match proofs, another complete path, or rejection are required.

A dependent open adds a nonempty binding vector:

```edn
{:bindings [{:binding-id 7 :values [101]}
            {:binding-id 8 :values [102]}]}
```

Binding IDs are opaque query-local nonnegative signed-64-bit integers. Each
open has unique IDs and unique typed input tuples. Values are fixed-arity
vectors in `:requires` order. Borrow them until cursor close; copy and account
for them if retaining them longer. Input-free opens omit `:bindings` entirely.

The effective input row/byte caps are the minimum of query limits and the
alternative's `:max-binding-rows`/`:max-binding-bytes`. Core splits by both.
A single oversized key fails. The provider must respect
`:max-request-binding-rows` when splitting a legal open into backend requests;
`1` supports an endpoint without bulk lookup. Each page, retry, and split
request acquires its own request permit.

<a id="6-cursor-requests-and-owned-batches"></a>

## Cursor batches

### Opening a cursor

`-open-cursor` receives the common context and these fields:

```edn
{:alternative-id :item-name/scan
 :plan plan-handle
 :source-symbol $items
 :definition-fingerprint "sha256:<definition-digest>"
 :scope-id "catalogue:all-items"
 :region-id 4}
```

Dependent opens additionally contain `:bindings`; explicit consumption-group
sessions additionally contain `:consumption-group`. Both fields are omitted
otherwise. There are no planning clauses, descriptor, demand, or available-input
fields on open. Known planning constants remain in the selected handle.

Validate handle ownership, source/region/scope, captured fingerprint, common
context, and any group/input contract before doing I/O. Core forwards the
selected handle by identity. A runtime binding is not permission to replan,
change coverage, refresh metadata, or switch read views.

### Fetching and handing off batches

`-next-batch!` receives `{:control control :max-rows n :max-bytes b}`. These
caps are requirements, not hints. Return one batch at a time:

```edn
{:columns [{:variable ?item :type :long :nullable? false}
           {:variable ?name :type :string :nullable? false}]
 :rows [[101 "Notebook"] [102 "Pencil"]]
 :row-count 2
 :byte-count computed-charge
 :reservation reservation-token
 :exhausted? false}
```

Columns exactly match the selected alternative in order. Rows are owned
fixed-arity vectors of values satisfying their declared types/nullability.
Counts must be exact and within the fetch caps. Return no lazy rows, JDBC
buffers, foreign pointers, or borrowed Arrow memory. Copy reusable buffers and
mutable values before handoff; they must remain valid after the next fetch and
after cursor/session close.

Dependent batches also contain `:binding-ids`, parallel to `:rows`. Every ID
must belong to the current open. Multiple rows per ID and identical rows for
different IDs are legal; do not infer association from output order or values.
Input-free batches omit this field entirely.

The final batch has `:exhausted? true` and may contain rows. An empty dependent
final batch has both `:rows []` and `:binding-ids []`; zero-match bindings need
no placeholder rows. An empty input-free final batch omits IDs. Empty non-final
batches and `nil` exhaustion are invalid. After a validated final batch, core
closes the cursor and does not advance it again.

Use the public charge helpers for the actual owned values:

```clojure
(source/batch-charge-bytes columns rows)             ; input-free
(source/batch-charge-bytes columns rows binding-ids) ; dependent
(source/binding-charge-bytes requires bindings)      ; dependent open
```

`:byte-count` is the helper's conservative retained-memory charge, including
containers, metadata, and retained backing, not wire bytes. A small `subvec`
can retain a large parent; detach it or account for that backing. Custom values
must supply their retained charge. An individual value too large for the cap
fails with `:query-source/limit`.

Reserve before allocation/work, including scratch buffers and copies. A returned
batch transfers a live reservation covering at least its byte charge and one
`:in-flight-batches` unit on this query's ledger. Core accepts that ownership
once and releases it after consumption or separately accounting for retained
state. **Do not release a transferred batch token from cursor/session close.**
Release your own reservations if handoff fails. For a zero-charge final batch,
`:reservation` may be omitted; empty dependent batches still have ID-container
charges, so compute the charge rather than assuming zero.

### Cursor metrics

`-cursor-metrics` returns all these cumulative fields, including after close:

```edn
{:batches 0 :rows 0 :charged-bytes 0
 :read-rows nil :read-bytes nil :external-requests 0 :elapsed-ns nil}
```

Known counters are nonnegative integers; unknown work is `nil`, not zero.
Distinguish physical read rows/bytes from transferred rows and retained charges.
Session setup is charged separately and must not be attributed to each cursor.
Metrics perform no I/O and carry no result data, credentials, or private text.
Ordinary `q` and plan-only `explain` need not request metrics.

<a id="7-resource-limits-early-stop-and-cancellation"></a>

## Resource limits and cancellation

The query-map `:query-source` option accepts these limits. The shown numbers
are current defaults for `:query-source {:limits ...}`; always honor the
effective request values. The map below is also the complete default `:limits`
map supplied to provider callbacks.

```edn
{:batch-rows 4096 :batch-bytes 1048576
 :binding-rows 256 :binding-bytes 262144
 :in-flight-batches 2
 :retained-bytes 67108864 :spill-bytes 1073741824
 :result-rows 1000000 :result-bytes 67108864
 :external-requests 1024 :concurrent-requests 4}
```

All are finite nonnegative signed-64-bit integers. Batch/binding row and byte
caps, in-flight batches, and concurrent requests must be positive. Zero spill
disables spilling; a positive quota does not itself provide a spill operator.
Unsupported retention/spill shapes fail instead of exceeding memory limits.

Limits cover the whole invocation, including provider work, native joins,
residuals, and final results. Nested work shares the ledger and deadline; it
cannot reset spent requests or extend its parent. `:external-requests` is
cumulative and is not refunded. Other ledger categories count live usage.
Batch/binding caps are checked on each batch, separately from the ledger.
Existing query `:timeout` supplies the deadline.

| Public helper | Use |
|---|---|
| `(check-active! control)` | Before open/fetch and during lengthy decode or computation; throws on cancellation, timeout, or closed invocation. |
| `(cancelled? control)` | Read cancellation state. |
| `(remaining-time-ms control)` | Nonnegative remaining time, or `nil` without a deadline. |
| `(register-cancel! control callback)` | Register a prompt, thread-safe cancellation callback; returns a registration. |
| `(unregister-cancel! registration)` | Idempotently remove it; an already dispatched callback may still run. |
| `(reserve! control charges)` | Atomically reserve all requested resource categories, or throw without charging. |
| `(release! reservation)` | Idempotently release live charges; never refunds spent requests. |

Register cancellation **before** blocking acquisition or I/O, including pool
checkout before a session exists. A callback receives a redacted reason map
with `:error` and `:phase`. It must signal backend cancellation promptly without
waiting for the blocked operation's execution lock. Registration after
cancellation signals immediately; callbacks must tolerate cleanup races.
Unregister on completion/failure. Core also signals acquired sessions through
`-cancel-session!`, retaining that registration through session close.

A backend that cannot make acquisition and ongoing I/O deadline/cancellation-aware
cannot be offered as a conforming access. Cancellation, timeout, limits,
snapshot loss, and provider errors fail the whole query. Previously received
rows must not become unmarked partial success.

`d/q` materializes results before returning. Result-size quotas are overflow
errors, never silent truncation. A semantic query `:limit` is applied after the
required joins and residuals; when core can safely stop early, it closes the
cursor without claiming full source exhaustion. Support close at any point.

<a id="8-caching-errors-and-explanation"></a>

## Errors, caching, and explanation

Raise `ex-info` with a stable `:error` and a structural `:phase`. Use non-secret
messages and context; backend messages, SQL, URLs, and causes may contain
credentials or data. Core supplies query/source/region/alternative context and
redacts provider failures. Cleanup errors remain secondary.

| Error category | Use |
|---|---|
| `:query-source/version` | Unsupported SPI revision. |
| `:query-source/unsupported` | Capability, query shape, read requirement, or complete plan unavailable. |
| `:query-source/invalid` | Malformed envelope/value or violated logical guarantee. |
| `:query-source/unauthorized` | Access denied. |
| `:query-source/stale-definition` | Captured definition no longer matches. |
| `:query-source/snapshot-lost` | Required pinned view lost. |
| `:query-source/incomplete` | Access cannot return all promised matches. |
| `:query-source/limit` | Resource quota exceeded. |
| `:query-source/cancelled` | Invocation cancelled. |
| `:query-source/provider` | Other backend/provider failure. |
| `:query/timeout` | Existing query deadline exceeded. |

Any external-source dependency bypasses query-result caching. Definitions,
source objects, and session IDs are not data-freshness tokens. Authorization
runs on every invocation. External plan/row reuse across queries is not granted
by this contract; native-only caching is unaffected.

Plan-only `d/explain` may acquire and authorize sessions, then plan and close
them. It opens no execution cursor. `:run?` defaults to `false`;
`:intermediate-counts?` defaults to `true` when execution is requested. Disabling
intermediate counts retains result sizes and provider cursor metrics.

Explain reports source read guarantees, selected alternatives, coverage,
required/returned variables, structural selection policy, advisory estimates,
limits, and—when executed—actual work. Estimates stay separate from observations.
Opaque handles, provider SQL, authorization tokens, bindings, and row values
are not displayed. Treat IDs and public definition/data-version tokens as
potentially visible; keep secrets out of them. Do not depend on the provider
`:explain :label` being displayed.

<a id="9-source-conformance"></a>
<a id="107-revision-conformance-and-failure-gates"></a>

## Testing an adapter

Test through public `d/q` and `d/explain` with a complete eager EAV relation as
the oracle. Test every advertised path independently; a scan passing does not
certify a lookup, and separate accesses passing do not certify fusion.
Preserve query-key equality and observable multiplicities when comparing
results, especially bytes, decimal values, and `:with`.

| Area | Cases to cover |
|---|---|
| Mapping | Stable/nominal identity, reference domains, exact conversion, absent/null values, empty and duplicate collections, and cardinality-many products. |
| Planning | No data I/O; unknown estimates; reordered alternatives; missing inputs/cycles; repeated clauses; aliases; rule/branch boundaries; invalid unused alternatives. |
| Dependent access | Duplicate outer keys and multiplicity, empty inputs, wrong/missing IDs, multiple matches, zero matches, input/output type agreement, full/tail and oversized binding batches. |
| Fusion | Fused/separate/eager parity; existence for `_` and hidden values; repeated patterns/variables; many-valued products; references; residuals; `:with`; rejection of unrelated entities or scopes. |
| Read guarantees | Shared alias view, independent source minima, weak pagination, pinned version loss, retries, and metadata drift. |
| Completion | Empty/final batches, page boundaries, backend caps, failed continuation, partial responses, and early close. |
| Ownership and limits | Mutable buffers survive advance/close; retained backing accounted; one-shot attempts; row/byte/request/rate/concurrency limits; no live resource leaks. |
| Cancellation and errors | Blocked acquisition/fetch/close, partial opens, cleanup failures, and no partial results or secret-bearing diagnostics. |
| Explanation | No cursor opens in plan-only mode; full coverage and input contracts; estimates distinct from actual metrics. |

Reusable test support lives under
[`test-src/datalevin/query/source`](../test-src/datalevin/query/source/):

- [`conformance.clj`](../test-src/datalevin/query/source/conformance.clj) offers
  `mapped-scan-semantics!` and `mapped-lookup-semantics!` for a source, its complete
  eager facts, and attribute roles. These use your adapter's own nominal values.
- [`mapped_conformance.clj`](../test-src/datalevin/query/source/mapped_conformance.clj)
  exercises public queries with descriptor/fact constructors and explicit
  fixture options. Read its constructor contract before adapting a provider.
- [`transport_conformance.clj`](../test-src/datalevin/query/source/transport_conformance.clj)
  exercises deterministic SQL, file, and HTTP transports. It does not replace
  integration tests against the actual backend.
- [`fake.clj`](../test-src/datalevin/query/source/fake.clj) is a deterministic
  eager fixture illustrating the current envelopes and accounting. It is test
  support, not a production adapter.

From a Datalevin checkout, the core entry-point smoke suite is:

```sh
lein test datalevin.query.source-smoke-test
```

Keep provider-specific fixtures, fault injection, backend integration, and
performance evidence with the adapter project. Test-only helpers are not part
of the runtime SPI and must not become production dependencies.

Features outside this reference—such as borrowed batches, delegated ordering
or aggregation, and planning probes—require an additional supported contract.
See the [extensions appendix](query-source-extensions.md) for design proposals;
those proposals do not authorize current adapters to use their envelopes.
