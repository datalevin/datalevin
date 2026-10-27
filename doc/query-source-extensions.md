# Query-source Extensions Appendix

Status: proposed optional contracts and companion extensions  
Release placement: [roadmap source milestones](../roadmap.md#source-milestones)

This appendix collects work beyond the [baseline query-source SPI](query-source-spi.md)
and [companion plan](../../federation/federation.md). Extension negotiation follows
[the SPI rules](query-source-spi.md#version-negotiation-and-source-information).

Required input bindings are part of the current
[query-source contract](query-source-spi.md#dependent-access). Probes
and the other protocol extensions below still require separate envelope design
and conformance gates.
DataFusion/Parquet is a companion backend choice, not an SPI feature keyword.
These adapters expose mapped entities and attributes through the revised EAV
interface. Columnar or tuple batches are execution representations; arbitrary
tuple inputs continue to use the existing Datalog facilities. Every extension
uses the same Datalevin query/rule planner and executor. Every optimization
phase, including future probes, repeated estimate rounds, and broader plan
search, extends that existing planner. Providers contribute access
implementations and delegated capabilities.
The [architectural layers and governing invariant](query-source-spi.md#architectural-layers)
apply to every extension: access capabilities determine how an admitted EAV
operation executes while preserving its meaning.

<a id="1-bulk-bindings"></a>

<a id="1-required-input-bindings-revision-input"></a>

## 1. Required input bindings (SPI v2)

The earlier optional `:bulk-bindings` proposal is superseded by the
[authoritative v2 binding contract](query-source-spi.md#105-dependent-access-and-binding-batches).
Use the [revised descriptor](query-source-spi.md#101-revision-and-descriptor).
Replace the prototype's open/batch envelopes in place; there is no v1
compatibility path or bulk-binding feature flag. Dependent access adds required
columns, bounded input batches,
query-key deduplication, per-row response IDs, all-bindings completion through
successful final exhaustion, and request splitting. Input-free access keeps the
plain batch shape and uses no runtime binding vector or row IDs.

The revision also specifies
[per-source reads](query-source-spi.md#103-per-source-read-requirements-and-actual-guarantees),
[session-default consumption permissions](query-source-spi.md#104-consumption-groups-and-replay),
[scope/completion](query-source-spi.md#102-accepted-scope-and-completion), and
[EAV access costs and fallback](query-source-spi.md#106-conditioned-planning-and-deterministic-selection).
Explicit consumption groups are needed only for independent resources requiring
separate open budgets or concurrency permissions; ordinary alternatives and
opens omit group IDs. The current contract supports bounded execution,
deterministic structural EAV selection, and safe same-entity fusion; estimates
may be entirely unknown. Cost-based ranking remains an
[optimization proposal](../roadmap.md#federation-optimization-follow-up). This appendix
adds no alternative envelope. The explicit anchor above remains for existing links.

### Additional conformance

Run the [F1 execution suite](../roadmap.md#f1-core), adding
batched-key association, multiplicity, snapshot, and request-limit checks.
Measure request count under backend parameter limits,
duplicate-heavy inputs, and skewed joins. Add scan-only and lookup-only
providers, native/file/API inputs feeding lookups, absent or unseeded cyclic requirements,
non-bulk endpoints, mixed consistency, session-default permissions and explicit
independent-resource groups, one-shot reuse, truncation, and cleanup.
A provider lacking a batch endpoint may scale requests per key within budgets;
only providers advertising a real bulk operation are held to batched request scaling.

## 2. DataFusion and Parquet

Choose an optional JVM/JNI, Rust library/Arrow C interface, or companion-process
boundary after the first workflows pass. Evaluate streaming, cancellation,
buffer lifetime/copying, failure containment, packaging, and dependency
isolation. Reuse DataFusion Federation/Table Providers inside the selected
backend where they pass conformance. Such a backend executes only the selected
provider access or proven delegated fragment; it does not own Datalevin query
meaning, cross-source joins, rules, or a replacement global optimizer. Pin the tested engine/provider/Arrow
version set and upgrade it as a unit. Add Parquet discovery/execution and rerun
the [F1 execution suite](../roadmap.md#f1-core).

## 3. Conditioned planning evidence

Design a negotiated probe extension separately. Compile selected counts/samples
from the same execution IR and retained existence predicates. Core allocates
one cancellable native/external work budget and can always choose no probes.
Evidence identifies its planning region, population, clause/condition/metadata fingerprints,
bindings, snapshot, cost, and exactness/bounds/confidence. A bounded count is
exact only on exhaustion; an estimated zero never proves emptiness. Exact-zero
shortcuts additionally require the execution read view and complete coverage.
Probe predicates and evidence stay within the originating region's scope;
evidence for one branch cannot establish emptiness or selectivity in another.

Samples identify method, seed, bias, and inclusion probabilities/weights when
known. A row prefix is a biased observation, not a representative sample.
Sampling before filtering measures a different population; report it as such.
Correlated probes use bounded key batches, not N+1 requests. Samples remain
query-scoped source data and are excluded from logs/schema/shared caches.
Evidence from another snapshot is advisory. Execution reuse requires an
explicit same-view continuation contract; otherwise execution restarts normally.
Implement PostgreSQL probes independently of the optional DataFusion backend.

## 4. Other negotiated capabilities

Shared executable-plan caches, borrowed/columnar batches, source limits/order,
and aggregate delegation each need a separate contract and conformance gate.
Aggregate delegation follows the [shared aggregate contract](../analytics.md#6-shared-aggregate-semantics).
These capabilities apply to any access shape that can implement their semantics.
OLAP is a workload/cost profile, not an SPI source kind; a SQL or resource API
label does not imply aggregate, ordering, or limit support. Capability contracts
must specify supported types, exactness, required inputs, output properties,
and interaction with residual predicates, joins, duplicates/`:with`, and
read consistency before advertising support. In particular, a source-local
limit cannot discard rows required by later residual checks or joins.

The [baseline EAV planning contract](query-source-spi.md#106-conditioned-planning-and-deterministic-selection)
integrates entity lookup, attribute enumeration, and optional value lookup with
the existing planner using structural selection. It returns alternatives and
advisory estimates together per source/planning region, permitting unknowns.
Core's [attribute-term lowering](query-source-spi.md#attribute-term-lowering)
specializes bound attributes and enumerates feasible schema domains using those
fixed-attribute accesses. Variable/wildcard attributes are logical EAV terms;
this handling is required in F1 and needs no optional provider feature.
[Region coverage and predicate scope](query-source-spi.md#logical-planning-regions)
apply to every extension; no extension may silently flatten rule/branch or
projection boundaries. Safe same-entity fusion is part of the current contract;
comparable-cost ranking remains a proposed improvement. General cross-entity delegation,
separate enumeration and estimate phases, repeated conditioned estimate rounds,
and broader plan search are later extensions within the existing planner,
beyond F0–F3 requirements. Specify and negotiate any added envelopes and planning
budgets separately; they cannot silently replace the baseline request or make
its unknown-cost path unusable.

For future delegation, extend comparable units and account for transferred
rows/bytes, provider work, startup, and differing local retained state. Retain
the EAV identity/cardinality/existence rules through every translation. For
analytical workloads, measure reductions from exact aggregation and legal ordering/limit pushdown;
maximum pushdown is not the selection rule. Columnar transfer has its own
ownership and lifetime gate and is independent of relational pushdown support.
Keep these extensions separately gated; naming them here does not enable them
in the baseline or change their roadmap placement.

Server, native-image, and language-binding modes need their own supported matrix.
External CDC/IVM needs a separate change-data contract under the
[IVM input scope](../IVM.md#23-non-goals-for-the-positive-core).
Distributed shuffle, arbitrary SQL/functions, automatic cross-system identity,
and unrestricted streams remain separate future work.
