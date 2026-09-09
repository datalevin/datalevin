# Custom data: ordered storage and indexing

Status: Phases 1–4 are implemented locally: type registration, function
contracts, ordered references, transactional payload storage, public KV custom
keys and ordered list items, and Datalog custom attributes. Phase 5 now supports
remote registration and server execution for values supported by the existing
wire codec, plus Java, Python, and JavaScript registration and UDF APIs.
Local Python and JavaScript native-value adapters, including query spilling,
are implemented. Phase 5 includes remote native values: database-scoped server
readers, caller registry readers, streamed batches, transactions, and durable
response replay. Separate-process Python and JavaScript tests exercise this path.
Phase 6 now includes reproducible JVM storage and brief Python adapter
measurements, with validation of results, payload cleanup, and lazy decoding.
The agreed initial implementation scope is complete; the measured costs below
remain performance work, particularly custom writes and collision matching.

Tracking issue: [Allow indexing of arbitrary data, #234](https://github.com/datalevin/datalevin/issues/234).

## Goal

Allow applications to store and index custom values through both the KV and
Datalog APIs. Applications supply an order function that returns values of
existing ordered backing types. Datalevin supplies the index encoding,
ordered access, payload storage, and value matching.

Each custom type has two parts:

| Part | Definition | Purpose |
| --- | --- | --- |
| Index | An order function and its scalar or tuple backing type | Determine ordered storage using existing codecs |
| Payload | Serialization and deserialization of the complete value | Reconstruct the original value, including its unindexed information |

Nippy is the default payload serializer. Optional payload functions support
values that need application or language-specific serialization. The order key
does not need to be reversible. The payload contains the whole value,
including information also present in the order key.

The [Datomic type extensions library](https://github.com/magnars/datomic-type-extensions)
provides useful precedent for registering a type with an existing backing type.
Our design adds a separate payload and implements the mechanism inside storage,
so normal KV and Datalog operations continue to exchange complete logical values.

## Agreed boundaries

- Implement the common facility at the KV storage level. Datalog attributes
  use the same registry, order functions, and payload references.
- Use existing ordered scalar encodings or tuples of supported scalar types.
  Users do not write ordered byte encoders or supply a separate comparator.
- Applications accept the ordering and query consequences of their chosen
  `:order-fn`. Document this responsibility; Datalevin does not infer or repair
  compatibility with arbitrary application predicates.
- Treat identity, candidate matching, and payload indirection like giant
  values. These are storage responsibilities, transparent to the query layer.
- Retain the existing key-size limits and giant-value tradeoffs. Account for
  reference overhead within the existing size budget. Removing these limits
  or redesigning ordered overflow storage is outside this work.
- Scope type registration to a KV environment. All DBIs and Datalog
  attributes in that environment share it; other environments are independent.
- Persist registrations in `datalevin/kv-info`.
- Support Clojure `inter-fn` functions and non-Clojure UDF descriptors for
  order functions and optional payload functions.
- Keep Nippy as the default general serializer. This work does not change
  Nippy's format or require a Rust storage/query implementation.

The initial implementation has one order function per registered type.
A tuple supplies one lexicographic order; independent indexes for different
components or multiple named order functions are later extensions.

## Type definition and registration

Public signature:

```clojure
(d/register-type kv-or-conn type-name definition)
```

Implemented for local and remote KV handles and Datalog connections; returns
`type-name`.
`:version` defaults to `1` and must be a positive integer. `:payload` defaults
to `:nippy`. A transaction handle makes registration part of that transaction,
including remote KV and Datalog write transactions.

`type-name` is a namespaced keyword. A KV handle registers directly. A Datalog
connection resolves its underlying KV handle using `datalog-kv` and delegates
to the same implementation. Registration through either handle is visible to
both APIs.

The examples use `:index :type` for a backing-type declaration: a KV scalar
type keyword, or a vector of scalar type keywords for a tuple. Normalize this
declaration to existing codec descriptors during registration.

Supported scalar keywords are `:keyword`, `:symbol`, `:string`, `:boolean`,
`:long`, `:double`, `:float`, `:instant`, `:uuid`, `:bytes`, `:bigint`, `:bigdec`,
and `:id`. Tuple declarations are nonempty vectors of these types except
`:bytes` and `:id`. Each vector declares a fixed arity, including a one-element
vector. Existing homogeneous-tuple nil rules apply to that one-element case.

```clojure
(require '[datalevin.core :as d]
         '[datalevin.interpret :as i])

(def task-type
  {:version 1
   :index {:type     [:long :string]
           :order-fn
           (i/inter-fn [task]
             [(:priority task)
              (clojure.string/lower-case (:name task))])}
   :payload :nippy})

(d/register-type kv :app/task task-type)
;; The same operation through a Datalog connection:
(d/register-type conn :app/task task-type)
```

For example, a task with priority `3`, name `"Compile"`, options, and metadata
has an order key of `[3 "compile"]`. Its payload retains the original
name, options, metadata, and every other part of the task.

The registry stores one definition per type, conceptually:

```text
[:types :app/task] -> task-type-definition
```

Registration must:

1. Validate the name, version, backing types, callable definitions, and payload
   function pairing before committing metadata.
2. Persist a normalized definition and advance a registry revision in the
   same transaction.
3. Treat registration of the same normalized definition as idempotent.
   Compare persisted function source/captures or UDF descriptors, rather than
   the identity of newly materialized function objects.
4. Reject a conflicting registration. Changes to indexed representations or
   payload codecs require an explicit migration; registration does not
   silently reinterpret existing data.
5. Publish cache changes after a successful commit and make other open
   handles refresh stale registry state before using a type.

Load the registry from `kv-info` on reopen. Persist descriptions and function
source where applicable; keep resolved callables and host runtime handles in
runtime state. Include registry metadata in the existing copy, dump/restore,
and transaction-log paths.

The local implementation stores each definition at `[:types type-name]` with
key type `[:keyword :keyword]`, and increments `:custom-types-revision` in the
same write transaction. Interpreted functions retain Nippy-encoded source and
captures under `:inter-fn/source`; opening metadata does not compile functions.
Revision checks refresh the committed registry cache. Writes use a separate
cache for the active native transaction, reusing definitions and functions
across rows. Registry revisions and UDF generations invalidate its contents;
a new writer, including a map-resize retry, gets a fresh cache. Aborted
definitions and writer-bound resolver closures never enter the committed cache.

## Order and payload functions

The contract for `:order-fn` is:

```text
order-fn(complete-value) -> order key (backing scalar or tuple)
```

The output must match the declared scalar type or tuple arity and component
types. Apply the existing backing types' validation and nil rules. The order
function must be deterministic for a registered version: insertion, lookup,
deletion, and preparation of logical range bounds must obtain the same result
for the same value. Values participating in a transaction's ordered collections
must remain stable; comparators may reuse their encoded order keys. Existing
backing-type rules determine index order.

The optional payload function contract is byte-oriented:

```text
serialize(complete-value) -> payload bytes
deserialize(payload bytes) -> complete-value
```

With `:payload :nippy`, these operations use the existing Nippy serialization
path. When applications supply functions, require both members of the pair.
Their bytes are the payload, stored without another mandatory Nippy wrapper.
Applications may implement the functions using Nippy or another serializer.
Deserialization must reconstruct the complete logical value under the same
value semantics used by the application and existing storage operations.

This keeps serialization independent of the order function. Changing only a
payload representation need not change the index order, but still needs a
compatible decoder or explicit payload migration. Changing the order function
requires rebuilding affected index entries.

### Clojure and UDF execution

`inter-fn` provides persisted source and serializable captured values for
Clojure functions. Use the existing source validation and materialization
path, then cache the callable.

Non-Clojure functions use the existing UDF descriptor, binding, and resolver
mechanism. The UDF kinds are `:order-fn`, `:serializer`, and
`:deserializer`:

```clojure
{:version 1
 :index {:type     [:long :string]
         :order-fn {:udf/lang    :python
                   :udf/kind    :order-fn
                   :udf/id      :app/task-index
                   :udf/version 1}}
 :payload
 {:serialize {:udf/lang    :python
              :udf/kind    :serializer
              :udf/id      :app/task-serialize
              :udf/version 1}
  :deserialize {:udf/lang    :python
                :udf/kind    :deserializer
                :udf/id      :app/task-deserialize
                :udf/version 1}}}
```

Persist the descriptors in the type definition. The runtime executing the
operation supplies their bindings or resolvers. A persisted descriptor does
not itself contain Python or JavaScript code. Resolution must work from a KV
handle as well as through a Datalog connection, and cached callables must
respect the existing UDF registry generation.

Local KV handles accept `{:runtime-opts {:udf-registry registry}}` in `open-kv`
options. Runtime options and compiled caches are neither persisted nor exposed
through environment options. Local Datalog environments accept the same runtime
options when first opened. Resolution is lazy per function
and cached per KV environment, type, registry revision, and UDF generation.
Registration validates UDF descriptors without requiring bindings to be loaded.

Remote registration requires database alter permission and participates in the
server's write admission and read-only replica checks. Definitions travel as
Nippy bytes, including interpreted function source and captures. The server
checks permissions before decoding and validates the definition before storing
it. Registration retries remain idempotent; conflicting definitions are rejected.

For remote operations, order and payload functions execute on the server.
Install UDF bindings there through the existing
`datalevin.server/*server-runtime-opts-fn*` hook. Those bindings are attached to
the underlying KV environment for both KV and Datalog stores, including stores
reopened from persisted sessions after server restart. Changing a registry's
generation refreshes the callable cache. Missing bindings return structured
errors without exporting runtime handles or committing partial writes.

The remote path accepts logical values through the existing Nippy wire codec.
Custom payload serde controls their stored bytes. Native values have a separate
wire form carrying the registered type name and serde-produced bytes. Its reader
must be supplied by the receiving runtime; sender codec IDs and byte equality
cannot substitute for the receiver's deserializer and logical equality. Native
class bindings install the caller's reader automatically.

The server first reads request routing fields with inert placeholders, then
decodes the original bytes on the authorized database/transaction context. The
second read constructs maps and sets using the receiver's logical equality.
Ordinary messages need only one read. Streamed input stays in the same context;
after a decode failure the server drains the batch transfer before reporting the
error, without committing any items. Client readers come from the handle's UDF
registry and are shared with its transaction and retry clients. Runtime handles
are removed from open options before transport.

Client write-retry hashes use the native wire form. Saved transaction responses
containing native values use version-2 client-operation records with
`:response-wire` bytes, decoded with the server's current registry on replay.
Ordinary version-1 response records remain unchanged. These records contain no
process-local codec IDs. Missing caller readers fail without retrying an already
committed write.

The order and serialization functions receive the logical value in their
registered runtime. Use the payload serde functions at language and
client/server boundaries: serialize the native value to raw bytes before
ordinary bridge or message conversion, carry those bytes through the existing
transport, and deserialize in the consuming runtime when the logical value is
needed. The registered type context selects the function pair. Return custom
results through the same byte path and deserialize them in the caller's
runtime. Binding adapters wire this path into ordinary operations; native
objects do not need to be directly serializable by the bridge or message codec.

Missing bindings, function failures, or invalid results must produce actionable
errors. An operation that needs an unavailable function must fail before
committing index or payload changes. A registered version must retain the same
behavior across runtimes and reopens.

## Storage representation

Assign a database-wide 64-bit value ID to each new stored custom-value
occurrence. It is an internal storage reference, distinct from a Datalog entity
ID. The initial design does not share payload ownership across unrelated
logical entries. A custom key with duplicate values owns one key payload for
the entire group; each custom duplicate item owns its own payload. EAV and AVE
refer to the same payload for one Datalog fact.

Use one shared payload DBI per KV environment, `datalevin/custom-values`:

```text
value-id -> payload bytes
```

The owning DBI or Datalog attribute identifies the registered type used to
decode the payload. Default payload bytes are Nippy serialization of the
complete value; registered payload functions can replace that serialization.

Logical index layouts are:

| Use | Key | Value / duplicate item |
| --- | --- | --- |
| KV custom key | `[order-fn(key), value-id]` | Associated KV value |
| KV ordered custom list item | Ordinary key | `[order-fn(item), value-id]` |
| Datalog EAV | Entity ID | `[attribute-id, order-fn(value), value-id]` |
| Datalog AVE | `[attribute-id, order-fn(value), value-id]` | Entity ID |

The reference component has the following byte layout, implemented in
`datalevin.custom-value`:

```text
F0 | framed native order-key bytes | 00 00 | value-id (8 bytes, big endian)
```

`F0` is the custom-reference discriminator, reserved separately from existing
scalar, tuple, and giant representations. The native order key comes from the
existing KV scalar or tuple encoder. Within that byte sequence, escape each
zero as `00 FF`; leave other bytes unchanged. The `00 00` terminator makes a
complete key sort before its extensions, independently of the appended ID.
Escaping preserves the native codec's unsigned byte order, including embedded
zero bytes. The payload supplies the original value; decoding an order key is
not required to reconstruct it.

The total reference budget defaults to 511 bytes, including the discriminator,
terminator, and ID: 11 framing/reference bytes plus any zero-byte escaping.
Callers embedding a reference reserve their own prefix/trailer space by passing
a smaller budget. If an encoded key exceeds this budget, retain the longest
whole escaped prefix that fits and terminate it with `00 01`. Exact lookup then
matches complete payloads within that truncated bucket, as with giant values.
Range precision is limited to that bucket; this retains the agreed bounded-key
tradeoff. Existing tuple component-size restrictions still apply.

Datalog embeds the same reference in the existing AVG layout:

```text
EAV: entity-id -> attribute-id (4 bytes) | custom reference | 00 01
AVE: attribute-id (4 bytes) | custom reference | 00 01 -> entity-id
```

The two trailing bytes retain AVG's separator and inline-value marker. The
custom reference budget is therefore 505 bytes. Its internal value ID selects
the shared payload; it is independent of the existing giant-datom ID space.
Both indexes use exactly the same reference for a fact. Repeated assertions
reuse it, and retracting the fact removes both index entries and its payload.

Allocated IDs are positive signed 64-bit integers, from 1 through
`Long/MAX_VALUE - 1`. Reserve 0 and `Long/MAX_VALUE` as lower and upper bucket
sentinels. Payload keys use the existing eight-byte `:id` codec; payload values
use `:raw`. The last allocated ID is stored at `:custom-value-id` in `kv-info`
with key type `:keyword` and value type `:data`. Allocation exhaustion and an
already occupied new ID fail without overwriting payloads.

The order key determines index order. The ID follows it and distinguishes
entries with identical order keys. Internal reference ordering must not turn
two different complete values into the same logical value.

### Matching and lifecycle

Use the giant-value matching model:

1. Compute the incoming logical value's order key.
2. Find the bucket of entries with matching order keys, respecting the existing
   bounded-key behavior where applicable.
3. Load candidate payloads by ID and reconstruct their complete values.
4. Apply existing value-equality semantics to identify a match.
5. Reuse the matched entry for replacement or retraction; allocate a new ID
   when inserting a new logical entry.

The order key narrows the candidates, just as a giant value's index prefix
does. Neither the order key nor an allocated ID replaces full-value
matching. No additional user-defined identity or equality function is required.

Allocate IDs under the existing write-transaction coordination. Commit the ID
allocation state, payload changes, and all corresponding index changes
atomically. Deletion and replacement must remove exactly the matched entry.
Handle entries inserted earlier in the same transaction as well as committed
entries.

The shared primitives prepare order keys and serialized payloads before
mutation, then apply the allocation counter, payload, and owning index rows
together under `with-transaction-kv`. Replacement reuses the matched ID and
updates its payload. Allocation has no separate in-memory counter to publish
or undo. A storage error aborts a containing transaction even if its caller
catches the error; preparation errors leave its earlier successful operations
intact. WAL receives the same ordinary KV transaction rows.

Candidate scans and payload reads use one LMDB snapshot. This is necessary for
a reader to resolve its index entries while another transaction deletes their
payloads. Write-transaction scans use that transaction's staged state.

Transaction-local indexes must use the registered order function and backing
ordering too. Their current datom comparators compare the original objects,
which can treat distinct non-Comparable objects of the same class as equal.
Update pending-value lookup, ordered caches, upserts, retractions, and range
reads to preserve full-value matching within each order-key bucket. Distinct
values sharing an order key must remain distinct in memory as well as on disk.
This is internal implementation work; users supply no additional comparator.

Track ownership sufficiently to clean up payloads when an attribute is
retracted, a KV key/list item is removed, or a DBI is cleared or dropped. Reuse
the existing transaction and giant-value lifecycle patterns rather than adding
a separate background consistency mechanism.

Ownership is represented by the live owning index entry, without a separate
reverse mapping or reference-count table. The internal key-index and list-item
primitives delete the matched entry and payload together. Clearing an owning
key index or one ordinary key's item list scans its references and removes
their payloads in the same transaction, without calling user functions. The
allocation/delete-row helpers also allow multiple indexes to share one payload;
their caller must include every associated index change in that transaction.

Open the shared payload DBI before starting an explicit writer transaction.
Native DBI opening itself owns a transaction, so the internal `open-store!`
initializer rejects nested initialization. Public KV adapters initialize this
facility when opening a custom DBI or reopening an environment that has one.
The Datalog adapter also initializes it when installing custom attributes.
Install the first custom index or attribute outside an explicit transaction,
so native DBI creation can complete before a writer starts.

### Range access and transparent reads

Storage prepares bounds using the registered order function and existing
backing codecs. For entries with the same order key as a boundary, an inclusive
endpoint includes the entire ID group and an exclusive endpoint excludes it.
Use internal ID sentinels to construct those bounds; callers do not supply
value IDs.

Storage returns complete logical values. A caller retrieving custom KV keys,
list items, datoms, entities, or query results does not receive an order key or
payload reference in place of the value. Counts and scans that need only
indexed information should avoid loading payloads where their semantics allow.

The upper query layer does not allocate IDs, access the payload DBI, resolve
collisions between order keys, or deserialize payloads. Keep these details
behind storage interfaces, as with giant values. This plan does not require a
new query operator to expose payload indirection.

Applications are responsible for choosing an order function whose ordering
and grouping suit their intended queries. Document the consequences of omitted
fields, normalization, colliding order keys, and reversed ordering. For example,
a lowercase order key groups `"A"` and `"a"` for range access, and a descending
numeric order function reverses the ordinary numeric order. An index range
using that order need not agree with a predicate comparing the original values.
Datalevin does not prove compatibility or automatically compensate for the
choice. Exact storage operations still use the complete-value matching
contract above.

Test consistent use of the registered order throughout storage and pending
transaction indexes. Compare indexed and ordinary predicate execution for
order functions compatible with those predicates, and document the application
responsibility for other choices.

### Dump and restore

Dump custom data in dependency order:

1. `datalevin/kv-info`, including the required type definitions, DBI metadata,
   and value-ID allocation state.
2. `datalevin/custom-values`, containing the ID-to-payload mapping.
3. The DBIs whose entries reference that mapping.

Read these sections from the same database snapshot. A single-DBI dump must
also include the required registry and mapping records ahead of that DBI.
Dump and restore payload bytes without invoking application serde functions.

Restore metadata and mapping records before dependent index entries, preserving
the referenced IDs. Restore the allocation state so subsequent writes allocate
above all restored IDs. Validate dependencies and reject conflicting type
definitions or value IDs in a populated destination before applying changes;
never overwrite an unrelated payload merely because its ID matches. Importing
independent environments with ID remapping is a later extension.

Implemented for text and Nippy binary dumps, both full-environment and
single-DBI bundles. Dumps without custom DBIs retain the existing format.
A single-DBI restore retains its original DBI name. An existing custom target
DBI must be empty or byte-identical to the dump; an occupied payload ID is
accepted only when both its bytes and owning entry match. Restore reconciles
the destination and source allocation counters with the highest stored ID.
Restore runs outside an explicit transaction because opening native DBIs owns
a transaction. Dependency and conflict checks finish before DBIs are created;
metadata, payloads, and index rows are then committed together.

Datalog dumps containing custom data use the same physical dependency bundle
in both text and Nippy formats. They include Datalog's internal DBIs, required
type definitions, and only their owned custom payloads. Full-environment dumps
also include user KV DBIs and their payloads. Datalog schema records precede
EAV and AVE; restore verifies that both indexes agree on each payload's owner.
The complete source allocation counter is retained even when the dump omits
payloads belonging to other DBIs. Dumping and replaying these bytes requires
no application function bindings. Rebind functions before reading logical values.

Custom Datalog dumps retain their stored schema. Apply schema changes after
restore; supplying a schema override to their loader or re-index operation is
rejected before destructive work begins.

## KV and Datalog schema use

KV DBIs declare a registered type for custom keys or ordered list items. The
declaration must persist so the DBI's interpretation is stable after reopen.
Extend the existing KV type arguments and DBI options consistently: explicit
operation types must agree with an installed custom-type declaration. Ordinary
DBIs retain their existing type-selection behavior.

The implemented option names are `:key-type` for a custom key and `:value-type`
for a custom ordered duplicate item. Their value is the registered type name.
`:value-type` in this custom indexing facility requires a list/dupsort DBI.
Both options can be used on the same list DBI. Declarations persist in DBI
metadata, and subsequent conflicts are rejected. Omitting an operation's type,
or using its default `:data`, selects the installed custom declaration.
Explicit type arguments must agree with that declaration. A custom type name
on an undeclared field raises an error.
The initial custom indexes require ordinary byte ordering; reverse/integer
comparison flags and fixed-size duplicate flags are rejected before writing.
An installed custom index's duplicate layout and custom-key size budget cannot
be changed by reopening it.

```clojure
(d/register-type kv :app/task task-type)
(d/open-dbi kv "tasks" {:key-type :app/task})
(def task {:priority 3 :name "Compile" :options {:debug true}})
(d/transact-kv kv "tasks" [[:put task :queued]])
(d/get-range kv "tasks" [:all])
;; => [[{:priority 3 :name "Compile" :options {:debug true}} :queued]]

(d/open-list-dbi kv "tasks-by-owner" {:value-type :app/task})
(d/put-list-items kv "tasks-by-owner" :alice [task] :keyword :app/task)
(d/get-list kv "tasks-by-owner" :alice :keyword :app/task)
;; => [{:priority 3 :name "Compile" :options {:debug true}}]
```

Ordinary reads and decoded callbacks return complete values. Explicit raw
callback modes retain their low-level buffer contract. Counts avoid payload
decoding, and lazy range sequences retain one snapshot for their index and
payload reads until closed. Consume them within `with-open`, including when
reading the entire sequence.

Clear removes owning entries and their payloads in the same transaction and
can participate in an explicit writer transaction. Dropping a custom DBI runs
outside an explicit transaction: it commits that cleanup, then drops the empty
native DBI while retaining the writer lock. Neither operation calls application
functions, and neither resets the allocation counter.

Datalog attribute schemas reference the same type name, for example:

```clojure
{:task/data {:db/valueType :app/task
             :db/cardinality :db.cardinality/many}}
```

Resolve the definition through the underlying KV environment. Generating order
keys, handling payloads, checking uniqueness, and matching exact values belong
to the shared storage path. Existing assertion, lookup-ref, retraction, and
query interfaces continue to operate on logical values.

```clojure
(d/register-type conn :app/task task-type)
(d/update-schema conn {:task/data {:db/valueType :app/task
                                  :db/cardinality :db.cardinality/many}})
(d/transact! conn [[:db/add 1 :task/data task]])
(d/q '[:find ?e :in $ ?task :where [?e :task/data ?task]] @conn task)
;; => #{[1]}
(d/index-range @conn :task/data
               {:priority 1 :name ""} {:priority 5 :name ""})
```

Custom attributes support cardinality one/many, identity upserts, unique-value
constraints, lookup references, retractions, query joins, pull, and entity and
datom reads. Exact operations compare complete values, while `index-range`
includes the entire order group at an inclusive endpoint. Pending transaction
indexes use native order prefixes and distinguish complete values within a
group, including non-Comparable objects with identical hashes. Adding uniqueness
checks all complete values in each group, rather than only adjacent IDs.

First opening a local Datalog environment with `:runtime-opts {:udf-registry registry}`
provides the underlying KV environment's default order and serde bindings.
These runtime bindings are not persisted. Remote environments use server-owned
bindings as described above. Cross-language invocation and local native-value
adapters, including automatic remote native-value binding, are implemented.

Changing a populated attribute to or from a custom type requires an explicit
rewrite; the automatic migration from untyped data to built-in types does not
apply. Custom attributes do not currently support fulltext indexing or custom
types as components of Datalog composite tuples.

### Language APIs

KV handles and Datalog connections expose the same registration operation:

| Language | Registration | UDF descriptor factories |
| --- | --- | --- |
| Java | `kv.registerType(name, definition)`, `conn.registerType(name, definition)` | `UdfDescriptor.orderFn`, `.serializer`, `.deserializer` |
| Python | `kv.register_type(name, definition)`, `conn.register_type(name, definition)` | `UdfDescriptor.order_fn`, `.serializer`, `.deserializer` |
| JavaScript | `await kv.registerType(name, definition)`, `await conn.registerType(name, definition)` | `UdfDescriptor.orderFn`, `.serializer`, `.deserializer` |

Definition maps use the Clojure field names (`index`, `type`, `order-fn`,
`payload`, `serialize`, `deserialize`, `version`). Keys accept a leading colon
or omit it. Backing types accept the existing keyword/type descriptors; Java
also accepts `KVType` scalar and tuple specifications. Attribute schemas select
the registered name as `:db/valueType`, and KV DBIs use `:key-type` or
`:value-type`. Operation type arguments accept the same registered name.

Java's `UdfRegistry` has `orderFn`, `serializer`, and `deserializer` registration
helpers. Python has `order_udf`, `serializer_udf`, and `deserializer_udf`
decorators; JavaScript has `orderUdf`, `serializerUdf`, and `deserializerUdf`.
The Python and JavaScript descriptor helpers default to their respective host
languages. All descriptors support the existing version and language options.

Payload callbacks return `byte[]` in Java, bytes-like values in Python, and
`Buffer` or `Uint8Array` in JavaScript. Deserializers receive the corresponding
byte representation. The JavaScript serializer adapter restores `byte[]` after
the interface proxy converts its `Buffer` result into an array. Invalid payload
results fail before committing. Host callback errors retain their message even
when a Java interface proxy wraps them in an exception without a message.

Local Java KV calls can already pass JVM objects directly to order and serde
functions. Local Python and JavaScript handles additionally support native class
bindings as described below, including for remote handles with matching
server-side order and serde implementations.

Examples are in the [Python binding README](../bindings/python/README.md#ordered-custom-types)
and [JavaScript binding README](../bindings/javascript/README.md#ordered-custom-types).

### Native Python values

Associate a Python class with its custom payload descriptors before opening
the local environment:

```python
registry.bind_native_type("app/task", Task, task_type_definition)
opts = {":runtime-opts": {":udf-registry": registry}}
```

This is a runtime binding, separate from the persisted `register_type`
operation. The definition must specify serializer and deserializer UDFs.
Bind one exact Python class per type name within a UDF registry; conflicting
bindings fail. Repeating the same binding is idempotent. Different registries
can bind the same class to different payload codecs without changing other
databases' conversion behavior. Recreate these bindings on reopen, alongside
the UDF implementations.

The handle scopes argument conversion to its registry. Native objects can be
passed directly as custom KV keys, ordered list items, Datalog attribute
values, range endpoints, lookup refs, and query inputs. Typed transaction and
query builders defer their conversion until execution. Returned custom values,
including nested results, entity/pull reads, decoded callbacks, and UDF results,
are reconstructed as Python instances. Borrowed KV handles, write transactions,
simulated database values, and asynchronous transactions retain these bindings.

The bridge serializes a native value into a defensive byte snapshot before
entering the JVM. An internal `NativeValue` carries that snapshot and its live
runtime binding. Storage still writes the registered payload bytes and uses the
existing custom references. Order and payload callbacks receive reconstructed
Python objects. Payload reads need only the deserializer. Binding changes use
the existing UDF generation mechanism, and missing bindings or callback errors
fail without partial writes. A native value tagged with a different registered
type is rejected before indexing it.

Exact matching and query joins use Python `==` on decoded complete values,
including when payload bytes differ for equal values. Each side uses its own
codec, so values from separate runtime registries can compare correctly.
Unhashable Python classes are supported: the JVM adapter currently uses a
constant hash to preserve equality for arbitrary classes. This can make hash
joins and deduplication expensive; Phase 6 includes a brief check of native
equality/hash costs, with deeper tuning deferred to the future Rust core.
The application's equality must behave as a stable equivalence
relation, and serde must reconstruct values under that equality.

This adapter supports embedded operations, including query results spilling to
disk using the temporary binding context described below. Native values in
untyped `:data` payloads and helpers that eagerly convert values without a
database handle remain unsupported. The carrier has no durable encoding.
Remote handles use the caller registry to reconstruct native results. Install
matching order and serde implementations on the server through its runtime
options hook; client callbacks and runtime IDs are not sent to the server.

### Native JavaScript values

Bind a native class before opening a KV environment or Datalog connection:

```javascript
await registry.bindNativeType("app/task", Task, taskTypeDefinition);
const opts = { ":runtime-opts": { ":udf-registry": registry } };
```

This mirrors Python's runtime class binding and is separate from persisted
`registerType`. It uses the definition's serializer/deserializer UDF descriptors.
Match exact class prototypes, reject conflicting bindings, and recreate the
binding on reopen. Built-in JavaScript classes keep their existing bridge
conversion. The same native class can have different codecs in separate
registries; concurrent operations retain their own conversion context.

Native instances work as custom KV keys and ordered list items, Datalog
attribute values, typed query/transaction values, query inputs, and lookup refs.
Reads, pull/entity results, decoded KV callbacks, and query UDF results reconstruct
the native instances. Borrowed handles, explicit KV transactions, simulated
database values, and async transactions retain the runtime bindings. This does
not change the binding's existing limitation on Datalog `withTransaction`.

Serde uses `Buffer` or `Uint8Array` snapshots and may return promises.
Deserializers receive a `Buffer` and must return an instance with the bound
class's exact prototype. A read needs only its deserializer. Missing bindings,
invalid return values, and callback failures return errors; failed writes roll
back index and payload changes together. The existing JVM `NativeValue` carrier
is shared with Python; custom storage payloads have no additional wrapper.

Equality defaults to `node:util.isDeepStrictEqual` on reconstructed values.
It compares complete values rather than serializer output bytes or JavaScript
object identity. For classes with private state or other equality requirements,
provide a runtime equality function:

```javascript
await registry.bindNativeType("app/task", Task, taskTypeDefinition, {
  equals: (left, right) => left.rank === right.rank && left.label === right.label
});
```

The equality function may be async and must return a boolean. It must define
a stable equivalence relation and agree across codecs used together. Deep
equality does not inspect private fields; such classes need an explicit function.
Each value is decoded using its own codec before comparison. The carrier uses
the same constant hash and follows the same limited performance scope as Python.

Map/set construction and EDN-list normalization that can invoke native equality
run asynchronously to let Java call back into JavaScript. Callback conversion
retains its owning registry across awaits. Equality proxies are retained by the
live registry; codec lookup and proxy back-references are weak, and no process-wide
native class mapping or daemon proxy is installed.

As with Python, local query spilling is supported. Native values in untyped
`:data` and eager helpers without a database handle remain unsupported.
Remote handles use the same caller registry, with matching server-side order and
serde implementations. Asynchronous deserializers and equality callbacks run
while the receiving JVM reconstructs values and collections.

### Local native-value spilling

Each spillable vector, map, or set owns a runtime binding table. Its temporary
Nippy records carry a codec ID and payload bytes. Reads restore the native
carrier using that collection's binding table, so values from different codecs
retain their own deserializers and equality callbacks. The table retains one
binding per codec with an empty payload, rather than keeping spilled values in
memory. Emptying a collection releases the table and its temporary database.
Internal spill stores do not keep global executors alive after the last
application database closes.

This encoding is restricted to the owning spill context. Ordinary Nippy
serialization of the carrier and reads without a matching binding fail.
Runtime IDs occur only in disposable spill files. Durable custom payloads use
the registered serde; the native wire form carries type names and payload bytes.

The native wire reader restores each value before Nippy constructs containing
maps and sets, so collection construction uses the receiver's logical equality.
It fails if no receiver binding is installed. Spill and wire snapshots share one
Nippy extension for the carrier class, with distinct payload forms and decoding
contexts; registering two freeze extensions for the same class would replace
the first. Protocol tests cover compressed and uncompressed messages, differing
sender/receiver codec IDs, equal values with different bytes, nested maps and
sets, missing readers, and spilling after receipt.

Spilled maps and sets store hash buckets of complete keys and values. Lookups,
updates, and removals compare decoded keys using logical equality, including
nested native values whose payload bytes differ. Hash collisions retain distinct
keys. Existing in-memory keys stay in memory; after spilling begins, new keys
go to disk even if memory pressure falls, avoiding duplicate entries across the
two portions. Counts track entries rather than hash buckets.

JavaScript reads of collections that may invoke native equality are asynchronous
so the JVM can call back into JavaScript during reconstruction. Tests force
spilling for native result conversion, queries, joins, deduplication, failure
handling, and collection reuse. Large native collision buckets retain the known
constant-hash cost; deeper tuning remains deferred to the Rust core.

## Storage measurements (2026-09-09)

These are small local storage diagnostics, separate from the Nippy codec
comparison. Reproduce from the repository root:

```sh
clojure -M:dev script/custom_data_bench.clj 2048 5 3 /tmp/custom-data-jvm.edn
bindings/python/.venv/bin/python script/custom_data_python_bench.py --output /tmp/custom-data-python.json
```

The scripts create and remove temporary databases. Python uses
`DATALEVIN_CLASSPATH`, or obtains it with `clojure -Spath` when unset.
[Raw samples and environment details](custom-data-performance.edn) accompany
the tables: `:jvm` contains the optimized run and `:jvm-before` retains the
initial baseline. Both used an Apple M3 Pro with 36 GiB RAM, macOS 26.6.2,
JDK 21.0.12.1, and Nippy 3.9.0.

Each JVM case stores 2,048 entries. Results are medians of five rounds after
three warmup rounds, alternating case order. Each round uses a fresh database;
opening, registration, data construction, validation, and closing are outside
timing. Writes and deletes each use one ascending batch, including commit.
WAL and background sampling are disabled; default LMDB sync and DBI flags
remain enabled. Reads run against the just-written database with warm pages.
Exact lookups traverse all keys in reverse order; scans are fully consumed ten
times. These measurements cover neither cold disks nor concurrent workloads,
remote transport, WAL-enabled throughput, or full query execution.

### JVM optimization results

JFR samples of custom KV writes/deletes showed repeated interpreter analysis.
Every write previously reloaded the type registry and bypassed compiled-function
caching. A separate Datalog profile showed registry reads and order-key encoding
inside transaction sorting. The implementation now:

- Reuses registry snapshots and compiled functions within the active native
  writer, with revision/generation checks and isolation from committed readers.
- Prepares each KV key's order prefix once and applies its payload and ordinary
  index rows through one existing WAL-aware transaction call.
- Reuses resolved types and encoded order keys within Datalog comparators.
  Type definitions are immutable; UDF rebinding and native-writer replacement
  invalidate the cached functions and keys.

Write times below are µs per row, measured with the same unprofiled harness and
settings before and after the changes. The full tables below show the new run.

| Custom storage | Before | After | Speedup |
| --- | ---: | ---: | ---: |
| KV scalar, small payload | 31.97 | 5.39 | 5.9× |
| KV tuple, small payload | 35.74 | 6.13 | 5.8× |
| KV scalar, large payload | 35.82 | 8.77 | 4.1× |
| KV scalar, 8 values / order key | 34.49 | 8.51 | 4.1× |
| KV scalar, 64 values / order key | 55.85 | 29.74 | 1.9× |
| Datalog scalar, small payload | 31.66 | 13.63 | 2.3× |

Scalar KV deletion also falls from 31.67 to 18.76 µs per row. Exact reads and
full scans remain broadly similar; collision matching still decodes candidate
payloads. These changes preserve the storage format and full-value matching.
Validation passed 78 focused JVM tests (2,591 assertions) and 66 core smoke
tests (432 assertions). New checks cover aborted registrations reaching the
same revision, writer-specific resolver context, map-resize retries, and UDF
and schema changes after comparator keys have been cached.

### JVM KV

Custom values are maps with `:rank`, `:id`, `:label`, and `:body`. The scalar
order is `:rank`; the tuple order is `[rank label]` backed by `[:long :string]`.
The default 32-character body produces an average 70.75-byte Nippy payload.
The 4,096-character body produces an average 4,135.75-byte payload. Custom
keys map to a separate long value. Built-in baselines use the backing key and
either a long or the same complete Nippy payload as their value. Keeping a
payload directly under a built-in key does not implement custom full-value
matching or retain multiple values with the same order key.

All times below are microseconds. Scan times include returning complete keys
and values. Live bytes count user-index and custom-payload B-tree pages per
entry, excluding registry, free pages, and environment metadata; they are not
serialized sizes or total database file sizes.

| Storage | Write / row | Exact hit | Scan / row | Delete / row | Live bytes / row |
| --- | ---: | ---: | ---: | ---: | ---: |
| Built-in long → long | 0.34 | 0.34 | 0.048 | 11.26 | 32 |
| Built-in long → small payload | 0.76 | 0.80 | 0.477 | 3.70 | 96 |
| Custom scalar, small payload | 5.39 | 4.18 | 0.764 | 18.76 | 144 |
| Built-in tuple → long | 0.76 | 0.47 | 0.302 | 10.16 | 48 |
| Built-in tuple → small payload | 1.16 | 0.92 | 0.745 | 4.01 | 104 |
| Custom tuple, small payload | 6.13 | 4.91 | 0.786 | 18.35 | 152 |
| Built-in long → large payload | 3.58 | 1.83 | 0.841 | 0.83 | 8,216 |
| Custom scalar, large payload | 8.77 | 5.93 | 1.240 | 16.62 | 8,264 |
| Custom scalar, 8 values / order key | 8.51 | 7.36 | 0.769 | 19.68 | 136 |
| Custom scalar, 64 values / order key | 29.74 | 29.35 | 0.788 | 19.34 | 136 |

Collision cases use small payloads and `rank = floor(id / bucket-size)`.
Exact-hit times average over all members, not just the first or last member.
Deletes proceed in insertion order, so each deletion matches the first
remaining member; they do not measure worst-case collision deletion. The large
payload crosses the LMDB overflow-page boundary in both representations, which
explains the jump in live bytes. Delete costs also depend on B-tree layout and
rebalancing; the unusually cheaper large-value baseline is specific to this
ascending batch and should not be generalized to random deletions.

Against the built-in scalar key with the same small payload, custom scalar
writes cost about 7.1×, exact hits 5.2×, and full scans 1.6×. Tuple full scans are
close in this sample, but their writes and exact hits retain substantial
overhead. Custom storage is functionally implemented; these numbers do not
establish performance parity with built-in types.

### Reads that can skip custom payloads

Counts and scans returning only the ordinary long values do not need the
custom key payload. Counts and lazy-first timings are per call; value-only
scans are per returned row. Each count or lazy-first sample repeats 100 times.

| Storage | Count all, µs | Values only, µs / row | Lazy first, µs |
| --- | ---: | ---: | ---: |
| Built-in long → long | 1.36 | 0.038 | 0.79 |
| Custom scalar, small payload | 2.54 | 0.103 | 4.30 |
| Custom scalar, large payload | 2.44 | 0.098 | 5.24 |
| Custom scalar, 64 values / order key | 2.51 | 0.101 | 4.47 |

A separate instrumented deserializer verifies the work performed in a
64-member bucket: the first exact hit decodes 1 payload, the last hit and a
same-order miss each decode 64, and a full scan decodes 64. Counts and
value-only scans decode 0. Taking the first lazy result with `:batch-size 1`
decodes 2: the current shared iterator fetches `batch-size + 1` entries.
Closing the lazy range releases its cursor and snapshot.

The standalone component diagnostic measures about 0.009 µs for direct rank
access, 0.075 µs for the resolved and validated `inter-fn` order callback, and
0.033 µs for a resolved JVM-local UDF callback. These include loop and result
consumption overhead, with function resolution outside timing. Small-payload
Nippy serialization/deserialization cost 0.364/0.409 µs; loading and decoding a
payload by an already-known reference costs 0.820 µs, including acquiring a
read snapshot. This last operation excludes order calculation and candidate
matching. Simple order callbacks and raw serialization account for only a
small fraction of current custom write time. Remaining JVM work includes
candidate scans, payload lookup overhead, and reducing per-row storage calls;
this diagnostic does not attribute their individual shares.

### JVM Datalog

These cases store one attribute per entity, using a built-in long or a custom
scalar with the small map payload. Index caching is disabled (`:cache-limit 0`).
Exact reads use `datoms :ave`; scans use `index-range`; entity reads retrieve
the attribute through `entity`. Results are consumed within timing. Thus this
compares storage APIs and complete-value reconstruction, including the extra
payload work in the custom case, without query/result-cache effects.

| Attribute | Write / row, µs | Exact hit, µs | Scan / row, µs | Entity read, µs | Retract / row, µs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Built-in long | 3.25 | 2.08 | 0.255 | 1.87 | 26.87 |
| Custom scalar | 13.63 | 7.18 | 2.471 | 5.96 | 35.75 |

### Brief Python adapter measurement

These figures retain the initial run, before the JVM optimizations above.
Python adapter tuning and remeasurement are deferred to the Rust core.

Python 3.14.7 and JPype 1.7.1 use the same JVM. This smaller run uses 256 entries,
three warmup rounds, and five measured rounds, again alternating case order.
The native value is a Python `Task(rank, label)` class with a 32-character
label, a scalar order UDF, and JSON byte serde. Its representative payload is
39 bytes at rank 42. The baseline stores long keys and long values; both paths
include the normal Python API conversion and JVM bridge. Scans run five times
and return Python values. JVM startup is outside timing.

| Python KV path | Write / row, µs | Exact hit, µs | Scan / row, µs | Count all, µs | Delete / row, µs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Built-in long | 10.19 | 22.58 | 20.89 | 23.88 | 10.89 |
| Native Task | 170.27 | 137.14 | 69.26 | 26.91 | 136.42 |

Pure Python JSON encode/decode cost 1.220/0.928 µs per value. A JVM loop calling
a minimal numeric Python UDF costs 13.024 µs per invocation, amortizing the
outer JPype call. This measures a minimal callback crossing, not the full
native-value adapter. The larger storage costs include wrapping, conversions,
multiple callbacks, and logical matching. Constant JVM hashes can additionally
make native-key map/set operations and query deduplication expensive; this KV
measurement does not quantify that growth. Substantial bridge and hashing
redesign remains deferred to the Rust core. JavaScript timing is also deferred;
its separate-process correctness and spill tests are already covered in Phase 5.

## Implementation phases

Phases 1–6 are implemented for the agreed initial scope. The measurements above
record current overhead and identify further performance work.

### Phase 1: Registry and function contracts

- Add the KV-level registry implementation and public `register-type` dispatch
  for KV handles and Datalog connections.
- Implement definition normalization, backing-type validation, function
  pairing, idempotency, and conflicting-registration errors.
- Extend `kv-info` persistence/loading and registry revision tracking.
- Materialize `inter-fn` and UDF order/payload functions through existing
  mechanisms, with KV runtime context and cache invalidation.
- Finalize the scalar/tuple declaration grammar and UDF kind names.

Completion: registration survives reopen, is shared through KV/Datalog
handles, remains isolated between environments, and is transactionally visible.

### Phase 2: Shared storage primitives

Implemented in `datalevin.custom-value`. The primitives operate on explicitly
opened internal raw key indexes and duplicate-item indexes. Phase 3 routes
public KV operations through these primitives.

- Add the payload DBI, ID allocation state, and ownership/cleanup bookkeeping.
- Finalize DBI option names and the custom-reference discriminator before
  writing indexed custom values.
- Compile registered backing types into existing scalar/tuple encoders.
- Implement custom reference encoding/decoding and inclusive/exclusive bounds.
- Implement candidate lookup by order key followed by complete-value matching.
- Apply the order function, serialization, and index preparation to the same
  logical value; implement atomic insertion, replacement, deletion, and rollback.

Completion: colliding order keys retain distinct payloads, exact operations
select the correct entry, and failures leave no committed dangling references.

### Phase 3: KV APIs

Implemented in `datalevin.custom-kv` and `datalevin.custom-kv-dump`, with routing
through the KV wrapper and native DBI metadata. Regression coverage includes
collisions, transactions and rollback, range variants, lazy snapshots, write
flags, cleanup, reopen/copy, dump conflicts, and physical WAL replay.

- Wire custom key and ordered list-item declarations into DBI metadata and
  existing transaction/read APIs.
- Support exact lookup, replacement, deletion, forward/backward ranges, range
  counts, and the existing applicable list operations.
- Preserve full-key replacement semantics and list-item matching when several
  custom values share one order key.
- Cover clear/drop, copy, dump/restore, and available transaction-log replay
  paths for registry and payload DBIs.
- Include registry and mapping dependencies in single-DBI dumps. Emit and load
  metadata, then the custom mapping DBI, then dependent DBIs; validate restore
  conflicts and preserve value-ID allocation state.

Completion: direct KV clients can use ordered custom data without managing
value IDs, payload DBIs, or serialization calls around each operation.

### Phase 4: Datalog storage

Implemented in `datalevin.custom-datalog`, the shared index codec, Datalog
storage, and transaction-local comparators. Tests cover complete-value
collisions, custom serde, order boundaries, truncated keys, uniqueness/upserts,
rollback, pending reads, concurrent payload deletion, reopen, backup, and WAL
replay. The query engine continues to consume complete logical values through
the existing storage interfaces.

- Accept registered type names in attribute schemas and resolve their storage
  descriptors through the KV registry.
- Route EAV/AVE writes and reads through the shared custom-value machinery.
- Cover cardinality one/many, repeated assertions, retractions, uniqueness,
  lookup references, and transaction-local reads.
- Adapt transaction-local indexes and datom comparison paths to the registered
  order and complete-value matching, retaining distinct values within a bucket.
- Verify query, pull, entity, and datom APIs return complete logical values
  while storage handles all indirection and collision matching.
- Check that pending and committed indexes use the registered order
  consistently. Check predicate equivalence for compatible order functions and
  document the query consequences of the application's choice.

Completion: Datalog custom attributes work through the existing logical-value
interfaces and share registrations with direct KV users.

### Phase 5: Language bindings and remote execution

Remote registration and server execution are implemented for existing wire
values. Tests cover KV and Datalog operations, order-key collisions, captured
interpreted functions, shared registrations, transaction commit/abort,
permissions, read-only replicas, UDF rebinding, failed writes, and session
reopen after server restart. Language registration APIs, custom UDF descriptor
kinds, and byte-oriented serializer adapters are also implemented. Binding tests
cover scalar/tuple orders, collisions, shared KV/Datalog registrations, payload
round trips, rollback, and reopen/rebind. Local Python and JavaScript native
class adapters also cover noncanonical payloads, unhashable values, exact queries,
upserts, decoded
callbacks, async/simulated transactions, and codec isolation. JavaScript additionally
covers private-state equality, async serde/equality, concurrent codec contexts,
and native map/set input conversion. Local spill transport and forced-spill
binding tests are implemented. Automatic remote native-value transport has
protocol and separate-process binding tests. JVM tests cover independent
readers, collision matching, streamed batches, transaction runners, permissions,
read-only rejection before decoding, repeated missing bindings, response replay
after reopen, and spilling during wire decoding.

- Expose registration and custom-type references through the applicable Java,
  Python, and JavaScript surfaces.
- Add order function, serializer, and deserializer UDF descriptor support and
  invocation adapters.
- Use serde-produced raw bytes for native values crossing language and
  client/server boundaries, including query inputs and returned custom values.
- Carry persisted definitions and payload bytes over existing client/server
  transports and resolve functions in the runtime performing the operation.
- Test missing bindings and reopen/rebind behavior without partial writes.

Completion: non-Clojure applications can register order and payload functions
and use custom values through KV and Datalog APIs.

### Phase 6: Validation, performance, and documentation

Implemented with the existing focused correctness suites and the two diagnostic
scripts above. The scripts check returned values/counts, empty indexes and
payload stores after deletion/retraction, and deserializer counts for collisions
and lazy reads. The JVM run covers scalar and tuple keys, two payload sizes,
collision buckets, direct payload reads, and Datalog storage. The Python run
provides the agreed brief adapter sample. All diagnostics completed successfully;
the Clojure script also passes lint without warnings. Existing core coverage
includes seeded reference-order checks, snapshots, rollback, WAL replay, dumps,
UDF rebinding, and local/remote language values. Production-scale workload
coverage and performance tuning remain follow-up work.

Keep performance work on the interim Python/JavaScript JVM adapters brief:
take a small representative measurement and fix obvious, low-cost issues.
Record remaining costs, including constant-hash behavior, and defer substantial
bridge or hashing redesign to the planned Rust core. Such tuning is not a
completion requirement; prioritize correctness and remaining functional gaps.
The storage comparisons below remain useful independently of the bindings.

- Add focused core tests and broad regression/property coverage in the sibling
  `../dtlvtest` project where appropriate.
- Compare native backing-type operations with custom-value operations for
  writes, exact lookups, ranges, payload reads, and retractions.
- Measure order function cost, UDF bridge cost, payload sizes, scans of buckets
  with colliding keys, and lazy payload loading separately from raw
  serialization throughput.
- Publish examples for scalar and tuple order keys, colliding order keys, and
  non-Clojure serialization/deserialization hooks.
- Document the application's responsibility for its order function and the
  resulting ordering, grouping, and query behavior.

Completion: correctness checks pass and measurements describe the actual
storage overhead without changing the existing Nippy codec comparison.

## Validation cases

- Scalar and tuple order keys at backing-type boundaries, including inherited
  key-size and giant-value behavior.
- Different payloads with the same order key; repeated insertion of the same
  value; equal logical values whose serialized representations differ.
- Exact lookup, replacement, and deletion within large collision buckets.
- Inclusive/exclusive and forward/backward ranges over identical order keys.
- Pending and committed custom values use the same registered order; distinct
  non-Comparable values and colliding order keys survive transaction-local
  lookup, assertion, upsert, and retraction.
- One environment accessed through multiple KV/Datalog handles, separate
  environments with independent registrations, and stale registry caches.
- Reopen and copy/dump/restore with the registry, indexes, payloads, and ID
  allocation state intact.
- Full and single-DBI dumps place required registry and mapping records before
  dependent entries; restore rejects destination conflicts before changes and
  subsequent inserts cannot reuse a restored value ID.
- Atomic rollback after a failure in the order function, serialization, result
  validation, or storage; insertion and deletion in the same transaction.
- Payload cleanup after replacement, retraction, clear, and drop.
- `inter-fn` captures, UDF bindings/resolvers, descriptor versions, and missing
  runtime implementations.
- Nippy defaults and custom payload function pairs, including native-language
  values that require the custom hooks to roundtrip.
- Query and direct-KV results contain complete logical values; internal IDs and
  payload references do not escape through ordinary value-returning APIs.

## Existing implementation touchpoints

| Area | Starting point |
| --- | --- |
| Public KV API and connection dispatch | [core.clj](../src/datalevin/core.clj), [conn.clj](../src/datalevin/conn.clj), [interface.clj](../src/datalevin/interface.clj) |
| KV metadata and native storage binding | [binding/cpp.clj](../src/datalevin/binding/cpp.clj), [lmdb.clj](../src/datalevin/lmdb.clj), [constants.clj](../src/datalevin/constants.clj) |
| Ordered encoding and value retrieval | [bits.clj](../src/datalevin/bits.clj), [index.clj](../src/datalevin/index.clj) |
| Giant-value matching and transactional lifecycle | [storage.clj](../src/datalevin/storage.clj) |
| Transaction-local indexes and value matching | [db.clj](../src/datalevin/db.clj), [datom.clj](../src/datalevin/datom.clj), [db/tx/prepare.clj](../src/datalevin/db/tx/prepare.clj), [db/tx/execute.clj](../src/datalevin/db/tx/execute.clj) |
| Dependency-ordered dumps and raw-byte transport | [lmdb.clj](../src/datalevin/lmdb.clj), [protocol.clj](../src/datalevin/protocol.clj) |
| Schema validation | [validate.clj](../src/datalevin/validate.clj) |
| Persisted functions and UDF resolution | [interpret.clj](../src/datalevin/interpret.clj), [udf.clj](../src/datalevin/udf.clj) |
| Payload codec background | [nippy.md](nippy.md) |

Add a shared custom-data module for definition normalization, registry access,
and compiled storage descriptors as needed. Keep user-facing query operations
independent of the payload mapping and value-ID machinery.
