# Custom data

Custom types let you store complete application values while indexing them by
an order you choose. They work as KV keys, ordered list items, and Datalog
attribute values.

Each type has an **order key** and a **payload**. Your `:order-fn` returns an
existing Datalevin scalar type or tuple to determine index order. The payload
stores the complete value, using Nippy by default or your own serializer.
Reads return the complete value. You do not need to write an ordered byte
encoder or manage separate payload records.

For example, a task can be ordered by its priority while retaining its name,
options, and other fields in the payload.

## Getting started

Register a type before using it in a DBI or attribute schema. Type names are
namespaced keywords. This example registers tasks ordered by priority:

```clojure
(require '[datalevin.core :as d]
         '[datalevin.interpret :as i])

(def task-type
  {:index {:type :long
           :order-fn (i/inter-fn [task] (:priority task))}})

(def compile-task {:priority 3 :name "Compile" :options {:debug true}})
(def test-task {:priority 3 :name "Test"})
(def urgent-task {:priority 1 :name "Fix build"})

(let [kv (d/open-kv "/tmp/datalevin-custom-kv")]
  (try
    (d/register-type kv :app/task task-type)
    (d/open-dbi kv "tasks" {:key-type :app/task})
    (d/transact-kv kv "tasks"
                   [[:put compile-task :queued]
                    [:put test-task :running]
                    [:put urgent-task :queued]])

    (d/get-value kv "tasks" compile-task)
    ;; => :queued

    (mapv (comp :priority first) (d/get-range kv "tasks" [:all]))
    ;; => [1 3 3]

    (set (map (comp :name first)
              (d/get-range kv "tasks" [:closed {:priority 3} {:priority 3}])))
    ;; => #{"Compile" "Test"}

    (finally (d/close-kv kv))))
```

`compile-task` and `test-task` have the same order key but remain distinct KV
keys. Exact lookup compares the complete value. A range endpoint selects an
order group, so the closed range above includes both tasks with priority `3`.

## Defining and registering a type

```clojure
(d/register-type kv-or-conn type-name definition)
```

The first argument can be a local or remote KV handle or Datalog connection.
The function returns `type-name`. Registration is shared by all DBIs and
Datalog attributes in the same database; other databases have independent
registries. Use `(d/datalog-kv conn)` to access a connection's KV store.

| Definition field | Meaning | Default |
| --- | --- | --- |
| `:index :type` | The order key's backing scalar type or tuple declaration | Required |
| `:index :order-fn` | A function from the complete value to its order key | Required |
| `:payload` | `:nippy`, or a map containing both `:serialize` and `:deserialize` | `:nippy` |
| `:version` | A positive integer identifying the definition's version | `1` |

Use `inter-fn` for Clojure functions, as above, or a [UDF descriptor](#udf-bindings).
An ordinary Clojure `fn` cannot be stored as a type function. `inter-fn`
persists its source and serializable captured values.

Supported scalar backing types are `:keyword`, `:symbol`, `:string`, `:boolean`,
`:long`, `:double`, `:float`, `:instant`, `:uuid`, `:bytes`, `:bigint`, `:bigdec`,
and `:id`. Use KV type names such as `:long`, rather than Datalog's
`:db.type/long`.

A tuple declaration is a nonempty vector of scalar types, excluding `:bytes`
and `:id`. It has a fixed arity, including when it has only one component.
The backing codecs' value validation, nil rules, and component-size limits
apply. The order function must return a value of the declared type; for a
tuple, it must return a vector with the declared number of components.

For priority followed by case-insensitive name ordering:

```clojure
(def task-by-name-type
  {:index {:type [:long :string]
           :order-fn (i/inter-fn [task]
                       [(:priority task)
                        (clojure.string/lower-case (:name task))])}})
```

Register this definition under a separate name, such as `:app/task-by-name`.
Its tuple gives one lexicographic order: priority first, then normalized name.
It does not create separate indexes for each component.

Registrations persist across reopen. Registering the same definition again is
idempotent; a conflicting definition is rejected. Changing `:version` does not
replace an existing registration. See [Changing a type](#changing-a-type).

## Choosing an order function

The order function determines which values are adjacent and which values fall
inside an indexed range. Choose it for the operations your application needs.

- Return the same order key for the same logical value on every call, including
  after reopening the database. Equal values must have the same order key.
- Keep values stable while a transaction uses them. Do not mutate fields that
  determine their order or equality during the operation.
- Include enough information to narrow exact lookups efficiently. Many values
  with the same order key require more candidate comparisons.
- Ensure range endpoints contain every field the order function needs. With
  `task-type`, `{:priority 3}` is sufficient; `task-by-name-type` also needs a
  string `:name`.

Exact lookup, deletion, uniqueness, and identity upserts use complete-value
matching. Sharing an order key does not make two values equal. For example,
normalizing a name to lowercase groups `"Compile"` and `"compile"` for ranges
without making their complete values identical.

Inclusive endpoints include every value in the endpoint's order group;
exclusive endpoints exclude the whole group. The relative order of distinct
values within one group should not be used as an application-level ordering.
Add a tie-breaking component to the order key if you need one.

An indexed range follows your order function. It need not agree with a query
predicate that compares the original values. Lowercasing, omitting fields, or
negating a number changes ordering or grouping. Datalevin does not infer a
correspondence between arbitrary predicates and your order function. Choose
compatible order and predicate semantics when relying on indexed comparisons.

## Using custom types with KV

Declare custom fields when opening a DBI:

| Use | DBI declaration |
| --- | --- |
| Custom keys | `(d/open-dbi kv "tasks" {:key-type :app/task})` |
| Ordered custom list items | `(d/open-list-dbi kv "tasks-by-owner" {:value-type :app/task})` |
| Both custom keys and custom list items | `(d/open-list-dbi kv "related-tasks" {:key-type :app/task :value-type :app/task})` |

The custom `:value-type` option requires a list/dupsort DBI. An ordinary DBI's
associated value continues to use the usual KV value types and serialization.

Custom declarations persist. Omitting an operation's type argument, or using
its default `:data`, selects the installed custom declaration. If you provide
an explicit type, it must agree with that declaration. Passing a custom type
name to an undeclared field raises an error.

This example uses the `task-type` and task values from Getting started:

```clojure
(let [kv (d/open-kv "/tmp/datalevin-custom-lists")]
  (try
    (d/register-type kv :app/task task-type)
    (d/open-list-dbi kv "tasks-by-owner" {:value-type :app/task})
    (d/put-list-items kv "tasks-by-owner" :alice
                      [compile-task test-task urgent-task] :keyword :app/task)

    (mapv :priority
          (d/get-list kv "tasks-by-owner" :alice :keyword :app/task))
    ;; => [1 3 3]

    (d/in-list? kv "tasks-by-owner" :alice compile-task :keyword :app/task)
    ;; => true

    (d/del-list-items kv "tasks-by-owner" :alice [test-task] :keyword :app/task)
    (d/list-count kv "tasks-by-owner" :alice :keyword)
    ;; => 2

    (finally (d/close-kv kv))))
```

Custom keys and list items support the applicable exact, forward/backward
range, count, update, and delete operations. Reads and decoded callbacks return
complete values. Explicit raw callback modes retain their buffer-based contract.

Close lazy ranges with `with-open`, even if you only consume their first item:

```clojure
(let [kv (d/open-kv "/tmp/datalevin-custom-kv")]
  (try
    (d/open-dbi kv "tasks")
    (with-open [^java.lang.AutoCloseable rows
                (d/range-seq kv "tasks" [:all] :app/task :data false
                             {:batch-size 32})]
      (first (seq rows)))
    (finally (d/close-kv kv))))
```

The range holds its read snapshot until closed. Counts and scans that return
only an ordinary associated value can avoid deserializing custom keys.

## Using custom types with Datalog

Register through the connection before installing an attribute schema:

```clojure
(let [conn (d/create-conn "/tmp/datalevin-custom-datalog")]
  (try
    (d/register-type conn :app/task task-type)
    (d/update-schema conn
                     {:task/data {:db/valueType :app/task
                                  :db/cardinality :db.cardinality/many}})
    (d/transact! conn [[:db/add 1 :task/data compile-task]
                       [:db/add 1 :task/data test-task]
                       [:db/add 2 :task/data urgent-task]])

    (d/q '[:find ?e :in $ ?task :where [?e :task/data ?task]]
         @conn compile-task)
    ;; => #{[1]}

    (set (map :v (d/index-range @conn :task/data
                                {:priority 3} {:priority 3})))
    ;; => a set containing compile-task and test-task

    (set (:task/data (d/entity @conn 1)))
    ;; => a set containing compile-task and test-task

    (d/transact! conn [[:db/retract 1 :task/data test-task]])
    (finally (d/close conn))))
```

Custom attributes support cardinality one/many, `:db.unique/identity`,
`:db.unique/value`, lookup references, retractions, joins, pull, entities, and
datom reads. All return complete logical values. Two distinct values sharing
an order key can both satisfy a uniqueness constraint; asserting the same
complete value on another entity is subject to the usual uniqueness rules.

`index-range` uses the registered order and includes both endpoint groups.
An exact datom lookup, such as `(d/datoms db :ave :task/data compile-task)`,
selects the complete value instead.

Custom attributes currently do not support fulltext indexing or use as
components of Datalog composite tuples.

## Custom serialization

Nippy is the default payload serializer. To use another representation, supply
both functions in `:payload`:

| Function | Input | Output |
| --- | --- | --- |
| `:serialize` | The complete value | Raw payload bytes |
| `:deserialize` | Raw payload bytes | The complete value |

The bytes are stored as your payload without another Nippy wrapper. The order
key need not be reversible because the payload reconstructs the value.
Deserialization must preserve the complete value according to the equality
semantics used by your application. Equal values do not need to produce
identical serialized bytes.

As with `:order-fn`, use `inter-fn` or a UDF descriptor. JVM serializers return
`byte[]`; Python serializers return bytes-like values; JavaScript serializers
return `Buffer` or `Uint8Array`. The deserializer receives the corresponding
byte representation. See the native-class examples below for complete
serializer/deserializer pairs.

### UDF bindings

UDF descriptors identify application functions by language, kind, ID, and
version. The supported kinds are `:order-fn`, `:serializer`, and
`:deserializer`. For example:

```clojure
(def python-task-type
  {:index {:type :long
           :order-fn {:udf/lang :python :udf/kind :order-fn
                      :udf/id :task/order :udf/version 1}}
   :payload
   {:serialize {:udf/lang :python :udf/kind :serializer
                :udf/id :task/encode :udf/version 1}
    :deserialize {:udf/lang :python :udf/kind :deserializer
                  :udf/id :task/decode :udf/version 1}}})
```

Registration persists these descriptors, not the Python or JavaScript code.
The runtime performing an operation must supply the corresponding functions.
Registration can succeed before the functions are bound; an operation needing
an unavailable function fails.

For local KV handles, supply `{:runtime-opts {:udf-registry registry}}` to
`open-kv`. Supply the same option when first opening a Datalog environment.
Runtime bindings are not persisted: recreate them before using the database
after reopen. Rebinding refreshes resolved functions, but must preserve the
registered type's ordering and payload compatibility.

### Language APIs

| Language | Register through a KV handle or connection | UDF descriptor factories |
| --- | --- | --- |
| Java | `handle.registerType(name, definition)` | `UdfDescriptor.orderFn`, `.serializer`, `.deserializer` |
| Python | `handle.register_type(name, definition)` | `UdfDescriptor.order_fn`, `.serializer`, `.deserializer` |
| JavaScript | `await handle.registerType(name, definition)` | `UdfDescriptor.orderFn`, `.serializer`, `.deserializer` |

Definition maps use the field names shown above. String keys accept an optional
leading colon. Java also accepts `KVType` scalar and tuple declarations.
Python and JavaScript descriptor factories default to their respective host
languages.

Java's `UdfRegistry` provides `orderFn`, `serializer`, and `deserializer` helpers.
Python provides `order_udf`, `serializer_udf`, and `deserializer_udf` decorators;
JavaScript provides `orderUdf`, `serializerUdf`, and `deserializerUdf` methods.
Local Java calls can pass JVM objects directly to the order and payload
functions. Python and JavaScript classes also need the runtime class binding
shown below.

### Native Python values

Bind a Python class to its payload functions before opening the database.
This runtime binding is separate from the persisted type registration.

```python
from dataclasses import dataclass
import json
from datalevin import UdfDescriptor, create_udf_registry, open_kv

@dataclass
class Task:
    priority: int
    name: str

registry = create_udf_registry()
registry.order_udf("task/order")(lambda task: task.priority)
registry.serializer_udf("task/encode")(
    lambda task: json.dumps([task.priority, task.name]).encode("utf-8"))
registry.deserializer_udf("task/decode")(
    lambda payload: Task(*json.loads(payload)))

definition = {
    "index": {"type": ":long", "order-fn": UdfDescriptor.order_fn("task/order")},
    "payload": {
        "serialize": UdfDescriptor.serializer("task/encode"),
        "deserialize": UdfDescriptor.deserializer("task/decode"),
    },
}
registry.bind_native_type("app/task", Task, definition)

with open_kv("/tmp/datalevin-custom-python",
             opts={":runtime-opts": {":udf-registry": registry}}) as kv:
    kv.register_type("app/task", definition)
    kv.open_dbi("tasks", {":key-type": ":app/task"})
    kv.transact([(":put", Task(3, "Compile"), "queued")], dbi_name="tasks")
    assert kv.get_value("tasks", Task(3, "Compile")) == "queued"
    assert kv.get_range("tasks", [":all"]) == [[Task(3, "Compile"), "queued"]]
```

Exact matching and joins use Python `==` on decoded values. Classes do not
need to be hashable. Bind one exact class per type name in a registry, and
recreate the class binding and UDF functions when reopening.

Native instances work as custom keys, list items, attributes, range endpoints,
lookup references, and query inputs. Reads reconstruct instances, including
pull/entity results and decoded callbacks. Local and remote operations and
query spilling are supported. Native instances in untyped `:data` payloads,
and helpers that convert them before a database handle is available, are not
supported. See the [Python binding guide](../bindings/python/README.md#ordered-custom-types).

### Native JavaScript values

Bind the class before opening the database. JavaScript operations and class
binding are asynchronous:

```javascript
import { UdfDescriptor, createUdfRegistry, openKv } from "datalevin-node";

class Task {
  constructor(priority, name) {
    this.priority = priority;
    this.name = name;
  }
}

const registry = await createUdfRegistry();
await registry.orderUdf("task/order", task => task.priority);
await registry.serializerUdf("task/encode", task =>
  Buffer.from(JSON.stringify([task.priority.toString(), task.name])));
await registry.deserializerUdf("task/decode", payload => {
  const [priority, name] = JSON.parse(payload.toString("utf8"));
  return new Task(BigInt(priority), name);
});
const definition = {
  index: { type: ":long", "order-fn": UdfDescriptor.orderFn("task/order") },
  payload: {
    serialize: UdfDescriptor.serializer("task/encode"),
    deserialize: UdfDescriptor.deserializer("task/decode")
  }
};
await registry.bindNativeType("app/task", Task, definition);

const kv = await openKv("/tmp/datalevin-custom-javascript", {
  ":runtime-opts": { ":udf-registry": registry }
});
try {
  await kv.registerType("app/task", definition);
  await kv.openDbi("tasks", { ":key-type": ":app/task" });
  await kv.transact([[":put", new Task(3n, "Compile"), "queued"]], { dbiName: "tasks" });
  console.log(await kv.getValue("tasks", new Task(3n, "Compile"))); // queued
  console.log(await kv.getRange("tasks", [":all"])); // [[Task { ... }, "queued"]]
} finally {
  await kv.close();
}
```

Exact matching defaults to `node:util.isDeepStrictEqual` on decoded values.
For classes with private fields, or a different value-equality rule, pass
`{ equals: (left, right) => /* boolean */ }` as the fourth argument to
`bindNativeType`. Equality must define a stable equivalence relation and agree
across codecs used together. Private fields are not inspected by the default
equality function.

Serializer, deserializer, and equality callbacks may be asynchronous. The
deserializer must return an instance with the bound class's exact prototype.
Recreate the binding and functions on reopen. Native values support the same
custom storage, query, remote, and spilling uses described for Python, with the
same restriction on untyped `:data` and eager conversion without a handle.
See the [JavaScript binding guide](../bindings/javascript/README.md#ordered-custom-types)
for its transaction API and other binding-specific behavior.

## Remote databases

Use the same registration API with a remote handle. Registration requires
database alter permission and is subject to the server's read-only checks.

Order and payload functions execute on the server. Install UDF bindings there
through `datalevin.server/*server-runtime-opts-fn*`; client runtime registries
are not sent to the server. For native Python or JavaScript values, also supply
the caller's registry through `:runtime-opts` so returned values can be
reconstructed. Install compatible order, serializer, and deserializer
implementations in the server runtime.

Supply server bindings on restart and caller bindings when opening remote
handles. Missing bindings produce errors rather than returning undecoded
payloads. See the [server guide](server.md) for connection and server
configuration.

## Transactions, backups, and reopening

Custom payload and index updates commit or roll back together. Registration
participates in an enclosing write transaction when passed its transaction
handle. Open the first custom DBI, or install the first custom attribute,
outside an explicit write transaction. Subsequent data operations use the
normal [transaction APIs](transact.md).

Deleting a key or item, retracting a fact, or clearing a DBI also removes its
owned custom payloads. Drop custom DBIs outside an explicit write transaction.

Use the normal copy and dump/restore APIs. Full and single-DBI dumps include
the type definitions and payload dependencies needed to read custom entries.
Text and Nippy binary dumps are supported. Dumping and restoring stored bytes
does not require application function bindings; reading logical values after
restore does.

Restore into an empty destination when transferring a database. Restore into
an existing destination checks type and payload conflicts instead of merging
unrelated custom indexes. A single-DBI restore retains its source DBI name.
Custom Datalog dumps retain their schema; apply schema changes after restore,
rather than supplying a schema override to the loader or re-index operation.
Run restore outside an explicit write transaction.

On reopen, persisted type definitions and custom DBI declarations are available
again. Reopen the named DBIs as usual. `inter-fn` source and captures persist;
UDF implementations and native-class bindings must be supplied again.

## Changing a type

Treat the registered definition as immutable. Changing an order function can
make existing entries inaccessible under their new order keys. Changing payload
encoding can make existing values unreadable by the new decoder. Rebinding a
UDF does not rebuild indexes or convert stored payloads.

For an incompatible change, register a new type name and rewrite the values
into a new DBI or attribute while the old reader is still available. Validate
the rewritten data before removing the old entries. Incrementing `:version`
alone does not perform this migration.

A populated Datalog attribute cannot be automatically converted to or from a
custom type. A custom DBI's installed type, duplicate layout, and custom-key
size budget cannot be changed by reopening it with different options.

## Limits and performance

Custom types inherit the backing codecs' size and value restrictions. The KV
key limit is 511 bytes, and a custom index entry needs part of that space for
its reference. Long encoded order keys may be truncated to a shared prefix.
Exact operations still compare complete values, but ranges can only distinguish
those prefix groups. Prefer compact scalar or tuple order keys when precise
range boundaries matter. See [general limits](limits.md) for other restrictions.

Each registered type provides one order function. A tuple is one lexicographic
index, not independent indexes on its fields. Custom DBIs require ordinary
byte ordering; reverse/integer comparison flags and fixed-size duplicate flags
are not supported.

Reading a custom value requires loading and decoding its payload. Exact
operations also compare candidates with the same order key; large collision
groups increase that cost. Counts and scans that omit custom values can avoid
payload decoding. Closing lazy ranges promptly releases their snapshots.

Python and JavaScript native values currently share a constant JVM hash so
arbitrary host equality remains correct. Large hash joins, maps, sets, and
deduplication can therefore be expensive even with selective order keys.

The following local JVM measurements illustrate storage overhead. They use
2,048 entries, five measured rounds after three warmup rounds, warm pages,
WAL disabled, and Datalog index caching disabled. The custom value is a map
ordered by a long, with an approximately 71-byte Nippy payload. The KV baseline
stores the same payload directly under a long key; the Datalog baseline stores
a long attribute without that payload. These baselines do not retain distinct
complete values sharing an order key.

| Operation | Built-in baseline, µs | Custom type, µs |
| --- | ---: | ---: |
| KV write / row | 0.79 | 5.62 |
| KV exact lookup | 0.80 | 3.99 |
| KV scan / row | 0.479 | 0.759 |
| Datalog write / row | 3.34 | 13.30 |
| Datalog exact lookup | 2.02 | 6.03 |
| Datalog scan / row | 0.256 | 1.022 |
| Datalog entity read | 1.85 | 4.96 |

Measured on 2026-09-09 with an Apple M3 Pro, JDK 21.0.12.1, and Nippy 3.9.0.
These are small local diagnostics; measure your own queries, payload sizes,
order-key collisions, and concurrency. [Raw samples](custom-data-performance.edn)
include tuple keys, larger payloads, collision groups, and Python adapter costs.
The `:jvm` entry contains the current JVM run. To reproduce the JVM diagnostic:

```sh
clojure -M:dev script/custom_data_bench.clj 2048 5 3 /tmp/custom-data-jvm.edn
```
