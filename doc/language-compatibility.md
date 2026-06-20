# Language Compatibility Matrix

This matrix tracks the public Datalevin API surface across Clojure, Java,
Python, and JavaScript. It is meant to make parity gaps explicit before 1.0.

Status key:

* Yes: public, idiomatic API exists.
* Partial: usable API exists, but ergonomics or coverage differ.
* No: no public API in that language yet.
* N/A: the Clojure API is already native data or a lower-level implementation
  detail, so a wrapper is not needed.

## Datalog

| Capability | Clojure | Java | Python | JavaScript |
| --- | --- | --- | --- | --- |
| Open local Datalog connection | Yes | Yes | Yes | Yes |
| Query, pull, explain | Yes | Yes | Yes | Yes |
| Multiple source databases in `q` / `explain` | Yes | Yes, pass `Connection` sources | Yes, pass `Connection` sources | Yes, pass `Connection` sources |
| Synchronous transaction | Yes | Yes | Yes | Yes |
| Async transaction | Yes | Yes, `CompletableFuture` | Yes, `Future` | Yes, `Promise` |
| Transaction listeners: `listen!` / `unlisten!` | Yes | Yes | Yes | Yes |
| Datalog transaction callback | Yes, `with-transaction` | Yes, `withTransaction` | Yes, `with_transaction` | No |
| Transaction entity maps/forms | Yes | Yes, maps and `Tx` builders | Yes, dictionaries/lists and helpers | Yes, objects/arrays and helpers |
| Transactable existing entity objects | Yes | No | No | No |
| Lazy entity reads | Yes | Yes | Yes | Yes |
| Eager entity map/touch | Yes | Yes | Yes | Yes |
| Datom/index reads: `datoms`, `seek-datoms`, `index-range`, `count-datoms` | Yes | Yes | Yes | Yes |
| Full-text datom reads | Yes | Yes | Yes | Yes |
| Bulk load: `init-db` / `fill-db` | Yes | Yes | Yes | Yes |
| Datalog-backed KV access | Yes | Yes | Yes | Yes |
| Datalog index cache limit getter/setter | Yes | Yes | Yes | Yes |
| Re-index Datalog store | Yes | Yes | Yes | Yes |

### Datalog Notes

JavaScript does not expose Datalog `withTransaction` at this time. The Node JVM
bridge deadlocks when Java calls a JavaScript proxy callback and that callback
then calls back into Datalevin, for example `await tx.transact(...)`. The safe
JavaScript alternatives are:

* use `conn.transact(...)` for one normal Datalog transaction,
* use `conn.transactAsync(...)` for async Datalog ingestion,
* use KV `withTransaction(...)` or explicit KV transaction handles when mixing
  multiple KV operations.

The Clojure transactable entity API lets an existing entity stage associative
updates and be transacted later. Non-Clojure bindings expose lazy entity reads,
but not staged mutation of entity objects. Use transaction maps/builders instead.

For multiple source database queries in non-Clojure bindings, pass another
`Connection` object as the source input corresponding to the extra `$` symbol.
Direct DB snapshot access remains an interop/compatibility detail.

## KV

| Capability | Clojure | Java | Python | JavaScript |
| --- | --- | --- | --- | --- |
| Open KV store | Yes | Yes | Yes | Yes |
| Open/list/clear/drop DBIs | Yes | Yes | Yes | Yes |
| Entries and basic get/range reads | Yes | Yes | Yes | Yes |
| KV transaction data writes | Yes | Yes | Yes | Yes |
| List DB operations: get, put, delete, count, membership | Yes | Yes | Yes | Yes |
| List DB range reads: range, first, first N, range counts | Yes | Yes | Yes | Yes |
| List DB functional scans: visit, filter, filter count, keep, some | Yes | Yes | Yes | Yes |
| List DB raw-buffer functional scans | Yes | Yes | Yes | Yes |
| Range helpers: range count, key range, key range count | Yes | Yes | Yes | Yes |
| Rank/sample helpers: first, first N, rank, by-rank, entry by-rank, sample | Yes | Yes | Yes | Yes |
| Stats, copy, sync | Yes | Yes | Yes | Yes |
| Runtime env flag read/mutation | Yes | Yes | Yes | Yes |
| KV transaction callback | Yes, `with-transaction-kv` | Yes, `withTransaction` | Yes, `with_transaction` | Yes, `withTransaction` |
| Explicit KV begin/commit/abort | Partial, lower-level storage primitives | Yes, `KVTransaction` | Yes, `KVTransaction` | Yes, `KVTransaction` |
| KV re-index | Yes | Yes | Yes | Yes |

### KV Notes

Raw-buffer list scans pass short-lived callback wrapper objects. Decode or copy
raw bytes during the callback; use the wrapper byte-copy helpers when data must
outlive the callback.

## Operational APIs

| Capability | Clojure | Java | Python | JavaScript |
| --- | --- | --- | --- | --- |
| Copy/backup | Yes | Yes | Yes | Yes |
| Sync | Yes | Yes | Yes | Yes |
| Snapshot creation/listing | Yes | Yes | Yes | Yes |
| Tx log watermarks | Yes | Yes | Yes | Yes |
| Tx log GC | Yes | Yes | Yes | Yes |
| Open/inspect tx log | Yes | Yes | Yes | Yes |
| Remote client open/close | Yes | Yes | Yes | Yes |

## Search, Vector, and Idoc

| Capability | Clojure | Java | Python | JavaScript |
| --- | --- | --- | --- | --- |
| Full-text search through Datalog schema/options | Yes | Yes | Yes | Yes |
| Full-text custom analyzer/query analyzer through Datalog search-domain UDFs | Yes | Yes | Yes | Yes |
| Vector search through Datalog schema/options | Yes | Yes | Yes | Yes |
| Idoc schema/options | Yes | Yes | Yes | Yes |
| Search/vector/idoc option builders | N/A, native maps | Yes | Yes | Yes |
| Standalone KV search engine | Yes | Yes | Yes | Yes |
| Standalone vector index | Yes | Yes | Yes | Yes |
| Local llama.cpp embedder/generator handles/providers | Yes, providers | Yes | Yes | Yes |
| Search index writer: create, write, commit | Yes | Yes | Yes | Yes |
| Search engine re-index | Yes | Yes | Yes | Yes |

## UDFs and Data Helpers

| Capability | Clojure | Java | Python | JavaScript |
| --- | --- | --- | --- | --- |
| UDF registry creation | Yes | Yes | Yes | Yes |
| UDF descriptor helper | Yes | Yes | Yes | Yes |
| Register/unregister UDF | Yes | Yes | Yes | Yes |
| Query UDF calls | Yes | Yes | Yes | Yes |
| Predicate UDF calls | Yes | Yes | Yes | Yes |
| Transaction UDF registration | Yes | Yes | Yes | Yes |
| Analyzer/query-analyzer UDF registration | Yes | Yes | Yes | Yes |
| Keyword helper | N/A, native keyword syntax | Yes | Yes | Yes |
| Symbol helper | N/A, native symbol syntax | Yes | Yes | Yes |
| EDN read/write helpers | Yes | Yes | Yes | Yes |
| Schema construction helpers | N/A, native maps | Yes | Yes | Yes |
| Transaction construction helpers | N/A, native maps/vectors | Yes | Yes | Yes |

## Known Parity Gaps

| Gap | Affected languages | Current workaround | Reason |
| --- | --- | --- | --- |
| Datalog transaction callback missing in JavaScript | JavaScript | Use single `conn.transact(...)`, `conn.transactAsync(...)`, or KV transaction APIs | Node JVM bridge deadlocks on Java proxy callback re-entry into Datalevin |
| Transactable existing entity objects missing outside Clojure | Java, Python, JavaScript | Use transaction maps/builders such as `Tx.entity`, `tx_entity`, or `txEntity` | Non-Clojure lazy entity wrappers are read-oriented and do not stage mutations |

Future changes should update this file together with public surface guard tests
and binding README examples.
