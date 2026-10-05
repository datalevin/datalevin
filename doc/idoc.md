# Datalevin Idoc (Indexed Documents)

Datalevin provides document database feature through idoc. Idoc is a
structured-document value type with a built-in index. The index is keyed on
paths in the document. It lets you store nested documents in regular datoms
while supporting fast path/value matching in Datalog queries.

Idoc is designed to be orthogonal to full-text and vector indices. You can index
the same document in multiple ways (idoc, fulltext, vector) and mix their
queries in a single Datalog query.

Another common use of path indexed documents is to have flexibility in data
modeling, as the document format can evolve independently without touching the
overall schema.

## Usage

### Schema

Declare idoc attributes with `:db/valueType :db.type/idoc`. You can choose an
optional idoc format and a domain name:

* `:db/idocFormat` -- one of `:edn` (default), `:json`, or `:markdown`.
* `:db/domain` -- optional idoc domain. If absent, the attribute name is used as
  the domain. If specified, the values of this attribute will be added to the
  domain. Domain allows scoped search of idocs.
* `:db.idoc/indexedPaths` -- optional collection of path prefix selectors to
  index for this attribute's domain. If absent, all paths are indexed. A
  selector can be a keyword, string, or vector path. For example, `:profile`
  indexes leaf paths such as `[:profile :age]` and `[:profile :name]`. Vector
  paths may include non-negative integer positions, e.g. `[:tags 1]`. Selectors
  that omit positions apply across vector elements.
* `:db.idoc/excludedPaths` -- optional collection of path prefix selectors to
  omit from this attribute's domain. Excluded paths win over included paths.

```clojure
(def schema
  {:doc/edn  {:db/valueType   :db.type/idoc
              :db/domain      "profiles"}
   :doc/json {:db/valueType   :db.type/idoc
              :db/idocFormat  :json}
   :doc/md   {:db/valueType   :db.type/idoc
              :db/idocFormat  :markdown}
   :doc/many {:db/valueType   :db.type/idoc
              :db/cardinality :db.cardinality/many}})
```

The same path controls can be supplied as store options. `:idoc-opts` applies
defaults to every idoc domain, and `:idoc-domains` overrides a named domain:

```clojure
(d/create-conn
  dir schema
  {:idoc-domains
   {"profiles" {:indexed-paths [:status :profile]
                :excluded-paths [[:profile :raw]]}}})
```

### Indexing mode

Idoc indexing defaults to `:sync`: source documents and their index entries are
updated in the same transaction. To move index maintenance to the background,
set `:indexing-mode :async` in the default idoc options or a named domain:

```clojure
;; All idoc domains use async indexing unless overridden.
(d/create-conn dir schema {:idoc-opts {:indexing-mode :async}})

;; Only the profiles domain uses async indexing.
(d/create-conn dir schema
  {:idoc-domains {"profiles" {:indexing-mode :async}}})
```

Domain options override the defaults; a domain that only specifies path controls
inherits the default indexing mode.

Async writes commit the source datoms and durable index jobs atomically. Pulls,
ordinary datom reads, and `:db.fn/patchIdoc` see the committed document immediately.
`idoc-match` is eventually consistent: results may be stale or missing until the
worker catches up. A replaced or deleted giant document is omitted if its old
index entry still refers to source bytes that are no longer present.

The worker resumes pending jobs on database open. It applies updates in source
transaction order within each domain, retaining the old and new documents and
patch hints in the job. Index changes and job completion commit together. A
failed or leased job blocks later jobs in that domain until it can be completed.
The worker uses the same retry and lease settings as other async secondary
indexes.

For a local connection, inspect progress or wait for index updates through a
source transaction:

```clojure
(d/secondary-index-status conn)
(d/wait-for-secondary-index conn
  {:type :idoc :domain "profiles" :timeout-ms 5000})
;; Check :caught-up? in the returned map; a timeout returns false.
```

Concurrent local `transact!` calls can share a commit when all active secondary
domains use async indexing. Their jobs commit or roll back with the source
datoms. A synchronous secondary domain retains the restriction on batching
general transactions. Drain pending jobs before changing an async domain to
synchronous indexing.

### Transact idoc values

Idoc values must be maps or vectors. Map keys must be keywords or strings.
Vectors are allowed as arrays, including at the root. You can nest maps and
vectors arbitrarily. However, the maximal path when binary encoded cannot be
over 511 bytes.

lists are **not** allowed. `nil` values are normalized to `:json/null`. Literal
`:json/null` is reserved and cannot be used in input. For `:edn` format, strings
are parsed with `clojure.edn/read-string` and must yield a map or vector.

```clojure
(d/transact! conn
  [{:db/id   1
    :doc/edn {:status  "active"
              :profile {:age 30 :name "Alice"}
              :tags    ["a" "b" "b"]}
    :doc/json "{\"name\":\"Alice\",\"middle\":null,\"age\":30}"
    :doc/md   "# User Profile\n## Getting Started!\nName: Alice\nAge: 30"
    :doc/many [{:profile {:age 30 :name "A"}} {:profile {:age 35 :name "B"}}]}])
```

`:doc/md` uses Markdown parsing (see Implementation below), producing a nested
map; the parsed map is stored in the datom and indexed.

### Patch idoc values

For `:db.type/idoc` attributes that have cardinality one, transacting a new idoc
value would replace existing one, and the system would intelligently identify
and update the changed paths/values only.

If you have small updates in idoc and you do not want to transact the updated
document as a whole,  `:db.fn/patchIdoc` is a built-in transaction function that
updates nested values in an idoc document without rewriting the full document in
user code:

```clojure
(d/transact! conn
  [[:db.fn/patchIdoc 1 :doc/edn
    [[:set    [:profile :age] 31]
     [:unset  [:profile :middle]]
     [:update [:tags] :conj "c"]]]])
```

For cardinality many idoc attributes, provide the old value to identify which
document to patch:

```clojure
(d/transact! conn
  [[:db.fn/patchIdoc 1 :doc/many {:profile {:age 35}}
    [[:set [:profile :age] 40]]]])
```

Patch ops:

* `:set`    — set value at path
* `:unset`  — remove map key or vector element at path
* `:update` — update value at path using one of:
  `:conj` (vector), `:merge`/`:assoc`/`:dissoc` (map), `:inc`/`:dec` (number)

Paths are vectors of keyword/string keys and integer indices. Wildcards (`:?`,
`:*`) are not allowed in patches. Integer segments address a specific vector
element. Idoc queries can likewise select a position, or omit positions to
match any element.

`patchIdoc` works on idoc attributes with cardinality one. For cardinality many,
an old value must be provided (see example above).

Entity id (`e`) accepts a numeric id, lookup ref (`[unique-attr value]`), or
keyword ident. Tempids are not supported for `patchIdoc`.

### Document requirements

Idoc enforces a few structural rules at ingest:

* Top-level value must be a map or vector (no top-level scalar).
* Keys must be keywords or strings.
* Lists are not allowed anywhere; use vectors for arrays.
* `nil` values are normalized to `:json/null`.
* Literal `:json/null` is reserved and rejected on input.
* EDN and JSON strings must parse to a map or vector.
* Markdown content must start under a header (text before any header is invalid).
* Circular references are rejected.

### Query with `idoc-match`

`idoc-match` is a Datalog query function that returns matching datoms as `[e a
v]` triples.

* Full DB search: `[(idoc-match $ {:status "active"}) [[?e ?a ?v]]]`
* Attribute specific: `[(idoc-match $ :doc/edn {:status "active"}) ...]`
* Domain specific: `[(idoc-match $ {:status "active"} {:domains ["profiles"]}) ...]`

```clojure
;; attribute-specific search
(d/q '[:find ?e ?a
       :in $ ?q
       :where [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
     db
     {:status "active" :profile {:age 30}})
```

#### Limit and offset

Finite relation queries that use `idoc-match` can use query-level `:limit` and
`:offset`:

```clojure
(d/q '[:find ?e
       :where
       [(idoc-match $ :doc/edn {:status "active"}) [[?e _ _]]]
       :offset 100
       :limit 20]
     db)
```

For eligible queries, the optimizer can scan idoc matches incrementally and
stop after enough distinct final result tuples survive the remaining joins and
filters. `:offset` and `:limit` apply to those final tuples, not directly to the
raw idoc candidates. Datalevin fetches additional candidate batches when the
remaining clauses filter out earlier matches.

This is a cost-based optimization rather than a different query mode.
Unbounded queries, queries whose shape cannot be evaluated safely in batches,
or plans for which the access path is not estimated to help use conventional
execution. If bounded execution reaches its work budget, it falls back to the
conventional plan without changing the query semantics.

Idoc access does not define a result order. Without `:order-by`, the returned
window has no ordering guarantee.

#### Nested maps and arrays

Map values can be nested maps. Vectors are treated as arrays: a match succeeds
if **any** array element matches.

```clojure
(d/q '[:find ?e
       :in $
       :where [(idoc-match $ :doc/edn {:tags "b"})
               [[?e ?a ?v]]]]
     db)
;; => #{[1]}
```

#### Root vectors

An idoc attribute can store a vector directly, with no surrounding document map
or additional schema option. The entire vector is stored as one datom value for
a cardinality-one attribute, preserving order and duplicate elements:

```clojure
(def schema
  {:attr1 {:db/valueType   :db.type/idoc
           :db/cardinality :db.cardinality/one}})

(d/transact! conn [{:db/id 1 :attr1 [1 2 5 3 5]}])
(d/pull (d/db conn) [:attr1] 1)
;; => {:attr1 [1 2 5 3 5]}

;; A scalar query matches any element of the root vector.
(d/q '[:find ?e ?v
       :where [(idoc-match $ :attr1 5) [[?e _ ?v]]]]
     (d/db conn))
;; => #{[1 [1 2 5 3 5]]}
```

Repeated elements do not produce additional matching datoms. For cardinality
many, each vector is a separate value; in an entity map, supply a collection of
vectors, e.g. `{:attr1 [[1 5] [2 5]]}`.

Logical combinators apply the same membership semantics at the root:

| Query argument | Meaning |
| --- | --- |
| `5` | Contains `5` |
| `[:and 1 5]` | Contains both `1` and `5`, possibly in different elements |
| `[:or 2 4]` | Contains either `2` or `4` |
| `[:not 5]` | Does not contain `5` |
| `{1 3}` | The second element matches `3` (zero-based position `1`) |
| `(> [] 3)` | Has an element greater than `3` |
| `(> [1] 3)` | The second element matches a value greater than `3` |
| `(< 2 [] 5)` | Has a single element strictly between `2` and `5` |
| `(nil? [])` | Has a null element |

Use `[]` as the root path in predicates. Predicate lists must be quoted when
passed as query data, just like predicates on named paths:

```clojure
(d/q '[:find ?e
       :in $ ?q
       :where [(idoc-match $ :attr1 ?q) [[?e _ _]]]]
     (d/db conn)
     '(< 2 [] 5))
```

Vector positions use the same query model as map keys. Integer query-map keys
select positions, and integer path segments select positions in predicates:

```clojure
;; The second element of the root vector is 3.
[(idoc-match $ :attr1 {1 3}) [[?e _ ?v]]]

;; The second tag in a nested vector is "b".
[(idoc-match $ :doc/edn {:tags {1 "b"}}) [[?e _ ?v]]]

;; The second tag is greater than 3.
[(idoc-match $ :doc/edn (> [:tags 1] 3)) [[?e _ ?v]]]
```

Integer positions are distinct from string keys: `{1 3}` selects a vector
element, while `{"1" 3}` matches a map field. When a selected element is itself
a vector, matching follows the same membership semantics as a vector-valued
map field.

For whole-vector equality, use an ordinary datom clause:
`[?e :attr1 [1 2 5 3 5]]`. Query vectors passed to `idoc-match` remain logical
expressions, rather than literal vector values. `idoc-get` with path `[]`
returns the entire vector. Patch paths such as `[0]` address individual root
vector elements; empty patch paths remain invalid.

#### Logical combinators

Boolean expressions can be used to combine match conditions. Use `[:and ...]`,
`[:or ...]`, and `[:not ...]` inside a query map:

```clojure
(d/q '[:find ?e
       :in $
       :where [(idoc-match $ :doc/edn
                            {:profile [:or {:age 30} {:age 40}]})
               [[?e ?a ?v]]]]
     db)
```

#### Predicates

Predicates can be used as map values or as a standalone expression with a path.
Supported predicates are `nil?`, `>`, `>=`, `<`, and `<=`. Comparison operators
support multiple arity, so you can express ranges without a dedicated
`between` predicate.

```clojure
;; inline predicate in a map value
(d/q '[:find ?e
       :in $
       :where [(idoc-match $ :doc/edn {:age (> 21)})
               [[?e ?a ?v]]]]
     db)

;; path predicate (quote the list when passed as data)
(d/q '[:find ?e
       :in $ ?q
       :where [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
     db
     '(>= [:profile :age] 30))

;; range predicate via multi-arity comparison
(d/q '[:find ?e
       :in $ ?q
       :where [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
     db
     '(< 20 [:profile :age] 40))
```

#### Wildcard paths

Wildcard segments can be used in path expressions and map keys:

* `:?` matches one map key or vector position.
* `:*` matches any depth (zero or more segments).

```clojure
;; any single key under :profile with value >= 30
(d/q '[:find ?e
       :in $ ?q
       :where [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
     db
     '(>= [:profile :?] 30))

;; match any depth for a key
(d/q '[:find ?e
       :in $
       :where [(idoc-match $ :doc/edn {:* {:product "B"}})
               [[?e ?a ?v]]]]
     db)
```

`:?` and `:*` are reserved as wildcard segments in queries.

`nil` is not a valid query value. To match nulls, use `(nil?)`:

```clojure
(d/q '[:find ?e
       :in $
       :where [(idoc-match $ :doc/json {"middle" (nil?)})
               [[?e ?a ?v]]]]
     db)
```

Note: JSON keys are strings. If the stored document comes from JSON, match with
string keys (e.g. `"middle"`), not keywords.

### Extract values with `idoc-get`

`idoc-get` extracts a value by path from an idoc document. It returns a vector
when the path traverses arrays.

```clojure
(def doc (:doc/edn (d/entity db 1)))
(idoc-get doc :profile :age)     ;; => 30
(idoc-get doc :tags)             ;; => ["a" "b" "b"]
```

## Implementation details

* **Document storage**: The original document is stored in the datom value. The
  idoc index only stores references to the datom, similar to fulltext indexing.
* **Index structure** (per idoc domain):
  * **doc-ref map**: `datom-ref -> doc-id` (doc-ref is datom or a giant datom id).
    idoc also keeps a spillable in-memory reverse map, `doc-id -> datom-ref`,
    for query emission. Access to the reverse map and document-id bitmap is
    synchronized per idoc index.
  * **path dictionary**: `path -> path-id` with stable numeric ids.
  * **inverted index**: `(path-id, typed-value) -> [doc-id ...]`.
* **Indexing**: By default, idoc indices are updated synchronously with source
  datoms. Async domains enqueue durable jobs in the source transaction; a worker
  commits the index changes and job completion together. If selective path
  indexing is configured, only selected leaf paths are entered in the path
  dictionary and inverted index.
* **Large values**: Values that exceed the index key size are indexed by a
  truncated prefix (same scheme used by core indices). This can introduce
  extra candidates, but exact matches are verified against the full document
  during query evaluation.
* **Id ranges**: Idoc assigns a per-domain document id using 32-bit signed
  integers (about 2.1 billion docs per domain). Path ids are also 32-bit
  integers and are append-only.
* **Paths**: Paths are encoded as strings with `/` separators and distinct
  markers for keyword keys, string keys, and integer vector positions. Vector
  elements are indexed at their actual positions; membership queries combine
  entries across positions without storing additional flattened entries.
* **Markdown**: Markdown is parsed into a nested map. Headers are normalized
  (lowercase, punctuation removed, whitespace to `-`). Text content under a
  header is stored as a string. The normalized header keys are indexed and
  matched during query.
* **JSON nulls**: JSON `null` values are normalized to `:json/null` at ingest.
  Querying for null requires `(nil?)`.
