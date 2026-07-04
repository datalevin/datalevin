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
  indexes leaf paths such as `[:profile :age]` and `[:profile :name]`.
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

### Transact idoc values

Idoc values must be maps whose keys are keywords or strings, i.e. top level of
the document must be a map. Vectors are allowed as arrays. You can nest map and
vectors arbitrarily. However, the maximal path when binary encoded cannot be
over 511 bytes.

lists are **not** allowed. `nil` values are normalized to `:json/null`. Literal
`:json/null` is reserved and cannot be used in input. For `:edn` format, strings
are parsed with `clojure.edn/read-string` and must yield a map.

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
`:*`) are not allowed. Integer segments address a specific vector element; idoc
indexing still matches arrays by value (any element).

`patchIdoc` works on idoc attributes with cardinality one. For cardinality many,
an old value must be provided (see example above).

Entity id (`e`) accepts a numeric id, lookup ref (`[unique-attr value]`), or
keyword ident. Tempids are not supported for `patchIdoc`.

### Document requirements

Idoc enforces a few structural rules at ingest:

* Top-level value must be a map (no top-level vector or scalar).
* Keys must be keywords or strings.
* Lists are not allowed anywhere; use vectors for arrays.
* `nil` values are normalized to `:json/null`.
* Literal `:json/null` is reserved and rejected on input.
* EDN strings must parse to a map.
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

* `:?` matches exactly one path segment.
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
* **Indexing**: During transactions, idoc indices are updated synchronously.
  There is no multi-step indexing process. If selective path indexing is
  configured, only selected leaf paths are entered in the path dictionary and
  inverted index.
* **Large values**: Values that exceed the index key size are indexed by a
  truncated prefix (same scheme used by core indices). This can introduce
  extra candidates, but exact matches are verified against the full document
  during query evaluation.
* **Id ranges**: Idoc assigns a per-domain document id using 32-bit signed
  integers (about 2.1 billion docs per domain). Path ids are also 32-bit
  integers and are append-only.
* **Paths**: Paths are encoded as strings with `/` separators and include
  keyword vs string segment markers for lossless decode.
* **Markdown**: Markdown is parsed into a nested map. Headers are normalized
  (lowercase, punctuation removed, whitespace to `-`). Text content under a
  header is stored as a string. The normalized header keys are indexed and
  matched during query.
* **JSON nulls**: JSON `null` values are normalized to `:json/null` at ingest.
  Querying for null requires `(nil?)`.
