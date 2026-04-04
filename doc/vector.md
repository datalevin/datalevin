# Datalevin Vector Indexing and Similarity Search

Datalevin supports two related but distinct similarity-search features:

* `:db.type/vec` stores user-supplied dense numeric vectors and indexes them for
  nearest-neighbor search.
* `:db/embedding` indexes string datoms by embedding similarity. Datalevin
  computes the vectors during transaction processing and queries return the
  original source datoms.

Both features use approximate nearest-neighbor search on equal-length dense
numeric vectors.

This functionality is developed on the basis of
[usearch](https://github.com/unum-cloud/usearch) library, which is an
implementation of Hierarchical Navigable Small World (HNSW) graph algorithm [1].
usearch leverages SIMD vector instructions in CPUs and is used in several OLAP
stores, such as clickhouse, DuckDB, and so on.

With these features, Datalevin can be used as a vector database to support
applications such as semantic search, image search, retrieval augmented
generation (RAG), and so on. This feature is currently available on Linux for
both x86_64 and arm64 CPUs, and on MacOSX for arm64. Windows support is
experimental.

## Configurations

These configurable options can be set when creating the vector index using
Datalevin:

* `:dimensions`, the number of dimensions of the vectors. **Required**, no
  default.

* `:metric-type` is the type of similarity metrics. Custom metric may be
  supported in the future.
  - `:euclidean`   [Euclidean
    distance](https://en.wikipedia.org/wiki/Euclidean_distance), length of line
    segment between points. This is the default.
  - `:cosine` [Cosine
    similarity](https://en.wikipedia.org/wiki/Cosine_similarity)i, angle between
    vectors.
  - `:dot-product` [Dot
    product](https://en.wikipedia.org/wiki/Dot_product), sum of element-wise
    products.
  - `:haversine` [Haversine
    formula](https://en.wikipedia.org/wiki/Haversine_formula), spheric
    great-circle distance.
  - `:divergence` [Jensen-Shannon
  divergence](https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence),
  symmetrized and smoothed version of Kullback-Leibler divergence of probability
  distributions.
  - `:pearson` [Pearson
    distance](https://en.wikipedia.org/wiki/Distance_correlation), one minus
    normalized Pearson correlation coefficient.
  - `:jaccard` [Jaccard index](https://en.wikipedia.org/wiki/Jaccard_index), set
    intersection over union.
  - `:hamming` [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance),
    number of positions that are different.
  - `:tanimoto` [Tanimoto
  similarity](https://en.wikipedia.org/wiki/Chemical_similarity) Similarity of
  structures, e.g. chemical fingerprint, molecules structural alignment, etc.
  - `:sorensen` [Sorensen-Dice
    index](https://en.wikipedia.org/wiki/Dice-S%C3%B8rensen_coefficient), F1
    score, combination of precision and recall, or intersection over bitwise union.

* `:quantization` is the scalar type of the vector elements. More types may be
  supported in the future.
  - `:float`, 32 bits float point number, the default.
  - `:double`, 64 bits double float point number
  - `:float16`, 16 bit float, i.e. IEEE 754 floating-point binary16, we expect
    the representation is already converted into a Java Short bit format, e.g.
    using `org.apache.arrow.memory.util.Float16/toFloat16`, or
    `jdk.incubator.vector.Float16/float16ToRawShortBits`
  - `:int8`, 8 bit integer, the representation is the same as byte
  - `:byte`, raw byte

* `:connectivity`, the number of connections per node in the index graph
  structure, i.e. the `M` parameter in the HNSW paper [1]. The default is 16. The
  paper says:
  > A reasonable range of M is 5 to 48. Simulations show that smaller M
  > generally produces better results for lower recalls and/or lower dimensional
  > data, while bigger M is better for high recall and/or high dimensional data.
  > The parameter also defines the memory consumption of the algorithm (which is
  > proportional to M), so it should be selected with care.

* `:expansion-add`, the number of candidates considered when adding a new vector
  to the index, i.e. the `efConstruction` parameter in the paper. It controls
  the index construction speed/quality tradeoff: the larger the number, the
  better is the quality (but with diminished return after certain size) and the
  longer is the indexing time. The default is 128.

* `:expansion-search`, the number of candidates considered during search, i.e.
  the `ef` parameter in the paper. It controls the search speed/quality
  tradeoff, similar to the above. The default is 64.

## Usage

The vector indexing and search functionalities are available to use in all
supported modes: key-value store, Datalog store, embedded, client/server, or
Babashka pods.

### Standalone Vector Indexing and Search

Datalevin can be used as a standalone vector database. The standalone vector API
involves only a few functions: `new-vector-index`, `add-vec`, `remove-vec`, and
`search-vec`.

Each vector is identified with a `vec-ref` that can be any Clojure data (less
than maximal key size of 512 bytes), which should be semantically meaningful for
the application, e.g. a document id, an image id, or a tag.

Multiple vectors can be associated with the same `vec-ref`. For example, you may
have a `vec-ref` `"cat"` for many vectors that are the embedding of different cat
images. Another example: each chunk of a large document (e.g. for RAG) has its
own vector embedding and these vectors are all associated with the same document
id.

```Clojure
(require '[datalevin.core :as d])

;; Vector indexing uses a key-value store to store mapping between vec-ref and
;; vector id
(def lmdb (d/open-kv "/tmp/vector-db"))

;; Create the vector index. The dimensions of the vectors need to be specified.
;; Other options use defaults here.
(def index (d/new-vector-index lmdb {:dimensions 300}))

;; User needs to supply the vectors. Here we load some word2vec vectors from a
;; CSV file, each row contains a word, followed by the elements of the vector,
;; return a map of words to vectors
(def data (->> (d/read-csv (slurp "test/data/word2vec.csv"))
               (drop 1)
               (reduce
                 (fn [m [w & vs]] (assoc m w (mapv Float/parseFloat vs)))
                 {})))

;; Add the vectors to the vector index. `add-vec` takes a `vec-ref`, in this
;; case, a word; and the actual vector, which can be anything castable as a
;; Clojure seq, e,g. Clojure vector, array, etc.
(doseq [[w vs] data] (d/add-vec index w vs))

;; Search by a query vector. return  a list of `:top` `vec-ref` ordered by
;; similarity to the query vector
(d/search-vec index (data "king") {:top 2})
;=> ("king" "queen")
```

### Standalone Text Embedding Providers

Datalevin also exposes a small embedding-provider API for cases where
applications want to embed text directly.

```Clojure
(require '[datalevin.core :as d])

(with-open [provider (d/new-embedding-provider
                       {:provider :default
                        :dir      "/tmp/mydb"})]
  ;; initialize lazily on first use
  (d/embedding-dimensions provider)
  ;; stable metadata about the embedding space
  (d/embedding-metadata provider)
  ;; embed one or more texts
  (d/embed-text provider "hello world")
  (d/embed-texts provider ["hello world" "bonjour le monde"]))
```

The built-in default provider uses the local llama.cpp embedder bundled in
`dtlvnative`. If `:model` or `:model-path` is not supplied, Datalevin expects
the default model `multilingual-e5-small-Q8_0.gguf` under `dir/embed/`, where
`dir` is the DB root. If the file is missing, Datalevin downloads it from
Hugging Face on first use. The default model (`intfloat/multilingual-e5-small`)
produces 384-dimensional vectors and supports a maximum input of 512 tokens.
Text longer than 512 tokens is truncated by the model.

Datalevin also ships a built-in `:openai-compatible` provider for remote
`/embeddings` APIs:

```Clojure
(with-open [provider (d/new-embedding-provider
                       {:provider :openai-compatible
                        :model    "text-embedding-3-small"
                        :base-url "https://api.openai.com/v1"
                        :api-key-env "OPENAI_API_KEY"})]
  (d/embed-text provider "hello world"))
```

Use `:endpoint` instead of `:base-url` when the server does not expose the
standard `/embeddings` path. `:api-key` and `:api-key-env` are runtime
authentication options. `:request-dimensions` requests a specific output size
when supported by the remote provider.

### Vector Indexing and Search in Datalog Store

Vectors can be stored in Datalog as attribute values of data type
`:db.type/vec`. Such attribute may have a property `:db.vec/domains`, indicating
which vector search domains the attribute should participate. By default, each
vector attribute is its own domain, with domain name the same as attribute name.

A query function `vec-neighbors` is provided to allow vector search in Datalog
queries. This function takes the DB, the query vector and an optional option map
(same as `search-vec`), and returns a sequence of matching datoms in the form of
`[e a v]` for easy destructuring, ordered by similarity to the query vector.

```Clojure
(let [conn (d/create-conn
                "/tmp/mydb"
                {:id        {:db/valueType :db.type/string
                             :db/unique    :db.unique/identity}
                 :embedding {:db/valueType :db.type/vec}}
                {:vector-opts {:dimensions  300
                               :metric-type :cosine}})]
    ;; use the same `data` defined above
    (d/transact! conn [{:id "cat" :embedding (data "cat")}
                       {:id "rooster" :embedding (data "rooster")}
                       {:id "jaguar" :embedding (data "jaguar")}
                       {:id "animal" :embedding (data "animal")}
                       {:id "physics" :embedding (data "physics")}
                       {:id "chemistry" :embedding (data "chemistry")}
                       {:id "history" :embedding (data "history")}])

    ;; attribute specific search, `?v` has the retrieved vectors.
    (set (d/q '[:find [?i ...]
                :in $ ?q
                :where
                [(vec-neighbors $ :embedding ?q {:top 4}) [[?e ?a ?v]]]
                [?e :id ?i]]
           (d/db conn) (data "cat")))

    ;; domains specific search
    (set (d/q '[:find [?i ...]
                :in $ ?q
                :where
                [(vec-neighbors $ ?q {:top 4 :domains ["embedding"]}) [[?e _ _]]]
                [?e :id ?i]]
           (d/db conn) (data "cat"))))
;;=>  #{"cat" "jaguar" "animal" "rooster"}
```

In the above example, we destructure the results into three variables,
`?e`, `?a` and `?v`.

The search can be specific to an attribute, or specific to a list of domains.

### Embedding Indexing and Search in Datalog Store

Embedding indexing is distinct from `:db.type/vec`. With `:db/embedding`,
Datalevin keeps the original text datoms as the source of truth and maintains a
secondary vector index over them. No hidden vector attributes are generated.

An embedding-enabled attribute must be a string attribute:

```Clojure
{:doc/text {:db/valueType            :db.type/string
            :db/embedding            true
            :db.embedding/domains    ["docs"]
            :db.embedding/autoDomain true}}
```

Embedding schema keys:

* `:db/embedding` enables embedding indexing for the source attribute.
* `:db.embedding/domains` adds the attribute to one or more embedding domains.
* `:db.embedding/autoDomain true` adds an attribute-specific domain derived
  from the attribute name. This is required for attribute-specific query syntax.

Embedding domains use the same attribute-to-domain naming rule as vector
domains:

* `:text` -> `"text"`
* `:doc/text` -> `"doc_text"`

Store options for embedding search are separate from vector options:

```Clojure
{:embedding-opts
 {:provider    :default
  :metric-type :cosine}

 :embedding-domains
 {"docs" {:provider    :default
          :metric-type :cosine}}}
```

`:embedding-opts` gives defaults for embedding domains. `:embedding-domains`
configures per-domain overrides. `:embedding-providers` is an optional
runtime-only map from provider ids to either provider instances or provider
specs; it is not persisted in LMDB.

The built-in default provider is `:default` (also available as `:llama.cpp`).
It uses the local GGUF model described above and resolves the default model path
relative to the DB root.

The built-in `:openai-compatible` provider can also be used in store options:

```Clojure
{:embedding-opts
 {:provider           :openai-compatible
  :model              "text-embedding-3-small"
  :base-url           "https://api.openai.com/v1"
  :api-key-env        "OPENAI_API_KEY"
  :request-dimensions 1536
  :metric-type        :cosine}}
```

For store persistence, direct `:api-key` values are never written into LMDB.
`:api-key-env` is safe to persist because it stores only the environment
variable name. `:request-dimensions` is useful for remote providers because it
lets Datalevin resolve the embedding space during store open without probing the
endpoint.

```Clojure
(let [conn (d/create-conn
             "/tmp/mydb"
             {:doc/id   {:db/valueType :db.type/string
                         :db/unique    :db.unique/identity}
              :doc/text {:db/valueType            :db.type/string
                         :db/embedding            true
                         :db.embedding/domains    ["docs"]
                         :db.embedding/autoDomain true}
              :doc/tag  {:db/valueType   :db.type/string
                         :db/cardinality :db.cardinality/many
                         :db/embedding   true}}
             {:embedding-opts {:provider    :default
                               :metric-type :cosine}})]
  ;; first use loads the model lazily and downloads it into
  ;; /tmp/mydb/embed/ if it is not already present
  (d/transact! conn [{:doc/id   "cat-1"
                      :doc/text "red cat"
                      :doc/tag  ["pet cat" "feline friend"]}
                     {:doc/id   "cat-2"
                      :doc/text "kitten animal"
                      :doc/tag  ["small pet"]}
                     {:doc/id   "dog-1"
                      :doc/text "friendly dog"
                      :doc/tag  ["canine pal"]}])

  ;; search an explicit embedding domain by query text
  (d/q '[:find [?id ...]
         :in $ ?q
         :where
         [(embedding-neighbors $ ?q {:domains ["docs"] :top 2})
          [[?e _ _]]]
         [?e :doc/id ?id]]
       (d/db conn) "cat")

  ;; search the attribute-specific embedding domain
  (d/q '[:find ?id .
         :in $ ?q
         :where
         [(embedding-neighbors $ :doc/text ?q {:top 1}) [[?e _ _]]]
         [?e :doc/id ?id]]
       (d/db conn) "cat"))
;;=> "cat-1"
```

`embedding-neighbors` returns source datom tuples in the form of `[e a v]`, or
`[e a v dist]` when `:display :refs+dists` is used.

Important rules:

* The query input to `embedding-neighbors` is text, not a vector.
* Domain-based search requires `:domains` in the option map.
* Attribute-specific search requires `:db.embedding/autoDomain true` on that
  attribute.
* If an embedding attribute does not specify `:db.embedding/domains`, it
  participates in the default embedding domain `"datalevin"`.
* `:db/embedding` may coexist with `:db/fulltext` on the same attribute.
* Changing embedding-related schema on a populated attribute is rejected and
  requires an explicit rebuild workflow.


### Search Configurations

#### Search options for vector search

`search-vec` and `vec-neighbors` functions support an option map that can be
passed at run time to customize search:

* `:top`  is the number of results desired, default is 10
* `:display` sepcifies how results are displayed, could be one of these:
   - `:refs` only returns `vec-ref`, the default.
   - `:refs+dists` add distances to results.
* `:vec-filter` is a boolean function that takes `vec-ref` and determine if to
  return it.
* `:domains` specifies a list of domains to be searched (see below).

#### Search options for embedding search

`embedding-neighbors` supports the same `:top` and `:display` options:

* `:top` is the number of results desired, default is 10
* `:display` controls the result shape:
  - `:refs` returns `[e a v]`, the default.
  - `:refs+dists` returns `[e a v dist]`.
* `:domains` specifies a list of embedding domains to search when using the
  domain-based form of `embedding-neighbors`.

#### Vector search domains

Vectors can be added to different vector search domains, each corresponding to a
vector index of its own. The option map of `new-vector-index` has a `:domain`
that is a string value. If not specified, the default domain is `"datalevin"`.

When starting a  Datalog store, a `:vector-domains` option can be added to the
option map, and its value is a map from domain strings to the option maps of each
vector search domain, which is the same option map as that of `new-vector-index`.

A `:vector-opts` option can be passed to the Datalog store to give default
options in case `:vector-domans` are not given. Note that `:dimensions` option
is required for a vector index.

By default, each attribute with type `:db.type/vec` becomes its own domain
automatically. The domain name follows the following rules:

* for attribute without namespace, the name without `":"` is the domain name,
  e.g. `:vec` has domain name `"vec"`

* for attribute with namespace, in addition, `"/"` needs to be replaced by
`"_"`, e.g. `:name/embedding` has domain name `"name_embedding"`.  This is to
avoid conflict with directory separator `/` on POSIX systems.

Such attribute can also have a `:db.vec/domains` property that
list additional domains this attribute participates in. Users need to make sure
the domains an attribute participate in all have the same vector dimensions.

During search, `:domains` can be added to the option map to specify the
domains to be searched.

#### Embedding domains and providers

Embedding domains are configured separately from vector domains. Each embedding
domain is backed by its own vector index and is associated with an embedding
provider and vector-search parameters such as `:metric-type` and
`:dimensions`.

When opening a Datalog store:

* `:embedding-opts` provides default settings for embedding domains.
* `:embedding-domains` provides per-domain overrides.
* `:embedding-providers` supplies runtime-only provider implementations or
  provider specs.

If a configured embedding domain references a provider that is not available at
runtime, opening the store fails. If a domain has stored dimensions that do not
match the runtime provider, opening the store also fails.

## References

[1] Malkov, Yu A and Yashunin, Dmitry A, "Efficient and robust approximate
nearest neighbor search using hierarchical navigable small world graphs", IEEE
transactions on pattern analysis and machine intelligence, 42:4, 2018.
