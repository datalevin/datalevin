# Datalevin Full-text Search Engine

Datalevin includes a built-in full-text search engine.

## Rationale

Traditionally, databases and search engines are separate technology fields.
However, from the point of view of an end user, there is hardly a reason why
these two should be separated. A database is for storing and querying data, so is
a search engine. Although many databases have some full-text search
capabilities, their performance in term of relevance and speed is limited
compared with standalone search engines.

In a Datalog database, a full-text search function can be seen as
just another function or predicate to be used in a query. For example, Datomic
On-Prem has a `fulltext` function that allows full text search on a single
attribute, which is implemented with [Apache Lucene](https://lucene.apache.org/)
search engine.

Datalevin's built-in full-text search engine supports more powerful
search across the whole database. The reason why developing a search engine of
our own for Datalevin, instead of using an existing search engine, is the
following:

Standalone search engines, such as Apache Lucene, often have to implement simple
database-like concepts, such as `fields`, in order to capture structural
information of documents. In fact, people often use them as specialized
databases because of that, e.g. Elasticsearch. Datalevin is a versatile Database
with full featured database transaction and query functionalities. Including a
standalone search engine would introduce redundant and unnecessary database-like
features that are less powerful and not as well integrated with the rest of the
database.

Another way to look at the issue is through the sizes of the dependencies. Popular
search engines, such as Lucene, have huge code base with many features. The
library jar of lucene-core, not including any options or modules, is 3.4 MB (the
whole package download is more than 85 MB), whereas the total size of Datalevin
library jar is 120 KB.

Finally, with a search engine of our own, we avoid unnecessary write
amplification introduced by storing the source text twice, once in the database,
again in the search engine. Instead, the embedded search engine only needs to
store a reference to the source content that is stored in the database.

## Usage

The full-text search functionalities are available to use in all supported
Datalevin modes: key-value store, Datalog store, embedded, client/server, or
Babashka pods.

### Standalone Full-text Search

Datalevin can be used as a standalone search engine. The standalone search API
involves only a few functions: `new-search-engine`, `add-doc`, `remove-doc`, and
`search`.

```Clojure
(require '[datalevin.core :as d])

;; A search engine depends on a key-value store to store the indices.
(def lmdb (d/open-kv "/tmp/search-db"))
(def engine (d/new-search-engine lmdb {:index-position? true}))

;; Here are the documents to be indexed, keyed by doc-id
(def docs
  {1 "The quick red fox jumped over the lazy red dogs."
   2 "Mary had a little lamb whose fleece was red as fire."
   3 "Moby Dick is a story of a whale and a man obsessed."})

;; Add the documents into the search index. `add-doc` takes a `doc-ref`, which
;; can be anything that uniquely identify a document, in this case, a doc-id
(d/add-doc engine 1 (docs 1))
(d/add-doc engine 2 (docs 2))
(d/add-doc engine 3 (docs 3))

;; search by default returns a list of `doc-ref` ordered by relevance to query
(d/search engine "red")
;=> (1 2)

;; search syntax supports arbitrary boolean expression and phrases
(d/search engine [:and {:phrase "little lamb"} "fleece"])
;=> (2)

;; we can alter the display to show offsets of term occurrences as well, useful
;; e.g. to highlight matched terms in documents
(d/search engine "red" {:display :offsets})
;=> ([1 (["red" [10 39]])] [2 (["red" [40]])])
```

### Search in Datalog

Searchable values of the Datalog attributes need to be declared in the
schema, with the `:db/fulltext true` property. The value does not have to be of
 string type, as the indexer will call `str` function on it to convert it to
 string.

Such attribute may have a property `:db.fulltext/domains`, indicating which
full-text search domains the attribute should participate. If it also has
`:db.fulltext/autoDomain true`, this attribute becomes its own domain, with
domain name the same as attribute name.

A query function `fulltext` is provided to allow full-text search in Datalog
queries. This function takes the db, the query and an optional option map (same
as `search`), and returns a sequence of matching datom tuples ordered by
relevance to the query. By default (`:display :refs`), each tuple is `[e a v]`
for easy destructuring. With other `:display` values, `fulltext` returns:

* `:texts` as `[e a v text]`
* `:offsets` as `[e a v offsets]`
* `:texts+offsets` as `[e a v text offsets]`

The query is the same as that of the standalone search engine, so you can use
the same search expression.

```Clojure
(let [db (-> (d/empty-db "/tmp/mydb"
               {:text {:db/valueType           :db.type/string
                       :db/fulltext            true
                       ;; `:text` attribute will be turned into a search domain of its own
                       :db.fulltext/autoDomain true}}
               ;; phrase search requires indexing position
               {:search-domains {"text" {:index-position? true}}})
             (d/db-with
                 [{:db/id 1,
                   :text  "The quick red fox jumped over the lazy red dogs."}
                  {:db/id 2,
                   :text  "Mary had a little lamb whose fleece was red as fire."}
                  {:db/id 3,
                   :text  "Moby Dick is a story of a whale and a man obsessed."}]))]
    ;; full DB search
    (d/q '[:find ?e ?a ?v
           :in $ ?q
           :where [(fulltext $ ?q) [[?e ?a ?v]]]]
          db
          "red fox")
;=> #{[1 :text "The quick red fox jumped over the lazy red dogs."]
;     [2 :text "Mary had a little lamb whose fleece was red as fire."]}

    ;; attribute specific search
    (d/q '[:find ?e ?a ?v
           :in $ ?q
           :where [(fulltext $ :text ?q {:top 1}) [[?e ?a ?v]]]]
          db
          [:and {:phrase "little lamb"} "fleece"]
;=> #{[2 :text "Mary had a little lamb whose fleece was red as fire."]}
```

In the above example, we destructure the results into three variables,
`?e`, `?a` and `?v`.

When `:display` is not `:refs`, destructure extra values in the result tuple:

```Clojure
;; include document text
(d/q '[:find ?e ?a ?v ?text
       :in $ ?q
       :where [(fulltext $ ?q {:top 1 :display :texts})
               [[?e ?a ?v ?text]]]]
     db
     "red fox")

;; include both text and offsets
(d/q '[:find ?e ?a ?v ?text ?offsets
       :in $ ?q
       :where [(fulltext $ ?q {:top 1 :display :texts+offsets})
               [[?e ?a ?v ?text ?offsets]]]]
     db
     "red fox")
```

The search can be across the whole database, specific to an
attribute, or specific to a list of domains.

To further filter the search results,
a `doc-filter` function can be supplied in the search option, that takes the
`doc-ref` and return true or false, e.g. `{:doc-filter #(= (:a %) :text)}` will
only return datoms that have attribute `:text`. Or one can opt to put this
constraint in the Datalog where clause instead.

### Search Configurations

The search feature can be customized at indexing time and at query time.

#### Custom analyzer

When creating the search engine, an `:analyzer` option can be used to supply
an analyzer function that takes a document as string, and output a list of `[term
position offset]`. `:query-analyzer` option is for analyzing query.

Many common utility functions for creating analyzers are also provided in
`datalevin.search-utils`  namespace: stemming, stop words, regular expression,
ngrams, prefix, and so on.

#### Search options

In `search` and `fulltext` functions, an option map can be passed as the
last argument at run time to customize search. It can have these keys:

* `:top`  is the number of results desired, default is 10
* `:proximity-expansion` is the expansion factor for proximity search, default
is 2,
* `:proximity-max-dist` is max distance considered for proximity search, default
is 45,
* `:display` specifies how results are displayed, could be one of these:
   - `:refs` only returns `doc-ref`, the default.
   - `:texts` add the raw text of the documents to the results.
   - `:offsets` add the offsets of the matched tokens to the results.
   - `:texts+offsets` add both texts and offsets to results.
  In Datalog `fulltext`, these map to tuple shapes `[e a v]`, `[e a v text]`,
  `[e a v offsets]`, and `[e a v text offsets]`.
* `:doc-filter` is a boolean function that takes `doc-ref` and determine if to
  return the document.
* `:domains` specifies a list of domains to be searched (see below).

#### Search domains

Documents can be put into different search domains, each corresponding to a
search engine of its own. The option map of `new-search-engine` has a `:domain`
that is a string value. If not specified, the default domain is `datalevin`.

When starting a  Datalog store, a `:search-domains` option can be added to the
option map, and its value is a map from domain strings to the option maps of each
search domain.

An attribute with `:db/fulltext true` can have a `:db.fulltext/domains`
property that list the domains this attribute participates in. By default, all
full-text attributes are added to the default `datalevin` domain.

These full-text attributes can also be set as `db.fulltext/autoDomain true`, so
the attribute becomes its own domain automatically, and the domain name is the
attribute name without `:`.

A `:search-opts` option can be passed to the Datalog store to give default
search options for `fulltext` function.

During search, `:domains` can be added to the option map to specify the
domains to be searched.

## Implementation

As mentioned, the search engine is implemented from scratch, see [blog](https://yyhh.org/blog/2021/11/t-wand-beat-lucene-in-less-than-600-lines-of-code/). Instead of
using a separate storage, the search engine indices are stored in the same
file along with other database data, i.e. all Datalevin data is stored in a
single [LMDB](http://www.lmdb.tech/doc/) data file. This improves cache locality
and reduces the complexity of managing data.

### Indexing

The search engine indices are stored in one inverted list and two key-value maps.
In addition to information about each term and each document, the positions of term
occurrences in the documents are also stored to support match highlighting and
proximity query.

Specifically, the following LMDB sub-databases are created for search purposes:

* `terms`: map of term -> `term-info`.
* `docs`: map of document id -> document reference and document norm.
* `positions`: inverted lists of term id and document id -> positions and offsets
  of the term in the document. This storage is optional, where user set it with
  `:index-position?` option.

Most information needed for scoring documents were pre-calculated during
indexing, and loaded into memory on demand. For example, the norms of all
documents were loaded into memory during search engine initialization, the same
as Lucene. Term specific information are loaded into memory in aggregates at the
beginning of query processing.

Specifically, a `term-info` consists of the following information about a term:

* `term-id`, unique id of the term, assigned auto-incrementally during indexing
* `max-weight`, the maximum weight of the term
* `doc-freq-sparse-list`, consists of compressed bitmap of document ids and
  corresponding list of term frequencies

`doc-freq-sparse-list` uses our implementation of a sparse integer list,
constructed with two data structures working together. One structure is the
index, containing document ids, represented by a bitmap; and the other is an
array list of integers, containing term frequencies. This is the primary data
structure for searching, and they are loaded into memory per user query terms.
The list of a term is un/compressed during retrieval/storage as a whole.

### Searching

Scoring and ranking of documents implements the standard `tf-idf` and vector space
model. In order to achieve a good balance between relevance and efficiency,
the weighting scheme chosen is `lnu.ltn` [2], i.e. the document vector has
log-weighted term frequency, no idf, and pivoted unique normalization, while the
query vector uses log-weighted term frequency, idf weighting, and no
normalization. One significant difference from the standard `BM25` schema (used
in Lucne) is the document normalization method. Pivoted unique normalization is
chosen for it takes document lengths into consideration, does not needlessly
penalize lengthy documents, and it is cheaper to calculate.

An original algorithm,  what we call *T-Wand*, is developed and implemented for
searching. *T* stands for "Tiered", *Wand* [1] is a state of art search
algorithm used in many search engines, including Lucene. Our algorithm is an
marriage of the main ideas of *Wand* and some of our owns.

As the name suggests, our algorithm works in tiers. It removes
early on those documents that are unlikely to be relevant due to missing query
terms. Not only is it efficient, this approach also addresses an often felt user
frustration with search engines: a document containing all query terms
may be ranked much lower than a document containing only partial query terms. In
our algorithm, the documents containing more query terms are considered first
and are guaranteed to return earlier than those documents with poorer term
coverage.

#### T-Wand algorithm

The details of the *T-Wand* algorithm is the following.

First, we want to consider documents containing all `n` user specified query
terms. Instead of looping over all `n` inverted lists of all query terms, we
first pick the query term with the least document frequency (i.e. the most rare term), use
its inverted list of documents as the candidates, and forgo all other documents.
This is sufficient, because, for a document to contain all `n` query terms, it
must contains the rarest one among them. More generally, there is a simple
(trivial?) mathematical property that can be stated as the following [3]:

```
Let there be a set X with size n and a set Y with any size. For any subset Z in
X of size (n-t+1), if the size of the intersection of X and Y is greater or
equal to t, then Z must intersect with Y.
```

This property allows us to develop search algorithms that use the inverted lists
of any `n-t+1` terms alone as the candidates, in order to find documents that
contain `t` query terms. We will of course choose the rarest ones (i.e. shortest
lists) to start with. For example, to search for documents with all `n` query
terms, it is sufficient to search in documents containing the rarest term; to
search for documents with `n-1` terms, it is sufficient to search in the union
of the rarest and the second rarest term's documents; so on and so forth. When
there are very rare terms in the query, very few candidate documents need to be
examined.

We loop over this list of candidate documents, for each document, check if it
appears in the inverted lists of subsequent query terms.

Critically, during this process, we remove a candidate as soon as one of the
following two conditions is met:

* this document is not going to appear in the required number of inverted lists,
  based on the aforementioned mathematical property. This is the main innovation
  of the *T-Wand* algorithm.

or,

* this document is not going to make into the top K results, based on an
  approximate score calculation using the pre-computed maximum weight of the
  terms. This is the main idea behind *Wand* algorithm's search efficiency [1],
  as it allows skipping full scoring of many documents.

If all candidates are exhausted and user still requests more results, the
documents containing the second rarest query term are added to candidates list.
They are checked against the remaining query terms to select the candidates that
appear in `n-1` inverted lists. If user keeps asking for more results, the
process continues until candidates only need to appear in `1` inverted list.

Essentially, this search algorithm processes documents in tiers. First tier are
those documents containing all `n` query terms, then `n-1` terms, then `n-2`, and
so on. Document ranking is performed within a tier, not cross tiers. As the
number of tiers goes up, the query is gradually turned from an all-AND to
an all-OR query.

#### *T-Wand* implementation

As can be seen, the implementation of *T-Wand* relies heavily on intersection
and union of document ids. Our implementation is helped by [Roaring
Bitmaps](https://roaringbitmap.org/), a fast compressed bitmap library used in
many projects. The parallel walking of document IDs of different terms required
by *Wand* is achieved by iterating the bitmaps.

As mentioned, our storage of term frequencies is handled by an integer array
list, indexed by a bitmap of document ids. The access of a term frequency is
through the `rank` method of Roaring Bitmaps, which seems to be an innovation,
as far as I can tell. When stored, the integer list is compressed with
[JavaFastPFOR](https://github.com/lemire/JavaFastPFOR), another excellent
library by Daniel Lemire, the same author of Roaring Bitmaps.

[Eclipse Collections](https://www.eclipse.org/collections/) is used to reduce
memory footprint whenever appropriate.

The query result preparation are implemented as Clojure transducers, and the
results are wrapped in the `sequence` function, which returns results
incrementally and on demand.

### Term Proximity Scoring

When the user elects to enable term positions indexing, it is beneficial to
utilize the positional information to enable term proximity scoring to enhance
precision of top results. This reflects the intuition that the closer the query
terms are placed together in a document, the more relevant the document
might be. We enable term proximity scoring when `:index-position?` is set to
`true`.

Instead of replacing term frequency by proximity score [5], which is
relatively expensive to calculate, or combining the proximity score with the
tf-idf score [4], which faces the difficult problem of determining the relative
weights of the two scores that may require machine learning, we decide to
perform a two stage procedure: we search by tf-idf based scoring first as usual,
then calculate proximity score only for the top results, and finally produce the
top `k` results according to the proximity score.

For the first tf-idf stage, instead of producing top `k` results only, we
produce top `m * k` results, where `m` is user configurable as
`:proximity-expansion` (default is `2`) search option. This parameter reflects a
search quality vs. time trade-off. The larger is `m`, the better is the search
quality, while the search time would be longer.

A span based proximity scoring algorithm is used to calculate the proximity
contribution of individual terms, and they are then plugged into the Okapi
ranking function to arrive at the final score [5].

### Boolean Search Expression

Boolean operators `:and`, `:or`, and `:not` can be arbitrarily nested to form
complex boolean expressions. For example:

`[:or "fox" "red" [:and "black" "sheep" [:not "yellow"]]]`

Since the boolean search expression is just a Clojure data structure, we just
walk the boolean expression as a tree, and apply the corresponding boolean
operators on the bitmaps of the operand terms' `doc-freq-sparse-list`. This is
done prior to the tf-idf scoring of documents, effectively pre-filtering
documents using the boolean expression.

### Phrase Search

Phrases can be encoded in a map, e.g. `{:phrase "computer science"}`. The
phrases can be embedded in a boolean expression, for example,

`[:or [:not [:and {:phrase "fleece jacket"} "robber"]] [:not {:phrase "red fox"}]]`

Phrase search feature is only available when `:index-position? true`. Using
phrases in the query without this option enabled for the search engine will
generate an error.

Phrase matching is done after documnt scoring and right before a document is
about to be put into the result priority queue, so the document will not be
added to the results if it does not have the required phrases or has the
forbidden phrases.

## Benchmark

The details of benchmark comparison with Lucene is
[here](https://github.com/juji-io/datalevin/tree/master/benchmarks/search-bench).
The summary is that Datalevin search engine beats Lucene in search speed, but
lags in write speed, as expected.

## References

[1] Broder, A.Z., Carmel, D., Herscovici, M, Soffer,A., and Zien, J..  Efficient
query evaluation using a two-level retrieval process. In Proceedings of the
twelfth international conference on Information and knowledge management (CIKM
'03). pp.426–434.

[2] Manning, C.D., Raghavan, P. and Schütze, H. Introduction to Information
Retrieval, Cambridge University Press. 2008.

[3] Okazaki, N. and Tsujii, J., Simple and Efficient Algorithm for Approximate
Dictionary Matching. Proceedings of the 23rd International Conference on
Computational Linguistics (COLING '10), 2010, pp. 851-859.

[4] Rasolofo, Y., & Savoy, J.. Term proximity scoring for keyword-based
retrieval systems. In Advances in Information Retrieval: 25th European
Conference on IR Research, (ECIR '03), pp. 207-218.

[5] Song, R., Taylor, M. J., Wen, J. R., Hon, H. W., & Yu, Y. Viewing term
proximity from a different perspective. In Advances in Information Retrieval:
30th European Conference on IR Research, (ECIR '08), pp. 346-357.
