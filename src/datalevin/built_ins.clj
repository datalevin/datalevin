;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.built-ins
  "Built-in predicates or functions for Datalog query, i.e. used in `q`"
  (:refer-clojure :exclude [and or < > <= >= min max = == not= + - * / quot rem
                            mod inc dec zero? pos? neg? even? odd? compare rand
                            rand-int true? false? nil? some? not complement
                            identical? identity keyword meta name namespace
                            type vector list set hash-map array-map count
                            range not-empty empty? contains? str print-str
                            println-str prn-str subs get re-find re-matches
                            re-seq re-pattern distinct])
  (:require
   [datalevin.db :as db]
   [datalevin.datom :as dd]
   [datalevin.storage :as st]
   [datalevin.idoc :as idoc]
   [datalevin.embedding :as emb]
   [datalevin.query.tuple :as qtuple]
   [datalevin.udf :as udf-reg]
   [datalevin.vector :as v]
   [datalevin.entity :as de]
   [datalevin.remote :as r]
   [datalevin.util :as u :refer [raise long-inc]]
   [datalevin.interface :refer [search schema search-vec attrs]])
  (:import
   [java.util List]
   [java.nio.charset StandardCharsets]
   [datalevin.utl LikeFSM LRUCache]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.roaringbitmap PeekableIntIterator RoaringBitmap]
   [datalevin.idoc IdocIndex]
   [datalevin.storage Store]
   [datalevin.remote DatalogStore]
   [datalevin.db DB]))

(def ^:no-doc like-cache (LRUCache. 256))
(def ^:no-doc not-like-cache (LRUCache. 256))
(def ^:no-doc like-options-cache (LRUCache. 256))

(defn- compile-like
  [^String pattern escape not?]
  (let [pb  (.getBytes pattern StandardCharsets/UTF_8)
        fsm (if escape (LikeFSM. pb escape) (LikeFSM. pb))
        f   #(.match fsm (.getBytes ^String % StandardCharsets/UTF_8))]
    (if not? #(clojure.core/not (f %)) f)))

(defn like
  "Predicate similar to `LIKE` in SQL, e.g. `[(like ?name \"%Smith\")]`"
  ([input pattern]
   (like input pattern nil false))
  ([input pattern opts]
   (like input pattern opts false))
  ([input ^String pattern {:keys [escape]} not?]
   (let [^LRUCache cache (if escape
                           like-options-cache
                           (if not? not-like-cache like-cache))
         k               (if escape [pattern escape not?] pattern)
         matcher         (clojure.core/or
                           (.get cache k)
                           (let [mf (compile-like pattern escape not?)]
                             (.put cache k mf)
                             mf))]
     (matcher input))))

(defn not-like
  "Predicate similar to `NOT LIKE` in SQL, e.g. `[(no-like ?name \"%Smith\")]`"
  ([input pattern]
   (not-like input pattern nil))
  ([input pattern opts]
   (like input pattern opts true)))

(defn in
  "Predicate similar to `IN` in SQL, e.g.
  `[(in ?name [\"Smith\" \"Cohen\" \"Doe\"])]`"
  ([input coll]
   (in input coll false))
  ([input coll not?]
   (assert (clojure.core/and (coll? coll) (clojure.core/not (map? coll)))
           "function `in` expects a collection")
   (let [checker (let [s (clojure.core/set coll)]
                   (if not? #(clojure.core/not (s %)) s))]
     (checker input))))

(defn not-in
  "Predicate similar to `NOT IN` in SQL,
  e.g. `[(not-in ?name [\"Smith\" \"Cohen\" \"Doe\"])]`"
  [input coll] (in input coll true))

(defn- -differ?
  [& xs]
  (let [l  (clojure.core/count xs)
        hl (clojure.core// l 2)]
    (clojure.core/not= (take hl xs) (drop hl xs))))

(defn get-else
  "Function. Return the value of attribute `a` of entity `e`, or `else-val` if
  it doesn't exist. e.g. `[(get-else $ ?a :artist/name \"N/A\") ?name]`"
  [db e a else-val]
  (when (clojure.core/nil? else-val)
    (raise "get-else: nil default value is not supported" {:error :query/where}))
  (if-some [datom (db/-first db [(db/entid db e) a])]
    (:v datom)
    else-val))

(defn get-some
  "Function. Takes a DB, an entity, and one or more cardinality one attributes,
   return a tuple of the first found attribute and its value. e.g.
  `[(get-some $ ?e :country :artist :book) [?attr ?val]]`"
  [db e & as]
  (unreduced
    (reduce
      (fn [_ a]
        (when-some [datom (db/-first db [(db/entid db e) a])]
          (reduced [(:a datom) (:v datom)])))
      nil
      as)))

(defn get-some-else
  "Total variant of `get-some`. Return the first found attribute and value, or
   `[nil else-val]` when none of the cardinality-one attributes exists. The
   explicit fallback makes this function cardinality preserving, which allows
   projection-only calls to run after top-k. e.g.
  `[(get-some-else $ ?e \"N/A\" :country :artist :book) [_ ?val]]`"
  [db e else-val & as]
  (clojure.core/or (apply get-some db e as) [nil else-val]))

(def ^:no-doc post-top-k-enrichment-fns
  "Built-in functions that deterministically produce exactly one tuple per
   input row and perform property enrichment suitable for post-top-k
   execution when the query planner proves the remaining dependencies safe."
  #{'get-some-else 'datalevin.built-ins/get-some-else})

(def ground
  "Function. Same as Clojure `identity`. E.g.
  `[(ground [:a :e :i :o :u]) [?vowel ...]]`"
  clojure.core/identity)

(def ^:private query-var
  (delay
    (clojure.core/or
      (resolve 'datalevin.query/q-nested)
      (requiring-resolve 'datalevin.query/q-nested))))

(defn q
  "Function. Run a nested Datalog query. A relation result can be bound to a
  relation binding, allowing aggregate results to feed later query clauses.
  An uncorrelated nested query, whose inputs contain no outer logic variables,
  is evaluated once. E.g.
  `[(q '[:find (min ?duration)
         :where [_ :track/duration ?duration]]
      $) [[?duration]]]`"
  [query & inputs]
  (if-let [query-fn @query-var]
    (apply query-fn query inputs)
    (raise "Can't resolve datalevin.query/q" {:error :query/where})))

(defn missing?
  "Predicate that returns true if the entity has no value for the attribute in DB
  e.g. [(missing? $ ?e :sales)]"
  [db e a]
  (clojure.core/nil? (clojure.core/get (de/entity db e) a)))

(defn- and-fn
  [& args]
  (unreduced (reduce (fn [_ b] (if b b (reduced b))) true args)))

(def and
  "Predicate that is similar to Clojure `and`
  e.g. [(and (= ?g \"f\"\") (like ?n \"A%\"\"))]"
  and-fn)

(defn- or-fn
  [& args]
  (unreduced (reduce (fn [_ b] (if b (reduced b) b)) nil args)))

(def or
  "Predicate that is similar to Clojure `or`
  e.g. [(or (= ?g \"f\"\") (like ?n \"A%\"\"))]"
  or-fn)

(defn- fulltext*
  [^FastList res aid->attr lmdb engines query opts domain ^ints needed]
  (let [engine  (engines domain)
        display (or (:display opts)
                    (get-in engine [:search-opts :display])
                    :refs)
        emit    (qtuple/make-fulltext-emitter
                  lmdb aid->attr display needed)]
    (doseq [d (search engine query opts)]
      (.add res (emit d)))))

(defn- extract-needed
  [arg3 arg2 arg1]
  (clojure.core/or
    (some-> arg3 clojure.core/meta :tuple-needed)
    (some-> arg2 clojure.core/meta :tuple-needed)
    (some-> arg1 clojure.core/meta :tuple-needed)))

(defrecord ^:no-doc FulltextRequest
    [store lmdb engines aid->attr query opts domains needed])

(defn ^:no-doc fulltext-request
  "Normalize one fulltext invocation without executing the search. This is the
  shared semantic boundary used by ordinary function execution and physical
  fulltext access paths."
  [^DB db arg1 arg2 arg3 ^ints needed]
  (let [^Store store (.-store db)
        engines      (.-search-engines store)
        attr?        (keyword? arg1)
        domains      (if attr?
                       [(u/keyword->string arg1)]
                       (:domains arg2))
        query        (if attr? arg2 arg1)
        opts         (if attr? arg3 arg2)]
    (when attr?
      (when-not (-> store schema arg1 :db.fulltext/autoDomain)
        (raise ":db.fulltext/autoDomain is not true for " arg1 {})))
    (->FulltextRequest
      store (.-lmdb store) engines (attrs store) query opts
      (if (seq domains) domains (keys engines)) needed)))

(defn ^:no-doc execute-fulltext-request
  "Execute a normalized fulltext request and return its compact tuples."
  [{:keys [lmdb engines aid->attr query opts domains needed]}]
  (let [res (FastList.)]
    (doseq [domain domains]
      (fulltext* res aid->attr lmdb engines query opts domain needed))
    res))

(defn ^:no-doc fulltext-request-results
  "Return the exact logical result stream of a normalized fulltext request.
  Search ranking uses the request's original options; tuple decoding remains
  lazy so an access cursor can stop consuming the stream early."
  [{:keys [lmdb engines aid->attr query opts domains needed]}]
  (mapcat
    (fn [domain]
      (let [engine  (engines domain)
            display (clojure.core/or
                      (:display opts)
                      (get-in engine [:search-opts :display])
                      :refs)
            emit    (qtuple/make-fulltext-emitter
                      lmdb aid->attr display needed)]
        (map emit (search engine query opts))))
    domains))

(defn fulltext
  "Function that does fulltext search. Returns matching tuples ordered by
  relevance.

  By default (`:display :refs`), each result tuple is `[e a v]`.

  The last argument of the 4 arity function is the search option map.
  See [[datalevin.core.search]].

  Additional values are returned when `:display` is set in the options:
  * `:refs+scores` returns `[e a v score]`
  * `:texts` returns `[e a v text]`
  * `:offsets` returns `[e a v offsets]`
  * `:texts+offsets` returns `[e a v text offsets]`

  When neither an attribute nor a `:domains` is specified, a full DB search
  is performed.

  Attribute-specific search requires `:db.fulltext/autoDomain true` on the
  attribute.

  For example:

  * Full DB search: `[(fulltext $ \"red\") [[?e ?a ?v]]]`

  * Attribute specific search: `[(fulltext $ :color \"red\") [[?e ?a ?v]]]`

  * Domain specific search:

    `[(fulltext $ \"red\" {:domains [\"color\"]} [[?e ?a ?v]])]`

  * Search with scores:

    `[(fulltext $ \"red\" {:display :refs+scores})
      [[?e ?a ?v ?score]]]`

  * Search with text and offsets:

    `[(fulltext $ \"red\" {:display :texts+offsets})
      [[?e ?a ?v ?text ?offsets]]]`"
  ([db query]
   (fulltext db query nil))
  ([db arg1 arg2]
   (fulltext db arg1 arg2 nil))
  ([^DB db arg1 arg2 arg3]
   (execute-fulltext-request
     (fulltext-request db arg1 arg2 arg3
                       (extract-needed arg3 arg2 arg1)))))

(defn fulltext-datoms
  ([db query]
   (fulltext-datoms db query nil))
  ([^DB db query opts]
   (let [store (.-store db)]
     (if (instance? DatalogStore store)
       (r/fulltext-datoms store query opts)
       (let [^FastList res (fulltext db query opts)]
         (mapv (fn [^objects t] [(aget t 0) (aget t 1) (aget t 2)])
               res))))))

(defn- vector-neighbor-results
  [aid->attr lmdb index query opts ^ints needed]
  (let [display (or (:display opts)
                    (:display (.-search-opts
                                ^datalevin.vector.VectorIndex index))
                    :refs)
        emit    (qtuple/make-vector-emitter
                  lmdb aid->attr display needed)]
    (map emit (search-vec index query opts))))

(defrecord ^:no-doc VectorRequest
    [kind store lmdb indices aid->attr query opts domains needed])

(defn ^:no-doc vec-neighbors-request
  "Normalize one vector-neighbor invocation without executing its approximate
  searches."
  [^DB db arg1 arg2 arg3 ^ints needed]
  (let [^Store store (.-store db)
        indices      (.-vector-indices store)
        attr?        (keyword? arg1)
        domains      (if attr?
                       [(v/attr-domain arg1)]
                       (:domains arg2))
        query        (if attr? arg2 arg1)
        opts         (if attr? arg3 arg2)]
    (when-not (and (sequential? domains) (seq domains))
      (raise "Need a vector search domain." {}))
    (->VectorRequest
      :vector store (.-lmdb store) indices (attrs store) query opts
      (vec domains) needed)))

(defn ^:no-doc embedding-neighbors-request
  "Normalize one embedding-neighbor invocation without embedding the query or
  executing its approximate searches."
  [^DB db arg1 arg2 arg3 ^ints needed]
  (let [^Store store (.-store db)
        indices      (.-embedding-indices store)
        attr?        (keyword? arg1)
        domains      (if attr?
                       [(v/attr-domain arg1)]
                       (:domains arg2))
        query        (if attr? arg2 arg1)
        opts         (if attr? arg3 arg2)]
    (when attr?
      (when-not (-> store schema arg1 :db.embedding/autoDomain)
        (raise ":db.embedding/autoDomain is not true for " arg1 {})))
    (when-not (string? query)
      (raise "Embedding query must be a string" {:query query}))
    (when-not (and (sequential? domains) (seq domains))
      (raise "Need an embedding search domain." {}))
    (let [domains (vec domains)
          missing (seq (remove indices domains))]
      (when missing
        (raise "Embedding domain not found: " missing {:domains missing}))
      (->VectorRequest
        :embedding store (.-lmdb store) indices (attrs store) query opts
        domains needed))))

(defn- embedding-query-vector
  [^Store store domain query]
  (let [provider (st/embedding-provider store domain)]
    (when-not provider
      (raise "Embedding provider is not initialized" {:domain domain}))
    (first
      (emb/embedding provider
                     [{:text query :kind :query :domain domain}]
                     nil))))

(defn ^:no-doc vector-request-results
  "Return the exact logical result stream of an existing approximate vector or
  embedding-neighbor invocation. Each domain retains its own configured top-N
  search and the original domain concatenation order."
  [{:keys [kind store lmdb indices aid->attr query opts domains needed]}]
  (mapcat
    (fn [domain]
      (when-let [index (indices domain)]
        (let [query-vector
              (case kind
                :vector    query
                :embedding (embedding-query-vector store domain query))]
          (vector-neighbor-results
            aid->attr lmdb index query-vector opts needed))))
    domains))

(defn ^:no-doc execute-vector-request
  "Materialize a normalized vector request for ordinary query-function
  execution."
  [request]
  (let [res (FastList.)]
    (doseq [tuple (vector-request-results request)]
      (.add res tuple))
    res))

(defn vec-neighbors
  "Function that does vector similarity search. Returns matching tuples of
  (e a v) for convenient destructuring.

  The last argument of the 4 arity function is the search option map.
  See [[datalevin.core.search-vec]].
  When `:display` is `:refs+dists`, each result is `[e a v dist]`.

  When neither an attribute nor a `:domains` is specified, an exception will
  be thrown.

  For example:

  * Attribute specific search:
         `[(vec-neighbors $ :color ?query-vec) [[?e ?a ?v]]]`

  * Domain specific search:
        `[(vec-neighbors $ ?query-vec {:domains [\"color\"]} [[?e ?a ?v]])]`"
  ([db query]
   (vec-neighbors db query nil))
  ([db arg1 arg2]
   (vec-neighbors db arg1 arg2 nil))
  ([^DB db arg1 arg2 arg3]
   (execute-vector-request
     (vec-neighbors-request
       db arg1 arg2 arg3 (extract-needed arg3 arg2 arg1)))))

(defn embedding-neighbors
  "Function that does embedding similarity search over `:db/embedding` domains.

  The query input is text. The function embeds the text using the provider
  configured for each searched domain, then returns matching source datom
  tuples `[e a v]` or `[e a v dist]` when `:display :refs+dists` is used.

  Attribute-specific search requires `:db.embedding/autoDomain true`."
  ([db query]
   (embedding-neighbors db query nil))
  ([db arg1 arg2]
   (embedding-neighbors db arg1 arg2 nil))
  ([^DB db arg1 arg2 arg3]
   (execute-vector-request
     (embedding-neighbors-request
       db arg1 arg2 arg3 (extract-needed arg3 arg2 arg1)))))

(defn- idoc-domain
  [store attr]
  (let [props ((schema store) attr)]
    (when-not (clojure.core/identical? (:db/valueType props) :db.type/idoc)
      (raise "Attribute is not an idoc type: " attr {:attribute attr}))
    (or (:db/domain props) (u/keyword->string attr))))

(defn- idoc-match-context
  [^Store store ^IdocIndex index query domain ^ints needed]
  (let [{:keys [ids exact? verify]} (idoc/candidate-ids* index query)
        lmdb      (.-lmdb store)
        aid->attr (attrs store)]
    {:index     index
     :query     query
     :domain    domain
     :ids       ids
     :exact?    exact?
     :verify    verify
     :verify?   (and (clojure.core/not exact?)
                     (clojure.core/not (idoc/ids-empty? verify)))
     :emit      (qtuple/make-datom-emitter lmdb aid->attr needed)
     :lmdb      lmdb}))

(defn- idoc-match-tuple
  [{:keys [^IdocIndex index query exact? verify verify? emit lmdb]}
   doc-id doc-ref]
  (cond
    exact?
    [(emit doc-ref) false]

    verify?
    (if (idoc/ids-contains? verify doc-id)
      (let [doc (idoc/doc-ref->doc lmdb doc-ref)]
        [(when (idoc/matches-doc? index doc query)
           (emit doc-ref doc))
         true])
      [(emit doc-ref) false])

    :else
    (let [doc (idoc/doc-ref->doc lmdb doc-ref)]
      [(when (idoc/matches-doc? index doc query)
         (emit doc-ref doc))
       true])))

(defn ^:no-doc idoc-match-domain
  [^Store store ^IdocIndex index query domain ^ints needed]
  (let [{:keys [ids exact? verify] :as context}
        (idoc-match-context store index query domain needed)]
    (if (clojure.core/nil? idoc/*trace*)
      (let [res (FastList.)]
        (idoc/ids-iterate-doc-refs
          index
          ids
          (fn [doc-id doc-ref]
            (when-let [tuple (first
                               (idoc-match-tuple
                                 context doc-id doc-ref))]
              (.add res tuple))))
        res)
      (let [start        (System/nanoTime)
            cand-count   (idoc/ids-count ids)
            verify-count (idoc/ids-count verify)
            doc-fetches  (volatile! 0)
            match-count  (volatile! 0)
            res          (FastList.)]
        (idoc/ids-iterate-doc-refs
          index
          ids
          (fn [doc-id doc-ref]
            (let [[tuple fetched?]
                  (idoc-match-tuple context doc-id doc-ref)]
              (when fetched?
                (vswap! doc-fetches long-inc))
              (when tuple
                (vswap! match-count long-inc)
                (.add res tuple)))))
        (idoc/*trace* {:event           :idoc-match-domain
                       :domain          domain
                       :candidate-count cand-count
                       :verify-count    verify-count
                       :doc-fetch-count @doc-fetches
                       :match-count     @match-count
                       :exact?          exact?
                       :elapsed-ns      (clojure.core/- (System/nanoTime) start)})
        res))))

(defrecord ^:no-doc IdocDomainMatchCursor
    [context ^PeekableIntIterator iterator candidate-count verify-count
     ^long started-at counters traced?])

(defrecord ^:no-doc IdocMatchCursor
    [request state closed?])

(defn- trace-idoc-domain!
  [^IdocDomainMatchCursor cursor partial?]
  (when (and idoc/*trace*
             (compare-and-set! (:traced? cursor) false true))
    (let [{:keys [domain exact?]} (:context cursor)
          {:keys [inspected doc-fetches matches]} @(:counters cursor)]
      (idoc/*trace* {:event           :idoc-match-domain
                     :domain          domain
                     :candidate-count (:candidate-count cursor)
                     :verify-count    (:verify-count cursor)
                     :inspected-count inspected
                     :doc-fetch-count doc-fetches
                     :match-count     matches
                     :exact?          exact?
                     :partial?        (boolean partial?)
                     :elapsed-ns      (clojure.core/-
                                        (System/nanoTime)
                                        (long (:started-at cursor)))}))))

(defn- prepare-idoc-domain-cursor
  [^Store store ^IdocIndex index query domain ^ints needed after-doc-id]
  (let [{:keys [ids verify] :as context}
        (idoc-match-context store index query domain needed)
        ^PeekableIntIterator iterator
        (.getIntIterator ^RoaringBitmap ids)]
    (when (clojure.core/some? after-doc-id)
      (if (clojure.core/< (long after-doc-id) Integer/MAX_VALUE)
        (.advanceIfNeeded iterator (unchecked-inc-int (int after-doc-id)))
        (while (.hasNext iterator) (.next iterator))))
    (->IdocDomainMatchCursor
      context iterator (idoc/ids-count ids) (idoc/ids-count verify)
      (System/nanoTime)
      (atom {:inspected 0 :doc-fetches 0 :matches 0})
      (atom false))))

(defn ^:no-doc prepare-idoc-match-cursor
  "Prepare a resumable idoc cursor. Candidate bitmaps are prepared lazily,
  one domain at a time, when the cursor is first read."
  ([request]
   (prepare-idoc-match-cursor request nil))
  ([request resume]
   (let [continuation (clojure.core/or (:continuation resume) resume)]
     (->IdocMatchCursor
       request
       (volatile!
         {:domain-index (long
                          (clojure.core/or
                            (:domain-index continuation) 0))
          :after-doc-id (:after-doc-id continuation)
          :domain-cursor nil})
       (atom false)))))

(defn- idoc-domain-cursor-batch
  [^IdocDomainMatchCursor cursor ^long maximum]
  (let [^PeekableIntIterator iterator (:iterator cursor)
        ids                           (FastList. (int maximum))]
    (loop [n 0
           last-doc-id nil]
      (if (and (clojure.core/< (long n) maximum)
               (.hasNext iterator))
        (let [doc-id (.next iterator)]
          (.add ids doc-id)
          (recur (unchecked-inc-int n) doc-id))
        (let [^FastList refs
              (idoc/doc-refs-by-ids
                (get-in cursor [:context :index]) ids)
              tuples      (FastList.)
              doc-fetches (volatile! 0)
              matches     (volatile! 0)]
          (loop [i 0
                 size (.size refs)]
            (when (clojure.core/< (long i) (long size))
              (let [[tuple fetched?]
                    (idoc-match-tuple
                      (:context cursor)
                      (.get refs i)
                      (.get refs (unchecked-inc-int i)))]
                (when fetched?
                  (vswap! doc-fetches long-inc))
                (when tuple
                  (vswap! matches long-inc)
                  (.add tuples tuple))
                (recur (unchecked-add-int i 2) size))))
          (swap! (:counters cursor)
                 (fn [counters]
                   (-> counters
                       (update :inspected clojure.core/+ n)
                       (update :doc-fetches clojure.core/+ @doc-fetches)
                       (update :matches clojure.core/+ @matches))))
          {:tuples      tuples
           :scanned     (long n)
           :last-doc-id last-doc-id
           :exhausted?  (clojure.core/not (.hasNext iterator))})))))

(defn ^:no-doc next-idoc-match-cursor-batch
  "Read at most `maximum` physical idoc candidates across the requested
  domains. Returns compact tuples plus an opaque continuation."
  [^IdocMatchCursor cursor ^long maximum]
  (if @(:closed? cursor)
    {:tuples (FastList.) :scanned 0 :continuation nil :exhausted? true}
    (let [{:keys [store indices query domains needed]} (:request cursor)
          domain-count (clojure.core/count domains)
          tuples       (FastList.)]
      (loop [scanned (long 0)]
        (let [{:keys [domain-index after-doc-id domain-cursor]}
              @(:state cursor)]
          (cond
            (clojure.core/<= domain-count (long domain-index))
            {:tuples tuples
             :scanned scanned
             :continuation nil
             :exhausted? true}

            (clojure.core/<= maximum (long scanned))
            {:tuples tuples
             :scanned scanned
             :continuation
             {:domain-index domain-index
              :after-doc-id after-doc-id}
             :exhausted? false}

            :else
            (let [domain        (nth domains domain-index)
                  domain-cursor
                  (clojure.core/or
                    domain-cursor
                    (prepare-idoc-domain-cursor
                      store (indices domain) query domain needed after-doc-id))
                  batch
                  (idoc-domain-cursor-batch
                    domain-cursor
                    (clojure.core/- maximum (long scanned)))
                  scanned       (long
                                  (clojure.core/+
                                    (long scanned)
                                    (long (:scanned batch))))
                  exhausted?    (:exhausted? batch)
                  last-doc-id   (:last-doc-id batch)]
              (.addAll tuples ^List (:tuples batch))
              (if exhausted?
                (do
                  (trace-idoc-domain! domain-cursor false)
                  (vreset! (:state cursor)
                           {:domain-index
                            (unchecked-inc (long domain-index))
                            :after-doc-id nil
                            :domain-cursor nil}))
                (vreset! (:state cursor)
                         {:domain-index domain-index
                          :after-doc-id last-doc-id
                          :domain-cursor domain-cursor}))
              (recur scanned))))))))

(defn ^:no-doc close-idoc-match-cursor
  [^IdocMatchCursor cursor]
  (when (compare-and-set! (:closed? cursor) false true)
    (when-let [domain-cursor (:domain-cursor @(:state cursor))]
      (trace-idoc-domain! domain-cursor true))))

(defrecord ^:no-doc IdocMatchRequest
    [store indices query opts domains needed])

(defn ^:no-doc idoc-match-request
  "Normalize one idoc-match invocation without preparing index candidates.
  Candidate preparation and tuple production are deferred to
  `execute-idoc-match-request`."
  [^DB db arg1 arg2 arg3 ^ints needed]
  (let [^Store store (.-store db)
        indices      (st/store-idoc-indices store)
        attr?        (keyword? arg1)
        domains0     (if attr?
                       [(idoc-domain store arg1)]
                       (:domains arg2))
        query        (if attr? arg2 arg1)
        opts         (if attr? arg3 arg2)
        domains      (or (when (map? opts) (:domains opts))
                         domains0
                         (keys indices))
        missing      (seq (remove indices domains))]
    (when missing
      (raise "Idoc domain not found: " missing {:domains missing}))
    (->IdocMatchRequest store indices query opts domains needed)))

(defn ^:no-doc execute-idoc-match-request
  "Execute a normalized idoc match request and return its compact tuples."
  [{:keys [indices query domains needed] :as request}]
  (let [^Store store (:store request)
        res          (FastList.)]
    (doseq [domain (if (seq domains) domains [])]
      (let [^List tuples (idoc-match-domain store (indices domain) query
                                             domain needed)]
        (when (and tuples (clojure.core/pos? (.size tuples)))
          (.addAll res tuples))))
    res))

(defn idoc-match
  "Function that searches indexed documents. Returns matching tuples of
  (e a v) for convenient destructuring.

  When neither an attribute nor :domains is specified, a full DB search is
  performed across all idoc domains.

  * Full DB search: `[(idoc-match $ {:status \"active\"}) [[?e ?a ?v]]]`
  * Attribute specific search:
       `[(idoc-match $ :person/profile {:status \"active\"}) [[?e ?a ?v]]]`
  * Domain specific search:
       `[(idoc-match $ {:status \"active\"} {:domains [\"profiles\"]})
         [[?e ?a ?v]]]`"
  ([db query]
   (idoc-match db query nil))
  ([db arg1 arg2]
   (idoc-match db arg1 arg2 nil))
  ([^DB db arg1 arg2 arg3]
   (execute-idoc-match-request
     (idoc-match-request
       db arg1 arg2 arg3 (extract-needed arg3 arg2 arg1)))))

(defn idoc-get
  "Function that extracts a value by path from a bound idoc document."
  [doc & path]
  (let [segments (if (and (clojure.core/= 1 (clojure.core/count path))
                          (vector? (first path)))
                   (first path)
                   (vec path))]
    (idoc/get-path doc segments)))

(defn- less
  ([_] true)
  ([x y]
   (clojure.core/neg? ^long (dd/compare-with-type x y)))
  ([x y & more]
   (if (less x y)
     (if (next more)
       (recur y (first more) (next more))
       (less y (first more)))
     false)))

(def <
  "Predicate similar to Clojure `<`"
  less)

(defn- greater
  ([_] true)
  ([x y] (clojure.core/pos? ^long (dd/compare-with-type x y)))
  ([x y & more]
   (if (greater x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater y (first more)))
     false)))

(def >
  "Predicate similar to Clojure `>`"
  greater)

(defn- less-equal
  ([_] true)
  ([x y]
   (clojure.core/not (clojure.core/pos? ^long (dd/compare-with-type x y))))
  ([x y & more]
   (if (less-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (less-equal y (first more)))
     false)))

(def <=
  "Predicate similar to Clojure `<=`"
  less-equal)

(defn- greater-equal
  ([_] true)
  ([x y]
   (clojure.core/not (clojure.core/neg? ^long (dd/compare-with-type x y))))
  ([x y & more]
   (if (greater-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater-equal y (first more)))
     false)))

(def >=
  "Predicate similar to Clojure `>=`"
  greater-equal)

(defn- smallest
  ([x] x)
  ([x y]
   (if (clojure.core/neg? ^long (dd/compare-with-type x y)) x y))
  ([x y & more]
   (reduce smallest (smallest x y) more)))

(def min
  "Function similar to Clojure `min`"
  smallest)

(defn- largest
  ([x] x)
  ([x y]
   (if (clojure.core/pos? ^long (dd/compare-with-type x y)) x y))
  ([x y & more]
   (reduce largest (largest x y) more)))

(def max
  "function similar to Clojure `max`"
  largest)

(def =
  "Predicate similar to Clojure `=`"
  clojure.core/=)

(def ==
  "Predicate similar to Clojure `==`"
  clojure.core/==)

(def not=
  "Predicate similar to Clojure `not=`"
  clojure.core/not=)

(def !=
  "Predicate similar to Clojure `not=`"
  clojure.core/not=)

(def +
  "Function similar to Clojure `+`"
  clojure.core/+)

(def -
  "Function similar to Clojure `-`"
  clojure.core/-)

(def *
  "Function similar to Clojure `*`"
  clojure.core/*)

(def /
  "Function similar to Clojure `/`"
  clojure.core//)

(def quot
  "Function similar to Clojure `quot`"
  clojure.core/quot)

(def rem
  "Function similar to Clojure `rem`"
  clojure.core/rem)

(def mod
  "Function similar to Clojure `mod`"
  clojure.core/mod)

(def inc
  "Function similar to Clojure `inc`"
  clojure.core/inc)

(def dec
  "Function similar to Clojure `dec`"
  clojure.core/dec)

(def zero?
  "Predicate similar to Clojure `zero?`"
  clojure.core/zero?)

(def pos?
  "Predicate similar to Clojure `pos?`"
  clojure.core/pos?)

(def neg?
  "Predicate similar to Clojure `neg?`"
  clojure.core/neg?)

(def even?
  "Predicate similar to Clojure `even?`"
  clojure.core/even?)

(def odd?
  "Predicate similar to Clojure `odd?`"
  clojure.core/odd?)

(def compare
  "Function similar to Clojure `compare`"
  clojure.core/compare)

(def rand
  "Function similar to Clojure `rand`"
  clojure.core/rand)

(def rand-int
  "Function similar to Clojure `rand-int`"
  clojure.core/rand-int)

(def true?
  "Predicate similar to Clojure `true?`"
  clojure.core/true?)

(def false?
  "Predicate similar to Clojure `false?`"
  clojure.core/false?)

(def nil?
  "Predicate similar to Clojure `nil?`"
  clojure.core/nil?)

(def some?
  "Predicate similar to Clojure `some?`"
  clojure.core/some?)

(def not
  "Predicate similar to Clojure `not`"
  clojure.core/not)

(def complement
  "Function similar to Clojure `complement`"
  clojure.core/complement)

(def identical?
  "Predicate similar to Clojure `identical?`"
  clojure.core/identical?)

(def identity
  "Function similar to Clojure `identity`"
  clojure.core/identity)

(def keyword
  "Function similar to Clojure `keyword`"
  clojure.core/keyword)

(def meta
  "Function similar to Clojure `meta`"
  clojure.core/meta)

(def name
  "Function similar to Clojure `name`"
  clojure.core/name)

(def namespace
  "Function similar to Clojure `namespace`"
  clojure.core/namespace)

(def type
  "Function similar to Clojure `type`"
  clojure.core/type)

(def vector
  "Function similar to Clojure `vector`"
  clojure.core/vector)

(def list
  "Function similar to Clojure `list`"
  clojure.core/list)

(def set
  "Function similar to Clojure `set`"
  clojure.core/set)

(def hash-map
  "Function similar to Clojure `hash-map`"
  clojure.core/hash-map)

(def array-map
  "Function similar to Clojure `array-map`"
  clojure.core/array-map)

(def count
  "Function similar to Clojure `count`"
  clojure.core/count)

(def range
  "Function similar to Clojure `range`"
  clojure.core/range)

(def not-empty
  "Function similar to Clojure `not-empty`"
  clojure.core/not-empty)

(def empty?
  "Function similar to Clojure `empty?`"
  clojure.core/empty?)

(def contains?
  "Function similar to Clojure `contains?`"
  clojure.core/contains?)

(def str
  "Function similar to Clojure `str`"
  clojure.core/str)

(def print-str
  "Function similar to Clojure `print-str`"
  clojure.core/print-str)

(def println-str
  "Function similar to Clojure `println-str`"
  clojure.core/println-str)

(def prn-str
  "Function similar to Clojure `prn-str`"
  clojure.core/prn-str)

(def subs
  "Function similar to Clojure `subs`"
  clojure.core/subs)

(def get
  "Function similar to Clojure `get`"
  clojure.core/get)

(def re-find
  "Function similar to Clojure `re-find`"
  clojure.core/re-find)

(def re-matches
  "Function similar to Clojure `re-matches`"
  clojure.core/re-matches)

(def re-seq
  "Function similar to Clojure `re-seq`"
  clojure.core/re-seq)

(def re-pattern
  "Function similar to Clojure `re-pattern`"
  clojure.core/re-pattern)

(def tuple
  "Function similar to Clojure `vector`"
  clojure.core/vector)

(def untuple
  "Function similar to Clojure `identity`"
  clojure.core/identity)

(def ^:dynamic *udf-db* nil)

(defn query-apply
  "Apply for use in queries. The first argument should be a function,
   which is resolved by the query engine from built-in functions or
   clojure.core before being passed here."
  [f & args]
  (apply apply f args))

(defn udf
  "Resolve and invoke a runtime UDF descriptor in query context."
  [descriptor & args]
  (when-not (instance? DB *udf-db*)
    (raise "Query UDF requires a database input"
           {:error :udf/query-context}))
  (let [registry    (db/udf-registry *udf-db*)
        descriptor  (or (db/installed-udf-descriptor
                          *udf-db* #{:query-fn :predicate} descriptor)
                        (udf-reg/descriptor-or-registered
                          registry #{:query-fn :predicate} descriptor))
        callable   (udf-reg/materialize
                     registry
                     {:db        *udf-db*
                      :kind      (:udf/kind descriptor)
                      :embedded? true
                      :store     (.-store ^DB *udf-db*)}
                     descriptor)]
    (apply callable args)))

(def query-fns
  {'=             =,
   '==            ==,
   'not=          not=,
   '!=            not=,
   '<             less
   '>             greater
   '<=            less-equal
   '>=            greater-equal
   '+             +,
   '-             -,
   '*             *,
   '/             /,
   'quot          quot,
   'rem           rem,
   'mod           mod,
   'inc           inc,
   'dec           dec,
   'max           largest,
   'min           smallest,
   'zero?         zero?,
   'pos?          pos?,
   'neg?          neg?,
   'even?         even?,
   'odd?          odd?,
   'compare       compare,
   'rand          rand,
   'rand-int      rand-int,
   'true?         true?,
   'false?        false?,
   'nil?          nil?,
   'some?         some?,
   'not           not,
   'and           and-fn,
   'or            or-fn,
   'complement    complement,
   'identical?    identical?,
   'identity      identity,
   'apply         query-apply,
   'udf           udf,
   'keyword       keyword,
   'meta          meta,
   'name          name,
   'namespace     namespace,
   'type          type,
   'vector        vector,
   'list          list,
   'set           set,
   'hash-map      hash-map,
   'array-map     array-map,
   'count         count,
   'range         range,
   'not-empty     not-empty,
   'empty?        empty?,
   'contains?     contains?,
   'str           str,
   'pr-str        pr-str,
   'print-str     print-str,
   'println-str   println-str,
   'prn-str       prn-str,
   'subs          subs,
   'get           get
   're-find       re-find,
   're-matches    re-matches,
   're-seq        re-seq,
   're-pattern    re-pattern,
   '-differ?      -differ?,
   'get-else      get-else,
   'get-some      get-some,
   'get-some-else get-some-else,
   'missing?      missing?,
   'ground        identity,
   'quote         identity,
   'q             q,
   'fulltext      fulltext,
   'embedding-neighbors embedding-neighbors,
   'idoc-match    idoc-match,
   'idoc-get      idoc-get,
   'vec-neighbors vec-neighbors,
   'tuple         vector,
   'untuple       identity
   'like          like
   'not-like      not-like
   'in            in
   'not-in        not-in})

;; Aggregates

(defn- aggregate-sum [coll] (reduce + 0 coll))

(def sum
  "Aggregation function that adds up collection"
  aggregate-sum)

(defn aggregate-avg ^double [coll]
  (/ ^double (aggregate-sum coll) (clojure.core/count coll)))

(def avg
  "Aggregation function that calculates the average."
  aggregate-avg)

(defn- aggregate-median [coll]
  (let [terms (sort coll)
        size  (clojure.core/count coll)
        med   (bit-shift-right size 1)]
    (cond-> ^double (nth terms med)
      (even? size)
      (-> (+ ^double (nth terms ^long (dec med)))
          (/ 2)))))

(def median
  "Aggregation function that calculates the median."
  aggregate-median)

(defn- aggregate-variance ^double [coll]
  (let [mean (aggregate-avg coll)
        sum  (aggregate-sum
               (for [x    coll
                     :let [delta (- ^double x ^double mean)]]
                 (* delta delta)))]
    (/ ^double sum (clojure.core/count coll))))

(def variance
  "Aggregation function that calculates the variance."
  aggregate-variance)

(defn- aggregate-stddev [coll] (Math/sqrt (aggregate-variance coll)))

(def stddev
  "Aggregation function that calculates the stddev."
  aggregate-stddev)

(defn- aggregate-min
  ([coll]
   (reduce
     (fn [acc x]
       (if (neg? (compare x acc))
         x acc))
     (first coll) (next coll)))
  ([n coll]
   (vec
     (reduce (fn [acc x]
               (cond
                 (< (clojure.core/count acc) ^long n)
                 (sort compare (conj acc x))
                 (neg? (compare x (last acc)))
                 (sort compare (conj (butlast acc) x))
                 :else acc))
             [] coll))))

(defn- aggregate-max
  ([coll]
   (reduce
     (fn [acc x]
       (if (pos? (compare x acc))
         x acc))
     (first coll) (next coll)))
  ([n coll]
   (vec
     (reduce (fn [acc x]
               (cond
                 (< (clojure.core/count acc) ^long n)
                 (sort compare (conj acc x))
                 (pos? (compare x (first acc)))
                 (sort compare (conj (next acc) x))
                 :else acc))
             [] coll))))

(defn- aggregate-rand
  ([coll] (rand-nth coll))
  ([n coll] (vec (repeatedly n #(rand-nth coll)))))

(defn- aggregate-sample [n coll]
  (vec (take n (shuffle coll))))

(defn sample
  "Aggregation function that randomly sample from a collection."
  [n coll]
  (aggregate-sample n coll))

(defn- aggregate-count-distinct [coll]
  (clojure.core/count (clojure.core/distinct coll)))

(defn distinct
  "Aggregation function that returns the distinctive values of a collection."
  [coll]
  (set coll))

(defn count-distinct
  "Aggregation function that count the distinctive values of a collection."
  [coll]
  (aggregate-count-distinct coll))

(def aggregates
  {'sum            aggregate-sum
   'avg            aggregate-avg
   'median         aggregate-median
   'variance       aggregate-variance
   'stddev         aggregate-stddev
   'distinct       set
   'vec            clojure.core/vec
   'min            aggregate-min
   'max            aggregate-max
   'rand           aggregate-rand
   'sample         aggregate-sample
   'count          count
   'count-distinct aggregate-count-distinct})
