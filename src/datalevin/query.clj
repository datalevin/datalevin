;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query
  "Datalog query engine"
  (:refer-clojure :exclude [update assoc])
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as w]
   [datalevin.db :as db]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as l]
   [datalevin.query-util :as qu
    :refer [IStep -execute -execute-pipe -explain]]
   [datalevin.relation :as r]
   [datalevin.join :as j]
   [datalevin.rules :as rules]
   [datalevin.storage]
   [datalevin.built-ins :as built-ins]
   [datalevin.util :as u :refer [cond+ raise concatv map+]]
   [datalevin.inline :refer [update assoc]]
   [datalevin.spill :as sp]
   [datalevin.pipe :as p]
   [datalevin.remote :as rt]
   [datalevin.parser :as dp]
   [datalevin.pull-api :as dpa]
   [datalevin.timeout :as timeout]
   [datalevin.constants :as c]
   [datalevin.bits :as b]
   [datalevin.interface
    :refer [dir db-name]])
  (:import
   [java.util List Collection Comparator HashSet HashMap]
   [java.util.concurrent ExecutorService Executors Future Callable]
   [datalevin.utl LRUCache]
   [datalevin.remote DatalogStore]
   [datalevin.db DB]
   [datalevin.relation Relation]
   [datalevin.storage Store]
   [datalevin.parser BindColl BindIgnore BindScalar BindTuple Constant
    FindColl FindRel FindScalar FindTuple Function PlainSymbol
    RulesVar SrcVar Variable Pattern]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *query-cache* (LRUCache. c/query-result-cache-size))

(def ^:dynamic *cache?* true)

(declare -collect -resolve-clause resolve-clause execute-steps
         hash-join-execute hash-join-execute-into sip-hash-join-execute)

;; Records

(defrecord Context [parsed-q rels sources rules opt-clauses late-clauses
                    optimizable-or-joins graph plan intermediates run?
                    result-set])

(defrecord Plan [steps cost size recency])

(defrecord InitStep
    [attr pred val range vars in out know-e? cols strata seen-or-joins mcount
     result sample]

  IStep
  (-type [_] :init)

  (-execute [_ db _]
    (let [get-v? (< 1 (count vars))
          e      (first vars)]
      (if result
        result
        (cond
          know-e?
          (let [src (doto (FastList.) (.add (object-array [e])))]
            (if get-v?
              (db/-eav-scan-v-list db src 0 [[attr {:skip? false}]])
              src))
          (nil? val)
          (db/-init-tuples-list
            db attr (or range [[[:closed c/v0] [:closed c/vmax]]]) pred get-v?)
          :else
          (db/-init-tuples-list
            db attr [[[:closed val] [:closed val]]] nil false)))))

  (-execute-pipe [_ db _ sink]
    (let [get-v? (< 1 (count vars))
          e      (first vars)]
      (if result
        (.addAll ^Collection sink result)
        (cond
          know-e?
          (let [pipe (if qu/*explain*
                       (p/counted-tuple-pipe)
                       (p/tuple-pipe))
                src  (doto ^Collection pipe
                       (.add (object-array [e]))
                       (p/finish))]
            (if get-v?
              (db/-eav-scan-v db src sink 0 [[attr {:skip? false}]])
              (p/drain-to src sink)))
          (nil? val)
          (db/-init-tuples
            db sink attr
            (or range [[[:closed c/v0] [:closed c/vmax]]]) pred get-v?)
          :else
          (db/-init-tuples
            db sink attr [[[:closed val] [:closed val]]] nil false)))))

  (-sample [_ db _]
    (let [get-v? (< 1 (count vars))]
      (cond
        val   (db/-sample-init-tuples-list
                db attr mcount [[[:closed val] [:closed val]]] nil false)
        range (db/-sample-init-tuples-list db attr mcount range pred get-v?)
        :else (cond-> (db/-e-sample db attr)
                get-v?
                (#(db/-eav-scan-v-list db % 0
                                       [[attr {:skip? false :pred pred}]]))
                (not get-v?)
                (#(db/-eav-scan-v-list db % 0
                                       [[attr {:skip? true :pred pred}]]))))))

  (-explain [_ _]
    (str "Initialize " vars " " (cond
                                  know-e? "by a known entity id."

                                  (nil? val)
                                  (if range
                                    (str "by range " range " on " attr ".")
                                    (str "by " attr "."))

                                  (some? val)
                                  (str "by " attr " = " val ".")))))

(defrecord MergeScanStep [index attrs-v vars in out cols strata seen-or-joins
                          result sample]

  IStep
  (-type [_] :merge)

  (-execute [_ db source]
    (if result
      result
      (db/-eav-scan-v-list db source index attrs-v)))

  (-execute-pipe [_ db source sink]
    (if result
      (do (when source
            (loop []
              (when (p/produce source)
                (recur))))
          (.addAll ^Collection sink result))
      (let [batch-size (long c/query-pipe-batch-size)]
        (if (zero? batch-size)
          (db/-eav-scan-v db source sink index attrs-v)
          (let [buffer (p/batch-buffer)]
            (loop []
              (if-let [tuple (p/produce source)]
                (do (.add buffer tuple)
                    (when (>= (.size buffer) batch-size)
                      (.addAll ^Collection sink
                               (db/-eav-scan-v-list db buffer index attrs-v))
                      (.clear buffer))
                    (recur))
                (when (pos? (.size buffer))
                  (.addAll ^Collection sink
                           (db/-eav-scan-v-list db buffer index attrs-v))))))))))

  (-sample [_ db tuples]
    (if (< 0 (.size ^List tuples))
      (db/-eav-scan-v-list db tuples index attrs-v)
      (FastList.)))

  (-explain [_ _]
    (if (seq vars)
      (str "Merge " (vec vars) " by scanning " (mapv first attrs-v) ".")
      (str "Filter by predicates on " (mapv first attrs-v) "."))))

(defrecord LinkStep [type index attr var fidx in out cols strata seen-or-joins]

  IStep
  (-type [_] :link)

  (-execute [_ db src]
    (cond
      (int? var) (db/-val-eq-scan-e-list db src index attr var)
      fidx       (db/-val-eq-filter-e-list db src index attr fidx)
      :else      (db/-val-eq-scan-e-list db src index attr)))

  (-execute-pipe [_ db src sink]
    (let [batch-size (long c/query-pipe-batch-size)]
      (if (zero? batch-size)
        (cond
          (int? var) (db/-val-eq-scan-e db src sink index attr var)
          fidx       (db/-val-eq-filter-e db src sink index attr fidx)
          :else      (db/-val-eq-scan-e db src sink index attr))
        (let [buffer (p/batch-buffer)]
          (loop []
            (if-let [tuple (p/produce src)]
              (do (.add buffer tuple)
                  (when (>= (.size buffer) batch-size)
                    (.addAll
                      ^Collection sink
                      (cond
                        (int? var)
                        (db/-val-eq-scan-e-list db buffer index attr var)
                        fidx
                        (db/-val-eq-filter-e-list db buffer index attr fidx)
                        :else
                        (db/-val-eq-scan-e-list db buffer index attr)))
                    (.clear buffer))
                  (recur))
              (when (pos? (.size buffer))
                (.addAll
                  ^Collection sink
                  (cond
                    (int? var)
                    (db/-val-eq-scan-e-list db buffer index attr var)
                    fidx
                    (db/-val-eq-filter-e-list db buffer index attr fidx)
                    :else
                    (db/-val-eq-scan-e-list db buffer index attr))))))))))

  (-explain [_ _]
    (str "Obtain " var " by "
         (if (identical? type :_ref) "reverse reference" "equal values")
         " of " attr ".")))

(declare sip-execute-pipe)

(defrecord HashJoinStep [link link-e in out in-cols cols strata seen-or-joins
                         tgt-steps in-size tgt-size]

  IStep
  (-type [_] :hash-join)

  (-execute [_ db src]
    (let [use-sip? (and (identical? (:type link) :_ref)
                        (> (long tgt-size) (* (long in-size)
                                              (long c/sip-ratio-threshold))))]
      (if use-sip?
        (sip-hash-join-execute db link link-e in-cols tgt-steps src)
        (hash-join-execute db in-cols tgt-steps src))))

  (-execute-pipe [_ db src sink]
    (let [use-sip? (and (identical? (:type link) :_ref)
                        (> (long tgt-size) (* (long in-size)
                                              (long c/sip-ratio-threshold))))]
      (if use-sip?
        (let [input (FastList.)]
          (when src
            (loop []
              (when-let [tuple (p/produce src)]
                (.add input tuple)
                (recur))))
          (when (pos? (.size input))
            (sip-execute-pipe db link link-e in-cols tgt-steps input sink)))
        (let [tgt-rel (execute-steps nil db tgt-steps)
              input   (FastList.)]
          (when src
            (loop []
              (when-let [tuple (p/produce src)]
                (.add input tuple)
                (recur))))
          (hash-join-execute-into in-cols tgt-rel input sink)))))

  (-explain [_ _]
    (let [use-sip? (and (identical? (:type link) :_ref)
                        (> (long tgt-size) (* (long in-size)
                                              (long c/sip-ratio-threshold))))]
      (str "Hash join to " (:tgt link) " by " (case (:type link)
                                                :_ref   "reverse reference"
                                                :val-eq "equal values"
                                                "link")
           (when use-sip? " with SIP") "."))))

(declare or-join-execute-link or-join-execute-link-into)

(defrecord OrJoinStep [clause bound-var bound-idx free-vars tgt tgt-attr
                       sources rules in out cols strata seen-or-joins]

  IStep
  (-type [_] :or-join)

  (-execute [_ db tuples]
    (or-join-execute-link db sources rules tuples clause bound-var
                          bound-idx free-vars tgt-attr))

  (-execute-pipe [_ db src sink]
    (let [input (FastList.)]
      (when src
        (loop []
          (when-let [tuple (p/produce src)]
            (.add input tuple)
            (recur))))
      (or-join-execute-link-into db sources rules input clause bound-var
                                 bound-idx free-vars tgt-attr sink)))

  (-explain [_ _]
    (str "Or-join from " bound-var " to " tgt " via " tgt-attr ".")))

(defrecord Node [links mpath mcount bound free])

(defrecord Link [type tgt var attrs attr])

(defrecord OrJoinLink [type tgt clause bound-var free-vars tgt-attr source])

(defrecord Clause [attr val var range count pred])

;; Require planner after record definitions to avoid circular dependency
;; (planner.clj imports these record classes)
(require '[datalevin.planner :as pl])

(defn solve-rule
  [context clause]
  (let [[rule-name & args] clause]
    (rules/solve-stratified context rule-name args resolve-clause)))

;; binding

(defn empty-rel
  ^Relation [binding]
  (let [vars (->> (dp/collect-vars-distinct binding)
                  (map :symbol))]
    (r/relation! (zipmap vars (range)) (FastList.))))

(defprotocol IBinding
  ^Relation (in->rel [binding value]))

(defn- bindtuple-attrs
  "Build attr -> index map for a tuple binding. Returns nil when binding
  contains unsupported elements or duplicate variables."
  [^BindTuple binding]
  (loop [i 0, bs (:bindings binding), attrs {}]
    (if (seq bs)
      (let [b (first bs)]
        (cond
          (instance? BindScalar b)
          (let [sym (get-in b [:variable :symbol])]
            (if (contains? attrs sym)
              nil
              (recur (inc i) (next bs) (assoc attrs sym i))))

          (instance? BindIgnore b)
          (recur (inc i) (next bs) attrs)

          :else nil))
      attrs)))

(defn- tuple-needed-indices
  "Returns an int array of indices that are needed (not BindIgnore) from a
   BindTuple. Returns nil if all indices are needed."
  [^BindTuple binding]
  (let [bs     (:bindings binding)
        n      (count bs)
        needed (int-array (keep-indexed
                            (fn [i b] (when-not (instance? BindIgnore b) i))
                            bs))]
    (when (< (alength needed) n)
      needed)))

(defn- compact-bindtuple-attrs
  "Build attr -> compact index map for a tuple binding when using needed indices.
   Maps each non-ignored variable to its position in the compact tuple."
  [^BindTuple binding]
  (loop [i 0, compact-i 0, bs (:bindings binding), attrs {}]
    (if (seq bs)
      (let [b (first bs)]
        (cond
          (instance? BindScalar b)
          (let [sym (get-in b [:variable :symbol])]
            (if (contains? attrs sym)
              nil
              (recur (inc i) (inc compact-i) (next bs) (assoc attrs sym compact-i))))

          (instance? BindIgnore b)
          (recur (inc i) compact-i (next bs) attrs)

          :else nil))
      attrs)))

(def ^:private tuple-producing-fns
  "Set of function symbols that produce tuples and can benefit from
   knowing which indices are needed."
  #{'fulltext 'idoc-match 'vec-neighbors})

(extend-protocol IBinding
  BindIgnore
  (in->rel [_ _]
    (r/prod-rel))

  BindScalar
  (in->rel [binding value]
    (r/relation! {(get-in binding [:variable :symbol]) 0}
                 (doto (FastList.) (.add (into-array Object [value])))))

  BindColl
  (in->rel [binding coll]
    (cond
      (instance? Relation coll) coll

      (not (u/seqable? coll))
      (raise "Cannot bind value " coll " to collection " (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})

      (empty? coll)
      (empty-rel binding)

      (and (instance? java.util.List coll)
           (instance? BindTuple (:binding binding)))
      (if-let [attrs (bindtuple-attrs (:binding binding))]
        (let [^List tuples coll
              size        (.size tuples)]
          (if (zero? size)
            (empty-rel binding)
            (let [t0 (.get tuples 0)]
              (if (u/array? t0)
                (let [^objects t0 t0
                      needed     (count (:bindings (:binding binding)))]
                  (when (< (alength t0) needed)
                    (raise "Not enough elements in a collection " coll
                           " to bind tuple " (dp/source binding)
                           {:error   :query/binding
                            :value   coll
                            :binding (dp/source binding)}))
                  (r/relation! attrs tuples))
                (transduce (map #(in->rel (:binding binding) %))
                           r/sum-rel coll)))))
        (transduce (map #(in->rel (:binding binding) %)) r/sum-rel coll))

      :else
      (transduce (map #(in->rel (:binding binding) %)) r/sum-rel coll)))

  BindTuple
  (in->rel [binding coll]
    (cond
      (not (u/seqable? coll))
      (raise "Cannot bind value " coll " to tuple " (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})

      (< (count coll) (count (:bindings binding)))
      (raise "Not enough elements in a collection " coll " to bind tuple "
             (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})

      :else
      (reduce r/prod-rel (map #(in->rel %1 %2) (:bindings binding) coll)))))

(defn resolve-ins
  [context values]
  (reduce
    (fn resolve-in [context [binding value]]
      (cond
        (and (instance? BindScalar binding)
             (instance? SrcVar (:variable binding)))
        (update context :sources assoc (get-in binding [:variable :symbol]) value)

        (and (instance? BindScalar binding)
             (instance? RulesVar (:variable binding)))
        (let [parsed (rules/parse-rules value)]
          (assoc context
                 :rules parsed
                 :rules-deps (rules/dependency-graph parsed)))

        :else (update context :rels conj (in->rel binding value))))
    context (zipmap (get-in context [:parsed-q :qin]) values)))

(defn- rel-with-attr [context sym]
  (some #(when ((:attrs %) sym) %) (:rels context)))

(defn substitute-constant [context pattern-el]
  (when (qu/binding-var? pattern-el)
    (when-some [rel (rel-with-attr context pattern-el)]
      (let [tuples (:tuples rel)]
        (when-some [tuple (first tuples)]
          (when (nil? (fnext tuples))
            (let [idx ((:attrs rel) pattern-el)]
              (if (u/array? tuple)
                (aget ^objects tuple idx)
                (get tuple idx)))))))))

(defn substitute-constants [context pattern]
  (mapv #(or (substitute-constant context %) %) pattern))

(defn- compute-rels-bound-values
  "Compute bound values for a variable from context relations."
  [context var]
  (when-some [rel (rel-with-attr context var)]
    (let [^List tuples (:tuples rel)
          n            (.size tuples)]
      (when (> n 1)
        (let [idx ((:attrs rel) var)
              res (HashSet.)]
          (dotimes [i n]
            (.add res (aget ^objects (.get tuples i) idx)))
          res)))))

(defn- bound-values
  "Extract unique values for a variable from context relations.
   Returns nil if not bound, or a set of values if bound to multiple values.
   Uses :rels-bound-cache volatile for lazy caching within a clause resolution."
  [context var]
  (when (qu/binding-var? var)
    (if-some [cache (:rels-bound-cache context)]
      ;; Use cached value or compute and cache
      (let [cached @cache]
        (if (contains? cached var)
          (get cached var)
          (let [result (or (compute-rels-bound-values context var)
                           (get (:delta-bound-values context) var))]
            (vswap! cache assoc var result)
            result)))
      ;; No cache, compute directly (fallback for non-clause contexts)
      (or (compute-rels-bound-values context var)
          (get (:delta-bound-values context) var)))))

(defn resolve-pattern-lookup-refs [source pattern]
  (if (db/-searchable? source)
    (let [[e a v] pattern
          e'      (if (or (qu/lookup-ref? e) (keyword? e))
                    (db/entid-strict source e)
                    e)
          v'      (if (and v
                           (keyword? a)
                           (db/ref? source a)
                           (or (qu/lookup-ref? v) (keyword? v)))
                    (db/entid-strict source v)
                    v)]
      (subvec [e' a v'] 0 (count pattern)))
    pattern))

(defn- resolve-entity-pairs
  [db entity-values]
  (keep (fn [e]
          (cond
            (integer? e)
            (when-not (neg? (long e))
              [e e])

            (or (qu/lookup-ref? e) (keyword? e))
            (when-let [eid (db/entid db e)]
              [e eid])

            :else
            nil))
        entity-values))

(def ^:const ^:private ^long multi-lookup-threshold 100000)

(defn- lookup-pattern-multi-entity
  "Perform multiple point lookups for bound entity values.
   More efficient than full table scan when entity is bound to multiple values.
   Returns tuples in format matching what full scan would return."
  [db pattern entity-pairs v-is-var?]
  (let [[_ a v] pattern
        a'      (if (keyword? a) a nil)
        v'      (if (or (qu/free-var? v) (= v '_)) nil v)
        acc     (FastList.)]
    (doseq [[e eid] entity-pairs]
      (let [tuples (db/-search-tuples db [eid a' v'])]
        (when tuples
          (let [^List ts tuples
                n        (.size ts)]
            (if v-is-var?
              (dotimes [i n]
                (let [^objects t (.get ts i)
                      result     (object-array 2)]
                  (aset result 0 e)
                  (aset result 1 (aget t 0))
                  (.add acc result)))
              (when (pos? n)
                (.add acc (object-array [e]))))))))
    acc))

(defn- lookup-pattern-multi-value
  "Perform multiple AV lookups for bound value variable.
   More efficient than full table scan when value is bound to multiple values.
   Returns tuples in format [e v] or [e] depending on pattern."
  [db pattern value-set e-is-var?]
  (let [[_ a _]   pattern
        ref-attr? (and (keyword? a) (db/ref? db a))
        acc       (FastList.)]
    (doseq [v value-set]
      (when-some [v' (if (and ref-attr?
                              (or (qu/lookup-ref? v) (keyword? v)))
                       (db/entid db v)
                       v)]
        (let [tuples (db/-search-tuples db [nil a v'])]
          (when tuples
            (let [^List ts tuples
                  n        (.size ts)]
              (if e-is-var?
                (dotimes [i n]
                  (let [^objects t (.get ts i)
                        result     (object-array 2)]
                    (aset result 0 (aget t 0))  ;; entity from av-tuples
                    (aset result 1 v)
                    (.add acc result)))
                (dotimes [_ n]
                  (.add acc (object-array [v])))))))))
    acc))

(defn lookup-pattern-db
  [context db pattern]
  (let [[e a v]           pattern
        ;; Check if entity is bound to multiple values
        entity-values     (when (and (qu/binding-var? e) (keyword? a))
                            (bound-values context e))
        use-entity-multi? (and entity-values
                               (<= (.size ^HashSet entity-values)
                                   multi-lookup-threshold))
        ;; Check if value is bound to multiple values (and entity is NOT bound)
        value-values      (when (and (not use-entity-multi?)
                                     (qu/binding-var? e)
                                     (qu/binding-var? v)
                                     (keyword? a))
                            (bound-values context v))
        use-value-multi?  (and value-values
                               (<= (.size ^HashSet value-values)
                                   multi-lookup-threshold))]
    (cond
      use-entity-multi?
      (let [resolved-pattern (resolve-pattern-lookup-refs db pattern)
            entity-pairs     (resolve-entity-pairs db entity-values)
            v-resolved       (nth resolved-pattern 2 nil)
            v-is-var?        (and (or (nil? v-resolved)
                                      (qu/free-var? v-resolved)
                                      (= v-resolved '_))
                                  (not (qu/placeholder? v-resolved)))
            attrs            (if (and (qu/binding-var? v) (not= v e))
                               {e 0, v 1}
                               {e 0})]
        (r/relation! attrs
                     (lookup-pattern-multi-entity db resolved-pattern
                                                  entity-pairs v-is-var?)))

      use-value-multi?
      (let [e-is-var? (qu/binding-var? e)
            attrs     (if e-is-var?
                        {e 0, v 1}
                        {v 0})]
        (r/relation! attrs
                     (lookup-pattern-multi-value db pattern value-values e-is-var?)))

      :else
      (let [search-pattern (->> pattern
                                (substitute-constants context)
                                (resolve-pattern-lookup-refs db)
                                (mapv #(if (or (qu/free-var? %) (= % '_)) nil %)))]
        (r/relation! (let [idxs (volatile! {})
                           i    (volatile! 0)]
                       (mapv (fn [p sp]
                               (when (nil? sp)
                                 (when (qu/binding-var? p)
                                   (vswap! idxs assoc p @i))
                                 (vswap! i u/long-inc)))
                             pattern search-pattern)
                       @idxs)
                     (db/-search-tuples db search-pattern))))))

(defn matches-pattern?
  [pattern tuple]
  (loop [tuple   tuple
         pattern pattern]
    (if (and tuple pattern)
      (let [t (first tuple)
            p (first pattern)]
        (if (or (= p '_) (qu/free-var? p) (= t p))
          (recur (next tuple) (next pattern))
          false))
      true)))

(defn lookup-pattern-coll
  [coll pattern]
  (r/relation! (into {}
                     (filter (fn [[s _]] (qu/binding-var? s)))
                     (map vector pattern (range)))
               (u/map-fl to-array
                         (filterv #(matches-pattern? pattern %) coll))))

(defn lookup-pattern
  [context source pattern]
  (if (db/-searchable? source)
    (lookup-pattern-db context source pattern)
    (lookup-pattern-coll source pattern)))

(defn collapse-rels
  [rels new-rel]
  (persistent!
    (loop [rels          rels
           new-rel       new-rel
           new-rel-attrs (:attrs new-rel)
           acc           (transient [])]
      (if-some [rel (first rels)]
        (if (not-empty (qu/intersect-keys new-rel-attrs (:attrs rel)))
          (let [joined (j/hash-join rel new-rel)]
            (recur (next rels) joined (:attrs joined) acc))
          (recur (next rels) new-rel new-rel-attrs (conj! acc rel)))
        (conj! acc new-rel)))))

(defn- context-resolve-val
  [context sym]
  (when-some [rel (rel-with-attr context sym)]
    (when-some [^objects tuple (.get ^List (:tuples rel) 0)]
      (aget tuple ((:attrs rel) sym)))))

(defn- rel-contains-attrs?
  [rel attrs]
  (let [rel-attrs (:attrs rel)]
    (some #(rel-attrs %) attrs)))

(defn- rel-prod-by-attrs
  [context attrs]
  (let [rels       (into #{}
                         (filter #(rel-contains-attrs? % attrs))
                         (:rels context))
        production (reduce r/prod-rel rels)]
    [(update context :rels #(remove rels %)) production]))

(defn- dot-form [f] (when (and (symbol? f) (str/starts-with? (name f) ".")) f))

(defn- dot-call
  [fname ^objects args]
  (let [obj (aget args 0)
        oc  (.getClass ^Object obj)
        as  (rest args)
        res (if (zero? (count as))
              (. (.getDeclaredMethod oc fname nil) (invoke obj nil))
              (. (.getDeclaredMethod
                   oc fname
                   (into-array Class (map #(.getClass ^Object %) as)))
                 (invoke obj (into-array Object as))))]
    (when (not= res false) res)))

(defn- opt-apply
  [f args]
  (if (u/array? args)
    (let [args ^objects args
          len  (alength args)]
      (case len
        0 (f)
        1 (f (aget args 0))
        2 (f (aget args 0) (aget args 1))
        3 (f (aget args 0) (aget args 1) (aget args 2))
        4 (f (aget args 0) (aget args 1) (aget args 2) (aget args 3))
        5 (f (aget args 0) (aget args 1) (aget args 2) (aget args 3)
             (aget args 4))
        6 (f (aget args 0) (aget args 1) (aget args 2) (aget args 3)
             (aget args 4) (aget args 5))
        7 (f (aget args 0) (aget args 1) (aget args 2) (aget args 3)
             (aget args 4) (aget args 5) (aget args 6))
        (apply f args)))
    (apply f args)))

(defn- make-call
  [f]
  (if (dot-form f)
    (let [fname (subs (name f) 1)] #(dot-call fname %))
    #(opt-apply f %)))

(defn- resolve-sym
  [sym]
  (when (symbol? sym)
    (when-let [v (or (resolve sym)
                     (when (find-ns 'pod.huahaiy.datalevin)
                       (ns-resolve 'pod.huahaiy.datalevin sym)))]
      @v)))

(defonce pod-fns (atom {}))

(defn- resolve-pred
  [f context]
  (let [fun (if (fn? f)
              f
              (or (get built-ins/query-fns f)
                  (context-resolve-val context f)
                  (dot-form f)
                  (resolve-sym f)
                  (when (nil? (rel-with-attr context f))
                    (raise "Unknown function or predicate '" f
                           {:error :query/where :var f}))))]
    (if-let [s (:pod.huahaiy.datalevin/inter-fn fun)]
      (@pod-fns s)
      fun)))

(defn -call-fn
  [context rel f args]
  (let [sources              (:sources context)
        attrs                (:attrs rel)
        len                  (count args)
        ^objects static-args (make-array Object len)
        ^objects tuples-args (make-array Object len)
        call                 (make-call (resolve-pred f context))]
    (dotimes [i len]
      (let [arg (nth args i)]
        (cond
          (symbol? arg) (if-some [source (get sources arg)]
                          (aset static-args i source)
                          (if-some [fn-val (or (get built-ins/query-fns arg)
                                               (resolve-sym arg))]
                            (aset static-args i fn-val)
                            (aset tuples-args i (get attrs arg))))
          (list? arg)   (aset tuples-args i
                              (-call-fn context rel (first arg) (rest arg)))
          :else         (aset static-args i arg))))
    (fn [^objects tuple]
      (dotimes [i len]
        (when-some [tuple-arg (aget tuples-args i)]
          (aset static-args i (if (fn? tuple-arg)
                                (tuple-arg tuple)
                                (aget tuple tuple-arg)))))
      (call static-args))))

(defn filter-by-pred
  [context clause]
  (let [[[f & args]]         clause
        attrs                (qu/collect-vars args)
        [context production] (rel-prod-by-attrs context attrs)

        new-rel (let [tuple-pred (-call-fn context production f args)]
                  (update production :tuples #(r/select-tuples tuple-pred %)))]
    (update context :rels conj new-rel)))

(defn- attach-needed-meta
  "Attach :tuple-needed metadata to the last argument or append a metadata map.
   Returns the modified args vector."
  [args ^ints needed]
  (let [v        (vec args)
        n        (count v)
        last-arg (when (pos? n) (peek v))
        meta-map (with-meta {} {:tuple-needed needed})]
    (cond
      (zero? n)
      [meta-map]

      ;; nil can't hold metadata, but we can replace it with a map
      (nil? last-arg)
      (assoc v (dec n) meta-map)

      ;; If last arg can hold metadata, attach it there
      (instance? clojure.lang.IObj last-arg)
      (assoc v (dec n) (with-meta last-arg {:tuple-needed needed}))

      ;; Otherwise append a new map with the metadata
      :else
      (conj v meta-map))))

(defn bind-by-fn
  [context clause]
  (let [[[f & args] out]     clause
        binding              (dp/parse-binding out)
        ;; Check if this is a tuple-producing function with ignored bindings
        tuple-bind?          (and (instance? BindColl binding)
                                  (instance? BindTuple (:binding binding)))
        needed               (when (and tuple-bind?
                                        (contains? tuple-producing-fns f))
                               (tuple-needed-indices (:binding binding)))
        args'                (if needed
                               (attach-needed-meta args needed)
                               args)
        attrs                (qu/collect-vars args)
        [context production] (rel-prod-by-attrs context attrs)
        ;; Check if scalar output variable is already bound in production
        out-var              (when (instance? BindScalar binding)
                               (get-in binding [:variable :symbol]))
        out-idx              (when out-var (get (:attrs production) out-var))
        new-rel
        (if out-idx
          ;; Output variable already bound - filter tuples where values match
          (let [tuple-fn (-call-fn context production f args')]
            (clojure.core/update
              production :tuples
              #(r/select-tuples
                 (fn [^objects tuple]
                   (let [val (tuple-fn tuple)]
                     (and (not (nil? val))
                          (= (aget tuple (int out-idx)) val))))
                 %)))
          ;; Output variable not bound - create new binding
          (let [tuple-fn (-call-fn context production f args')
                rels     (for [tuple (:tuples production)
                               :let  [val (tuple-fn tuple)]
                               :when (not (nil? val))]
                           (if needed
                             ;; Compact tuples - use compact attrs
                             (r/prod-rel
                               (r/relation! (:attrs production)
                                            (doto (FastList.) (.add tuple)))
                               (r/relation!
                                 (compact-bindtuple-attrs (:binding binding))
                                 val))
                             ;; Regular path
                             (r/prod-rel
                               (r/relation! (:attrs production)
                                            (doto (FastList.) (.add tuple)))
                               (in->rel binding val))))]
            (if (empty? rels)
              (r/prod-rel production (empty-rel binding))
              (reduce r/sum-rel rels))))]
    (update context :rels collapse-rels new-rel)))

(defn dynamic-lookup-attrs
  [source pattern]
  (let [[e a v] pattern]
    (cond-> #{}
      (qu/binding-var? e)   (conj e)
      (and
        (qu/binding-var? v)
        (not (qu/binding-var? a))
        (db/ref? source a)) (conj v))))

(defn limit-rel
  [rel vars]
  (when-some [attrs (not-empty (select-keys (:attrs rel) vars))]
    (assoc rel :attrs attrs)))

(defn limit-context
  [context vars]
  (assoc context :rels (keep #(limit-rel % vars) (:rels context))))

(defn bound-vars
  [context]
  (into #{} (mapcat #(keys (:attrs %))) (:rels context)))

(defn check-bound
  [bound vars form]
  (when-not (set/subset? vars bound)
    (let [missing (set/difference vars bound)]
      (raise "Insufficient bindings: " missing " not bound in " form
             {:error :query/where :form form :vars missing}))))

(defn check-free-same
  [bound branches form]
  (let [free (mapv #(set/difference (qu/collect-vars %) bound) branches)]
    (when-not (apply = free)
      (raise "All clauses in 'or' must use same set of free vars, had " free
             " in " form
             {:error :query/where :form form :vars free}))))

(defn check-free-subset
  [bound vars branches]
  (let [free (into #{} (remove bound) vars)]
    (doseq [branch branches]
      (when-some [missing (not-empty
                            (set/difference free (qu/collect-vars branch)))]
        (raise "All clauses in 'or' must use same set of free vars, had "
               missing " not bound in " branch
               {:error :query/where :form branch :vars missing})))))

(defn single
  [coll]
  (assert (nil? (next coll)) "Expected single element")
  (first coll))

(defn looks-like?
  [pattern form]
  (cond
    (= '_ pattern)    true
    (= '[*] pattern)  (sequential? form)
    (symbol? pattern) (= form pattern)

    (sequential? pattern)
    (if (= (last pattern) '*)
      (and (sequential? form)
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (mapv vector (butlast pattern) form)))
      (and (sequential? form)
           (= (count form) (count pattern))
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (mapv vector pattern form))))
    :else ;; (predicate? pattern)
    (pattern form)))

(defn- clause-vars [clause] (into #{} (filter qu/binding-var?) (nfirst clause)))

(defn -resolve-clause
  ([context clause]
   (-resolve-clause context clause clause))
  ([context clause orig-clause]
   (condp looks-like? clause
     [[symbol? '*]] ;; predicate [(pred ?a ?b ?c)]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (filter-by-pred context clause))

     [[fn? '*]] ;; predicate [(pred ?a ?b ?c)]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (filter-by-pred context clause))

     [[symbol? '*] '_] ;; function [(fn ?a ?b) ?res]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (bind-by-fn context clause))

     [[fn? '*] '_] ;; function [(fn ?a ?b) ?res]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (bind-by-fn context clause))

     [qu/source? '*] ;; source + anything
     (let [[source-sym & rest] clause]
       (binding [qu/*implicit-source* (get (:sources context) source-sym)]
         (-resolve-clause context rest clause)))

     '[or *] ;; (or ...)
     (let [[_ & branches] clause
           _              (check-free-same (bound-vars context) branches clause)
           contexts       (map #(resolve-clause context %) branches)]
       (assoc (first contexts) :rels [(transduce
                                        (map #(reduce j/hash-join (:rels %)))
                                        r/sum-rel-dedupe
                                        contexts)]))

     '[or-join [[*] *] *] ;; (or-join [[req-vars] vars] ...)
     (let [[_ [req-vars & vars] & branches] clause
           req-vars                         (into #{} (filter qu/binding-var?) req-vars)
           bound                            (bound-vars context)]
       (check-bound bound req-vars orig-clause)
       (check-free-subset bound vars branches)
       (recur context (list* 'or-join (concatv req-vars vars) branches) clause))

     '[or-join [*] *] ;; (or-join [vars] ...)
     (let [[_ vars & branches] clause
           vars                (into #{} (filter qu/binding-var?) vars)
           _                   (check-free-subset (bound-vars context) vars
                                                  branches)
           join-context        (limit-context context vars)]
       (update context :rels collapse-rels
               (transduce (comp (map (fn [branch]
                                    (-> join-context
                                        (resolve-clause branch)
                                        (limit-context vars))))
                             (map #(let [rels (:rels %)]
                                     (if (seq rels)
                                       (reduce j/hash-join rels)
                                       []))))
                          r/sum-rel-dedupe branches)))

     '[and *] ;; (and ...)
     (let [[_ & clauses] clause]
       (reduce resolve-clause context clauses))

     '[not *] ;; (not ...)
     (let [[_ & clauses] clause
           bound         (bound-vars context)
           negation-vars (qu/collect-vars clauses)
           _             (when (empty? (u/intersection bound negation-vars))
                           (raise "Insufficient bindings: none of "
                                  negation-vars " is bound in " orig-clause
                                  {:error :query/where :form orig-clause}))
           context1      (assoc context :rels
                                [(reduce j/hash-join (:rels context))])]
       (assoc context1 :rels
              [(j/subtract-rel
                 (single (:rels context1))
                 (reduce j/hash-join
                         (:rels (reduce resolve-clause context1 clauses))))]))

     '[not-join [*] *] ;; (not-join [vars] ...)
     (let [[_ vars & clauses] clause
           vars               (into #{} (filter qu/binding-var?) vars)
           bound              (bound-vars context)
           _                  (check-bound bound vars orig-clause)
           context1           (assoc context :rels
                                     [(reduce j/hash-join (:rels context))])
           join-context       (limit-context context1 vars)
           negation-context   (-> (reduce resolve-clause join-context clauses)
                                  (limit-context vars))
           neg-rel            (reduce j/hash-join (:rels negation-context))]
       (assoc context1 :rels
              [(j/subtract-rel
                 (single (:rels context1))
                 neg-rel)]))

     '[*] ;; pattern
     (let [source   qu/*implicit-source*
           pattern' (resolve-pattern-lookup-refs source clause)
           relation (lookup-pattern context source pattern')]
       (binding [qu/*lookup-attrs* (if (db/-searchable? source)
                                     (dynamic-lookup-attrs source pattern')
                                     qu/*lookup-attrs*)]
         (update context :rels collapse-rels relation))))))

(defn resolve-clause
  [context clause]
  (let [context (assoc context :rels-bound-cache (volatile! {}))]
    (if (some r/rel-empty (:rels context))
      (assoc context :rels
             [(r/relation!
                (zipmap (mapcat #(keys (:attrs %)) (:rels context)) (range))
                (FastList.))])
      (if (qu/rule? context clause)
        (if (qu/source? (first clause))
          (binding [qu/*implicit-source* (get (:sources context) (first clause))]
            (resolve-clause context (next clause)))
          (update context :rels collapse-rels (solve-rule context clause)))
        (-resolve-clause context clause)))))

(defn- or-join-build
  [sources rules ^List tuples clause bound-var bound-idx free-vars]
  (when (pos? (.size tuples))
    (let [bound-vals     (let [s (HashSet.)]
                           (dotimes [i (.size tuples)]
                             (.add s (aget ^objects (.get tuples i) bound-idx)))
                           (vec s))
          bound-rel      (r/relation! {bound-var 0}
                                      (let [fl (FastList.)]
                                        (doseq [v bound-vals]
                                          (.add fl (object-array [v])))
                                        fl))
          or-context     {:sources sources
                          :rules   rules
                          :rels    [bound-rel]}
          result-context (binding [qu/*implicit-source* (get sources '$)]
                           (resolve-clause or-context clause))
          result-rels    (:rels result-context)]
      (when (seq result-rels)
        (let [or-result-rel       (if (< 1 (count result-rels))
                                    (reduce j/hash-join result-rels)
                                    (first result-rels))
              or-attrs            (:attrs or-result-rel)
              or-tuples           ^List (:tuples or-result-rel)
              free-var            (first free-vars)
              free-var-idx        (or-attrs free-var)
              bound-var-idx-in-or (or-attrs bound-var)
              or-by-bound
              (let [m (HashMap.)]
                (dotimes [i (.size or-tuples)]
                  (let [^objects t (.get or-tuples i)
                        bv         (aget t bound-var-idx-in-or)]
                    (.putIfAbsent m bv (FastList.))
                    (.add ^List (.get m bv) t)))
                m)]
          {:or-by-bound  or-by-bound
           :free-var-idx free-var-idx
           :tuple-len    (alength ^objects (.get tuples 0))})))))

(defn- or-join-execute-link
  [db sources rules ^List tuples clause bound-var bound-idx free-vars tgt-attr]
  (if-let [{:keys [or-by-bound free-var-idx tuple-len]}
           (or-join-build sources rules tuples clause bound-var bound-idx
                          free-vars)]
    (let [size   (.size tuples)
          joined (FastList. size)]
      (dotimes [i size]
        (let [^objects in-tuple (.get tuples i)
              bv                (aget in-tuple bound-idx)]
          (when-let [^List or-matches (.get ^HashMap or-by-bound bv)]
            (dotimes [j (.size or-matches)]
              (let [^objects or-tuple (.get or-matches j)
                    fv                (aget or-tuple free-var-idx)
                    joined-tuple      (object-array (inc ^long tuple-len))]
                (System/arraycopy in-tuple 0 joined-tuple 0 tuple-len)
                (aset joined-tuple tuple-len fv)
                (.add joined joined-tuple))))))
      (if (zero? (.size joined))
        (FastList.)
        (db/-val-eq-scan-e-list db joined tuple-len tgt-attr)))
    (FastList.)))

(defn- or-join-execute-link-into
  [db sources rules ^List tuples clause bound-var bound-idx free-vars tgt-attr
   sink]
  (when-let [{:keys [or-by-bound free-var-idx tuple-len]}
             (or-join-build sources rules tuples clause bound-var bound-idx
                            free-vars)]
    (when-not (.isEmpty ^HashMap or-by-bound)
      (let [pipe (p/or-join-tuple-pipe tuples bound-idx or-by-bound free-var-idx
                                       tuple-len)]
        (db/-val-eq-scan-e db pipe sink tuple-len tgt-attr))))
  sink)

(defn- save-intermediates
  [context steps ^objects sinks ^List tuples]
  (when-let [res (:intermediates context)]
    (vswap! res merge
            (u/reduce-indexed
              (fn [m step i]
                (assoc m (:out step)
                       {:tuples-count
                        (let [sink (aget sinks i)]
                          (if (p/pipe? sink)
                            (p/total sink)
                            (.size ^Collection (p/remove-end-scan sink))))}))
              {(:out (peek steps)) {:tuples-count (.size tuples)}}
              (butlast steps)))))

(defn- cols->attrs
  [cols]
  (u/reduce-indexed
    (fn [m col i]
      (let [v (if (set? col)
                (some #(when (symbol? %) %) col)
                col)]
        (assoc m v i)))
    {} cols))

(defn- hash-join-execute
  [db in-cols tgt-steps ^List tuples]
  (let [out (FastList. (.size tuples))]
    (hash-join-execute-into db in-cols tgt-steps tuples out)
    out))

(defn- hash-join-execute-into
  ([db in-cols tgt-steps tuples sink]
   (when (and tuples (pos? (.size ^List tuples)))
     (let [tgt-rel (execute-steps nil db tgt-steps)]
       (hash-join-execute-into in-cols tgt-rel tuples sink))))
  ([in-cols tgt-rel tuples sink]
   (when (and tuples (pos? (.size ^List tuples)))
     (let [in-rel (r/relation! (cols->attrs in-cols) tuples)]
       (j/hash-join-into in-rel tgt-rel sink)))))

(defn- build-sip-bitmap
  "Build a 64-bit bitmap from the values at col-idx in input tuples"
  [^List input ^long col-idx]
  (let [bm (b/bitmap64)]
    (dotimes [i (.size input)]
      (let [tuple ^objects (.get input i)
            v     (aget tuple col-idx)]
        (when (integer? v)
          (b/bitmap64-add bm (long v)))))
    bm))

(defn- values->ranges
  "Convert a collection of values to single-value ranges"
  [values]
  (mapv (fn [v] [[:closed v] [:closed v]]) values))

(defn- compose-pred
  "Compose a new predicate with an existing one"
  [existing-pred new-pred]
  (if existing-pred
    (fn [v] (and (existing-pred v) (new-pred v)))
    new-pred))

(defn- find-attr-in-attrs-v
  "Find the index of attr in attrs-v and return [index opts]"
  [attrs-v attr]
  (reduce-kv
    (fn [_ i [a opts]]
      (when (= a attr)
        (reduced [i opts])))
    nil attrs-v))

(defn- modify-init-step-for-sip
  "Modify InitStep with SIP optimization - either ranges or bitmap pred"
  [init-step bm]
  (let [cardinality (b/bitmap64-cardinality bm)]
    (if (<= cardinality ^long c/sip-range-threshold)
      ;; Small cardinality: convert to individual ranges
      (let [values     (b/bitmap64->longs bm)
            new-ranges (values->ranges values)
            old-range  (:range init-step)]
        (assoc init-step :range (if old-range
                                  (pl/intersect-ranges old-range new-ranges)
                                  new-ranges)))
      ;; Large cardinality: use min/max range + bitmap predicate
      (let [min-v     (b/bitmap64-min bm)
            max-v     (b/bitmap64-max bm)
            new-range [[[:closed min-v] [:closed max-v]]]
            old-range (:range init-step)
            bm-pred   (fn [v] (b/bitmap64-contains? bm v))
            old-pred  (:pred init-step)]
        (assoc init-step
               :range (if old-range
                        (pl/intersect-ranges old-range new-range)
                        new-range)
               :pred (compose-pred old-pred bm-pred))))))

(defn- modify-merge-scan-step-for-sip
  "Modify MergeScanStep attrs-v to add bitmap predicate for join attr"
  [merge-step bm join-attr]
  (let [attrs-v (:attrs-v merge-step)
        bm-pred (fn [v] (b/bitmap64-contains? bm v))
        new-attrs-v
        (mapv (fn [[a opts :as entry]]
                (if (= a join-attr)
                  [a (update opts :pred #(compose-pred % bm-pred))]
                  entry))
              attrs-v)]
    (assoc merge-step :attrs-v new-attrs-v)))

(defn- apply-sip-to-tgt-steps
  "Apply SIP optimization to target steps for :_ref link type"
  [tgt-steps bm join-attr]
  (let [init-step (first tgt-steps)
        init-attr (:attr init-step)]
    (if (= init-attr join-attr)
      (assoc (vec tgt-steps) 0
             (modify-init-step-for-sip init-step bm))
      (if (< 1 (count tgt-steps))
        (let [merge-step (second tgt-steps)
              attrs-v    (:attrs-v merge-step)]
          (if (find-attr-in-attrs-v attrs-v join-attr)
            (assoc (vec tgt-steps) 1
                   (modify-merge-scan-step-for-sip merge-step bm join-attr))
            tgt-steps))
        tgt-steps))))

(defn- sip-execute-pipe
  "Execute hash join with SIP (Sideways Information Passing) optimization.
   Called when SIP is determined to be beneficial."
  [db link link-e in-cols tgt-steps ^FastList input sink]
  (let [join-attr   (:attr link)
        col-idx     (pl/find-index link-e in-cols)
        bm          (build-sip-bitmap input col-idx)
        cardinality (b/bitmap64-cardinality bm)]
    (when (pos? cardinality)
      (let [modified-tgt-steps (apply-sip-to-tgt-steps tgt-steps bm join-attr)
            tgt-rel            (execute-steps nil db modified-tgt-steps)]
        (hash-join-execute-into in-cols tgt-rel input sink)))))

(defn- sip-hash-join-execute
  "Execute hash join with SIP optimization (for -execute path)"
  [db link link-e in-cols tgt-steps input]
  (when (and input (pos? (.size ^List input)))
    (let [join-attr   (:attr link)
          col-idx     (pl/find-index link-e in-cols)
          bm          (build-sip-bitmap input col-idx)
          cardinality (b/bitmap64-cardinality bm)]
      (if (pos? cardinality)
        (let [modified-tgt-steps (apply-sip-to-tgt-steps tgt-steps bm join-attr)
              tgt-rel            (execute-steps nil db modified-tgt-steps)
              out                (FastList. cardinality)]
          (hash-join-execute-into in-cols tgt-rel input out)
          out)
        (FastList.)))))

(def pipe-thread-pool (Executors/newCachedThreadPool))

(defn- writing? [db] (l/writing? (.-lmdb ^Store (.-store ^DB db))))

(defn- pipelining
  [context db attrs steps n]
  (let [n-1    (dec ^long n)
        tuples (FastList. (int c/init-exec-size-threshold))
        pipes  (object-array (repeatedly n-1 #(if qu/*explain*
                                                (p/counted-tuple-pipe)
                                                (p/tuple-pipe))))
        work   (fn [step ^long i]
                 (if (zero? i)
                   (-execute-pipe step db nil (aget pipes 0))
                   (let [src (aget pipes (dec i))]
                     (if (= i n-1)
                       (-execute-pipe step db src tuples)
                       (-execute-pipe step db src (aget pipes i))))))
        finish #(when (not= % n-1) (p/finish (aget pipes %)))]
    (if (writing? db)
      (dotimes [i n]
        (let [step (nth steps i)]
          (try
            (work step i)
            (finally (finish i)))))
      (let [tasks (mapv (fn [step i]
                          ^Callable
                          (bound-fn []
                            (try
                              (work step i)
                              (catch Throwable e
                                (raise "Error in executing step" i e
                                       {:step step}))
                              (finally
                                (finish i)))))
                        steps (range))]
        (doseq [^Future f (.invokeAll ^ExecutorService pipe-thread-pool tasks)]
          (.get f))))
    (p/remove-end-scan tuples)
    (save-intermediates context steps pipes tuples)
    (r/relation! attrs tuples)))

(defn- execute-steps
  "execute all steps of a component's plan to obtain a relation"
  [context db steps]
  (let [steps (vec steps)
        n     (count steps)
        attrs (cols->attrs (:cols (peek steps)))
        run*  (fn []
                (condp = n
                  1 (let [tuples (-execute (first steps) db nil)]
                      (save-intermediates context steps nil tuples)
                      (r/relation! attrs tuples))
                  2 (let [src    (-execute (first steps) db nil)
                          tuples (-execute (peek steps) db src)]
                      (save-intermediates context steps (object-array [src])
                                          tuples)
                      (r/relation! attrs tuples))
                  (pipelining context db attrs steps n)))]
    (if (instance? DB db)
      (let [^Store store (.-store ^DB db)
            lmdb         (.-lmdb store)]
        (if (and lmdb (boolean (:kv-wal? @(l/kv-info lmdb))))
          (kv/with-wal-overlay-scan-bindings lmdb run*)
          (run*)))
      (run*))))

(defn- execute-plan
  [{:keys [plan sources] :as context}]
  (if (= 1 (transduce (map (fn [[_ components]] (count components))) + plan))
    (update context :rels collapse-rels
            (let [[src components] (first plan)
                  all-steps        (vec (mapcat :steps (first components)))]
              (execute-steps context (sources src) all-steps)))
    (reduce
      (fn [c r] (update c :rels collapse-rels r))
      context (->> plan
                   (mapcat (fn [[src components]]
                             (let [db (sources src)]
                               (for [plans components]
                                 [db (mapcat :steps plans)]))))
                   (map+ #(apply execute-steps context %))
                   (sort-by #(count (:tuples %)))))))

(defn- planning
  [context]
  (pl/planning or-join-execute-link context))

(defn -q
  [context run?]
  (kv/with-wal-overlay-scan-bindings
    (fn []
      (binding [qu/*implicit-source* (get (:sources context) '$)]
        (let [{:keys [result-set] :as context} (planning context)]
          (if (= result-set #{})
            (do (pl/plan-explain) context)
            (as-> context c
              (do (pl/plan-explain) c)
              (if run? (execute-plan c) c)
              (if run? (reduce resolve-clause c (:late-clauses c)) c))))))))

(defn -collect-tuples
  [acc rel ^long len copy-map]
  (->Eduction
    (comp
      (map (fn [^objects t1]
             (->Eduction
               (map (fn [t2]
                      (let [res (aclone t1)]
                        (if (u/array? t2)
                          (dotimes [i len]
                            (when-some [idx (aget ^objects copy-map i)]
                              (aset res i (aget ^objects t2 idx))))
                          (dotimes [i len]
                            (when-some [idx (aget ^objects copy-map i)]
                              (aset res i (get t2 idx)))))
                        res)))
               (:tuples rel))))
      cat)
    acc))

(defn -collect
  ([context symbols]
   (let [rels (:rels context)]
     (-collect [(make-array Object (count symbols))] rels symbols)))
  ([acc rels symbols]
   (cond+
     :let [rel (first rels)]

     (nil? rel) acc

     ;; one empty rel means final set has to be empty
     (empty? (:tuples rel)) []

     :let [keep-attrs (select-keys (:attrs rel) symbols)]

     (empty? keep-attrs) (recur acc (next rels) symbols)

     :let [copy-map (to-array (map #(get keep-attrs %) symbols))
           len      (count symbols)]

     :else
     (recur (-collect-tuples acc rel len copy-map) (next rels) symbols))))

(defn collect
  [{:keys [result-set] :as context} symbols]
  (if (= result-set #{})
    context
    (assoc context :result-set (into (sp/new-spillable-set) (map vec)
                                     (-collect context symbols)))))

(defprotocol IContextResolve
  (-context-resolve [var context]))

(extend-protocol IContextResolve
  Variable
  (-context-resolve [var context]
    (context-resolve-val context (.-symbol var)))
  SrcVar
  (-context-resolve [var context]
    (get-in context [:sources (.-symbol var)]))
  PlainSymbol
  (-context-resolve [var _]
    (or (get built-ins/aggregates (.-symbol var))
        (resolve-sym (.-symbol var))))
  Constant
  (-context-resolve [var _]
    (.-value var)))

(defn- compute-aggregate
  "Compute an aggregate over tuples at the given tuple index."
  [element context tuples tuple-idx]
  (let [f    (-context-resolve (:fn element) context)
        args (mapv #(-context-resolve % context)
                   (butlast (:args element)))
        vals (map #(nth % tuple-idx) tuples)]
    (apply f (conj args vals))))

(defn- eval-find-expr
  "Evaluate a FindExpr by computing its inner aggregates and applying the
  operator."
  [expr context tuples var->idx]
  (let [op   (get built-ins/query-fns (:symbol (:fn expr)))
        args (mapv (fn [arg]
                     (cond
                       (dp/aggregate? arg)
                       (let [var-sym (-> arg :args last :symbol)
                             idx     (get var->idx var-sym)]
                         (compute-aggregate arg context tuples idx))

                       (dp/find-expr? arg)
                       (eval-find-expr arg context tuples var->idx)

                       :else
                       (-context-resolve arg context)))
                   (:args expr))]
    (apply op args)))

(defn- build-var->idx
  "Build a mapping from variable symbols to tuple indices."
  [find-elements]
  (loop [elements find-elements
         idx      0
         result   {}]
    (if (empty? elements)
      result
      (let [elem (first elements)
            vars (dp/-find-vars elem)]
        (recur (rest elements)
               (+ idx (count vars))
               (into result
                     (map vector vars (range idx (+ idx (count vars))))))))))

(defn -aggregate
  [find-elements context tuples]
  (let [var->idx    (build-var->idx find-elements)
        first-tuple (first tuples)]
    (loop [elements  find-elements
           tuple-idx 0
           result    []]
      (if (empty? elements)
        result
        (let [elem     (first elements)
              num-vars (count (dp/-find-vars elem))]
          (cond
            (dp/find-expr? elem)
            (recur (rest elements)
                   (+ tuple-idx num-vars)
                   (conj result (eval-find-expr elem context tuples var->idx)))

            (dp/aggregate? elem)
            (recur (rest elements)
                   (inc tuple-idx)
                   (conj result
                         (compute-aggregate elem context tuples tuple-idx)))

            :else
            (recur (rest elements)
                   (inc tuple-idx)
                   (conj result (nth first-tuple tuple-idx)))))))))

(defn- groupable-elem?
  "Check if an element should be used for grouping
  (not an aggregate or find-expr)."
  [elem]
  (not (or (dp/aggregate? elem) (dp/find-expr? elem))))

(defn aggregate
  [find-elements context resultset]
  (let [group-idxs (u/idxs-of groupable-elem? find-elements)
        group-fn   (fn [tuple] (map #(nth tuple %) group-idxs))
        grouped    (group-by group-fn resultset)]
    (for [[_ tuples] grouped]
      (-aggregate find-elements context tuples))))

(defn- find-aggregate-idx
  "Find the index of an aggregate in find-elements by matching structure."
  [aggregate find-elements]
  (let [agg-var (-> aggregate :args last :symbol)]
    (loop [elems find-elements
           idx   0]
      (when (seq elems)
        (let [elem (first elems)]
          (cond
            ;; Direct aggregate match
            (and (dp/aggregate? elem)
                 (= (-> elem :fn :symbol) (-> aggregate :fn :symbol))
                 (= (-> elem :args last :symbol) agg-var))
            idx

            ;; Skip to next element
            :else
            (recur (rest elems) (inc idx))))))))

(defn- eval-having-arg
  "Evaluate a having predicate argument against an aggregated result tuple."
  [arg find-elements result-tuple]
  (cond
    (dp/aggregate? arg)
    (let [idx (find-aggregate-idx arg find-elements)]
      (when idx (nth result-tuple idx)))

    (dp/find-expr? arg)
    ;; For find-expr in having, we'd need to find the matching column
    ;; For now, just look for a matching find-expr in find-elements
    (let [idx (u/index-of #(and (dp/find-expr? %)
                                (= (:fn %) (:fn arg)))
                          find-elements)]
      (when idx (nth result-tuple idx)))

    (instance? Constant arg)
    (:value arg)

    :else
    arg))

(defn- eval-having-pred
  "Evaluate a single having predicate on an aggregated result tuple."
  [pred find-elements result-tuple]
  (let [pred-fn (get built-ins/query-fns (-> pred :fn :symbol))
        args    (mapv #(eval-having-arg % find-elements result-tuple)
                      (:args pred))]
    (when (and pred-fn (every? some? args))
      (apply pred-fn args))))

(defn apply-having
  "Filter aggregated results by having predicates."
  [having find-elements results]
  (if (seq having)
    (filter (fn [result-tuple]
              (every? #(eval-having-pred % find-elements result-tuple)
                      having))
            results)
    results))

(defn- typed-aget [a i] (aget ^objects a ^Long i))

(defn- tuple-get [tuple]
  (if (u/array? tuple) typed-aget get))

(defn tuples->return-map
  [return-map tuples]
  (if (seq tuples)
    (let [symbols (:symbols return-map)
          idxs    (range 0 (count symbols))
          get-i   (tuple-get (first tuples))]
      (persistent!
        (reduce
          (fn [coll tuple]
            (conj! coll
                   (persistent!
                     (reduce
                       (fn [m i] (assoc! m (nth symbols i) (get-i tuple i)))
                       (transient {}) idxs))))
          (transient #{}) tuples)))
    #{}))

(defprotocol IPostProcess
  (-post-process [find return-map tuples]))

(extend-protocol IPostProcess
  FindRel
  (-post-process [_ return-map tuples]
    (if (nil? return-map)
      tuples
      (tuples->return-map return-map tuples)))

  FindColl
  (-post-process [_ _ tuples]
    (into [] (map first) tuples))

  FindScalar
  (-post-process [_ _ tuples]
    (ffirst tuples))

  FindTuple
  (-post-process [_ return-map tuples]
    (if (some? return-map)
      (first (tuples->return-map return-map [(first tuples)]))
      (first tuples))))

(defn- pull
  [find-elements context resultset]
  (let [resolved (for [find find-elements]
                   (when (dp/pull? find)
                     (let [db      (-context-resolve (:source find) context)
                           pattern (-context-resolve (:pattern find) context)]
                       (dpa/parse-opts db pattern))))]
    (for [tuple resultset]
      (mapv
        (fn [parsed-opts el]
          (if parsed-opts (dpa/pull-impl parsed-opts el) el))
        resolved
        tuple))))

(defn- resolve-redudants
  "handle pathological cases of variable is already bound in where clauses"
  [{:keys [parsed-q] :as context}]
  (let [{:keys [qwhere]} parsed-q
        get-v            #(nth (:pattern %) 2)
        const-v          (fn [patterns]
                           (some #(let [v (get-v %)]
                                    (when (instance? Constant v) (:value v)))
                                 patterns))
        redundant-groups
        (into []
              (->> qwhere
                   (eduction (filter #(instance? Pattern %)))
                   (group-by (fn [{:keys [source pattern]}]
                               [source (first pattern) (second pattern)]))
                   (eduction (filter
                               #(let [ps (val %)]
                                  (and (< 1 (count ps)) (const-v ps)))))))]
    (reduce
      (fn [c [_ patterns]]
        (let [v (const-v patterns)]
          (reduce
            (fn [c pattern]
              (let [origs (get-in c [:parsed-q :qorig-where])
                    idx   (u/index-of #(= pattern %) origs)]
                (-> c
                    (update-in [:parsed-q :qwhere] #(remove #{pattern} %))
                    (update-in [:parsed-q :qorig-where]
                               #(u/remove-idxs #{idx} %))
                    (update :rels conj
                            (r/relation! {(:symbol (get-v pattern)) 0}
                                         (doto (FastList.)
                                           (.add (object-array [v]))))))))
            c (eduction (filter #(instance? Variable (get-v %))) patterns))))
      context
      redundant-groups)))

(defn- result-explain
  ([context result]
   (result-explain context)
   (when qu/*explain* (vswap! qu/*explain* assoc :result result)))
  ([{:keys [graph result-set plan opt-clauses late-clauses run?] :as context}]
   (when qu/*explain*
     (let [{:keys [^long planning-time ^long parsing-time ^long building-time]}
           @qu/*explain*

           et  (double (/ (- ^long (System/nanoTime)
                             (+ ^long qu/*start-time* planning-time
                                parsing-time building-time))
                          1000000))
           bt  (double (/ building-time 1000000))
           plt (double (/ planning-time 1000000))
           pat (double (/ parsing-time 1000000))
           ppt (double (/ (+ parsing-time building-time planning-time)
                          1000000))]
       (vswap! qu/*explain* assoc
               :actual-result-size (count result-set)
               :parsing-time (format "%.3f" pat)
               :building-time (format "%.3f" bt)
               :planning-time (format "%.3f" plt)
               :prepare-time (format "%.3f" ppt)
               :execution-time (format "%.3f" et)
               :opt-clauses opt-clauses
               :query-graph (w/postwalk
                              (fn [e]
                                (if (map? e)
                                  (apply dissoc e
                                         (for [[k v] e
                                               :when (nil? v)] k))
                                  e)) graph)
               :plan (w/postwalk
                       (fn [e]
                         (if (instance? Plan e)
                           (let [{:keys [steps] :as plan} e]
                             (cond->
                                 (assoc plan :steps
                                        (mapv #(-explain % context) steps))
                               run? (assoc :actual-size
                                           (get-in @(:intermediates context)
                                                   [(:out (last steps))
                                                    :tuples-count]))))
                           e)) plan)
               :late-clauses late-clauses)))))

(defn- parsed-q
  [q]
  (or (.get ^LRUCache *query-cache* q)
      (let [res (dp/parse-query q)]
        (.put ^LRUCache *query-cache* q res)
        res)))

(defn- order-comp
  [tg idx di]
  (if (identical? di :asc)
    (fn [t1 t2] (compare (tg t1 idx) (tg t2 idx)))
    (fn [t1 t2] (compare (tg t2 idx) (tg t1 idx)))))

(defn- order-comps
  [tg find-vars order]
  (let [pairs (partition-all 2 order)
        idxs  (mapv (fn [v]
                      (if (integer? v)
                        v
                        (u/index-of #(= v %) find-vars)))
                    (into [] (map first) pairs))
        comps (reverse (mapv #(order-comp tg %1 %2) idxs
                             (into [] (map second) pairs)))]
    (reify Comparator
      (compare [_ t1 t2]
        (loop [comps comps res (num 0)]
          (if (not-empty comps)
            (recur (next comps) (let [r ((first comps) t1 t2)]
                                  (if (= 0 r) res r)))
            res))))))

(defn- order-result
  [find-vars result order]
  (if (seq result)
    (sort (order-comps (tuple-get (first result)) find-vars order) result)
    result))

(defn- q*
  [parsed-q inputs]
  (binding [timeout/*deadline* (timeout/to-deadline (:qtimeout parsed-q))]
    (let [find          (:qfind parsed-q)
          find-elements (dp/find-elements find)
          result-arity  (count find-elements)
          with          (:qwith parsed-q)
          having        (:qhaving parsed-q)
          find-vars     (dp/find-vars find)
          all-vars      (concatv find-vars (map :symbol with))

          [parsed-q inputs] (pl/plugin-inputs parsed-q inputs)

          context
          (-> (Context. parsed-q [] {} {} [] nil nil nil nil
                        (volatile! {}) true nil)
              (resolve-ins inputs)
              (resolve-redudants)
              (rules/rewrite)
              (pl/rewrite-unused-vars)
              (-q true)
              (collect all-vars))
          result
          (cond->> (:result-set context)
            with (mapv #(subvec % 0 result-arity))

            (some #(or (dp/aggregate? %) (dp/find-expr? %)) find-elements)
            (aggregate find-elements context)

            (seq having)
            (apply-having having find-elements)

            (some dp/pull? find-elements) (pull find-elements context)

            true (-post-process find (:qreturn-map parsed-q)))]
      (result-explain context result)
      (if-let [order (:qorder parsed-q)]
        (if (instance? FindRel find)
          (order-result find-vars result order)
          result)
        result))))

(defn- resolve-qualified-fns
  "Convert qualified fns to fn-objects so that function implementation
  changes invalidate query result cache."
  [qualified-fns]
  (into #{} (map #(some-> % resolve deref)) qualified-fns))

(defn- query-cache-deps
  "Extract conservative dependencies for query-result cache invalidation.

  If dependency analysis is uncertain, return {:all? true} so the entry is
  invalidated on any transaction."
  [parsed-q]
  (letfn [(keyword-constant [term]
            (when (instance? Constant term)
              (let [v (:value ^Constant term)]
                (when (keyword? v) v))))
          (merge-deps [x y]
            (if (or (:all? x) (:all? y))
              {:all? true}
              {:all?  false
               :attrs (into (:attrs x #{}) (:attrs y #{}))}))
          (pattern-deps [parsed-q]
            (let [patterns (dp/collect #(instance? Pattern %) (:qwhere parsed-q))]
              (loop [ps    patterns
                     attrs (transient #{})
                     all?  false]
                (cond
                  all?
                  {:all? true}

                  (empty? ps)
                  {:all? false :attrs (persistent! attrs)}

                  :else
                  (let [^Pattern p (first ps)
                        attr-term  (nth (:pattern p) 1 nil)]
                    (cond
                      (instance? Constant attr-term)
                      (recur (rest ps) (conj! attrs (:value ^Constant attr-term))
                             false)

                      ;; Variable / placeholder in attribute position means query
                      ;; may touch arbitrary attributes.
                      :else
                      (recur nil attrs true)))))))
          (tuple-fn-deps [parsed-q]
            (let [fns (dp/collect #(instance? Function %) (:qwhere parsed-q))]
              (reduce
                (fn [acc ^Function f]
                  (let [fname (some-> (:fn f) :symbol)
                        args  (:args f)]
                    (if (contains? tuple-producing-fns fname)
                      (if-let [a (keyword-constant (nth args 1 nil))]
                        (merge-deps acc {:all? false :attrs #{a}})
                        ;; No explicit attribute (or non-constant) means a
                        ;; domain-wide / DB-wide tuple-producing search.
                        {:all? true})
                      acc)))
                {:all? false :attrs #{}}
                fns)))]
    (let [find-elements (dp/find-elements (:qfind parsed-q))]
      (if (some dp/pull? find-elements)
        {:all? true}
        (merge-deps (pattern-deps parsed-q) (tuple-fn-deps parsed-q))))))

(defn- store-write-context-token
  [store]
  (if (instance? Store store)
    (let [lmdb      (.-lmdb ^Store store)
          tx-holder (l/write-txn lmdb)
          tx        @tx-holder
          writing?  (l/writing? lmdb)]
      (when tx
        ;; Bind query-cache entries to a concrete write-txn context so values
        ;; produced during an open write session cannot leak into the steady
        ;; state after commit/abort. Include read/write role to keep reader and
        ;; writer snapshots isolated while the write-txn is active.
        [(if writing? :write :read)
         (System/identityHashCode tx-holder)
         (System/identityHashCode tx)]))
    (let [tx-holder (l/write-txn store)]
      (when (l/writing? store)
        [(System/identityHashCode tx-holder) 0]))))

(defn- cache-input-token
  [input]
  (if (db/-searchable? input)
    (let [store (.-store ^DB input)]
      [:db-input
       (db-name store)
       (dir store)
       (store-write-context-token store)])
    input))

(defn- q-result
  [parsed-q inputs]
  (if *cache?*
    (if-let [store (some #(when (db/-searchable? %) (.-store^DB %)) inputs)]
      (let [parsed-q' (-> (update parsed-q :qwhere-qualified-fns
                                  resolve-qualified-fns)
                          (dissoc :limit :offset))
            deps      (query-cache-deps parsed-q')
            k         [:query-result deps parsed-q' (mapv cache-input-token inputs)]]
        (if-let [cached (db/cache-get store k)]
          cached
          (let [res (q* parsed-q inputs)]
            (db/cache-put store k res)
            res)))
      (q* parsed-q inputs))
    (q* parsed-q inputs)))

(defn- parse-explain
  []
  (when qu/*explain*
    (vswap! qu/*explain* assoc :parsing-time
            (- (System/nanoTime) ^long qu/*start-time*))))

(defn- perform
  [q & inputs]
  (let [parsed-q (parsed-q q)
        _        (parse-explain)
        result   (q-result parsed-q inputs)]
    (if (instance? FindRel (:qfind parsed-q))
      (let [limit  (:qlimit parsed-q)
            offset (:qoffset parsed-q)]
        (->> result
             (#(if offset (drop offset %) %))
             (#(if (or (nil? limit) (= limit -1)) % (take limit %)))))
      result)))

(defn- plan-only
  [q & inputs]
  (let [parsed-q (parsed-q q)]
    (parse-explain)
    (binding [timeout/*deadline* (timeout/to-deadline (:qtimeout parsed-q))]
      (let [[parsed-q inputs] (pl/plugin-inputs parsed-q inputs)]
        (-> (Context. parsed-q [] {} {} [] nil nil nil nil (volatile! {})
                      false nil)
            (resolve-ins inputs)
            (resolve-redudants)
            (rules/rewrite)
            (pl/rewrite-unused-vars)
            (-q false)
            (result-explain))))))

(defn- explain*
  [{:keys [run?] :or {run? false}} & args]
  (binding [qu/*explain*    (volatile! {})
            *cache?*     false
            qu/*start-time* (System/nanoTime)]
    (if run?
      (do (apply perform args) @qu/*explain*)
      (do (apply plan-only args)
          (dissoc @qu/*explain* :actual-result-size :execution-time)))))

(defn- only-remote-db
  "Return [remote-db [updated-inputs]] if the inputs contain only one db
  and its backing store is a remote one, where the remote-db in the inputs is
  replaced by `:remote-db-placeholder, otherwise return `nil`"
  [inputs]
  (let [dbs (filter db/-searchable? inputs)]
    (when-let [rdb (first dbs)]
      (let [rstore (.-store ^DB rdb)]
        (when (and (= 1 (count dbs))
                   (instance? DatalogStore rstore)
                   (db/db? rdb))
          [rstore (vec (replace {rdb :remote-db-placeholder} inputs))])))))

(defn q
  [query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (rt/q store query inputs')
    (apply perform query inputs)))

(defn explain
  [opts query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (rt/explain store opts query inputs')
    (apply explain* opts query inputs)))
