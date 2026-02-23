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
   [clojure.core.reducers :as rd]
   [clojure.walk :as w]
   [datalevin.db :as db]
   [datalevin.lmdb :as l]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.join :as j]
   [datalevin.rules :as rules]
   [datalevin.storage :as s]
   [datalevin.built-ins :as built-ins]
   [datalevin.util :as u :refer [cond+ raise conjv concatv map+]]
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
    :refer [av-size dir db-name]])
  (:import
   [java.util Arrays List Collection Comparator HashSet HashMap]
   [java.util.concurrent ConcurrentHashMap ExecutorService Executors Future
    Callable]
   [datalevin.utl LikeFSM LRUCache]
   [datalevin.remote DatalogStore]
   [datalevin.db DB]
   [datalevin.relation Relation]
   [datalevin.storage Store]
   [datalevin.parser And BindColl BindIgnore BindScalar BindTuple Constant
    DefaultSrc FindColl FindRel FindScalar FindTuple Function Or PlainSymbol
    RulesVar SrcVar Variable Pattern Predicate Not RuleExpr]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *query-cache* (LRUCache. c/query-result-cache-size))

(def ^:dynamic *plan-cache* (LRUCache. c/query-result-cache-size))

(def ^:dynamic *explain* nil)

(def ^:dynamic *cache?* true)

(def ^:dynamic *start-time* nil)

(declare -collect -resolve-clause resolve-clause execute-steps
         hash-join-execute hash-join-execute-into sip-hash-join-execute
         estimate-hash-join-cost get-or-join-vars get-or-join-source)

;; Records

(defrecord Context [parsed-q rels sources rules opt-clauses late-clauses
                    optimizable-or-joins graph plan intermediates run?
                    result-set])

(defrecord Plan [steps cost size recency])

(defprotocol IStep
  (-type [step] "return the type of step as a keyword")
  (-execute [step db source] "execute query step and return tuples")
  (-execute-pipe [step db source sink] "execute as part of pipeline")
  (-sample [step db source] "sample the step, not all steps implement")
  (-explain [step context] "explain the query step"))

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
          (let [pipe (if *explain*
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

;; optimizer

(defn- or-join-var?
  [clause s]
  (and (list? clause)
       (= 'or-join (first clause))
       (some #(= % s) (tree-seq sequential? seq (second clause)))))

(defn- plugin-inputs*
  [parsed-q inputs]
  (let [qins    (:qin parsed-q)
        finds   (tree-seq sequential? seq (:qorig-find parsed-q))
        owheres (:qorig-where parsed-q)
        to-rm   (keep-indexed
                  (fn [i qin]
                    (let [v (:variable qin)
                          s (:symbol v)
                          val (nth inputs i)]
                      (when (and (instance? BindScalar qin)
                                 (instance? Variable v)
                                 ;; keep sequential inputs as variables so
                                 ;; function calls don't eagerly evaluate them
                                 (not (sequential? val))
                                 (not (some #(= s %) finds))
                                 (not (some #(or-join-var? % s) owheres)))
                        [i s])))
                  qins)
        rm-idxs (into #{} (map first) to-rm)
        smap    (reduce (fn [m [i s]] (assoc m s (nth inputs i))) {} to-rm)]
    [(assoc parsed-q
            :qwhere (reduce-kv
                      (fn [ws s v]
                        (w/postwalk
                          (fn [e]
                            (if (and (instance? Variable e)
                                     (= s (:symbol e)))
                              (Constant. v)
                              e))
                          ws))
                      (:qwhere parsed-q) smap)
            :qorig-where (w/postwalk-replace smap owheres)
            :qin (u/remove-idxs rm-idxs qins))
     (u/remove-idxs rm-idxs inputs)]))

(defn- plugin-inputs
  "optimization that plugs simple value inputs into where clauses"
  [parsed-q inputs]
  (let [ins (:qin parsed-q)
        cb  (count ins)
        cv  (count inputs)]
    (cond
      (< cb cv) (raise "Extra inputs passed, expected: "
                       (mapv #(:source (meta %)) ins) ", got: " cv
                       {:error :query/inputs :expected ins :got inputs})
      (> cb cv) (raise "Too few inputs passed, expected: "
                       (mapv #(:source (meta %)) ins) ", got: " cv
                       {:error :query/inputs :expected ins :got inputs})
      :else     (plugin-inputs* parsed-q inputs))))

(defn- var-symbol
  [v]
  (when (instance? Variable v)
    (:symbol v)))

(defn- collect-var-usage
  [qwhere]
  (let [counts    (volatile! {})
        kinds     (volatile! {})
        protected (volatile! #{})]
    (letfn [(note-var! [sym kind]
              (when (qu/binding-var? sym)
                (vswap! counts update sym (fnil inc 0))
                (vswap! kinds update sym (fnil conj #{}) kind)))
            (protect-var! [sym]
              (when (qu/free-var? sym)
                (vswap! protected conj sym)))
            (note-var [v kind]
              (when-let [sym (var-symbol v)]
                (note-var! sym kind)))
            (protect-var [v]
              (when-let [sym (var-symbol v)]
                (protect-var! sym)))
            (protect-vars-in-form [form]
              (doseq [sym (qu/collect-vars form)]
                (protect-var! sym)))
            (protect-arg-vars [arg]
              (when (instance? Constant arg)
                (protect-vars-in-form (:value arg))))
            (walk-binding [binding]
              (cond
                (instance? BindScalar binding)
                (note-var (:variable binding) :binding)

                (instance? BindTuple binding)
                (doseq [b (:bindings binding)]
                  (walk-binding b))

                (instance? BindColl binding)
                (walk-binding (:binding binding))

                :else nil))
            (walk-clause [clause]
              (cond
                (instance? Pattern clause)
                (doseq [el (:pattern clause)]
                  (note-var el :pattern))

                (instance? Function clause)
                (do
                  (protect-var (:fn clause))
                  (doseq [arg (:args clause)]
                    (protect-var arg)
                    (protect-arg-vars arg))
                  (walk-binding (:binding clause)))

                (instance? Predicate clause)
                (do
                  (protect-var (:fn clause))
                  (doseq [arg (:args clause)]
                    (protect-var arg)
                    (protect-arg-vars arg)))

                (instance? RuleExpr clause)
                (doseq [arg (:args clause)]
                  (protect-var arg))

                (instance? And clause)
                (doseq [c (:clauses clause)]
                  (walk-clause c))

                (instance? Or clause)
                (doseq [c (:clauses clause)]
                  (protect-vars-in-form c)
                  (walk-clause c))

                (instance? Not clause)
                (doseq [c (:clauses clause)]
                  (protect-vars-in-form c)
                  (walk-clause c))

                :else nil))]
      (doseq [c qwhere] (walk-clause c))
      {:counts @counts :kinds @kinds :protected @protected})))

(defn- unused-var-replacements
  [parsed-q]
  (let [find-vars (set (dp/find-vars (:qfind parsed-q)))
        with-vars (set (map :symbol (or (:qwith parsed-q) [])))
        in-vars   (set (map :symbol (dp/collect-vars-distinct (:qin parsed-q))))
        used      (set/union find-vars with-vars in-vars)
        {:keys [counts kinds protected]}
        (collect-var-usage (:qwhere parsed-q))]
    (into {}
          (keep (fn [[sym n]]
                  (when (and (= 1 n)
                             (not (contains? used sym))
                             (not (contains? protected sym)))
                    (let [kind (get kinds sym)]
                      [sym (if (contains? kind :binding)
                             '_
                             (qu/placeholder-sym sym))]))))
          counts)))

(defn- replace-unused-vars-form
  [form replacements]
  (letfn [(walk [form]
            (cond
              (qu/quoted-form? form) form
              (symbol? form)         (get replacements form form)
              (map? form)            (into (empty form)
                                           (map (fn [[k v]]
                                                  [(walk k) (walk v)]))
                                           form)
              (seq? form)            (apply list (map walk form))
              (coll? form)           (into (empty form) (map walk) form)
              :else                  form))]
    (walk form)))

(defn- rewrite-unused-vars
  [{:keys [parsed-q] :as context}]
  (let [replacements (unused-var-replacements parsed-q)]
    (if (empty? replacements)
      context
      (let [qorig-where  (mapv #(replace-unused-vars-form % replacements)
                               (:qorig-where parsed-q))
            qwhere       (dp/parse-where qorig-where)]
        (assoc context :parsed-q
               (assoc parsed-q :qorig-where qorig-where :qwhere qwhere))))))

(defn- optimizable?
  "only optimize attribute-known patterns referring to Datalevin source"
  [sources resolved clause]
  (when (instance? Pattern clause)
    (let [{:keys [pattern]} clause]
      (when (and (instance? Constant (second pattern))
                 (not-any? resolved (map :symbol pattern)))
        (if-let [s (get-in clause [:source :symbol])]
          (when-let [src (get sources s)] (db/-searchable? src))
          (when-let [src (get sources '$)] (db/-searchable? src)))))))

(defn- parsed-rule-expr? [clause] (instance? datalevin.parser.RuleExpr clause))

(defn- rule-clause?
  "Check if clause is a rule call"
  [rules clause]
  (or (parsed-rule-expr? clause)
      (when (and (sequential? clause) (not (vector? clause)))
        (let [head (qu/clause-head clause)]
          (and (symbol? head)
               (not (qu/free-var? head))
               (not (qu/rule-head head))
               (contains? rules head))))))

(defn- get-rule-args
  [clause]
  (if (parsed-rule-expr? clause)
    (mapv #(when (instance? Variable %) (:symbol %)) (:args clause))
    (qu/clause-args clause)))

(defn- or-join-clause?
  [clause]
  (or (instance? Or clause)
      (and (sequential? clause)
           (not (vector? clause))
           (= 'or-join (first clause)))))

(defn- get-or-join-vars
  [clause]
  (cond
    (instance? Or clause)
    (let [rule-vars (:rule-vars clause)]
      (when rule-vars
        (into []
              (comp (map :symbol) (filter qu/free-var?))
              (concat (:required rule-vars) (:free rule-vars)))))

    (or-join-clause? clause)
    (let [[_ vars & _] clause]
      (if (vector? (first vars))
        (into (vec (first vars)) (rest vars))
        (vec vars)))))

(defn- get-or-join-branches
  [clause]
  (cond
    (instance? Or clause)
    (:clauses clause)

    (or-join-clause? clause)
    (let [[_ vars & branches] clause]
      (if (and (sequential? vars) (vector? (first vars)))
        branches
        branches))))

(defn- infer-branch-source
  "Infer the source for an or-join branch.
   A branch can be:
   - A single pattern: [?e :attr ?v] or [$src ?e :attr ?v]
   - An (and ...) clause containing patterns
   Returns the source symbol (e.g., '$, '$src) or nil if ambiguous/unknown."
  [branch]
  (cond
    (instance? Pattern branch)
    (let [src (:source branch)]
      (if (instance? DefaultSrc src) '$ (:symbol src)))

    (instance? And branch)
    (when-let [first-clause (first (:clauses branch))]
      (infer-branch-source first-clause))

    (vector? branch)
    (if (qu/source? (first branch))
      (first branch)
      '$)

    (and (sequential? branch) (= 'and (first branch)))
    (when-let [first-clause (second branch)]
      (infer-branch-source first-clause))

    :else nil))

(defn- single-source-or-join?
  [sources clause]
  (let [branches       (get-or-join-branches clause)
        branch-sources (keep infer-branch-source branches)]
    (and (seq branch-sources)
         (= (count branch-sources) (count branches))
         (apply = branch-sources)
         (contains? sources (first branch-sources)))))

(defn- or-join-branch-valid?
  "Check if an or-join branch is valid for optimization.
   A branch is valid if it contains patterns that can be executed."
  [branch]
  (cond
    (instance? Pattern branch)
    true

    (instance? And branch)
    (every? #(or (instance? Pattern %)
                 (instance? Predicate %)
                 (instance? Function %))
            (:clauses branch))

    ;; Raw pattern vector
    (vector? branch)
    true

    ;; Raw (and ...) form
    (and (sequential? branch) (= 'and (first branch)))
    true

    ;; Other forms (not, not-join, nested or-join) - not valid for optimization
    :else false))

(defn- or-join-optimizable?
  "Check if an or-join can be optimized as a link step in the query graph.
   An or-join is optimizable when:
   1. It's an or-join clause (not plain or)
   2. Has exactly ONE bound join var (from input or patterns)
   3. Has at least one unbound join var (to be derived)
   4. Bound var is not rule-derived (would create dependency issue)
   5. Bound var is a graph node (pattern entity var)
   6. All branches reference same source
   7. All branches are valid patterns

   Returns the clause if optimizable, nil otherwise."
  [sources resolved clause pattern-entity-vars rule-derived-vars]
  (when (or-join-clause? clause)
    (let [vars          (get-or-join-vars clause)
          ;; A variable is considered "will be bound" if it's:
          ;; 1. Already bound by input relations (in resolved), OR
          ;; 2. A pattern entity var (will be bound when patterns execute)
          will-be-bound (set/union resolved pattern-entity-vars)
          bound-vars    (filterv will-be-bound vars)
          free-vars     (filterv (complement will-be-bound) vars)]
      (when (and (= 1 (count bound-vars))
                 (seq free-vars)
                 (not (contains? (or rule-derived-vars #{})
                                 (first bound-vars)))
                 (contains? pattern-entity-vars (first bound-vars))
                 (single-source-or-join? sources clause)
                 (every? or-join-branch-valid? (get-or-join-branches clause)))
        clause))))

(defn- find-rule-derived-vars
  "Find entity variables that should be derived from rules/or-join rather than
   scanned. A variable is rule-derived if:
   1. It appears in a rule call or or-join alongside an already-bound variable
   2. The patterns using it as entity have no value constraints

   This detects cases where a rule/or-join connects a bound var to an unbound
   var. "
  [clauses rules input-bound-vars optimizable-or-joins]
  (let [patterns             (filter #(instance? Pattern %) clauses)
        constrained-entities
        (into #{}
              (comp
                (filter
                  (fn [p]
                    (let [pat (:pattern p)]
                      (and (>= (count pat) 3)
                           (let [v (nth pat 2)]
                             (or (instance? Constant v)
                                 (and (instance? Variable v)
                                      (input-bound-vars (:symbol v)))))))))
                (map #(get-in % [:pattern 0 :symbol]))
                (filter qu/binding-var?))
              patterns)
        ref-connected
        (reduce
          (fn [m p]
            (let [pat   (:pattern p)
                  e-var (get-in pat [0 :symbol])
                  v-var (when (<= 3 (count pat))
                          (let [v (nth pat 2)]
                            (when (instance? Variable v) (:symbol v))))]
              (if (and (qu/binding-var? e-var) (qu/binding-var? v-var))
                (-> m
                    (update e-var (fnil conj #{}) v-var)
                    (update v-var (fnil conj #{}) e-var))
                m)))
          {} patterns)
        reachable
        (loop [frontier constrained-entities
               visited  #{}]
          (if (empty? frontier)
            visited
            (let [next-frontier (into #{}
                                      (comp
                                        (mapcat ref-connected)
                                        (remove visited))
                                      frontier)]
              (recur next-frontier (into visited frontier)))))
        all-pattern-vars
        (into #{}
              (comp
                (mapcat (fn [p]
                          (let [pat   (:pattern p)
                                e-var (get-in pat [0 :symbol])
                                v-var (when (>= (count pat) 3)
                                        (let [v (nth pat 2)]
                                          (when (instance? Variable v)
                                            (:symbol v))))]
                            (cond-> []
                              (qu/binding-var? e-var) (conj e-var)
                              (qu/binding-var? v-var) (conj v-var)))))
                (filter qu/binding-var?))
              patterns)
        all-entity-vars      (into #{}
                                   (comp
                                     (map #(get-in % [:pattern 0 :symbol]))
                                     (filter qu/binding-var?))
                                   patterns)
        unreachable-entities (set/difference all-entity-vars reachable)
        unreachable-vars     (set/difference all-pattern-vars reachable)
        directly-rule-derived
        (reduce
          (fn [derived clause]
            (if (or (parsed-rule-expr? clause) (rule-clause? rules clause))
              (let [args                (get-rule-args clause)
                    rule-vars           (filterv qu/binding-var? args)
                    has-reachable?      (some reachable rule-vars)
                    unreachable-in-rule (filter unreachable-entities rule-vars)]
                (if (and has-reachable? (seq unreachable-in-rule))
                  (into derived unreachable-in-rule)
                  derived))
              derived))
          #{} clauses)
        optimizable-set      (set optimizable-or-joins)
        or-join-derived
        (reduce
          (fn [derived clause]
            (if (or-join-clause? clause)
              (if (optimizable-set clause)
                derived
                (let [or-vars           (get-or-join-vars clause)
                      has-reachable?    (some reachable or-vars)
                      unreachable-in-or (filter unreachable-vars or-vars)]
                  (if (and has-reachable? (seq unreachable-in-or))
                    (into derived unreachable-in-or)
                    derived)))
              derived))
          directly-rule-derived clauses)
        indirectly-derived
        (loop [frontier or-join-derived
               derived  or-join-derived]
          (if (empty? frontier)
            derived
            (let [newly-derived (into #{}
                                      (comp
                                        (mapcat ref-connected)
                                        (filter unreachable-entities)
                                        (remove derived))
                                      frontier)]
              (recur newly-derived (into derived newly-derived)))))
        rule-only-indirectly
        (loop [frontier directly-rule-derived
               derived  directly-rule-derived]
          (if (empty? frontier)
            derived
            (let [newly-derived (into #{}
                                      (comp
                                        (mapcat ref-connected)
                                        (filter unreachable-entities)
                                        (remove derived))
                                      frontier)]
              (recur newly-derived (into derived newly-derived)))))]
    {:rule-derived (when (seq rule-only-indirectly) rule-only-indirectly)
     :all-derived  (when (seq indirectly-derived) indirectly-derived)}))

(defn- depends-on-rule-output?
  "Check if a pattern's entity variable should be derived from a rule."
  [clause rule-derived-vars]
  (when (and rule-derived-vars (instance? Pattern clause))
    (let [e-var (get-in clause [:pattern 0 :symbol])]
      (and (qu/binding-var? e-var)
           (contains? rule-derived-vars e-var)))))

(defn- split-clauses
  "Split clauses into two parts: opt-clauses (to be optimized) and late-clauses.
   Also identifies optimizable or-join clauses for graph integration."
  [{:keys [sources parsed-q rels rules] :as context}]
  (let [resolved    (reduce (fn [rs {:keys [attrs]}]
                              (set/union rs (set (keys attrs))))
                            #{} rels)
        ;; Variables bound by input relations
        input-bound (reduce (fn [s {:keys [attrs]}]
                              (into s (keys attrs)))
                            #{} rels)
        qwhere      (:qwhere parsed-q)
        clauses     (:qorig-where parsed-q)

        ;; Collect entity vars from patterns (these will be graph nodes)
        pattern-entity-vars (into #{}
                                  (comp
                                    (filter #(instance? Pattern %))
                                    (map #(get-in % [:pattern 0 :symbol]))
                                    (filter qu/binding-var?))
                                  qwhere)

        ;; Find optimizable or-joins
        opt-or-joins
        (filterv #(or-join-optimizable? sources resolved % pattern-entity-vars
                                        nil)
                 qwhere)

        ;; Find variables derived from rules
        ;; Use all-derived which includes both rule-derived and or-join-derived
        rule-derived
        (:all-derived
         (find-rule-derived-vars qwhere rules input-bound opt-or-joins))

        opt-or-join-set (set opt-or-joins)

        ptn-idxs
        (set (u/idxs-of
               (fn [clause]
                 (and (optimizable? sources resolved clause)
                      (not (depends-on-rule-output? clause rule-derived))))
               qwhere))

        or-join-idxs
        (set (u/idxs-of (fn [clause] (opt-or-join-set clause)) qwhere))]
    (assoc context
           :opt-clauses (u/keep-idxs ptn-idxs clauses)
           :optimizable-or-joins (u/keep-idxs or-join-idxs clauses)
           :late-clauses (u/remove-idxs (set/union ptn-idxs or-join-idxs)
                                        clauses))))

(defn- make-node
  [[e patterns]]
  [e (reduce (fn [m pattern]
               (let [attr   (second pattern)
                     clause (map->Clause {:attr attr})]
                 (if-let [v (qu/get-v pattern)]
                   (if (qu/free-var? v)
                     (update m :free conjv (assoc clause :var v))
                     (update m :bound conjv (assoc clause :val v)))
                   (update m :free conjv clause))))
             (map->Node {}) patterns)])

(defn- link-refs
  [graph]
  (let [es (set (keys graph))]
    (reduce-kv
      (fn [g e {:keys [free]}]
        (reduce
          (fn [g {:keys [attr var]}]
            (if (es var)
              (-> g
                  (update-in [e :links] conjv (Link. :ref var nil nil attr))
                  (update-in [var :links] conjv (Link. :_ref e nil nil attr)))
              g))
          g free))
      graph graph)))

(defn- link-eqs
  [graph]
  (reduce-kv
    (fn [g v lst]
      (if (< 1 (count lst))
        (reduce
          (fn [g [[e1 k1] [e2 k2]]]
            (let [attrs {e1 k1 e2 k2}]
              (-> g
                  (update-in
                    [e1 :links] conjv (Link. :val-eq e2 v attrs nil))
                  (update-in
                    [e2 :links] conjv (Link. :val-eq e1 v attrs nil)))))
          g (u/combinations lst 2))
        g))
    graph (reduce-kv (fn [m e {:keys [free]}]
                       (reduce (fn [m {:keys [attr var]}]
                                 (if var (update m var conjv [e attr]) m))
                               m free))
                     {} graph)))

(defn- make-nodes
  [[src patterns]]
  [src (let [patterns' (mapv qu/replace-blanks patterns)
             graph     (into {} (map make-node) (group-by first patterns'))]
         (if (< 1 (count graph))
           (-> graph link-refs link-eqs)
           graph))])

(defn- extract-or-join-info
  "Extract bound/free variable information from an or-join clause.
   Currently, we only support single bound var. "
  [will-be-bound clause]
  (let [vars       (get-or-join-vars clause)
        bound-vars (filterv will-be-bound vars)
        free-vars  (filterv (complement will-be-bound) vars)]
    {:bound-var (first bound-vars)
     :free-vars free-vars
     :clause    clause}))

(defn- find-nodes-using-vars
  "Find entity nodes that have patterns using any of the given vars in value
  position."
  [graph vars]
  (let [var-set (set vars)]
    (into []
          (comp
            (filter (fn [[_e node]]
                      (some (fn [{:keys [var]}]
                              (contains? var-set var))
                            (:free node))))
            (map first))
          graph)))

(defn- link-or-joins
  "Add or-join links to the graph.
   Creates links from bound variable node to entity nodes that USE the free
   variables.
   Returns {:graph updated-graph :unlinked or-joins-that-couldnt-link}"
  [graph will-be-bound or-join-clauses source]
  (reduce
    (fn [{:keys [graph unlinked]} clause]
      (let [{:keys [bound-var free-vars]}
            (extract-or-join-info will-be-bound clause)
            target-entities (find-nodes-using-vars graph free-vars)]

        (if (and bound-var (seq target-entities) (contains? graph bound-var))
          {:graph
           (reduce
             (fn [g tgt-entity]
               (let [tgt-node (get graph tgt-entity)
                     tgt-attr (some (fn [fv]
                                      (some (fn [{:keys [attr var]}]
                                              (when (= var fv) attr))
                                            (:free tgt-node)))
                                    free-vars)]
                 (update-in g [bound-var :links] conjv
                            (OrJoinLink.
                              :or-join tgt-entity clause bound-var free-vars
                              tgt-attr source))))
             graph
             target-entities)
           :unlinked unlinked}
          {:graph    graph
           :unlinked (conj unlinked clause)})))
    {:graph graph :unlinked []} or-join-clauses))

(defn- resolve-lookup-refs
  [sources [src patterns]]
  [src (mapv #(resolve-pattern-lookup-refs (sources src) %) patterns)])

(defn- remove-src
  [[src patterns]]
  [src (mapv #(if (= (first %) src) (vec (rest %)) %) patterns)])

(defn- get-src [[f & _]] (if (qu/source? f) f '$))

(defn- get-or-join-source
  [clause]
  (or (infer-branch-source (first (get-or-join-branches clause))) '$))

(defn- init-graph
  "Build one graph per Datalevin db from pattern clauses."
  [context]
  (let [opt-clauses          (:opt-clauses context)
        optimizable-or-joins (:optimizable-or-joins context)
        sources              (:sources context)
        rels                 (:rels context)

        ;; Get input-bound vars
        input-bound         (reduce (fn [s {:keys [attrs]}]
                                      (into s (keys attrs)))
                                    #{} rels)
        ;; Get pattern entity vars that will be bound
        ;; opt-clauses contains original vector patterns, not Pattern records
        pattern-entity-vars (into #{}
                                  (comp (filter vector?)
                                     (map first)  ;; entity position
                                     (filter qu/binding-var?))
                                  opt-clauses)
        will-be-bound       (into input-bound pattern-entity-vars)

        ;; Build base graph from patterns (grouped by source)
        base-graphs (into {}
                          (comp
                            (map remove-src)
                            (map #(resolve-lookup-refs sources %))
                            (map make-nodes))
                          (group-by get-src opt-clauses))

        ;; Add or-join links to the graph
        {:keys [graph unlinked]}
        (reduce
          (fn [{:keys [graph unlinked]} clause]
            (let [src (get-or-join-source clause)]
              (if-let [nodes (get graph src)]
                (let [result (link-or-joins nodes will-be-bound [clause] src)]

                  {:graph    (assoc graph src (:graph result))
                   :unlinked (into unlinked (:unlinked result))})
                ;; Source not in graph, can't link
                {:graph graph :unlinked (conj unlinked clause)})))
          {:graph base-graphs :unlinked []}
          optimizable-or-joins)]
    (-> context
        (assoc :graph graph)
        ;; Move unlinked or-joins to late-clauses
        (update :late-clauses into unlinked)
        ;; Clear optimizable-or-joins since they're now in the graph
        (assoc :optimizable-or-joins []))))

(defn- pushdownable
  "predicates that can be pushed down involve only one free variable"
  [where gseq]
  (when (instance? Predicate where)
    (let [{:keys [args]} where
          syms           (qu/collect-vars args)]
      (when (= (count syms) 1)
        (let [s (first syms)]
          (some #(when (= s (:var %)) s) gseq))))))

(defn- range-compare
  ([r1 r2]
   (range-compare r1 r2 true))
  ([[p i] [q j] from?]
   (case i
     :db.value/sysMin -1
     :db.value/sysMax 1
     (case j
       :db.value/sysMax -1
       :db.value/sysMin 1
       (let [res (compare i j)]
         (if (zero? res)
           (if from?
             (cond
               (identical? p q)       0
               (identical? p :closed) -1
               :else                  1)
             (cond
               (identical? p q)     0
               (identical? p :open) -1
               :else                1))
           res))))))

(def range-compare-to #(range-compare %1 %2 false))

(defn- combine-ranges*
  [ranges]
  (let [orig-from (sort range-compare (map first ranges))]
    (loop [intervals (transient [])
           from      (rest orig-from)
           to        (sort range-compare-to (map peek ranges))
           thread    (transient [(first orig-from)])]
      (if (seq to)
        (let [fc (first from)
              tc (first to)]
          (if (= (count from) (count to))
            (recur (conj! intervals (persistent! thread)) (rest from) to
                   (transient [fc]))
            (if fc
              (if (< ^long (range-compare fc tc) 0)
                (recur intervals (rest from) to (conj! thread fc))
                (recur intervals from (rest to) (conj! thread tc)))
              (recur intervals from (rest to) (conj! thread tc)))))
        (mapv (fn [t] [(first t) (peek t)])
              (persistent! (conj! intervals (persistent! thread))))))))

(defn combine-ranges
  [ranges]
  (reduce
    (fn [vs [[cl l] [cr r] :as n]]
      (let [[[pcl pl] [pcr pr]] (peek vs)]
        (if (and (= pr l) (not (= pcr cl :open)))
          (conj (pop vs) [[pcl pl] [cr r]])
          (conj vs n))))
    [] (combine-ranges* ranges)))

(defn- flip [c] (if (identical? c :open) :closed :open))

(defn flip-ranges
  ([ranges] (flip-ranges ranges c/v0 c/vmax))
  ([ranges v0 vmax]
   (let [vs (reduce
              (fn [vs [[cl l] [cr r]]]
                (-> vs
                    (assoc-in [(dec (count vs)) (count (peek vs))]
                              [(if (= l v0) cl (flip cl)) l])
                    (conj [[(if (= r vmax) cr (flip cr)) r]])))
              [[[:closed v0]]] ranges)]
     (assoc-in vs [(dec (count vs)) (count (peek vs))]
               [:closed vmax]))))

(defn intersect-ranges
  [& ranges]
  (let [n         (count ranges)
        ranges    (apply concatv ranges)
        orig-from (sort range-compare (map first ranges))
        res
        (loop [res  []
               from (rest orig-from)
               fp   (first orig-from)
               to   (sort range-compare-to (map peek ranges))
               i    1
               j    0]
          (let [tc (first to)]
            (if (seq from)
              (let [fc (first from)]
                (if (<= 0 ^long (range-compare fc tc))
                  (if (= i (+ j n))
                    (recur (conj res [fp tc]) (rest from) fc
                           (drop n to) (inc i) i)
                    (recur res (rest from) fc to (inc i) j))
                  (recur res (rest from) fc to (inc i) j)))
              (if (and (<= ^long (range-compare fp tc) 0) (= i (+ j n)))
                (conj res [fp tc])
                res))))]
    (when (seq res) res)))

(defn- add-range [m & rs]
  (let [old-range (:range m)]
    (assoc m :range (if old-range
                      (if-let [new-range (intersect-ranges old-range rs)]
                        new-range
                        :empty-range)
                      (combine-ranges rs)))))

(defn- prefix-max-string
  [^String prefix]
  (let [n (alength (.getBytes prefix))]
    (if (< n c/+val-bytes-wo-hdr+)
      (let [l  (- c/+val-bytes-wo-hdr+ n)
            ba (byte-array l)]
        (Arrays/fill ba (unchecked-byte 0xFF))
        (str prefix (String. ba)))
      prefix)))

(def ^:const wildm (int \%))
(def ^:const wilds (int \_))
(def ^:const max-string (b/text-ba->str c/max-bytes))

(defn- like-convert-range
  "turn wildcard-free prefix into range"
  [m ^String pattern not?]
  (let [wm-s (.indexOf pattern wildm)
        ws-s (.indexOf pattern wilds)]
    (cond
      (or (zero? wm-s) (zero? ws-s)) m
      ;; not-like w/ a wildcard-free pattern
      (== wm-s ws-s -1)
      (add-range m [[:closed ""] [:open pattern]]
                 [[:open pattern] [:closed max-string]])
      :else
      (let [min-s    (min wm-s ws-s)
            end      (if (== min-s -1) (max wm-s ws-s) min-s)
            prefix-s (subs pattern 0 end)
            prefix-e (prefix-max-string prefix-s)
            range    [[:closed prefix-s] [:closed prefix-e]]]
        (if not?
          (apply add-range m (flip-ranges [range] "" max-string))
          (add-range m range))))))

(defn- like-pattern-as-string
  "Used for plain text matching, e.g. as bounded val or range, not as FSM"
  [^String pattern escape]
  (let [esc (str (or escape \!))]
    (-> pattern
        (str/replace (str esc esc) esc)
        (str/replace (str esc "%") "%")
        (str/replace (str esc "_") "_"))))

(defn- wildcard-free-like-pattern
  [^String pattern {:keys [escape]}]
  (LikeFSM/isValid (.getBytes pattern) (or escape \!))
  (let [pstring (like-pattern-as-string pattern escape)]
    (when (and (not (str/includes? pstring "%"))
               (not (str/includes? pstring "_")))
      pstring)))

(defn- activate-var-pred
  [var clause]
  (when clause
    (if (fn? clause)
      clause
      (let [[f & args] clause
            idxs       (u/idxs-of #(= var %) args) ;; may appear more than once
            ni         (count idxs)
            idxs-arr   (int-array idxs)
            args-arr   (object-array args)
            call       (make-call (resolve-pred f nil))]
        (fn var-pred [x]
          (dotimes [i ni] (aset args-arr (aget idxs-arr i) x))
          (call args-arr))))))

(defn- add-pred
  ([old-pred new-pred]
   (add-pred old-pred new-pred false))
  ([old-pred new-pred or?]
   (if new-pred
     (if old-pred
       (if or?
         (fn [x] (or (old-pred x) (new-pred x)))
         (fn [x] (and (old-pred x) (new-pred x))))
       new-pred)
     old-pred)))

(defn- optimize-like
  [m pred [_ ^String pattern {:keys [escape]}] v not?]
  (let [pstring (like-pattern-as-string pattern escape)
        m'      (update m :pred add-pred (activate-var-pred v pred))]
    (like-convert-range m' pstring not?)))

(defn- inequality->range
  [m f args v]
  (let [args (vec args)
        ac-1 (dec (count args))
        i    ^long (u/index-of #(= % v) args)
        fa   (first args)
        pa   (peek args)]
    (case f
      <  (cond
           (zero? i)  (add-range m [[:closed c/v0] [:open pa]])
           (= i ac-1) (add-range m [[:open fa] [:closed c/vmax]])
           :else      (add-range m [[:open fa] [:open pa]]))
      <= (cond
           (zero? i)  (add-range m [[:closed c/v0] [:closed pa]])
           (= i ac-1) (add-range m [[:closed fa] [:closed c/vmax]])
           :else      (add-range m [[:closed fa] [:closed pa]]))
      >  (cond
           (zero? i)  (add-range m [[:open pa] [:closed c/vmax]])
           (= i ac-1) (add-range m [[:closed c/v0] [:open fa]])
           :else      (add-range m [[:open pa] [:open fa]]))
      >= (cond
           (zero? i)  (add-range m [[:closed pa] [:closed c/vmax]])
           (= i ac-1) (add-range m [[:closed c/v0] [:closed fa]])
           :else      (add-range m [[:closed pa] [:closed fa]])))))

(defn- range->inequality
  [v [[so sc :as s] [eo ec :as e]]]
  (cond
    (= s [:closed c/v0])
    (if (identical? eo :open) (list '< v ec) (list '<= v ec))
    (= e [:closed c/vmax])
    (if (identical? so :open) (list '< sc v) (list '<= sc v))
    :else
    (if (identical? so :open) (list '< sc v ec) (list '<= sc v ec))))

(defn- equality->range
  [m args]
  (let [c (some #(when-not (qu/free-var? %) %) args)]
    (add-range m [[:closed c] [:closed c]])))

(defn- in-convert-range
  [m [_ coll] not?]
  (assert (and (coll? coll) (not (map? coll)))
          "function `in` expects a collection")
  (apply add-range m
         (let [ranges (map (fn [v] [[:closed v] [:closed v]]) (sort coll))]
           (if not? (flip-ranges ranges) ranges))))

(defn- nested-pred
  [f args v]
  (let [len      (count args)
        fn-arr   (object-array len)
        args-arr (object-array args)
        call     (make-call (resolve-pred f nil))]
    (dotimes [i len]
      (let [arg (aget args-arr i)]
        (when (list? arg)
          (aset fn-arr i (if (some #(list? %) arg)
                           (nested-pred (first arg) (rest arg) v)
                           (activate-var-pred v arg))))))
    (fn [x]
      (dotimes [i len]
        (when-some [f (aget fn-arr i)]
          (aset args-arr i (f x))))
      (call args-arr))))

(defn- split-and-clauses
  "If pred is an (and ...) form where every arg is a predicate list,
  return a flat seq of those predicate lists; otherwise nil."
  [pred]
  (when (and (list? pred) (= 'and (first pred)))
    (let [args (rest pred)]
      (when (every? list? args)
        (mapcat (fn [arg]
                  (or (split-and-clauses arg) [arg]))
                args)))))

(defn- add-pred-clause
  [graph clause v]
  (let [pred       (first clause)
        and-clauses (split-and-clauses pred)
        preds      (or and-clauses [pred])
        apply-pred (fn [m pred]
                     (let [[f & args] pred]
                       (if (some list? pred)
                         (update m :pred add-pred (nested-pred f args v))
                         (case f
                           (< <= > >=) (inequality->range m f args v)
                           =           (equality->range m args)
                           like        (optimize-like m pred args v false)
                           not-like    (optimize-like m pred args v true)
                           in          (in-convert-range m args false)
                           not-in      (in-convert-range m args true)
                           (update m :pred add-pred (activate-var-pred v pred))))))]
    (w/postwalk
      (fn [m]
        (if (= (:var m) v)
          (reduce apply-pred m preds)
          m))
      graph)))

(defn- free->bound
  "cases where free var can be rewritten as bound:
    * like pattern is free of wildcards"
  [graph clause v]
  (w/postwalk
    (fn [m]
      (if-let [free (:free m)]
        (if-let [[new k]
                 (u/some-indexed
                   (fn [{:keys [var] :as old}]
                     (when (= v var)
                       (let [[f & args] (first clause)]
                         (when (= f 'like)
                           (let [[_ pattern opts] args]
                             (when-let [ps (wildcard-free-like-pattern
                                             pattern opts)]
                               (-> old
                                   (dissoc :var)
                                   (assoc :val ps))))))))
                   free)]
          (-> m
              (update :bound conjv new)
              (update :free u/vec-remove k))
          m)
        m))
    graph))

(defn- pushdown-predicates
  "optimization that pushes predicates down to value scans"
  [{:keys [parsed-q graph] :as context}]
  (let [gseq (tree-seq coll? seq graph)]
    (u/reduce-indexed
      (fn [c where i]
        (if-let [v (pushdownable where gseq)]
          (let [clause (nth (:qorig-where parsed-q) i)]
            (-> c
                (update :late-clauses #(remove #{clause} %))
                (update :opt-clauses conj clause)
                (update :graph #(free->bound % clause v))
                (update :graph #(add-pred-clause % clause v))))
          c))
      context (:qwhere parsed-q))))

(defn- estimate-round [x]
  (let [v (Math/ceil (double x))]
    (if (>= v (double Long/MAX_VALUE))
      Long/MAX_VALUE
      (long v))))

(defn- attr-var [{:keys [var]}] (or var '_))

(defn- build-graph
  "Split clauses, turn the group of clauses to be optimized into a query
  graph that looks like this:
  {$
    {?e  {:links [{:type :ref :tgt ?e0 :attr :friend}
                  {:type :val-eq :tgt ?e1 :var ?a :attrs {?e :age ?e1 :age}}]
          :mpath [:bound 0]
          :bound [{:attr :name :val \"Tom\" :count 5}]
          :free  [{:attr :age :var ?a :range [[:less-than 18]] :count 1089}
                  {:attr :school :var ?s :count 108 :pred [(.startsWith ?s \"New\")]}
                  {:attr :friend :var ?e0 :count 2500}]}
    ?e0 {:links [{:type :_ref :tgt ?e :attr :friend}]
         :mpath [:free 0]
         :free  [{:attr :age :var ?a :count 10890}]}
    ...
    }}

  Remaining clauses are in :late-clauses. "
  [context]
  (-> context
      split-clauses
      init-graph
      pushdown-predicates))

(defn- nillify [v] (if (or (identical? v c/v0) (identical? v c/vmax)) nil v))

(defn- range->start-end [[[_ lv] [_ hv]]] [(nillify lv) (nillify hv)])

(defn- range-count
  [db attr ranges ^long cap]
  (if (identical? ranges :empty-range)
    0
    (unreduced
      (reduce
        (fn [^long sum range]
          (let [s (+ sum (let [[lv hv] (range->start-end range)]
                           ^long (db/-index-range-size db attr lv hv)))]
            (if (< s cap) s (reduced cap))))
        0 ranges))))

(defn- count-node-datoms
  [^DB db {:keys [free bound] :as node}]
  (let [store (.-store db)]
    (reduce
      (fn [{:keys [mcount] :as node} [k i {:keys [attr val range]}]]
        (let [^long c (cond
                        (some? val) (av-size store attr val)
                        range       (range-count db attr range mcount)
                        :else       (db/-count db [nil attr nil] mcount))]
          (cond
            (zero? c)          (reduced (assoc node :mcount 0))
            (< c ^long mcount) (-> node
                                   (assoc-in [k i :count] c)
                                   (assoc :mcount c :mpath [k i]))
            :else              (assoc-in node [k i :count] c))))
      (assoc node :mcount Long/MAX_VALUE)
      (let [flat (fn [k m] (map-indexed (fn [i clause] [k i clause]) m))]
        (concat (flat :bound bound) (flat :free free))))))

(defn- count-known-e-datoms
  [db e {:keys [free] :as node}]
  (u/reduce-indexed
    (fn [{:keys [mcount] :as node} {:keys [attr]} i]
      (let [^long c (db/-count db [e attr nil] mcount)]
        (cond
          (zero? c)          (reduced (assoc node :mcount 0))
          (< c ^long mcount) (-> node
                                 (assoc-in [:free i :count] c)
                                 (assoc :mcount c :mpath [:free i]))
          :else              (assoc-in node [:free i :count] c))))
    (assoc node :mcount Long/MAX_VALUE) free))

(defn- count-datoms
  [db e node]
  (unreduced (if (int? e)
               (count-known-e-datoms db e node)
               (count-node-datoms db node))))

(defn- add-back-range
  [v {:keys [pred range]}]
  (if range
    (reduce
      (fn [p r]
        (if r
          (add-pred p (activate-var-pred v (range->inequality v r)) true)
          p))
      pred range)
    pred))

(defn- attrs-vec
  [attrs preds skips fidxs]
  (mapv (fn [a p f]
          [a (cond-> {:pred p :skip? false :fidx nil}
               (skips a) (assoc :skip? true)
               f         (assoc :fidx f :skip? true))])
        attrs preds fidxs))

(defn- aid [db] #(((db/-schema db) %) :db/aid))

(defn- init-steps
  [db e node single?]
  (let [{:keys [bound free mpath mcount]}            node
        {:keys [attr var val range pred] :as clause} (get-in node mpath)

        know-e? (int? e)
        no-var? (or (not var) (qu/placeholder? var))

        init (cond-> (map->InitStep
                       {:attr attr :vars [e] :out [e]
                        :mcount (:count clause)})
               var     (assoc :pred pred
                              :vars (cond-> [e]
                                      (not no-var?) (conj var))
                              :range range)
               val     (assoc :val val)
               know-e? (assoc :know-e? true)
               true    (#(let [vars (:vars %)]
                           (assoc % :cols (if (= 1 (count vars))
                                            [e]
                                            [e #{attr var}])
                                  :strata [(set vars)]
                                  :seen-or-joins #{})))

               (not single?)
               (#(if (< ^long c/init-exec-size-threshold ^long mcount)
                   (assoc % :sample (-sample % db nil))
                   (assoc % :result (-execute % db nil)))))]
    (cond-> [init]
      (< 1 (+ (count bound) (count free)))
      (conj
        (let [[k i]   mpath
              bound1  (mapv (fn [{:keys [val] :as b}]
                              (-> b
                                  (update :pred add-pred #(= val %))
                                  (assoc :var (gensym "?bound"))))
                            (if (= k :bound) (u/vec-remove bound i) bound))
              all     (->> (concatv bound1
                                    (if (= k :free) (u/vec-remove free i) free))
                           (sort-by (fn [{:keys [attr]}] ((aid db) attr))))
              attrs   (mapv :attr all)
              vars    (mapv attr-var all)
              skips   (cond-> (set (sequence
                                     (comp (map (fn [a v]
                                               (when (or (= v '_)
                                                         (qu/placeholder? v))
                                                 a)))
                                        (remove nil?))
                                     attrs vars))
                        no-var? (conj attr))
              preds   (mapv add-back-range vars all)
              attrs-v (attrs-vec attrs preds skips (repeat nil))
              cols    (into (:cols init)
                            (sequence
                              (comp (map (fn [a v] (when-not (skips a) #{a v})))
                                 (remove nil?))
                              attrs vars))
              strata  (conj (:strata init) (set vars))
              ires    (:result init)
              isp     (:sample init)
              step    (MergeScanStep. 0 attrs-v vars [e] [e] cols strata
                                      #{} nil nil)]
          (cond-> step
            ires (assoc :result (-execute step db ires))
            isp  (assoc :sample (-sample step db isp))))))))

(defn- n-items
  [attrs-v k]
  (reduce
    (fn [^long c [_ m]] (if (m k) (inc c) c))
    0 attrs-v))

(defn- estimate-scan-v-size
  [^long e-size steps]
  (cond+
    (= (count steps) 1) e-size ; no merge step

    :let [{:keys [know-e?] res1 :result sp1 :sample} (first steps)
          {:keys [attrs-v result sample]} (peek steps)]

    know-e? (count attrs-v)

    :else
    (estimate-round
      (* e-size (double
                  (cond
                    result (let [s (.size ^List result)]
                             (if (< 0 s)
                               (/ s (.size ^List res1))
                               c/magic-scan-ratio))
                    sample (let [s (.size ^List sample)]
                             (if (< 0 s)
                               (/ s (.size ^List sp1))
                               c/magic-scan-ratio))))))))

(defn- factor
  [magic ^long n]
  (if (zero? n) 1 ^long (estimate-round (* ^double magic n))))

(defn- estimate-scan-v-cost
  [{:keys [attrs-v vars]} ^long size]
  (* size
     ^double c/magic-cost-merge-scan-v
     ^long (factor c/magic-cost-var (count vars))
     ^long (factor c/magic-cost-pred (n-items attrs-v :pred))
     ^long (factor c/magic-cost-fidx (n-items attrs-v :fidx))))

(defn- estimate-base-cost
  [{:keys [mcount]} steps]
  (let [{:keys [pred]} (first steps)
        init-cost      (estimate-round
                         (cond-> (* ^double c/magic-cost-init-scan-e
                                    ^long mcount)
                           pred (* ^double c/magic-cost-pred)))]
    (if (< 1 (count steps))
      (+ ^long init-cost ^long (estimate-scan-v-cost (peek steps) mcount))
      init-cost)))

(defn- base-plan
  ([db nodes e]
   (base-plan db nodes e false))
  ([db nodes e single?]
   (let [node   (get nodes e)
         mcount (:mcount node)]
     (when-not (zero? ^long mcount)
       (let [isteps (init-steps db e node single?)]
         (if single?
           (Plan. isteps nil nil 0)
           (Plan. isteps
                  (estimate-base-cost node isteps)
                  (estimate-scan-v-size mcount isteps)
                  0)))))))

(defn- writing? [db] (l/writing? (.-lmdb ^Store (.-store ^DB db))))

(defn- update-nodes
  [db nodes]
  (if (= (count nodes) 1)
    (let [[e node] (first nodes)] {e (count-datoms db e node)})
    (let [f (fn [e] [e (count-datoms db e (get nodes e))])]
      (into {} (if (writing? db)
                 (map f (keys nodes))
                 (map+ f (keys nodes)))))))

(defn- build-base-plans
  [db nodes component]
  (let [f (fn [e] [[e] (base-plan db nodes e)])]
    (into {} (if (writing? db)
               (map f component)
               (map+ f component)))))

(defn- find-index
  [a-or-v cols]
  (when a-or-v
    (u/index-of (fn [x] (if (set? x) (x a-or-v) (= x a-or-v))) cols)))

(defn- merge-scan-step
  [db last-step index new-key new-steps]
  (let [in       (:out last-step)
        out      (if (set? in) (set new-key) new-key)
        lcols    (:cols last-step)
        lstrata  (:strata last-step)
        ncols    (:cols (peek new-steps))
        [s1 s2]  new-steps
        val1     (:val s1)
        [_ v1]   (:vars s1)
        a1       (:attr s1)
        ip       (cond-> (add-back-range v1 s1)
                   (some? val1) (add-pred #(= % val1)))
        attrs-v2 (:attrs-v s2)
        get-a    (fn [coll] (some #(when (keyword? %) %) coll))
        [attrs-v vars cols]
        (reduce
          (fn [[attrs-v vars cols] col]
            (let [v (some #(when (symbol? %) %) col)]
              (if (and ip (= v v1))
                [attrs-v vars cols]
                (let [a (get-a col)
                      p (some #(when (= a (first %)) (:pred (peek %))) attrs-v2)]
                  (if-let [f (find-index v lcols)]
                    [(conj attrs-v [a {:pred p :skip? true :fidx f}]) vars cols]
                    [(conj attrs-v [a {:pred  p
                                       :skip? (if (some #(when (= a (first %))
                                                           (:skip? (peek %)))
                                                        attrs-v2)
                                                true false)
                                       :fidx  nil}])
                     (conj vars v) (conj cols col)])))))
          (if (or ip (nil? v1))
            [[[a1 {:pred  ip
                   :skip? (if (and v1 (find-index v1 ncols)) false true)
                   :fidx  nil}]]
             (if v1 [v1] [])
             (if v1 [#{a1 v1}] [])]
            [[] [] []])
          (rest ncols))
        fcols    (into lcols (sort-by (comp (aid db) get-a) cols))
        strata   (conj lstrata (set vars))
        lseen    (:seen-or-joins last-step)]
    (MergeScanStep. index attrs-v vars in out fcols strata lseen nil nil)))

(defn- index-by-link
  [cols link-e link]
  (case (:type link)
    :ref     (or (find-index (:tgt link) cols)
                 (find-index (:attr link) cols))
    :_ref    (find-index link-e cols)
    :val-eq  (or (find-index (:var link) cols)
                 (find-index ((:attrs link) link-e) cols))
    ;; For or-join, return index where tgt will be after step adds free-vars
    ;; and tgt
    :or-join (+ (count cols) (count (:free-vars link)))))

(defn- enrich-cols
  [cols index attr]
  (let [pa (cols index)]
    (mapv (fn [e] (if (and (= e pa) (set? e)) (conj e attr) e)) cols)))

(defn- col-var
  [col]
  (if (set? col)
    (some #(when (symbol? %) %) col)
    col))

(defn- col-attrs
  [col]
  (if (set? col)
    (into #{} (filter keyword?) col)
    #{}))

(defn- merge-join-cols
  "Merge input and target cols for hash join output, preserving input order.
   Returns [merged-cols new-vars]."
  [in-cols tgt-cols]
  (let [in-vars    (mapv col-var in-cols)
        tgt-vars   (mapv col-var tgt-cols)
        tgt-map    (zipmap tgt-vars tgt-cols)
        in-var-set (set in-vars)
        merged-in  (mapv (fn [col v]
                           (if-let [tcol (tgt-map v)]
                             (let [attrs (set/union (col-attrs col)
                                                    (col-attrs tcol))]
                               (if (seq attrs)
                                 (conj attrs v)
                                 v))
                             col))
                         in-cols in-vars)
        new-cols   (reduce (fn [acc [v col]]
                             (if (in-var-set v) acc (conj acc col)))
                           [] (map vector tgt-vars tgt-cols))
        new-vars   (set/difference (set tgt-vars) in-var-set)]
    [(into merged-in new-cols) new-vars]))

(defn- link-step
  [type last-step index attr tgt new-key]
  (let [in      (:out last-step)
        out     (if (set? in) (set new-key) new-key)
        lcols   (:cols last-step)
        lstrata (:strata last-step)
        lseen   (:seen-or-joins last-step)
        fidx    (find-index tgt lcols)
        cols    (cond-> (enrich-cols lcols index attr)
                  (nil? fidx) (conj tgt))]
    [(LinkStep. type index attr tgt fidx in out cols (conj lstrata #{tgt}) lseen)
     (or fidx (dec (count cols)))]))

(defn- rev-ref-plan
  [db last-step index {:keys [type attr tgt]} new-key new-steps]
  (let [[step n-index] (link-step type last-step index attr tgt new-key)]
    (if (= 1 (count new-steps))
      [step]
      [step (merge-scan-step db step n-index new-key new-steps)])))

(defn- val-eq-plan
  [db last-step index {:keys [type attrs tgt]} new-key new-steps]
  (let [attr           (attrs tgt)
        [step n-index] (link-step type last-step index attr tgt new-key)]
    (if (= 1 (count new-steps))
      [step]
      [step (merge-scan-step db step n-index new-key new-steps)])))

(defn- hash-join-plan
  [_db {:keys [steps cost size]} link-e link new-key
   new-base-plan result-size]
  (let [last-step       (peek steps)
        in              (:out last-step)
        out             (if (set? in) (set new-key) new-key)
        lcols           (:cols last-step)
        lstrata         (:strata last-step)
        lseen           (:seen-or-joins last-step)
        tgt-steps       (:steps new-base-plan)
        in-size         (or size 0)
        tgt-size        (or (:size new-base-plan) 0)
        tgt-cols        (:cols (peek tgt-steps))
        [cols new-vars] (merge-join-cols lcols tgt-cols)
        step            (HashJoinStep. link link-e in out lcols cols
                                       (conj lstrata new-vars) lseen tgt-steps
                                       in-size tgt-size)
        base-cost       (or (:cost new-base-plan) 0)
        join-cost       (estimate-hash-join-cost in-size tgt-size)]
    (Plan. [step]
           (+ ^long cost ^long base-cost ^long join-cost)
           result-size
           (- ^long (find-index link-e (:strata last-step))))))

(defn- or-join-plan*
  [db sources rules last-step
   {:keys [clause bound-var free-vars tgt tgt-attr]} new-key new-base]
  (let [in        (:out last-step)
        out       (if (set? in) (set new-key) new-key)
        lcols     (:cols last-step)
        lstrata   (:strata last-step)
        lseen     (:seen-or-joins last-step)
        bound-idx (find-index bound-var lcols)
        or-cols   (-> lcols (into free-vars) (conj tgt))
        or-seen   (conj lseen clause)
        or-step   (OrJoinStep. clause
                               bound-var
                               bound-idx
                               free-vars
                               tgt
                               tgt-attr
                               sources
                               rules
                               in out or-cols
                               (conj lstrata #{tgt})
                               or-seen)
        tgt-idx   (dec (count or-cols))]
    (if new-base
      (let [new-steps (:steps new-base)]
        [or-step (merge-scan-step db or-step tgt-idx new-key new-steps)])
      [or-step])))

(defn- count-init-follows
  [^DB db tuples attr index]
  (let [store (.-store db)]
    (rd/fold
      +
      (rd/map #(av-size store attr (aget ^objects % index))
              (p/remove-end-scan tuples)))))

(defn- count-init-follows-stats
  [^DB db tuples attr index]
  (let [store    (.-store db)
        ^List ts (p/remove-end-scan tuples)
        n        (.size ts)]
    (loop [i     0
           sum   0.0
           sumsq 0.0
           mx    0.0]
      (if (< i n)
        (let [^objects t (.get ts i)
              f          (double (av-size store attr (aget t index)))]
          (recur (u/long-inc i)
                 (+ sum f)
                 (+ sumsq (* f f))
                 (if (> f mx) f mx)))
        {:n       n
         :sum     sum
         :sumsq   sumsq
         :max-val mx}))))

(defn- link-ratio-key
  [link-e {:keys [type attr attrs tgt]}]
  (case type
    :val-eq [type (attrs link-e) (attrs tgt)]
    :_ref   [type attr]
    [type attr]))

(defn- estimate-link-size
  [db link-e {:keys [type attr attrs tgt var]} ^ConcurrentHashMap ratios
   prev-size prev-plan index]
  (let [prev-steps              (:steps prev-plan)
        attr                    (or attr (attrs tgt))
        ratio-key               (link-ratio-key link-e {:type  type
                                                        :attr  attr
                                                        :var   var
                                                        :attrs attrs
                                                        :tgt   tgt})
        {:keys [result sample]} (peek prev-steps)
        ^long ssize             (if sample (.size ^List sample) 0)
        ^long rsize             (if result (.size ^List result) 0)]
    (estimate-round
      (cond
        (< 0 ssize)
        (let [{:keys [^long n ^double sum ^double sumsq ^double max-val]}
              (count-init-follows-stats db sample attr index)

              mean       (if (pos? n) (/ sum (double n)) 0.0)
              variance   (if (pos? n)
                           (max 0.0 (- (/ sumsq (double n)) (* mean mean)))
                           0.0)
              cv2        (if (pos? mean) (/ variance (* mean mean)) 0.0)
              base-ratio (double (db/-default-ratio db attr))
              k-eff      (* (double c/link-estimate-prior-size)
                            (+ 1.0 (* (double c/link-estimate-var-alpha) cv2)))
              blended    (if (pos? (+ (double n) k-eff))
                           (/ (+ sum (* k-eff base-ratio))
                              (+ (double n) k-eff))
                           base-ratio)
              ub         (if (pos? n)
                           (min (+ mean (/ (- max-val mean)
                                           (Math/sqrt (double n))))
                                (* mean (double c/link-estimate-max-multi)))
                           base-ratio)
              ratio      (max base-ratio blended ub (double c/magic-link-ratio))]
          (.put ratios ratio-key ratio)
          (* (double prev-size) ratio))

        (< 0 rsize)
        (let [^long size (count-init-follows db result attr index)
              ratio      (/ size rsize)]
          (.put ratios ratio-key ratio)
          size)

        (.containsKey ratios ratio-key)
        (* ^long prev-size ^double (.get ratios ratio-key))

        :else
        (let [ratio (db/-default-ratio db attr)]
          (.put ratios ratio-key ratio)
          (* ^long prev-size ^double ratio))))))

(defn- count-or-join-follows
  "Execute or-join on input tuples and count output size."
  [db sources rules tuples {:keys [clause bound-var free-vars tgt-attr]}
   bound-idx]
  (let [result (or-join-execute-link db sources rules tuples clause bound-var
                                     bound-idx free-vars tgt-attr)]
    (.size ^List result)))

(defn- estimate-or-join-size
  [db sources rules ^ConcurrentHashMap ratios prev-plan link]
  (let [prev-size               (:size prev-plan)
        prev-steps              (:steps prev-plan)
        last-step               (peek prev-steps)
        bound-idx               (find-index (:bound-var link) (:cols last-step))
        ratio-key               [:or-join (:bound-var link) (:tgt link)]
        {:keys [result sample]} last-step
        ^long ssize             (if sample (.size ^List sample) 0)
        ^long rsize             (if result (.size ^List result) 0)]
    (estimate-round
      (cond
        (< 0 ssize)
        (let [^long size (count-or-join-follows db sources rules sample link
                                                bound-idx)
              ratio      (max (double (/ size ssize))
                              ^double c/magic-or-join-ratio)]
          (.put ratios ratio-key ratio)
          (* ^long prev-size ratio))

        (< 0 rsize)
        (let [^long size (count-or-join-follows db sources rules result link
                                                bound-idx)
              ratio      (/ size rsize)]
          (.put ratios ratio-key ratio)
          size)

        (.containsKey ratios ratio-key)
        (* ^long prev-size ^double (.get ratios ratio-key))

        :else
        (do (.put ratios ratio-key c/magic-or-join-ratio)
            (* ^long prev-size ^double c/magic-or-join-ratio))))))

(defn- estimate-join-size
  [db sources rules link-e link ratios prev-plan index new-base-plan]
  (let [prev-size (:size prev-plan)
        steps     (:steps new-base-plan)]
    (case (:type link)
      :ref     [nil (estimate-scan-v-size prev-size steps)]
      :or-join (let [or-size (estimate-or-join-size db sources rules ratios
                                                    prev-plan link)]
                 ;; or-join doesn't have new-base-plan steps to merge
                 [or-size or-size])
      ;; :_ref and :val-eq
      (let [e-size (estimate-link-size db link-e link ratios prev-size
                                       prev-plan index)]
        [e-size (estimate-scan-v-size e-size steps)]))))

(defn- estimate-link-cost
  [^long outer-size ^long result-size]
  (estimate-round
    (+ (* outer-size ^double c/magic-cost-link-probe)
       (* result-size ^double c/magic-cost-link-retrieval))))

(defn- estimate-hash-join-cost
  [^long left-size ^long right-size]
  (estimate-round (* ^double c/magic-cost-hash-join
                     (+ left-size right-size))))

(defn- estimate-e-plan-cost
  [prev-size e-size cur-steps]
  (let [step1 (first cur-steps)]
    (if (= 1 (count cur-steps))
      (if (identical? (-type step1) :merge)
        (estimate-scan-v-cost step1 prev-size)
        (estimate-link-cost prev-size e-size))
      (+ ^long (estimate-link-cost prev-size e-size)
         ^long (estimate-scan-v-cost (peek cur-steps) e-size)))))

(defn- e-plan
  [db {:keys [steps cost size]} index link-e link new-key new-base-plan e-size
   result-size]
  (let [new-steps (:steps new-base-plan)
        last-step (peek steps)
        cur-steps
        (case (:type link)
          :ref    [(merge-scan-step db last-step index new-key new-steps)]
          :_ref   (rev-ref-plan db last-step index link new-key new-steps)
          :val-eq (val-eq-plan db last-step index link new-key new-steps))]
    (Plan. cur-steps
           (+ ^long cost ^long (estimate-e-plan-cost size e-size cur-steps))
           result-size
           (- ^long (find-index link-e (:strata last-step))))))

(defn- compare-plans
  "Compare two plans. Prefer lower cost, then lower size as tiebreaker."
  [p1 p2]
  (let [c1 ^long (:cost p1)
        c2 ^long (:cost p2)]
    (if (= c1 c2)
      (if (< ^long (:size p2) ^long (:size p1)) p2 p1)
      (if (< ^long c2 ^long c1) p2 p1))))

(defn- or-join-plan
  [base-plans new-e db sources rules ratios prev-plan link last-step new-key
   link-e]
  (let [new-base  (base-plans [new-e])
        or-size   (estimate-or-join-size db sources rules ratios prev-plan link)
        cur-steps (or-join-plan* db sources rules last-step link new-key
                                 new-base)
        or-cost   (estimate-e-plan-cost (:size prev-plan) or-size cur-steps)]
    (Plan. cur-steps
           (+ ^long (:cost prev-plan) ^long or-cost)
           or-size
           (- ^long (find-index link-e (:strata last-step))))))

(defn- binary-plan*
  [db sources rules base-plans ratios prev-plan link-e new-e link new-key]
  (let [last-step (peek (:steps prev-plan))
        index     (index-by-link (:cols last-step) link-e link)
        link-type (:type link)]
    (if (identical? :or-join link-type)
      (or-join-plan base-plans new-e db sources rules ratios prev-plan link
                    last-step new-key link-e)
      (let [new-base (base-plans [new-e])
            [e-size result-size]
            (estimate-join-size db sources rules link-e link ratios prev-plan
                                index new-base)]
        (if (and (#{:_ref :val-eq} link-type) new-base)
          (let [link-plan (e-plan db prev-plan index link-e link new-key
                                  new-base e-size result-size)]
            (if (< ^long (:size prev-plan) ^long c/hash-join-min-input-size)
              link-plan
              (let [hash-plan (hash-join-plan db prev-plan link-e link new-key
                                              new-base result-size)]
                (compare-plans link-plan hash-plan))))
          (e-plan db prev-plan index link-e link new-key new-base e-size
                  result-size))))))

(defn- binary-plan
  [db sources rules nodes base-plans ratios prev-plan link-e new-e new-key]
  (let [last-step     (peek (:steps prev-plan))
        seen-or-joins (or (:seen-or-joins last-step) #{})
        links         (get-in nodes [link-e :links])
        filtered-links
        (into []
              (comp
                (filter #(= new-e (:tgt %)))
                (filter #(or (not= :or-join (:type %))
                             (not (contains? seen-or-joins (:clause %))))))
              links)
        candidates
        (mapv #(binary-plan* db sources rules base-plans ratios prev-plan
                             link-e new-e % new-key)
              filtered-links)]
    (when (seq candidates)
      (apply u/min-key-comp (juxt :recency :cost :size) candidates))))

(defn- plans
  [db sources rules nodes pairs base-plans prev-plans ratios]
  (apply
    u/merge-with compare-plans
    (mapv
      (fn [[prev-key prev-plan]]
        (let [prev-key-set (set prev-key)]
          (persistent!
            (reduce
              (fn [t [link-e new-e]]
                (if (and (prev-key-set link-e) (not (prev-key-set new-e)))
                  (let [new-key  (conj prev-key new-e)
                        cur-plan (t new-key)
                        new-plan
                        (binary-plan db sources rules nodes base-plans
                                     ratios prev-plan link-e new-e new-key)]
                    (if new-plan
                      (if (or (nil? cur-plan)
                              (identical? new-plan
                                          (compare-plans cur-plan new-plan)))
                        (assoc! t new-key new-plan)
                        t)
                      t))
                  t))
              (transient {}) pairs))))
      prev-plans)))

(defn- connected-pairs
  "Get all connected pairs in a component.
   All links (including or-join) use :tgt pointing to entity nodes."
  [nodes component]
  (let [pairs (volatile! #{})]
    (doseq [e    component
            link (get-in nodes [e :links])]
      (vswap! pairs conj [e (:tgt link)]))
    @pairs))

(defn- shrink-space
  [plans]
  (persistent!
    (reduce-kv
      (fn [m k ps]
        (assoc! m k (-> (peek (apply min-key (fn [p] (:cost (peek p))) ps))
                        (update :steps (fn [ss]
                                         (if (= 1 (count ss))
                                           [(update (first ss) :out set)]
                                           [(first ss)
                                            (update (peek ss) :out set)]))))))
      (transient {}) (group-by (fn [p] (set (nth p 0))) plans))))

(defn- trace-steps
  [^List tables ^long n-1]
  (let [final-plans (vals (.get tables n-1))]
    (reduce
      (fn [plans i]
        (cons ((.get tables i) (:in (first (:steps (first plans))))) plans))
      [(apply min-key :cost final-plans)]
      (range (dec n-1) -1 -1))))

(defn- plan-component
  [db sources rules nodes component]
  (let [n (count component)]
    (if (= n 1)
      [(base-plan db nodes (first component) true)]
      (let [base-plans (build-base-plans db nodes component)]
        (if (some nil? (vals base-plans))
          [nil]
          (let [pairs  (connected-pairs nodes component)
                tables (FastList. n)
                ratios (ConcurrentHashMap.)
                n-1    (dec n)
                pn     ^long (min (long c/plan-search-max)
                                  (long (u/n-permutations n 2)))]
            (.add tables base-plans)
            (dotimes [i n-1]
              (let [plans (plans db sources rules nodes pairs base-plans
                                 (.get tables i) ratios)]
                (if (< pn (count plans))
                  (.add tables (shrink-space plans))
                  (.add tables plans))))
            (trace-steps tables n-1)))))))

(defn- dfs
  [graph start]
  (loop [stack [start] visited #{}]
    (if (empty? stack)
      visited
      (let [v     (peek stack)
            stack (pop stack)]
        (if (visited v)
          (recur stack visited)
          (let [links     (:links (graph v))
                neighbors (map :tgt links)]
            (recur (into stack neighbors) (conj visited v))))))))

(defn- connected-components
  [graph]
  (loop [vertices (keys graph) components []]
    (if (empty? vertices)
      components
      (let [component (dfs graph (first vertices))]
        (recur (remove component vertices)
               (conj components component))))))

(defn- build-plan*
  [db sources rules nodes]
  (let [cc (connected-components nodes)]
    (if (= 1 (count cc))
      [(plan-component db sources rules nodes (first cc))]
      (map+ #(plan-component db sources rules nodes %) cc))))

(defn- strip-step-result
  [step]
  (let [step (if (instance? HashJoinStep step)
               (update step :tgt-steps (fn [steps]
                                         (mapv strip-step-result steps)))
               step)]
    (assoc step :result nil :sample nil)))

(defn- strip-result
  [plans]
  (mapv (fn [plan-vec]
          (mapv #(update % :steps (fn [steps]
                                    (mapv strip-step-result steps)))
                plan-vec))
        plans))

(defn- build-plan
  "Generate a query plan that looks like this:

  [{:op :init :attr :name :val \"Tom\" :out #{?e} :vars [?e]
    :cols [?e]}
   {:op :merge-scan  :attrs [:age :friend] :preds [(< ?a 20) nil]
    :vars [?a ?f] :in #{?e} :index 0 :out #{?e} :cols [?e :age :friend]}
   {:op :link :attr :friend :var ?e1 :in #{?e} :index 2
    :out #{?e ?e1} :cols [?e :age :friend ?e1]}
   {:op :merge-scan :attrs [:name] :preds [nil] :vars [?n] :index 3
    :in #{?e ?e1} :out #{?e ?e1} :cols [?e :age :friend ?e1 :name]}]

  :op here means step type.
  :result-set will be #{} if there is any clause that matches nothing."
  [{:keys [graph sources rules] :as context}]
  (if graph
    (unreduced
      (reduce-kv
        (fn [c src nodes]
          (let [^DB db (sources src)
                k      [(.-store db) nodes]]
            (if-let [cached (.get ^LRUCache *plan-cache* k)]
              (assoc-in c [:plan src] cached)
              (let [nodes (update-nodes db nodes)
                    plans (if (< 1 (count nodes))
                            (build-plan* db sources rules nodes)
                            [[(base-plan db nodes (ffirst nodes) true)]])]
                (if (some #(some nil? %) plans)
                  (reduced (assoc c :result-set #{}))
                  (do (.put ^LRUCache *plan-cache* k (strip-result plans))
                      (assoc-in c [:plan src] plans)))))))
        context graph))
    context))

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
                                  (intersect-ranges old-range new-ranges)
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
                        (intersect-ranges old-range new-range)
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
        col-idx     (find-index link-e in-cols)
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
          col-idx     (find-index link-e in-cols)
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

(defn- pipelining
  [context db attrs steps n]
  (let [n-1    (dec ^long n)
        tuples (FastList. (int c/init-exec-size-threshold))
        pipes  (object-array (repeatedly n-1 #(if *explain*
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
                          #(try
                             (work step i)
                             (catch Throwable e
                               (raise "Error in executing step" i e
                                      {:step step}))
                             (finally
                               (finish i))))
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
        attrs (cols->attrs (:cols (peek steps)))]
    (condp = n
      1 (let [tuples (-execute (first steps) db nil)]
          (save-intermediates context steps nil tuples)
          (r/relation! attrs tuples))
      2 (let [src    (-execute (first steps) db nil)
              tuples (-execute (peek steps) db src)]
          (save-intermediates context steps (object-array [src]) tuples)
          (r/relation! attrs tuples))
      (pipelining context db attrs steps n))))

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

(defn- plan-explain
  []
  (when *explain*
    (let [{:keys [^long parsing-time ^long building-time]} @*explain*]
      (vswap! *explain* assoc :planning-time
              (- ^long (System/nanoTime)
                 (+ ^long *start-time* parsing-time building-time))))))

(defn- build-explain
  []
  (when *explain*
    (let [{:keys [^long parsing-time]} @*explain*]
      (vswap! *explain* assoc :building-time
              (- ^long (System/nanoTime)
                 (+ ^long *start-time* parsing-time))))))

;; TODO improve plan cache
(defn- planning
  [context]
  (-> context
      build-graph
      ((fn [c] (build-explain) c))
      build-plan))

(defn -q
  [context run?]
  (binding [qu/*implicit-source* (get (:sources context) '$)]
    (let [{:keys [result-set] :as context} (planning context)]
      (if (= result-set #{})
        (do (plan-explain) context)
        (as-> context c
          (do (plan-explain) c)
          (if run? (execute-plan c) c)
          (if run? (reduce resolve-clause c (:late-clauses c)) c))))))

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
   (when *explain* (vswap! *explain* assoc :result result)))
  ([{:keys [graph result-set plan opt-clauses late-clauses run?] :as context}]
   (when *explain*
     (let [{:keys [^long planning-time ^long parsing-time ^long building-time]}
           @*explain*

           et  (double (/ (- ^long (System/nanoTime)
                             (+ ^long *start-time* planning-time
                                parsing-time building-time))
                          1000000))
           bt  (double (/ building-time 1000000))
           plt (double (/ planning-time 1000000))
           pat (double (/ parsing-time 1000000))
           ppt (double (/ (+ parsing-time building-time planning-time)
                          1000000))]
       (vswap! *explain* assoc
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

          [parsed-q inputs] (plugin-inputs parsed-q inputs)

          context
          (-> (Context. parsed-q [] {} {} [] nil nil nil nil
                        (volatile! {}) true nil)
              (resolve-ins inputs)
              (resolve-redudants)
              (rules/rewrite)
              (rewrite-unused-vars)
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
              {:all? false
               :attrs (into (:attrs x #{}) (:attrs y #{}))}))
          (pattern-deps [parsed-q]
            (let [patterns (dp/collect #(instance? Pattern %) (:qwhere parsed-q))]
              (loop [ps      patterns
                     attrs   (transient #{})
                     all?    false]
                (cond
                  all?
                  {:all? true}

                  (empty? ps)
                  {:all? false :attrs (persistent! attrs)}

                  :else
                  (let [^Pattern p   (first ps)
                        attr-term (nth (:pattern p) 1 nil)]
                    (cond
                      (instance? Constant attr-term)
                      (recur (rest ps) (conj! attrs (:value ^Constant attr-term))
                             false)

                      ;; Variable / placeholder in attribute position means query may
                      ;; touch arbitrary attributes.
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
  (when *explain*
    (vswap! *explain* assoc :parsing-time
            (- (System/nanoTime) ^long *start-time*))))

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
      (let [[parsed-q inputs] (plugin-inputs parsed-q inputs)]
        (-> (Context. parsed-q [] {} {} [] nil nil nil nil (volatile! {})
                      false nil)
            (resolve-ins inputs)
            (resolve-redudants)
            (rules/rewrite)
            (rewrite-unused-vars)
            (-q false)
            (result-explain))))))

(defn- explain*
  [{:keys [run?] :or {run? false}} & args]
  (binding [*explain*    (volatile! {})
            *cache?*     false
            *start-time* (System/nanoTime)]
    (if run?
      (do (apply perform args) @*explain*)
      (do (apply plan-only args)
          (dissoc @*explain* :actual-result-size :execution-time)))))

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
