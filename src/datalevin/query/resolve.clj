;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.resolve
  "Clause resolution."
  (:refer-clojure :exclude [update assoc])
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [datalevin.built-ins :as built-ins]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.inline :refer [update assoc]]
   [datalevin.join :as j]
   [datalevin.parser :as dp]
   [datalevin.pipe :as p]
   [datalevin.query.tuple :as qtuple]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.rules :as rules]
   [datalevin.util :as u :refer [raise concatv]])
  (:import
   [java.util HashMap HashSet IdentityHashMap List]
   [datalevin.parser BindColl BindIgnore BindScalar BindTuple RulesVar SrcVar]
   [datalevin.relation Relation]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defrecord OrJoinLink [type tgt clause bound-var free-vars tgt-attr source])

(defrecord Clause [attr val var range count pred])

(def ^:dynamic *resolver-mode*
  "Controls host function resolution for query predicates/functions.

  :embedded preserves same-process Datalevin behavior and may resolve host vars.
  :server-safe is for client/server queries and only allows built-in query
  functions, sandboxed inter-fn values, query context values that are not used
  as call targets, and UDFs reached through the built-in udf function."
  :embedded)

(defn server-safe-resolver?
  []
  (= :server-safe *resolver-mode*))

(declare resolve-clause)

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

(def tuple-producing-fns
  "Set of function symbols that produce tuples and can benefit from
  knowing which indices are needed."
  #{'fulltext 'idoc-match 'vec-neighbors 'embedding-neighbors})

(defn- tuple-list->rel
  [binding ^List tuples
   {:keys [attrs source-attrs needed source-width output-width]}]
  (let [size         (.size tuples)
        source-width (long source-width)
        output-width (long output-width)]
    (if (zero? size)
      (empty-rel binding)
      (let [t0 (.get tuples 0)]
        (if (u/array? t0)
          (let [^objects t0 t0]
            (when (< (alength t0) source-width)
              (raise "Not enough elements in a collection " tuples
                     " to bind tuple " (dp/source binding)
                     {:error   :query/binding
                      :value   tuples
                      :binding (dp/source binding)}))
            (r/relation! source-attrs tuples))
          (let [^ints src-idxs
                (or needed (int-array (range source-width)))
                res (FastList. size)]
            (dotimes [i size]
              (let [row (.get tuples i)]
                (when-not (u/seqable? row)
                  (raise "Cannot bind value " row " to tuple "
                         (dp/source (:binding binding))
                         {:error   :query/binding
                          :value   row
                          :binding (dp/source (:binding binding))}))
                (when (< (count row) source-width)
                  (raise "Not enough elements in a collection " row
                         " to bind tuple "
                         (dp/source (:binding binding))
                         {:error   :query/binding
                          :value   row
                          :binding (dp/source (:binding binding))}))
                (let [tuple (object-array output-width)]
                  (dotimes [j output-width]
                    (aset tuple j (nth row (aget src-idxs j))))
                  (.add res tuple))))
            (r/relation! attrs res)))))))

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

      (instance? BindScalar (:binding binding))
      (r/relation! {(get-in binding [:binding :variable :symbol]) 0}
                   (r/vertical-tuples coll))

      (and (instance? java.util.List coll)
           (instance? BindTuple (:binding binding)))
      (if-let [projection
               (qtuple/tuple-binding-projection (:binding binding))]
        (tuple-list->rel binding coll projection)
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
      (reduce j/hash-join
              (map #(in->rel %1 %2) (:bindings binding) coll)))))

(defn resolve-ins
  [context values]
  (loop [context  context
         bindings (seq (get-in context [:parsed-q :qin]))
         values   (seq values)]
    (if-some [binding (first bindings)]
      (let [value (when values (first values))
            context
            (cond
              (and (instance? BindScalar binding)
                   (instance? SrcVar (:variable binding)))
              (update context :sources assoc (get-in binding [:variable :symbol])
                      value)

              (and (instance? BindScalar binding)
                   (instance? RulesVar (:variable binding)))
              (let [parsed (rules/parse-rules value)]
                (assoc context
                       :rules parsed
                       :rules-deps (rules/dependency-graph parsed)))

              :else
              (update context :rels conj (in->rel binding value)))]
        (recur context
               (next bindings)
               (when values (next values))))
      context)))

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
  (mapv (fn [pattern-el]
          (if (qu/binding-var? pattern-el)
            (let [substituted (substitute-constant context pattern-el)]
              (if (nil? substituted) pattern-el substituted))
            pattern-el))
        pattern))

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
      (let [cached @cache]
        (if (contains? cached var)
          (get cached var)
          (let [result (or (compute-rels-bound-values context var)
                           (get (:delta-bound-values context) var))]
            (vswap! cache assoc var result)
            result)))
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

;; Guardrail against pathological per-key work. Within this limit, the cost
;; comparison below chooses between indexed probes and a full scan.
(def ^:const ^:private ^long multi-lookup-safety-limit 1000000)

(defn- estimate-multi-lookup-output
  ^long [^long bound-count ^long scan-count]
  (long
    (min scan-count
         (Math/ceil (* (double bound-count)
                       (double c/magic-link-ratio))))))

(defn- estimate-multi-lookup-cost
  ^double [^long bound-count ^long output-count]
  (let [bound  (double bound-count)
        output (double output-count)]
    (+ (* bound (double c/magic-cost-link-probe))
       (* output (double c/magic-cost-link-retrieval))
       (* (+ bound output) (double c/magic-cost-hash-join)))))

(defn- estimate-full-lookup-cost
  ^double [^long bound-count ^long scan-count]
  (let [bound (double bound-count)
        scan  (double scan-count)]
    (+ (* scan (double c/magic-cost-init-scan-e))
       (* (+ bound scan) (double c/magic-cost-hash-join)))))

(defn- multi-lookup-cheaper?
  [^long bound-count ^long scan-count]
  (and (pos? bound-count)
       (<= bound-count multi-lookup-safety-limit)
       (< (estimate-multi-lookup-cost
            bound-count
            (estimate-multi-lookup-output bound-count scan-count))
          (estimate-full-lookup-cost bound-count scan-count))))

(defn- lookup-pattern-multi-entity
  "Perform multiple point lookups for bound entity values.
   More efficient than full table scan when entity is bound to multiple values.
   Wildcard values use existence probes and emit one tuple per entity."
  [db pattern entity-pairs v-is-var?]
  (let [[_ a v]          pattern
        a'               (if (keyword? a) a nil)
        v'               (if (or (qu/free-var? v) (= v '_)) nil v)
        existence-only?  (or (= v '_) (qu/placeholder? v))
        acc              (FastList.)]
    (if existence-only?
      (let [input (FastList. (count entity-pairs))]
        (doseq [[e eid] entity-pairs]
          (.add input (object-array [e eid])))
        (when-let [^List matches
                   (db/-eav-filter-presence-list db input 1 a')]
          (dotimes [i (.size matches)]
            (let [^objects tuple (.get matches i)]
              (.add acc (object-array [(aget tuple 0)]))))))
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
                  (.add acc (object-array [e])))))))))
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
                    (aset result 0 (aget t 0))
                    (aset result 1 v)
                    (.add acc result)))
                (dotimes [_ n]
                  (.add acc (object-array [v])))))))))
    acc))

(defn- resolve-value-pairs
  [db attr values]
  (let [ref-attr? (and (keyword? attr) (db/ref? db attr))]
    (keep (fn [value]
            (if ref-attr?
              (when-some [resolved (if (or (qu/lookup-ref? value)
                                           (keyword? value))
                                     (db/entid db value)
                                     value)]
                [value resolved])
              [value value]))
          values)))

(defn- pairs-by-resolved
  [pairs]
  (reduce (fn [m [original resolved]]
            (update m resolved (fnil conj []) original))
          {}
          pairs))

(defn- bounded-side-fanout
  ^long [db patterns]
  (reduce
    (fn [^long total pattern]
      (let [n (long (db/-count db pattern))]
        (if (> n (- Long/MAX_VALUE total))
          (reduced Long/MAX_VALUE)
          (+ total n))))
    0
    patterns))

(defn- bounded-both-strategy
  [db attr entity-pairs value-pairs]
  (let [entity-count  (long (count entity-pairs))
        value-count   (long (count value-pairs))
        cardinality-many?
        (identical? :db.cardinality/many
                    (get-in (db/-schema db) [attr :db/cardinality]))
        ;; A cardinality-one EAV lookup emits at most one value per entity, so
        ;; counting every entity first would merely duplicate the actual scan.
        entity-fanout (if cardinality-many?
                        (bounded-side-fanout
                          db (map (fn [[_ e]] [e attr nil]) entity-pairs))
                        entity-count)
        value-fanout  (bounded-side-fanout
                        db (map (fn [[_ v]] [nil attr v]) value-pairs))
        scan-count    (long (db/-count db [nil attr nil]))
        alternatives
        (cond-> [{:kind       :full
                  :candidates scan-count
                  :cost       (estimate-full-lookup-cost
                                (+ entity-count value-count) scan-count)}]
          (<= entity-count multi-lookup-safety-limit)
          (conj {:kind       :entity
                 :candidates entity-fanout
                 :cost       (estimate-multi-lookup-cost
                               entity-count entity-fanout)})

          (<= value-count multi-lookup-safety-limit)
          (conj {:kind       :value
                 :candidates value-fanout
                 :cost       (estimate-multi-lookup-cost
                               value-count value-fanout)}))]
    (:kind (apply min-key :cost alternatives))))

(defn- pair-input-list
  [pairs]
  (let [input (FastList. (count pairs))]
    (doseq [[_ resolved] pairs]
      (.add input (object-array [resolved])))
    input))

(defn- add-bounded-match!
  [^List acc entity-originals value-originals]
  (doseq [entity entity-originals
          value  value-originals]
    (.add acc (object-array [entity value])))
  acc)

(defn- lookup-pattern-bounded-both
  "Intersect a pattern with bound sets on both entity and value while reading
   only the cheaper indexed side (or one full scan). Unlike a generic lookup
   followed by two hash joins, non-matching tuples are never materialized."
  [db pattern entity-values value-values]
  (let [[_ attr _]  pattern
        entity-pairs (vec (resolve-entity-pairs db entity-values))
        value-pairs  (vec (resolve-value-pairs db attr value-values))
        entities     (pairs-by-resolved entity-pairs)
        values       (pairs-by-resolved value-pairs)
        strategy     (bounded-both-strategy db attr entity-pairs value-pairs)
        acc          (FastList.)]
    (case strategy
      :entity
      (when-let [^List tuples
                 (db/-eav-scan-v-list
                   db (pair-input-list entity-pairs) 0
                   [[attr {:skip? false}]])]
        (dotimes [i (.size tuples)]
          (let [^objects tuple (.get tuples i)
                entity-originals (get entities (aget tuple 0))
                value-originals  (get values (aget tuple 1))]
            (when (and entity-originals value-originals)
              (add-bounded-match! acc entity-originals value-originals)))))

      :value
      (when-let [^List tuples
                 (db/-val-eq-scan-e-list
                   db (pair-input-list value-pairs) 0 attr)]
        (dotimes [i (.size tuples)]
          (let [^objects tuple (.get tuples i)
                value-originals  (get values (aget tuple 0))
                entity-originals (get entities (aget tuple 1))]
            (when (and entity-originals value-originals)
              (add-bounded-match! acc entity-originals value-originals)))))

      :full
      (when-let [^List tuples (db/-search-tuples db [nil attr nil])]
        (dotimes [i (.size tuples)]
          (let [^objects tuple (.get tuples i)
                entity-originals (get entities (aget tuple 0))
                value-originals  (get values (aget tuple 1))]
            (when (and entity-originals value-originals)
              (add-bounded-match! acc entity-originals value-originals))))))
    acc))

(defn lookup-pattern-db
  [context db pattern]
  (let [[e a v]           pattern
        search-pattern    (delay
                            (->> pattern
                                 (substitute-constants context)
                                 (resolve-pattern-lookup-refs db)
                                 (mapv #(if (or (qu/free-var? %) (= % '_))
                                          nil
                                          %))))
        scan-count        (delay (long (db/-count db @search-pattern)))
        entity-values     (when (and (qu/binding-var? e) (keyword? a))
                            (bound-values context e))
        value-values      (when (and (qu/binding-var? e)
                                     (qu/binding-var? v)
                                     (not= e v)
                                     (keyword? a))
                            (bound-values context v))
        use-bounded-both? (and entity-values value-values)
        use-entity-multi? (and (not use-bounded-both?)
                               entity-values
                               (multi-lookup-cheaper?
                                 (long (.size ^HashSet entity-values))
                                 @scan-count))
        use-value-multi?  (and (not use-bounded-both?)
                               value-values
                               (multi-lookup-cheaper?
                                 (long (.size ^HashSet value-values))
                                 @scan-count))]
    (cond
      use-bounded-both?
      (r/relation! {e 0, v 1}
                   (lookup-pattern-bounded-both db pattern entity-values
                                                value-values))

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
                     (lookup-pattern-multi-value db pattern value-values
                                                 e-is-var?)))

      :else
      (let [search-pattern @search-pattern]
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

(defn- integer-tuple-column?
  [^List tuples ^long idx]
  (let [n (.size tuples)]
    (loop [i (long 0)]
      (or (== i n)
          (and (integer? (aget ^objects (.get tuples (int i)) idx))
               (recur (u/long-inc i)))))))

(defn- filter-bound-entity-presence
  "Apply `[?e :attr _]` directly to the relation that binds `?e`. This is an
  indexed semi-join: it preserves every matching input tuple without building
  and joining a separate one-column relation."
  [context db pattern]
  (let [[e a v]        pattern
        presence-only? (and (= 3 (count pattern))
                            (or (= v '_) (qu/placeholder? v)))
        rel             (when (qu/binding-var? e) (rel-with-attr context e))
        ^List tuples    (when (and presence-only?
                                   rel
                                   (keyword? a)
                                   (contains? (db/-schema db) a))
                          (:tuples rel))
        eid-idx         (when tuples (long ((:attrs rel) e)))
        bound-count     (when tuples (.size tuples))]
    (when (and bound-count
               (> ^long bound-count 1)
               (integer-tuple-column? tuples eid-idx)
               (multi-lookup-cheaper?
                 (long bound-count)
                 (long (db/-count db [nil a nil]))))
      (let [tuples   (db/-eav-filter-presence-list db tuples eid-idx a)
            filtered (assoc rel :tuples tuples)]
        (assoc context :rels
               (mapv #(if (identical? % rel) filtered %) (:rels context)))))))

(defn matches-pattern?
  [pattern tuple]
  (let [n (min (count pattern) (count tuple))]
    (loop [i 0]
      (if (< i n)
        (let [t (nth tuple i)
              p (nth pattern i)]
          (if (or (= p '_) (qu/free-var? p) (= t p))
            (recur (unchecked-inc i))
            false))
        true))))

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

(defn context-resolve-val
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

(defn dot-form [f]
  (when (and (symbol? f) (str/starts-with? (name f) "."))
    f))

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

(defn make-call
  [f]
  (if (dot-form f)
    (let [fname (subs (name f) 1)] #(dot-call fname %))
    #(opt-apply f %)))

(defn resolve-sym
  [sym]
  (when (symbol? sym)
    (when-let [v (or (resolve sym)
                     (when (find-ns 'pod.huahaiy.datalevin)
                       (ns-resolve 'pod.huahaiy.datalevin sym)))]
      @v)))

(defonce pod-fns (atom {}))

(defn- disallowed-server-query-function!
  [f]
  (raise "Server query cannot call unregistered function or predicate '" f
         {:error :query/where :var f :resolver-mode *resolver-mode*}))

(defn- resolve-built-in-query-fn
  [f]
  (get built-ins/query-fns f))

(defn- inter-fn?
  [f]
  (= (:type (meta f)) :datalevin/inter-fn))

(defn- validate-server-safe-apply!
  [f args]
  (when (and (server-safe-resolver?) (= 'apply f))
    (let [target (first args)]
      (when-not (and (symbol? target)
                     (not= 'apply target)
                     (contains? built-ins/query-fns target))
        (disallowed-server-query-function! target)))))

(defn resolve-pred
  [f context]
  (let [fun (cond
              (inter-fn? f)
              f

              (fn? f)
              (if (server-safe-resolver?)
                (disallowed-server-query-function! f)
                f)

              (resolve-built-in-query-fn f)
              (resolve-built-in-query-fn f)

              (and (not (server-safe-resolver?))
                   (context-resolve-val context f))
              (context-resolve-val context f)

              (and (server-safe-resolver?)
                   (rel-with-attr context f))
              (disallowed-server-query-function! f)

              (and (server-safe-resolver?)
                   (or (qualified-symbol? f) (dot-form f)))
              (disallowed-server-query-function! f)

              (and (not (server-safe-resolver?))
                   (dot-form f))
              (dot-form f)

              (and (not (server-safe-resolver?))
                   (resolve-sym f))
              (resolve-sym f)

              :else
              (raise "Unknown function or predicate '" f
                     {:error :query/where :var f}))]
    (if-let [s (:pod.huahaiy.datalevin/inter-fn fun)]
      (@pod-fns s)
      fun)))

(defn -call-fn
  [context rel f args]
  (validate-server-safe-apply! f args)
  (let [sources              (:sources context)
        attrs                (:attrs rel)
        len                  (count args)
        ^objects static-args (make-array Object len)
        ^objects tuples-args (make-array Object len)
        call                 (make-call (resolve-pred f context))]
    (dotimes [i len]
      (let [arg (nth args i)]
        (cond
          (symbol? arg)
          (if-some [source (get sources arg)]
            (aset static-args i source)
            (if-some [fn-val (or (resolve-built-in-query-fn arg)
                                 (when-not (server-safe-resolver?)
                                   (resolve-sym arg)))]
              (aset static-args i fn-val)
              (if (contains? attrs arg)
                (aset tuples-args i (get attrs arg))
                (when (server-safe-resolver?)
                  (disallowed-server-query-function! arg)))))

          (list? arg)
          (aset tuples-args i (-call-fn context rel (first arg) (rest arg)))

          :else
          (aset static-args i arg))))
    (let [tuple-bindings
          (into []
                (keep-indexed
                  (fn [i tuple-arg]
                    (when (and (some? tuple-arg) (not (fn? tuple-arg)))
                      [i tuple-arg])))
                tuples-args)
          nested-bindings
          (into []
                (keep-indexed
                  (fn [i tuple-arg]
                    (when (fn? tuple-arg) [i tuple-arg])))
                tuples-args)
          ^ints tuple-positions (int-array (map first tuple-bindings))
          ^ints tuple-indexes   (int-array (map (comp int second)
                                                tuple-bindings))
          ^ints nested-positions (int-array (map first nested-bindings))
          ^objects nested-fns    (object-array (map second nested-bindings))
          tuple-count            (alength tuple-positions)
          nested-count           (alength nested-positions)]
      (fn [^objects tuple]
        (dotimes [i tuple-count]
          (aset static-args (aget tuple-positions i)
                (aget tuple (aget tuple-indexes i))))
        (dotimes [i nested-count]
          (aset static-args (aget nested-positions i)
                ((aget nested-fns i) tuple)))
        (call static-args)))))

(defn filter-by-pred
  [context clause]
  (let [[[f & args]]         clause
        attrs                (qu/collect-fn-arg-vars args)
        [context production] (rel-prod-by-attrs context attrs)
        new-rel              (let [tuple-pred (-call-fn context production f args)]
                               (update production :tuples
                                       #(r/select-tuples tuple-pred %)))]
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

      (nil? last-arg)
      (assoc v (dec n) meta-map)

      (instance? clojure.lang.IObj last-arg)
      (assoc v (dec n) (with-meta last-arg {:tuple-needed needed}))

      :else
      (conj v meta-map))))

(defn- bind-scalar-tuples
  [production out-var tuple-fn]
  (let [attrs        (:attrs production)
        attr-keys    (vec (keys attrs))
        n            (count attr-keys)
        ^ints idxs   (int-array (map attrs attr-keys))
        ^List tuples (:tuples production)
        size         (.size tuples)
        res          (FastList. size)]
    (dotimes [i size]
      (let [^objects tuple (.get tuples i)
            val            (tuple-fn tuple)]
        (when-not (nil? val)
          (let [^objects out (object-array (unchecked-inc n))]
            (dotimes [j n]
              (aset out j (aget tuple (aget idxs j))))
            (aset out n val)
            (.add res out)))))
    (r/relation! (zipmap (conj attr-keys out-var) (range)) res)))

(defn- compile-tuple-product
  [left-attrs right-attrs]
  (let [left-vars        (vec (keys left-attrs))
        right-vars       (vec (keys right-attrs))
        common-vars      (into [] (filter #(contains? right-attrs %))
                               left-vars)
        new-right-vars   (into [] (remove #(contains? left-attrs %))
                               right-vars)]
    (object-array
      [right-attrs
       (zipmap (u/concatv left-vars new-right-vars) (range))
       (int-array (map left-attrs left-vars))
       (int-array (map right-attrs new-right-vars))
       (int-array (map left-attrs common-vars))
       (int-array (map right-attrs common-vars))])))

(defn- compile-flat-tuple-product
  [left-attrs {:keys [cols source-attrs source-width]}]
  (let [left-vars       (vec (keys left-attrs))
        common-vars     (into [] (filter #(contains? left-attrs %)) cols)
        new-right-vars  (into [] (remove #(contains? left-attrs %)) cols)]
    (object-array
      [(zipmap (u/concatv left-vars new-right-vars) (range))
       (int-array (map left-attrs left-vars))
       (int-array (map left-attrs common-vars))
       (int-array (map source-attrs common-vars))
       (int-array (map source-attrs new-right-vars))
       (long source-width)])))

(defn- flat-tuple-product-match?
  [^objects left-tuple value ^objects product-plan]
  (let [^ints left-idxs   (aget product-plan 2)
        ^ints source-idxs (aget product-plan 3)
        n                 (alength left-idxs)]
    (loop [i 0]
      (or (== i n)
          (and (= (aget left-tuple (aget left-idxs i))
                  (nth value (aget source-idxs i)))
               (recur (unchecked-inc-int i)))))))

(defn- append-flat-tuple-product!
  [^List res ^objects left-tuple value ^objects product-plan]
  (when (flat-tuple-product-match? left-tuple value product-plan)
    (let [^ints left-idxs   (aget product-plan 1)
          ^ints source-idxs (aget product-plan 4)
          left-size         (alength left-idxs)
          source-size       (alength source-idxs)
          ^objects out      (object-array (+ left-size source-size))]
      (dotimes [i left-size]
        (aset out i (aget left-tuple (aget left-idxs i))))
      (dotimes [i source-size]
        (aset out (+ left-size i) (nth value (aget source-idxs i))))
      (.add res out))))

(defn- bind-flat-tuple-tuples
  "Bind a flat tuple-valued function directly into each production tuple.
  This avoids constructing and hash-joining a pair of temporary relations for
  every function result. Duplicate or nested tuple bindings keep using the
  general binding path because `tuple-binding-projection` rejects them."
  [production binding projection tuple-fn]
  (let [^List tuples (:tuples production)
        size         (.size tuples)
        res          (FastList. size)
        product-plan (compile-flat-tuple-product
                       (:attrs production) projection)
        source-width (long (aget ^objects product-plan 5))]
    (dotimes [i size]
      (let [^objects tuple (.get tuples i)
            value          (tuple-fn tuple)]
        (when-not (nil? value)
          (when-not (u/seqable? value)
            (raise "Cannot bind value " value " to tuple "
                   (dp/source binding)
                   {:error :query/binding, :value value,
                    :binding (dp/source binding)}))
          (when (< (count value) source-width)
            (raise "Not enough elements in a collection " value
                   " to bind tuple " (dp/source binding)
                   {:error :query/binding, :value value,
                    :binding (dp/source binding)}))
          (append-flat-tuple-product! res tuple value product-plan))))
    (r/relation! (aget ^objects product-plan 0) res)))

(defn- tuple-product-match?
  [^objects left-tuple ^objects right-tuple ^objects product-plan]
  (let [^ints left-idxs  (aget product-plan 4)
        ^ints right-idxs (aget product-plan 5)
        n                (alength left-idxs)]
    (loop [i 0]
      (or (== i n)
          (and (= (aget left-tuple (aget left-idxs i))
                  (aget right-tuple (aget right-idxs i)))
               (recur (unchecked-inc-int i)))))))

(defn- append-tuple-product!
  [^List res ^objects left-tuple bound-rel ^objects product-plan]
  (let [^List right-tuples (:tuples bound-rel)
        ^ints left-idxs    (aget product-plan 2)
        ^ints right-idxs   (aget product-plan 3)
        size               (.size right-tuples)]
    (dotimes [i size]
      (let [^objects right-tuple (.get right-tuples i)]
        (when (tuple-product-match? left-tuple right-tuple product-plan)
          (.add res (r/join-tuples left-tuple left-idxs
                                   right-tuple right-idxs)))))))

(defn- bind-coll-tuples
  [production binding projection needed tuple-fn]
  (let [^List tuples (:tuples production)
        size         (.size tuples)
        initial-res  (FastList. size)]
    (loop [i 0, ^objects product-plan nil, ^List res initial-res]
      (if (< i size)
        (let [tuple ^objects (.get tuples i)
              val   (tuple-fn tuple)]
          (if (nil? val)
            (recur (unchecked-inc-int i) product-plan res)
            (let [bound-rel
                  (cond
                    needed
                    (r/relation! (:attrs projection) val)

                    (and projection (instance? java.util.List val))
                    (tuple-list->rel binding val projection)

                    :else
                    (in->rel binding val))
                  bound-attrs (:attrs bound-rel)]
              (if (or (nil? product-plan)
                      (= (aget product-plan 0) bound-attrs))
                (let [plan (or product-plan
                               (compile-tuple-product (:attrs production)
                                                      bound-attrs))]
                  (append-tuple-product! res tuple bound-rel plan)
                  (recur (unchecked-inc-int i) plan res))
                (let [joined (j/hash-join
                               (r/relation! (:attrs production)
                                            (r/single-tuples tuple))
                               bound-rel)
                      merged (r/sum-rel
                               (r/relation! (aget product-plan 1) res)
                               joined)]
                  (recur (unchecked-inc-int i)
                         product-plan (:tuples merged)))))))
        (if product-plan
          (r/relation! (aget product-plan 1) res)
          (j/hash-join production (empty-rel binding)))))))

(defn bind-by-fn
  [context clause]
  (let [[[f & args] out]     clause
        binding              (dp/parse-binding out)
        flat-tuple-projection
        (when (instance? BindTuple binding)
          (qtuple/tuple-binding-projection binding))
        coll-tuple-bind?     (and (instance? BindColl binding)
                                  (instance? BindTuple (:binding binding)))
        projection           (when coll-tuple-bind?
                               (qtuple/tuple-binding-projection
                                 (:binding binding)))
        needed               (when (and projection
                                        (contains? tuple-producing-fns f))
                               (:needed projection))
        args'                (if needed
                               (attach-needed-meta args needed)
                               args)
        attrs                (qu/collect-fn-arg-vars args)
        [context production] (rel-prod-by-attrs context attrs)
        out-var              (when (instance? BindScalar binding)
                               (get-in binding [:variable :symbol]))
        out-idx              (when out-var (get (:attrs production) out-var))
        new-rel
        (if out-idx
          (let [tuple-fn (-call-fn context production f args')]
            (clojure.core/update
              production :tuples
              #(r/select-tuples
                 (fn [^objects tuple]
                   (let [val (tuple-fn tuple)]
                     (and (not (nil? val))
                          (= (aget tuple (int out-idx)) val))))
                 %)))
          (let [tuple-fn (-call-fn context production f args')]
            (if (instance? BindScalar binding)
              (bind-scalar-tuples production out-var tuple-fn)
              (if flat-tuple-projection
                (bind-flat-tuple-tuples
                  production binding flat-tuple-projection tuple-fn)
                (bind-coll-tuples
                  production binding projection needed tuple-fn)))))]
    (update context :rels collapse-rels new-rel)))

(defn dynamic-lookup-attrs
  [source pattern]
  (let [[e a v] pattern]
    (cond-> #{}
      (qu/binding-var? e)   (conj e)
      (and (qu/binding-var? v)
           (not (qu/binding-var? a))
           (db/ref? source a)) (conj v))))

(defn limit-rel
  [rel vars]
  (if-some [attrs (not-empty (select-keys (:attrs rel) vars))]
    (assoc rel :attrs attrs)
    ;; Projecting away every column of a non-empty relation is existential
    ;; success, so that relation can be dropped. An empty relation is branch
    ;; failure, however, and must remain as an attribute-free annihilator.
    (when (r/rel-empty rel)
      (assoc rel :attrs {}))))

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
    :else
    (pattern form)))

(defn- clause-vars [clause]
  (into #{} (filter qu/binding-var?) (nfirst clause)))

(defn- call-vars
  [[f & args]]
  (cond-> (qu/collect-fn-arg-vars args)
    (qu/binding-var? f) (conj f)))

(defn- clause-binding-requirements
  [clause]
  (let [clause (if (and (sequential? clause)
                        (qu/source? (first clause)))
                 (next clause)
                 clause)
        head   (when (sequential? clause) (first clause))]
    (cond
      ;; Predicate and function expressions need their call arguments. The
      ;; function binding, if present, is an output and is not required.
      (and (vector? clause) (sequential? head))
      {:required (call-vars head)}

      ;; Plain not joins on whichever variables it shares with the surrounding
      ;; context. Its existing validation requires at least one such variable.
      (= 'not head)
      {:required-any (qu/collect-vars (next clause))}

      (= 'not-join head)
      {:required (qu/collect-vars (second clause))}

      (= 'or-join head)
      (let [vars-form (second clause)
            req-form  (when (and (sequential? vars-form)
                                 (sequential? (first vars-form)))
                        (first vars-form))]
        {:required (qu/collect-vars req-form)})

      :else {})))

(defn- clause-bindings-ready?
  [bound clause]
  (let [{:keys [required required-any]}
        (clause-binding-requirements clause)]
    (and (set/subset? required bound)
         (or (empty? required-any)
             (some bound required-any)))))

(defn- resolve-clauses
  "Resolve conjunction clauses in dependency order, retaining source order
  among clauses whose input bindings are already available."
  [context clauses]
  (loop [context context
         pending (vec clauses)]
    (if (empty? pending)
      context
      (let [bound (bound-vars context)
            idx   (first
                    (keep-indexed
                      (fn [i clause]
                        (when (clause-bindings-ready? bound clause) i))
                      pending))]
        (if (some? idx)
          (recur (resolve-clause context (nth pending idx))
                 (u/vec-remove pending idx))
          ;; Preserve the resolver's detailed insufficient-binding error when
          ;; the conjunction has no clause capable of making progress.
          (reduce resolve-clause context pending))))))

(defn -resolve-clause
  ([context clause]
   (-resolve-clause context clause clause))
  ([context clause orig-clause]
   (condp looks-like? clause
     [[symbol? '*]]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (filter-by-pred context clause))

     [[fn? '*]]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (filter-by-pred context clause))

     [[symbol? '*] '_]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (bind-by-fn context clause))

     [[fn? '*] '_]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (bind-by-fn context clause))

     [qu/source? '*]
     (let [[source-sym & rest] clause]
       (binding [qu/*implicit-source* (get (:sources context) source-sym)]
         (-resolve-clause context rest clause)))

     '[or *]
     (let [[_ & branches] clause
           _              (check-free-same (bound-vars context) branches clause)
           contexts       (map #(resolve-clause context %) branches)]
       (assoc (first contexts) :rels [(transduce
                                        (map #(reduce j/hash-join (:rels %)))
                                        r/sum-rel-dedupe
                                        contexts)]))

     '[or-join [[*] *] *]
     (let [[_ [req-vars & vars] & branches] clause
           req-vars                         (into #{} (filter qu/binding-var?)
                                                     req-vars)
           bound                            (bound-vars context)]
       (check-bound bound req-vars orig-clause)
       (check-free-subset bound vars branches)
       (recur context (list* 'or-join (concatv req-vars vars) branches) clause))

     '[or-join [*] *]
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

     '[and *]
     (let [[_ & clauses] clause]
       (resolve-clauses context clauses))

     '[not *]
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

     '[not-join [*] *]
     (let [[_ vars & clauses] clause
           vars               (into []
                                    (comp (filter qu/binding-var?) (distinct))
                                    vars)
           var-set            (set vars)
           bound              (bound-vars context)
           _                  (check-bound bound var-set orig-clause)
           context1           (assoc context :rels
                                     [(reduce j/hash-join (:rels context))])
           outer-rel          (single (:rels context1))
           join-context       (assoc context1 :rels
                                     [(r/project-distinct outer-rel vars)])
           negation-context   (-> (reduce resolve-clause join-context clauses)
                                  (limit-context var-set))
           neg-rel            (-> (reduce j/hash-join
                                          (:rels negation-context))
                                  (r/project-distinct vars))]
       (assoc context1 :rels
              [(j/subtract-rel
                 outer-rel
                 neg-rel)]))

     '[*]
     (let [source   qu/*implicit-source*
           pattern' (resolve-pattern-lookup-refs source clause)]
       (if-let [filtered-context
                (when (satisfies? db/ITuples source)
                  (filter-bound-entity-presence context source pattern'))]
         filtered-context
         (let [relation (lookup-pattern context source pattern')]
           (binding [qu/*lookup-attrs* (if (db/-searchable? source)
                                         (dynamic-lookup-attrs source pattern')
                                         qu/*lookup-attrs*)]
             (update context :rels collapse-rels relation))))))))

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

(defn or-join-build
  [sources rules ^List tuples clause bound-var bound-idx free-vars]
  (when (pos? (.size tuples))
    (let [bound-rel      (r/relation!
                           {bound-var 0}
                           (let [seen (HashSet.)
                                 res  (FastList.)]
                             (dotimes [i (.size tuples)]
                               (let [v (aget ^objects (.get tuples i)
                                             bound-idx)]
                                 (when (.add seen v)
                                   (.add res (object-array [v])))))
                             res))
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

(defn or-join-build-cached
  "Reuse an or-join build for the same input list and immutable link shape."
  [^IdentityHashMap cache sources rules ^List tuples clause bound-var bound-idx
   free-vars]
  (let [^HashMap builds (or (.get cache tuples)
                            (let [m (HashMap.)]
                              (.put cache tuples m)
                              m))
        build-key       [clause bound-var bound-idx free-vars]]
    (if (.containsKey builds build-key)
      (.get builds build-key)
      (let [built (or-join-build sources rules tuples clause bound-var
                                 bound-idx free-vars)]
        (.put builds build-key built)
        built))))

(defn or-join-execute-link
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

(defn or-join-count-built
  "Count target tuples from an existing or-join build."
  [db ^List tuples bound-idx tgt-attr built]
  (if-let [{:keys [or-by-bound free-var-idx]} built]
    (let [size    (.size tuples)
          fanouts (HashMap.)]
      (loop [i     (long 0)
             total (long 0)]
        (if (< i size)
          (let [^objects in-tuple (.get tuples i)
                bv                (aget in-tuple bound-idx)
                ^List matches     (.get ^HashMap or-by-bound bv)
                total
                (if matches
                  (loop [j     (long 0)
                         total (long total)]
                    (if (< j (.size matches))
                      (let [^objects match (.get matches j)
                            fv             (aget match free-var-idx)
                            cached         (.get fanouts fv)
                            fanout         (if cached
                                             (long cached)
                                             (let [n (long
                                                       (db/-count
                                                         db [nil tgt-attr fv]
                                                         Long/MAX_VALUE))]
                                               (.put fanouts fv n)
                                               n))]
                        (recur (unchecked-inc j)
                               (unchecked-add total fanout)))
                      total))
                  total)]
            (recur (unchecked-inc i) (long total)))
          total)))
    0))

(defn or-join-count-link
  "Count linked or-join tuples without materializing the final target tuples."
  [db sources rules ^List tuples clause bound-var bound-idx free-vars tgt-attr]
  (or-join-count-built
    db tuples bound-idx tgt-attr
    (or-join-build sources rules tuples clause bound-var bound-idx free-vars)))

(defn or-join-execute-link-into
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
