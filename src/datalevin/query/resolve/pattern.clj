;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.resolve.pattern
  "Indexed and collection pattern lookup, bounded probes, and presence filtering."
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.inline :refer [update assoc]]
   [datalevin.query-util :as qu]
   [datalevin.query.predicate :as qpred]
   [datalevin.query.resolve.context :refer [rel-with-attr]]
   [datalevin.relation :as r]
   [datalevin.util :as u])
  (:import
   [java.util HashSet List]
   [org.eclipse.collections.impl.list.mutable FastList]))

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

(defn resolve-entity-pairs
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

(def ^:const ^long multi-lookup-safety-limit 1000000)

;; Match the storage scan's target chunk size. Below this point individual
;; probes avoid sorting and projecting an input batch; at and above it one EAV
;; merge scan can also use the storage layer's CPU-aware parallel chunking.

(def ^:const ^long merged-eav-lookup-threshold 4000)

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

(defn multi-lookup-cheaper?
  [^long bound-count ^long scan-count]
  (and (pos? bound-count)
       (<= bound-count multi-lookup-safety-limit)
       (< (estimate-multi-lookup-cost
            bound-count
            (estimate-multi-lookup-output bound-count scan-count))
          (estimate-full-lookup-cost bound-count scan-count))))

(defn lookup-pattern-multi-entity
  "Perform multiple point lookups for bound entity values.
   More efficient than full table scan when entity is bound to multiple values.
   Large value-producing batches use one merged EAV scan. Wildcard values use
   existence probes and emit one tuple per entity."
  [db pattern entity-pairs v-is-var?]
  (let [[_ a v]          pattern
        a'               (if (keyword? a) a nil)
        v'               (if (or (qu/free-var? v) (= v '_)) nil v)
        existence-only?  (or (= v '_) (qu/placeholder? v))
        acc              (FastList.)]
    (cond
      existence-only?
      (let [input (FastList. (count entity-pairs))]
        (doseq [[e eid] entity-pairs]
          (.add input (object-array [e eid])))
        (when-let [^List matches
                   (db/-eav-filter-presence-list db input 1 a')]
          (dotimes [i (.size matches)]
            (let [^objects tuple (.get matches i)]
              (.add acc (object-array [(aget tuple 0)]))))))

      (and v-is-var?
           (<= merged-eav-lookup-threshold (count entity-pairs)))
      (let [input (FastList. (count entity-pairs))]
        ;; Keep the original value beside its resolved eid. EAV appends the
        ;; attribute value, after which the resolved eid is projected away.
        ;; This preserves lookup refs (and aliases resolving to the same eid).
        (doseq [[e eid] entity-pairs]
          (.add input (object-array [e eid])))
        (when-let [^List matches
                   (db/-eav-scan-v-list
                     db input 1 [[a' {:skip? false}]])]
          (dotimes [i (.size matches)]
            (let [^objects tuple (.get matches i)]
              (.add acc (object-array [(aget tuple 0) (aget tuple 2)]))))))

      :else
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

(defn lookup-pattern-domain-filtered-entity
  "Scan a set of bound entities once while applying an immutable value-domain
  predicate in the EAV read. This avoids the exact per-entity fan-out probes
  used to choose among the fully generic bounded-both alternatives."
  [db pattern entity-values value-values]
  (let [[_ attr _]  pattern
        entity-pairs (vec (resolve-entity-pairs db entity-values))
        value-pairs  (vec (resolve-value-pairs db attr value-values))
        values       (pairs-by-resolved value-pairs)
        acc          (FastList.)]
    (if (<= merged-eav-lookup-threshold (count entity-pairs))
      (let [input (FastList. (count entity-pairs))
            pred  (qpred/shareable-predicate #(contains? values %))]
        ;; Preserve each original entity binding beside the eid used by EAV.
        ;; The scan appends the matched value, so the eid can be discarded.
        (doseq [[entity eid] entity-pairs]
          (.add input (object-array [entity eid])))
        (when-let [^List matches
                   (db/-eav-scan-v-list
                     db input 1 [[attr {:skip? false :pred pred}]])]
          (dotimes [i (.size matches)]
            (let [^objects tuple (.get matches i)
                  entity        (aget tuple 0)
                  value         (aget tuple 2)]
              (doseq [original (get values value)]
                (.add acc (object-array [entity original])))))))
      (doseq [[entity eid] entity-pairs]
        (when-let [^List tuples (db/-search-tuples db [eid attr nil])]
          (dotimes [i (.size tuples)]
            (let [value (aget ^objects (.get tuples i) 0)]
              (doseq [original (get values value)]
                (.add acc (object-array [entity original]))))))))
    acc))

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
            entity-pairs     (vec (resolve-entity-pairs db entity-values))
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

(defn filter-bound-entity-presence
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

(defn dynamic-lookup-attrs
  [source pattern]
  (let [[e a v] pattern]
    (cond-> #{}
      (qu/binding-var? e)   (conj e)
      (and (qu/binding-var? v)
           (not (qu/binding-var? a))
           (db/ref? source a)) (conj v))))
