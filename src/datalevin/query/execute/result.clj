;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.execute.result
  "Tuple collection, result projection, ordering, and execution explanations."
  (:refer-clojure :exclude [assoc])
  (:require
   [clojure.walk :as w]
   [datalevin.inline :refer [assoc]]
   [datalevin.parser :as dp]
   [datalevin.pull-api :as dpa]
   [datalevin.query-optimizer :as qo]
   [datalevin.query.access :as qaccess]
   [datalevin.query.aggregate :as qagg]
   [datalevin.query.plan :as qplan]
   [datalevin.relation :as r]
   [datalevin.spill :as sp]
   [datalevin.util :as u :refer [cond+]])
  (:import
   [clojure.lang IPersistentCollection PersistentVector]
   [java.util Comparator List PriorityQueue]
   [datalevin.parser FindColl FindRel]
   [datalevin.utl UniqueVectorSet]))

(def ^:dynamic *deferred-result-explain* nil)

(defn adaptive-limit-query?
  [parsed-q]
  (let [find          (:qfind parsed-q)
        find-elements (dp/find-elements find)]
    (and (instance? FindRel find)
         (nil? (:qwith parsed-q))
         (empty? (:qhaving parsed-q))
         (nil? (:qreturn-map parsed-q))
         (not-any? #(or (dp/aggregate? %) (dp/find-expr? %)
                        (dp/pull? %))
                   find-elements))))

(defn tuple->persistent-vector
  [^objects tuple]
  ;; PersistentVector/adopt avoids copying the query engine's immutable tuple
  ;; array, but its adopted array is the vector tail and therefore must not
  ;; exceed the 32-element branching width.
  (if (<= (alength tuple) 32)
    (PersistentVector/adopt tuple)
    (vec tuple)))

(def ^:private ^:const unique-vector-set-max-size 2000000)

(defn spillable-result-set
  [^List tuples]
  (let [size       (.size tuples)
        result-set (sp/new-spillable-set nil {:initial-capacity size})]
    (dotimes [i size]
      (.cons ^IPersistentCollection result-set
             (tuple->persistent-vector ^objects (.get tuples i))))
    result-set))

(defn indexed-unique-result-set
  [^List tuples]
  (let [size (.size tuples)
        runtime (Runtime/getRuntime)
        used (- (.totalMemory runtime) (.freeMemory runtime))
        headroom (- (.maxMemory runtime) used)
        index-bytes (when (UniqueVectorSet/supportsSize size)
                      (UniqueVectorSet/estimatedIndexBytes size))
        materialization-bytes
        (when index-bytes
          (+ (long index-bytes) (* 32 (long size))))
        direct? (and (<= size unique-vector-set-max-size)
                     materialization-bytes
                     (>= headroom (* 2 (long materialization-bytes))))]
    (if direct?
      (UniqueVectorSet/fromUniqueTuples tuples)
      (spillable-result-set tuples))))

(defn -collect-tuples
  [acc rel ^ints dst-idxs ^ints src-idxs]
  (let [n (alength src-idxs)]
    (->Eduction
      (comp
        (map (fn [^objects t1]
               (->Eduction
                 (map (fn [t2]
                        (let [res (aclone t1)]
                          (if (u/array? t2)
                            (dotimes [i n]
                              (aset res (aget dst-idxs i)
                                    (aget ^objects t2 (aget src-idxs i))))
                            (dotimes [i n]
                              (aset res (aget dst-idxs i)
                                    (get t2 (aget src-idxs i)))))
                          res)))
                 (:tuples rel))))
        cat)
      acc)))

(defn -collect
  ([context symbols]
   (let [rels (:rels context)]
     (-collect [(make-array Object (count symbols))] rels symbols)))
  ([acc rels symbols]
   (cond+
     :let [rel (first rels)]

     (nil? rel) acc

     (empty? (:tuples rel)) []

     :let [keep-attrs (select-keys (:attrs rel) symbols)]

     (empty? keep-attrs) (recur acc (next rels) symbols)

     :let [copy-pairs (keep-indexed
                        (fn [dst-idx sym]
                          (when-some [src-idx (get keep-attrs sym)]
                            [dst-idx src-idx]))
                        symbols)
           dst-idxs  (int-array (map first copy-pairs))
           src-idxs  (int-array (map second copy-pairs))]

     :else
     (recur (-collect-tuples acc rel dst-idxs src-idxs)
            (next rels) symbols))))

(defn- collect-exact-unique-relation
  [rels symbols]
  (let [symbols  (vec symbols)
        relevant (filterv
                   (fn [rel]
                     (some #(contains? (:attrs rel) %) symbols))
                   rels)]
    (when (= 1 (count relevant))
      (let [rel          (first relevant)
            expected     (zipmap symbols (range))
            tuples ^List (:tuples rel)]
        (when (and (= expected (:attrs rel))
                   (r/unique-on? rel symbols)
                   tuples
                   ;; Symbol-disjoint relations only affect a set-valued
                   ;; projection through emptiness. Nonempty ones may be
                   ;; ignored without changing the public result.
                   (every? r/rel-not-empty rels))
          (let [result-set (indexed-unique-result-set tuples)]
            ;; The rule engine already proved both physical projection and
            ;; distinctness. Convert its tuples directly to the public query
            ;; representation and build its lookup index without rechecking
            ;; duplicate keys.
            result-set))))))

(defn collect
  [{:keys [result-set rels] :as context} symbols]
  (if (or (= result-set #{})
          (= (vec symbols) (:terminal-collected-symbols context)))
    context
    (assoc context :result-set
           (or (collect-exact-unique-relation rels symbols)
               (into (sp/new-spillable-set) (map vec)
                     (-collect context symbols))))))

(defn- typed-aget [a i]
  (aget ^objects a ^Long i))

(defn tuple-get [tuple]
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

(defn pull
  [find-elements context resultset]
  (let [resolved (for [find find-elements]
                   (when (dp/pull? find)
                     (let [db      (qagg/-context-resolve (:source find) context)
                           pattern (qagg/-context-resolve (:pattern find) context)]
                       (dpa/parse-opts db pattern))))]
    (for [tuple resultset]
      (mapv
        (fn [parsed-opts el]
          (if parsed-opts (dpa/pull-impl parsed-opts el) el))
        resolved
        tuple))))

(defn result-explain
  ([context result]
   (if *deferred-result-explain*
     (vreset! *deferred-result-explain*
              {:context context :result result})
     (do
       (result-explain context)
       (when qplan/*explain* (vswap! qplan/*explain* assoc :result result)))))
  ([{:keys [graph result-set plan opt-clauses late-clauses run?
            access-plans preferred-access-plan property-memo
            deferred-base-samples attribute-group-planning
            post-top-k-enrichment access-path-execution
            explain-actual-result-size keyed-group-reduction
            terminal-result-collection]
     :as context}]
   (when qplan/*explain*
     (let [{:keys [^long planning-time ^long parsing-time ^long building-time]}
           @qplan/*explain*
           memo-summary (some-> property-memo qo/property-memo-summary)
           selected-summary (:selected memo-summary)
           runtime-post-top-k (:post-top-k-enrichment @qplan/*explain*)
           conventional-cost
           (some #(when (= :conventional (:kind %)) (:cost %))
                 (:alternatives memo-summary))
           conventional-plan
           (when-not access-path-execution
             (w/postwalk
               (fn [e]
                 (if (qplan/plan? e)
                   (let [{:keys [steps] :as plan} e]
                     (cond->
                         (assoc plan :steps
                                (mapv #(qplan/step-explain % context) steps))
                       (and run? qplan/*intermediate-counts?*)
                       (assoc :actual-size
                              (get-in @(:intermediates context)
                                      [(:out (last steps))
                                       :tuples-count]))))
                   e)) plan))
           explained-plan (or access-path-execution conventional-plan)
           executed-summary
           (cond
             (not run?) {:kind :not-run}
             access-path-execution
             (cond-> selected-summary
               (:fallback access-path-execution)
               (assoc :fallback (:fallback access-path-execution)))
             selected-summary selected-summary
             :else {:kind :conventional})
           et  (double (/ (- ^long (System/nanoTime)
                             (+ ^long qplan/*start-time* planning-time
                                parsing-time building-time))
                          1000000))
           bt  (double (/ building-time 1000000))
           plt (double (/ planning-time 1000000))
           pat (double (/ parsing-time 1000000))
           ppt (double (/ (+ parsing-time building-time planning-time)
                          1000000))]
       (vswap! qplan/*explain* assoc
               :actual-result-size (or explain-actual-result-size
                                       (count result-set))
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
               :plan explained-plan
               :deferred-base-samples deferred-base-samples
               :attribute-group-planning attribute-group-planning
               :post-top-k-enrichment (or runtime-post-top-k
                                          post-top-k-enrichment)
               :keyed-group-reduction keyed-group-reduction
               :terminal-result-collection terminal-result-collection
               :late-clauses late-clauses
               :access-plans (mapv qaccess/plan-summary access-plans)
               :preferred-access-plan
               (some-> preferred-access-plan qaccess/plan-summary)
               :conventional-plan-cost conventional-cost
               :access-path-selected?
               (= :access (get-in memo-summary [:selected :kind]))
               :physical-plan-alternatives (:alternatives memo-summary)
               :physical-plan-subsets (:subsets memo-summary)
               :selected-plan-alternative selected-summary
               :recommended-plan-alternative selected-summary
               :executed-plan-alternative executed-summary)))))

(defn order-comps
  [tg find-vars order]
  (let [pairs     (vec (partition-all 2 order))
        n         (count pairs)
        idxs      (long-array n)
        ascending (boolean-array n)]
    (dotimes [i n]
      (let [[v direction] (pairs i)]
        (aset idxs i (long (if (integer? v)
                             v
                             (u/index-of #(= v %) find-vars))))
        (aset ascending i (identical? direction :asc))))
    (reify Comparator
      (compare [_ t1 t2]
        (loop [i 0]
          (if (< i n)
            (let [idx (aget idxs i)
                  res (if (aget ascending i)
                        (compare (tg t1 idx) (tg t2 idx))
                        (compare (tg t2 idx) (tg t1 idx)))]
              (if (zero? res)
                (recur (unchecked-inc-int i))
                res))
            0))))))

(defn- finite-limit?
  [limit]
  (and (some? limit) (not= -1 limit)))

(defn result-window
  [result limit offset]
  (let [offset (long (or offset 0))]
    (if (or (pos? offset) (finite-limit? limit))
      (into []
            (cond-> (drop offset)
              (finite-limit? limit) (comp (take limit)))
            result)
      result)))

(defn- top-k-result
  [^Comparator cmp result limit offset]
  (let [offset     (long (or offset 0))
        window-end (+ offset (long limit))]
    (if (or (zero? window-end) (> window-end Integer/MAX_VALUE))
      (if (zero? window-end)
        []
        (result-window (sort cmp result) limit offset))
      (let [worst-first
            (reify Comparator
              (compare [_ a b] (.compare cmp b a)))
            ^PriorityQueue heap
            (PriorityQueue. (int (max 1 window-end)) worst-first)]
        (doseq [tuple result]
          (if (< (.size heap) window-end)
            (.add heap tuple)
            (when (neg? (.compare cmp tuple (.peek heap)))
              (.poll heap)
              (.add heap tuple))))
        (result-window (sort cmp (seq heap)) limit offset)))))

(defn order-result
  [find-vars result order limit offset]
  (if (seq result)
    (let [cmp (order-comps (tuple-get (first result)) find-vars order)]
      (if (finite-limit? limit)
        (top-k-result cmp result limit offset)
        (result-window (sort cmp result) limit offset)))
    result))

(defn query-result-size
  [parsed-q result]
  (let [find (:qfind parsed-q)]
    (if (or (instance? FindRel find) (instance? FindColl find))
      (count result)
      (if (nil? result) 0 1))))
