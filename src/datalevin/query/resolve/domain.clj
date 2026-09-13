;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.resolve.domain
  "Candidate planning for singleton-domain scans and bound-value presence fusion."
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.inline :refer [update assoc]]
   [datalevin.query-util :as qu]
   [datalevin.query.resolve.context :refer [bound-vars rel-with-attr]]
   [datalevin.query.resolve.pattern
    :refer [multi-lookup-safety-limit resolve-entity-pairs]]
   [datalevin.util :as u])
  (:import
   [java.util HashSet List]))

(def ^:private ^:const ^long singleton-domain-min-consumer-size 64)

(defn- indexed-clause-pattern
  [context clause]
  (cond
    (and (vector? clause) (= 3 (count clause)))
    {:source  qu/*implicit-source*
     :pattern clause}

    (and (vector? clause)
         (= 4 (count clause))
         (qu/source? (first clause)))
    {:source  (get (:sources context) (first clause))
     :pattern (subvec clause 1)}))

(defn singleton-domain-candidate-values
  "Return value variables that occur in two indexed patterns with distinct
  entity variables and the same source. This structural prepass lets ordinary
  property-enrichment conjunctions bypass runtime-domain planning entirely."
  [context clauses]
  (let [groups
        (reduce
          (fn [groups clause]
            (if-let [{:keys [source pattern]}
                     (indexed-clause-pattern context clause)]
              (let [[entity attr value] pattern]
                (if (and (some? source)
                         (db/-searchable? source)
                         (keyword? attr)
                         (qu/binding-var? entity)
                         (not (qu/placeholder? entity))
                         (qu/binding-var? value)
                         (not (qu/placeholder? value))
                         (not= entity value))
                  (update groups [source value] (fnil conj #{}) entity)
                  groups))
              groups))
          {}
          clauses)]
    (into #{}
          (keep (fn [[[ _ value] entities]]
                  (when (< 1 (count entities)) value)))
          groups)))

(defn singleton-domain-planning-input?
  "Whether the current context is large enough for runtime-domain planning to
  repay even its structural prepass. The exact distinct-size guard is applied
  later once a compatible pattern pair has been found."
  [context]
  (some (fn [rel]
          (let [^List tuples (:tuples rel)]
            (and tuples
                 (<= singleton-domain-min-consumer-size (.size tuples)))))
        (:rels context)))

(defn singleton-domain-candidate-clause?
  [context candidate-values clause]
  (when (seq candidate-values)
    (when-let [{[_ _ value] :pattern}
               (indexed-clause-pattern context clause)]
      (contains? candidate-values value))))

(defn- bound-value-scan-pattern
  [context idx clause]
  (when-let [{:keys [source pattern] :as indexed}
             (indexed-clause-pattern context clause)]
    (let [[entity attr value] pattern
          delta-bound         (:delta-bound-values context)
          entity-rel          (when (qu/binding-var? entity)
                                (rel-with-attr context entity))
          value-rel           (when (qu/binding-var? value)
                                (rel-with-attr context value))]
      (when (and (some? source)
                 (db/-searchable? source)
                 (keyword? attr)
                 (qu/binding-var? entity)
                 (not (qu/placeholder? entity))
                 (qu/binding-var? value)
                 (not (qu/placeholder? value))
                 (not= entity value)
                 entity-rel
                 (nil? value-rel)
                 (not (contains? delta-bound value)))
        (assoc indexed
               :idx idx
               :clause clause
               :entity entity
               :attr attr
               :value value
               :entity-rel entity-rel)))))

(defn- relation-domain-shape
  "Classify one relation column as singleton or multiple without building its
  complete distinct-value set. Multiple columns normally stop after two rows."
  [rel sym]
  (let [idx          (long ((:attrs rel) sym))
        ^List tuples (:tuples rel)
        n            (.size tuples)]
    (when (pos? n)
      (let [value (aget ^objects (.get tuples 0) idx)]
        (loop [i (long 1)]
          (cond
            (== i n) {:kind :singleton, :value value}
            (= value (aget ^objects (.get tuples (int i)) idx))
            (recur (u/long-inc i))
            :else {:kind :multiple}))))))

(defn- singleton-domain-count
  [{:keys [source attr entity-values]}]
  (when-let [[_ owner] (first (resolve-entity-pairs source entity-values))]
    (long (db/-count source [owner attr nil]))))

(defn- singleton-domain-pair
  [left right]
  (when (and (identical? (:source left) (:source right))
             (= (:value left) (:value right))
             (not= (:entity left) (:entity right)))
    (let [left-shape  (relation-domain-shape (:entity-rel left)
                                              (:entity left))
          right-shape (relation-domain-shape (:entity-rel right)
                                              (:entity right))
          [domain consumer]
          (cond
            (and (= :singleton (:kind left-shape))
                 (= :multiple (:kind right-shape)))
            [left right]

            (and (= :singleton (:kind right-shape))
                 (= :multiple (:kind left-shape)))
            [right left])
          domain-shape   (if (identical? domain left)
                           left-shape right-shape)
          consumer-values
          (when consumer
            (qu/relation-distinct-values (:entity-rel consumer)
                                         (:entity consumer)))
          domain-values
          (when domain
            (doto (HashSet.) (.add (:value domain-shape))))
          domain          (when domain
                            (assoc domain
                                   :entity-values domain-values
                                   :entity-count 1))
          consumer        (when consumer
                            (assoc consumer
                                   :entity-values consumer-values
                                   :entity-count
                                   (.size ^HashSet consumer-values)))]
      (when domain
        (when-some [domain-count (singleton-domain-count domain)]
          (let [domain-count   (long domain-count)
                consumer-count (long (:entity-count consumer))]
            (when (and (<= domain-count (long c/sip-range-threshold))
                       (<= singleton-domain-min-consumer-size consumer-count)
                       (or (zero? domain-count)
                           (<= (* domain-count
                                  (long c/sip-ratio-threshold))
                               consumer-count))
                       (<= consumer-count multi-lookup-safety-limit))
              {:domain      domain
               :consumer    consumer
               :domain-cost domain-count})))))))

(defn singleton-domain-plan
  [context pending ^long selected-idx]
  (when-let [selected (bound-value-scan-pattern
                        context selected-idx (nth pending selected-idx))]
    (->> pending
         (keep-indexed
           (fn [idx clause]
             (when (not= idx selected-idx)
               (when-let [{candidate-source :source
                           [candidate-entity _ candidate-value] :pattern}
                          (indexed-clause-pattern context clause)]
                 ;; Most bound property-enrichment clauses produce unrelated
                 ;; values. Reject those before inspecting relation columns or
                 ;; constructing a full candidate descriptor.
                 (when (and (identical? (:source selected) candidate-source)
                            (= (:value selected) candidate-value)
                            (not= (:entity selected) candidate-entity))
                   (when-let [candidate (bound-value-scan-pattern
                                          context idx clause)]
                     (singleton-domain-pair selected candidate)))))))
         (sort-by :domain-cost)
         first)))

(defn bound-value-expansion
  [context clause]
  (when-let [{:keys [source pattern] :as indexed}
             (indexed-clause-pattern context clause)]
    (let [[entity attr value] pattern
          bound               (bound-vars context)
          delta-bound         (:delta-bound-values context)]
      (when (and (some? source)
                 (db/-searchable? source)
                 (keyword? attr)
                 (qu/binding-var? entity)
                 (not (qu/placeholder? entity))
                 (qu/binding-var? value)
                 (not (qu/placeholder? value))
                 (not= entity value)
                 (not (or (contains? bound entity)
                          (contains? delta-bound entity)))
                 (or (contains? bound value)
                     (contains? delta-bound value)))
        (assoc indexed :entity entity)))))

(defn- entity-presence-clause
  [context {:keys [source entity]} clause]
  (when-let [{presence-source :source
              [presence-entity attr value] :pattern
              :as indexed}
             (indexed-clause-pattern context clause)]
    (when (and (identical? source presence-source)
               (= entity presence-entity)
               (keyword? attr)
               (or (= value '_) (qu/placeholder? value)))
      (assoc indexed :clause clause))))

(defn contiguous-entity-presence-clauses
  [context expansion pending ^long producer-idx]
  (loop [idx      (u/long-inc producer-idx)
         presence []]
    (if (< idx (count pending))
      (if-let [clause (entity-presence-clause
                        context expansion (nth pending idx))]
        (recur (u/long-inc idx) (conj presence clause))
        presence)
      presence)))
