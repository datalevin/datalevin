;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.execute.enrichment
  "Planning attribute enrichment after top-k selection."
  (:refer-clojure :exclude [assoc])
  (:require
   [datalevin.built-ins :as built-ins]
   [datalevin.db :as db]
   [datalevin.inline
    :refer [assoc]]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.query.execute.late
    :refer [clause-vars data-pattern-parts late-clause-deps
            strip-clause-source]]
   [datalevin.query.execute.result
    :refer [adaptive-limit-query?]])
  (:import
   [datalevin.parser BindTuple]))

(defn- post-top-k-enrichment-entry
  [clause]
  (let [clause       (strip-clause-source clause)
        call         (when (and (vector? clause) (= 2 (count clause)))
                       (first clause))
        f            (when (sequential? call) (first call))
        [_ source entity else-val & attrs] call
        binding      (when call (dp/parse-binding (second clause)))
        binding-vars (when call
                       (filterv qu/binding-var?
                                (qu/collect-vars (second clause))))]
    (when (and (contains? built-ins/post-top-k-enrichment-fns f)
               (qu/source? source)
               (qu/binding-var? entity)
               (not (qu/binding-var? else-val))
               (seq attrs)
               (every? keyword? attrs)
               (instance? BindTuple binding)
               (= (count binding-vars) (count (distinct binding-vars))))
      (let [{:keys [requires provides]} (late-clause-deps clause)
            entity-requires (disj (set requires) entity)]
        (when (and (empty? entity-requires) (seq provides))
          {:clause   clause
           :function f
           :source   source
           :entity   entity
           :requires (set requires)
           :provides (set provides)})))))

(defn- exclusive-enrichment-outputs?
  [{:keys [qorig-where qin qwith qhaving]} provides ordering-vars]
  (and
    ;; Every output is introduced by exactly this one where clause.
    (every?
      (fn [v]
        (= 1 (count (filter #(contains? (clause-vars %) v) qorig-where))))
      provides)
    ;; Find projection is the only legal consumer. In particular, ordering or
    ;; a later predicate would make enrichment part of row selection.
    (not-any? #(some provides (clause-vars %))
              [qin qwith qhaving])
    (not-any? provides ordering-vars)))

(defn- symbolic-ordering
  [find-vars ordering]
  (into []
        (mapcat
          (fn [[v direction]]
            [(if (integer? v) (nth find-vars v) v) direction]))
        (partition 2 ordering)))

(defn- unique-projected-key-for-entity?
  [{:keys [parsed-q sources]} retained-vars
   {enrichment-source :source entity :entity}]
  (or (contains? retained-vars entity)
      (boolean
        (some
          (fn [clause]
            (when-let [{:keys [source attr value]
                        pattern-entity :entity}
                       (data-pattern-parts clause '$)]
              (when (and (= enrichment-source source)
                         (= entity pattern-entity)
                         (contains? retained-vars value))
                (some-> (get sources source)
                        db/-schema
                        (get attr)
                        :db/unique))))
          (:qorig-where parsed-q)))))

(defn plan-post-top-k-enrichment
  "Defer explicitly total property enrichment until after top-k when its
   outputs are projection-only and a retained entity key proves that early
   distinct cannot change the selected result window."
  [{:keys [parsed-q late-clauses] :as context}]
  (let [find-vars (vec (dp/find-vars (:qfind parsed-q)))
        limit     (:qlimit parsed-q)]
    (if (and (adaptive-limit-query? parsed-q)
             (seq (:qorder parsed-q))
             (some? limit)
             (not= -1 limit)
             (pos? (long limit))
             (= (count find-vars) (count (distinct find-vars))))
      (let [projected     (set find-vars)
            ordering      (symbolic-ordering find-vars (:qorder parsed-q))
            ordering-vars (into #{} (take-nth 2) ordering)
            entries       (into []
                                (comp
                                  (keep post-top-k-enrichment-entry)
                                  (filter
                                    (fn [{:keys [provides]}]
                                      (and (every? projected provides)
                                           (exclusive-enrichment-outputs?
                                             parsed-q provides
                                             ordering-vars)))))
                                late-clauses)
            provides      (into #{} (mapcat :provides) entries)
            requires      (into #{} (mapcat :requires) entries)
            retained      (into #{} (remove provides) find-vars)
            keyed?        (every?
                            #(unique-projected-key-for-entity?
                               context retained %)
                            entries)]
        (if (and (seq entries)
                 keyed?
                 (not-any? provides requires))
          (let [candidate-vars
                (into (vec (remove provides find-vars))
                      (remove retained)
                      (sort-by str requires))]
            (assoc context :post-top-k-enrichment
                   {:clauses        (mapv :clause entries)
                    :functions      (mapv :function entries)
                    :candidate-vars candidate-vars
                    :ordering       ordering
                    :offset         (long (or (:qoffset parsed-q) 0))
                    :limit          (long limit)
                    :proof          {:cardinality-preserving true
                                     :projection-only true
                                     :stable-distinct-key true}}))
          (dissoc context :post-top-k-enrichment)))
      (dissoc context :post-top-k-enrichment))))
