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
   [clojure.set :as set]
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
   [datalevin.db DB]
   [datalevin.parser BindTuple FindRel Variable]
   [datalevin.storage Store]))

(def ^:dynamic *aggregate-enrichment?* true)

(defn- dependency-patterns
  [{:keys [parsed-q sources]}]
  (into []
    (keep-indexed
      (fn [idx form]
        (when-let [{:keys [source attr] :as pattern}
                   (data-pattern-parts form '$)]
          (let [database (get sources source)
                props (when (and (instance? DB database)
                                 (instance? Store (:store database))
                                 (not (db/pending-tx-cache? database)))
                        (get (db/-schema database) attr))]
            (when (and (:db/aid props) (not (:db/noindex props))
                       (not= :db.cardinality/many (:db/cardinality props)))
              (assoc pattern :idx idx :form form
                     :unique? (and (:db/unique props)
                                   (contains? #{:db.type/long :db.type/ref
                                                :db.type/string :db.type/keyword
                                                :db.type/symbol :db.type/uuid
                                                :db.type/instant :db.type/boolean}
                                              (:db/valueType props)))))))))
    (:qorig-where parsed-q)))

(defn- dependency-closure
  "Each new variable records one schema-backed proof edge. Cardinality one
  proves entity -> value; declared uniqueness also proves value -> entity."
  [patterns seeds]
  (let [edges (mapcat (fn [{:keys [entity value unique?] :as pattern}]
                        (cond-> [(assoc pattern :from entity :to value)]
                          unique? (conj (assoc pattern :from value :to entity))))
                      patterns)]
    (loop [known (set seeds) proofs {} ordered []]
      (let [[next-known next-proofs next-ordered]
            (reduce
              (fn [[known proofs ordered] {:keys [from to] :as edge}]
                (if (and (known from) (not (known to)))
                  [(conj known to) (assoc proofs to edge) (conj ordered edge)]
                  [known proofs ordered]))
              [known proofs ordered] edges)]
        (if (= known next-known)
          {:known known :proofs proofs :ordered ordered}
          (recur next-known next-proofs next-ordered))))))

(defn- dependency-frontier
  [{:keys [proofs ordered]} outputs]
  (let [needed (loop [pending (seq outputs) seen #{}]
                 (if-let [v (first pending)]
                   (if (seen v)
                     (recur (next pending) seen)
                     (recur (cond-> (next pending)
                              (get proofs v) (conj (:from (get proofs v))))
                            (conj seen v)))
                   seen))]
    (into [] (comp (filter #(needed (:to %)))
                   (map :form) (distinct)) ordered)))

(defn- projection-only?
  [parsed-q ordering-vars value]
  (and (not (ordering-vars value))
       (not-any? #(contains? (clause-vars %) value)
                 [(:qin parsed-q) (:qwith parsed-q) (:qhaving parsed-q)])
       (= 1 (count (filter #(contains? (clause-vars %) value)
                           (:qorig-where parsed-q))))))

(defn plan-aggregate-enrichment
  "Reduce aggregate keys only when retained group keys functionally determine
  projection-only attributes. Keep their EAV existence tests before aggregation;
  ordinary bound lookups reconstruct the selected rows after the result window."
  [{:keys [parsed-q sources] :as context}]
  (let [find (:qfind parsed-q)
        elements (dp/find-elements find)
        limit (:qlimit parsed-q)]
    (if (and *aggregate-enrichment?*
             (instance? FindRel find)
             (some dp/aggregate? elements)
             (every? #(or (instance? Variable %) (dp/aggregate? %)) elements)
             (seq (:qorder parsed-q))
             (some? limit) (pos? (long limit))
             (not (:aggregate-enrichment context)))
      (let [find-vars (vec (dp/find-vars find))
            groups (into #{} (keep #(when (instance? Variable %) (:symbol %))) elements)
            order-indices (mapv (fn [[v direction]]
                                  [(if (integer? v) v
                                     (.indexOf ^java.util.List find-vars v)) direction])
                                (partition 2 (:qorder parsed-q)))
            ordering-vars (into #{} (map #(nth find-vars (first %))) order-indices)
            aggregate-vars (into #{} (mapcat #(dp/find-vars (dp/->FindRel [%])))
                                 (filter dp/aggregate? elements))
            patterns (dependency-patterns context)
            unique-vars (into #{} (map :value) (filter :unique? patterns))
            candidates (into #{}
                         (comp (map :value)
                               (filter #(and (groups %) (not (unique-vars %))
                                             (not (aggregate-vars %))
                                             (projection-only? parsed-q ordering-vars %))))
                         patterns)
            retained (set/difference groups candidates)
            closure (dependency-closure patterns retained)
            deferred (set/intersection candidates (:known closure))]
        (if (seq deferred)
          (let [indices (into [] (keep-indexed #(when-not (deferred (:symbol %2)) %1)) elements)
                inner-elements (mapv elements indices)
                forms (mapv
                        (fn [form]
                          (if-let [{:keys [value]} (data-pattern-parts form '$)]
                            (if (deferred value)
                              (assoc form (dec (count form)) '_) form)
                            form))
                        (:qorig-where parsed-q))
                inner (assoc parsed-q
                             :qfind (dp/->FindRel inner-elements)
                             :qorig-find (mapv (vec (:qorig-find parsed-q)) indices)
                             :qwhere (dp/parse-where forms) :qorig-where forms
                             :qreturn-map nil
                             :qorder (into [] (mapcat (fn [[idx direction]]
                                                      [(.indexOf ^java.util.List indices idx) direction]))
                                           order-indices))
                output-vars (mapv #(if (dp/aggregate? %) (gensym "?aggregate") (:symbol %)) elements)
                binding-vars (mapv output-vars indices)
                source-vars (vec (sort (keys sources)))
                outer-forms (dependency-frontier closure deferred)
                outer (assoc parsed-q
                             :qfind (dp/->FindRel (mapv dp/->Variable output-vars))
                             :qorig-find output-vars :qwith nil :qhaving nil
                             :qorder (vec (mapcat identity order-indices)) :qoffset 0
                             :qin (conj (mapv dp/parse-binding source-vars)
                                        (dp/parse-binding [binding-vars]))
                             :qwhere (dp/parse-where outer-forms) :qorig-where outer-forms)
                summary {:deferred-vars (vec (sort deferred))
                         :group-vars (into [] (keep #(when (instance? Variable %) (:symbol %))) inner-elements)
                         :presence-clauses (into [] (filter #(and (vector? %) (= '_ (peek %)))) forms)
                         :proof {:schema-functional-dependencies true
                                 :presence-before-aggregation true
                                 :projection-only true}}]
            (assoc context :parsed-q inner
                   :aggregate-enrichment {:outer-query outer :source-vars source-vars
                                          :summary summary}))
          context))
      context)))

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
