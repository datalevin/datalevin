;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.execute.point-lookup
  "Recognition and execution of unique point-lookup projections."
  (:refer-clojure :exclude [assoc])
  (:require
   [datalevin.db :as db]
   [datalevin.inline :refer [assoc]]
   [datalevin.parser :as dp]
   [datalevin.query.execute.result
    :refer [query-result-size result-explain tuple->persistent-vector]]
   [datalevin.query.plan :as qplan]
   [datalevin.spill :as sp])
  (:import
   [clojure.lang IPersistentCollection]
   [java.util List]
   [datalevin.parser BindScalar Constant DefaultSrc FindRel FindTuple Pattern SrcVar Variable]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def point-lookup-projection-key
  :datalevin.query.execute/point-lookup-projection)

(defn point-lookup-projection-shape
  [parsed-q]
  (let [find          (:qfind parsed-q)
        find-elements (dp/find-elements find)
        find-symbols  (when (and (or (instance? FindRel find)
                                     (instance? FindTuple find))
                                 (<= 2 (count find-elements))
                                 (every? #(instance? Variable %) find-elements))
                        (mapv :symbol find-elements))
        in            (:qin parsed-q)
        source-in     (first in)
        value-in      (second in)
        source-var    (when (instance? BindScalar source-in)
                        (:variable source-in))
        input-var     (when (instance? BindScalar value-in)
                        (:variable value-in))
        input-symbol  (when (instance? Variable input-var)
                        (:symbol input-var))
        patterns      (:qwhere parsed-q)]
    (when (and find-symbols
               (apply distinct? find-symbols)
               (= 2 (count in))
               (instance? SrcVar source-var)
               (= '$ (:symbol source-var))
               input-symbol
               (nil? (:qwith parsed-q))
               (nil? (:qreturn-map parsed-q))
               (empty? (:qhaving parsed-q))
               (nil? (:qorder parsed-q))
               (nil? (:qlimit parsed-q))
               (nil? (:qoffset parsed-q))
               (= (inc (count find-symbols)) (count patterns))
               (every? #(and (instance? Pattern %)
                             (instance? DefaultSrc (:source %))
                             (= 3 (count (:pattern %))))
                       patterns))
      (let [parts
            (mapv
              (fn [pattern]
                (let [[entity attr value] (:pattern pattern)]
                  (when (and (instance? Variable entity)
                             (instance? Constant attr)
                             (keyword? (:value attr))
                             (instance? Variable value))
                    {:entity (:symbol entity)
                     :attr   (:value attr)
                     :value  (:symbol value)})))
              patterns)]
        (when (every? some? parts)
          (let [entities    (set (map :entity parts))
                anchor      (filterv #(= input-symbol (:value %)) parts)
                projections (into {} (map (juxt :value identity)) parts)]
            (when (and (= 1 (count entities))
                       (= 1 (count anchor))
                       (not (contains? (set (map :value parts))
                                       (first entities)))
                       (not (contains? (set find-symbols) input-symbol))
                       (= (set find-symbols)
                          (disj (set (map :value parts)) input-symbol))
                       (every? #(contains? projections %) find-symbols))
              {:find-type (if (instance? FindTuple find) :tuple :relation)
               :entity    (first entities)
               :identity  (:attr (first anchor))
               :input     input-symbol
               :projections
               (mapv #(select-keys (get projections %) [:attr :value])
                     find-symbols)})))))))

(defn prepare-query
  "Attach immutable execution metadata for query shapes with a direct path."
  [parsed-q]
  (if-let [shape (point-lookup-projection-shape parsed-q)]
    (assoc parsed-q point-lookup-projection-key shape)
    parsed-q))

(defn point-lookup-projection-db
  [shape inputs]
  (when (= 2 (count inputs))
    (let [database (first inputs)
          schema   (when (db/db? database) (db/-schema database))
          identity-schema (get schema (:identity shape))]
      (when (and schema
                 (not (db/pending-tx-cache? database))
                 (:db/unique identity-schema)
                 (not= :db.type/ref (:db/valueType identity-schema))
                 (every? #(some? (get-in schema [(:attr %) :db/aid]))
                         (:projections shape)))
        database))))

(defn- point-projection-row
  [^objects tuple ^ints result-indexes]
  (let [width  (alength result-indexes)
        result (object-array width)]
    (dotimes [i width]
      (aset result i (aget tuple (aget result-indexes i))))
    (tuple->persistent-vector result)))

(defn- point-projection-result
  [{:keys [find-type]} ^List tuples ^ints result-indexes]
  (let [size (.size tuples)]
    (case find-type
      :tuple
      (when (pos? size)
        (point-projection-row (.get tuples 0) result-indexes))

      :relation
      (if (zero? size)
        #{}
        (let [result (sp/new-spillable-set nil {:initial-capacity size})]
          (dotimes [i size]
            (.cons ^IPersistentCollection result
                   (point-projection-row (.get tuples i) result-indexes)))
          result)))))

(defn- begin-point-lookup-projection-explain!
  []
  (when qplan/*explain*
    (let [parsing-time (long (or (:parsing-time @qplan/*explain*) 0))]
      (vswap! qplan/*explain* assoc
              :building-time 0
              :planning-time (- (System/nanoTime)
                                (+ ^long qplan/*start-time* parsing-time))))))

(defn- point-lookup-projection-summary
  ([shape]
   {:identity   (:identity shape)
    :attributes (mapv :attr (:projections shape))
    :find-type  (:find-type shape)})
  ([shape output-tuples]
   (assoc (point-lookup-projection-summary shape)
          :output-tuples output-tuples)))

(defn- explain-point-lookup-projection!
  [parsed-q shape result]
  (when qplan/*explain*
    (let [actual-size (query-result-size parsed-q result)]
      (result-explain {:run? true
                       :graph {}
                       :plan nil
                       :result-set nil
                       :opt-clauses []
                       :late-clauses []
                       :access-plans []
                       :explain-actual-result-size actual-size}
                      result)
      (vswap! qplan/*explain* assoc
              :point-lookup-projection
              (point-lookup-projection-summary shape actual-size)
              :executed-plan-alternative {:kind :point-lookup-projection}))))

(defn explain-point-lookup-projection-plan!
  [shape]
  (begin-point-lookup-projection-explain!)
  (result-explain {:run? false
                   :graph {}
                   :plan nil
                   :result-set nil
                   :opt-clauses []
                   :late-clauses []
                   :access-plans []})
  (let [summary {:kind :point-lookup-projection}]
    (vswap! qplan/*explain* assoc
            :point-lookup-projection (point-lookup-projection-summary shape)
            :selected-plan-alternative summary
            :recommended-plan-alternative summary)))

(defn execute-point-lookup-projection
  [parsed-q shape database lookup-value]
  (begin-point-lookup-projection-explain!)
  (let [schema           (db/-schema database)
        scan-projections (vec
                           (sort-by #(get-in schema [(:attr %) :db/aid])
                                    (:projections shape)))
        scan-indexes     (into {}
                               (map-indexed
                                 (fn [idx projection]
                                   [(:value projection)
                                    (unchecked-inc-int (int idx))]))
                               scan-projections)
        result-indexes   (int-array
                           (map #(get scan-indexes (:value %))
                                (:projections shape)))
        tuples
        (if-some [eid (db/entid database [(:identity shape) lookup-value])]
          (let [input   (doto (FastList.) (.add (object-array [eid])))
                attrs-v (mapv (fn [{:keys [attr]}]
                                [attr {:skip? false}])
                              scan-projections)]
            (or (db/-eav-scan-v-list database input 0 attrs-v)
                (FastList.)))
          (FastList.))
        result (point-projection-result shape tuples result-indexes)]
    (explain-point-lookup-projection! parsed-q shape result)
    result))
