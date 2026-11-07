;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.db.tx.add
  "Prepare independent ordinary additions without interpreter or index mutations."
  (:require [datalevin.datom :refer [datom]]
            [datalevin.db.tx.common :as common]
            [datalevin.db.tx.prepare :as txprep]
            [datalevin.interface :as i]
            [datalevin.prepare :as prepare])
  (:import [datalevin.datom Datom]
           [java.util ArrayList HashMap HashSet]))

(def ^:private scalar-types
  #{nil :db.type/long :db.type/string :db.type/keyword :db.type/symbol
    :db.type/float :db.type/double :db.type/boolean :db.type/instant
    :db.type/uuid :db.type/bytes :db.type/bigint :db.type/bigdec})

(def ^:private unique-types
  #{:db.type/boolean :db.type/instant :db.type/keyword :db.type/long
    :db.type/string :db.type/symbol :db.type/uuid :db.type/ref})

(defn- ordinary-unique? [schema props]
  (or (nil? (:db/unique props))
      (contains? unique-types (:db/valueType props))
      (and (= :db.type/tuple (:db/valueType props))
           (every? unique-types
                   (or (some->> (:db/tupleAttrs props)
                                (map #(:db/valueType (schema %))))
                       (:db/tupleTypes props)
                       [(:db/tupleType props)])))))

(defn- ordinary-attr? [a props]
  (and props (keyword? a) (not= "db" (namespace a))
       (not (common/reverse-ref? a))
       (not= :db.cardinality/many (:db/cardinality props))
       (not= :db.unique/identity (:db/unique props))
       (not (:db.attr/preds props))
       (or (contains? scalar-types (:db/valueType props))
           (#{:db.type/ref :db.type/tuple} (:db/valueType props)))))

(defn- resolved-ref [tempids value]
  (if (and (integer? value) (not (neg? (long value))))
    value
    (get tempids value)))

(defn- resolved-value? [props tempids v]
  (and (some? v)
       (case (:db/valueType props)
         :db.type/ref (some? (resolved-ref tempids v))
         :db.type/tuple
         (and (vector? v)
              (every? (fn [[t x]]
                        (or (not= :db.type/ref t)
                            (and (integer? x) (not (neg? (long x))))))
                      (map vector (or (:db/tupleTypes props)
                                      (repeat (:db/tupleType props))) v)))
         true)))

(defn- append-new-entity!
  [^ArrayList additions schema options sources tempids eid tx entity]
  (let [values
        (reduce-kv
          (fn [values a v]
            (if (= :db/id a)
              values
              (let [props (schema a)]
                (if (and (ordinary-attr? a props)
                         (ordinary-unique? schema props)
                         (not (:db/tupleAttrs props))
                         (resolved-value? props tempids v)
                         (every? #(and (ordinary-attr? % (schema %))
                                       (ordinary-unique? schema (schema %)))
                                 (keys (get sources a))))
                  (let [v (prepare/correct-value-with-props
                            options props a
                            (if (= :db.type/ref (:db/valueType props))
                              (resolved-ref tempids v) v))]
                    (.add additions (datom eid a v tx))
                    (assoc values a v))
                  (reduced nil)))))
          {} entity)]
    (when values
      ;; Build each new tuple once from normalized sources. Identity tuples
      ;; and predicates retain the interpreter's ordered resolution path.
      (let [tuples (reduce-kv
                     (fn [tuples a _]
                       (reduce-kv (fn [tuples tuple _] (assoc tuples tuple true))
                                  tuples (get sources a)))
                     {} values)]
        (reduce-kv
          (fn [_ a _]
            (.add additions
                  (with-meta
                    (datom eid a
                           (prepare/correct-value-with-props
                             options (schema a) a
                             (mapv values (:db/tupleAttrs (schema a)))) tx)
                    {:datalevin.db.tx.execute/internal true})))
          nil tuples))
      true)))

(defn- plan-additions
  [db entities schema options max-eid tx]
  (let [sources (:db/attrTuples (i/rschema (:store db)))
        additions (ArrayList.)
        updates (HashMap.)]
    (loop [es (seq entities) eid (long max-eid) tempids {} read-safe? true n-updates 0]
      (if es
        (let [entity (first es)]
          (cond
            (meta entity) nil

            (map? entity)
            (let [tempid (:db/id entity)]
              (when (and (or (not (contains? entity :db/id))
                             (and (txprep/tempid? tempid)
                                  (not (txprep/auto-tempid? tempid))))
                         (not (contains? tempids tempid))
                         (> (count entity) (if (contains? entity :db/id) 1 0)))
                (let [next-eid (inc eid)
                      tempids' (cond-> tempids tempid (assoc tempid next-eid))]
                  (when (append-new-entity! additions schema options sources
                                           tempids' next-eid tx entity)
                    (recur (next es) next-eid tempids' read-safe? n-updates)))))

            (and (vector? entity) (= 4 (count entity))
                 (= :db/add (nth entity 0)))
            (let [[_ e a v] entity props (schema a)]
              (when (and (integer? e) (pos? (long e)) (<= (long e) (long max-eid))
                         (ordinary-attr? a props)
                         (contains? scalar-types (:db/valueType props))
                         (nil? (:db/unique props))
                         (not (:db/tupleAttrs props)) (not (get sources a))
                         (some? v)
                         (let [entity-id (long e)
                               ^HashSet attributes
                               (or (.get updates entity-id)
                                   (let [attributes (HashSet. 4)]
                                     (.put updates entity-id attributes)
                                     attributes))]
                           (.add attributes a)))
                (.add additions (datom e a (prepare/correct-value-with-props
                                            options props a v) tx))
                (recur (next es) eid tempids
                       (and read-safe? (some? (:db/valueType props)))
                       (inc n-updates))))

            :else nil))
        (cond-> {:additions additions :updates updates :max-eid eid :tempids tempids}
          (and (> n-updates 1) read-safe?)
          (assoc :prepare-reader? true))))))

(defn- unique-values-available?
  [db schema ^ArrayList additions]
  (let [seen (HashSet.)]
    (loop [idx 0]
      (if (< idx (.size additions))
        (let [^Datom addition (.get additions idx)
              a (.-a addition) v (.-v addition)]
          (when (or (nil? (:db/unique (schema a)))
                    (and (.add seen [a v]) (nil? (common/av-first-e db a v))))
            (recur (unchecked-inc idx))))
        true))))

(defn prepare-mixed-report
  "Prepare mixed existing-entity updates and new maps on the owned writer.
  Each existing E/A is written once; new maps own distinct IDs. This makes
  intermediate pending-index maintenance unnecessary. Unsupported shapes and
  uniqueness conflicts return nil without changing either resolver index.
  The caller publishes the resulting datoms through the shared commit path."
  [report entities read-old-values]
  ;; Scalar-only and insert-only requests already have preparation paths.
  (when (and (some map? entities) (some vector? entities))
    (let [db (:db-after report) store (:store db)
          schema (i/schema store) options (i/opts store)]
      (when-not (:auto-entity-time? options)
        (let [before (max (long (:max-eid db)) (long (i/init-max-eid store)))
              tx (inc (long (:max-tx db)))]
          (when-let [plan (plan-additions db entities schema options before tx)]
            (let [^ArrayList additions (:additions plan)]
              (when (unique-values-available? db schema additions)
                (let [^HashMap updates (:updates plan)
                      n-entities (.size updates)
                      ^HashMap old-values (read-old-values db updates)
                      read-old (when (and (:prepare-reader? plan)
                                          (< (.size old-values) n-entities))
                                 (common/ea-first-v-reader db))
                      datoms
                      (loop [idx 0 out (transient [])]
                        (if (< idx (.size additions))
                          (let [^Datom addition (.get additions idx)
                                e (.-e addition) a (.-a addition) v (.-v addition)
                                old (when (<= e before)
                                      (if-let [values (.get old-values e)]
                                        (get values a)
                                        (if read-old (read-old e a)
                                            (common/ea-first-v db e a))))]
                            (recur (unchecked-inc idx)
                                   (cond
                                     (= old v) out
                                     (nil? old) (conj! out addition)
                                     :else (-> out
                                               (conj! (datom e a old tx false))
                                               (conj! addition)))))
                          (persistent! out)))]
                  (assoc report
                         :db-after (assoc db :max-eid (:max-eid plan) :max-tx tx)
                         :tx-data datoms
                         :tempids (assoc (:tempids plan) :db/current-tx tx)))))))))))
