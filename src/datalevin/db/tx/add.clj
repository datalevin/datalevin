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
  (:require [datalevin.datom :as d :refer [datom]]
            [datalevin.db.tx.common :as common]
            [datalevin.db.tx.prepare :as txprep]
            [datalevin.interface :as i]
            [datalevin.prepare :as prepare]
            [datalevin.storage :as storage])
  (:import [datalevin.datom Datom]
           [java.util ArrayList HashMap HashSet SortedSet]
           [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]))

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

(deftype ^:private MixedAttrInfo
  [props ^boolean ordinary? ^boolean insert? ^boolean update? tuples ^boolean typed?])

(defn- attr-info
  "Classify an attribute once per preparation call. Value correction and
  reference resolution still depend on each entity's values and tempids."
  [^HashMap infos schema sources a]
  (or (.get infos a)
      (let [props (schema a)
            ordinary? (and (ordinary-attr? a props)
                           (ordinary-unique? schema props))
            source-tuples (get sources a)
            ;; Composite tuples cannot themselves be tuple sources, so this
            ;; resolves each derived tuple's metadata without a dependency cycle.
            ordinary-tuples?
            (and ordinary?
                 (reduce-kv
                   (fn [_ tuple _]
                     (let [^MixedAttrInfo info (attr-info infos schema sources tuple)]
                       (if (.-ordinary? info) true (reduced false))))
                   true source-tuples))
            info (MixedAttrInfo.
                   props (boolean ordinary?)
                   (boolean (and ordinary? (not (:db/tupleAttrs props))
                                 ordinary-tuples?))
                   (boolean (and ordinary?
                                 (contains? scalar-types (:db/valueType props))
                                 (nil? (:db/unique props))
                                 (not (:db/tupleAttrs props)) (not source-tuples)))
                   source-tuples (some? (:db/valueType props)))]
        (.put infos a info)
        info)))

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
  [^ArrayList additions ^HashMap infos schema options sources tempids eid tx entity]
  (let [values
        (reduce-kv
          (fn [values a v]
            (if (= :db/id a)
              values
              (let [^MixedAttrInfo info (attr-info infos schema sources a)
                    props (.-props info)]
                (if (and (.-insert? info) (resolved-value? props tempids v))
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
                       (let [^MixedAttrInfo info (.get infos a)]
                         (reduce-kv (fn [tuples tuple _] (assoc tuples tuple true))
                                    tuples (.-tuples info))))
                     {} values)]
        (reduce-kv
          (fn [_ a _]
            (let [^MixedAttrInfo info (.get infos a)
                  props (.-props info)]
              (.add additions
                    (with-meta
                      (datom eid a
                             (prepare/correct-value-with-props
                               options props a
                               (mapv values (:db/tupleAttrs props))) tx)
                      {:datalevin.db.tx.execute/internal true}))))
          nil tuples))
      true)))

(defn- pending-values?
  [db]
  (and common/*batch-prepare*
       (if-let [^LongObjectHashMap pending (common/scalar-pending-index)]
         (not (.isEmpty pending))
         (not (.isEmpty ^SortedSet (:eavt db))))))

(def ^:private ^:const mixed-preflight-min-count
  ;; Short accepted requests do not justify another pass. Larger prefixes can
  ;; discard substantial normalization work when a late form forces fallback.
  32)

(defn- mixed-request?
  [entities]
  (loop [es (seq entities) maps? false vectors? false]
    (if (and maps? vectors?)
      true
      (when es
        (let [entity (first es)]
          (recur (next es) (or maps? (map? entity))
                 (or vectors? (vector? entity))))))))

(defn- preflight-additions
  "Reject structural fallback cases before correcting values. Detailed
  attribute and value eligibility remains with the planner."
  [entities identities]
  (let [updates (HashSet.)
        identity? (fn [_ a _value]
                    (if (contains? identities a) (reduced true) false))]
    (loop [es (seq entities)]
      (if es
        (let [entity (first es)]
          (cond
            (meta entity) nil

            (map? entity)
            (let [tempid (:db/id entity)]
              (when (and (or (not (contains? entity :db/id))
                             (and (txprep/tempid? tempid)
                                  (not (txprep/auto-tempid? tempid))))
                         (> (count entity) (if (contains? entity :db/id) 1 0))
                         (not (reduce-kv identity? false entity)))
                (recur (next es))))

            (and (vector? entity) (= 4 (count entity))
                 (= :db/add (nth entity 0)))
            (let [[_ e a v] entity]
              ;; Match the construction path's canonical datom entity keys.
              (when (and (instance? Long e) (pos? (long e))
                         (keyword? a) (not (contains? identities a))
                         (some? v) (.add updates [e a]))
                (recur (next es))))

            :else nil))
        updates))))

(defn- plan-additions
  [db entities schema options max-eid tx]
  (let [sources (:db/attrTuples (i/rschema (:store db)))
        infos (HashMap.)
        additions (ArrayList.)
        updates (HashSet.)]
    (loop [es (seq entities) eid (long max-eid) tempids {} read-safe? true]
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
                  (when (append-new-entity! additions infos schema options sources
                                           tempids' next-eid tx entity)
                    (recur (next es) next-eid tempids' read-safe?)))))

            (and (vector? entity) (= 4 (count entity))
                 (= :db/add (nth entity 0)))
            (let [[_ e a v] entity
                  ^MixedAttrInfo info (attr-info infos schema sources a)
                  props (.-props info)]
              ;; Java set keys must use the datom's canonical Long entity ID.
              ;; Other integer representations need ordered resolution.
              (when (and (instance? Long e) (pos? (long e))
                         (<= (long e) (long max-eid))
                         (.-update? info)
                         (some? v) (.add updates [(long e) a]))
                (.add additions (datom e a (prepare/correct-value-with-props
                                            options props a v) tx))
                (recur (next es) eid tempids
                       (and read-safe? (.-typed? info)))))

            :else nil))
        (cond-> {:additions additions :max-eid eid :tempids tempids :infos infos}
          ;; Pending lookups already avoid native setup. Keep their shared
          ;; resolver rather than preparing an unused native reader.
          (and (> (.size updates) 1) read-safe? (not (pending-values? db)))
          (assoc :prepare-reader? true))))))

(defn- unique-values-available?
  [db ^HashMap infos ^ArrayList additions]
  (let [seen (HashSet.)]
    (loop [idx 0]
      (if (< idx (.size additions))
        (let [^Datom addition (.get additions idx)
              a (.-a addition) v (.-v addition)
              ^MixedAttrInfo info (.get infos a)]
          (when (or (nil? (:db/unique (.-props info)))
                    (and (.add seen [a v]) (nil? (common/av-first-e db a v))))
            (recur (unchecked-inc idx))))
        true))))

(defn prepare-mixed-report
  "Prepare mixed existing-entity updates and new maps on the owned writer.
  Each existing E/A is written once; new maps own distinct IDs. This makes
  intermediate pending-index maintenance unnecessary. Unsupported shapes and
  uniqueness conflicts return nil without changing either resolver index.
  The caller publishes the resulting datoms through the shared commit path.
  An owned writer may supply its current maximum; other callers scan the store."
  ([report entities]
   (prepare-mixed-report report entities nil))
  ([report entities owned-max-eid]
   ;; Scalar-only and insert-only requests already have preparation paths.
   (when (mixed-request? entities)
     (let [db (:db-after report) store (:store db)
           schema (i/schema store) options (i/opts store)]
       (when (and (not (:auto-entity-time? options))
                  (or (< (long (bounded-count mixed-preflight-min-count entities))
                         mixed-preflight-min-count)
                      (preflight-additions entities
                        (:db.unique/identity (i/rschema store)))))
         (let [before (max (long (:max-eid db))
                           (long (or owned-max-eid (i/init-max-eid store))))
               tx (inc (long (:max-tx db)))]
           (when-let [plan (plan-additions db entities schema options before tx)]
             (let [^ArrayList additions (:additions plan)]
               (when (unique-values-available? db (:infos plan) additions)
                 (let [read-old (when (:prepare-reader? plan)
                                 (storage/ea-first-v-reader store))
                       datoms
                       (loop [idx 0 out (transient [])]
                         (if (< idx (.size additions))
                           (let [^Datom addition (.get additions idx)
                                 e (.-e addition) a (.-a addition) v (.-v addition)
                                 old (when (<= e before)
                                       (if read-old (read-old e a)
                                           (common/ea-first-v db e a)))]
                             (recur (unchecked-inc idx)
                                    (cond
                                      (d/value-equal? old v) out
                                      (nil? old) (conj! out addition)
                                      :else (-> out
                                                (conj! (datom e a old tx false))
                                                (conj! addition)))))
                           (persistent! out)))]
                   (assoc report
                          :db-after (assoc db :max-eid (:max-eid plan) :max-tx tx)
                          :tx-data datoms
                          :tempids (assoc (:tempids plan) :db/current-tx tx))))))))))))
