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
            [datalevin.db.tx.compile :as compile]
            [datalevin.db.tx.prepare :as txprep]
            [datalevin.interface :as i]
            [datalevin.index :as idx]
            [datalevin.prepare :as prepare]
            [datalevin.storage :as storage])
  (:import [datalevin.datom Datom]
           [datalevin.db.tx.plan MixedAttrInfo]
           [java.util ArrayList HashMap HashSet SortedSet]
           [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]))

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
              (let [^MixedAttrInfo info (compile/ordinary-attr-info! infos schema sources a)
                    props (.-props info)]
                (if (and (.-insert? info) (resolved-value? props tempids v))
                  (let [v (prepare/correct-value-with-props
                            options props a
                            (if (= :db.type/ref (:db/valueType props))
                              (resolved-ref tempids v) v))]
                    (.add additions (datom eid a v tx))
                    (if (.-tuples info) (assoc values a v) values))
                  (reduced nil)))))
          {} entity)]
    (when values
      ;; Build each new tuple once from normalized sources. Identity tuples
      ;; and predicates retain the interpreter's ordered resolution path.
      (when (seq values)
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
            nil tuples)))
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

(defn- identity-map?
  [identity-attrs entity]
  (and (map? entity)
       (some #(contains? entity %) identity-attrs)))

(defn- preflight-checker
  [schema sources ^HashMap infos ^HashSet updates]
  (let [insert? (fn [_ a _value]
                  (if (= :db/id a)
                    true
                    (let [^MixedAttrInfo info (compile/ordinary-attr-info! infos schema sources a)]
                      (if (.-insert? info) true (reduced false)))))]
    (fn [entity kind]
      (when-not (meta entity)
        (if (identical? kind :insert)
          (let [tempid (:db/id entity)]
            (and (or (not (contains? entity :db/id))
                     (and (txprep/tempid? tempid)
                          (not (txprep/auto-tempid? tempid))))
                 (> (count entity) (if (contains? entity :db/id) 1 0))
                 (reduce-kv insert? true entity)))
          (when (and (= 4 (count entity)) (= :db/add (nth entity 0)))
            (let [[_ e a v] entity]
              ;; Keep canonical Long E/A keys shared with construction.
              (and (instance? Long e) (pos? (long e)) (keyword? a) (some? v)
                   (.-update? ^MixedAttrInfo (compile/ordinary-attr-info! infos schema sources a))
                   (.add updates [e a])))))))))

(defn- preflight-additions
  "Recognize the mixture and validate shapes and attributes before correction.
  Defer metadata work until both shapes occur, keeping pure requests cheap."
  [entities schema sources ^HashMap infos]
  (let [head (seq entities)]
    (loop [es head index 0 first-kind nil updates nil check nil]
      (if es
        (let [entity (first es)
              kind (cond (map? entity) :insert (vector? entity) :update)]
          (when kind
            (let [mixed? (and first-kind (not (identical? first-kind kind)))
                  initialize? (and mixed? (nil? updates))
                  updates (if initialize? (HashSet.) updates)
                  check (if initialize?
                          (preflight-checker schema sources infos updates) check)]
              (when (and
                      ;; The leading homogeneous prefix has only been classified
                      ;; by shape. Validate its metadata once mixing is proven.
                      (or (not initialize?)
                          (loop [prefix head remaining index]
                            (if (zero? remaining)
                              true
                              (when (check (first prefix) first-kind)
                                (recur (next prefix) (unchecked-dec remaining))))))
                      (or (nil? check) (check entity kind)))
                (recur (next es) (unchecked-inc index) (or first-kind kind)
                       updates check)))))
        updates))))

(defn- plan-additions
  [entities schema options max-eid tx ^HashSet updates preflight?
   ^HashMap infos sources]
  (let [additions (ArrayList.)]
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
                  ^MixedAttrInfo info (compile/ordinary-attr-info! infos schema sources a)
                  props (.-props info)]
              ;; Java set keys must use the datom's canonical Long entity ID.
              ;; Other integer representations need ordered resolution.
              (when (and (or preflight?
                             (and (instance? Long e) (pos? (long e))))
                         (<= (long e) (long max-eid))
                         (or preflight?
                             (and (.-update? info) (some? v) (.add updates [e a]))))
                (.add additions (datom e a (prepare/correct-value-with-props
                                            options props a v) tx))
                (recur (next es) eid tempids
                       (and read-safe? (.-typed? info)))))

            :else nil))
        (cond-> {:additions additions :max-eid eid :tempids tempids :infos infos}
          (and (> (.size updates) 1) read-safe?)
          (assoc :prepare-reader? true))))))

(defn- unique-additions-available?
  "Validate unique additions before preparing any old-value reads. Neither
  request duplicates nor native/pending owners can enter the mixed report."
  [db ^ArrayList additions ^HashMap infos]
  (or (not (compile/ordinary-unique-attrs-present? infos))
      (loop [index 0 seen nil]
        (if (< index (.size additions))
          (let [^Datom addition (.get additions index)
                a (.-a addition)
                ^MixedAttrInfo info (.get infos a)]
            (if (:db/unique (.-props info))
              (let [v (.-v addition)
                    ^HashSet seen (or seen (HashSet.))]
                (when (and (.add seen [a v])
                           (nil? (common/av-first-e db a v)))
                  (recur (unchecked-inc index) seen)))
              (recur (unchecked-inc index) seen)))
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
   (prepare-mixed-report report entities owned-max-eid false))
  ([report entities owned-max-eid shape-checked?]
   ;; Short requests keep their inexpensive mixed-shape check. Large requests
   ;; recognize the mixture while preflighting every form and attribute.
   (let [preflight? (>= (long (bounded-count mixed-preflight-min-count entities))
                        mixed-preflight-min-count)]
     (when (or preflight? shape-checked? (mixed-request? entities))
       (let [db (:db-after report) store (:store db)
             schema (i/schema store) options (i/opts store)]
         (when-not (:auto-entity-time? options)
           (let [rschema (i/rschema store)
                 sources (:db/attrTuples rschema)
                 infos (HashMap.)
                 updates (if preflight?
                           (let [head (seq entities)]
                             ;; A leading identity map already rules out this plan.
                             ;; Reuse its sequence without walking the batch again.
                             (when-not (identity-map? (:db.unique/identity rschema)
                                                       (first head))
                               (preflight-additions head schema sources infos)))
                           (HashSet.))]
             (when updates
               (let [before (max (long (:max-eid db))
                                 (long (or owned-max-eid (i/init-max-eid store))))
                     tx (inc (long (:max-tx db)))]
                 (when-let [plan (plan-additions entities schema options before tx
                                                updates preflight? infos sources)]
                   (let [^ArrayList additions (:additions plan)]
                     (when (unique-additions-available? db additions (:infos plan))
                       (let [read-old (when (:prepare-reader? plan)
                                        (storage/ea-first-v-reader
                                          store idx/avg-buffer->prepared-value))
                             pending? (and read-old (pending-values? db))
                             datoms
                             (loop [index 0 out (transient [])]
                               (if (< index (.size additions))
                                 (let [^Datom addition (.get additions index)
                                       e (.-e addition) a (.-a addition) v (.-v addition)
                                       old (when (<= e before)
                                             (if (and read-old (not pending?))
                                               (read-old e a)
                                               (common/preparation-first-v db e a read-old)))]
                                   (recur (unchecked-inc index)
                                          (cond
                                            (d/value-equal? (idx/logical-value old) v) out
                                            (nil? old) (conj! out addition)
                                            :else (-> out
                                                      (conj! (idx/retraction e a old tx))
                                                      (conj! addition)))))
                                 (persistent! out)))]
                         (assoc report
                                :db-after (assoc db :max-eid (:max-eid plan) :max-tx tx)
                                :tx-data datoms
                                :tempids (assoc (:tempids plan) :db/current-tx tx)))))))))))))))
