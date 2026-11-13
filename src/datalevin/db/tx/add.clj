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
            [datalevin.interface :as i]
            [datalevin.index :as idx]
            [datalevin.prepare :as prepare]
            [datalevin.storage :as storage])
  (:import [datalevin.datom Datom]
           [datalevin.db.tx.plan MixedAttrInfo MixedInsert MixedOperands MixedUpdate TxPlan]
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

(defn- append-new-attribute!
  [^ArrayList additions tempids eid tx values a v ^MixedAttrInfo info]
  (let [props (.-props info)]
    (when (and (.-insert? info) (resolved-value? props tempids v))
      (let [value (if (= :db.type/ref (:db/valueType props))
                    (resolved-ref tempids v) v)]
        (.add additions (datom eid a value tx))
        (if (.-tuples info) (assoc values a value) values)))))

(defn- append-derived-tuples!
  [^ArrayList additions ^HashMap infos options eid tx values]
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
    true))

(defn- append-mixed-entity!
  "Bind canonical attributes and emit their resolved logical additions."
  [^ArrayList additions ^HashMap infos options tempids eid tx ^objects attrs]
  (loop [offset 0 values {}]
    (if (< offset (alength attrs))
      (when-let [values (append-new-attribute!
                         additions tempids eid tx values
                         (aget attrs offset) (aget attrs (inc offset))
                         (aget attrs (+ offset 2)))]
        (recur (+ offset 3) values))
      (append-derived-tuples! additions infos options eid tx values))))

(defn- append-mixed-update!
  "Emit one canonical replacement from the compiler plan."
  [^ArrayList additions tx ^MixedUpdate entry]
  (let [a (.-attr entry)
        value (.-value entry)]
    (.add additions
          (datom (.-e entry) a value tx))))

(defn- pending-values?
  [db]
  (and common/*batch-prepare*
       (if-let [^LongObjectHashMap pending (common/scalar-pending-index)]
         (not (.isEmpty pending))
         (not (.isEmpty ^SortedSet (:eavt db))))))

(defn- bind-mixed-additions
  "Bind indexed compiler operands and emit their resolved logical additions."
  [^MixedOperands operands ctx max-eid tx ^HashMap infos]
  (let [^ArrayList entries (.-entries operands)
        options (:opts ctx)
        additions (ArrayList.)]
    (loop [index 0 eid (long max-eid) tempids {}]
      (if (< index (.size entries))
        (let [entry (.get entries index)]
          (if (instance? MixedInsert entry)
            (let [^MixedInsert entry entry
                  tempid (.-tempid entry)
                  next-eid (inc eid)
                  tempids' (cond-> tempids tempid (assoc tempid next-eid))]
              (when (append-mixed-entity! additions infos options tempids'
                      next-eid tx (.-attrs entry))
                (recur (unchecked-inc index) next-eid tempids')))
            (do
              (append-mixed-update! additions tx entry)
              (recur (unchecked-inc index) eid tempids))))
        (cond-> {:additions additions :max-eid eid :tempids tempids :infos infos}
          (.-prepare-reader? operands) (assoc :prepare-reader? true))))))

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

(defn- finish-mixed-report
  [report db store before tx plan]
  (let [before (long before) tx (long tx)
        ^ArrayList additions (:additions plan)]
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
               :tempids (assoc (:tempids plan) :db/current-tx tx))))))

(defn execute-additions
  "Execute a compiler mixed plan with epoch, request-state and ID-bound guards."
  ([report plan] (execute-additions report plan nil))
  ([report plan owned-max-eid]
   (when (instance? TxPlan plan)
     (let [db (:db-after report) store (:store db) ctx (:ctx plan)]
       (when (and (identical? :mixed (:strategy plan))
                  (empty? (:tx-data report)) (empty? (:tempids report))
                  (identical? (:schema ctx) (i/schema store))
                  (identical? (:opts ctx) (i/opts store)))
         (let [^MixedOperands operands (:entries plan)
               infos (.-infos operands)
               before (max (long (:max-eid db))
                           (long (or owned-max-eid (i/init-max-eid store))))
               tx (inc (long (:max-tx db)))]
           (when (<= (.-required-eid operands) before)
             (when-let [bound (bind-mixed-additions operands ctx before tx infos)]
               (finish-mixed-report report db store before tx bound)))))))))
