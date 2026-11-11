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
           [datalevin.db.tx.plan MixedAttrInfo MixedInsert MixedOperands MixedUpdate]
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
  [^ArrayList additions options tempids eid tx values a v ^MixedAttrInfo info canonical?]
  (let [props (.-props info)]
    (when (and (.-insert? info) (resolved-value? props tempids v))
      (let [resolved (if (= :db.type/ref (:db/valueType props))
                      (resolved-ref tempids v) v)
            value (if canonical? resolved
                      (prepare/correct-value-with-props options props a resolved))]
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

(defn- append-new-entity!
  [^ArrayList additions ^HashMap infos schema options sources tempids eid tx entity]
  (let [values (reduce-kv
                 (fn [values a v]
                   (if (= :db/id a)
                     values
                     (or (append-new-attribute!
                           additions options tempids eid tx values a v
                           (compile/ordinary-attr-info! infos schema sources a) false)
                         (reduced nil))))
                 {} entity)]
    (append-derived-tuples! additions infos options eid tx values)))

(defn- append-mixed-entity!
  "Bind/correct a symbolic attribute stream and emit its logical additions."
  [^ArrayList additions ^HashMap infos options tempids eid tx ^objects attrs canonical?]
  (loop [offset 0 values {}]
    (if (< offset (alength attrs))
      (when-let [values (append-new-attribute!
                         additions options tempids eid tx values
                         (aget attrs offset) (aget attrs (inc offset))
                         (aget attrs (+ offset 2)) canonical?)]
        (recur (+ offset 3) values))
      (append-derived-tuples! additions infos options eid tx values))))

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

(defn- legacy-preflight-checker
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
                   (compile/distinct-mixed-update? updates e a)))))))))

(defn- preflight-additions
  "Recognize the mixture and validate shapes and attributes before correction.
  Defer metadata work until both shapes occur, keeping pure requests cheap."
  [entities ctx ^HashMap infos]
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
                          (let [compiled-check (compile/mixed-preflight-checker ctx infos updates)]
                            (fn [entity kind]
                              (let [decision (compiled-check entity kind)]
                                (if (identical? decision ::compile/unsupported-mixed-preflight)
                                  ((legacy-preflight-checker
                                     (:schema ctx) (:db/attrTuples (:rschema ctx))
                                     infos updates) entity kind)
                                  decision)))) check)]
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
  [entities ctx max-eid tx ^HashSet updates preflight? ^HashMap infos]
  (let [schema (:schema ctx) options (:opts ctx)
        sources (:db/attrTuples (:rschema ctx))
        additions (ArrayList.)]
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
                (let [^MixedInsert operand (compile/mixed-insert-operands ctx infos entity)
                      next-eid (inc eid)
                      tempids' (cond-> tempids tempid (assoc tempid next-eid))]
                  (when (if operand
                          (append-mixed-entity! additions infos options tempids'
                            next-eid tx (.-attrs operand) false)
                          ;; Preserve correction/decline order for containers,
                          ;; tempids and attributes outside symbolic analysis.
                          (append-new-entity! additions infos schema options sources
                            tempids' next-eid tx entity))
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
                             (and (.-update? info) (some? v)
                                  (compile/distinct-mixed-update? updates e a))))
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

(defn execute-mixed-region
  "Bind compiled mixed operands on the current writer without revisiting forms.
  Declines stale epochs, nonempty attempt state and out-of-bound existing IDs
  before reads. Unique additions are checked before old-value reader setup."
  [report plan owned-max-eid]
  (let [db (:db-after report) store (:store db) ctx (:ctx plan)]
    (when (and (identical? :mixed (:strategy plan))
               (empty? (:tx-data report)) (empty? (:tempids report))
               (identical? (:schema ctx) (i/schema store))
               (identical? (:opts ctx) (i/opts store)))
      (let [^MixedOperands operands (:entries plan)
            ^ArrayList entries (.-entries operands)
            infos (.-infos operands)
            before (max (long (:max-eid db))
                        (long (or owned-max-eid (i/init-max-eid store))))
            tx (inc (long (:max-tx db)))
            additions (ArrayList.)]
        (when (<= (.-required-eid operands) before)
          (loop [index 0 eid before tempids {}]
            (if (< index (.size entries))
              (let [entry (.get entries index)]
                (if (instance? MixedInsert entry)
                  (let [^MixedInsert entry entry
                        next-eid (inc eid)
                        tempid (.-tempid entry)
                        tempids (cond-> tempids tempid (assoc tempid next-eid))]
                    (when (append-mixed-entity!
                            additions infos (:opts ctx) tempids next-eid tx
                            (.-attrs entry) true)
                      (recur (unchecked-inc index) next-eid tempids)))
                  (let [^MixedUpdate entry entry]
                    (.add additions (datom (.-e entry) (.-attr entry) (.-value entry) tx))
                    (recur (unchecked-inc index) eid tempids))))
              (finish-mixed-report
                report db store before tx
                {:additions additions :max-eid eid :tempids tempids :infos infos
                 :prepare-reader? (.-prepare-reader? operands)}))))))))

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
                 ctx (prepare/->PrepareCtx schema rschema options)
                 infos (HashMap.)
                 updates (if preflight?
                           (let [head (seq entities)]
                             ;; A leading identity map already rules out this plan.
                             ;; Reuse its sequence without walking the batch again.
                             (when-not (identity-map? (:db.unique/identity rschema)
                                                       (first head))
                               (preflight-additions head ctx infos)))
                           (HashSet.))]
             (when updates
               (let [before (max (long (:max-eid db))
                                 (long (or owned-max-eid (i/init-max-eid store))))
                     tx (inc (long (:max-tx db)))]
                 (when-let [plan (plan-additions entities ctx before tx updates preflight? infos)]
                   (finish-mixed-report report db store before tx plan)))))))))))
