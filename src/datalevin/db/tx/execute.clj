;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.db.tx.execute
  "Execute normalized scalar, blind and patch operands against the current writer.
  Raw-form binding lives in tx.bind; ordered/dynamic attempts live in tx.dynamic."
  (:require [datalevin.constants :as c]
            [datalevin.datom :as d :refer [datom]]
            [datalevin.db.tx.compile :as compile]
            [datalevin.db.tx.common :as txcommon]
            [datalevin.db.tx.prepare :as txprep]
            [datalevin.db.tx.plan :as plan]
            [datalevin.storage :as s]
            [datalevin.idoc :as idoc]
            [datalevin.index :as idx]
            [datalevin.interface :refer [opts]]
            [datalevin.prepare :as coreprep]
            [datalevin.validate :as vld])
  (:import [datalevin.datom Datom]
           [datalevin.db.tx.plan BlindEntity ScalarEntityRead ScalarSequence]
           [datalevin.utl QueryKey$EquivSet]
           [clojure.lang LazilyPersistentVector]
           [org.eclipse.collections.impl.list.mutable FastList]
           [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
           [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]
           [java.util Arrays HashSet]))

(defn read-scalar-entity
  [store eid ^ScalarEntityRead layout]
  (first (s/select-entities store [eid] (.-names layout) (.-aids layout)
                            false idx/avg-buffer->prepared-value)))

(defn- read-scalar-update-entity
  [db eid ^ScalarEntityRead layout]
  (let [store (:store db)]
    (if-let [^LongObjectHashMap pending (txcommon/scalar-pending-index)]
      (if-let [^java.util.HashMap pending (.get pending (long eid))]
        (let [^objects names (.-names layout)
              ^longs aids (.-aids layout)
              n (alength names)]
          (loop [idx 0 values {} remaining []]
            (if (< idx n)
              (let [attr (aget names idx)]
                (if-let [^Datom datom (.get pending attr)]
                  (recur (unchecked-inc idx)
                         (assoc values attr (when (d/datom-added datom) (.-v datom)))
                         remaining)
                  (recur (unchecked-inc idx) values (conj remaining idx))))
              (if (empty? values)
                (read-scalar-entity store eid layout)
                (merge (when (seq remaining)
                         (first (s/select-entities
                                  store [eid]
                                  (object-array (map #(aget names %) remaining))
                                  (long-array (map #(aget aids %) remaining))
                                  false idx/avg-buffer->prepared-value)))
                       values)))))
        (read-scalar-entity store eid layout))
      (if (txcommon/resolver-index-active? db)
        (let [^TreeSortedSet eavt (:eavt db)
              ^java.util.SortedSet pending
              (.subSet eavt
                       (datom eid nil nil c/tx0) (datom eid nil nil c/txmax))]
          (if (.isEmpty pending)
            (read-scalar-entity store eid layout)
            (let [^objects names (.-names layout)
                  ^longs aids (.-aids layout)
                  n (alength names)]
              (loop [idx 0 values {} remaining []]
                (if (< idx n)
                  (let [attr (aget names idx)
                        ^java.util.SortedSet pending
                        (.subSet eavt (datom eid attr nil c/tx0)
                                 (datom eid attr nil c/txmax))]
                    (if (.isEmpty pending)
                      (recur (unchecked-inc idx) values (conj remaining idx))
                      ;; Scalar attributes have at most one current addition.
                      ;; Retractions alone mean absent, and never override an
                      ;; addition regardless of index sort order. Seek only the
                      ;; requested attributes, avoiding unrelated pending datoms.
                      (recur (unchecked-inc idx)
                             (assoc values attr (:v (first (filter d/datom-added pending))))
                             remaining)))
                  (if (empty? values)
                    (read-scalar-entity store eid layout)
                    (merge (when (seq remaining)
                             (first (s/select-entities
                                      store [eid]
                                      (object-array (map #(aget names %) remaining))
                                      (long-array (map #(aget aids %) remaining))
                                      false idx/avg-buffer->prepared-value)))
                           values)))))))
        (read-scalar-entity store eid layout)))))

(defn- scalar-update-datoms
  [db entries tx-id entry-offset]
  (let [entry-offset (long entry-offset)
        n (count entries)
        attr-offset (inc entry-offset)
        value-offset (+ 2 entry-offset)]
    (if (= n 1)
      (let [entry (nth entries 0)
            e (nth entry entry-offset)
            attr (nth entry attr-offset)
            value (nth entry value-offset)
            old-value (txcommon/preparation-first-v db e attr)]
        (txcommon/emit-transition conj [] e attr old-value value tx-id))
      (if-let [^objects buffer
               (when (and (<= n 16)
                          (instance? datalevin.db.tx.common.ScalarDatomBuffer
                                     txcommon/*batch-prepare*))
                 (txcommon/acquire-scalar-datom-buffer! txcommon/*batch-prepare* (* 2 n)))]
        (try
          (let [append (fn [out datom]
                         (let [out (long out)]
                           (aset buffer out datom)
                           (unchecked-inc out)))]
            (loop [i 0 out 0]
              (if (< i n)
                (let [entry (nth entries i)
                      e (nth entry entry-offset)
                      attr (nth entry attr-offset)
                      value (nth entry value-offset)
                      old-value (txcommon/preparation-first-v db e attr)]
                  (recur (unchecked-inc i)
                         (long (txcommon/emit-transition
                                 append out e attr old-value value tx-id))))
                ;; The final tail is owned by this request. Never publish the
                ;; scratch array: later stamps reuse it, including after abort.
                (if (zero? out) []
                    (LazilyPersistentVector/createOwning
                      (Arrays/copyOf buffer (int out)))))))
          (finally
            (Arrays/fill buffer (int 0) (int (* 2 n)) nil)
            (txcommon/release-scalar-datom-buffer! txcommon/*batch-prepare* buffer)))
        (loop [i 0 out (transient [])]
          (if (< i n)
            (let [entry (nth entries i)
                  e (nth entry entry-offset)
                  attr (nth entry attr-offset)
                  value (nth entry value-offset)
                  old-value (txcommon/preparation-first-v db e attr)]
              (recur (unchecked-inc i)
                     (txcommon/emit-transition conj! out e attr old-value value tx-id)))
            (persistent! out)))))))

(defn scalar-entities-within-bound?
  "Check the current writer's allocation floor before scalar execution."
  ([entries max-eid] (scalar-entities-within-bound? entries max-eid 0))
  ([entries ^long max-eid entry-offset]
   (loop [i 0]
     (or (= i (count entries))
         (and (<= (long (nth (nth entries i) entry-offset)) max-eid)
              (recur (unchecked-inc i)))))))

(defn execute-scalar-region
  "Read current values and emit one owned logical datom vector. Caller supplies
  validated distinct E/A operands and owns the writer or simulation scope."
  ([db entries layouts tx-id specialize?]
   (execute-scalar-region db entries layouts tx-id specialize? 0))
  ([db entries layouts tx-id specialize? entry-offset]
   (let [entry-offset (long entry-offset)
         n (count entries)
         attr-offset (inc entry-offset)
         value-offset (+ 2 entry-offset)]
     (if (and specialize? (nil? layouts))
       (scalar-update-datoms db entries tx-id entry-offset)
       (let [old-values
             (reduce-kv
              (fn [values eid layout]
                (assoc values eid (read-scalar-update-entity db eid layout)))
              {} layouts)]
         (loop [i 0, out (transient [])]
           (if (< i n)
             (let [entry (nth entries i)
                   e (nth entry entry-offset)
                   attr (nth entry attr-offset)
                   value (nth entry value-offset)
                   ;; Explicit additions select this loop and retain their
                   ;; supplied transaction ID in the next operand slot.
                   tx-id (long (or (nth entry (inc value-offset) nil) tx-id))
                   annotations (nth entry (+ value-offset 2) nil)
                   old-value (if (contains? layouts e)
                               (get (get old-values e) attr)
                               (txcommon/preparation-first-v db e attr))]
               (recur (unchecked-inc i)
                      (txcommon/emit-transition
                        conj! out e attr old-value value tx-id annotations)))
             (persistent! out))))))))

(defn execute-scalar-sequence
  "Execute a typed, concrete-EID replacement chain. Slots forward the prepared
  value between dependent operations; the existing pending view seeds each slot.
  No callbacks, refs, uniqueness or tuple effects enter this region. Every
  logical transition is retained, and no shared pending state is mutated here."
  ([report operands tx-id]
   (execute-scalar-sequence report operands tx-id txcommon/preparation-first-v))
  ([report ^ScalarSequence operands tx-id read-old]
  (let [entries (.-entries operands) ^ints slots (.-slots operands)
        values (object-array (int (.-slot-count operands)))
        initialized (boolean-array (alength values)) db (:db-after report)]
    (loop [i 0 out (transient [])]
      (if (< i (count entries))
        (let [entry (nth entries i) e (nth entry 0) attr (nth entry 1)
              value (nth entry 2) slot (aget slots i)
              old (if (aget initialized slot) (aget values slot)
                      (read-old db e attr))
              before (count out)
              out (txcommon/emit-transition conj! out e attr old value
                    (long (or (nth entry 3 nil) tx-id)) (nth entry 4 nil))]
          ;; Retain physical old-value references on a redundant addition.
          (aset values slot (if (= before (count out)) old value))
          (aset-boolean initialized slot true)
          (recur (unchecked-inc i) out))
        (assoc report :db-after (assoc db :max-tx tx-id) :tx-data (persistent! out)
                      :tempids {:db/current-tx tx-id}))))))

(defn execute-scalar-program
  "Execute typed add/CAS/retract instructions through owned E/A datom slots.
  Initial reads honor the writer's pending view. Forwarded datoms preserve CAS
  diagnostics, giant retraction references and redundant-addition semantics."
  ([report operands tx-id]
   (execute-scalar-program report operands tx-id txcommon/preparation-first-datom))
  ([report ^ScalarSequence operands tx-id read-old]
   (let [entries (.-entries operands) ^ints slots (.-slots operands)
         values (object-array (int (.-slot-count operands)))
         initialized (boolean-array (alength values)) db (:db-after report)]
     (loop [i 0 out (transient [])]
       (if (< i (count entries))
         (let [[e attr opcode match value tx annotations raw-value] (nth entries i)
               slot (aget slots i)
               ^Datom old (if (aget initialized slot) (aget values slot) (read-old db e attr))
               old-value (idx/datom-prepared-value old)
               addition? (#{:add :cas} opcode)
               _ (when (= :cas opcode)
                   (vld/validate-cas-value false e attr match raw-value (when old [old])))
               before (count out)
               out (if addition?
                     (txcommon/emit-transition conj! out e attr old-value value
                       (long (or tx tx-id)) annotations)
                     (if (and old (or (= :clear opcode) (nil? match)
                                     (zero? (long (d/compare-index-values (.-v old) match)))))
                       (conj! out (idx/retraction e attr old-value tx-id)) out))
               changed? (not= before (count out))]
           (aset values slot (if changed? (when addition? (nth out (dec (count out)))) old))
           (aset-boolean initialized slot true)
           (recur (unchecked-inc i) out))
         (assoc report :db-after (assoc db :max-tx tx-id) :tx-data (persistent! out)
                       :tempids {:db/current-tx tx-id}))))))

(defn execute-scalar-report
  "Emit already-bound scalar operands into a fresh adapter-owned report.
  Caller retains epoch, ID-bound and alias admission, transaction ID binding
  and read-layout selection. A nonempty attempt declines before old-value reads."
  ([report entries layouts tx-id specialize?]
   (execute-scalar-report report entries layouts tx-id specialize? 0))
  ([report entries layouts tx-id specialize? entry-offset]
   (when (and (empty? (:tx-data report)) (empty? (:tempids report)))
     (let [db (:db-after report)
           tx-data (execute-scalar-region db entries layouts tx-id specialize? entry-offset)]
       (assoc report :db-after (assoc db :max-tx tx-id) :tx-data tx-data
                     :tempids {:db/current-tx tx-id})))))

(defn execute-blind-inserts
  "Bind normalized new-entity operands and emit datoms without reading raw forms.
  The caller owns the writer, current allocation maxima and epoch validation."
  [report ^FastList entities auto-entity-time?]
  (when (and (empty? (:tx-data report)) (empty? (:tempids report)))
    (let [db                 (:db-after report)
          tx-id              (inc (long (:max-tx db)))
          tx-time            (when auto-entity-time? (System/currentTimeMillis))
          entity-count       (.size entities)
          [tx-data tempids max-eid]
          (loop [i        0
                 tx-data  (transient [])
                 tempids  (transient {})
                 next-eid (long (:max-eid db))]
            (if (< i entity-count)
              (let [^BlindEntity entity (.get entities i)
                    eid      (unchecked-inc next-eid)
                    tempid   (.-tempid entity)
                    ^FastList attrs (.-attrs entity)
                    attr-count (.size attrs)
                    tx-data (cond-> tx-data
                              auto-entity-time?
                              (conj! (Datom. eid :db/created-at tx-time
                                             tx-id 0 nil))

                              auto-entity-time?
                              (conj! (Datom. eid :db/updated-at tx-time
                                             tx-id 0 nil)))
                    tx-data
                    (loop [j 0
                           tx-data tx-data]
                      (if (< j attr-count)
                        (recur (+ j 2)
                               (conj! tx-data
                                      (Datom. eid
                                              (.get attrs j)
                                              (.get attrs (unchecked-inc j))
                                              tx-id 0 nil)))
                        tx-data))
                    tempids (if (and tempid (not (txprep/auto-tempid? tempid)))
                              (assoc! tempids tempid eid)
                              tempids)]
                (recur (unchecked-inc i) tx-data tempids eid))
              [(persistent! tx-data) (persistent! tempids) next-eid]))
          tempids            (assoc tempids :db/current-tx tx-id)
          db-after           (-> db
                                 (assoc :max-eid max-eid)
                                 (assoc :max-tx tx-id))
          result             (assoc report :db-after db-after :tx-data tx-data :tempids tempids)]
      result)))

(defn blind-unique-values-absent?
  "Require distinct normalized unique AVs before probing the current view."
  [db ^FastList avs]
  (and (compile/blind-unique-avs-distinct? avs)
       (or (nil? avs)
           (loop [i 0]
             (or (>= i (.size avs))
                 (let [[a v] (.get avs i)]
                   (and (nil? (txcommon/av-first-e db a v))
                        (recur (unchecked-inc i)))))))))

(defn- append-blind-identity-upsert!
  [db ^BlindEntity entity identity-attr eid tx-id layout tx-data]
  (let [^FastList attrs (.-attrs entity)
        old-values (when layout (read-scalar-entity (:store db) eid layout))]
    (loop [i 0 tx-data tx-data]
      (if (< i (.size attrs))
        (let [attr  (.get attrs i)
              value (.get attrs (unchecked-inc i))]
          (if (= attr identity-attr)
            (recur (+ i 2) tx-data)
            (let [old-value (if layout
                              (get old-values attr)
                              (txcommon/preparation-first-v db eid attr))]
              (recur (+ i 2)
                     (txcommon/emit-transition conj! tx-data eid attr old-value value tx-id)))))
        tx-data))))

(defn execute-blind-identity-upsert
  "Emit one bound identity upsert into a fresh adapter-owned report. Pending
  batches retain point reads; other callers may supply the existing read layout."
  [report ^BlindEntity entity identity-attr eid layout]
  (when (and (empty? (:tx-data report)) (empty? (:tempids report)))
    (let [db (:db-after report)
          tx-id (inc (long (:max-tx db)))
          layout (when-not (txcommon/resolver-index-active? db) layout)
          tx-data (persistent! (append-blind-identity-upsert!
                                 db entity identity-attr eid tx-id layout (transient [])))
          tempid (.-tempid entity)
          tempids (cond-> {:db/current-tx tx-id}
                    (and tempid (not (txprep/auto-tempid? tempid))) (assoc tempid eid))
          db-after (cond-> (assoc db :max-tx tx-id)
                     (> (long eid) (long (:max-eid db))) (assoc :max-eid eid))]
      (assoc report :db-after db-after :tx-data tx-data :tempids tempids))))

(defn execute-blind-upserts
  "Bind every independent identity owner before old-value reads or emission.
  Consume shared normalized operands and optional preproven identity AVs. Return
  an updated fresh report, or nil for ambiguous AVs/owners without changing state."
  [report ^FastList entities ^FastList identity-avs store-schema]
  (when (and (empty? (:tx-data report)) (empty? (:tempids report)))
    (let [db (:db-after report)
          seen-avs (QueryKey$EquivSet.)
          seen-eids (HashSet.)
          ^FastList resolutions (FastList. (.size entities))
          max-eid
          (loop [i 0 max-eid (long (:max-eid db))]
            (if (< i (.size entities))
              (when-let [[a v :as av]
                         (if identity-avs
                           (.get identity-avs i)
                           (compile/blind-identity-upsert-av store-schema
                             (.get entities i)))]
                (when (.add seen-avs (plan/unique-av-key av))
                  (let [stored-eid (txcommon/av-first-e db a v)
                        eid (if stored-eid (long stored-eid) (inc max-eid))]
                    (when (.add seen-eids eid)
                      (.add resolutions [a eid (some? stored-eid)])
                      (recur (inc i) (if stored-eid max-eid eid))))))
              max-eid))]
      (when max-eid
        (let [tx-id (inc (long (:max-tx db)))
              [tx-data tempids]
              (loop [i 0 tx-data (transient []) tempids (transient {})]
                (if (< i (.size entities))
                  (let [^BlindEntity entity (.get entities i)
                        [identity-attr eid stored?] (.get resolutions i)
                        ^FastList attrs (.-attrs entity)
                        tx-data (if stored?
                                  (append-blind-identity-upsert!
                                    db entity identity-attr eid tx-id nil tx-data)
                                  (loop [j 0 tx-data tx-data]
                                    (if (< j (.size attrs))
                                      (recur (+ j 2)
                                             (conj! tx-data
                                                    (d/datom eid (.get attrs j)
                                                             (.get attrs (inc j)) tx-id)))
                                      tx-data)))
                        tempid (.-tempid entity)]
                    (recur (inc i) tx-data
                           (if (and tempid (not (txprep/auto-tempid? tempid)))
                             (assoc! tempids tempid eid) tempids)))
                  [(persistent! tx-data)
                   (assoc! tempids :db/current-tx tx-id)]))]
          (assoc report :db-after (assoc db :max-eid max-eid :max-tx tx-id)
                 :tx-data tx-data :tempids (persistent! tempids)))))))

(defn prepare-patch-addition
  "Apply one eligible nonempty patch to its known old document. Return the
  validated, corrected addition with patch-path metadata, without document reads
  or publication. A nil source retains dynamic addition-form error context.
  The standalone adapter retains its live-options checkpoint after patch demand."
  ([ctx entry old-doc tx-id]
   (prepare-patch-addition ctx entry old-doc tx-id nil))
  ([ctx [e attr operations source] old-doc tx-id store]
   (let [{raw-doc :doc paths :paths}
         (idoc/apply-patch (or (idx/logical-value old-doc) {}) operations)
         annotations {:idoc/patch {:paths paths}}
         entity (or source (with-meta [:db/add e attr raw-doc] annotations))]
     (vld/validate-val raw-doc entity)
     (with-meta
       (datom e attr (coreprep/correct-value-with-props
                      (if store (opts store) (:opts ctx))
                      (get (:schema ctx) attr) attr raw-doc) tx-id)
       annotations))))

(defn- execute-patch-document
  [initial-report prepared e old-doc]
  (let [db (:db-after initial-report)
        entry (assoc (first (:entries prepared)) 0 e)
        [_ attr ops] entry
        tx-id       (inc (long (:max-tx db)))
        base-report (assoc initial-report :db-after (assoc db :max-tx tx-id)
                                           :tx-data [] :tempids {:db/current-tx tx-id})]
    (if (empty? ops)
      base-report
      (let [add (prepare-patch-addition (:ctx prepared) entry old-doc tx-id (:store db))
            db-after (cond-> (:db-after base-report)
                       (nil? old-doc)
                       (assoc :max-eid
                              (max (long (:max-eid db)) (long e))))
            tx-data (cond
                      (nil? old-doc) [add]
                      (= (idx/logical-value old-doc) (.-v ^Datom add)) []
                      :else [(idx/retraction e attr old-doc tx-id) add])]
        (assoc base-report :db-after db-after :tx-data tx-data)))))

(defn execute-patch-region
  "Execute a single-document patch plan under the current writer. Empty patches
  keep their no-read behavior; report metadata and logical patch paths survive."
  [initial-report prepared]
  (let [db (:db-after initial-report)
        [eid attr ops] (first (:entries prepared))
        e (txcommon/entid-strict db eid)
        before (when-not (empty? ops)
                 (idx/datom-prepared-value (txcommon/ea-first-datom db e attr)))]
    (execute-patch-document initial-report prepared e before)))

(defn clear-tx-cache
  "Clear request-owned resolver overlays outside collected preparation."
  [db]
  (when-not txcommon/*batch-prepare*
    (.clear ^TreeSortedSet (:eavt db))
    (.clear ^TreeSortedSet (:avet db)))
  db)

(defn stage-compiled-report!
  "Populate the existing request-local EAV/AVE view from finalized logical datoms.
  Queryable preparation uses this outside batch preparation; commit-only writer
  callers retain the existing commit path's pending-state staging."
  ([report] (stage-compiled-report! report false))
  ([report preserve-overlays?]
  (let [db (:db-after report)
        db (if preserve-overlays? db (clear-tx-cache db))]
    (doseq [datom (:tx-data report)] (txcommon/stage-batch-datom! db datom))
    report)))

