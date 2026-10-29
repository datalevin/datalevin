;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.storage.scan
  "Datom conversion, indexed tuple scans, and ordered parallel scan chunks."
  (:require
   [datalevin.binding.cpp :as cpp]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.custom-datalog :as cd]
   [datalevin.custom-value :as cv]
   [datalevin.datom :as d]
   [datalevin.index :as idx
    :refer [datom->indexable index->ktype index->vtype gt->datom retrieved->v]]
   [datalevin.interface :as i
    :refer [visit-list-sample visit-list-key-range near-list get-list attrs]]
   [datalevin.lmdb :as lmdb]
   [datalevin.query.predicate :as qpred]
   [datalevin.relation :as r]
   [datalevin.scan :as scan :refer [visit-list*]]
   [datalevin.util :as u :refer [raise]])
  (:import
   [java.util ArrayList List Comparator Collection Map Arrays Iterator]
   [datalevin.utl QueryKey TupleSort]
   [java.util.concurrent Callable ForkJoinPool ForkJoinWorkerThread Future]
   [java.nio ByteBuffer]
   [java.lang AutoCloseable]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
   [datalevin.bits Indexable Retrieved]))

(defn e-aid-v->datom
  [store e-aid-v]
  (d/datom (nth e-aid-v 0) ((attrs store) (nth e-aid-v 1)) (peek e-aid-v)))

(defn retrieved->attr [attrs ^Retrieved r] (attrs (.-a r)))

(defn kv->datom
  [lmdb attrs ^long k ^Retrieved v]
  (let [g (.-g v)]
    (if (= g c/normal)
      (d/datom k (attrs (.-a v)) (retrieved->v lmdb v))
      (gt->datom lmdb g))))

(defn retrieved->datom
  [lmdb attrs [k v :as kv]]
  (when kv
    (if (integer? k)
      (let [r ^Retrieved v]
        (if (.-g r)
          (kv->datom lmdb attrs k r)
          (d/datom (.-e r) (attrs (.-a r)) k)))
      (kv->datom lmdb attrs v k))))

(defn datom-pred->kv-pred
  [lmdb attrs index pred]
  (fn [kv]
    (let [k (b/read-buffer (lmdb/k kv) (index->ktype index))
          v (b/read-buffer (lmdb/v kv) (index->vtype index))]
      (pred (retrieved->datom lmdb attrs [k v])))))

(defn content-scan?
  "Serialized generic keys cannot bound byte-content equality: map/set order
  and host collection representations may differ between equal values."
  [value-type value]
  (and (= value-type :data) (QueryKey/containsBytes value)))

(defn content-entities
  "Match a generic byte-containing value within one attribute's AVE range.
  The caller supplies predicate or join-key equality as appropriate."
  [lmdb aid value equal?]
  (i/list-range-keep
    lmdb c/ave
    (fn [retrieved eid]
      (when (equal? value (retrieved->v lmdb retrieved)) eid))
    [:closed (b/indexable nil aid c/v0 :data c/g0)
     (b/indexable nil aid c/vmax :data c/gmax)]
    :avg [:all] :id false))

(defn content-present?
  "Match within an entity/attribute EAV range, including unindexed values."
  [lmdb aid eid value equal?]
  (boolean
    (i/list-range-some
      lmdb c/eav
      (fn [_ retrieved] (equal? value (retrieved->v lmdb retrieved)))
      [:closed eid eid] :id
      [:closed (b/indexable nil aid c/v0 :data c/g0)
       (b/indexable nil aid c/vmax :data c/gmax)] :avg false)))

(defn- giant-value-equal? [a b] (QueryKey/equiv a b))

(defn scan-giant-av
  "Apply a list-range reader to complete-value matches across giant IDs.
  The low key must use c/g0 as its giant ID.
  Decode both cursor fields before the payload lookup can reuse native buffers."
  ([lmdb scan-fn low] (scan-giant-av lmdb scan-fn low nil))
  ([lmdb scan-fn ^Indexable low eid]
   (let [v (.-v low)]
     (scan-fn lmdb c/ave
              (fn [r e]
                (when (giant-value-equal? v (retrieved->v lmdb r)) e))
              [:closed low
               (Indexable. nil (.-a low) v (.-f low) (.-b low) c/gmax)]
              :avg (if (some? eid) [:closed eid eid] [:all]) :id false))))

(defn- giant-av-match? [lmdb key eid]
  (boolean (scan-giant-av lmdb i/list-range-some key eid)))

(defn av-entities [lmdb schema a v]
  (let [props (schema a)
        vt (idx/storage-type lmdb props)]
    (if (map? vt)
      (cd/exact-entities lmdb (:db/aid props) vt v)
      (let [key (datom->indexable lmdb schema (d/datom c/e0 a v) false)]
        (if (b/giant? key)
          (scan-giant-av lmdb i/list-range-keep key)
          (get-list lmdb c/ave key :avg :id))))))

(defn ave-key-range
  "Encode an attribute/value interval shared by AVE keys and EAV duplicates.
  Callers must reserve encoding scratch and admit any custom codec first."
  [aid vt val-range]
  (let [[[cl lv] [ch hv]] val-range
        op                (cond
                            (and (identical? cl :closed)
                                 (identical? ch :closed)) :closed
                            (identical? ch :closed)       :open-closed
                            (identical? cl :closed)       :closed-open
                            :else                         :open)]
    (if (map? vt)
      [:closed
       (b/indexable nil aid lv vt (if (= cl :closed) cv/min-id cv/max-id))
       (b/indexable nil aid hv vt (if (= ch :closed) cv/max-id cv/min-id))]
      [op (b/indexable nil aid lv vt
                      (if (and (= cl ch :closed) (= lv hv)) c/g0 c/gmax))
       (b/indexable nil aid hv vt c/gmax)])))

(defn ave-tuples-scan*
  [lmdb aid vt val-ranges sample-indices work]
  (doseq [val-range val-ranges]
    (let [[[cl lv] [ch hv]] val-range
          k-range (ave-key-range aid vt val-range)
          ;; Query equality is represented as a closed singleton range. The
          ;; order bucket alone cannot decide complete-value equality.
          work (if (or (map? vt)
                       (and (= cl ch :closed) (= lv hv)
                            (b/giant? (second k-range))))
                 (fn [entry]
                   (let [entry (cd/copy-kv entry)]
                     (when (or (not (and (= cl ch :closed) (= lv hv)))
                               (giant-value-equal?
                                 lv (idx/avg-buffer->v lmdb (lmdb/k entry))))
                       (work entry))))
                 work)]
      (if sample-indices
        (visit-list-sample
         lmdb c/ave sample-indices work k-range :avg :id)
        (visit-list-key-range
         lmdb c/ave work k-range :avg :id)))))

(defn ave-tuples-scan-need-v
  [lmdb ^Collection out aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (let [e (.getLong ^ByteBuffer (lmdb/v kv) 0)
            v (idx/avg-buffer->v lmdb (lmdb/k kv))]
        (.add out (object-array [e v]))))))

(defn- controlled-key! [policy key scan!]
  (if-let [with-key! (:with-key! policy)]
    (with-key! key (fn [key-policy]
                     (when-not (:empty? key-policy) (scan! (merge policy key-policy)))))
    (scan! policy)))

(defn ave-tuples-scan-controlled
  "Read an attribute through the ordinary AVE scan driver with
  explicit row admission and a bounded raw-value visitor. This execution-only
  API neither samples nor populates native tuple/result caches. The policy's
  visitor must complete synchronously before the cursor advances."
  [lmdb ^Collection out aid get-v? {:keys [check! key value-first?] :as policy}]
  (check!)
  (let [failure (volatile! nil)]
    (try
      (controlled-key! policy key
        (fn [{:keys [index-range admit-row! visit-value!]}]
          (let [[key-range key-type] (or index-range
                                          [[:closed-open (int aid) (unchecked-inc-int (int aid))] :int])]
            (visit-list-key-range
              lmdb c/ave
              (fn [kv]
                (try
                  (check!)
                  (let [eid (.getLong ^ByteBuffer (lmdb/v kv) 0)]
                    (if get-v?
                      (visit-value! (lmdb/k kv) eid
                                    (fn [value]
                                      (check!) (admit-row!)
                                      (.add out (object-array (if value-first? [value eid] [eid value])))))
                      (do (admit-row!) (.add out (object-array [eid])))))
                  (catch Throwable e (vreset! failure e) (throw e))))
              key-range key-type :id))))
      ;; The ordinary storage visitor adds native context to failures. Preserve
      ;; the original controlled admission/cancellation error after that scan
      ;; has unwound its cursor and transaction leases.
      (catch Throwable e (throw (or @failure e)))))
  (check!))

(defn ave-value-tuples-scan-controlled
  "Read indexed candidates once for each distinct value key. The policy owns
  bound construction and logical matching; the shared driver owns cursors."
  [lmdb out aid ^List values {:keys [check!] :as policy}]
  (dotimes [i (.size values)]
    (check!)
    (ave-tuples-scan-controlled lmdb out aid true
                               (assoc policy :key (.get values i) :value-first? true)))
  (check!))

(defn eav-tuples-scan-controlled
  "Read a fixed attribute for distinct entity keys through the ordinary list
  range visitor. Match every fact, including many-valued wildcard witnesses;
  never decode an ignored value or invoke a value codec to construct bounds."
  [lmdb ^Collection out aid ^List entities get-v? {:keys [check!] :as policy}]
  (check!)
  (let [failure (volatile! nil)
        attribute-range [:closed-open (int aid) (unchecked-inc-int (int aid))]]
    (try
      (dotimes [i (.size entities)]
        (check!)
        (let [key (.get entities i) eid (aget ^objects key 0)]
          (controlled-key! policy (or (:key policy) key)
            (fn [{:keys [index-range admit-row! visit-value!]}]
              (let [[value-range value-type] (or index-range [attribute-range :int])]
                (i/visit-list-range
                  lmdb c/eav
                  (fn [kv]
                    (try
                      (check!)
                      (if get-v?
                        (visit-value! (lmdb/v kv) eid
                                      (fn [value]
                                        (check!) (admit-row!)
                                        (.add out (object-array (if (:value-first? policy) [value eid] [eid value])))))
                        (do (admit-row!) (.add out (object-array [eid]))))
                      (catch Throwable e (vreset! failure e) (throw e))))
                  [:closed eid eid] :id value-range value-type))))))
      (catch Throwable e (throw (or @failure e)))))
  (check!))

(defn eav-attribute-tuples-scan-controlled
  "Enumerate entity keys for an unindexed attribute, then use the shared
  controlled EAV visitor. Entity keys and payloads are borrowed synchronously;
  no unbounded entity collection or ignored payload decoding is needed."
  [lmdb out aid get-v? {:keys [check!] :as policy}]
  (check!)
  (let [failure (volatile! nil)]
    (try
      (i/visit-key-range lmdb c/eav
        (fn [^ByteBuffer key]
          (try
            (check!)
            (eav-tuples-scan-controlled lmdb out aid
              [(object-array [(.getLong key 0)])] get-v? policy)
            (catch Throwable e (vreset! failure e) (throw e))))
        [:all] :id true)
      (catch Throwable e (throw (or @failure e)))))
  (check!))

(defn ave-tuples-scan-need-v-vpred
  [lmdb ^Collection out vpred aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (let [e (.getLong ^ByteBuffer (lmdb/v kv) 0)
            v (idx/avg-buffer->v lmdb (lmdb/k kv))]
        (when (vpred v)
          (.add out (object-array [e v])))))))

(defn ave-tuples-scan-no-v
  [lmdb ^Collection out aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (.add out (object-array [(.getLong ^ByteBuffer (lmdb/v kv) 0)])))))

(defn ave-tuples-scan-no-v-vpred
  [lmdb ^Collection out vpred aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      ;; Reading a giant value or running a predicate can reuse cursor buffers.
      (let [e (.getLong ^ByteBuffer (lmdb/v kv) 0)
            v (idx/avg-buffer->v lmdb (lmdb/k kv))]
        (when (vpred v)
          (.add out (object-array [e])))))))

(defn sort-tuples-by-eid
  [^List tuples ^long eid-idx]
  (doto tuples
    (TupleSort/sortByLong (int eid-idx))))

(defn sort-tuples-by-val
  [^List tuples ^long v-idx vt]
  (if (or (identical? vt :db.type/ref)
          (identical? vt :db.type/long))
    (sort-tuples-by-eid tuples v-idx)
    (doto tuples
      (.sort (reify Comparator
               (compare [_ a b]
                 (d/compare-with-type (aget ^objects a v-idx)
                                      (aget ^objects b v-idx))))))))

(defn sorted-distinct-tuple-values
  ^long [^List tuples ^long value-idx]
  (let [n (.size tuples)]
    (if (zero? n)
      0
      (loop [i          (long 1)
             last-value (aget ^objects (.get tuples 0) value-idx)
             total      (long 1)]
        (if (== i n)
          total
          (let [value (aget ^objects (.get tuples (int i)) value-idx)]
            (if (== 0 (long (d/compare-with-type value last-value)))
              (recur (u/long-inc i) last-value total)
              (recur (u/long-inc i) value (u/long-inc total)))))))))

(def ^:private ^:const parallel-scan-target-chunk-size 4000)

(defn eav-filter-presence-chunk
  [lmdb ^List in eid-idx aid]
  (let [out      (FastList. (.size in))
        dbi-name c/eav]
    (scan/scan lmdb dbi-name
      (cpp/filter-list-id-int-prefix! rtx cur in eid-idx aid out)
      (raise "Fail to filter EAV attribute presence: " e
               {:eid-idx eid-idx :aid aid}))))

(declare val-eq-scan-e-bound* val-eq-filter-e*)

(defn ave-filter-bound-id-chunk
  [lmdb ^List in value-idx aid value-type bound-id]
  (let [out      (FastList. (.size in))
        dbi-name c/ave]
    (scan/scan lmdb dbi-name
      (if (or (map? value-type)
              (some #(content-scan? value-type (aget ^objects % value-idx)) in))
        (do
          (doseq [^objects tuple in]
            (val-eq-scan-e-bound* lmdb rtx cur out tuple aid
                                  (aget tuple value-idx) value-type bound-id))
          out)
        (cpp/filter-list-avg-bound-id!
          rtx cur in value-idx aid value-type bound-id out lmdb giant-av-match?))
      (raise "Fail to filter AVE by bound entity: " e
               {:value-idx value-idx :aid aid :bound-id bound-id}))))

(defn ave-filter-tuple-id-chunk
  [lmdb ^List in value-idx entity-idx aid value-type]
  (let [out      (FastList. (.size in))
        dbi-name c/ave]
    (scan/scan lmdb dbi-name
      (if (or (map? value-type)
              (some #(content-scan? value-type (aget ^objects % value-idx)) in))
        (do
          (doseq [^objects tuple in]
            (val-eq-filter-e* lmdb rtx cur out tuple aid
                              (aget tuple value-idx) value-type (aget tuple entity-idx)))
          out)
        (cpp/filter-list-avg-tuple-id!
          rtx cur in value-idx entity-idx aid value-type out lmdb giant-av-match?))
      (raise "Fail to filter AVE by tuple entity: " e
               {:value-idx value-idx :entity-idx entity-idx :aid aid}))))

(defn parallel-scan-participant-capacity
  ^long [^long n ^long cpu-count ^long pool-slots]
  (let [cpu-capacity      (max 1 cpu-count)
        executor-capacity (inc (max 0 pool-slots))
        useful-work       (max
                            1
                            (quot (+ n
                                     (dec parallel-scan-target-chunk-size))
                                  parallel-scan-target-chunk-size))]
    (long (min cpu-capacity executor-capacity useful-work))))

(defn parallel-scan-participant-count
  ^long [^long n]
  (let [^ForkJoinPool pool (ForkJoinPool/commonPool)
        ^Thread thread    (Thread/currentThread)
        parallelism      (long (.getParallelism pool))
        pool-slots       (if (and (instance? ForkJoinWorkerThread thread)
                                  (identical?
                                    pool
                                    (.getPool ^ForkJoinWorkerThread thread)))
                           (max 0 (dec parallelism))
                           parallelism)]
    (parallel-scan-participant-capacity
      n (.availableProcessors (Runtime/getRuntime)) pool-slots)))

(defn ordered-parallel-list-chunks
  [^List in ^long requested-participants f]
  (let [n            (.size in)
        participants (long (max 1 (min requested-participants (max 1 n))))]
    (if (== 1 participants)
      (f in)
      (let [^ForkJoinPool pool (ForkJoinPool/commonPool)
            bindings          (get-thread-bindings)
            futures           (ArrayList. (dec participants))]
        (dotimes [offset (dec participants)]
          (let [i     (inc offset)
                start (quot (* i n) participants)
                end   (quot (* (inc i) n) participants)
                chunk (.subList in (int start) (int end))]
            (.add futures
                  (.submit
                    pool
                    ^Callable
                    (reify Callable
                      (call [_]
                        (with-bindings bindings
                          (f chunk))))))))
        (try
          (let [first-end   (quot n participants)
                first-chunk (.subList in 0 (int first-end))
                out         (FastList. n)]
            (.addAll out ^Collection (f first-chunk))
            (dotimes [i (.size futures)]
              (.addAll out ^Collection
                       (.get ^Future (.get futures i))))
            out)
          (catch Throwable e
            (dotimes [i (.size futures)]
              (.cancel ^Future (.get futures i) true))
            (throw e)))))))

(defn group-counts
  [aids]
  (sequence (comp (partition-by identity) (map count)) aids))

(defn group-starts
  [counts]
  (int-array (->> counts (reductions +) butlast (into [0]))))

(defn bounded-attribute-cardinality
  "Count an attribute's distinct AVE keys, stopping at limit + 1. This bounds
  the cost of rejecting a high-cardinality grouping during physical planning."
  [lmdb schema attr limit]
  (let [seen (long-array 1)
        limit (long limit)]
    (i/visit-key-range
      lmdb c/ave
      (fn [_]
        (let [n (inc (aget seen 0))]
          (aset seen 0 n)
          (when (> n limit) :datalevin/terminate-visit)))
      [:closed
       (datom->indexable lmdb schema (d/datom c/e0 attr nil) false)
       (datom->indexable lmdb schema (d/datom c/emax attr nil) true)]
      :avg true)
    (aget seen 0)))

(defn- snapshot-schema-revision [lmdb rtx]
  (let [dbi (i/get-dbi lmdb c/meta false)]
    (lmdb/put-key rtx :schema-revision :attr)
    (if-let [buffer (lmdb/get-kv dbi rtx)]
      (b/read-buffer buffer :long) 0)))

(defn fold-entity-rows
  "Stream required cardinality-one attributes in entity order into a borrowed
  scratch row. `consume!` must not retain the row. Unrequested attributes are
  never decoded; incomplete entities are skipped. The caller supplies sorted
  attribute ids, their output columns, a row with entity in column zero, and
  an invocation-local cancellation check. Return true on completion, or nil
  before delivering any rows if the optional schema revision has changed."
  ([lmdb aids columns row consume! check!]
   (fold-entity-rows lmdb aids columns row consume! check! nil))
  ([lmdb ^ints aids ^ints columns ^objects row consume! check! revision]
   (let [n (alength aids)
         low (aget aids 0)
         high (aget aids (dec n))
         slots (int-array (inc (- high low)))]
     (Arrays/fill slots (int -1))
     (dotimes [i n] (aset-int slots (- (aget aids i) low) (aget columns i)))
     (check!)
     (scan/scan lmdb c/eav
       (when (or (nil? revision) (= revision (snapshot-schema-revision lmdb rtx)))
         (with-open [^AutoCloseable iterator
                     (.iterator ^Iterable
                       (lmdb/iterate-list-key-range-val-full dbi rtx cur [:all] :id))]
           (let [^Iterator iterator iterator
                 borrowed (when (satisfies? lmdb/IBorrowedKVIterator iterator)
                            (lmdb/current-kv iterator))]
             (loop [entity (long -1)
                    seen (long 0)
                    previous-aid (long -1)
                    visited (long 0)]
               (if (.hasNext iterator)
                 (let [entry (or borrowed (.next iterator))
                       eid (.getLong ^ByteBuffer (lmdb/k entry) 0)
                       same? (== entity eid)
                       _ (when (zero? (bit-and visited 4095)) (check!))
                       _ (when (and (not same?) (== seen n))
                           (aset row 0 (Long/valueOf entity))
                           (consume! row))
                       ^ByteBuffer value (lmdb/v entry)
                       aid (.getInt value 0)
                       column (if (and (<= low aid) (<= aid high))
                                (aget slots (- aid low)) -1)
                       read? (and (not (neg? column))
                                  (or (not same?) (not= aid previous-aid)))
                       seen (if same? seen 0)]
                   (when read? (aset row column (idx/avg-buffer->v lmdb value)))
                   (recur eid (if read? (inc seen) seen) aid (inc visited)))
                 (do
                   (when (== seen n)
                     (aset row 0 (Long/valueOf entity))
                     (consume! row))
                   (check!)
                   true))))))
       (throw e)))))

(defn read-entity-values
  "Read and filter one entity's scalar attributes in aid order. Returns a values
  array, :skip when no values are requested, :reject when a predicate or fidx
  check fails, or nil when a required attribute is missing."
  [lmdb iter na nvs ^objects tuple eid-idx ^ints aids ^objects preds
   ^objects fidxs ^booleans skips]
  (let [na (long na)
        te ^long (aget tuple eid-idx)
        vs (when (pos? ^long nvs) (object-array (int nvs)))]
    (loop [next? (lmdb/seek-key iter te :id)
           ai    0
           vi    0]
      (if (and next? (< ^long ai ^long na))
        (let [vb ^ByteBuffer (lmdb/next-val iter)
              a  (.getInt vb 0)]
          (cond
            (== ^int a ^int (aget aids ai))
            (let [pred (aget preds ai)
                  fidx (aget fidxs ai)
                  skip (aget skips ai)]
              (if (and skip (nil? pred) (nil? fidx))
                ;; Presence-only attribute: the value is never needed.
                (recur (and (< (inc ai) na) (lmdb/has-next-val iter))
                       (u/long-inc ai) vi)
                (let [v (idx/avg-buffer->v lmdb vb)]
                  (if (and (or (nil? pred) (pred v))
                           (or (nil? fidx) (QueryKey/equiv v (aget tuple (int fidx)))))
                    (if skip
                      (recur (and (< (inc ai) na) (lmdb/has-next-val iter))
                             (u/long-inc ai) vi)
                      (do (aset ^objects vs (int vi) v)
                          ;; Stop moving the cursor after the final match.
                          (recur (and (< (inc ai) na)
                                      (lmdb/has-next-val iter))
                                 (u/long-inc ai) (u/long-inc vi))))
                    :reject))))
            (< ^int a ^int (aget aids ai))
            (recur (lmdb/has-next-val iter) ai vi)
            ;; Past a required attribute: it is missing, so stop walking.
            :else nil))
        (when (== ^long ai ^long na)
          (if (zero? ^long nvs) :skip vs))))))

(defn read-entity-joined
  "Read one entity's scalar attributes directly into a new tuple prefixed by the
  input tuple. Returns the tuple, :skip when no values are requested, :reject on
  a failed predicate/fidx check, or nil when a required attribute is missing.
  Avoids a separate values array when the entity is not repeated."
  [lmdb iter na nvs ^objects tuple eid-idx ^ints aids ^objects preds
   ^objects fidxs ^booleans skips]
  (let [na (long na)
        te ^long (aget tuple eid-idx)
        base (alength tuple)
        res (when (pos? ^long nvs) (object-array (+ base (int nvs))))]
    (when res (System/arraycopy tuple 0 res 0 base))
    (loop [next? (lmdb/seek-key iter te :id)
           ai    0
           vi    0]
      (if (and next? (< ^long ai ^long na))
        (let [vb ^ByteBuffer (lmdb/next-val iter)
              a  (.getInt vb 0)]
          (cond
            (== ^int a ^int (aget aids ai))
            (let [pred (aget preds ai)
                  fidx (aget fidxs ai)
                  skip (aget skips ai)]
              (if (and skip (nil? pred) (nil? fidx))
                ;; Presence-only attribute: the value is never needed.
                (recur (and (< (inc ai) na) (lmdb/has-next-val iter))
                       (u/long-inc ai) vi)
                (let [v (idx/avg-buffer->v lmdb vb)]
                  (if (and (or (nil? pred) (pred v))
                           (or (nil? fidx) (QueryKey/equiv v (aget tuple (int fidx)))))
                    (if skip
                      (recur (and (< (inc ai) na) (lmdb/has-next-val iter))
                             (u/long-inc ai) vi)
                      (do (aset ^objects res (+ base vi) v)
                          ;; Stop moving the cursor after the final match.
                          (recur (and (< (inc ai) na)
                                      (lmdb/has-next-val iter))
                                 (u/long-inc ai) (u/long-inc vi))))
                    :reject))))
            (< ^int a ^int (aget aids ai))
            (recur (lmdb/has-next-val iter) ai vi)
            ;; Past a required attribute: it is missing, so stop walking.
            :else nil))
        (when (== ^long ai ^long na)
          (if (zero? ^long nvs) :skip res))))))

(defn read-entity-many-values
  "Read and group one entity's multi-valued attributes. Returns the many-tuples
  result or nil when a required attribute has no matching value."
  [lmdb iter na ^objects tuple eid-idx ^ints aids ^objects preds ^objects fidxs
   ^booleans skips ^ints gstarts ^ints gcounts]
  (let [te ^long (aget tuple eid-idx)
        vs (object-array na)
        fa ^int (aget aids 0)
        la ^int (aget aids (dec ^long na))]
    (dotimes [i na] (aset vs i (FastList.)))
    (loop [next? (lmdb/seek-key iter te :id)
           gi    0
           pa    (int (aget aids 0))
           in?   false]
      (when next?
        (let [vb ^ByteBuffer (lmdb/next-val iter)
              a  (.getInt vb 0)]
          (cond
            (neg? (Integer/compare a fa))
            (recur (lmdb/has-next-val iter) gi pa false)
            (not (pos? (Integer/compare a la)))
            (let [gi (if (== pa ^int a)
                       gi
                       (if in? (inc gi) gi))
                  s  (aget gstarts gi)]
              (if (== ^int a ^int (aget aids s))
                (let [v (idx/avg-buffer->v lmdb vb)]
                  (dotimes [i (aget gcounts gi)]
                    (let [aj   (+ s i)
                          pred (aget preds aj)
                          fidx (aget fidxs aj)]
                      (when (and (or (nil? pred) (pred v))
                                 (or (nil? fidx)
                                     (QueryKey/equiv v (aget tuple (int fidx)))))
                        (.add ^FastList (aget vs aj) v))))
                  (recur (lmdb/has-next-val iter) gi (int a) true))
                (recur (lmdb/has-next-val iter) gi pa false)))
            :else :done))))
    (when-not (some #(.isEmpty ^FastList %) vs)
      (r/many-tuples (sequence
                       (comp (map (fn [v s] (when-not s v)))
                          (remove nil?))
                       vs skips)))))

(defn eav-scan-v-single*
  [lmdb iter na nvs ^Collection out ^objects tuple eid-idx
   ^LongObjectHashMap seen ^ints aids ^objects preds ^objects fidxs
   ^booleans skips]
  (let [te ^long (aget tuple eid-idx)
        ts (when seen (.get seen te))]
    (if ts
      (if (identical? ts :skip)
        (.add out tuple)
        (.add out (r/join-tuples tuple ts)))
      (let [x (read-entity-values lmdb iter na nvs tuple eid-idx aids preds
                                  fidxs skips)]
        (cond
          (identical? x :reject) nil
          (nil? x) nil
          (identical? x :skip)
          (do (when seen (.put seen te :skip))
              (.add out tuple))
          :else
          (do (when seen (.put seen te x))
              (.add out (r/join-tuples tuple x))))))))

(defn eav-scan-v-multi*
  [lmdb iter na ^Collection out ^objects tuple eid-idx
   ^LongObjectHashMap seen ^ints aids ^objects preds ^objects fidxs
   ^booleans skips ^ints gstarts ^ints gcounts]
  (let [te ^long (aget tuple eid-idx)
        ts (when seen (.get seen te))]
    (if ts
      (.addAll out (r/prod-tuples (r/single-tuples tuple) ts))
      (let [vst (read-entity-many-values lmdb iter na tuple eid-idx aids preds
                                         fidxs skips gstarts gcounts)]
        (when vst
          (when seen (.put seen te vst))
          (.addAll out (r/prod-tuples (r/single-tuples tuple) vst)))))))

(defn val-eq-scan-e*
  [lmdb iter ^Collection out tuple ^Map seen aid v vt]
  (if-let [ts (.get seen v)]
    (when-not (identical? ts :no-result)
      (.addAll out (r/prod-tuples (r/single-tuples tuple) ts)))
    (let [ts (FastList.)]
      (cond
        (content-scan? vt v)
        (doseq [e (content-entities lmdb aid v #(QueryKey/equals %1 %2))]
          (.add ts (object-array [e])))

        (map? vt)
        (doseq [e (cd/exact-entities lmdb aid vt v)] (.add ts (object-array [e])))

        :else
        (let [key (b/indexable nil aid v vt c/g0)]
          (if (b/giant? key)
            (doseq [e (scan-giant-av lmdb i/list-range-keep key)]
              (.add ts (object-array [e])))
            (visit-list* iter
                         (fn [^ByteBuffer vb]
                           (.add ts (object-array [(.getLong vb 0)])))
                         key :avg vt true))))
      (if (.isEmpty ts)
        (.put seen v :no-result)
        (do (.put seen v ts)
            (.addAll out (r/prod-tuples (r/single-tuples tuple) ts)))))))

(defn val-eq-scan-e-bound*
  [lmdb rtx cur ^Collection out tuple aid v vt bound]
  (when (cond
          (content-scan? vt v)
          (content-present? lmdb aid bound v #(QueryKey/equals %1 %2))
          (map? vt) (some #{bound} (cd/exact-entities lmdb aid vt v))
          :else (cpp/list-avg-id? rtx cur aid v vt bound lmdb giant-av-match?))
    (.add out (r/conj-tuple tuple (long bound)))))

(defn val-eq-filter-e*
  [lmdb rtx cur ^Collection out tuple aid v vt old-e]
  (when (cond
          (content-scan? vt v)
          (content-present? lmdb aid old-e v #(QueryKey/equals %1 %2))
          (map? vt) (some #{old-e} (cd/exact-entities lmdb aid vt v))
          :else (cpp/list-avg-id? rtx cur aid v vt old-e lmdb giant-av-match?))
    (.add out tuple)))

(defn single-attrs?
  [schema attrs-v]
  (let [attrs (mapv first attrs-v)]
    (and (apply distinct? attrs)
         (not-any? #(identical? (-> % schema :db/cardinality)
                                :db.cardinality/many)
                   attrs))))

(defn eav-scan-v-list-chunk
  [lmdb ^List in eid-idx attrs-v single? na nvs ^ints aids ^objects preds
   ^objects fidxs ^booleans skips cache-eids? ^ints gstarts ^ints gcounts]
  (let [nt       (.size in)
        out      (FastList. nt)
        ;; Batch input is sorted by entity id, so equal ids are adjacent. A
        ;; one-slot cache replaces the per-batch hash map where caching is
        ;; permitted, avoiding its allocation and hashing.
        last-eid (when cache-eids? (long-array 1))
        last-val (when cache-eids? (object-array 1))
        cached?  (when cache-eids? (boolean-array 1))
        preds    (qpred/fork-predicates preds)
        dbi-name c/eav]
    (scan/scan lmdb dbi-name
      (with-open [^AutoCloseable iter
                  (lmdb/val-iterator
                    (lmdb/iterate-list-val-full dbi rtx cur))]
        (if single?
          (dotimes [i nt]
            (let [^objects tuple (.get in i)
                  te      (long (aget tuple eid-idx))
                  hit?    (and cached? (aget ^booleans cached? 0)
                               (== te (aget ^longs last-eid 0)))
                  repeat? (and (not hit?) cached?
                               (< (inc i) nt)
                               (== te (long (aget ^objects
                                                  (.get in (inc i)) eid-idx))))
                  direct? (not (or hit? repeat?))
                  x       (cond
                            hit? (aget ^objects last-val 0)
                            repeat?
                            (let [x (read-entity-values
                                      lmdb iter na nvs tuple eid-idx aids preds
                                      fidxs skips)]
                              (when (and cached?
                                         (or (identical? x :skip)
                                             (and (some? x)
                                                  (not (keyword? x)))))
                                (aset ^longs last-eid 0 te)
                                (aset ^objects last-val 0 x)
                                (aset ^booleans cached? 0 true))
                              x)
                            :else
                            (read-entity-joined
                              lmdb iter na nvs tuple eid-idx aids preds fidxs
                              skips))]
              (cond
                (identical? x :reject) nil
                (nil? x) nil
                (identical? x :skip) (.add out tuple)
                direct? (.add out x)
                :else (.add out (r/join-tuples tuple x)))))
          (dotimes [i nt]
            (let [^objects tuple (.get in i)
                  te (long (aget tuple eid-idx))
                  x  (if (and cached? (aget ^booleans cached? 0)
                              (== te (aget ^longs last-eid 0)))
                       (aget ^objects last-val 0)
                       (let [x (read-entity-many-values
                                 lmdb iter na tuple eid-idx aids preds fidxs
                                 skips gstarts gcounts)]
                         (when (and cached? (some? x))
                           (aset ^longs last-eid 0 te)
                           (aset ^objects last-val 0 x)
                           (aset ^booleans cached? 0 true))
                         x))]
              (when (some? x)
                (.addAll out (r/prod-tuples (r/single-tuples tuple) x)))))))
      (raise "Fail to eav-scan-v: " e
               {:eid-idx eid-idx :attrs-v attrs-v}))
    out))

(defn ea->avg-buffer
  [schema lmdb e a]
  (when-let [aid (:db/aid (schema a))]
    (when-let [^ByteBuffer bf (near-list lmdb c/eav e aid :id :int)]
      (when (= ^int aid (.getInt bf 0))
        bf))))

(defn vpred
  [v]
  (cond
    (string? v)  (fn [x] (if (string? x) (.equals ^String v x) false))
    (integer? v) (fn [x] (if (integer? x) (= (long v) (long x)) false))
    (keyword? v) (fn [x] (.equals ^Object v x))
    (nil? v)     (fn [x] (nil? x))
    :else        (fn [x] (QueryKey/equiv v x))))
