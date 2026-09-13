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
   [datalevin.interface
    :refer [visit-list-sample visit-list-key-range near-list get-list attrs]]
   [datalevin.lmdb :as lmdb]
   [datalevin.query.predicate :as qpred]
   [datalevin.relation :as r]
   [datalevin.scan :as scan :refer [visit-list*]]
   [datalevin.util :as u :refer [raise]])
  (:import
   [java.util ArrayList List Comparator Collection HashMap]
   [java.util.concurrent Callable ForkJoinPool ForkJoinWorkerThread Future]
   [java.nio ByteBuffer]
   [java.lang AutoCloseable]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
   [datalevin.bits Retrieved]))

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

(defn av-entities [lmdb schema a v]
  (let [props (schema a)
        vt (idx/storage-type lmdb props)]
    (if (map? vt)
      (cd/exact-entities lmdb (:db/aid props) vt v)
      (get-list lmdb c/ave (datom->indexable lmdb schema (d/datom c/e0 a v) false)
                :avg :id))))

(defn- ave-key-range
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
      [op (b/indexable nil aid lv vt c/gmax) (b/indexable nil aid hv vt c/gmax)])))

(defn ave-tuples-scan*
  [lmdb aid vt val-ranges sample-indices work]
  (doseq [val-range val-ranges]
    (let [[[cl lv] [ch hv]] val-range
          ;; Query equality is represented as a closed singleton range. The
          ;; order bucket alone cannot decide complete-value equality.
          work (if (map? vt)
                 (fn [entry]
                   (let [entry (cd/copy-kv entry)]
                     (when (or (not (and (= cl ch :closed) (= lv hv)))
                               (= lv (idx/avg-buffer->v lmdb (lmdb/k entry))))
                       (work entry))))
                 work)]
      (if sample-indices
        (visit-list-sample
         lmdb c/ave sample-indices work (ave-key-range aid vt val-range) :avg :id)
        (visit-list-key-range
         lmdb c/ave work (ave-key-range aid vt val-range) :avg :id)))))

(defn ave-tuples-scan-need-v
  [lmdb ^Collection out aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (let [e (.getLong ^ByteBuffer (lmdb/v kv) 0)
            v (idx/avg-buffer->v lmdb (lmdb/k kv))]
        (.add out (object-array [e v]))))))

(defn ave-tuples-scan-need-v-vpred
  [lmdb ^Collection out vpred aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (let [v (idx/avg-buffer->v lmdb (lmdb/k kv))]
        (when (vpred v)
          (.add out (object-array [(.getLong ^ByteBuffer (lmdb/v kv) 0)
                                  v])))))))

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
      (let [v (idx/avg-buffer->v lmdb (lmdb/k kv))]
        (when (vpred v)
          (.add out (object-array [(.getLong ^ByteBuffer (lmdb/v kv) 0)])))))))

(defn sort-tuples-by-eid
  [^List tuples ^long eid-idx]
  (doto tuples
    (.sort (reify Comparator
             (compare [_ a b]
               (Long/compare ^long (aget ^objects a eid-idx)
                             ^long (aget ^objects b eid-idx)))))))

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

(defn ave-filter-bound-id-chunk
  [lmdb ^List in value-idx aid value-type bound-id]
  (let [out      (FastList. (.size in))
        dbi-name c/ave]
    (scan/scan lmdb dbi-name
      (if (map? value-type)
        (do
          (doseq [^objects tuple in]
            (when (some #{bound-id} (cd/exact-entities lmdb aid value-type (aget tuple value-idx)))
              (.add out (r/conj-tuple tuple (long bound-id)))))
          out)
        (cpp/filter-list-avg-bound-id!
          rtx cur in value-idx aid value-type bound-id out))
      (raise "Fail to filter AVE by bound entity: " e
               {:value-idx value-idx :aid aid :bound-id bound-id}))))

(defn ave-filter-tuple-id-chunk
  [lmdb ^List in value-idx entity-idx aid value-type]
  (let [out      (FastList. (.size in))
        dbi-name c/ave]
    (scan/scan lmdb dbi-name
      (if (map? value-type)
        (do
          (doseq [^objects tuple in]
            (when (some #{(aget tuple entity-idx)}
                        (cd/exact-entities lmdb aid value-type (aget tuple value-idx)))
              (.add out tuple)))
          out)
        (cpp/filter-list-avg-tuple-id!
          rtx cur in value-idx entity-idx aid value-type out))
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
      (let [vs (object-array (int nvs))]
        (loop [next? (lmdb/seek-key iter te :id)
               ai    0
               vi    0]
          (if (and next? (< ^long ai ^long na))
            (let [vb ^ByteBuffer (lmdb/next-val iter)
                  a  (.getInt vb 0)]
              (if (== ^int a ^int (aget aids ai))
                (let [v    (idx/avg-buffer->v lmdb vb)
                      pred (aget preds ai)
                      fidx (aget fidxs ai)]
                  (if (and (or (nil? pred) (pred v))
                           (or (nil? fidx) (= v (aget tuple (int fidx)))))
                    (if (aget skips ai)
                      (recur (lmdb/has-next-val iter) (u/long-inc ai) vi)
                      (do (aset vs (int vi) v)
                          (recur (lmdb/has-next-val iter) (u/long-inc ai)
                                 (u/long-inc vi))))
                    :reject))
                (recur (lmdb/has-next-val iter) ai vi)))
            (when (== ^long ai ^long na)
              (if (zero? ^long nvs)
                (do (when seen (.put seen te :skip))
                    (.add out tuple))
                (do (when seen (.put seen te vs))
                    (.add out (r/join-tuples tuple vs)))))))))))

(defn eav-scan-v-multi*
  [lmdb iter na ^Collection out ^objects tuple eid-idx
   ^LongObjectHashMap seen ^ints aids ^objects preds ^objects fidxs
   ^booleans skips ^ints gstarts ^ints gcounts]
  (let [te ^long (aget tuple eid-idx)
        ts (when seen (.get seen te))]
    (if ts
      (.addAll out (r/prod-tuples (r/single-tuples tuple) ts))
      (let [vs (object-array na)
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
                                         (= v (aget tuple (int fidx)))))
                            (.add ^FastList (aget vs aj) v))))
                      (recur (lmdb/has-next-val iter) gi (int a) true))
                    (recur (lmdb/has-next-val iter) gi pa false)))
                :else :done))))
        (when-not (some #(.isEmpty ^FastList %) vs)
          (let [vst (r/many-tuples (sequence
                                     (comp (map (fn [v s] (when-not s v)))
                                        (remove nil?))
                                     vs skips))]
            (when seen (.put seen te vst))
            (.addAll out (r/prod-tuples (r/single-tuples tuple)
                                        vst))))))))

(defn val-eq-scan-e*
  [lmdb iter ^Collection out tuple ^HashMap seen aid v vt]
  (if-let [ts (.get seen v)]
    (when-not (identical? ts :no-result)
      (.addAll out (r/prod-tuples (r/single-tuples tuple) ts)))
    (let [ts (FastList.)]
      (if (map? vt)
        (doseq [e (cd/exact-entities lmdb aid vt v)] (.add ts (object-array [e])))
        (visit-list* iter
                     (fn [^ByteBuffer vb]
                       (.add ts (object-array [(.getLong vb 0)])))
                     (b/indexable nil aid v vt nil) :avg vt true))
      (if (.isEmpty ts)
        (.put seen v :no-result)
        (do (.put seen v ts)
            (.addAll out (r/prod-tuples (r/single-tuples tuple) ts)))))))

(defn val-eq-scan-e-bound*
  [lmdb rtx cur ^Collection out tuple aid v vt bound]
  (when (if (map? vt)
          (some #{bound} (cd/exact-entities lmdb aid vt v))
          (cpp/list-avg-id? rtx cur aid v vt bound))
    (.add out (r/conj-tuple tuple (long bound)))))

(defn val-eq-filter-e*
  [lmdb rtx cur ^Collection out tuple aid v vt old-e]
  (when (if (map? vt)
          (some #{old-e} (cd/exact-entities lmdb aid vt v))
          (cpp/list-avg-id? rtx cur aid v vt old-e))
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
        seen     (when cache-eids? (LongObjectHashMap. nt))
        preds    (qpred/fork-predicates preds)
        dbi-name c/eav]
    (scan/scan lmdb dbi-name
      (with-open [^AutoCloseable iter
                  (lmdb/val-iterator
                    (lmdb/iterate-list-val-full dbi rtx cur))]
        (if single?
          (dotimes [i nt]
            (eav-scan-v-single*
              lmdb iter na nvs out (.get in i) eid-idx seen aids preds fidxs
              skips))
          (dotimes [i nt]
            (eav-scan-v-multi*
              lmdb iter na out (.get in i) eid-idx seen aids preds fidxs skips
              gstarts gcounts))))
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
    :else        (fn [x] (= v x))))
