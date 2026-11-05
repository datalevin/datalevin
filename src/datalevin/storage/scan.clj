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
   [datalevin.binding.cpp.iter :as cpp-iter]
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
   [datalevin.bits Indexable Retrieved]
   [datalevin.cpp Cursor EavProjection EavStringCache]))

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

(def ^:dynamic *project-entity-rows?*
  "Use native EAV projection when the binding and uncompressed DBI support it."
  true)

(def ^:dynamic *selective-materialization?*
  "Choose native EAV materialization once per fused scan. The default :auto
  compares requested fields with the anchor population's estimated row width.
  Bind true/false to force selective/full materialization for diagnostics.
  The datalevin.eav.selective-materialization JVM property accepts auto, true,
  or false. Direct folds without an estimate conservatively use full mode."
  (case (System/getProperty "datalevin.eav.selective-materialization" "auto")
    "auto" :auto
    "true" true
    "false" false
    (throw (IllegalArgumentException.
             "datalevin.eav.selective-materialization must be auto, true, or false"))))

(def ^:dynamic *cache-string-values?*
  "Reuse short decoded strings within one projected entity scan. Bind false
  for diagnostic comparisons; caches never survive a scan or read snapshot."
  true)

(defn- first-short-string?
  [lmdb key-range]
  (let [short? (volatile! false)]
    (i/visit-key-range lmdb c/ave
      (fn [^ByteBuffer key]
        (let [n (.limit key)]
          (vreset! short? (and (<= 7 n EavStringCache/MAX_KEY_BYTES)
                              (= c/type-string (.get key 4))
                              (= c/false-value (.get key (dec n))))))
        :datalevin/terminate-visit)
      key-range :avg true)
    @short?))

(defn string-value-cache-capacity
  "Compute advisory dictionary capacity from distinct AVE keys and the first
  encoded key's width. The caller can memoize this metadata by attribute."
  [lmdb schema attr]
  (let [key-range
        [:closed
         (datom->indexable lmdb schema (d/datom c/e0 attr nil) false)
         (datom->indexable lmdb schema (d/datom c/emax attr nil) true)]
        cardinality (long (i/key-range-count lmdb c/ave key-range :avg))]
    (if (and (<= 1 cardinality EavStringCache/MAX_ENTRIES)
             (first-short-string? lmdb key-range))
      cardinality 0)))

(defn string-value-cache-plan
  "Choose bounded string dictionaries. capacity-for supplies attribute-cached
  metadata; runtime misses always retain the ordinary decoder."
  [schema attributes capacity-for]
  (when *cache-string-values?*
    (let [capacities
          (int-array
            (map (fn [attr]
                   (if (= :db.type/string (get-in schema [attr :db/valueType]))
                     (capacity-for attr)
                     0))
                 attributes))]
      (when (some pos? capacities)
        {:capacities capacities
         :summary {:max-entries EavStringCache/MAX_ENTRIES
                   :max-key-bytes EavStringCache/MAX_KEY_BYTES
                   :attributes (into {} (keep (fn [[attr capacity]]
                                                (when (pos? (long capacity)) [attr capacity])))
                                     (map vector attributes capacities))}}))))

(defn- sampled-entity-widths
  [lmdb schema anchor population-size]
  (when (pos? (long population-size))
    (let [n (min 8 (long population-size))
          last-index (dec (long population-size))
          denominator (max 1 (dec n))
          step (quot last-index denominator)
          remainder (rem last-index denominator)
          ;; Divide before multiplying so offsets stay within the long range.
          indices (long-array
                    (map (fn [^long i]
                           (+ (* i step) (quot (* i remainder) denominator)))
                         (range n)))
          entities (volatile! [])]
      ;; Spread the bounded sample across the anchor population: early entity
      ;; ids can have a different shape (including co-located entity types).
      (i/visit-list-sample lmdb c/ave indices
        (fn [entry]
          (vswap! entities conj (.getLong ^ByteBuffer (lmdb/v entry) 0)))
        [:closed
         (datom->indexable lmdb schema (d/datom c/e0 anchor nil) false)
         (datom->indexable lmdb schema (d/datom c/emax anchor nil) true)]
        :avg :id true)
      (mapv #(long (i/list-count lmdb c/eav % :id)) @entities))))

(defn entity-materialization-plan
  "Estimate row width from at most eight entities carrying the anchor attribute.
  Count EAV duplicates, including cardinality-many values, without materializing
  whole rows. The smallest observed width conservatively estimates the
  population rather than treating unrelated schema attributes as one row type.
  This is advisory costing only; it never changes row eligibility or results."
  [lmdb schema anchor requested population-size]
  (let [dbi (i/get-dbi lmdb c/eav false)
        native? (and *project-entity-rows?*
                     (nil? (cpp-iter/dbi-key-codec dbi))
                     (nil? (cpp-iter/dbi-value-codec dbi)))
        auto? (= :auto *selective-materialization?*)
        widths (when (and native? auto? (EavProjection/selectiveAvailable))
                 (sampled-entity-widths lmdb schema anchor population-size))
        width (when (seq widths) (reduce min widths))
        ;; End-to-end controls admit 12-value rows, while smaller wide-string
        ;; rows can regress despite cheaper JNI arguments. Keep dense rows full.
        selective? (if auto?
                     (and width (<= 12 (long width)) (< (* 2 (long requested)) (long width)))
                     (true? *selective-materialization?*))
        mode (cond
               (not native?) :ordinary
               (and selective? (EavProjection/selectiveAvailable)) :selective
               (EavProjection/available false) :full
               :else :ordinary)]
    {:mode mode :requested-attributes requested
     :estimated-row-values width :sampled-entities (count widths)
     :decision (if auto? :estimated :override)}))

(defn- fold-projected-uncached-rows
  [^EavProjection projection lmdb ^ints columns ^objects row consume! check! entity-value?]
  (let [n (alength columns)]
    (loop [visited (long 0)]
      (when (zero? (bit-and visited 4095)) (check!))
      (let [status (.advance projection)]
        (if (neg? status)
          (do (check!) true)
          (do
            (when (pos? status)
              (when entity-value?
                (aset row 0 (Long/valueOf (.entity projection))))
              (dotimes [i n]
                (aset row (aget columns i)
                      (idx/avg-buffer->v lmdb (.value projection i))))
              (consume! row))
            (recur (inc visited))))))))

(defn- fold-projected-cached-rows
  [^EavProjection projection lmdb ^ints columns ^objects row consume! check! entity-value?
   ^objects caches ^ints active]
  (let [n (alength columns)]
    (loop [visited (long 0)]
      (if (zero? (aget active 0))
        (fold-projected-uncached-rows projection lmdb columns row consume! check! entity-value?)
        (do
          (when (zero? (bit-and visited 4095)) (check!))
          (let [status (.advance projection)]
            (if (neg? status)
              (do (check!) true)
              (do
                (when (pos? status)
                  (when entity-value?
                    (aset row 0 (Long/valueOf (.entity projection))))
                  (dotimes [i n]
                    (let [value (.value projection i)
                          ^EavStringCache cache (aget caches i)]
                      (aset row (aget columns i)
                            (if cache
                              (or (.lookup cache value)
                                  (let [decoded (idx/avg-buffer->v lmdb value)]
                                    (if (.isEnabled cache)
                                      (.remember cache decoded)
                                      (do
                                        (aset caches i nil)
                                        (aset active 0 (dec (aget active 0)))
                                        decoded))))
                              (idx/avg-buffer->v lmdb value)))))
                  (consume! row))
                (recur (inc visited))))))))))

(defn- fold-projected-entity-rows
  [^EavProjection projection lmdb ^ints columns ^objects row consume! check! entity-value?
   ^ints string-cache-capacities]
  (with-open [projection projection]
    (if (and *cache-string-values?* string-cache-capacities)
      (let [n (alength columns)
            caches (object-array n)
            active (int-array 1)]
        (dotimes [i n]
          (let [capacity (aget string-cache-capacities i)]
            (when (pos? capacity)
              (aset caches i (EavStringCache. capacity c/type-string c/false-value))
              (aset active 0 (inc (aget active 0))))))
        (fold-projected-cached-rows projection lmdb columns row consume! check! entity-value?
                                    caches active))
      (fold-projected-uncached-rows projection lmdb columns row consume! check! entity-value?))))

(defn- fold-unprojected-entity-rows
  [dbi rtx cur lmdb ^ints aids ^ints columns ^objects row consume! check! entity-value?
   entity-range]
  (let [n (alength aids)
        low (aget aids 0)
        high (aget aids (dec n))
        slots (int-array (inc (- high low)))]
    (Arrays/fill slots (int -1))
    (dotimes [i n] (aset-int slots (- (aget aids i) low) (aget columns i)))
    (with-open [^AutoCloseable iterator
                (.iterator ^Iterable
                  (lmdb/iterate-list-key-range-val-full
                    dbi rtx cur
                    (if-let [[start end] entity-range]
                      (if end [:closed-open start end] [:at-least start])
                      [:all]) :id))]
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
                      (when entity-value? (aset row 0 (Long/valueOf entity)))
                      (consume! row))
                  ^ByteBuffer value (lmdb/v entry)
                  aid (.getInt value 0)
                  column (if (and (<= low aid) (<= aid high))
                           (aget slots (- aid low)) (int -1))
                  read? (and (not (neg? column))
                             (or (not same?) (not= aid previous-aid)))
                  seen (if same? seen 0)]
              (when read? (aset row column (idx/avg-buffer->v lmdb value)))
              (recur eid (if read? (inc seen) seen) aid (inc visited)))
            (do
              (when (== seen n)
                (when entity-value? (aset row 0 (Long/valueOf entity)))
                (consume! row))
              (check!)
              true)))))))

(defn fold-entity-rows
  "Stream required cardinality-one attributes in entity order into a borrowed
  scratch row. `consume!` must not retain the row. Unrequested attributes are
  never decoded; incomplete entities are skipped. The caller supplies sorted
  attribute ids, their output columns, a row with entity in column zero, and
  an invocation-local cancellation check. Return true on completion, or nil
  before delivering any rows if the optional schema revision has changed.
  Set entity-value? false only when the consumer does not read column zero."
  ([lmdb aids columns row consume! check!]
   (fold-entity-rows lmdb aids columns row consume! check! nil))
  ([lmdb aids columns row consume! check! revision]
   (fold-entity-rows lmdb aids columns row consume! check! revision true))
  ([lmdb aids columns row consume! check! revision entity-value?]
   (fold-entity-rows lmdb aids columns row consume! check! revision entity-value? nil))
  ([lmdb ^ints aids ^ints columns ^objects row consume! check! revision entity-value?
    string-cache-capacities]
   (fold-entity-rows lmdb aids columns row consume! check! revision entity-value?
                     string-cache-capacities nil))
  ([lmdb ^ints aids ^ints columns ^objects row consume! check! revision entity-value?
    string-cache-capacities entity-range]
   (check!)
   (scan/scan lmdb c/eav
     (when (or (nil? revision) (= revision (snapshot-schema-revision lmdb rtx)))
       (if (and *project-entity-rows?*
                (EavProjection/available (true? *selective-materialization?*))
                (nil? (cpp-iter/dbi-key-codec dbi))
                (nil? (cpp-iter/dbi-value-codec dbi)))
         (fold-projected-entity-rows (if-let [[start end] entity-range]
                                      (EavProjection. ^Cursor cur aids (long start)
                                                       (long (or end -1)))
                                      (EavProjection. ^Cursor cur aids
                                                       (true? *selective-materialization?*)))
                                    lmdb columns row consume! check! entity-value?
                                    string-cache-capacities)
         (fold-unprojected-entity-rows dbi rtx cur lmdb aids columns row
                                       consume! check! entity-value? entity-range)))
     (throw e))))

(defn entity-ranges
  "Partition an EAV walk at entity boundaries sampled by datom rank. This
  balances stored work even with sparse IDs and never splits one entity."
  [lmdb participants]
  (let [total (long (i/entries lmdb c/eav))
        boundaries (volatile! [0])]
    (when (pos? total)
      (i/visit-list-sample lmdb c/eav
        (long-array (map #(quot (* total (long %)) (long participants))
                         (range 1 participants)))
        (fn [entry] (vswap! boundaries conj (.getLong ^ByteBuffer (lmdb/k entry) 0)))
        [:all] :id :raw true))
    (mapv vec (partition 2 1 (conj (vec (distinct @boundaries)) nil)))))

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

(defn visit-ave-entities
  "Visit entity ids for fixed-width indexed value keys using one cursor.
   The caller supplies the pinned aid/type and checks cancellation. Returning
   :datalevin/terminate-visit stops only the current key's witness scan."
  [lmdb aid vt ^List keys check! visitor]
  (scan/scan lmdb c/ave
    (with-open [^AutoCloseable iter
                (lmdb/val-iterator (lmdb/iterate-list-val-full dbi rtx cur))]
      (dotimes [i (.size keys)]
        (check!)
        (let [value (aget ^objects (.get keys i) 0)]
          (loop [next? (lmdb/seek-key iter (b/indexable nil aid value vt c/g0) :avg)]
            (when next?
              (check!)
              (let [^ByteBuffer value-buffer (lmdb/next-val iter)
                    result (visitor value (.getLong value-buffer 0))]
                (when-not (identical? result :datalevin/terminate-visit)
                  (recur (lmdb/has-next-val iter)))))))))
    (throw e)))

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
