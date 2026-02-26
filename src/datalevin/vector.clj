;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.vector
  "Vector indexing and search"
  (:require
   [datalevin.lmdb :as l]
   [datalevin.util :as u :refer [raise]]
   [datalevin.spill :as sp]
   [datalevin.constants :as c]
   [datalevin.async :as a]
   [datalevin.remote :as r]
   [datalevin.bits :as b]
   [datalevin.interface :as i]
   [datalevin.txlog :as txlog]
   [clojure.string :as s]
   [taoensso.nippy :as nippy])
  (:import
   [datalevin.dtlvnative DTLV DTLV$usearch_index_t]
   [datalevin.cpp VecIdx VecIdx$SearchResult VecIdx$IndexInfo]
   [datalevin.spill SpillableMap]
   [datalevin.async IAsyncWork]
   [datalevin.remote KVStore]
   [datalevin.interface IAdmin IVectorIndex]
   [java.io File FileOutputStream FileInputStream DataOutputStream
    DataInputStream]
   [java.util Arrays Map]
   [java.util.concurrent.atomic AtomicLong]
   [java.util.concurrent.locks ReentrantReadWriteLock]
   [org.bytedeco.javacpp BytePointer]))

(defn- metric-key->type
  [k]
  (int (case k
         :custom      DTLV/usearch_metric_unknown_k
         :cosine      DTLV/usearch_metric_cos_k
         :dot-product DTLV/usearch_metric_ip_k
         :euclidean   DTLV/usearch_metric_l2sq_k
         :haversine   DTLV/usearch_metric_haversine_k
         :divergence  DTLV/usearch_metric_divergence_k
         :pearson     DTLV/usearch_metric_pearson_k
         :jaccard     DTLV/usearch_metric_jaccard_k
         :hamming     DTLV/usearch_metric_hamming_k
         :tanimoto    DTLV/usearch_metric_tanimoto_k
         :sorensen    DTLV/usearch_metric_sorensen_k)))

(defn- scalar-kind
  [k]
  (int (case k
         :custom  DTLV/usearch_scalar_unknown_k
         :double  DTLV/usearch_scalar_f64_k
         :float   DTLV/usearch_scalar_f32_k
         :float16 DTLV/usearch_scalar_f16_k
         :int8    DTLV/usearch_scalar_i8_k
         :byte    DTLV/usearch_scalar_b1_k)))

(defn index-fname
  [lmdb domain]
  (str (i/env-dir lmdb) u/+separator+ domain c/vector-index-suffix))

(defn- create-index
  [dimensions metric-key quantization connectivity expansion-add
   expansion-search]
  (VecIdx/create
    ^long dimensions
    ^int (metric-key->type metric-key)
    ^int (scalar-kind quantization)
    ^long connectivity
    ^long expansion-add
    ^long expansion-search))

(defn- ->array
  [quantization vec-data]
  (case quantization
    :double       (double-array vec-data)
    :float        (float-array vec-data)
    :float16      (short-array vec-data)
    (:int8 :byte) (byte-array vec-data)))

(defn- arr-len
  [quantization arr]
  (case quantization
    :double       (alength ^doubles arr)
    :float        (alength ^floats arr)
    :float16      (alength ^shorts arr)
    (:int8 :byte) (alength ^bytes arr)))

(defn- validate-arr
  [^long dimensions quantization arr]
  (if (== dimensions ^long (arr-len quantization arr))
    arr
    (raise "Expect a " dimensions " dimensions vector" {})))

(defn- vec->arr
  [^long dimensions quantization vec-data]
  (if (u/array? vec-data)
    (validate-arr dimensions quantization vec-data)
    (if (== dimensions (count vec-data))
      (->array quantization vec-data)
      (raise "Expect a " dimensions " dimensions vector" {}))))

(defn- add
  [index quantization ^long k arr]
  (case quantization
    :double  (VecIdx/addDouble index k ^doubles arr)
    :float   (VecIdx/addFloat index k ^floats arr)
    :float16 (VecIdx/addShort index k ^shorts arr)
    :int8    (VecIdx/addInt8 index k ^bytes arr)
    :byte    (VecIdx/addByte index k ^bytes arr)))

(defn- remove-id
  [index ^long k]
  (VecIdx/remove index k))

(defn- search
  [index query quantization top]
  (case quantization
    :double  (VecIdx/searchDouble index query top)
    :float   (VecIdx/searchFloat index query top)
    :float16 (VecIdx/searchShort index query top)
    :int8    (VecIdx/searchInt8 index query top)
    :byte    (VecIdx/searchByte index query top)))

(defn- get-vec*
  [index id quantization dimensions]
  (case quantization
    :double  (VecIdx/getDouble index id (int dimensions))
    :float   (VecIdx/getFloat index id (int dimensions))
    :float16 (VecIdx/getShort index id (int dimensions))
    :int8    (VecIdx/getInt8 index id (int dimensions))
    :byte    (VecIdx/getByte index id (int dimensions))))

(defn- open-dbi
  [lmdb vecs-dbi]
  (assert (not (i/closed-kv? lmdb)) "LMDB env is closed.")

  ;; vec-ref -> vec-ids
  (i/open-list-dbi lmdb vecs-dbi {:key-size c/+max-key-size+
                                  :val-size c/+id-bytes+}))

(defn- init-vecs
  [lmdb vecs-dbi]
  (let [vecs   (sp/new-spillable-map)
        max-id (volatile! 0)
        load   (fn [kv]
                 (let [ref (b/read-buffer (l/k kv) :data)
                       id  (b/read-buffer (l/v kv) :id)]
                   (when (< ^long @max-id ^long id) (vreset! max-id id))
                   (.put ^SpillableMap vecs id ref)))]
    (i/visit-list-range lmdb vecs-dbi load [:all] :data [:all] :id)
    [@max-id vecs]))

(defn- open-vec-blob-dbis
  [lmdb]
  (assert (not (i/closed-kv? lmdb)) "LMDB env is closed.")
  (i/open-dbi lmdb c/vec-index-dbi)
  (i/open-dbi lmdb c/vec-meta-dbi))

(defn- vec-temp-dir
  [lmdb]
  (let [dir (File. (str (i/env-dir lmdb) u/+separator+ "tmp"))]
    (.mkdirs dir)
    dir))

(defn- txn-log-enabled?
  [lmdb]
  (true? (:txn-log? (i/env-opts lmdb))))

(defn- vec-checkpoint-max-buffer-bytes
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})
        requested (or (:txn-log-vec-max-buffer-bytes opts)
                      (:vec-max-buffer-bytes opts)
                      c/*txn-log-vec-max-buffer-bytes*
                      c/*vec-max-buffer-bytes*)]
    (long (max 1 requested))))

(defn- vec-checkpoint-chunk-bytes
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})
        requested (or (:txn-log-vec-chunk-bytes opts)
                      (:vec-chunk-bytes opts)
                      c/*txn-log-vec-chunk-bytes*
                      c/*vec-chunk-bytes*)]
    (long (max 1 requested))))

(defn- vec-checkpoint-interval-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})
        requested (or (:txn-log-vec-checkpoint-interval-ms opts)
                      c/*txn-log-vec-checkpoint-interval-ms*)]
    (long (max 1 requested))))

(defn- vec-checkpoint-max-lsn-delta
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})
        requested (or (:txn-log-vec-max-lsn-delta opts)
                      c/*txn-log-vec-max-lsn-delta*)]
    (long (max 1 requested))))

(def ^:dynamic *txn-log-vector-apply-failpoint*
  nil)

(defn- mark-txlog-fatal!
  [lmdb ex]
  (when-let [state (txlog/state lmdb)]
    (when-let [fatal (:fatal-error state)]
      (vreset! fatal ex))
    true))

(defn- vector-commit-unknown!
  [lmdb operation ex-data cause]
  (let [ex (ex-info "Txn-log vector apply failed after LMDB commit"
                    (merge {:type :txn/commit-unknown
                            :operation operation}
                           ex-data)
                    cause)]
    (mark-txlog-fatal! lmdb ex)
    (throw ex)))

(defn- maybe-run-txn-log-vector-apply-failpoint!
  [lmdb operation context]
  (when (and (txn-log-enabled? lmdb)
             (fn? *txn-log-vector-apply-failpoint*))
    (*txn-log-vector-apply-failpoint* operation context)))

(defn- decode-marker-slot-safe
  [slot]
  (when (bytes? slot)
    (try
      (txlog/decode-commit-marker-slot-bytes slot)
      (catch Exception _
        nil))))

(defn- marker-applied-lsn
  [lmdb]
  (let [slot-a (decode-marker-slot-safe
                (i/get-value lmdb c/kv-info c/txn-log-marker-a
                             :keyword :bytes))
        slot-b (decode-marker-slot-safe
                (i/get-value lmdb c/kv-info c/txn-log-marker-b
                             :keyword :bytes))
        curr   (cond
                 (and slot-a slot-b)
                 (if (>= ^long (:revision slot-a)
                         ^long (:revision slot-b))
                   slot-a
                   slot-b)
                 slot-a slot-a
                 slot-b slot-b
                 :else nil)]
    (long (or (:applied-lsn curr) 0))))

(defn- txlog-state-next-lsn
  [lmdb]
  (try
    (when-let [state (txlog/state lmdb)]
      (when-let [next-lsn-v (:next-lsn state)]
        (let [next-lsn @next-lsn-v]
          (when (some? next-lsn)
            (long next-lsn)))))
    (catch Exception _
      nil)))

(defn- txn-log-next-lsn
  [lmdb]
  (when (txn-log-enabled? lmdb)
    (let [state-next  (txlog-state-next-lsn lmdb)
          marker-next (inc (marker-applied-lsn lmdb))]
      (long (or state-next marker-next 1)))))

(defn- txn-log-applied-lsn
  [lmdb]
  (when (txn-log-enabled? lmdb)
    (long (marker-applied-lsn lmdb))))

(defn- meta-map
  [v]
  (if (map? v) v {}))

(defn- maybe-long
  [x]
  (when (some? x) (long x)))

(defn- nonneg-long
  [x]
  (long (max 0 (or x 0))))

(defn- current-snapshot-lsn
  [meta]
  (txlog/parse-optional-floor-lsn
   (or (:current-snapshot-lsn meta)
       (:vec-current-snapshot-lsn meta))
   [:vec-meta :current-snapshot-lsn]))

(defn- previous-snapshot-lsn
  [meta]
  (txlog/parse-optional-floor-lsn
   (or (:previous-snapshot-lsn meta)
       (:vec-previous-snapshot-lsn meta))
   [:vec-meta :previous-snapshot-lsn]))

(defn- replay-floor-lsn
  [meta]
  (txlog/parse-optional-floor-lsn
   (:vec-replay-floor-lsn meta)
   [:vec-meta :vec-replay-floor-lsn]))

(defn- checkpoint-stats-snapshot
  [checkpoint-stats total-bytes]
  (let [m (if (some? checkpoint-stats) @checkpoint-stats {})
        bytes (or (:vec-checkpoint-bytes m) total-bytes)]
    {:vec-checkpoint-count (nonneg-long (:vec-checkpoint-count m))
     :vec-checkpoint-duration-ms
     (nonneg-long (:vec-checkpoint-duration-ms m))
     :vec-checkpoint-bytes (when (some? bytes) (nonneg-long bytes))
     :vec-checkpoint-failure-count
     (nonneg-long (:vec-checkpoint-failure-count m))}))

(defn- checkpoint-replay-lag-lsn
  [lmdb meta]
  (when (txn-log-enabled? lmdb)
    (let [applied-lsn  (txn-log-applied-lsn lmdb)
          replay-floor (replay-floor-lsn meta)]
      (if (and (number? applied-lsn)
               (number? replay-floor))
        ;; Replay lag is defined as retained vector delta count from replay floor.
        (nonneg-long (inc (- ^long applied-lsn ^long replay-floor)))
        0))))

(defn- checkpoint-state
  [lmdb meta checkpoint-stats]
  (let [m (meta-map meta)
        stats (checkpoint-stats-snapshot checkpoint-stats
                                         (maybe-long (:total-bytes m)))]
    {:current-snapshot-lsn  (current-snapshot-lsn m)
     :previous-snapshot-lsn (previous-snapshot-lsn m)
     :vec-replay-floor-lsn  (replay-floor-lsn m)
     :chunk-count           (maybe-long (:chunk-count m))
     :total-bytes           (maybe-long (:total-bytes m))
     :last-checkpoint-ms    (maybe-long (:last-checkpoint-ms m))
     :checksum              (:checksum m)
     :version               (:version m)
     :vec-checkpoint-count  (:vec-checkpoint-count stats)
     :vec-checkpoint-duration-ms (:vec-checkpoint-duration-ms stats)
     :vec-checkpoint-bytes  (:vec-checkpoint-bytes stats)
     :vec-checkpoint-failure-count (:vec-checkpoint-failure-count stats)
     :vec-replay-lag-lsn    (checkpoint-replay-lag-lsn lmdb m)}))

(defn- snapshot-covers-lsn?
  [meta lsn]
  (let [current  (current-snapshot-lsn meta)
        previous (previous-snapshot-lsn meta)]
    (or (and (some? current) (>= ^long current ^long lsn))
        (and (some? previous) (>= ^long previous ^long lsn)))))

(defn- replay-floor-meta
  [meta commit-lsn]
  (let [meta0      (meta-map meta)
        old-replay (replay-floor-lsn meta0)]
    (if (snapshot-covers-lsn? meta0 commit-lsn)
      meta0
      (assoc meta0 :vec-replay-floor-lsn
             (long (if (some? old-replay)
                     (min ^long old-replay ^long commit-lsn)
                     commit-lsn))))))

(defn- replay-floor-put-tx
  [lmdb domain commit-lsn]
  (when (and (some? commit-lsn) (>= ^long commit-lsn 0))
    (let [old-meta (meta-map (i/get-value lmdb c/vec-meta-dbi domain
                                          :string :data))
          new-meta (replay-floor-meta old-meta commit-lsn)]
      (when (not= old-meta new-meta)
        (l/kv-tx :put c/vec-meta-dbi domain new-meta :string :data)))))

(defn- checkpoint-meta
  [old-meta chunk-count total-bytes target-lsn]
  (let [now-ms     (System/currentTimeMillis)
        meta0      (-> (meta-map old-meta)
                       (dissoc :vec-current-snapshot-lsn
                               :vec-previous-snapshot-lsn))
        old-current (current-snapshot-lsn meta0)
        old-replay  (replay-floor-lsn meta0)
        meta1       (assoc meta0
                           :chunk-count chunk-count
                           :total-bytes total-bytes
                           :last-checkpoint-ms now-ms)
        meta2       (if (some? target-lsn)
                      (cond-> (assoc meta1 :current-snapshot-lsn target-lsn)
                        (some? old-current)
                        (assoc :previous-snapshot-lsn old-current))
                      meta1)]
    (if (and (some? old-replay)
             (some? target-lsn)
             (>= ^long target-lsn ^long old-replay))
      (dissoc meta2 :vec-replay-floor-lsn)
      meta2)))

(defn- checkpoint-to-lmdb
  [lmdb ^DTLV$usearch_index_t index ^String domain]
  (let [target-lsn   (txn-log-applied-lsn lmdb)
        total-bytes  (VecIdx/serializedLength index)
        chunk-size   (vec-checkpoint-chunk-bytes lmdb)
        safe-buffer  (long (min (vec-checkpoint-max-buffer-bytes lmdb)
                                (long Integer/MAX_VALUE)))
        old-meta     (i/get-value lmdb c/vec-meta-dbi domain :string :data)
        old-chunks   (long (or (:chunk-count old-meta) 0))
        chunk-count  (long (Math/ceil (/ (double total-bytes)
                                         (double chunk-size))))
        txs          (java.util.ArrayList.)]
    ;; Remove prior chunks in the same LMDB write txn so stale chunks are not kept.
    (dotimes [i old-chunks]
      (.add txs (l/kv-tx :del c/vec-index-dbi [domain i] :data)))
    (if (<= total-bytes safe-buffer)
      ;; Buffer mode: serialize to native memory, copy to JVM byte[] and chunk.
      (let [buf (BytePointer. (long total-bytes))]
        (try
          (VecIdx/saveBuffer index buf total-bytes)
          (let [all-bytes (byte-array (int total-bytes))]
            (.get buf all-bytes)
            (dotimes [i chunk-count]
              (let [offset (int (* i chunk-size))
                    end    (int (min (+ offset chunk-size) total-bytes))
                    chunk  (Arrays/copyOfRange all-bytes offset end)]
                (.add txs (l/kv-tx :put c/vec-index-dbi
                                   [domain i] chunk :data :bytes)))))
          (finally
            (.close buf))))
      ;; File-spool mode: avoid very large contiguous buffers.
      (let [tmp-file (File/createTempFile "vec-checkpoint-" ".tmp"
                                          (vec-temp-dir lmdb))]
        (try
          (VecIdx/save index (.getAbsolutePath tmp-file))
          (with-open [fis (FileInputStream. tmp-file)]
            (let [read-buf (byte-array (int (min chunk-size
                                             (long Integer/MAX_VALUE))))]
              (loop [chunk-id 0]
                (let [n (.read fis read-buf)]
                  (when (pos? n)
                    (let [chunk (Arrays/copyOf read-buf n)]
                      (.add txs (l/kv-tx :put c/vec-index-dbi
                                         [domain chunk-id] chunk :data :bytes))
                      (recur (inc chunk-id))))))))
          (finally
            (.delete tmp-file)))))
    (.add txs (l/kv-tx :put c/vec-meta-dbi domain
                       (checkpoint-meta old-meta chunk-count total-bytes
                                        target-lsn)
                       :string :data))
    (i/transact-kv lmdb txs)
    {:chunk-count chunk-count
     :total-bytes (long total-bytes)
     :target-lsn target-lsn}))

(defn- load-from-lmdb
  [lmdb ^DTLV$usearch_index_t index ^String domain]
  (when-let [meta-val (i/get-value lmdb c/vec-meta-dbi domain :string :data)]
    (let [total-bytes (long (:total-bytes meta-val))
          chunk-count (long (:chunk-count meta-val))
          safe-buffer (long (min (vec-checkpoint-max-buffer-bytes lmdb)
                                 (long Integer/MAX_VALUE)))]
      (if (<= total-bytes safe-buffer)
        ;; Buffer mode: load chunks into one byte[] then call native load.
        (let [all-bytes (byte-array (int total-bytes))]
          (loop [i 0, offset 0]
            (when (< i chunk-count)
              (let [chunk0 (or (i/get-value lmdb c/vec-index-dbi
                                            [domain i] :data :bytes)
                               (raise "Missing vector blob chunk in LMDB"
                                      {:domain domain :chunk-id i}))
                    ^bytes chunk chunk0
                    len          (alength chunk)]
                (System/arraycopy ^bytes chunk 0 ^bytes all-bytes
                                  (int offset) len)
                (recur (inc i) (+ offset (long len))))))
          (let [buf (BytePointer. all-bytes)]
            (try
              (VecIdx/loadBuffer index buf total-bytes)
              (finally
                (.close buf)))))
        ;; File-spool mode: materialize chunks to a temp file then load.
        (let [tmp-file (File/createTempFile "vec-load-" ".tmp" (vec-temp-dir lmdb))]
          (try
            (with-open [fos (FileOutputStream. tmp-file)]
              (dotimes [i chunk-count]
                (let [chunk0 (or (i/get-value lmdb c/vec-index-dbi
                                              [domain i] :data :bytes)
                                 (raise "Missing vector blob chunk in LMDB"
                                        {:domain domain :chunk-id i}))
                      ^bytes chunk chunk0]
                  (.write ^FileOutputStream fos chunk))))
            (VecIdx/load index (.getAbsolutePath tmp-file))
            (finally
              (.delete tmp-file)))))
      true)))

(defn- clear-vec-blobs
  [lmdb ^String domain]
  (when-let [meta-val (i/get-value lmdb c/vec-meta-dbi domain :string :data)]
    (let [chunk-count (long (:chunk-count meta-val))
          txs         (java.util.ArrayList.)]
      (dotimes [i chunk-count]
        (.add txs (l/kv-tx :del c/vec-index-dbi [domain i] :data)))
      (.add txs (l/kv-tx :del c/vec-meta-dbi domain :string))
      (i/transact-kv lmdb txs))))

(defn- checkpoint-stats-success!
  [checkpoint-stats total-bytes duration-ms completed-ms]
  (when (some? checkpoint-stats)
    (swap! checkpoint-stats
           (fn [m]
             (let [m (or m {})]
               (-> m
                   (update :vec-checkpoint-count (fnil inc 0))
                   (update :vec-checkpoint-duration-ms
                           (fnil + 0)
                           (nonneg-long duration-ms))
                   (assoc :vec-checkpoint-bytes (nonneg-long total-bytes)
                          :last-checkpoint-ms (nonneg-long completed-ms))))))))

(defn- checkpoint-stats-failure!
  [checkpoint-stats]
  (when (some? checkpoint-stats)
    (swap! checkpoint-stats update :vec-checkpoint-failure-count (fnil inc 0))))

(defn- checkpoint-last-checkpoint-ms
  [meta checkpoint-stats]
  (or (maybe-long (:last-checkpoint-ms meta))
      (some-> checkpoint-stats deref :last-checkpoint-ms maybe-long)))

(defn- auto-checkpoint?
  [lmdb domain checkpoint-stats]
  (if-not (txn-log-enabled? lmdb)
    true
    (let [meta               (meta-map (i/get-value lmdb c/vec-meta-dbi domain
                                                    :string :data))
          now-ms             (System/currentTimeMillis)
          snapshot-lsn       (current-snapshot-lsn meta)
          applied-lsn        (txn-log-applied-lsn lmdb)
          max-lsn-delta      (vec-checkpoint-max-lsn-delta lmdb)
          interval-ms        (vec-checkpoint-interval-ms lmdb)
          lag-lsn            (when (some? applied-lsn)
                               (if (some? snapshot-lsn)
                                 (nonneg-long (- ^long applied-lsn
                                                 ^long snapshot-lsn))
                                 (nonneg-long (inc ^long applied-lsn))))
          lag-trigger?       (and (some? lag-lsn)
                                  (>= ^long lag-lsn ^long max-lsn-delta))
          last-checkpoint-ms (checkpoint-last-checkpoint-ms meta checkpoint-stats)
          interval-trigger?  (or (nil? last-checkpoint-ms)
                                 (>= (- now-ms ^long last-checkpoint-ms)
                                     ^long interval-ms))]
      (or (nil? snapshot-lsn)
          lag-trigger?
          interval-trigger?))))

(def default-search-opts {:display    :refs
                          :top        10
                          :vec-filter (constantly true)})

(defn- vec-save-key* [fname] (->> fname hash (str "vec-save-") keyword))

(def vec-save-key (memoize vec-save-key*))

(def ^:dynamic *submit-async-vec-save*
  (fn [work]
    (a/exec (a/get-executor) work)))

(defn submit-async-vec-save!
  [work]
  (*submit-async-vec-save* work))

(deftype AsyncVecSave [lmdb-lock vec-index lmdb ^String domain checkpoint-stats
                       fname ^ReentrantReadWriteLock vec-lock]
  IAsyncWork
  (work-key [_] (vec-save-key fname))
  (do-work [_]
    ;; Keep lock order as LMDB -> vector-lock to avoid deadlocks with
    ;; datalog tx path that already holds LMDB write lock while adding vectors.
    (locking lmdb-lock
      (let [rlock (.readLock vec-lock)]
        (when (.tryLock rlock)
          (try
            (when (and (not (i/vec-closed? vec-index))
                       (auto-checkpoint? lmdb domain checkpoint-stats))
              (i/persist-vecs vec-index))
            (catch Throwable _)
            (finally
              (.unlock rlock)))))))
  (combine [_] first)
  (callback [_] nil))

(declare display-xf new-vector-index)

(deftype VectorIndex [lmdb
                      closed?
                      ^DTLV$usearch_index_t index
                      ^String fname
                      ^String domain
                      ^long dimensions
                      ^clojure.lang.Keyword metric-type
                      ^clojure.lang.Keyword quantization
                      ^long connectivity
                      ^long expansion-add
                      ^long expansion-search
                      ^String vecs-dbi
                      ^SpillableMap vecs     ; vec-id -> vec-ref
                      ^AtomicLong max-vec
                      ^Map search-opts
                      checkpoint-stats
                      ^ReentrantReadWriteLock vec-lock]
  IVectorIndex
  (add-vec [this vec-ref vec-data]
    (let [vec-arr (vec->arr dimensions quantization vec-data)
          wlock   (.writeLock vec-lock)
          tx-lock (l/write-txn lmdb)
          vec-id  (volatile! nil)]
      (.lock wlock)
      (try
        (when @closed?
          (raise "Vector index is closed." {:domain domain}))
        (let [id (.incrementAndGet max-vec)]
          (vreset! vec-id id)
          ;; Apply LMDB mapping first; mutate in-memory index only after success.
          (let [commit-lsn (txn-log-next-lsn lmdb)
                meta-op    (replay-floor-put-tx lmdb domain commit-lsn)
                txs        (cond-> [(l/kv-tx :put vecs-dbi vec-ref id
                                              :data :id)]
                             (some? meta-op) (conj meta-op))]
            (i/transact-kv lmdb txs))
          (try
            (maybe-run-txn-log-vector-apply-failpoint!
             lmdb :add-vec
             {:domain domain :vec-ref vec-ref :vec-id id})
            (add index quantization id vec-arr)
            (.put vecs id vec-ref)
            (catch Throwable e
              (if (txn-log-enabled? lmdb)
                (vector-commit-unknown!
                 lmdb :add-vec
                 {:domain domain
                  :vec-ref vec-ref
                  :vec-id id}
                 e)
                (do
                  (try
                    (i/transact-kv
                     lmdb [(l/kv-tx :del-list vecs-dbi vec-ref [id] :data :id)])
                    (catch Throwable rollback-e
                      (throw (ex-info "Fail to rollback vector ref mapping"
                                      {:vec-ref vec-ref :vec-id id}
                                      rollback-e))))
                  (throw (ex-info "Fail to add vector after LMDB apply"
                                  {:vec-ref vec-ref :vec-id id}
                                  e)))))))
        (finally
          (.unlock wlock)))
      (submit-async-vec-save!
       (AsyncVecSave. tx-lock this lmdb domain checkpoint-stats
                      fname vec-lock))
      @vec-id))

  (get-vec [_ vec-ref]
    (let [rlock (.readLock vec-lock)]
      (.lock rlock)
      (try
        (let [ids (i/get-list lmdb vecs-dbi vec-ref :data :id)]
          (for [^long id ids]
            (get-vec* index id quantization dimensions)))
        (finally
          (.unlock rlock)))))

  (remove-vec [this vec-ref]
    (let [wlock    (.writeLock vec-lock)
          tx-lock  (l/write-txn lmdb)
          removed? (volatile! false)]
      (.lock wlock)
      (try
        (when @closed?
          (raise "Vector index is closed." {:domain domain}))
        (let [ids (vec (i/get-list lmdb vecs-dbi vec-ref :data :id))]
          ;; Apply LMDB deletion first; mutate in-memory index only after success.
          (let [commit-lsn (txn-log-next-lsn lmdb)
                meta-op    (when (seq ids)
                             (replay-floor-put-tx lmdb domain commit-lsn))
                txs        (cond-> [(l/kv-tx :del vecs-dbi vec-ref)]
                             (some? meta-op) (conj meta-op))]
            (i/transact-kv lmdb txs))
          (try
            (maybe-run-txn-log-vector-apply-failpoint!
             lmdb :remove-vec
             {:domain domain :vec-ref vec-ref :ids ids})
            (doseq [^long id ids]
              (remove-id index id))
            (doseq [^long id ids]
              (.remove vecs id))
            (vreset! removed? (boolean (seq ids)))
            (catch Throwable e
              (if (txn-log-enabled? lmdb)
                (vector-commit-unknown!
                 lmdb :remove-vec
                 {:domain domain
                  :vec-ref vec-ref
                  :ids ids}
                 e)
                (do
                  (when (seq ids)
                    (try
                      (i/transact-kv
                       lmdb [(l/kv-tx :put-list vecs-dbi vec-ref ids :data :id)])
                      (catch Throwable rollback-e
                        (throw (ex-info "Fail to rollback vector ref mapping"
                                        {:vec-ref vec-ref :ids ids}
                                        rollback-e)))))
                  (throw (ex-info "Fail to remove vector after LMDB apply"
                                  {:vec-ref vec-ref :ids ids}
                                  e)))))))
        (finally
          (.unlock wlock)))
      (when @removed?
        (submit-async-vec-save!
         (AsyncVecSave. tx-lock this lmdb domain checkpoint-stats
                        fname vec-lock)))
      nil))

  (persist-vecs [_]
    (when-not @closed?
      (let [started-ms (System/currentTimeMillis)]
        (try
          (let [{:keys [total-bytes]} (checkpoint-to-lmdb lmdb index domain)
                completed-ms (System/currentTimeMillis)
                duration-ms (nonneg-long
                             (- completed-ms started-ms))]
            (checkpoint-stats-success! checkpoint-stats total-bytes duration-ms
                                       completed-ms))
          (catch Throwable e
            (checkpoint-stats-failure! checkpoint-stats)
            (throw e))))))

  (close-vecs [this]
    (let [wlock (.writeLock vec-lock)]
      (.lock wlock)
      (try
        (when-not (.vec-closed? this)
          (.persist_vecs this)
          (vreset! closed? true)
          (swap! l/vector-indices dissoc fname)
          (VecIdx/free index))
        (finally
          (.unlock wlock)))))

  (vec-closed? [_] @closed?)

  (clear-vecs [this]
    (.close-vecs this)
    (.empty vecs)
    (i/clear-dbi lmdb vecs-dbi)
    (clear-vec-blobs lmdb domain)
    (when (u/file-exists fname)
      (u/delete-files fname)))

  (vecs-info [_]
    (let [rlock (.readLock vec-lock)]
      (.lock rlock)
      (try
        (let [^VecIdx$IndexInfo info (VecIdx/info index)
              meta-val               (i/get-value lmdb c/vec-meta-dbi domain
                                                  :string :data)]
          {:size             (.getSize info)
           :memory           (.getMemory info)
           :capacity         (.getCapacity info)
           :hardware         (.getHardware info)
           :filename         fname
           :domain           domain
           :dimensions       dimensions
           :metric-type      metric-type
           :quantization     quantization
           :connectivity     connectivity
           :expansion-add    expansion-add
           :expansion-search expansion-search
           :checkpoint       (checkpoint-state lmdb meta-val checkpoint-stats)})
        (finally
          (.unlock rlock)))))

  (vec-indexed? [_ vec-ref] (i/get-value lmdb vecs-dbi vec-ref))

  (search-vec [this query-vec]
    (.search-vec this query-vec {}))
  (search-vec [this query-vec {:keys [display top vec-filter]
                               :or   {display    (:display search-opts)
                                      top        (:top search-opts)
                                      vec-filter (:vec-filter search-opts)}}]
    (let [rlock (.readLock vec-lock)]
      (.lock rlock)
      (try
        (let [query                    (vec->arr dimensions quantization query-vec)
              ^VecIdx$SearchResult res (search index query quantization (int top))]
          (doall (sequence
                   (display-xf this vec-filter display)
                   (.getKeys res) (.getDists res))))
        (finally
          (.unlock rlock)))))

  IAdmin
  (re-index [this opts]
    (try
      (let [dfname (str fname ".dump")
            dos    (DataOutputStream. (FileOutputStream. ^String dfname))]
        (nippy/freeze-to-out!
          dos (for [[vec-id vec-ref] vecs]
                [vec-ref (get-vec* index vec-id quantization dimensions)]))
        (.flush dos)
        (.close dos)
        (.clear-vecs this)
        (let [new (new-vector-index lmdb opts)
              dis (DataInputStream. (FileInputStream. ^String dfname))]
          (doseq [[vec-ref vec-data] (nippy/thaw-from-in! dis)]
            (i/add-vec new vec-ref vec-data))
          (.close dis)
          (u/delete-files dfname)
          new))
      (catch Exception e
        (u/raise "Unable to re-index vectors. " e {:dir (i/env-dir lmdb)})))))

(defn- get-ref
  [^VectorIndex index vec-filter vec-id _]
  (when-let [vec-ref ((.-vecs index) vec-id)]
    (when (vec-filter vec-ref) vec-ref)))

(defn- get-ref-dist
  [^VectorIndex index vec-filter vec-id dist]
  (when-let [vec-ref ((.-vecs index) vec-id)]
    (when (vec-filter vec-ref) [vec-ref dist])))

(defn- display-xf
  [index vec-filter display]
  (case display
    :refs       (comp (map #(get-ref index vec-filter %1 %2))
                   (remove nil?))
    :refs+dists (comp (map #(get-ref-dist index vec-filter %1 %2))
                   (remove nil?))))

(def default-opts {:metric-type      c/default-metric-type
                   :quantization     c/default-quantization
                   :connectivity     c/default-connectivity
                   :expansion-add    c/default-expansion-add
                   :expansion-search c/default-expansion-search
                   :search-opts      default-search-opts})

(defn new-vector-index*
  [lmdb {:keys [domain metric-type quantization dimensions connectivity
                expansion-add expansion-search search-opts]
         :or   {metric-type      (default-opts :metric-type)
                quantization     (default-opts :quantization)
                connectivity     (default-opts :connectivity)
                expansion-add    (default-opts :expansion-add)
                expansion-search (default-opts :expansion-search)
                search-opts      (default-opts :search-opts)
                domain           c/default-domain}}]
  (assert dimensions ":dimensions is required")
  (let [vecs-dbi (str domain "/" c/vec-refs)
        domain   (str domain)]
    (open-dbi lmdb vecs-dbi)
    (open-vec-blob-dbis lmdb)
    (let [[max-vec-id vecs] (init-vecs lmdb vecs-dbi)
          fname             (index-fname lmdb domain)
          index             (create-index dimensions metric-type
                                          quantization connectivity
                                          expansion-add expansion-search)]
      ;; Prefer LMDB blob. If absent, migrate from legacy .vid file.
      (when-not (load-from-lmdb lmdb index domain)
        (when (u/file-exists fname)
          (VecIdx/load index fname)
          (checkpoint-to-lmdb lmdb index domain)
          (u/delete-files fname)))
      (let [meta-val (i/get-value lmdb c/vec-meta-dbi domain :string :data)
            checkpoint-stats
            (atom {:vec-checkpoint-count 0
                   :vec-checkpoint-duration-ms 0
                   :vec-checkpoint-bytes
                   (nonneg-long (or (:total-bytes meta-val) 0))
                   :vec-checkpoint-failure-count 0})]
      (let [vi (->VectorIndex lmdb
                              (volatile! false)
                              index
                              fname
                              domain
                              dimensions
                              metric-type
                              quantization
                              connectivity
                              expansion-add
                              expansion-search
                              vecs-dbi
                              vecs
                              (AtomicLong. max-vec-id)
                              search-opts
                              checkpoint-stats
                              (ReentrantReadWriteLock.))]
        (swap! l/vector-indices assoc fname vi)
        vi)))))

(defn new-vector-index
  [lmdb opts]
  (if (instance? KVStore lmdb)
    (r/new-vector-index lmdb opts)
    (new-vector-index* lmdb opts)))

(defn transfer
  [^VectorIndex old lmdb]
  (->VectorIndex lmdb
                 (.-closed? old)
                 (.-index old)
                 (.-fname old)
                 (.-domain old)
                 (.-dimensions old)
                 (.-metric-type old)
                 (.-quantization old)
                 (.-connectivity old)
                 (.-expansion-add old)
                 (.-expansion-search old)
                 (.-vecs-dbi old)
                 (.-vecs old)
                 (.-max-vec old)
                 (.-search-opts old)
                 (.-checkpoint-stats old)
                 (ReentrantReadWriteLock.)))

(defn attr-domain [attr] (s/replace (u/keyword->string attr) "/" "_"))
