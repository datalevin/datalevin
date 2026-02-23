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
   [datalevin.remote :as r]
   [datalevin.bits :as b]
   [datalevin.interface :as i]
   [clojure.string :as s]
   [taoensso.nippy :as nippy])
  (:import
   [datalevin.dtlvnative DTLV DTLV$usearch_index_t]
   [datalevin.cpp VecIdx VecIdx$SearchResult VecIdx$IndexInfo]
   [datalevin.spill SpillableMap]
   [datalevin.remote KVStore]
   [datalevin.interface IAdmin IVectorIndex]
   [java.io File FileOutputStream FileInputStream DataOutputStream
    DataInputStream]
   [java.util Arrays ArrayList HashMap HashSet Map]
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
  "Create a fresh native HNSW index without loading any data."
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
  (let [opts {:key-size c/+max-key-size+
              :val-size c/+id-bytes+}]
    ;; Open in WAL overlays.
    (i/open-list-dbi lmdb vecs-dbi opts)
    ;; Also ensure base DBI is opened for internal bypass-wal writes used by
    ;; vector direct/shadow commit paths.
    (binding [c/*bypass-wal* true]
      (i/open-list-dbi lmdb vecs-dbi opts))))

(defn- open-vec-blob-dbis
  "Open the vec-index-dbi and vec-meta-dbi for LMDB blob storage."
  [lmdb]
  ;; Open in WAL overlays.
  (i/open-dbi lmdb c/vec-index-dbi)
  (i/open-dbi lmdb c/vec-meta-dbi)
  ;; Also ensure base DBIs are opened for checkpoint/clear paths that bypass
  ;; WAL and write blob chunks directly to base LMDB.
  (binding [c/*bypass-wal* true]
    (i/open-dbi lmdb c/vec-index-dbi)
    (i/open-dbi lmdb c/vec-meta-dbi)))

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

;;; ---------------------------------------------------------------
;;; LMDB Blob Checkpoint / Load
;;; ---------------------------------------------------------------

(defn- checkpoint-to-lmdb
  "Serialize the native HNSW index and store as chunked blobs in LMDB.
   Bypasses WAL since this is a bulk snapshot write to LMDB base."
  [lmdb ^DTLV$usearch_index_t index ^String domain]
  (let [total-bytes (VecIdx/serializedLength index)]
    (when (pos? total-bytes)
      (let [chunk-size  (long c/*wal-vec-chunk-bytes*)
            chunk-count (long (Math/ceil (/ (double total-bytes)
                                           (double chunk-size))))
            txs         (java.util.ArrayList.)]
        (if (<= total-bytes (long c/*wal-vec-max-buffer-bytes*))
          ;; Buffer mode: serialize to native buffer, copy to JVM, write chunks
          (let [buf (BytePointer. (long total-bytes))]
            (try
              (VecIdx/saveBuffer index buf total-bytes)
              (let [ba (byte-array total-bytes)]
                (.get buf ba)
                (dotimes [i chunk-count]
                  (let [offset (int (* i chunk-size))
                        end    (int (min (+ offset chunk-size) total-bytes))
                        chunk  (Arrays/copyOfRange ba offset end)]
                    (.add txs (l/kv-tx :put c/vec-index-dbi
                                       [domain i] chunk :data :bytes)))))
              (finally (.close buf))))
          ;; File-spool mode: save to temp file, stream into chunks
          (let [tmp-dir  (File. (str (i/env-dir lmdb) u/+separator+ "tmp"))
                _        (.mkdirs tmp-dir)
                tmp-file (File/createTempFile "vec-checkpoint-" ".tmp" tmp-dir)]
            (try
              (VecIdx/save index (.getAbsolutePath tmp-file))
              (with-open [fis (FileInputStream. tmp-file)]
                (let [read-buf (byte-array (int (min chunk-size
                                                     Integer/MAX_VALUE)))]
                  (loop [chunk-id 0]
                    (let [n (.read fis read-buf)]
                      (when (pos? n)
                        (let [chunk (Arrays/copyOf read-buf n)]
                          (.add txs (l/kv-tx :put c/vec-index-dbi
                                             [domain chunk-id] chunk
                                             :data :bytes))
                          (recur (inc chunk-id))))))))
              (finally (.delete tmp-file)))))
        ;; Write metadata
        (.add txs (l/kv-tx :put c/vec-meta-dbi domain
                           {:chunk-count chunk-count :total-bytes total-bytes}
                           :string :data))
        ;; Bypass WAL for blob checkpoint (bulk snapshot write)
        (binding [c/*bypass-wal*    true
                  c/*trusted-apply* true]
          (i/transact-kv lmdb txs))))))

(defn- load-from-lmdb
  "Read chunked blobs from LMDB and deserialize into the native index.
   Returns true if data was loaded, false if no blob found."
  [lmdb ^DTLV$usearch_index_t index ^String domain]
  (let [meta-val (i/get-value lmdb c/vec-meta-dbi domain :string :data)]
    (if (nil? meta-val)
      false
      (let [total-bytes (long (:total-bytes meta-val))
            chunk-count (long (:chunk-count meta-val))]
        (if (<= total-bytes (long c/*wal-vec-max-buffer-bytes*))
          ;; Buffer mode: read chunks into byte array, copy to native, load
          (let [ba (byte-array total-bytes)]
            (loop [i 0, offset 0]
              (when (< i chunk-count)
                (let [chunk ^bytes (i/get-value lmdb c/vec-index-dbi
                                               [domain i] :data :bytes)
                      len   (alength chunk)]
                  (System/arraycopy chunk 0 ba (int offset) len)
                  (recur (inc i) (+ offset (long len))))))
            (let [buf (BytePointer. ba)]
              (try
                (VecIdx/loadBuffer index buf total-bytes)
                (finally (.close buf)))))
          ;; File-spool mode: write chunks to temp file, load from file
          (let [tmp-dir  (File. (str (i/env-dir lmdb) u/+separator+ "tmp"))
                _        (.mkdirs tmp-dir)
                tmp-file (File/createTempFile "vec-load-" ".tmp" tmp-dir)]
            (try
              (with-open [fos (FileOutputStream. tmp-file)]
                (dotimes [i chunk-count]
                  (let [chunk ^bytes (i/get-value lmdb c/vec-index-dbi
                                                 [domain i] :data :bytes)]
                    (.write fos chunk))))
              (VecIdx/load index (.getAbsolutePath tmp-file))
              (finally (.delete tmp-file)))))
        true))))

(defn- clear-vec-blobs
  "Delete all chunks for domain from vec-index-dbi and metadata from
   vec-meta-dbi."
  [lmdb ^String domain]
  (let [meta-val (i/get-value lmdb c/vec-meta-dbi domain :string :data)]
    (when meta-val
      (let [chunk-count (long (:chunk-count meta-val))
            txs         (java.util.ArrayList.)]
        (dotimes [i chunk-count]
          (.add txs (l/kv-tx :del c/vec-index-dbi [domain i] nil :data)))
        (.add txs (l/kv-tx :del c/vec-meta-dbi domain nil :string))
        (binding [c/*bypass-wal*    true
                  c/*trusted-apply* true]
          (i/transact-kv lmdb txs))))))

;;; ---------------------------------------------------------------

(def default-search-opts {:display    :refs
                          :top        10
                          :vec-filter (constantly true)})

(declare display-xf new-vector-index)

(defn- merge-search-results
  "Merge base and shadow search results, filtering tombstones.
   Returns [long-array float-array] of at most `top` results sorted by distance."
  [^VecIdx$SearchResult base-res shadow-res ^HashSet tombstones ^long top]
  (let [bk (.getKeys base-res)
        bd (.getDists base-res)]
    (if (and (nil? shadow-res) (nil? tombstones))
      [bk bd]
      (let [pairs (ArrayList.)]
        ;; Add base results, filtering tombstones
        (dotimes [i (alength bk)]
          (let [k (aget bk i)]
            (when-not (and tombstones (.contains tombstones k))
              (.add pairs (long-array [k (Float/floatToRawIntBits (aget bd i))])))))
        ;; Add shadow results
        (when shadow-res
          (let [sk (.getKeys ^VecIdx$SearchResult shadow-res)
                sd (.getDists ^VecIdx$SearchResult shadow-res)]
            (dotimes [i (alength sk)]
              (.add pairs (long-array [(aget sk i)
                                       (Float/floatToRawIntBits (aget sd i))])))))
        ;; Sort by distance (stored as raw int bits)
        (.sort pairs (reify java.util.Comparator
                       (compare [_ a b]
                         (Float/compare
                           (Float/intBitsToFloat (int (aget ^longs a 1)))
                           (Float/intBitsToFloat (int (aget ^longs b 1)))))))
        ;; Take top results
        (let [n     (min top (.size pairs))
              rkeys (long-array n)
              rdist (float-array n)]
          (dotimes [i n]
            (let [^longs p (.get pairs i)]
              (aset rkeys i (aget p 0))
              (aset rdist i (Float/intBitsToFloat (int (aget p 1))))))
          [rkeys rdist])))))

(deftype VectorIndex [lmdb
                      closed?
                      ^DTLV$usearch_index_t index
                      ^String fname
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
                      ^ReentrantReadWriteLock vec-lock
                      ^String domain
                      shadow               ; volatile! nil or shadow map
                      dirty?]              ; volatile! boolean — true when index mutated since last checkpoint
  IVectorIndex
  (add-vec [_ vec-ref vec-data]
    (let [vec-id  (.incrementAndGet max-vec)
          vec-arr (vec->arr dimensions quantization vec-data)]
      (if-let [s @shadow]
        ;; Shadow mode: add to shadow index only
        (let [^HashMap sv   (:vecs s)
              ^HashMap ri   (:ref->ids s)]
          (add (:index s) quantization vec-id vec-arr)
          (.put sv vec-id vec-ref)
          (.computeIfAbsent ri vec-ref
            (reify java.util.function.Function
              (apply [_ _] (ArrayList.))))
          (.add ^ArrayList (.get ri vec-ref) (Long/valueOf vec-id)))
        ;; Direct mode: mutate base + write vecs-dbi (current behavior)
        (do (add index quantization vec-id vec-arr)
            (.put vecs vec-id vec-ref)
            (binding [c/*bypass-wal*    true
                      c/*trusted-apply* true]
              (i/transact-kv
                lmdb [(l/kv-tx :put vecs-dbi vec-ref vec-id :data :id)]))))
      (vreset! dirty? true)
      vec-id))

  (get-vec [_ vec-ref]
    (let [s           @shadow
          tombstones  (when s (:tombstones s))
          base-ids    (i/get-list lmdb vecs-dbi vec-ref :data :id)
          base-vecs   (for [^long id base-ids
                            :when (not (and tombstones
                                            (.contains ^HashSet tombstones id)))]
                        (get-vec* index id quantization dimensions))
          shadow-vecs (when s
                        (when-let [ids (.get ^HashMap (:ref->ids s) vec-ref)]
                          (for [^long id ids]
                            (get-vec* (:index s) id quantization dimensions))))]
      (concat base-vecs shadow-vecs)))

  (remove-vec [_ vec-ref]
    (if-let [s @shadow]
      ;; Shadow mode
      (let [base-ids (i/get-list lmdb vecs-dbi vec-ref :data :id)]
        (doseq [^long id base-ids]
          (.add ^HashSet (:tombstones s) id))
        (when (seq base-ids)
          (.add ^HashSet (:del-refs s) vec-ref))
        ;; Remove from shadow if present
        (when-let [shadow-ids (.get ^HashMap (:ref->ids s) vec-ref)]
          (doseq [^long id shadow-ids]
            (VecIdx/remove (:index s) id)
            (.remove ^HashMap (:vecs s) id))
          (.remove ^HashMap (:ref->ids s) vec-ref)))
      ;; Direct mode (current behavior)
      (let [ids (i/get-list lmdb vecs-dbi vec-ref :data :id)]
        (doseq [^long id ids]
          (VecIdx/remove index id)
          (.remove vecs id))
        (binding [c/*bypass-wal*    true
                  c/*trusted-apply* true]
          (i/transact-kv lmdb [(l/kv-tx :del vecs-dbi vec-ref)]))))
    (vreset! dirty? true))

  (persist-vecs [this]
    (when (and (not @closed?) @dirty?)
      (.commit-vec-tx this)
      (checkpoint-to-lmdb lmdb index domain)
      (vreset! dirty? false)))

  (close-vecs [this]
    (let [wlock (.writeLock vec-lock)]
      (.lock wlock)
      (try
        (when-not (.vec-closed? this)
          (.persist_vecs this)
          (when-let [s @shadow]
            (VecIdx/free ^DTLV$usearch_index_t (:index s))
            (vreset! shadow nil))
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
    (clear-vec-blobs lmdb domain))

  (vecs-info [_]
    (let [^VecIdx$IndexInfo info (VecIdx/info index)]
      {:size             (.getSize info)
       :memory           (.getMemory info)
       :capacity         (.getCapacity info)
       :hardware         (.getHardware info)
       :filename         fname
       :dimensions       dimensions
       :metric-type      metric-type
       :quantization     quantization
       :connectivity     connectivity
       :expansion-add    expansion-add
       :expansion-search expansion-search}))

  (vec-indexed? [_ vec-ref]
    (or (i/get-value lmdb vecs-dbi vec-ref)
        (when-let [s @shadow]
          (.containsKey ^HashMap (:ref->ids s) vec-ref))))

  (search-vec [this query-vec]
    (.search-vec this query-vec {}))
  (search-vec [this query-vec {:keys [display top vec-filter]
                               :or   {display    (:display search-opts)
                                      top        (:top search-opts)
                                      vec-filter (:vec-filter search-opts)}}]
    (let [query                    (vec->arr dimensions quantization query-vec)
          s                        @shadow
          ^VecIdx$SearchResult res (search index query quantization (int top))
          shadow-res               (when (and s
                                              (pos? (.getSize
                                                      ^VecIdx$IndexInfo
                                                      (VecIdx/info
                                                        ^DTLV$usearch_index_t
                                                        (:index s)))))
                                    (search (:index s) query
                                            quantization (int top)))
          [merged-keys merged-dists]
          (merge-search-results res shadow-res
                                (when s (:tombstones s)) top)]
      (doall (sequence
               (display-xf this vec-filter display)
               merged-keys merged-dists))))

  (begin-vec-tx [_]
    (vreset! shadow
      {:index      (create-index dimensions metric-type quantization
                                 connectivity expansion-add expansion-search)
       :vecs       (HashMap.)
       :ref->ids   (HashMap.)
       :tombstones (HashSet.)
       :del-refs   (HashSet.)
       :saved-max  (.get max-vec)}))

  (commit-vec-tx [_]
    (when-let [s @shadow]
      (let [shadow-idx  ^DTLV$usearch_index_t (:index s)
            shadow-vecs ^HashMap (:vecs s)
            tombstones  ^HashSet (:tombstones s)
            del-refs    ^HashSet (:del-refs s)
            txs         (ArrayList.)]
        ;; 1. Apply shadow additions to base
        (doseq [[vec-id vec-ref] shadow-vecs]
          (let [^long vid  vec-id
                vec-data   (get-vec* shadow-idx vid quantization dimensions)]
            (add index quantization vid vec-data)
            (.put vecs vid vec-ref)
            (.add txs (l/kv-tx :put vecs-dbi vec-ref vid :data :id))))
        ;; 2. Apply tombstones to base
        (doseq [id tombstones]
          (let [^long vid id]
            (VecIdx/remove index vid)
            (.remove vecs vid)))
        ;; 3. Delete refs from vecs-dbi
        (doseq [ref del-refs]
          (.add txs (l/kv-tx :del vecs-dbi ref)))
        ;; 4. Write vecs-dbi changes
        (when (pos? (.size txs))
          (binding [c/*bypass-wal*    true
                    c/*trusted-apply* true]
            (i/transact-kv lmdb txs)))
        ;; 5. Free shadow
        (VecIdx/free shadow-idx)
        (vreset! shadow nil))))

  (abort-vec-tx [_]
    (when-let [s @shadow]
      (VecIdx/free ^DTLV$usearch_index_t (:index s))
      (.set max-vec (long (:saved-max s)))
      (vreset! shadow nil)))

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

(defn- resolve-vec-ref
  "Resolve vec-id to vec-ref, checking shadow then base."
  [^VectorIndex vi ^long vec-id]
  (or (when-let [s @(.-shadow vi)]
        (.get ^HashMap (:vecs s) vec-id))
      ((.-vecs vi) vec-id)))

(defn- get-ref
  [^VectorIndex index vec-filter vec-id _]
  (when-let [vec-ref (resolve-vec-ref index vec-id)]
    (when (vec-filter vec-ref) vec-ref)))

(defn- get-ref-dist
  [^VectorIndex index vec-filter vec-id dist]
  (when-let [vec-ref (resolve-vec-ref index vec-id)]
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
  (let [vecs-dbi (str domain "/" c/vec-refs)]
    (open-dbi lmdb vecs-dbi)
    (open-vec-blob-dbis lmdb)
    (let [[max-vec-id vecs] (init-vecs lmdb vecs-dbi)
          fname             (index-fname lmdb domain)
          index             (create-index dimensions metric-type
                                          quantization connectivity
                                          expansion-add expansion-search)]
      ;; Load from LMDB blob; if not found, migrate from legacy .vid file
      (let [loaded? (load-from-lmdb lmdb index domain)]
        (when-not loaded?
          (when (u/file-exists fname)
            (VecIdx/load index fname)
            (checkpoint-to-lmdb lmdb index domain)
            (u/delete-files fname))))
      (swap! l/vector-indices assoc fname index)
      (->VectorIndex lmdb
                     (volatile! false)
                     index
                     fname
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
                     (ReentrantReadWriteLock.)
                     domain
                     (volatile! nil)
                     (volatile! false)))))

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
                 (ReentrantReadWriteLock.)
                 (.-domain old)
                 (.-shadow old)
                 (.-dirty? old)))

(defn attr-domain [attr] (s/replace (u/keyword->string attr) "/" "_"))
