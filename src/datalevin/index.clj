;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.index
  "Shared index utility functions used by storage and prepare."
  (:require
   [datalevin.interface :as i :refer [get-value]]
   [datalevin.bits :as b]
   [datalevin.custom-datalog :as cd]
   [datalevin.datom :as d]
   [datalevin.lmdb :as l]
   [datalevin.constants :as c]
   [datalevin.util :refer [raise]]
   [taoensso.nippy :as nippy])
  (:import
   [com.github.luben.zstd ZstdCompressCtx ZstdDecompressCtx]
   [java.nio ByteBuffer]
   [java.io DataOutput]
   [java.lang.ref WeakReference]
   [java.util Arrays]
   [java.util.concurrent ArrayBlockingQueue]
   [datalevin.bits Indexable Retrieved CustomReference]
   [datalevin.datom Datom]))

(defn value-type
  [props]
  (if-let [vt (:db/valueType props)]
    (if (identical? vt :db.type/tuple)
      (if-let [tts (props :db/tupleTypes)]
        tts
        (if-let [tt (props :db/tupleType)] [tt] :data))
      vt)
    :data))

(defn storage-type [lmdb props]
  (let [vt (value-type props)]
    (if (cd/custom-type? vt) (cd/descriptor lmdb vt) vt)))

(defn datom->indexable
  ([schema d high?] (datom->indexable nil schema d high?))
  ([lmdb schema ^Datom d high?]
   (let [e  (if-some [e (.-e d)] e (if high? c/emax c/e0))
         vm (if high? c/vmax c/v0)
         gm (if high? c/gmax c/g0)]
     (if-let [a (.-a d)]
       (let [p (schema a)
             aid (:db/aid p)]
         (if (some? aid)
           (if-some [v (.-v d)]
             (b/indexable e aid v (storage-type lmdb p)
                          (if (and (not high?) (cd/custom-type? (value-type p))) 0 gm))
             (b/indexable e aid vm (storage-type lmdb p) gm))
           (b/indexable e c/a0 c/v0 nil gm)))
       (let [am (if high? c/amax c/a0)]
         (if-some [v (.-v d)]
           (if (or (integer? v)
                   (identical? v :db.value/sysMax)
                   (identical? v :db.value/sysMin))
             (if e
               (b/indexable e am v :db.type/ref gm)
               (b/indexable (if high? c/emax c/e0) am v :db.type/ref gm))
             (raise
               "When v is known but a is unknown, v must be a :db.type/ref"
               {:v v}))
           (b/indexable e am vm :db.type/sysMin gm)))))))

(defn datom-range-predicate
  "Match the independent, inclusive entity and AVG bounds used by both native
  indexes. Value bounds use native encoding, including sentinel and prefix
  bucket semantics; transaction IDs do not participate in these ranges."
  [lmdb schema low high]
  (let [^Indexable low (datom->indexable lmdb schema low false)
        ^Indexable high (datom->indexable lmdb schema high true)
        le (long (.-e low))
        he (long (.-e high))
        la (.-a low)
        ha (.-a high)
        lo (b/indexable-bytes low)
        hi (b/indexable-bytes high)]
    (fn [^Datom datom]
      (let [e   (.-e datom)
            aid (:db/aid (schema (.-a datom)))]
        (and (<= le e he)
             (some? aid)
             (<= la (long aid) ha)
             (or (< la (long aid) ha)
                 ;; Pending values have no persisted giant/custom payload ID.
                 ;; The low encoder uses the minimum ID, which suffices here:
                 ;; closed bounds include the entire matching prefix bucket.
                 (let [bs (b/indexable-bytes
                            (datom->indexable lmdb schema datom false))]
                   (and (or (< la (long aid))
                            (not (neg? (Arrays/compareUnsigned bs lo))))
                        (or (< (long aid) ha)
                            (not (pos? (Arrays/compareUnsigned bs hi))))))))))))

(defonce index->dbi {:eav c/eav :ave c/ave})

(defonce index->ktype {:eav :id :ave :avg})

(defonce index->vtype {:eav :avg :ave :id})

(def ^:private ^"[B" giant-zstd-magic
  (byte-array [(byte 0x44) (byte 0x4C) (byte 0x47) (byte 0x5A)]))

(def ^:private giant-zstd-version (byte 1))

(def ^:private ^:const giant-zstd-header-size 9)

(defn- giant-zstd-envelope?
  [^bytes bs]
  (and (<= giant-zstd-header-size (long (alength bs)))
       (= (aget bs 0) (aget giant-zstd-magic 0))
       (= (aget bs 1) (aget giant-zstd-magic 1))
       (= (aget bs 2) (aget giant-zstd-magic 2))
       (= (aget bs 3) (aget giant-zstd-magic 3))
       (= (aget bs 4) giant-zstd-version)))

;; Zstd/compress and Zstd/decompress create and free a native context on every
;; call. Giant datoms reuse contexts from small bounded pools instead: writes are
;; serialized per store, reads are concurrent. A context has no finalizer, so one
;; that does not go back to its pool is closed.
(def ^:private ^:const giant-zstd-compressors-max 2)

(def ^:private ^:const giant-zstd-decompressors-max 16)

;; Filled on first use, so no native context is created at load time.
(defonce ^:private ^ArrayBlockingQueue giant-zstd-compressors
  (ArrayBlockingQueue. giant-zstd-compressors-max))

(defonce ^:private ^ArrayBlockingQueue giant-zstd-decompressors
  (ArrayBlockingQueue. giant-zstd-decompressors-max))

(defn- giant-zstd-compress
  ^bytes [^bytes raw ^long level]
  (let [^ZstdCompressCtx ctx (or (.poll giant-zstd-compressors)
                                 (ZstdCompressCtx.))
        compressed           (try
                               (.compress (.setLevel ctx (int level)) raw)
                               (catch Throwable e
                                 (.close ctx)
                                 (throw e)))]
    (when-not (.offer giant-zstd-compressors ctx) (.close ctx))
    compressed))

(defn- giant-zstd-decompress
  "Decompress `len` bytes of `bs` from `off` into a new array of `raw-len`."
  ^bytes [^bytes bs ^long off ^long len ^long raw-len]
  (let [^ZstdDecompressCtx ctx (or (.poll giant-zstd-decompressors)
                                   (ZstdDecompressCtx.))
        raw                    (try
                                 (.decompress ctx bs (int off) (int len)
                                              (int raw-len))
                                 (catch Throwable e
                                   (.close ctx)
                                   (throw e)))]
    (when-not (.offer giant-zstd-decompressors ctx) (.close ctx))
    raw))

(defn- maybe-compress-giant-datom-bytes
  ^bytes [^bytes raw]
  (let [threshold (long c/*giants-zstd-threshold*)]
    (when (<= threshold (long (alength raw)))
      (let [compressed (giant-zstd-compress raw (long c/*giants-zstd-level*))]
        (when (< (alength compressed) (alength raw))
          (let [compressed-len (alength compressed)
                out            (byte-array
                                 (unchecked-add-int
                                   (int giant-zstd-header-size)
                                   compressed-len))
                bb             (ByteBuffer/wrap out)]
            (.put bb ^bytes giant-zstd-magic)
            (.put bb ^byte giant-zstd-version)
            (.putInt bb (alength raw))
            (.put bb ^bytes compressed)
            out))))))

(deftype ^:private EncodedGiantDatom [^Datom datom ^bytes value])

;; Use the existing datom wire ID and fields. The standalone value owns its
;; Nippy cache: the preceding attribute is a single uncached keyword and the
;; following transaction ID cannot introduce references into that value.
(nippy/extend-freeze EncodedGiantDatom :datalevin/datom
  [^EncodedGiantDatom x ^DataOutput out]
  (let [^Datom datom (.-datom x)]
    (.writeLong out (.-e datom))
    (nippy/freeze-to-out! out (.-a datom))
    (.write out ^bytes (.-value x))
    (when-let [tx (.-tx datom)]
      (nippy/freeze-to-out! out tx))))

(defn encode-giant-datom
  "Encode a datom for `datalevin/giants`.
  Returns owned bytes as {:value bytes :vtype :raw}. An optional standalone
  Nippy value encoding avoids serializing an untyped index payload again.
  The latter form takes a storage datom with a keyword attribute, no metadata."
  ([datom] (encode-giant-datom datom nil))
  ([^Datom datom encoded-value]
   (let [encoded (if encoded-value (EncodedGiantDatom. datom encoded-value) datom)
         raw (if (and encoded-value nippy/*shared-dict*)
               ;; Let the value's own marker activate its shared dictionary.
               ;; A preceding attribute marker would reset the same thaw cache.
               (binding [nippy/*shared-dict* nil]
                 (b/serialize encoded))
               (b/serialize encoded))]
     {:value (or (maybe-compress-giant-datom-bytes raw) raw) :vtype :raw})))

(defn decode-giant-datom
  "Decode `datalevin/giants` value bytes (supports both legacy :data encoding
  and zstd-compressed raw envelope)."
  [^bytes bs]
  (if (giant-zstd-envelope? bs)
    (let [raw-len (.getInt (ByteBuffer/wrap bs) 5)]
      ;; decompress straight from the envelope, no copy of the payload
      (b/deserialize
        (giant-zstd-decompress bs giant-zstd-header-size
                               (- (alength bs) giant-zstd-header-size)
                               raw-len)))
    (b/deserialize bs)))

(defn- giant-zstd-buffer? [^ByteBuffer buffer]
  (let [p (.position buffer)]
    (and (<= giant-zstd-header-size (.remaining buffer))
         (= (.getInt buffer p) 0x444C475A)
         (= (.get buffer (+ p 4)) giant-zstd-version))))

(defn giant-datom-raw-size
  "Uncompressed byte count, without copying or decoding a borrowed giant value.
  Consumers must reserve for both encoded and expanded buffers before decoding."
  ^long [^ByteBuffer buffer]
  (if (giant-zstd-buffer? buffer)
    (let [size (.getInt buffer (+ (.position buffer) 5))]
      (when (neg? size) (raise "Invalid giant value size" {}))
      size)
    (.remaining buffer)))

(defn giant-datom-buffer
  "Expose raw Nippy bytes, decompressing the storage Zstd envelope if present.
  The caller must reserve giant-datom-raw-size plus the encoded size first.
  Uncompressed buffers are borrowed and cannot outlive the read transaction."
  ^ByteBuffer [^ByteBuffer buffer]
  (if (giant-zstd-buffer? buffer)
    (let [size (giant-datom-raw-size buffer)
          bytes (b/get-bytes (.duplicate buffer))]
      (ByteBuffer/wrap
        (giant-zstd-decompress bytes giant-zstd-header-size
                               (- (alength ^bytes bytes) giant-zstd-header-size)
                               size)))
    (.duplicate buffer)))

(defn index->k
  ([index schema datom high?] (index->k index nil schema datom high?))
  ([index lmdb schema ^Datom datom high?]
   (case index
     :eav (or (.-e datom) (if high? c/emax c/e0))
     :ave (datom->indexable lmdb schema datom high?))))

(defn index->v
  ([index schema datom high?] (index->v index nil schema datom high?))
  ([index lmdb schema ^Datom datom high?]
   (case index
     :eav (datom->indexable lmdb schema datom high?)
     :ave (or (.-e datom) (if high? c/emax c/e0)))))

(deftype ^:private ResolvedGiant
  [value ^long e a ^long id ^WeakReference writer ^bytes avg])

(defn- owned-writer
  [lmdb]
  (when (l/writing? lmdb)
    (let [writer (l/write-txn lmdb)]
      (when (Thread/holdsLock writer) @writer))))

(defn logical-value
  "Return the logical value of a preparation read. Inline values pass through."
  [value]
  (if (instance? ResolvedGiant value) (.-value ^ResolvedGiant value) value))

(defn datom-prepared-value
  "Keep a resolved giant's physical reference when preparing its replacement."
  [^Datom datom]
  (when datom (or (d/storage-reference datom) (.-v datom))))

(defn retraction
  "Retract a preparation value, retaining its writer-scoped physical reference."
  [e a value tx]
  (let [datom (d/datom e a (logical-value value) tx false)]
    (if (instance? ResolvedGiant value)
      (d/set-storage-reference! datom value)
      datom)))

(defn- current-giant-reference
  [lmdb ^Datom datom]
  (when-let [reference (d/storage-reference datom)]
    (when (instance? ResolvedGiant reference)
      (let [^ResolvedGiant reference reference
            writer (.get ^WeakReference (.-writer reference))]
        (when (and writer (identical? writer (owned-writer lmdb))
                   (= (.-e reference) (.-e datom))
                   (= (.-a reference) (.-a datom))
                   (identical? (.-value reference) (.-v datom)))
          reference)))))

(defn take-giant-reference!
  "Consume a physical reference only in the native writer that resolved it.
  The caller must still check EAV membership before using the ID. References
  hold the writer weakly and never appear in datom metadata or serialization."
  [lmdb ^Datom datom]
  (let [reference (current-giant-reference lmdb datom)]
    (when (d/storage-reference datom) (d/set-storage-reference! datom nil))
    (when reference (.-id ^ResolvedGiant reference))))

(defn take-giant-key!
  "Consume an owned AVG key under the writer that resolved it. The attribute
  ID must still match, and the caller must verify exact EAV membership. Reads
  without an encoded key retain their ID reference for the ordinary fallback."
  [lmdb ^Datom datom aid]
  (when-let [^ResolvedGiant reference (current-giant-reference lmdb datom)]
    (when-let [avg (.-avg reference)]
      (when (= aid (b/avg->aid (ByteBuffer/wrap avg)))
        (d/set-storage-reference! datom nil)
        avg))))

(defprotocol PreparationRead
  (preparation-first-v [store e a]
    "Resolve a value and retain a giant reference under an owned writer."))

(extend-protocol PreparationRead
  Object
  (preparation-first-v [store e a] (i/ea-first-v store e a)))

(defn gt->datom
  ([lmdb gt] (gt->datom lmdb gt nil))
  ([lmdb gt avg]
   (let [writer (owned-writer lmdb)]
     (when-let [bs (get-value lmdb c/giants gt :id :raw)]
       (let [^Datom datom (decode-giant-datom bs)]
         (if writer
           (d/set-storage-reference!
             datom (ResolvedGiant. (.-v datom) (.-e datom) (.-a datom)
                                   gt (WeakReference. writer) avg))
           datom))))))

(defn retrieved->v
  [lmdb ^Retrieved r]
  (let [g (.-g r)]
    (if (= g c/normal)
      (let [v (.-v r)]
        (if (instance? CustomReference v) (cd/read-value lmdb (.-a r) v) v))
      (d/datom-v (gt->datom lmdb g)))))

(defn avg-buffer->v
  [lmdb ^ByteBuffer bf]
  (let [g (b/avg->giant-id bf)]
    (if (= g c/normal)
      (let [v (b/avg->inline-value bf)]
        (if (instance? CustomReference v) (cd/read-value lmdb (b/avg->aid bf) v) v))
      (d/datom-v (gt->datom lmdb g)))))

(defn avg-buffer->prepared-value
  [lmdb ^ByteBuffer bf]
  (let [g (b/avg->giant-id bf)]
    (if (= g c/normal)
      (let [v (b/avg->inline-value bf)]
        (if (instance? CustomReference v) (cd/read-value lmdb (b/avg->aid bf) v) v))
      ;; Giant lookup and decoding can reuse or invalidate the cursor buffer.
      ;; Detach the complete logical AVG key before either operation occurs.
      (let [avg (when (owned-writer lmdb)
                  (b/get-bytes (doto (.duplicate bf) (.position 0))))]
        (datom-prepared-value (gt->datom lmdb g avg))))))

(defn avg-buffer->datom
  [lmdb ^ByteBuffer bf e a]
  (let [value (avg-buffer->prepared-value lmdb bf)
        datom (d/datom e a (logical-value value))]
    (if (instance? ResolvedGiant value)
      (d/set-storage-reference! datom value)
      datom)))
