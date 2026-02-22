;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.wal
  "Write Ahead Log (WAL)"
  (:require
   [clojure.java.io :as io]
   [clojure.stacktrace :as stt]
   [clojure.string :as s]
   [datalevin.bits :as b]
   [datalevin.lmdb :as l]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.util :as u :refer [raise]]
   [datalevin.interface :as i])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
    DataOutputStream EOFException File]
   [java.util.zip CRC32C]
   [java.util Arrays Collections WeakHashMap]
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.channels FileChannel]
   [java.nio.file StandardOpenOption]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent TimeUnit ScheduledExecutorService
    ScheduledFuture]
   [com.github.luben.zstd Zstd]))

(def ^:const wal-format-major 1)
(def ^:const wal-format-minor 0)
(def ^:const wal-compression-threshold 1024)
(def ^:const wal-compression-level 3)
(def ^:private wal-record-magic (byte-array [(byte 0x44) (byte 0x4C)
                                             (byte 0x57) (byte 0x4C)]))
(def ^:private wal-meta-magic (byte-array [(byte 0x44) (byte 0x4C)
                                           (byte 0x57) (byte 0x4D)]))

(def ^:const wal-meta-version (byte 1))

(def ^:const op-kv-put (byte 0x10))
(def ^:const op-kv-del (byte 0x11))
(def ^:const op-kv-put-list (byte 0x16))
(def ^:const op-kv-del-list (byte 0x17))

(def ^:const wal-type-tuple-tag (byte 0x00))

(def ^:private wal-flag-keywords
  [:nooverwrite :nodupdata :current :reserve :append :appenddup :multiple
   :dupsort])

(def ^:private wal-flag->id
  (zipmap wal-flag-keywords
          (mapv byte (range 1 (inc (count wal-flag-keywords))))))

(def ^:private wal-id->flag
  (into {} (map (fn [[k v]] [(int (bit-and (int v) 0xFF)) k])) wal-flag->id))

(def ^:private wal-type-keywords
  [:string :bigint :bigdec :long :float :double :bytes :keyword :symbol
   :boolean :instant :uuid :instant-pre-06 :byte :short :int :id :int-int
   :ints :bitmap :term-info :doc-info :pos-info :attr :avg :raw :datom :data])

(def ^:private wal-type->id
  (zipmap wal-type-keywords
          (mapv byte (range 1 (inc (count wal-type-keywords))))))

(def ^:private wal-id->type
  (into {} (map (fn [[k v]] [(int (bit-and (int v) 0xFF)) k])) wal-type->id))

(defn- type->byte
  [t]
  (let [t (or t :data)]
    (or (wal-type->id t)
        (u/raise "Unsupported WAL value type" {:error :wal/invalid-type
                                               :type  t}))))

(defn- byte->type
  [b]
  (or (wal-id->type (int (bit-and (int b) 0xFF)))
      (u/raise "Unknown WAL value type tag"
               {:error :wal/invalid-type-tag :tag b})))

(declare write-u16 read-u16)

(defn- flag->byte
  [f]
  (or (wal-flag->id f)
      (u/raise "Unsupported WAL flag" {:error :wal/invalid-flag
                                       :flag  f})))

(defn- byte->flag
  [b]
  (or (wal-id->flag (int (bit-and (int b) 0xFF)))
       (u/raise "Unknown WAL flag tag"
                {:error :wal/invalid-flag-tag :tag b})))

(defn- with-segment-data-input
  [path f]
  (try
    (with-open [^DataInputStream in (DataInputStream. (io/input-stream path))]
      (f in))
    (catch java.io.IOException _)))

(defn- normalize-flags
  [flags]
  (cond
    (nil? flags) []
    (set? flags) (->> flags (sort-by name) vec)
    (sequential? flags) (vec flags)
    :else (u/raise "Invalid WAL flags" {:error :wal/invalid-flags
                                        :flags flags})))

(defn- write-type!
  [^DataOutputStream out t]
  (if (vector? t)
    (let [cnt (count t)]
      (when (zero? cnt)
        (u/raise "Tuple type must not be empty"
                 {:error :wal/invalid-type :type t}))
      (.writeByte out (int wal-type-tuple-tag))
      (write-u16 out cnt)
      (doseq [et t]
        (when (vector? et)
          (u/raise "Nested tuple types are not supported in WAL"
                   {:error :wal/invalid-type :type t}))
        (.writeByte out (int (type->byte et)))))
    (.writeByte out (int (type->byte t)))))

(defn- read-type
  [^DataInputStream in]
  (let [tag (.readUnsignedByte in)]
    (if (zero? ^int tag)
      (let [cnt (read-u16 in)]
        (when (zero? ^short cnt)
          (u/raise "Tuple type must not be empty"
                   {:error :wal/invalid-type-count :count cnt}))
        (vec (repeatedly cnt #(byte->type (byte (.readUnsignedByte in))))))
      (byte->type (byte tag)))))

(defn- write-flags!
  [^DataOutputStream out flags]
  (let [fs  (normalize-flags flags)
        cnt (count fs)]
    (when (> cnt 255)
      (u/raise "Too many WAL flags"
               {:error :wal/flags-overflow :count cnt}))
    (.writeByte out (int cnt))
    (doseq [f fs]
      (.writeByte out (int (flag->byte f))))))

(defn- read-flags
  [^DataInputStream in]
  (let [cnt (.readUnsignedByte in)]
    (when (pos? ^int cnt)
      (vec (repeatedly cnt #(byte->flag (byte (.readUnsignedByte in))))))))

(defn- crc32c
  ^long [^bytes bs off len]
  (let [crc (CRC32C.)]
    (.update crc bs off len)
    (.getValue crc)))

(defn- write-bytes!
  [^DataOutputStream out ^bytes bs]
  (.write out bs 0 (alength bs)))

(defn- read-bytes
  [^DataInputStream in n]
  (let [bs (byte-array n)]
    (.readFully in bs)
    bs))

(defn- encode-bits
  ^bytes [x t]
  (let [t (or t :data)]
    (loop [^ByteBuffer buf (bf/get-tl-buffer)]
      (let [result (try
                     (b/put-bf buf x t)
                     (b/get-bytes (.duplicate buf))
                     (catch Exception e
                       (if (instance? BufferOverflowException e)
                         ::overflow
                         (throw e))))]
        (if (not= result ::overflow)
          result
          (let [new-buf (ByteBuffer/allocate (* 2 (.capacity buf)))]
            (bf/set-tl-buffer new-buf)
            (recur new-buf)))))))

(defn- encode-to-buf
  "Encode value x with type t into the thread-local ByteBuffer.
  Returns the buffer flipped and ready to read (position=0, limit=encoded-len).
  Avoids allocating a byte array — caller can write directly from backing array."
  ^ByteBuffer [x t]
  (let [t (or t :data)]
    (loop [^ByteBuffer buf (bf/get-tl-buffer)]
      (let [ok (try
                 (b/put-bf buf x t)
                 true
                 (catch Exception e
                   (if (instance? BufferOverflowException e)
                     false
                     (throw e))))]
        (if ok
          buf
          (let [new-buf (ByteBuffer/allocate (* 2 (.capacity buf)))]
            (bf/set-tl-buffer new-buf)
            (recur new-buf)))))))

(defn- write-buf!
  "Write the readable portion of a heap ByteBuffer to a DataOutputStream.
  Does not allocate — writes directly from the backing array."
  [^DataOutputStream out ^ByteBuffer buf]
  (.write out (.array buf) (.position buf) (.remaining buf)))

(defn- dbi-name-bytes
  ^bytes [dbi-name]
  (.getBytes ^String dbi-name StandardCharsets/UTF_8))

(defn- write-u16
  [^DataOutputStream out ^long v]
  (.writeShort out (int v)))

(defn- write-u32
  [^DataOutputStream out ^long v]
  (.writeInt out (unchecked-int v)))

(defn- write-u64
  [^DataOutputStream out ^long v]
  (.writeLong out v))

(defn- read-u16
  [^DataInputStream in]
  (bit-and (.readUnsignedShort in) 0xFFFF))

(defn- read-u32
  [^DataInputStream in]
  (bit-and (.readInt in) 0xFFFFFFFF))

(defn- read-u64
  [^DataInputStream in]
  (.readLong in))

(defn- normalize-type
  [t]
  (or t :data))

(defn- write-kv-op!
  "Encode and write a single KV op directly to the output stream."
  [^DataOutputStream out op-map]
  (let [{:keys [op dbi k v kt vt flags dupsort? k-bytes dbi-bytes]} op-map

        ^bytes dbi-bs (or dbi-bytes (dbi-name-bytes dbi))
        k-type        (normalize-type kt)
        ^bytes k-bs   (or k-bytes (encode-bits k k-type))
        dbi-len       (alength dbi-bs)
        k-len         (alength k-bs)
        flags         (cond-> (normalize-flags flags)
                        dupsort? (conj :dupsort))]
    (case op
      :put
      (do
        (.writeByte out (int op-kv-put))
        (write-u16 out dbi-len) (write-bytes! out dbi-bs)
        (write-type! out k-type) (write-u16 out k-len) (write-bytes! out k-bs)
        (let [v-type           (normalize-type vt)
              ^ByteBuffer vbuf (encode-to-buf v v-type)]
          (write-type! out v-type) (write-u32 out (.remaining vbuf))
          (write-buf! out vbuf))
        (write-flags! out flags))

      :del
      (do
        (.writeByte out (int op-kv-del))
        (write-u16 out dbi-len) (write-bytes! out dbi-bs)
        (write-type! out k-type) (write-u16 out k-len) (write-bytes! out k-bs)
        (write-flags! out flags))

      :put-list
      (do
        (.writeByte out (int op-kv-put-list))
        (write-u16 out dbi-len) (write-bytes! out dbi-bs)
        (write-type! out k-type) (write-u16 out k-len) (write-bytes! out k-bs)
        ;; The value list is Nippy-encoded as a collection (:data), but
        ;; the type tag records the *element* type so that WAL replay
        ;; can re-encode individual elements into the overlay with the
        ;; correct DBI value type.
        (let [v-type           (normalize-type vt)
              ^ByteBuffer vbuf (encode-to-buf v :data)]
          (write-type! out v-type) (write-u32 out (.remaining vbuf))
          (write-buf! out vbuf))
        (write-flags! out flags))

      :del-list
      (do
        (.writeByte out (int op-kv-del-list))
        (write-u16 out dbi-len) (write-bytes! out dbi-bs)
        (write-type! out k-type) (write-u16 out k-len) (write-bytes! out k-bs)
        (let [v-type           (normalize-type vt)
              ^ByteBuffer vbuf (encode-to-buf v :data)]
          (write-type! out v-type) (write-u32 out (.remaining vbuf))
          (write-buf! out vbuf))
        (write-flags! out flags))

      (u/raise "Unsupported KV WAL op" {:error :wal/invalid-op :op op}))))

(defn- decode-kv-op
  [^DataInputStream in]
  (let [opcode  (byte (.readUnsignedByte in))
        dbi-len (read-u16 in)
        dbi-bs  (read-bytes in dbi-len)
        dbi     (String. ^bytes dbi-bs StandardCharsets/UTF_8)
        k-type  (read-type in)
        k-len   (read-u16 in)
        k-bs    (read-bytes in k-len)]
    (case opcode
      0x10 (let [v-type    (read-type in)
                 v-len     (read-u32 in)
                 v-bs      (read-bytes in v-len)
                 flags     (read-flags in)
                 dupsort?  (boolean (some #{:dupsort} flags))
                 wal-flags (seq (remove #{:dupsort} flags))]
             (cond-> {:op :put :dbi dbi :k k-bs :kt k-type
                      :v v-bs :vt v-type :raw? true}
               dupsort?      (assoc :dupsort? true)
               wal-flags     (assoc :flags (vec wal-flags))))
      0x11 (let [flags     (read-flags in)
                 dupsort?  (boolean (some #{:dupsort} flags))
                 wal-flags (seq (remove #{:dupsort} flags))]
             (cond-> {:op :del :dbi dbi :k k-bs :kt k-type :raw? true}
               dupsort?      (assoc :dupsort? true)
               wal-flags     (assoc :flags (vec wal-flags))))
      0x16 (let [v-type    (read-type in)
                 v-len     (read-u32 in)
                 v-bs      (read-bytes in v-len)
                 flags     (read-flags in)
                 dupsort?  (boolean (some #{:dupsort} flags))
                 wal-flags (seq (remove #{:dupsort} flags))]
             (cond-> {:op :put-list :dbi dbi :k k-bs :kt k-type
                      :v v-bs :vt v-type :raw? true}
               dupsort?      (assoc :dupsort? true)
               wal-flags     (assoc :flags (vec wal-flags))))
      0x17 (let [v-type    (read-type in)
                 v-len     (read-u32 in)
                 v-bs      (read-bytes in v-len)
                 flags     (read-flags in)
                 dupsort?  (boolean (some #{:dupsort} flags))
                 wal-flags (seq (remove #{:dupsort} flags))]
             (cond-> {:op :del-list :dbi dbi :k k-bs :kt k-type
                      :v v-bs :vt v-type :raw? true}
               dupsort?      (assoc :dupsort? true)
               wal-flags     (assoc :flags (vec wal-flags))))
      (u/raise "Unknown WAL opcode" {:error  :wal/invalid-opcode
                                     :opcode opcode}))))

(def ^:const ^:private record-header-size 14)
(def ^:private ^:const flag-compressed 0x01)

(defn- compress-body
  "Compress body bytes using zstd if compression is beneficial.
   Returns [compressed-bytes compressed?]"
  [^bytes body]
  (if (> (alength body) wal-compression-threshold)
    (let [compressed (Zstd/compress body wal-compression-level)
          body-len   (alength body)
          comp-len   (alength compressed)]
      (if (< comp-len body-len)
        [compressed true]
        [body false]))
    [body false]))

(defn- decompress-body
  "Decompress body bytes using zstd. Returns decompressed bytes."
  [^bytes compressed ^long uncompressed-size]
  (Zstd/decompress compressed uncompressed-size))

(def ^:private ^ThreadLocal tl-record-bos
  (ThreadLocal/withInitial
    (reify java.util.function.Supplier
      (get [_] (ByteArrayOutputStream. 512)))))

(defn record-bytes
  [wal-id user-tx-id tx-time ops]
  (let [op-count                   (count ops)
        ^ByteArrayOutputStream bos (.get tl-record-bos)
        _                          (.reset bos)
        out                        (DataOutputStream. bos)]
    ;; Build body first (without header)
    (write-u64 out wal-id)
    (write-u64 out user-tx-id)
    (write-u64 out user-tx-id)
    (write-u32 out 1)
    ;; user-tx entry
    (write-u64 out user-tx-id)
    (write-u64 out tx-time)
    (write-u32 out 0)                          ;; tx-meta-len
    (write-u32 out 0)                          ;; op-start
    (write-u32 out op-count)                   ;; op-end (exclusive)
    (write-u32 out op-count)
    (doseq [op ops]
      (write-kv-op! out op))
    (.close out)
    (let [body-raw           (.toByteArray bos)
          [body compressed?] (compress-body body-raw)
          body-len           (alength ^bytes body)
          total-len          (+ ^long record-header-size body-len)
          buf                (byte-array total-len)
          bb                 (ByteBuffer/wrap buf)]
      ;; Write header
      (.put bb ^bytes wal-record-magic)
      (.put bb (byte wal-format-major))
      (.put bb (byte (if compressed? flag-compressed 0)))
      (.putInt bb body-len)
      ;; Calculate checksum over body only
      (let [checksum (crc32c body 0 body-len)]
        (.putInt bb (unchecked-int checksum)))
      ;; Write body
      (System/arraycopy body 0 buf record-header-size body-len)
      buf)))

(defn wal-dir-path
  [dir]
  (str dir u/+separator+ c/wal-dir))

(defn wal-meta-path
  [dir]
  (str (wal-dir-path dir) u/+separator+ c/wal-meta-file))

(defn segment-path
  [dir segment-id]
  (format "%s%ssegment-%016d.wal" (wal-dir-path dir) u/+separator+ segment-id))

(defn segment-files
  "Return a sorted seq of WAL segment files in the given directory."
  [dir]
  (let [wal-dir (io/file (wal-dir-path dir))]
    (when (.exists wal-dir)
      (->> (.listFiles wal-dir)
           (filter #(and (.isFile ^File %)
                         (.endsWith (.getName ^File %) ".wal")))
           (sort-by #(.getName ^File %))))))

(defn parse-segment-id
  [^File f]
  (let [name (.getName f)
        n    (count name)]
    (try
      (Long/parseLong (subs name 8 (- n 4)))
      (catch Exception _ nil))))

(defn- open-channel
  [path]
  (FileChannel/open (.toPath (io/file path))
                    (into-array StandardOpenOption
                                [StandardOpenOption/CREATE
                                 StandardOpenOption/WRITE
                                 StandardOpenOption/APPEND])))

(defn sync-channel!
  [^FileChannel ch mode]
  (case mode
    :none nil
    :fdatasync (.force ch false)
    :fsync (.force ch true)
    (.force ch true)))

(defn- read-record
  [^DataInputStream in]
  (let [^bytes magic (read-bytes in 4)]
    (when (not (Arrays/equals magic ^bytes wal-record-magic))
      (u/raise "Bad WAL record magic" {:error :wal/bad-magic}))
    (let [version     (.readUnsignedByte in)
          flags       (.readUnsignedByte in)
          body-len    (read-u32 in)
          checksum    (read-u32 in)
          compressed? (not= 0 (bit-and flags flag-compressed))
          body        (let [bs (byte-array body-len)]
                        (.readFully in bs)
                        bs)
          actual      (crc32c body 0 body-len)]
      (when (not= checksum actual)
        (u/raise "WAL record checksum mismatch"
                 {:error    :wal/bad-checksum
                  :expected checksum
                  :actual   actual}))
      (when (not= version wal-format-major)
        (u/raise "WAL format version mismatch"
                 {:error    :wal/bad-version
                  :expected wal-format-major
                  :actual   version}))
      ;; Decompress if needed - original size is stored in zstd frame header
      (let [body (if compressed?
                   (let [orig-size (Zstd/getFrameContentSize body)]
                     (decompress-body body orig-size))
                   body)]
        (with-open [bin (ByteArrayInputStream. body)
                    din (DataInputStream. bin)]
          (let [wal-id      (read-u64 din)
                _user-start (read-u64 din)
                _user-end   (read-u64 din)
                user-count  (read-u32 din)
                tx-time     (loop [i 0 t 0]
                              (if (< i ^int user-count)
                                (let [_uid    (read-u64 din)
                                      tt      (read-u64 din)
                                      ^int ml (read-u32 din)]
                                  (when (pos? ml) (read-bytes din ml))
                                  (read-u32 din)
                                  (read-u32 din)
                                  (recur (inc i) (max (long t) (long tt))))
                                t))
                op-count    ^int (read-u32 din)
                ops         (loop [i 0 acc []]
                              (if (< i op-count)
                                 (recur (inc i) (conj acc (decode-kv-op din)))
                                 acc))]
             {:wal/tx-id wal-id :wal/tx-time tx-time :wal/ops ops}))))))

(declare find-start-segment)

(defn read-wal-records
  [dir from-id upto-id]
  (let [from-id (long (or from-id 0))
        upto-id (long (or upto-id Long/MAX_VALUE))
        all-files (segment-files dir)
        ;; Skip segments whose max WAL ID is <= from-id.
        relevant-files (find-start-segment all-files from-id)]
    (lazy-seq
      (mapcat
        (fn [^File f]
          (let [path (.getAbsolutePath f)]
            (or
             (with-segment-data-input
               path
               (fn [in]
                 (loop [acc (transient [])]
                   (let [rec (try
                               (read-record in)
                               (catch EOFException _ ::eof))]
                     (if (= rec ::eof)
                       (persistent! acc)
                       (let [wal-id (long (:wal/tx-id rec))]
                         (cond
                           (<= wal-id from-id) (recur acc)
                           (<= wal-id upto-id) (recur (conj! acc rec))
                           :else               (persistent! acc))))))))
             [])))
        relevant-files))))

(defn scan-last-wal
  [dir]
  (let [files (segment-files dir)]
    (loop [fs        files
           last-id   0
           last-seg  0
           last-time 0]
      (if-let [^File f (first fs)]
        (let [seg-id (long (or (parse-segment-id f) 0))
              path   (.getAbsolutePath f)
              [cur-id cur-time]
              (or
                (with-segment-data-input
                  path
                  (fn [in]
                    (loop [cid   last-id
                           ctime last-time]
                      (let [rec
                            (try
                              (read-record in)
                              (catch EOFException _ ::eof)
                              (catch Exception e
                                (binding [*out* *err*]
                                  (println
                                    (str "WAL parse error in segment "
                                         path
                                         " (stopping scan at last good record): "
                                         (.getMessage e))))
                                ::eof))]
                        (if (= rec ::eof)
                          [cid ctime]
                          (recur (long (:wal/tx-id rec))
                                 (max ctime
                                      (long (:wal/tx-time rec)))))))))
                [last-id last-time])]
          (recur (next fs)
                 (long cur-id)
                 (max last-seg seg-id)
                 (long cur-time)))
        {:last-wal-id     last-id
         :last-segment-id last-seg
         :last-segment-ms last-time}))))

;;
;; Per-segment max-WAL-ID cache keyed by [absolute-path file-size].
;; File-size auto-invalidates entries for the active (still-growing) segment;
;; call evict-segment-max-id-cache! when a segment is deleted by GC.
;;
(def ^:private ^java.util.concurrent.ConcurrentHashMap segment-max-id-cache
  (java.util.concurrent.ConcurrentHashMap.))

(defn evict-segment-max-id-cache!
  "Remove the cached max-WAL-ID entry for f. Must be called before deleting
  a segment file so stale entries don't survive GC."
  [^File f]
  (.remove segment-max-id-cache (.getAbsolutePath f)))

(defn- scan-segment-max-wal-id
  "Scan a segment file and return its highest WAL ID, or 0 if empty/unreadable.
  For uncompressed records reads only the 8-byte wal-id then skips the rest of
  the body, avoiding a full-body read. Compressed records are decompressed in
  full because the wal-id is not addressable before decompression."
  [^File f]
  (let [path (.getAbsolutePath f)]
    (or
      (with-segment-data-input
        path
        (fn [^DataInputStream in]
          (loop [max-id 0]
            (let [result
                  (try
                    (let [^bytes magic (read-bytes in 4)]
                      (when-not (Arrays/equals magic ^bytes wal-record-magic)
                        (u/raise "Bad WAL record magic"
                                 {:error :wal/bad-magic}))
                      (let [_version    (.readUnsignedByte in)
                            flags       (.readUnsignedByte in)
                            body-len    (read-u32 in)
                            _checksum   (read-u32 in)
                            compressed? (not= 0 (bit-and flags flag-compressed))
                            wal-id
                            (if compressed?
                              ;; Compressed: must read and decompress full body.
                              (let [body (let [bs (byte-array body-len)]
                                           (.readFully in bs)
                                           bs)]
                                (.getLong
                                  (ByteBuffer/wrap
                                    (decompress-body
                                      body
                                      (Zstd/getFrameContentSize body)))))
                              ;; Uncompressed: read wal-id (first 8 bytes) then
                              ;; skip the rest
                              (let [id      (.readLong in)
                                    to-skip ^long (- ^long body-len 8)]
                                (when (pos? to-skip)
                                  (let [^long remaining
                                        (loop [rem to-skip]
                                          (if (pos? rem)
                                            (let [n (.skip in rem)]
                                              (if (pos? n)
                                                (recur (- rem n))
                                                rem))
                                            0))]
                                    ;; If we cannot skip the full record body,
                                    ;; treat it as truncated EOF and ignore this
                                    ;; partial tail record.
                                    (when (pos? remaining)
                                      (throw (EOFException.)))))
                                id))]
                        wal-id))
                    (catch EOFException _ ::eof)
                    (catch Exception _ ::eof))]
              (if (= result ::eof)
                max-id
                (recur (long result)))))))
      0)))

(defn segment-max-wal-id
  "Return the highest wal-id in a segment file, or 0 if empty/unreadable.
  Results are cached by (path, file-size). A growing active segment is
  re-scanned whenever its size has changed; deleted segments should be
  evicted via evict-segment-max-id-cache! before deletion."
  [^File f]
  (let [path  (.getAbsolutePath f)
        fsize (.length f)
        entry (.get segment-max-id-cache path)]
    (if (and entry (= ^long (nth entry 0) fsize))
      (nth entry 1)
      (let [max-id (scan-segment-max-wal-id f)]
        (.put segment-max-id-cache path [fsize max-id])
        max-id))))

(defn- find-start-segment
  "Find the first segment file that contains WAL IDs > from-id.
  Opens each segment to read its max WAL ID, skipping any whose
  highest ID is <= from-id. This is correct regardless of how many
  transactions a single segment holds (e.g. with large 256 MB segments
  a single file may span thousands of WAL IDs, so an ID-ratio heuristic
  would incorrectly discard valid data)."
  [files from-id]
  (let [from-id (long from-id)]
    (if (<= from-id 0)
      files
      (drop-while
       (fn [^File f]
         (<= (long (segment-max-wal-id f)) from-id))
       files))))

(def ^:private wal-meta-slot-size 64)
(def ^:private wal-meta-payload-off 9)
(def ^:private wal-meta-off-committed-wal 9)
(def ^:private wal-meta-off-committed-user 17)
(def ^:private wal-meta-off-committed-last-ms 25)
(def ^:private wal-meta-off-indexed-wal 33)
(def ^:private wal-meta-off-revision 41)
(def ^:private wal-meta-off-format-major 49)
(def ^:private wal-meta-off-format-minor 50)
(def ^:private wal-meta-off-enabled 51)
(def ^:private wal-meta-off-last-segment 52)
(def ^:private ^java.util.Map wal-meta-publish-locks
  (Collections/synchronizedMap (WeakHashMap.)))

(defn- wal-meta-publish-lock
  ^Object [dir]
  (let [path (wal-meta-path dir)]
    (or (.get wal-meta-publish-locks path)
        (locking wal-meta-publish-locks
          (or (.get wal-meta-publish-locks path)
              (let [lock (Object.)]
                (.put wal-meta-publish-locks path lock)
                lock))))))

(defn- merge-wal-meta-snapshot
  [existing snapshot]
  (let [max-field (fn [k]
                    (max ^long (long (or (get existing k) 0))
                         ^long (long (or (get snapshot k) 0))))
        committed (max-field c/last-committed-wal-tx-id)
        indexed   (min ^long committed ^long (max-field c/last-indexed-wal-tx-id))
        user      (min ^long committed
                       ^long (max-field c/last-committed-user-tx-id))]
    (-> (merge existing snapshot)
        (assoc c/last-committed-wal-tx-id committed
               c/last-indexed-wal-tx-id indexed
               c/last-committed-user-tx-id user
               c/committed-last-modified-ms
               (max-field c/committed-last-modified-ms)
               :wal/last-segment-id (max-field :wal/last-segment-id)))))

(defn- meta-slot-bytes
  [snapshot]
  (let [buf (byte-array wal-meta-slot-size)
        bb  (ByteBuffer/wrap buf)]
    (b/put-bytes bb wal-meta-magic)
    (b/put-byte bb wal-meta-version)
    (.position bb ^int wal-meta-payload-off)
    (b/put-long bb (get snapshot c/last-committed-wal-tx-id 0))
    (b/put-long bb (get snapshot c/last-committed-user-tx-id 0))
    (b/put-long bb (get snapshot c/committed-last-modified-ms 0))
    (b/put-long bb (get snapshot c/last-indexed-wal-tx-id 0))
    (b/put-long bb (get snapshot c/wal-meta-revision 0))
    (b/put-byte bb wal-format-major)
    (b/put-byte bb wal-format-minor)
    (b/put-byte bb (if (get snapshot :wal/enabled? true) 1 0))
    (b/put-long bb (get snapshot :wal/last-segment-id 0))
    (.position bb ^int wal-meta-slot-size)
    (let [checksum (crc32c buf wal-meta-payload-off
                           (- ^int wal-meta-slot-size
                              ^int wal-meta-payload-off))]
      (aset-byte buf 5 (unchecked-byte (bit-shift-right checksum 24)))
      (aset-byte buf 6 (unchecked-byte (bit-shift-right checksum 16)))
      (aset-byte buf 7 (unchecked-byte (bit-shift-right checksum 8)))
      (aset-byte buf 8 (unchecked-byte checksum)))
    buf))

(defn- read-slot
  [^bytes slot]
  (when (and slot (= (alength slot) wal-meta-slot-size))
    (let [magic (byte-array 4)]
      (System/arraycopy slot 0 magic 0 4)
      (when (Arrays/equals ^bytes magic ^bytes wal-meta-magic)
        (let [version  (aget slot 4)
              c1       (bit-and (int (aget slot 5)) 0xFF)
              c2       (bit-and (int (aget slot 6)) 0xFF)
              c3       (bit-and (int (aget slot 7)) 0xFF)
              c4       (bit-and (int (aget slot 8)) 0xFF)
              checksum (bit-or (bit-shift-left c1 24)
                               (bit-shift-left c2 16)
                               (bit-shift-left c3 8)
                               c4)
              actual   (crc32c slot wal-meta-payload-off
                               (- ^int wal-meta-slot-size
                                  ^int wal-meta-payload-off))]
          (when (and (= (byte wal-meta-version) version)
                     (= checksum actual))
            (let [bb (ByteBuffer/wrap slot)]
              {c/last-committed-wal-tx-id
               (.getLong bb wal-meta-off-committed-wal)
               c/last-committed-user-tx-id
               (.getLong bb wal-meta-off-committed-user)
               c/committed-last-modified-ms
               (.getLong bb wal-meta-off-committed-last-ms)
               c/last-indexed-wal-tx-id
               (.getLong bb wal-meta-off-indexed-wal)
               c/wal-meta-revision
               (.getLong bb wal-meta-off-revision)
               :wal-format-major
               (bit-and (int (.get bb ^int wal-meta-off-format-major)) 0xFF)
               :wal-format-minor
               (bit-and (int (.get bb ^int wal-meta-off-format-minor)) 0xFF)
               :wal/enabled?
               (= 1 (.get bb ^int wal-meta-off-enabled))
               :wal/last-segment-id
               (.getLong bb wal-meta-off-last-segment)})))))))

(defn read-wal-meta
  [dir]
  (let [f (io/file (wal-meta-path dir))]
    (when (.exists f)
      (try
        (with-open [in (DataInputStream. (io/input-stream f))]
          (let [slot-a (read-bytes in 64)
                slot-b (read-bytes in 64)
                a      (read-slot slot-a)
                b      (read-slot slot-b)]
            (cond
              (and a b)
              (let [ra ^long (get a c/wal-meta-revision)
                    rb ^long (get b c/wal-meta-revision)]
                (cond
                  (> ra rb)                                    a
                  (< ra rb)                                    b
                  (> ^long (get a c/last-committed-wal-tx-id)
                     ^long (get b c/last-committed-wal-tx-id)) a
                  :else                                        b))
              a     a
              b     b
              :else nil)))
        (catch java.io.EOFException _
          nil)
        (catch java.io.IOException _
          nil)))))

(defn- write-fully!
  "Write all bytes from buf to ch, looping until the buffer is exhausted.
  FileChannel.write(ByteBuffer) is not guaranteed to write all remaining bytes
  in a single call (e.g. on network-mounted or compressed filesystems)."
  [^FileChannel ch ^ByteBuffer buf]
  (while (.hasRemaining buf)
    (.write ch buf)))

(defn publish-wal-meta!
  ([dir snapshot]
   (publish-wal-meta! dir snapshot c/*wal-sync-mode*))
  ([dir snapshot sync-mode]
   (u/file (wal-dir-path dir))
   (locking (wal-meta-publish-lock dir)
     (let [f          (io/file (wal-meta-path dir))
           existed?   (.exists f)
           existing   (read-wal-meta dir)
           rev        (u/long-inc (or (get existing c/wal-meta-revision) 0))
           merged     (assoc (merge-wal-meta-snapshot existing snapshot)
                             c/wal-meta-revision rev
                             :wal/enabled? true
                             :wal-format-major wal-format-major
                             :wal-format-minor wal-format-minor)
           slot-bytes (meta-slot-bytes merged)
           empty-slot (byte-array 64)
           use-a?     (odd? rev)
           slot-a     (if use-a?
                        slot-bytes
                        (if existing (meta-slot-bytes existing) empty-slot))
           slot-b     (if use-a?
                        (if existing (meta-slot-bytes existing) empty-slot)
                        slot-bytes)
           payload    (byte-array 128)
           metadata?  (not= sync-mode :none)]
       (System/arraycopy slot-a 0 payload 0 64)
       (System/arraycopy slot-b 0 payload 64 64)
       (with-open [ch (FileChannel/open
                        (.toPath f)
                        (into-array StandardOpenOption
                                    [StandardOpenOption/CREATE
                                     StandardOpenOption/WRITE]))]
         ;; Write in-place at fixed offsets instead of truncating first.
         ;; TRUNCATE_EXISTING would destroy both slots before writing,
         ;; so a crash between truncation and write completion could
         ;; lose all meta.  Fixed-offset overwrites preserve the
         ;; untouched slot if the process dies mid-write.
         (let [buf (ByteBuffer/wrap payload)]
           (.position ch 0)
           (write-fully! ch buf))
         (when metadata? (sync-channel! ch sync-mode)))
        (when (and (not existed?) metadata?)
          (try
            (with-open [dch (FileChannel/open
                              (.toPath (io/file (wal-dir-path dir)))
                              (into-array StandardOpenOption
                                          [StandardOpenOption/READ]))]
              ;; Always sync directory on creation using fdatasync.
              ;; This ensures directory entry is persisted on filesystems with
              ;; delayed allocation (e.g., ext4), even when sync mode is :none.
              (sync-channel! dch :fdatasync))
            (catch Exception e
              (raise "Failed to sync WAL directory"
                     {:error :wal/dir-sync-failed
                      :dir (wal-dir-path dir)
                      :exception e}))))
        merged))))

(defn open-segment-channel
  "Open a FileChannel for a WAL segment. Caller is responsible for closing."
  ^FileChannel [dir segment-id]
  (u/file (wal-dir-path dir))
  (open-channel (segment-path dir segment-id)))

(defn close-segment-channel!
  "Close a cached WAL segment channel if open."
  [^FileChannel ch]
  (when (and ch (.isOpen ch))
    (.close ch)))

(defn append-record-bytes!
  "Append a WAL record. When ch is provided, uses it directly (caller manages
  lifecycle). When ch is nil, opens and closes a channel per call.
  If an exception occurs when using a provided channel, the channel is closed
  to prevent further use in an unknown state."
  ([dir segment-id ^bytes record sync-mode]
   (u/file (wal-dir-path dir))
   (with-open [^FileChannel ch (open-channel (segment-path dir segment-id))]
     (write-fully! ch (ByteBuffer/wrap record))
     (sync-channel! ch sync-mode))
   (alength ^bytes record))
  ([^FileChannel ch ^bytes record sync-mode]
   (try
     (write-fully! ch (ByteBuffer/wrap record))
     (sync-channel! ch sync-mode)
     (alength ^bytes record)
     (catch Exception e
       ;; Close the channel to prevent further use in an unknown state
       (close-segment-channel! ch)
       (raise "WAL write failed, channel closed"
              {:error :wal/write-failed
               :channel-open (.isOpen ch)
               :exception e})))))

(defn append-kv-record!
  [dir segment-id wal-id user-tx-id tx-time ops sync-mode]
  (append-record-bytes!
    dir segment-id (record-bytes wal-id user-tx-id tx-time ops) sync-mode))

(defn ensure-wal-channel!
  "Return a cached FileChannel for the given segment, opening a new one if
  needed (or if the segment rotated). Closes the old channel on rotation. "
  [lmdb dir ^long seg-id]
  (let [info            @(l/kv-info lmdb)
        ^FileChannel ch (:wal-channel info)
        ^long ch-seg    (or (:wal-channel-segment-id info) -1)]
    (if (and ch (.isOpen ch) (= ch-seg seg-id))
      ch
      (do (close-segment-channel! ch)
          (let [new-ch (open-segment-channel dir seg-id)]
            (vswap! (l/kv-info lmdb) assoc
                    :wal-channel new-ch
                    :wal-channel-segment-id seg-id)
            new-ch)))))

(defn append-kv-wal-record!
  "Append a WAL record for KV operations.
  Returns map with :wal-id, :tx-time, :ops."
  [lmdb ops tx-time]
  (when (seq ops)
    (let [start-ms (or tx-time (System/currentTimeMillis))
          dir      (i/env-dir lmdb)]
      (locking (l/kv-info lmdb)
        (let [info             @(l/kv-info lmdb)
              max-bytes        (or (:wal-segment-max-bytes info)
                                   c/*wal-segment-max-bytes*)
              max-ms           (or (:wal-segment-max-ms info)
                                   c/*wal-segment-max-ms*)
              sync-mode        (or (:wal-sync-mode info)
                                   c/*wal-sync-mode*)
              group-size       (or (:wal-group-commit info)
                                   c/*wal-group-commit*)
              group-ms         (or (:wal-group-commit-ms info)
                                   c/*wal-group-commit-ms*)
              wal-id           (u/long-inc (or (:wal-next-tx-id info) 0))
              user-tx-id       wal-id
              record           (record-bytes wal-id user-tx-id tx-time ops)
              record-bytes     (alength ^bytes record)
              seg-id           (or (:wal-segment-id info) 1)
              seg-bytes        (or (:wal-segment-bytes info) 0)
              seg-start-ms     (or (:wal-segment-created-ms info) start-ms)
              rotate?          (or (>= (+ ^long seg-bytes record-bytes)
                                       ^long max-bytes)
                                   (>= (- ^long start-ms ^long seg-start-ms)
                                       ^long max-ms))
              new-seg-id       (if rotate? (u/long-inc seg-id) seg-id)
              new-seg-start-ms (if rotate? start-ms seg-start-ms)
              new-seg-bytes    (if rotate? 0 seg-bytes)
              unsynced         (u/long-inc (or (:wal-unsynced-count info) 0))
              last-sync-ms     (or (:wal-last-sync-ms info) 0)
              elapsed-ms       (if (pos? ^long last-sync-ms)
                                 (- (System/currentTimeMillis)
                                    ^long last-sync-ms)
                                 0)
              sync?            (or rotate?
                                   (>= ^long unsynced ^long group-size)
                                   (and (pos? ^long group-ms)
                                        (>= ^long elapsed-ms ^long group-ms)))
              ch
              (if rotate?
                (let [^FileChannel old-ch (:wal-channel info)]
                  (when (and old-ch (.isOpen old-ch))
                    (sync-channel! old-ch sync-mode)
                    (close-segment-channel! old-ch))
                  ;; Open new segment channel
                  (let [new-ch (open-segment-channel dir new-seg-id)]
                    (vswap! (l/kv-info lmdb) assoc
                            :wal-channel new-ch
                            :wal-channel-segment-id new-seg-id)
                    new-ch))
                ;; No rotation - use existing or open current segment
                (ensure-wal-channel! lmdb dir new-seg-id))
              now-ms           (System/currentTimeMillis)]
          (write-fully! ch (ByteBuffer/wrap record))
          ;; Sync if needed. On rotation the old segment was already synced
          ;; before closing, but the record was written to the NEW segment,
          ;; so that channel must be synced here too.
          (when sync?
            (sync-channel! ch sync-mode))
          (vswap! (l/kv-info lmdb)
                  (fn [current-info]
                    (cond->
                        (assoc current-info
                               :wal-next-tx-id wal-id
                               :last-committed-wal-tx-id wal-id
                               :last-committed-user-tx-id wal-id
                               :committed-last-modified-ms now-ms
                               :wal-segment-id new-seg-id
                               :wal-segment-created-ms new-seg-start-ms
                               :wal-segment-bytes
                               (+ ^long new-seg-bytes record-bytes)
                               :wal-unsynced-count (if sync? 0 unsynced)
                               :wal-last-sync-ms (if sync? now-ms last-sync-ms))
                      sync? (assoc :last-synced-wal-tx-id wal-id))))
          {:wal-id  wal-id
           :tx-time now-ms
           :ops     ops})))))

(defn- get-kv-info-id
  "Get a value from the kv-info dbi."
  [lmdb k]
  (binding [c/*bypass-wal* true]
    (i/get-value lmdb c/kv-info k :data :data)))

(defn read-kv-wal-meta
  "Read WAL metadata for the given LMDB instance."
  [lmdb]
  (read-wal-meta (i/env-dir lmdb)))

(defn refresh-kv-wal-info!
  "Refresh WAL info from metadata or by scanning segments.
  Expects lmdb to have .-info volatile and implement env-dir and get-value."
  [lmdb]
  (try
    (let [dir          (i/env-dir lmdb)
          now-ms       (System/currentTimeMillis)
          meta         (read-wal-meta dir)
          files        (or (segment-files dir) [])
          ^File last-f (last files)
          meta-id      (long (or (get meta c/last-committed-wal-tx-id) 0))
          scanned-id   (if last-f
                         (long (segment-max-wal-id last-f))
                         0)
          committed-id ^long (max ^long meta-id ^long scanned-id)
          indexed-id   ^long (or (get-kv-info-id lmdb
                                                 c/last-indexed-wal-tx-id)
                                 (get meta c/last-indexed-wal-tx-id)
                                 0)
          meta-ms      (long (or (get meta c/committed-last-modified-ms) 0))
          scanned-ms   (long (or (when last-f (.lastModified last-f)) 0))
          committed-ms ^long (max ^long meta-ms ^long scanned-ms)
          meta-user    (long (or (get meta c/last-committed-user-tx-id) 0))
          user-id      ^long (if (pos? ^long meta-user) meta-user committed-id)
          next-id      committed-id
          applied-id   ^long (or (get-kv-info-id lmdb c/applied-wal-tx-id)
                                 (get-kv-info-id lmdb c/legacy-applied-tx-id)
                                 0)
          meta-seg     (long (or (get meta :wal/last-segment-id) 0))
          scanned-seg  (long (or (some-> last-f parse-segment-id) 0))
          seg-id       ^long (max ^long meta-seg ^long scanned-seg 1)
          seg-file     (io/file (segment-path dir seg-id))
          seg-bytes    (if (.exists seg-file) (.length seg-file) 0)
          seg-created  ^long (or (when (.exists seg-file)
                                   (.lastModified seg-file))
                                 now-ms)]
      ;; Refresh can run concurrently with active writers (e.g. reader miss path).
      ;; Never regress in-memory WAL counters/state from a partial/stale scan.
      (vswap!
        (l/kv-info lmdb)
        (fn [cur]
          (let [cur-next    (long (or (:wal-next-tx-id cur) 0))
                cur-applied (long (or (:applied-wal-tx-id cur) 0))
                cur-commit  (long (or (:last-committed-wal-tx-id cur) 0))
                cur-synced  (long (or (:last-synced-wal-tx-id cur) 0))
                cur-indexed (long (or (:last-indexed-wal-tx-id cur) 0))
                cur-user    (long (or (:last-committed-user-tx-id cur) 0))
                cur-ms      (long (or (:committed-last-modified-ms cur) 0))
                cur-seg-id  (long (or (:wal-segment-id cur) 1))
                cur-seg-by  (long (or (:wal-segment-bytes cur) 0))
                cur-seg-ms  (long (or (:wal-segment-created-ms cur) 0))

                next*       (max ^long next-id ^long cur-next)
                commit*     (max ^long committed-id ^long cur-commit)
                synced*     (min ^long commit*
                                 ^long (max ^long committed-id ^long cur-synced))
                indexed*    (min ^long commit*
                                 ^long (max ^long indexed-id ^long cur-indexed))
                applied*    (min ^long indexed*
                                 ^long (max ^long applied-id ^long cur-applied))
                user*       (min ^long commit*
                                 ^long (max ^long user-id ^long cur-user))
                commit-ms*  (max ^long committed-ms ^long cur-ms)
                seg-id*     (max ^long seg-id ^long cur-seg-id)
                seg-bytes*  (if (= seg-id* cur-seg-id)
                              (if (= seg-id seg-id*)
                                (max ^long seg-bytes ^long cur-seg-by)
                                cur-seg-by)
                              seg-bytes)
                seg-ms*     (if (= seg-id* cur-seg-id)
                              (if (= seg-id seg-id*)
                                (max ^long seg-created ^long cur-seg-ms)
                                cur-seg-ms)
                              seg-created)]
            (assoc cur
                   :wal-next-tx-id next*
                   :applied-wal-tx-id applied*
                   :last-committed-wal-tx-id commit*
                   :last-synced-wal-tx-id synced*
                   :last-indexed-wal-tx-id indexed*
                   :last-committed-user-tx-id user*
                   :committed-last-modified-ms commit-ms*
                   :wal-segment-id seg-id*
                   :wal-segment-created-ms seg-ms*
                   :wal-segment-bytes seg-bytes*)))))
    (catch java.nio.file.AccessDeniedException e
      (stt/print-stack-trace e)
      (raise "WAL directory permission denied"
             {:error :wal/permission-denied
              :dir   (i/env-dir lmdb)}))
    (catch java.io.IOException e
      (stt/print-stack-trace e)
      ;; Possibly transient - could retry
      (raise "WAL I/O error"
             {:error     :wal/io-error
              :retryable true
              :dir       (i/env-dir lmdb)}))
    (catch Exception e
      (stt/print-stack-trace e)
      ;; Check for corruption indicators
      (let [msg (ex-message e)]
        (if (or (s/includes? msg "checksum")
                (s/includes? msg "magic")
                (s/includes? msg "truncated"))
          (raise "WAL corruption detected"
                 {:error :wal/corruption
                  :fatal true
                  :dir   (i/env-dir lmdb)})
          (raise "WAL read error"
                 {:error :wal/unknown
                  :dir   (i/env-dir lmdb)}))))))

(defn refresh-kv-wal-meta-info!
  "Refresh WAL metadata info (timestamps, revision). "
  [lmdb]
  (let [now-ms (System/currentTimeMillis)]
    (if-let [meta (read-kv-wal-meta lmdb)]
      (let [^long last-ms (or (get meta c/committed-last-modified-ms) 0)]
        (vswap! (l/kv-info lmdb) assoc
                :wal-meta-revision
                (or (get meta c/wal-meta-revision) 0)
                :wal-meta-last-modified-ms last-ms
                :wal-meta-last-flush-ms (if (pos? last-ms) last-ms now-ms)
                :wal-meta-pending-txs 0))
      (vswap! (l/kv-info lmdb) assoc
              :wal-meta-revision 0
              :wal-meta-last-modified-ms 0
              :wal-meta-last-flush-ms now-ms
              :wal-meta-pending-txs 0))))

(defn publish-kv-wal-meta!
  "Publish WAL metadata to persistent storage. "
  [lmdb wal-id now-ms]
  (let [info       @(l/kv-info lmdb)
        ;; Only persist watermarks that have been durably synced to avoid
        ;; claiming records are committed when they may be lost on crash.
        synced-id  (or (:last-synced-wal-tx-id info) 0)
        durable-id (min ^long wal-id ^long synced-id)
        seg-id     (or (:wal-segment-id info) 0)
        indexed    (or (:last-indexed-wal-tx-id info) 0)
        user-id    (or (:last-committed-user-tx-id info) durable-id)
        commit-ms  (or (:committed-last-modified-ms info) now-ms)
        snapshot   {c/last-committed-wal-tx-id   durable-id
                    c/last-indexed-wal-tx-id     indexed
                    c/last-committed-user-tx-id  (min ^long user-id
                                                      ^long durable-id)
                    c/committed-last-modified-ms commit-ms
                    :wal/last-segment-id         seg-id
                    :wal/enabled?                true}
        sync-mode  (or (:wal-sync-mode info) c/*wal-sync-mode*)
        meta'      (publish-wal-meta! (i/env-dir lmdb) snapshot sync-mode)
        rev        (or (:wal-meta-revision meta') 0)]
    (vswap! (l/kv-info lmdb) assoc
            :wal-meta-revision rev
            :wal-meta-last-modified-ms now-ms
            :wal-meta-last-flush-ms now-ms
            :wal-meta-pending-txs 0)
    meta'))

(defn maybe-publish-kv-wal-meta!
  "Conditionally publish WAL metadata based on thresholds. "
  [lmdb wal-id]
  (let [now-ms     (System/currentTimeMillis)
        info'      (vswap! (l/kv-info lmdb) update :wal-meta-pending-txs
                           (fnil u/long-inc 0))
        pending    (or (:wal-meta-pending-txs info') 0)
        flush-txs  (max 1 (long (or (:wal-meta-flush-max-txs info')
                                    c/*wal-meta-flush-max-txs*)))
        flush-ms   (max 0 (long (or (:wal-meta-flush-max-ms info')
                                    c/*wal-meta-flush-max-ms*)))
        last-flush (or (:wal-meta-last-flush-ms info') 0)
        due?       (or (>= ^long pending ^long flush-txs)
                       (>= (- ^long now-ms ^long last-flush) ^long flush-ms))]
    (when due?
      (publish-kv-wal-meta! lmdb wal-id now-ms))))

(defn wal-checkpoint!
  "Flush WAL to base LMDB, persist vector indices, and GC old segments.
  Expects lmdb to support ILMDB protocol operations.
  The vector-indices-atom parameter should be the atom containing the vector
  indices map."
  [lmdb info vector-indices-atom]
  (try
    (i/flush-kv-indexer! lmdb)
    (doseq [idx (keep @vector-indices-atom (u/list-files (i/env-dir lmdb)))]
      (i/persist-vecs idx))
    (i/gc-wal-segments! lmdb)
    (vswap! info assoc :wal-checkpoint-fail-count 0)
    (catch Exception e
      (let [n (long (vswap! info update :wal-checkpoint-fail-count
                            (fnil inc 0)))]
        (when (or (= n 1) (zero? ^long (mod n 10)))
          (binding [*out* *err*]
            (println (str "WARNING: WAL checkpoint failed ("
                          n " consecutive failure(s))"))
            (stt/print-stack-trace e)))))))

(defn start-scheduled-wal-checkpoint
  "Start scheduled background WAL checkpointing.
  The vector-indices-atom parameter should be the atom containing the vector
  indices map."
  [info lmdb vector-indices-atom]
  (let [scheduler ^ScheduledExecutorService (u/get-scheduler)
        fut       (.scheduleWithFixedDelay
                    scheduler
                    ^Runnable #(wal-checkpoint! lmdb info vector-indices-atom)
                    ^long c/wal-checkpoint-interval
                    ^long c/wal-checkpoint-interval
                    TimeUnit/SECONDS)]
    (vswap! info assoc :scheduled-wal-checkpoint fut)))

(defn stop-scheduled-wal-checkpoint
  "Stop scheduled WAL checkpointing."
  [info]
  (when-let [fut ^ScheduledFuture (:scheduled-wal-checkpoint @info)]
    (.cancel fut true)
    (vswap! info dissoc :scheduled-wal-checkpoint)))

(defn maybe-flush-kv-indexer-on-pressure!
  "Flush WAL indexer when memory pressure is detected.
  Expects lmdb to have .-info volatile and support i/flush-kv-indexer!."
  [lmdb]
  (try
    (let [info @(l/kv-info lmdb)]
      (when (:kv-wal? info)
        (require 'datalevin.spill)
        (let [sp-memory-updater  (resolve 'datalevin.spill/memory-updater)
              sp-memory-pressure (resolve 'datalevin.spill/memory-pressure)
              _                  (when sp-memory-updater (@sp-memory-updater))
              pressure-val       (if sp-memory-pressure
                                   (deref @sp-memory-pressure)
                                   0)
              threshold          (or (get-in info [:spill-opts :spill-threshold])
                                     c/default-spill-threshold)
              committed          (or (:last-committed-wal-tx-id info) 0)
              indexed            (or (:last-indexed-wal-tx-id info) 0)]
          (when (and (>= ^long pressure-val ^long threshold)
                     (> ^long committed ^long indexed))
            (i/flush-kv-indexer! lmdb)))))
    (catch Exception e
      (binding [*out* *err*]
        (println "WARNING: WAL memory-pressure flush failed")
        (stt/print-stack-trace e)))))

(defn- wal-op->kv-tx
  [op]
  (let [{:keys [op dbi k v kt vt flags raw?]} op
        flags                                (when (seq flags)
                                               (let [fs (vec (remove #{:dupsort}
                                                                      flags))]
                                                 (when (seq fs) fs)))
        ;; When raw?, keys are already serialized byte arrays;
        ;; replay them with :raw type to avoid decode/re-encode.
        [k kt]                                (if raw? [k :raw] [k kt])
        ;; For put/del, value is a single serialized item → use :raw.
        ;; For put-list/del-list, value is a Nippy-encoded collection
        ;; that must be deserialized so transact-kv can iterate elements.
        [v vt]                                (if raw?
                                                (case op
                                                  (:put-list :del-list) [(b/deserialize v) vt]
                                                  [v :raw])
                                                [v vt])]
    (case op
      :put      (l/kv-tx :put dbi k v kt vt flags)
      :del      (l/kv-tx :del dbi k kt)
      :put-list (l/kv-tx :put-list dbi k v kt vt)
      :del-list (l/kv-tx :del-list dbi k v kt vt)
      (u/raise "Unsupported KV WAL op for replay"
               {:error :wal/invalid-op
                :op    op}))))

(defn apply-wal-kv-ops!
  "Apply canonical KV WAL ops through the trusted internal mutation tier.
   Intended for WAL replay/indexer paths only."
  [db ops]
  (when (seq ops)
    (binding [c/*trusted-apply* true
              c/*bypass-wal*    true]
      (i/transact-kv db (mapv wal-op->kv-tx ops)))))

(defn- dbi-name-from-kv-info-key
  [k]
  (when (and (vector? k)
             (= 2 (count k))
             (= :dbis (first k))
             (string? (second k)))
    (second k)))

(def ^:private byte-array-class (class (byte-array 0)))

(defn- decode-wal-field
  [raw? x t]
  (if (and raw? (instance? byte-array-class x))
    (b/read-buffer (ByteBuffer/wrap ^bytes x) (normalize-type t))
    x))

(defn- wal-recorded-dbi-opts
  [records]
  (reduce
    (fn [m {:wal/keys [ops]}]
      (reduce
        (fn [acc {:keys [op dbi k v kt vt raw?]}]
          (if (= dbi c/kv-info)
            (let [k* (decode-wal-field raw? k kt)]
              (if-let [dbi-name (dbi-name-from-kv-info-key k*)]
                (case op
                  :put (let [v* (decode-wal-field raw? v vt)]
                         (if (map? v*) (assoc acc dbi-name v*) acc))
                  :del (dissoc acc dbi-name)
                  acc)
                acc))
            acc))
        m
        ops))
    {}
    records))

(defn- dbi-open?
  [lmdb dbi-name]
  (try
    (boolean (i/get-dbi lmdb dbi-name false))
    (catch Exception _ false)))

(defn- ensure-replay-dbis-open!
  [lmdb records]
  (let [wal-dbi-opts (wal-recorded-dbi-opts records)]
    (binding [c/*trusted-apply* true
              c/*bypass-wal*    true]
      (doseq [dbi-name (into #{}
                             (comp
                               (mapcat :wal/ops)
                               (keep :dbi)
                               (remove #{c/kv-info}))
                             records)]
        (when-not (dbi-open? lmdb dbi-name)
          (if-let [opts (or (i/dbi-opts lmdb dbi-name)
                            (get wal-dbi-opts dbi-name)
                            (i/get-value lmdb c/kv-info
                                         [:dbis dbi-name]
                                         [:keyword :string]
                                         :data))]
            (i/open-dbi lmdb dbi-name opts)
            (u/raise "WAL replay references unknown DBI"
                     {:error :wal/unknown-dbi
                      :dbi   dbi-name})))))))

(defn- kv-wal-enabled?
  [lmdb]
  (boolean (:kv-wal? (i/env-opts lmdb))))

(defn- kv-info-id
  [lmdb k]
  (binding [c/*bypass-wal* true]
    (i/get-value lmdb c/kv-info k :data :data)))

(defn- kv-applied-id
  [lmdb]
  (or (kv-info-id lmdb c/applied-wal-tx-id)
      (kv-info-id lmdb c/legacy-applied-tx-id)))

(defn kv-wal-watermarks
  "Return KV WAL watermark values as a map with ID values:
   `:last-committed-wal-tx-id`, `:last-indexed-wal-tx-id`,
   and `:last-committed-user-tx-id`."
  [lmdb]
  (let [info (i/env-opts lmdb)]
    (if-not (:kv-wal? info)
      {:last-committed-wal-tx-id  0
       :last-indexed-wal-tx-id    0
       :last-committed-user-tx-id 0}
      {:last-committed-wal-tx-id
       (long (or (:last-committed-wal-tx-id info) 0))
       :last-indexed-wal-tx-id
       (long (or (kv-info-id lmdb c/last-indexed-wal-tx-id)
                 (:last-indexed-wal-tx-id info)
                 0))
       :last-committed-user-tx-id
       (long (or (:last-committed-user-tx-id info)
                 (:last-committed-wal-tx-id info)
                 0))})))

(defn replay-kv-wal!
  "Replay committed KV WAL records into base KV DBIs and advance
   `:wal/last-indexed-wal-tx-id`.

   This is an internal/indexer helper. Replayed writes are executed inside an
   explicit transaction context so they do not append new WAL records."
  ([lmdb]
   (replay-kv-wal! lmdb nil))
  ([lmdb upto-wal-id]
   (if-not (kv-wal-enabled? lmdb)
     {:from 0 :to 0 :applied 0}
     (let [info         (i/env-opts lmdb)
           from-id      (long (or (kv-info-id lmdb c/last-indexed-wal-tx-id)
                                  (kv-applied-id lmdb)
                                  0))
           committed-id (long (or (:last-committed-wal-tx-id info) 0))
           target-id    (let [upto-id (if (some? upto-wal-id)
                                        (long upto-wal-id)
                                        committed-id)]
                          (long (min committed-id upto-id)))]
       (if (<= target-id from-id)
         {:from from-id :to target-id :applied 0}
         (let [initial-applied-wal (kv-info-id lmdb c/applied-wal-tx-id)
               initial-applied     (or initial-applied-wal
                                       (kv-info-id lmdb c/legacy-applied-tx-id)
                                       0)
               start-id            (max ^long from-id ^long initial-applied)
               records             (read-wal-records (i/env-dir lmdb)
                                                     start-id target-id)
               applied-count       (volatile! 0)
               applied-id          (volatile! start-id)]
           (when (and (> target-id start-id) (nil? (seq records)))
             (u/raise "Missing KV WAL records during replay"
                      {:error     :wal/missing-record
                       :wal-tx-id target-id}))
           (ensure-replay-dbis-open! lmdb records)
           (binding [c/*trusted-apply* true
                     c/*bypass-wal*    true]
             (l/with-transaction-kv [db lmdb]
               (doseq [rec records]
                 (let [wal-id       (long (:wal/tx-id rec))
                       ops          (:wal/ops rec)
                       expected-wal (unchecked-inc (long @applied-id))]
                   (when (not= wal-id expected-wal)
                     (u/raise "KV WAL replay out of order"
                              {:error       :wal/out-of-order
                               :applied-wal @applied-id
                               :expected-wal-tx-id expected-wal
                               :wal-tx-id   wal-id
                               :start-id    start-id
                               :target-id   target-id
                               :applied-cnt @applied-count}))
                   (apply-wal-kv-ops! db ops)
                   (vreset! applied-id wal-id)
                   (vswap! applied-count u/long-inc)))
               (i/transact-kv
                 db c/kv-info
                 (cond-> [[:put c/last-indexed-wal-tx-id @applied-id]]
                   (or (nil? initial-applied-wal)
                       (> (long @applied-id) ^long initial-applied))
                   (conj [:put c/applied-wal-tx-id @applied-id])))))
           {:from from-id :to @applied-id :applied @applied-count}))))))

(defn flush-kv-indexer!
  "Catch up KV indexer watermark to committed WAL via replay and return:
   `{:indexed-wal-tx-id <id> :committed-wal-tx-id <id> :drained? <bool>}`.

   When `upto-wal-id` is provided, replay is bounded by that wal id, while
   `:drained?` still reflects indexed vs committed equality at return time."
  ([lmdb]
   (flush-kv-indexer! lmdb nil))
  ([lmdb upto-wal-id]
   (let [start-ms  (System/currentTimeMillis)
         res       (replay-kv-wal! lmdb upto-wal-id)
         end-ms    (System/currentTimeMillis)
         duration  (- end-ms start-ms)
         info      (i/env-opts lmdb)
         committed (long (or (:last-committed-wal-tx-id info) 0))
         indexed   (long (or (:to res) 0))]
     (vswap! (l/kv-info lmdb) assoc
             :wal-indexer-last-flush-duration-ms duration
             :wal-indexer-last-flush-ms end-ms)
     {:indexed-wal-tx-id   indexed
      :committed-wal-tx-id committed
      :drained?            (= indexed committed)})))

(defn open-tx-log
  "Return a seq of WAL transaction records starting after `from-wal-id`
   (exclusive). Each record is a map:
     {:wal/tx-id <long>, :wal/ops [{:op :put|:del|:put-list|:del-list
                                     :dbi <string> :k <bytes> :v <bytes>
                                     :kt <keyword> :vt <keyword>} ...]}
   Returns empty seq if WAL is not enabled or no records exist after from-wal-id.
   Without `upto-wal-id`, reads up to last committed."
  ([lmdb from-wal-id]
   (open-tx-log lmdb from-wal-id nil))
  ([lmdb from-wal-id upto-wal-id]
   (if-not (kv-wal-enabled? lmdb)
     ()
     (let [committed (long (:last-committed-wal-tx-id
                            (kv-wal-watermarks lmdb)))
           upto      (if (some? upto-wal-id)
                       (min (long upto-wal-id) committed)
                       committed)]
       (read-wal-records (i/env-dir lmdb) from-wal-id upto)))))

(defn- overlay-entry-count
  [info]
  (if-let [overlay-env (:kv-overlay-env info)]
    (try
      (reduce
       (fn [^long acc dbi-name]
         (+ acc (long (i/entries overlay-env dbi-name))))
       0
       (i/list-dbis overlay-env))
      (catch Exception _
        0))
    0))

(defn kv-wal-metrics
  "Report WAL overlay/indexer/segment diagnostics."
  [lmdb]
  (let [info            (i/env-opts lmdb)
        committed       (long (or (:last-committed-wal-tx-id info) 0))
        indexed         (long (or (:last-indexed-wal-tx-id info) 0))
        lag             (max 0 (- committed indexed))
        committed-count (overlay-entry-count info)
        private-count   (long (or (:kv-overlay-private-entries info) 0))
        dir             (i/env-dir lmdb)
        segments        (or (segment-files dir) [])
        segment-count   (count segments)
        segment-bytes   (reduce
                          (fn [^long acc ^File f] (+ acc (.length f)))
                          0
                          segments)
        eligible-wm     indexed
        [eligible-count eligible-bytes]
        (reduce
          (fn [[cnt bytes] ^File f]
            (let [max-id (segment-max-wal-id f)]
              (if (<= ^long max-id ^long eligible-wm)
                [(u/long-inc cnt) (+ ^int bytes (.length f))]
                [cnt bytes])))
          [0 0] segments)
        last-flush-ms   (long (or (:wal-indexer-last-flush-ms info) 0))
        last-duration   (long (or (:wal-indexer-last-flush-duration-ms info) 0))
        since-last      (if (pos? last-flush-ms)
                          (max 0 (- (System/currentTimeMillis) last-flush-ms))
                          0)]
    {:overlay-entries                committed-count
     :private-overlay-entries        private-count
     :indexer-lag                    lag
     :segment-count                  segment-count
     :segment-bytes                  segment-bytes
     :gc-eligible-segment-count      eligible-count
     :gc-eligible-segment-bytes      eligible-bytes
     :indexer-last-flush-duration-ms last-duration
     :indexer-ms-since-last-flush    since-last}))

(defn gc-wal-segments!
  "Delete WAL segments that are fully below the GC watermark and exceed
   the retention policy.  A segment is eligible for deletion only when
   every record in it has wal-id <= min(last-indexed-wal-tx-id, retain-wal-id).

   Among eligible segments, one is actually deleted when *either*:
     - the total WAL size exceeds `:wal-retention-bytes` (default 1 GB), or
     - the segment is older than `:wal-retention-ms` (default 7 days).

   The active (newest) segment is never deleted.

   `retain-wal-id` is the minimum wal-id that must be retained (e.g. the
   lowest wal-id a replica still needs). If omitted, uses last-indexed-wal-tx-id.

   Returns {:deleted <count> :retained <count>}."
  ([lmdb]
   (gc-wal-segments! lmdb nil))
  ([lmdb retain-wal-id]
   (if-not (kv-wal-enabled? lmdb)
     {:deleted 0 :retained 0}
     (let [wm         (kv-wal-watermarks lmdb)
           indexed-id (long (:last-indexed-wal-tx-id wm))
           gc-wm      (if (some? retain-wal-id)
                        (min indexed-id (long retain-wal-id))
                        indexed-id)
           files      (segment-files (i/env-dir lmdb))]
       (if (or (nil? files) (empty? files))
         {:deleted 0 :retained 0}
         (let [opts          (i/env-opts lmdb)
               max-bytes     (long (or (:wal-retention-bytes opts)
                                       c/*wal-retention-bytes*))
               max-age-ms    (long (or (:wal-retention-ms opts)
                                       c/*wal-retention-ms*))
               now-ms        (System/currentTimeMillis)
               active-file   (last files)
               ;; Compute total WAL size (all files, newest-first)
               total-bytes   (long (reduce (fn [^long acc ^java.io.File f]
                                             (+ acc (.length f)))
                                           0 files))
               ;; Walk oldest-first, accumulate bytes to delete
               deleted       (volatile! (long 0))
               retained      (volatile! (long 0))
               running-bytes (volatile! total-bytes)]
           (doseq [^java.io.File f files]
             (if (identical? f active-file)
               (vswap! retained u/long-inc)
               (let [max-id (long (segment-max-wal-id f))]
                 (if (<= max-id gc-wm)
                   ;; Eligible for GC — check retention policy
                   (let [fsize      (.length f)
                         age-ms     (- now-ms (.lastModified f))
                         over-size? (> ^long @running-bytes max-bytes)
                         over-age?  (> age-ms max-age-ms)]
                     (if (or over-size? over-age?)
                       (do (evict-segment-max-id-cache! f)
                           (if (.delete f)
                             (do (vswap! running-bytes
                                         (fn [^long v] (- v fsize)))
                                 (vswap! deleted u/long-inc))
                             (vswap! retained u/long-inc)))
                       (vswap! retained u/long-inc)))
                   (vswap! retained u/long-inc)))))
           {:deleted @deleted :retained @retained}))))))
