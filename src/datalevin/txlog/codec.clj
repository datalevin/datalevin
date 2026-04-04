;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.txlog.codec
  "Txn-log binary codec helpers."
  (:require
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.lmdb]
   [datalevin.util :as u :refer [raise map+]])
  (:import
   [java.nio ByteBuffer ByteOrder BufferOverflowException]
   [java.nio.charset StandardCharsets]
   [java.util Arrays HashMap List Collection]
   [java.util.zip CRC32C]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:const record-header-size 14)
(def ^:const format-major 1)
(def ^:const compressed-flag 0x01)
(def ^:private magic-bytes
  (byte-array [(byte 0x44) (byte 0x4c) (byte 0x57) (byte 0x4c)]))

(def ^:const meta-slot-payload-size 64)
(def ^:const meta-slot-size (+ meta-slot-payload-size 4))
(def ^:const meta-format-major 1)
(def ^:private meta-magic-bytes
  (byte-array [(byte 0x44) (byte 0x4c) (byte 0x54) (byte 0x4d)])) ; DLTM

(def ^:const commit-marker-slot-payload-size 60)
(def ^:const commit-marker-slot-size (+ commit-marker-slot-payload-size 4))
(def ^:const commit-marker-format-major 1)
(def ^:private commit-marker-magic-bytes
  (byte-array [(byte 0x44) (byte 0x4c) (byte 0x43) (byte 0x4d)])) ; DLCM
(def ^:private commit-payload-ha-term-flag 0x01)

(def ^:const op-kv-put (byte 0x10))
(def ^:const op-kv-del (byte 0x11))
(def ^:const op-kv-put-list (byte 0x16))
(def ^:const op-kv-del-list (byte 0x17))

(def ^:const txlog-type-tuple-tag (byte 0x00))

(def ^:private commit-payload-format-major 1)
(def ^:private commit-payload-magic-bytes
  (byte-array [(byte 0x44) (byte 0x4c) (byte 0x54) (byte 0x58)])) ; DLTX
(def ^:private commit-payload-lsn-offset 8)
(def ^:private commit-payload-tx-time-offset 16)

(def ^:private txlog-flag-keywords
  [:nooverwrite :nodupdata :current :reserve :append :appenddup :multiple
   :dupsort])

(def ^:private txlog-flag->id
  (zipmap txlog-flag-keywords
          (mapv byte (range 1 (inc (count txlog-flag-keywords))))))

(def ^:private txlog-id->flag
  (into {} (map (fn [[k v]] [(int (bit-and (int v) 0xFF)) k])) txlog-flag->id))

(def ^:private txlog-type-keywords
  [:string :bigint :bigdec :long :float :double :bytes :keyword :symbol
   :boolean :instant :uuid :instant-pre-06 :byte :short :int :id :int-int
   :ints :bitmap :term-info :doc-info :pos-info :attr :avg :raw :datom :data])

(def ^:private txlog-type->id
  (zipmap txlog-type-keywords
          (mapv byte (range 1 (inc (count txlog-type-keywords))))))

(def ^:private txlog-id->type
  (into {} (map (fn [[k v]] [(int (bit-and (int v) 0xFF)) k])) txlog-type->id))

(def ^:private tl-commit-body-buffer-initial-cap 8192)
(def ^:private tl-bits-buffer-initial-cap 4096)
(def ^:private tl-row-encode-buffer-initial-cap 4096)
(def ^:private tl-buffer-max-retained-cap (* 1024 1024))
(def ^:private tl-buffer-shrink-trigger-cap tl-buffer-max-retained-cap)
(def ^:private parallel-row-encode-min-rows 1024)
(def ^:private parallel-row-encode-target-tasks-per-core 2)
(def ^:private parallel-row-encode-min-chunk-size 64)

(def ^:private ^ThreadLocal tl-crc32c
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_] (CRC32C.)))))

(def ^ThreadLocal tl-commit-body-buffer
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_] (ByteBuffer/allocate tl-commit-body-buffer-initial-cap)))))

(def ^ThreadLocal tl-bits-buffer
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_] (ByteBuffer/allocate tl-bits-buffer-initial-cap)))))

(def ^ThreadLocal tl-row-encode-buffer
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_] (ByteBuffer/allocate tl-row-encode-buffer-initial-cap)))))

(def ^:private ^ThreadLocal tl-dbi-name-cache
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_] (HashMap. 8)))))

(defn crc32c
  [^bytes bs]
  (let [^CRC32C crc (.get tl-crc32c)]
    (.reset crc)
    (.update crc bs 0 (alength bs))
    (unchecked-int (.getValue crc))))

(defn put-u32
  [^ByteBuffer bf ^long x]
  (.putInt bf (unchecked-int (bit-and x 0xffffffff))))

(defn big-endian-buffer!
  ^ByteBuffer [^ByteBuffer bf]
  (.order bf ByteOrder/BIG_ENDIAN))

(defn checked-record-body-len
  [^bytes body]
  (let [body-len (alength body)]
    (when (neg? body-len)
      (raise "Invalid record body length" {:body-len body-len}))
    body-len))

(defn record-body-checksum
  [body]
  (bit-and 0xffffffff ^int (crc32c body)))

(defn write-record-header!
  [^ByteBuffer header-bf body-len compressed? checksum]
  (.clear header-bf)
  (.put header-bf ^bytes magic-bytes)
  (.put header-bf (byte format-major))
  (.put header-bf (byte (if compressed? compressed-flag 0x00)))
  (put-u32 header-bf body-len)
  (put-u32 header-bf checksum)
  (.flip header-bf)
  header-bf)

(defn encode-record
  ([body] (encode-record body {}))
  ([^bytes body {:keys [compressed?]
                 :or   {compressed? false}}]
   (let [body-len  (checked-record-body-len body)
         checksum  (record-body-checksum body)
         total-len (+ record-header-size (long body-len))
         record    (byte-array (int total-len))
         header-bf (big-endian-buffer!
                    (ByteBuffer/wrap record 0 record-header-size))]
     (write-record-header! header-bf body-len (boolean compressed?) checksum)
     (when (pos? (long body-len))
       (System/arraycopy body 0 record record-header-size (int body-len)))
     record)))

(defn read-int-unsigned [^ByteBuffer bf] (bit-and 0xffffffff (.getInt bf)))

(defn decode-header-buffer
  [^ByteBuffer header offset]
  (when-not (= (.remaining header) record-header-size)
    (raise "Invalid txn-log record header size"
           {:offset offset :size (.remaining header)}))
  (let [m0 (bit-and 0xff (int (.get header)))
        m1 (bit-and 0xff (int (.get header)))
        m2 (bit-and 0xff (int (.get header)))
        m3 (bit-and 0xff (int (.get header)))
        g0 (bit-and 0xff (int (aget ^bytes magic-bytes 0)))
        g1 (bit-and 0xff (int (aget ^bytes magic-bytes 1)))
        g2 (bit-and 0xff (int (aget ^bytes magic-bytes 2)))
        g3 (bit-and 0xff (int (aget ^bytes magic-bytes 3)))]
    (when-not (and (= m0 g0) (= m1 g1) (= m2 g2) (= m3 g3))
      (raise "Invalid txn-log record magic"
             {:offset offset :magic [m0 m1 m2 m3]}))
    (let [major    (int (.get header))
          flags    (bit-and 0xff (int (.get header)))
          body-len (read-int-unsigned header)
          checksum (read-int-unsigned header)]
      (when-not (= major format-major)
        (raise "Unsupported txn-log record format major"
               {:offset offset :major major :expected format-major}))
      {:major    major
       :flags    flags
       :body-len body-len
       :checksum checksum})))

(defn decode-header-bytes
  [^bytes header-bytes offset]
  (let [header (big-endian-buffer! (ByteBuffer/wrap header-bytes))]
    (decode-header-buffer header offset)))

(defn decode-record-bytes
  "for test only"
  ([^bytes data] (decode-record-bytes data 0))
  ([^bytes data ^long offset]
   (let [total (alength data)]
     (when (> (+ offset record-header-size) total)
       (raise "Not enough bytes to decode record header"
              {:offset offset :size total}))
     (let [header (Arrays/copyOfRange data (int offset)
                                      (int (+ offset record-header-size)))
           {:keys [major flags body-len checksum]}
           (decode-header-bytes header offset)
           end    (+ offset ^long record-header-size ^long body-len)]
       (when (> ^long end total)
         (raise "Not enough bytes to decode txn-log record body"
                {:offset offset :body-len body-len :size total}))
       (let [body (Arrays/copyOfRange data
                                      (int (+ offset record-header-size))
                                      (int end))]
         (when-not (= ^int checksum (bit-and 0xffffffff ^int (crc32c body)))
           (raise "Txn-log record checksum mismatch"
                  {:offset offset :expected checksum}))
         {:offset      offset
          :next-offset end
          :major       major
          :flags       flags
          :compressed? (pos? (long (bit-and (long flags) compressed-flag)))
          :body-len    body-len
          :checksum    checksum
          :body        body})))))

(defn encode-meta-slot
  [{:keys [revision
           last-committed-lsn
           last-durable-lsn
           last-applied-lsn
           segment-id
           segment-offset
           updated-ms]
    :or {revision 0
         last-committed-lsn 0
         last-durable-lsn 0
         last-applied-lsn 0
         segment-id 0
         segment-offset 0
         updated-ms 0}}]
  (let [payload (big-endian-buffer! (ByteBuffer/allocate meta-slot-payload-size))]
    (.put payload ^bytes meta-magic-bytes)
    (.put payload (byte meta-format-major))
    (.put payload (byte 0x00))
    (.putShort payload (short 0))
    (.putLong payload (long revision))
    (.putLong payload (long last-committed-lsn))
    (.putLong payload (long last-durable-lsn))
    (.putLong payload (long last-applied-lsn))
    (.putLong payload (long segment-id))
    (.putLong payload (long segment-offset))
    (.putLong payload (long updated-ms))
    (let [^bytes payload-bytes (.array payload)
          slot (big-endian-buffer! (ByteBuffer/allocate meta-slot-size))]
      (.put slot payload-bytes)
      (put-u32 slot (crc32c payload-bytes))
      (.array slot))))

(defn decode-meta-slot-bytes
  [^bytes slot-bytes]
  (when-not (= (alength slot-bytes) meta-slot-size)
    (raise "Invalid txn-log meta slot size"
           {:size (alength slot-bytes) :expected meta-slot-size}))
  (let [payload (Arrays/copyOfRange slot-bytes 0 meta-slot-payload-size)
        checksum-buf (big-endian-buffer!
                      (ByteBuffer/wrap slot-bytes meta-slot-payload-size 4))
        expected-check (long (read-int-unsigned checksum-buf))
        actual-check (long (bit-and 0xffffffff (long (crc32c payload))))]
    (when-not (= expected-check actual-check)
      (raise "Txn-log meta slot checksum mismatch"
             {:expected expected-check :actual actual-check}))
    (let [bf (big-endian-buffer! (ByteBuffer/wrap payload))
          magic (byte-array 4)]
      (.get bf magic)
      (when-not (Arrays/equals magic ^bytes meta-magic-bytes)
        (raise "Invalid txn-log meta slot magic" {:magic (vec magic)}))
      (let [major (bit-and 0xff (int (.get bf)))
            _flags (bit-and 0xff (int (.get bf)))
            _reserved (.getShort bf)
            revision (.getLong bf)
            last-committed-lsn (.getLong bf)
            last-durable-lsn (.getLong bf)
            last-applied-lsn (.getLong bf)
            segment-id (.getLong bf)
            segment-offset (.getLong bf)
            updated-ms (.getLong bf)]
        (when-not (= major meta-format-major)
          (raise "Unsupported txn-log meta slot format major"
                 {:major major :expected meta-format-major}))
        {:revision revision
         :last-committed-lsn last-committed-lsn
         :last-durable-lsn last-durable-lsn
         :last-applied-lsn last-applied-lsn
         :segment-id segment-id
         :segment-offset segment-offset
         :updated-ms updated-ms
         :checksum expected-check}))))

(defn encode-commit-marker-slot
  [{:keys [revision
           applied-lsn
           txlog-segment-id
           txlog-record-offset
           txlog-record-crc
           updated-ms]
    :or {revision 0
         applied-lsn 0
         txlog-segment-id 0
         txlog-record-offset 0
         txlog-record-crc 0
         updated-ms 0}}]
  (let [payload (big-endian-buffer!
                 (ByteBuffer/allocate commit-marker-slot-payload-size))]
    (.put payload ^bytes commit-marker-magic-bytes)
    (.put payload (byte commit-marker-format-major))
    (.put payload (byte 0x00))
    (.putShort payload (short 0))
    (.putLong payload (long revision))
    (.putLong payload (long applied-lsn))
    (.putLong payload (long txlog-segment-id))
    (.putLong payload (long txlog-record-offset))
    (put-u32 payload (long txlog-record-crc))
    (.putLong payload (long updated-ms))
    (let [^bytes payload-bytes (.array payload)
          slot (big-endian-buffer! (ByteBuffer/allocate commit-marker-slot-size))]
      (.put slot payload-bytes)
      (put-u32 slot (crc32c payload-bytes))
      (.array slot))))

(defn decode-commit-marker-slot-bytes
  [^bytes slot-bytes]
  (when-not (= (alength slot-bytes) commit-marker-slot-size)
    (raise "Invalid txn-log commit marker slot size"
           {:size (alength slot-bytes)
            :expected commit-marker-slot-size}))
  (let [payload (Arrays/copyOfRange slot-bytes 0 commit-marker-slot-payload-size)
        checksum-buf (big-endian-buffer!
                      (ByteBuffer/wrap slot-bytes
                                       commit-marker-slot-payload-size
                                       4))
        expected-check (long (read-int-unsigned checksum-buf))
        actual-check (long (bit-and 0xffffffff (long (crc32c payload))))]
    (when-not (= expected-check actual-check)
      (raise "Txn-log commit marker slot checksum mismatch"
             {:expected expected-check :actual actual-check}))
    (let [bf (big-endian-buffer! (ByteBuffer/wrap payload))
          magic (byte-array 4)]
      (.get bf magic)
      (when-not (Arrays/equals magic ^bytes commit-marker-magic-bytes)
        (raise "Invalid txn-log commit marker slot magic" {:magic (vec magic)}))
      (let [major (bit-and 0xff (int (.get bf)))
            _flags (bit-and 0xff (int (.get bf)))
            _reserved (.getShort bf)
            revision (.getLong bf)
            applied-lsn (.getLong bf)
            txlog-segment-id (.getLong bf)
            txlog-record-offset (.getLong bf)
            txlog-record-crc (long (read-int-unsigned bf))
            updated-ms (.getLong bf)]
        (when-not (= major commit-marker-format-major)
          (raise "Unsupported txn-log commit marker format major"
                 {:major major :expected commit-marker-format-major}))
        {:revision revision
         :applied-lsn applied-lsn
         :txlog-segment-id txlog-segment-id
         :txlog-record-offset txlog-record-offset
         :txlog-record-crc txlog-record-crc
         :updated-ms updated-ms
         :checksum expected-check}))))

(defn ensure-fast-list
  ^FastList [x]
  (cond
    (instance? FastList x) x
    (instance? Collection x) (FastList. ^Collection x)
    (nil? x) (FastList.)
    :else (let [^FastList out (FastList.)]
            (doseq [v x] (.add out v))
            out)))

(defn vector-checkpoint-op?
  "Return true if canonical txn-log op targets vector checkpoint DBIs."
  [op]
  (let [dbi-name (when (and (vector? op) (<= 2 (count op)))
                   (nth op 1 nil))]
    (and (string? dbi-name)
         (or (= dbi-name c/vec-index-dbi)
             (= dbi-name c/vec-meta-dbi)))))

(defn classify-record-kind
  "Derive record kind from canonical ops.
   v1 stores no explicit tx-kind in the payload."
  [ops]
  (cond
    (not (sequential? ops)) :unknown
    (empty? ops) :unknown
    (every? vector-checkpoint-op? ops) :vector-checkpoint
    :else :user))

(defn- ensure-room!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^long needed]
  (if (>= (.remaining bf) needed)
    bf
    (let [required (+ ^long (long (.position bf)) ^long needed)
          new-cap (loop [cap (long (max 1 (.capacity bf)))]
                    (if (>= cap required)
                      cap
                      (recur (* 2 cap))))
          ^ByteBuffer grown (ByteBuffer/allocate (int new-cap))]
      (.flip bf)
      (.put grown bf)
      (.set tl grown)
      grown)))

(defn- next-capacity-at-least
  ^long [^long base-cap ^long needed]
  (loop [cap (long (max 1 base-cap))]
    (if (>= cap needed)
      cap
      (recur (* 2 cap)))))

(defn- maybe-shrink-threadlocal-buffer!
  [^ThreadLocal tl ^ByteBuffer bf ^long used-bytes ^long initial-cap]
  (let [cap (long (.capacity bf))
        used (long (max 0 (long used-bytes)))]
    (when (and (>= cap (long tl-buffer-shrink-trigger-cap))
               (<= used (long tl-buffer-max-retained-cap)))
      (let [new-cap (next-capacity-at-least initial-cap (long (max 1 used)))]
        (when (< new-cap cap)
          (.set tl (ByteBuffer/allocate (int new-cap))))))))

(defn- maybe-shrink-bits-buffer!
  []
  (let [^ByteBuffer bf (.get tl-bits-buffer)]
    (maybe-shrink-threadlocal-buffer!
     tl-bits-buffer
     bf
     (.remaining bf)
     tl-bits-buffer-initial-cap)))

(defn- ensure-readable!
  [^ByteBuffer bf ^long needed context]
  (when (< (.remaining bf) needed)
    (raise "Truncated txn-log payload"
           (merge {:type :txlog/corrupt
                   :needed needed
                   :remaining (.remaining bf)}
                  context))))

(defn- bb-put-byte!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl v]
  (let [^ByteBuffer bf (ensure-room! bf tl 1)]
    (.put bf (byte v))
    bf))

(defn- bb-put-u16!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^long v]
  (when (or (neg? v) (> v 0xffff))
    (raise "Txn-log u16 field out of range"
           {:value v :type :txlog/corrupt}))
  (let [^ByteBuffer bf (ensure-room! bf tl Short/BYTES)]
    (.putShort bf (short v))
    bf))

(defn- bb-put-u32!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^long v]
  (when (or (neg? v) (> v 0xffffffff))
    (raise "Txn-log u32 field out of range"
           {:value v :type :txlog/corrupt}))
  (let [^ByteBuffer bf (ensure-room! bf tl Integer/BYTES)]
    (.putInt bf (unchecked-int v))
    bf))

(defn- bb-put-long!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^long v]
  (let [^ByteBuffer bf (ensure-room! bf tl Long/BYTES)]
    (.putLong bf v)
    bf))

(defn- bb-put-bytes!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^bytes bs]
  (let [len (alength bs)
        ^ByteBuffer bf (ensure-room! bf tl len)]
    (.put bf bs 0 len)
    bf))

(defn- bb-put-buffer!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^ByteBuffer src]
  (let [len (.remaining src)
        ^ByteBuffer bf (ensure-room! bf tl len)]
    (.put bf (.array src) (.position src) len)
    bf))

(defn- bb-get-u8
  ([^ByteBuffer bf] (bb-get-u8 bf nil))
  ([^ByteBuffer bf context]
   (ensure-readable! bf 1 context)
   (bit-and 0xFF (int (.get bf)))))

(defn- bb-get-u16
  ([^ByteBuffer bf] (bb-get-u16 bf nil))
  ([^ByteBuffer bf context]
   (ensure-readable! bf Short/BYTES context)
   (bit-and 0xFFFF (int (.getShort bf)))))

(defn- bb-get-u32
  ([^ByteBuffer bf] (bb-get-u32 bf nil))
  ([^ByteBuffer bf context]
   (ensure-readable! bf Integer/BYTES context)
   (bit-and 0xFFFFFFFF (.getInt bf))))

(defn- bb-get-long
  ([^ByteBuffer bf] (bb-get-long bf nil))
  ([^ByteBuffer bf context]
   (ensure-readable! bf Long/BYTES context)
   (.getLong bf)))

(defn- bb-get-bytes
  [^ByteBuffer bf n context]
  (let [len (long n)]
    (when (or (neg? len) (> len Integer/MAX_VALUE))
      (raise "Invalid txn-log byte length"
             (merge {:type :txlog/corrupt
                     :length len}
                    context)))
    (ensure-readable! bf len context)
    (let [bs (byte-array (int len))]
      (.get bf bs)
      bs)))

(defn- type->byte
  [t]
  (let [t (or t :data)]
    (or (txlog-type->id t)
        (raise "Unsupported txn-log value type"
               {:type :txlog/corrupt
                :value-type t}))))

(defn- byte->type
  [b]
  (or (txlog-id->type (int (bit-and (int b) 0xFF)))
      (raise "Unknown txn-log value type tag"
             {:type :txlog/corrupt
              :tag b})))

(defn- bb-write-type!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl t]
  (if (vector? t)
    (let [cnt (count t)]
      (when (zero? cnt)
        (raise "Tuple type must not be empty"
               {:type :txlog/corrupt}))
      (let [bf (-> bf
                   (bb-put-byte! tl txlog-type-tuple-tag)
                   (bb-put-u16! tl cnt))]
        (reduce
         (fn [^ByteBuffer bf et]
           (when (vector? et)
             (raise "Nested tuple types are not supported in txn-log"
                    {:type :txlog/corrupt
                     :value-type t}))
           (bb-put-byte! bf tl (int (type->byte et))))
         bf
         t)))
    (bb-put-byte! bf tl (int (type->byte t)))))

(defn- bb-read-type
  [^ByteBuffer bf]
  (let [tag (bb-get-u8 bf {:field :type-tag})]
    (if (zero? ^int tag)
      (let [cnt (bb-get-u16 bf {:field :type-count})]
        (when (zero? ^int cnt)
          (raise "Tuple type must not be empty"
                 {:type :txlog/corrupt}))
        (vec (repeatedly cnt #(byte->type
                               (byte (bb-get-u8 bf {:field :type-elem}))))))
      (byte->type (byte tag)))))

(defn- normalize-flags
  [flags]
  (cond
    (nil? flags) []
    (set? flags) (->> flags (sort-by name) vec)
    (sequential? flags) (vec flags)
    :else (raise "Invalid txn-log flags"
                 {:type :txlog/corrupt
                  :flags flags})))

(defn- flag->byte
  [f]
  (or (txlog-flag->id f)
      (raise "Unsupported txn-log flag"
             {:type :txlog/corrupt
              :flag f})))

(defn- byte->flag
  [b]
  (or (txlog-id->flag (int (bit-and (int b) 0xFF)))
      (raise "Unknown txn-log flag tag"
             {:type :txlog/corrupt
              :tag b})))

(defn- bb-write-flags!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl flags]
  (let [fs (normalize-flags flags)
        cnt (count fs)]
    (when (> cnt 255)
      (raise "Too many txn-log flags"
             {:type :txlog/corrupt
              :count cnt}))
    (let [bf (bb-put-byte! bf tl cnt)]
      (reduce (fn [^ByteBuffer bf f]
                (bb-put-byte! bf tl (int (flag->byte f))))
              bf
              fs))))

(defn- bb-read-flags
  [^ByteBuffer bf]
  (let [cnt (bb-get-u8 bf {:field :flags-count})]
    (when (pos? ^int cnt)
      (vec (repeatedly cnt #(byte->flag
                             (byte (bb-get-u8 bf {:field :flag-tag}))))))))

(defn- encode-to-buf
  "Encode value `x` with type `t` into the thread-local ByteBuffer and return
  it flipped for reading."
  ^ByteBuffer [x t]
  (let [t (or t :data)]
    (loop [^ByteBuffer bf (.get tl-bits-buffer)]
      (let [result (try
                     (b/put-bf bf x t)
                     bf
                     (catch Exception e
                       (if (instance? BufferOverflowException e)
                         ::overflow
                         (throw e))))]
        (if (= result ::overflow)
          (let [new-bf (ByteBuffer/allocate (* 2 (.capacity bf)))]
            (.set tl-bits-buffer new-bf)
            (recur new-bf))
          ^ByteBuffer result)))))

(defn- decode-bits
  [^bytes bs t]
  (b/read-buffer (ByteBuffer/wrap bs) (or t :data)))

(defn- dbi-name->string
  ^String [dbi-name]
  (cond
    (string? dbi-name) dbi-name
    (keyword? dbi-name) (name dbi-name)
    (symbol? dbi-name) (name dbi-name)
    :else (str dbi-name)))

(defn- dbi-name-bytes
  (^bytes [dbi-name]
   (.getBytes (dbi-name->string dbi-name) StandardCharsets/UTF_8))
  (^bytes [dbi-name ^HashMap cache]
   (if cache
     (let [^String s (dbi-name->string dbi-name)]
       (if-let [^bytes cached (.get cache s)]
         cached
         (let [^bytes bs (.getBytes s StandardCharsets/UTF_8)]
           (.put cache s bs)
           bs)))
     (dbi-name-bytes dbi-name))))

(defn- write-kv-components!
  ^ByteBuffer [^ThreadLocal tl
               ^ByteBuffer bf
               op
               dbi
               k
               v
               kt
               vt
               flags
               row
               ^HashMap dbi-cache]
  (let [^String dbi-name (dbi-name->string dbi)
        ^bytes dbi-bs (dbi-name-bytes dbi-name dbi-cache)
        dbi-len (alength dbi-bs)
        k-type (or kt :data)
        ^ByteBuffer k-bf (encode-to-buf k k-type)
        k-len (.remaining k-bf)]
    (case op
      :put
      (let [^ByteBuffer bf1 (-> bf
                                (bb-put-byte! tl (int op-kv-put))
                                (bb-put-u16! tl dbi-len)
                                (bb-put-bytes! tl dbi-bs)
                                (bb-write-type! tl k-type)
                                (bb-put-u16! tl k-len))
            ^ByteBuffer bf2 (bb-put-buffer! bf1 tl k-bf)
            v-type (or vt :data)
            ^ByteBuffer v-bf (encode-to-buf v v-type)
            v-len (.remaining v-bf)
            ^ByteBuffer bf3 (-> bf2
                                (bb-write-type! tl v-type)
                                (bb-put-u32! tl v-len))]
        (-> bf3
            (bb-put-buffer! tl v-bf)
            (bb-write-flags! tl flags)))

      :del
      (let [^ByteBuffer bf1 (-> bf
                                (bb-put-byte! tl (int op-kv-del))
                                (bb-put-u16! tl dbi-len)
                                (bb-put-bytes! tl dbi-bs)
                                (bb-write-type! tl k-type)
                                (bb-put-u16! tl k-len))]
        (-> bf1
            (bb-put-buffer! tl k-bf)
            (bb-write-flags! tl flags)))

      :put-list
      (let [^ByteBuffer bf1 (-> bf
                                (bb-put-byte! tl (int op-kv-put-list))
                                (bb-put-u16! tl dbi-len)
                                (bb-put-bytes! tl dbi-bs)
                                (bb-write-type! tl k-type)
                                (bb-put-u16! tl k-len))
            ^ByteBuffer bf2 (bb-put-buffer! bf1 tl k-bf)
            v-type (or vt :data)
            ^ByteBuffer v-bf (encode-to-buf v :data)
            v-len (.remaining v-bf)]
        (-> bf2
            (bb-write-type! tl v-type)
            (bb-put-u32! tl v-len)
            (bb-put-buffer! tl v-bf)
            (bb-write-flags! tl flags)))

      :del-list
      (let [^ByteBuffer bf1 (-> bf
                                (bb-put-byte! tl (int op-kv-del-list))
                                (bb-put-u16! tl dbi-len)
                                (bb-put-bytes! tl dbi-bs)
                                (bb-write-type! tl k-type)
                                (bb-put-u16! tl k-len))
            ^ByteBuffer bf2 (bb-put-buffer! bf1 tl k-bf)
            v-type (or vt :data)
            ^ByteBuffer v-bf (encode-to-buf v :data)
            v-len (.remaining v-bf)]
        (-> bf2
            (bb-write-type! tl v-type)
            (bb-put-u32! tl v-len)
            (bb-put-buffer! tl v-bf)
            (bb-write-flags! tl flags)))

      (raise "Unsupported txn-log KV op"
             {:type :txlog/corrupt
              :op op
              :row row}))))

(defn- write-kv-row!
  ([^ThreadLocal tl ^ByteBuffer bf row]
   (write-kv-row! tl bf row nil))
  ([^ThreadLocal tl ^ByteBuffer bf row ^HashMap dbi-cache]
   (cond
     (instance? datalevin.lmdb.KVTxData row)
     (let [^datalevin.lmdb.KVTxData tx row]
       (write-kv-components! tl
                             bf
                             (.-op tx)
                             (.-dbi-name tx)
                             (.-k tx)
                             (.-v tx)
                             (.-kt tx)
                             (.-vt tx)
                             (.-flags tx)
                             row
                             dbi-cache))

     (vector? row)
     (write-kv-components! tl
                           bf
                           (nth row 0 nil)
                           (nth row 1 nil)
                           (nth row 2 nil)
                           (nth row 3 nil)
                           (nth row 4 nil)
                           (nth row 5 nil)
                           (nth row 6 nil)
                           row
                           dbi-cache)

     (instance? java.util.List row)
     (let [^List row* row
           n (.size row*)]
       (write-kv-components! tl
                             bf
                             (when (< 0 n) (.get row* 0))
                             (when (< 1 n) (.get row* 1))
                             (when (< 2 n) (.get row* 2))
                             (when (< 3 n) (.get row* 3))
                             (when (< 4 n) (.get row* 4))
                             (when (< 5 n) (.get row* 5))
                             (when (< 6 n) (.get row* 6))
                             row
                             dbi-cache))

     :else
     (raise "Txn-log row must be a vector or KVTxData"
            {:type :txlog/corrupt
             :row row}))))

(defn use-parallel-row-encoding?
  [^long row-count]
  (and (>= row-count (long parallel-row-encode-min-rows))
       (> (.availableProcessors (Runtime/getRuntime)) 1)))

(defn- parallel-row-encode-chunk-size
  ^long [^long row-count]
  (let [cores (long (max 1 (long (.availableProcessors (Runtime/getRuntime)))))
        target-task-count (long (max 1
                                    (* (long parallel-row-encode-target-tasks-per-core)
                                       cores)))
        ceil-size (long (quot (+ row-count (dec target-task-count))
                              target-task-count))]
    (long (max (long parallel-row-encode-min-chunk-size) ceil-size))))

(defn- encode-kv-row-chunk-bytes
  ^bytes [^FastList rowsv ^long start ^long end]
  (let [^ThreadLocal tl tl-row-encode-buffer
        ^HashMap dbi-cache (.get tl-dbi-name-cache)
        _ (.clear dbi-cache)
        end* (int end)
        ^ByteBuffer bf0 (.clear ^ByteBuffer (.get tl))
        ^ByteBuffer bfN
        (loop [i (int start)
               ^ByteBuffer bf bf0]
          (if (< i end*)
            (recur (unchecked-inc-int i)
                   (write-kv-row! tl bf (.get rowsv i) dbi-cache))
            bf))
        len (.position bfN)
        out (byte-array len)]
    (.flip bfN)
    (.get bfN out)
    (maybe-shrink-threadlocal-buffer!
     tl
     bfN
     len
     tl-row-encode-buffer-initial-cap)
    (maybe-shrink-bits-buffer!)
    out))

(defn- append-kv-row-bytes!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl rows]
  (reduce (fn [^ByteBuffer acc ^bytes row-bytes]
            (bb-put-bytes! acc tl row-bytes))
          bf
          rows))

(defn- append-parallel-kv-row-bytes!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^FastList rowsv ^long row-count]
  (let [chunk-size (parallel-row-encode-chunk-size row-count)
        ranges (loop [start (long 0)
                      acc []]
                 (if (< start row-count)
                   (let [end (min row-count (+ start chunk-size))]
                     (recur end (conj acc [start end])))
                   acc))]
    (append-kv-row-bytes!
     bf
     tl
     (map+ (fn [[start end]]
             (encode-kv-row-chunk-bytes rowsv (long start) (long end)))
           ranges))))

(defn- decode-kv-row
  [^ByteBuffer bf]
  (let [opcode (int (bb-get-u8 bf {:field :opcode}))
        dbi-len (bb-get-u16 bf {:field :dbi-len})
        dbi-bs (bb-get-bytes bf dbi-len {:field :dbi-bytes})
        dbi (String. ^bytes dbi-bs StandardCharsets/UTF_8)
        k-type (bb-read-type bf)
        k-len (bb-get-u16 bf {:field :key-len})
        k-bs (bb-get-bytes bf k-len {:field :key-bytes})
        k (decode-bits k-bs k-type)]
    (case opcode
      0x10 (let [v-type (bb-read-type bf)
                 v-len (bb-get-u32 bf {:field :val-len})
                 v-bs (bb-get-bytes bf v-len {:field :val-bytes})
                 flags (bb-read-flags bf)
                 v (decode-bits v-bs v-type)]
             (cond-> [:put dbi k v k-type v-type]
               (seq flags) (conj flags)))
      0x11 (let [flags (bb-read-flags bf)]
             (cond-> [:del dbi k k-type]
               (seq flags) (conj flags)))
      0x16 (let [v-type (bb-read-type bf)
                 v-len (bb-get-u32 bf {:field :val-len})
                 v-bs (bb-get-bytes bf v-len {:field :val-bytes})
                 flags (bb-read-flags bf)
                 v (decode-bits v-bs :data)]
             (cond-> [:put-list dbi k v k-type v-type]
               (seq flags) (conj flags)))
      0x17 (let [v-type (bb-read-type bf)
                 v-len (bb-get-u32 bf {:field :val-len})
                 v-bs (bb-get-bytes bf v-len {:field :val-bytes})
                 flags (bb-read-flags bf)
                 v (decode-bits v-bs :data)]
             (cond-> [:del-list dbi k v k-type v-type]
               (seq flags) (conj flags)))
      (raise "Unknown txn-log opcode"
             {:type :txlog/corrupt
              :opcode opcode}))))

(defn encode-commit-row-payload
  "Encode canonical txn-log payload as raw binary bytes."
  ([lsn tx-time rows]
   (encode-commit-row-payload lsn tx-time rows {}))
  ([lsn tx-time rows {:keys [ha-term]}]
   (let [^FastList rowsv (ensure-fast-list rows)
         row-count (long (.size rowsv))
         parallel-rows? (use-parallel-row-encoding? row-count)
         ha-term (some-> ha-term long)
         flags (int (if (some? ha-term)
                      commit-payload-ha-term-flag
                      0))
         ^ThreadLocal tl tl-commit-body-buffer
         ^ByteBuffer bf0 (.clear ^ByteBuffer (.get tl))
         ^HashMap dbi-cache (when-not parallel-rows?
                              (.get tl-dbi-name-cache))
         _ (when dbi-cache (.clear dbi-cache))
         ^ByteBuffer bf1 (-> bf0
                             (bb-put-bytes! tl commit-payload-magic-bytes)
                             (bb-put-byte! tl commit-payload-format-major)
                             (bb-put-byte! tl flags)
                             (bb-put-u16! tl 0)
                             (bb-put-long! tl (long lsn))
                             (bb-put-long! tl (long tx-time))
                             (cond-> (some? ha-term)
                               (bb-put-long! tl ha-term))
                             (bb-put-u32! tl row-count))
         ^ByteBuffer bfN (if parallel-rows?
                           (append-parallel-kv-row-bytes! bf1
                                                          tl
                                                          rowsv
                                                          row-count)
                           (loop [i 0
                                  ^ByteBuffer bf bf1]
                             (if (< i row-count)
                               (recur (unchecked-inc-int i)
                                      (write-kv-row! tl bf (.get rowsv i)
                                                     dbi-cache))
                               bf)))
         len (.position bfN)
         out (byte-array len)]
     (.flip bfN)
     (.get bfN out)
     (maybe-shrink-threadlocal-buffer!
      tl
      bfN
      len
      tl-commit-body-buffer-initial-cap)
     (maybe-shrink-bits-buffer!)
     out)))

(defn patch-commit-row-payload-header!
  [^bytes body ^long lsn ^long tx-time]
  (let [^ByteBuffer bf (ByteBuffer/wrap body)]
    (.position bf (int commit-payload-lsn-offset))
    (.putLong bf (long lsn))
    (.position bf (int commit-payload-tx-time-offset))
    (.putLong bf (long tx-time)))
  body)

(defn- decode-commit-row-payload-prefix
  [^ByteBuffer bf]
  (let [^bytes magic (bb-get-bytes bf 4 {:field :magic})
        major (bb-get-u8 bf {:field :major})
        flags (bb-get-u8 bf {:field :flags})
        _reserved (bb-get-u16 bf {:field :reserved})
        lsn (bb-get-long bf {:field :lsn})
        tx-time (bb-get-long bf {:field :tx-time})
        ha-term (when (pos? (long (bit-and (long flags)
                                           (long commit-payload-ha-term-flag))))
                  (bb-get-long bf {:field :ha-term}))
        op-count (bb-get-u32 bf {:field :op-count})]
    (when-not (Arrays/equals magic ^bytes commit-payload-magic-bytes)
      (raise "Invalid txn-log payload magic"
             {:type :txlog/corrupt}))
    (when-not (= major commit-payload-format-major)
      (raise "Unsupported txn-log payload format major"
             {:type :txlog/corrupt
              :major major
              :expected commit-payload-format-major}))
    (when (> (long op-count) (long Integer/MAX_VALUE))
      (raise "Txn-log payload op count overflow"
             {:type :txlog/corrupt
              :op-count op-count}))
    {:lsn lsn
     :ts tx-time
     :ha-term (some-> ha-term long)
     :op-count op-count}))

(defn decode-commit-row-payload-header
  "Decode only the fixed-size txn-log payload header without materializing ops."
  [^bytes body]
  (let [{:keys [lsn ts ha-term op-count]}
        (decode-commit-row-payload-prefix (ByteBuffer/wrap body))]
    (cond-> {:lsn lsn
             :ts ts
             :op-count op-count}
      (some? ha-term)
      (assoc :ha-term ha-term))))

(defn decode-commit-row-payload
  "Decode raw binary txn-log payload bytes."
  [^bytes body]
  (let [bf (ByteBuffer/wrap body)
        {:keys [lsn ts ha-term op-count]}
        (decode-commit-row-payload-prefix bf)]
    (let [ops (loop [i (int 0)
                     acc []]
                (if (< i ^long op-count)
                  (recur (unchecked-inc-int i) (conj acc (decode-kv-row bf)))
                  acc))]
      (cond-> {:lsn lsn
               :ts ts
               :ops ops}
        (some? ha-term)
        (assoc :ha-term (long ha-term))))))
