;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.txlog
  "WAL record codec, segment management, sync state, and metadata helpers."
  (:require
   [datalevin.buffer :as bf]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.lmdb]
   [clojure.java.io :as io]
   [datalevin.util :as u :refer [raise map+]])
  (:import
   [java.io File]
   [java.nio ByteBuffer ByteOrder BufferOverflowException]
   [java.nio.channels FileChannel]
   [java.nio.file Files Path StandardCopyOption StandardOpenOption
    AtomicMoveNotSupportedException]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent.locks ReentrantLock]
   [java.util Arrays HashMap List Collection]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util.zip CRC32C]))

(def ^:const record-header-size 14)
(def ^:const format-major 1)
(def ^:const compressed-flag 0x01)
(def ^:private magic-bytes
  (byte-array [(byte 0x44) (byte 0x4c) (byte 0x57) (byte 0x4c)]))
(def ^:private segment-pattern #"^segment-(\d{16})\.wal$")
(def ^:private prepared-segment-pattern #"^segment-(\d{16})\.wal\.tmp$")
(def ^:private ^"[Ljava.nio.file.StandardOpenOption;"
  open-segment-read-options
  (into-array StandardOpenOption [StandardOpenOption/READ]))
(def ^:private ^"[Ljava.nio.file.StandardOpenOption;"
  open-segment-create-read-write-options
  (into-array StandardOpenOption
              [StandardOpenOption/CREATE
               StandardOpenOption/READ
               StandardOpenOption/WRITE]))

(def ^:const meta-slot-payload-size 64)
(def ^:const meta-slot-size (+ meta-slot-payload-size 4))
(def ^:const meta-format-major 1)
(def ^:private meta-file-name "meta")
(def ^:private meta-magic-bytes
  (byte-array [(byte 0x44) (byte 0x4c) (byte 0x54) (byte 0x4d)])) ; DLTM

(def ^:const commit-marker-slot-payload-size 60)
(def ^:const commit-marker-slot-size (+ commit-marker-slot-payload-size 4))
(def ^:const commit-marker-format-major 1)
(def ^:private commit-marker-magic-bytes
  (byte-array [(byte 0x44) (byte 0x4c) (byte 0x43) (byte 0x4d)])) ; DLCM

(def ^:private sync-mode-values #{:fsync :fdatasync :none})
(def ^:private segment-prealloc-mode-values #{:native :none})

(declare segment-files
         append-near-roll-sample-max
         append-near-roll-stats-array-size
         scan-segment
         segment-path
         open-segment-channel
         segment-end-offset
         truncate-partial-tail!
         read-meta-file
         meta-path
         ensure-next-segment-prepared!
         new-sync-manager
         sync-manager-pending?
         request-sync-on-append!
         begin-sync!
         complete-sync-success!
         complete-sync-failure!
         await-durable-lsn!
         classify-record-kind
         decode-commit-row-payload)

(defn enabled? [info] (true? (:wal? info)))

(defn sync-mode
  [info]
  (or (:wal-sync-mode info) c/*wal-sync-mode*))

(defn durability-profile
  [info]
  (or (:wal-durability-profile info)
      c/*wal-durability-profile*))

(defn commit-marker?
  [info]
  (if (contains? info :wal-commit-marker?)
    (boolean (:wal-commit-marker? info))
    c/*wal-commit-marker?*))

(defn commit-marker-version
  [info]
  (or (:wal-commit-marker-version info)
      c/*wal-commit-marker-version*))

(defn group-commit
  [info]
  (or (:wal-group-commit info) c/*wal-group-commit*))

(defn group-commit-ms
  [info]
  (or (:wal-group-commit-ms info) c/*wal-group-commit-ms*))

(defn meta-flush-max-txs
  [info]
  (or (:wal-meta-flush-max-txs info) c/*wal-meta-flush-max-txs*))

(defn meta-flush-max-ms
  [info]
  (or (:wal-meta-flush-max-ms info) c/*wal-meta-flush-max-ms*))

(defn commit-wait-ms
  [info]
  (or (:wal-commit-wait-ms info) c/*wal-commit-wait-ms*))

(defn sync-adaptive?
  [info]
  (if (contains? info :wal-sync-adaptive?)
    (boolean (:wal-sync-adaptive? info))
    c/*wal-sync-adaptive?*))

(defn segment-max-bytes
  [info]
  (or (:wal-segment-max-bytes info) c/*wal-segment-max-bytes*))

(defn segment-max-ms
  [info]
  (or (:wal-segment-max-ms info) c/*wal-segment-max-ms*))

(defn segment-prealloc?
  [info]
  (if (contains? info :wal-segment-prealloc?)
    (boolean (:wal-segment-prealloc? info))
    c/*wal-segment-prealloc?*))

(defn segment-prealloc-mode
  [info]
  (or (:wal-segment-prealloc-mode info)
      c/*wal-segment-prealloc-mode*))

(defn segment-prealloc-bytes
  [info]
  (or (:wal-segment-prealloc-bytes info)
      (:wal-segment-max-bytes info)
      c/*wal-segment-prealloc-bytes*
      c/*wal-segment-max-bytes*))

(defn validate-runtime-config!
  [info]
  (let [profile (durability-profile info)
        allowed #{:strict :relaxed}]
    (when-not (allowed profile)
      (raise "Unsupported WAL durability profile"
             {:wal-durability-profile profile
              :allowed                allowed})))
  (let [version (long (commit-marker-version info))]
    (when-not (= version commit-marker-format-major)
      (raise "Unsupported WAL commit marker version"
             {:wal-commit-marker-version version
              :supported                 commit-marker-format-major})))
  (let [mode (segment-prealloc-mode info)]
    (when-not (segment-prealloc-mode-values mode)
      (raise "Unsupported WAL segment preallocation mode"
             {:wal-segment-prealloc-mode mode
              :allowed                   segment-prealloc-mode-values})))
  (let [bytes (long (segment-prealloc-bytes info))]
    (when (neg? bytes)
      (raise "WAL segment preallocation bytes must be non-negative"
             {:wal-segment-prealloc-bytes bytes}))))

(defn- decode-scanned-record-entry
  [^long segment-id ^String path record]
  (try
    (let [payload (decode-commit-row-payload ^bytes (:body record))
          lsn     (long (or (:lsn payload) 0))]
      (when-not (pos? lsn)
        (raise "Txn-log payload missing valid positive LSN"
               {:type :txlog/corrupt
                :segment-id segment-id
                :path path
                :offset (long (:offset record))
                :record record}))
      (let [tx-time (long (or (:tx-time payload)
                              (:ts payload)
                              0))
            rows    (vec (or (:ops payload) []))
            tx-kind (classify-record-kind rows)]
        {:lsn lsn
         :tx-kind tx-kind
         :tx-time tx-time
         :rows rows
         :segment-id segment-id
         :offset (long (:offset record))
         :checksum (long (:checksum record))
         :path path}))
    (catch Exception e
      (raise "Malformed txn-log payload"
             e
             {:type :txlog/corrupt
              :segment-id segment-id
              :path path
              :offset (long (:offset record))}))))

(defn- txlog-records-cache-entry
  [segment-id path file-bytes modified-ms records]
  {:segment-id segment-id
   :path path
   :file-bytes file-bytes
   :modified-ms modified-ms
   :min-lsn (some-> records first :lsn long)
   :records records})

(defn- scan-segment-records-cache-entry
  [^long segment-id ^File file allow-preallocated-tail?]
  (let [path (.getPath file)
        file-bytes (long (.length file))
        modified-ms (long (.lastModified file))
        acc (FastList.)
        scan (scan-segment path
                           {:allow-preallocated-tail? allow-preallocated-tail?
                            :collect-records? false
                            :on-record
                            (fn [record]
                              (.add acc
                                    (decode-scanned-record-entry
                                     segment-id
                                     path
                                     record)))})
        records (vec acc)]
    {:cache-entry (txlog-records-cache-entry
                   segment-id path file-bytes modified-ms records)
     :partial-tail? (boolean (:partial-tail? scan))
     :last-lsn (some-> records peek :lsn long)}))

(defn init-runtime-state
  [info marker-state]
  (validate-runtime-config! info)
  (let [dir               (or (:wal-dir info)
                              (str (:dir info) u/+separator+ "txlog"))
        _                 (u/create-dirs dir)
        segments          (segment-files dir)
        [closed-record-cache last-from-closed]
        (reduce
         (fn [[cache ^long closed-last-lsn] {:keys [id file]}]
           (let [segment-id (long id)
                 {:keys [cache-entry partial-tail? last-lsn]}
                 (scan-segment-records-cache-entry segment-id file false)]
             (when partial-tail?
               (raise "Partial tail found on closed txn-log segment"
                      {:type :txlog/corrupt
                       :path (.getPath ^File file)}))
             [(assoc cache segment-id cache-entry)
              (long (or last-lsn closed-last-lsn))]))
         [{} 0]
         (butlast segments))
        active-id         (if (seq segments) (:id (last segments)) 1)
        active-path       (segment-path dir active-id)
        _                 (when-not (.exists (io/file active-path))
                            (let [^FileChannel tmp-ch
                                  (open-segment-channel active-path)]
                              (try
                                nil
                                (finally
                                  (.close tmp-ch)))))
        active-scan       (truncate-partial-tail!
                            active-path {:allow-preallocated-tail? true})
        active-records    (mapv (fn [record]
                                  (decode-scanned-record-entry
                                   (long active-id)
                                   active-path
                                   record))
                                (or (:records active-scan) []))
        active-offset     (segment-end-offset active-scan)
        active-file       (io/file active-path)
        active-record-cache
        (txlog-records-cache-entry
         (long active-id)
         active-path
         (long (.length ^File active-file))
         (long (.lastModified ^File active-file))
         active-records)
        txlog-records-cache (assoc closed-record-cache
                                   (long active-id)
                                   active-record-cache)
        closed-bytes      (reduce
                            (fn [acc {:keys [id file]}]
                              (if (= ^long (long id) ^long active-id)
                                acc
                                (+ ^long acc ^long (.length ^File file))))
                            0 segments)
        total-bytes       (+ ^long closed-bytes ^long active-offset)
        meta-path         (meta-path dir)
        meta              (read-meta-file meta-path)
        meta-cur          (:current meta)
        marker-cur        (:current marker-state)
        last-from-active  (long (or (some-> active-records
                                            peek
                                            :lsn)
                                    0))
        last-from-seg     (long (max last-from-active last-from-closed))
        last-committed    (max (long (or (:last-committed-lsn meta-cur) 0))
                               last-from-seg)
        last-durable      (max (long (or (:last-durable-lsn meta-cur) 0))
                               last-committed)
        last-applied      (long (or (:last-applied-lsn meta-cur) 0))
        last-sync-ms      (long (or (:updated-ms meta-cur)
                                    (System/currentTimeMillis)))
        now               (System/currentTimeMillis)
        ch                (open-segment-channel active-path)
        profile           (durability-profile info)
        prealloc-mode     (segment-prealloc-mode info)
        prealloc?         (and (segment-prealloc? info)
                               (not= prealloc-mode :none))
        state
        {:dir                    dir
         :meta-path              meta-path
         :segment-id             (volatile! active-id)
         :segment-created-ms     (volatile! now)
         :segment-channel        (volatile! ch)
         :segment-offset         (volatile! active-offset)
         :append-lock            (Object.)
         :segment-roll-lock      (ReentrantLock.)
         :next-lsn               (volatile! (inc last-committed))
         :segment-max-bytes      (long (segment-max-bytes info))
         :segment-max-ms         (long (segment-max-ms info))
         :segment-prealloc?      prealloc?
         :segment-prealloc-mode  prealloc-mode
         :segment-prealloc-bytes (long (segment-prealloc-bytes info))
         :durability-profile     profile
         :sync-mode              (sync-mode info)
         :commit-marker?         (commit-marker? info)
         :commit-marker-version  (long (commit-marker-version info))
         :meta-last-applied-lsn  last-applied
         :meta-revision          (volatile! (long (or (:revision meta-cur) -1)))
         :meta-publish-lock      (Object.)
         :meta-flush-max-txs     (long (meta-flush-max-txs info))
         :meta-flush-max-ms      (long (meta-flush-max-ms info))
         :meta-last-flush-ms     (volatile! last-sync-ms)
         :meta-pending-txs       (volatile! 0)
         :marker-revision        (volatile! (or (:revision marker-cur)
                                                -1))
         :commit-wait-ms         (long (commit-wait-ms info))

         :sync-manager (new-sync-manager
                         {:last-durable-lsn  last-durable
                          :last-appended-lsn last-committed
                          :last-sync-ms      last-sync-ms
                          :group-commit      (long (group-commit info))
                          :group-commit-ms   (long (group-commit-ms info))
                          :sync-adaptive?    (sync-adaptive? info)
                          :track-trailing?   (= :strict profile)})

         :segment-roll-count                      (volatile! 0)
         :segment-roll-duration-ms                (volatile! 0)
         :segment-prealloc-success-count          (volatile! 0)
         :segment-prealloc-failure-count          (volatile! 0)
         :append-near-roll-durations
         (volatile! (long-array append-near-roll-sample-max))
         :append-near-roll-sorted-durations
         (volatile! (long-array append-near-roll-stats-array-size))
         :append-p99-near-roll-ms                 (volatile! nil)
         :retention-backpressure-last-check-ms    (volatile! 0)
         :retention-backpressure-state            (volatile! nil)
         :retention-backpressure-blocked-since-ms (volatile! nil)
         :segment-summaries-cache                 (volatile! {})
         :txlog-records-cache                     (volatile! txlog-records-cache)
         :retention-total-bytes                   (volatile! total-bytes)
         :fatal-error                             (volatile! nil)}]
    (ensure-next-segment-prepared! state)
    {:dir dir :state state}))

(defn segment-file-name [segment-id] (format "segment-%016d.wal" segment-id))

(defn segment-path
  [dir segment-id]
  (str dir u/+separator+ (segment-file-name segment-id)))

(defn prepared-segment-file-name
  [segment-id]
  (str (segment-file-name segment-id) ".tmp"))

(defn prepared-segment-path
  [dir segment-id]
  (str dir u/+separator+ (prepared-segment-file-name segment-id)))

(defn meta-path [dir] (str dir u/+separator+ meta-file-name))

(defn parse-segment-id
  [file-name]
  (when-let [[_ sid] (re-matches segment-pattern file-name)]
    (Long/parseLong sid)))

(defn parse-prepared-segment-id
  [file-name]
  (when-let [[_ sid] (re-matches prepared-segment-pattern file-name)]
    (Long/parseLong sid)))

(defn segment-files
  [dir]
  (->> (or (u/list-files dir) [])
       (keep (fn [^File f]
               (when-let [sid (parse-segment-id (.getName f))]
                 {:id sid :file f})))
       (sort-by :id)
       vec))

(defn prepared-segment-files
  [dir]
  (->> (or (u/list-files dir) [])
       (keep (fn [^File f]
               (when-let [sid (parse-prepared-segment-id (.getName f))]
                 {:id sid :file f})))
       (sort-by :id)
       vec))

(def ^:private ^ThreadLocal tl-crc32c
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_] (CRC32C.)))))

(defn- crc32c
  [^bytes bs]
  (let [^CRC32C crc (.get tl-crc32c)]
    (.reset crc)
    (.update crc bs 0 (alength bs))
    (unchecked-int (.getValue crc))))

(defn- put-u32
  [^ByteBuffer bf ^long x]
  (.putInt bf (unchecked-int (bit-and x 0xffffffff))))

(def ^:private ^ThreadLocal tl-record-header-buffer
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_]
       (doto (ByteBuffer/allocate record-header-size)
         (.order ByteOrder/BIG_ENDIAN))))))

(defn- checked-record-body-len
  [^bytes body]
  (let [body-len (alength body)]
    (when (neg? body-len)
      (raise "Invalid record body length" {:body-len body-len}))
    body-len))

(defn- record-body-checksum
  [body]
  (bit-and 0xffffffff ^int (crc32c body)))

(defn- write-record-header!
  [^ByteBuffer header-bf body-len compressed? checksum]
  (.clear header-bf)
  (.put header-bf ^bytes magic-bytes)
  (.put header-bf (byte format-major))
  (.put header-bf (byte (if compressed? compressed-flag 0x00)))
  (put-u32 header-bf body-len)
  (put-u32 header-bf checksum)
  (.flip header-bf)
  header-bf)

(defn- write-fully!
  [^FileChannel ch ^ByteBuffer bf]
  (while (.hasRemaining bf)
    (let [n (.write ch bf)]
      (when-not (pos? n)
        (raise "Unable to progress while writing txn-log data"
               {:remaining (.remaining bf)})))))

(defn- buffers-remaining?
  [^"[Ljava.nio.ByteBuffer;" bufs]
  (let [n (alength bufs)]
    (loop [i (int 0)]
      (cond
        (>= i n) false
        (.hasRemaining ^ByteBuffer (aget bufs i)) true
        :else (recur (unchecked-inc-int i))))))

(defn- total-buffers-remaining
  ^long [^"[Ljava.nio.ByteBuffer;" bufs]
  (reduce (fn [^long acc ^ByteBuffer bf]
            (+ acc (long (.remaining bf))))
          0
          bufs))

(defn- write-fully-buffers!
  [^FileChannel ch ^"[Ljava.nio.ByteBuffer;" bufs]
  (while (buffers-remaining? bufs)
    (let [n (.write ch bufs)]
      (when-not (pos? n)
        (raise "Unable to progress while writing txn-log data"
               {:remaining (total-buffers-remaining bufs)})))))

(defn force-channel!
  "Force channel to disk according to sync mode:
  - `:fsync` -> force data + metadata
  - `:fdatasync` -> force data only
  - `:none` -> no force"
  ([^FileChannel ch] (force-channel! ch :fdatasync))
  ([^FileChannel ch sync-mode]
   (when-not (sync-mode-values sync-mode)
     (raise "Unsupported txn-log sync mode"
            {:sync-mode sync-mode :allowed sync-mode-values}))
   (case sync-mode
     :none      nil
     :fdatasync (.force ch false)
     :fsync     (.force ch true))
   sync-mode))

(defn encode-record
  ([body] (encode-record body {}))
  ([^bytes body {:keys [compressed?]
                 :or   {compressed? false}}]
   (let [body-len  (checked-record-body-len body)
         checksum  (record-body-checksum body)
         total-len (+ record-header-size body-len)
         record    (byte-array (int total-len))
         header-bf (doto (ByteBuffer/wrap record 0 record-header-size)
                     (.order ByteOrder/BIG_ENDIAN))]
     (write-record-header! header-bf body-len (boolean compressed?) checksum)
     (when (pos? body-len)
       (System/arraycopy body 0 record record-header-size (int body-len)))
     record)))

(defn- read-int-unsigned [^ByteBuffer bf] (bit-and 0xffffffff (.getInt bf)))

(defn- decode-header-buffer
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

(defn- decode-header-bytes
  [^bytes header-bytes offset]
  (let [header (doto (ByteBuffer/wrap header-bytes)
                 (.order ByteOrder/BIG_ENDIAN))]
    (decode-header-buffer header offset)))

(defn- read-fully-at!
  ^ByteBuffer [^FileChannel ch ^long pos ^ByteBuffer bf]
  (loop [p pos]
    (if (.hasRemaining bf)
      (let [n (.read ch bf p)]
        (when (neg? n)
          (raise "Unexpected EOF reading txn-log segment"
                 {:position p :remaining (.remaining bf)}))
        (when (zero? n)
          (raise "Unable to progress while reading txn-log segment"
                 {:position p :remaining (.remaining bf)}))
        (recur (+ p n)))
      bf)))

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
          :compressed? (pos? ^long (bit-and flags compressed-flag))
          :body-len    body-len
          :checksum    checksum
          :body        body})))))

(defn- read-fully-at
  [^FileChannel ch ^long pos ^long len]
  (let [out (byte-array (int len))
        bf  (ByteBuffer/wrap out)]
    (read-fully-at! ch pos bf)
    out))

(defn- buffer-all-zero?
  [^ByteBuffer bf]
  (let [start (.position bf)
        end   (.limit bf)]
    (loop [i start]
      (if (< i end)
        (if (zero? (int (.get bf i)))
          (recur (unchecked-inc-int i))
          false)
        true))))

(defn- tail-all-zero?
  ([^FileChannel ch ^long offset ^long size]
   (let [^ByteBuffer read-bf (bf/get-array-buffer 8192)]
     (try
       (tail-all-zero? ch offset size read-bf)
       (finally
         (bf/return-array-buffer read-bf)))))
  ([^FileChannel ch ^long offset ^long size ^ByteBuffer read-bf]
   (loop [pos offset]
     (if (>= pos size)
       true
       (let [len (int (min 8192 (- size pos)))]
         (.clear read-bf)
         (.limit read-bf len)
         (read-fully-at! ch pos read-bf)
         (.flip read-bf)
         (if (buffer-all-zero? read-bf)
           (recur (+ pos len))
           false))))))

(defn scan-segment
  "Scan a txn-log segment.

  Returns:
  - `:records` decoded record maps
  - `:valid-end` byte offset after the last valid record
  - `:size` file size in bytes
  - `:partial-tail?` true only for a trailing incomplete record/header

  Options:
  - `:allow-preallocated-tail?` accept all-zero trailing bytes
  - `:collect-records?` retain decoded records in `:records` (default true)
  - `:on-record` callback invoked for each decoded record"
  ([^String path] (scan-segment path {}))
  ([^String path {:keys [allow-preallocated-tail? collect-records? on-record]
                  :or   {allow-preallocated-tail? false
                         collect-records?         true}}]
   (let [f (io/file path)]
     (with-open [^FileChannel ch (FileChannel/open
                                   (.toPath f)
                                   open-segment-read-options)]
       (let [size                (.size ch)
             ^ByteBuffer read-bf (doto (bf/get-array-buffer 8192)
                                   (.order ByteOrder/BIG_ENDIAN))]
         (try
           (loop [offset  0
                  records (when collect-records? [])]
             (let [remaining (- size offset)]
               (cond
                 (zero? remaining)
                 {:records       records
                  :valid-end     offset
                  :size          size
                  :partial-tail? false}

                 (< remaining record-header-size)
                 {:records       records
                  :valid-end     offset
                  :size          size
                  :partial-tail? true}

                 :else
                 (let [_         (doto read-bf
                                   (.clear)
                                   (.limit record-header-size))
                       _         (read-fully-at! ch offset read-bf)
                       _         (.flip read-bf)
                       header-map
                       (try
                         (decode-header-buffer read-bf offset)
                         (catch Exception e
                           (if (and allow-preallocated-tail?
                                    (tail-all-zero? ch offset size read-bf))
                             :txlog/preallocated-tail
                             (throw (ex-info "Txn-log segment corruption"
                                             {:type   :txlog/corrupt
                                              :path   path
                                              :offset offset}
                                             e)))))
                       body-len  (when (map? header-map)
                                   (:body-len header-map))
                       total-len (when body-len
                                   (+ ^long record-header-size
                                      ^long body-len))]
                   (cond
                     (identical? header-map :txlog/preallocated-tail)
                     {:records            records
                      :valid-end          offset
                      :size               size
                      :partial-tail?      true
                      :preallocated-tail? true}

                     (> ^long total-len ^long remaining)
                     {:records       records
                      :valid-end     offset
                      :size          size
                      :partial-tail? true}

                     :else
                     (let [record
                           (try
                             (let [{:keys [major flags body-len checksum]}
                                   header-map
                                   body (read-fully-at
                                          ch
                                          (+ offset record-header-size)
                                          body-len)]
                               (when-not (= checksum
                                            ^long (bit-and 0xffffffff
                                                           (crc32c body)))
                                 (raise "Txn-log record checksum mismatch"
                                        {:offset   offset
                                         :expected checksum}))
                               {:major       major
                                :flags       flags
                                :compressed? (pos? (bit-and flags
                                                            compressed-flag))
                                :body-len    body-len
                                :checksum    checksum
                                :body        body})
                             (catch Exception e
                               (throw (ex-info "Txn-log segment corruption"
                                               {:type   :txlog/corrupt
                                                :path   path
                                                :offset offset}
                                               e))))
                           next-offset (long (+ offset total-len))
                           record*     (assoc record
                                              :offset offset
                                              :next-offset next-offset)]
                       (when on-record
                         (on-record record*))
                       (recur next-offset
                              (if collect-records?
                                (conj records record*)
                                records))))))))
           (finally
             (bf/return-array-buffer read-bf))))))))

(defn truncate-partial-tail!
  ([^String path] (truncate-partial-tail! path {}))
  ([^String path scan-opts]
   (let [{:keys [valid-end size partial-tail?] :as scan}
         (scan-segment path scan-opts)]
     (if partial-tail?
       (with-open [^FileChannel ch (FileChannel/open
                                     (.toPath (io/file path))
                                     (into-array StandardOpenOption
                                                 [StandardOpenOption/WRITE]))]
         (.truncate ch (long valid-end))
         (assoc scan
                :size valid-end
                :valid-end valid-end
                :partial-tail? false
                :preallocated-tail? false
                :truncated? true
                :old-size size
                :new-size valid-end
                :dropped-bytes (- size valid-end)))
       (assoc scan
              :truncated? false
              :old-size size
              :new-size valid-end
              :dropped-bytes 0)))))

(defn segment-end-offset
  [scan]
  (long
   (if (:truncated? scan)
     (or (:new-size scan) 0)
     (or (:valid-end scan) 0))))

(defn open-segment-channel
  [^String path]
  (FileChannel/open
   (.toPath (io/file path))
   open-segment-create-read-write-options))

(defn append-record-at!
  ([^FileChannel ch ^long offset ^bytes body]
   (append-record-at! ch offset body {}))
  ([^FileChannel ch ^long offset ^bytes body
    {:keys [compressed?] :or {compressed? false}}]
   (let [body-len        (checked-record-body-len body)
         checksum        (record-body-checksum body)
         total-size      (+ record-header-size body-len)
         ^ByteBuffer hdr (write-record-header!
                           (.get tl-record-header-buffer)
                           body-len
                           (boolean compressed?)
                           checksum)]
     (.position ch offset)
     (if (pos? body-len)
       (write-fully-buffers! ch
                             (into-array ByteBuffer [hdr (ByteBuffer/wrap body)]))
       (write-fully! ch hdr))
     {:offset offset :size total-size :checksum checksum})))

(defn append-record!
  ([^FileChannel ch ^bytes body]
   (append-record! ch body {}))
  ([^FileChannel ch ^bytes body opts]
   (let [offset (.size ch)]
     (append-record-at! ch offset body opts))))

(defn force-segment!
  [state ^FileChannel ch sync-mode]
  (force-channel! ch sync-mode))

(defn prepare-segment!
  "Create and preallocate a segment at `tmp-path`."
  [^String tmp-path ^long bytes]
  (let [p (.toPath (io/file tmp-path))]
    (with-open [^FileChannel ch
                (FileChannel/open
                  p (into-array StandardOpenOption
                                [StandardOpenOption/CREATE
                                 StandardOpenOption/TRUNCATE_EXISTING
                                 StandardOpenOption/READ
                                 StandardOpenOption/WRITE]))]
      (when (pos? bytes)
        (.position ch (dec bytes))
        (.write ch (ByteBuffer/wrap (byte-array [(byte 0x00)]))))
      (.force ch true))
    tmp-path))

(defn activate-prepared-segment!
  "Atomically (when possible) move `tmp-path` into final segment path."
  [^String tmp-path ^String final-path]
  (let [^Path src (.toPath (io/file tmp-path))
        ^Path dst (.toPath (io/file final-path))]
    (try
      (Files/move src dst
                  (into-array StandardCopyOption
                              [StandardCopyOption/REPLACE_EXISTING
                               StandardCopyOption/ATOMIC_MOVE]))
      (catch AtomicMoveNotSupportedException _
        (Files/move src dst
                    (into-array StandardCopyOption
                                [StandardCopyOption/REPLACE_EXISTING]))))
    final-path))

(defn prepare-next-segment!
  [^String dir ^long segment-id ^long bytes]
  (prepare-segment! (prepared-segment-path dir segment-id) bytes))

(defn activate-next-segment!
  "Activate preallocated next segment if present, else create empty segment."
  [^String dir ^long segment-id]
  (let [tmp-path   (prepared-segment-path dir segment-id)
        final-path (segment-path dir segment-id)
        tmp-file   (io/file tmp-path)
        final-file (io/file final-path)]
    (cond
      (.exists final-file) final-path
      (.exists tmp-file)   (activate-prepared-segment! tmp-path final-path)
      :else
      (do
        (with-open [^FileChannel _ (open-segment-channel final-path)])
        final-path))))

(defn preallocation-enabled-state?
  [state]
  (and (:segment-prealloc? state)
       (contains? #{:native} (:segment-prealloc-mode state))
       (pos? (long (or (:segment-prealloc-bytes state) 0)))))

(defn- inc-volatile-long!
  [v]
  (when (some? v)
    (vswap! v u/long-inc)
    #_(vreset! v (u/long-inc @v))))

(defn- add-volatile-long!
  [v delta]
  (when (some? v)
    (vreset! v (+ ^long @v ^long (or delta 0)))))

(defn ensure-next-segment-prepared!
  [state]
  (when (preallocation-enabled-state? state)
    (let [next-id    (inc ^long @(:segment-id state))
          dir        (:dir state)
          tmp-file   (io/file (prepared-segment-path dir next-id))
          final-file (io/file (segment-path dir next-id))]
      (cond
        (.exists final-file) false
        (.exists tmp-file)   true
        :else
        (try
          (prepare-next-segment! dir next-id
                                 (long (:segment-prealloc-bytes state)))
          (inc-volatile-long! (:segment-prealloc-success-count state))
          true
          (catch Exception _
            (inc-volatile-long! (:segment-prealloc-failure-count state))
            false))))))

(defn- activated-segment-offset
  [^String path preserve-preallocated-tail?]
  (let [scan (scan-segment path {:allow-preallocated-tail? true
                                 :collect-records?         false})]
    (cond
      (and preserve-preallocated-tail?
           (:preallocated-tail? scan))
      (:valid-end scan)

      (:partial-tail? scan)
      (segment-end-offset
        (truncate-partial-tail! path {:allow-preallocated-tail? true}))

      :else
      (:valid-end scan))))

(def ^:private append-near-roll-sample-max 512)
(def ^:private append-near-roll-linear-bucket-max-ms 255)
(def ^:private append-near-roll-linear-bucket-count
  (inc append-near-roll-linear-bucket-max-ms))
(def ^:private append-near-roll-tail-bucket-count 32)
(def ^:private append-near-roll-tail-base-shift 8) ; 2^8 = 256ms
(def ^:private append-near-roll-hist-bucket-count
  (+ append-near-roll-linear-bucket-count
     append-near-roll-tail-bucket-count))
(def ^:private append-near-roll-stats-head-idx 0)
(def ^:private append-near-roll-stats-size-idx 1)
(def ^:private append-near-roll-stats-hist-offset 2)
(def ^:private append-near-roll-stats-array-size
  (+ append-near-roll-stats-hist-offset append-near-roll-hist-bucket-count))
(def ^:private long-array-class (class (long-array 0)))

(defn- near-roll-threshold
  [^long segment-max-bytes]
  (if (pos? segment-max-bytes)
    (max 1 ^long (min (quot ^long segment-max-bytes 20)
                      (* 16 1024 1024)))
    0))

(defn- near-roll-append?
  [state ^long segment-offset]
  (let [max-bytes (long (or (:segment-max-bytes state) 0))
        threshold (near-roll-threshold max-bytes)]
    (and (pos? max-bytes)
         (>= segment-offset (max 0 (- max-bytes threshold))))))

(defn- ensure-fast-list
  ^FastList [x]
  (cond
    (instance? FastList x) x

    (instance? Collection x) (FastList. ^Collection x)

    (nil? x) (FastList.)

    :else (let [^FastList out (FastList.)]
            (doseq [v x] (.add out v))
            out)))

(defn- append-near-roll-bucket-idx
  ^long [^long duration-ms]
  (if (<= duration-ms append-near-roll-linear-bucket-max-ms)
    duration-ms
    (let [lg (long (- 63 (Long/numberOfLeadingZeros duration-ms)))
          tail-idx (max 0 (- lg append-near-roll-tail-base-shift))]
      (+ append-near-roll-linear-bucket-count
         (min (dec append-near-roll-tail-bucket-count) tail-idx)))))

(defn- append-near-roll-bucket-upper-ms
  ^long [^long bucket-idx]
  (if (< bucket-idx append-near-roll-linear-bucket-count)
    bucket-idx
    (let [tail-idx (max 0 (- bucket-idx append-near-roll-linear-bucket-count))
          shift (+ (inc append-near-roll-tail-base-shift) tail-idx)]
      (if (>= shift 63)
        Long/MAX_VALUE
        (dec (bit-shift-left 1 shift))))))

(defn- append-near-roll-hist-inc!
  [^longs stats ^long duration-ms]
  (let [bucket-idx (append-near-roll-bucket-idx duration-ms)
        hist-idx (+ append-near-roll-stats-hist-offset bucket-idx)]
    (aset-long stats hist-idx
               (long (inc (long (aget stats hist-idx)))))))

(defn- append-near-roll-hist-dec!
  [^longs stats ^long duration-ms]
  (let [bucket-idx (append-near-roll-bucket-idx duration-ms)
        hist-idx (+ append-near-roll-stats-hist-offset bucket-idx)
        current (long (aget stats hist-idx))]
    (when (pos? current)
      (aset-long stats hist-idx (dec current)))))

(defn- append-near-roll-p99-from-hist
  ^long [^longs stats ^long sample-size]
  (if (pos? sample-size)
    (let [rank (long (inc (quot (* 99 (dec sample-size)) 100)))]
      (loop [bucket-idx 0
             seen 0]
        (if (< bucket-idx append-near-roll-hist-bucket-count)
          (let [cnt (long (aget stats (+ append-near-roll-stats-hist-offset
                                         bucket-idx)))
                seen* (+ ^long seen ^long cnt)]
            (if (>= seen* rank)
              (append-near-roll-bucket-upper-ms bucket-idx)
              (recur (inc bucket-idx) seen*)))
          (append-near-roll-bucket-upper-ms
           (dec append-near-roll-hist-bucket-count)))))
    0))

(defn- ensure-append-near-roll-structures!
  [samples-v sorted-v]
  (let [ring0 @samples-v
        stats0 @sorted-v
        ring-ok? (and (instance? long-array-class ring0)
                      (= (alength ^longs ring0)
                         append-near-roll-sample-max))
        stats-ok? (and (instance? long-array-class stats0)
                       (= (alength ^longs stats0)
                          append-near-roll-stats-array-size))]
    (if (and ring-ok? stats-ok?)
      [ring0 stats0]
      (let [^longs ring (long-array append-near-roll-sample-max)
            ^longs stats (long-array append-near-roll-stats-array-size)]
        (vreset! samples-v ring)
        (vreset! sorted-v stats)
        [ring stats]))))

(defn- record-append-near-roll-ms!
  [state duration-ms]
  (when-let [samples-v (:append-near-roll-durations state)]
    (when-let [sorted-v (:append-near-roll-sorted-durations state)]
      (let [duration-ms  (long (max 0 (or duration-ms 0)))
            p99-v        (:append-p99-near-roll-ms state)
            metrics-lock (or (:append-lock state) state)]
        (locking metrics-lock
          (let [[^longs ring ^longs stats]
                (ensure-append-near-roll-structures! samples-v sorted-v)
                head (long (aget stats append-near-roll-stats-head-idx))
                size (long (aget stats append-near-roll-stats-size-idx))
                full? (>= size append-near-roll-sample-max)
                dropped (when full?
                          (long (aget ring (int head))))
                size* (if full? size (inc size))
                next-head (if (= (inc head) append-near-roll-sample-max)
                            0
                            (inc head))]
            (when full?
              (append-near-roll-hist-dec! stats dropped))
            (aset-long ring (int head) duration-ms)
            (append-near-roll-hist-inc! stats duration-ms)
            (aset-long stats append-near-roll-stats-head-idx next-head)
            (aset-long stats append-near-roll-stats-size-idx size*)
            (when p99-v
              (vreset! p99-v (append-near-roll-p99-from-hist stats size*)))))))))

(defn should-roll-segment?
  [^long segment-bytes ^long segment-created-ms ^long now-ms
   {:keys [segment-max-bytes segment-max-ms]
    :or {segment-max-bytes (* 256 1024 1024)
         segment-max-ms 300000}}]
  (or (>= segment-bytes segment-max-bytes)
      (>= (- now-ms segment-created-ms) segment-max-ms)))

(defn- maybe-roll-segment-candidate?
  [state ^long now-ms]
  (let [created-src (:segment-created-ms state)
        created (long (if (instance? clojure.lang.IDeref created-src)
                        @created-src
                        (or created-src now-ms)))
        offset-src (:segment-offset state)
        offset (when (some? offset-src)
                 (long (if (instance? clojure.lang.IDeref offset-src)
                         @offset-src
                         offset-src)))]
    (if (some? offset)
      (should-roll-segment? offset
                            created
                            now-ms
                            {:segment-max-bytes (:segment-max-bytes state)
                             :segment-max-ms (:segment-max-ms state)})
      ;; When fast byte probe is unavailable, keep previous behavior.
      true)))

(defn maybe-roll-segment!
  [state now-ms]
  (when (maybe-roll-segment-candidate? state now-ms)
    (let [roll-once!
          (fn []
            (let [append-lock (or (:append-lock state) state)
                  roll-candidate
                  (locking append-lock
                    (let [sync-manager (:sync-manager state)
                          pending? (and sync-manager
                                        (sync-manager-pending? sync-manager))
                          ^FileChannel ch @(:segment-channel state)
                          created (long @(:segment-created-ms state))
                          bytes (if-let [segment-offset (:segment-offset state)]
                                  (long @segment-offset)
                                  (.size ch))]
                      (when (and (not pending?)
                                 (should-roll-segment?
                                  bytes created now-ms
                                  {:segment-max-bytes (:segment-max-bytes state)
                                   :segment-max-ms (:segment-max-ms state)}))
                        {:segment-id (long @(:segment-id state))
                         :channel ch})))]
              (when-let [{:keys [segment-id channel]} roll-candidate]
                (let [roll-start-ms (System/currentTimeMillis)
                      next-id (inc ^long segment-id)
                      dir (:dir state)
                      tmp-path (prepared-segment-path dir next-id)
                      next-path (segment-path dir next-id)
                      tmp-file (io/file tmp-path)
                      next-file (io/file next-path)
                      tmp-exists? (.exists tmp-file)
                      next-exists? (.exists next-file)
                      final-path (activate-next-segment! dir next-id)
                      preserve-preallocated-tail?
                      (and (preallocation-enabled-state? state)
                           tmp-exists?
                           (not next-exists?))
                      next-offset (activated-segment-offset
                                   final-path
                                   preserve-preallocated-tail?)
                      ^FileChannel next-ch (open-segment-channel next-path)
                      swapped? (volatile! false)]
                  (try
                    (let [swap-result
                          (locking append-lock
                            (let [sync-manager (:sync-manager state)
                                  pending? (and sync-manager
                                                (sync-manager-pending? sync-manager))
                                  current-segment-id (long @(:segment-id state))
                                  ^FileChannel current-channel @(:segment-channel state)
                                  created (long @(:segment-created-ms state))
                                  bytes (if-let [segment-offset (:segment-offset state)]
                                          (long @segment-offset)
                                          (.size current-channel))]
                              (when (and (= segment-id current-segment-id)
                                         (identical? channel current-channel)
                                         (not pending?)
                                         (should-roll-segment?
                                          bytes created now-ms
                                          {:segment-max-bytes
                                           (:segment-max-bytes state)
                                           :segment-max-ms
                                           (:segment-max-ms state)}))
                                (vreset! swapped? true)
                                (vreset! (:segment-id state) next-id)
                                (vreset! (:segment-channel state) next-ch)
                                (when-let [segment-offset (:segment-offset state)]
                                  (vreset! segment-offset next-offset))
                                (vreset! (:segment-created-ms state) now-ms)
                                {:old-channel current-channel
                                 :old-bytes bytes})))]
                      (if-let [{:keys [old-channel old-bytes]} swap-result]
                        (do
                          (try
                            (.truncate ^FileChannel old-channel ^long old-bytes)
                            (force-channel! old-channel (:sync-mode state))
                            (finally
                              (.close ^FileChannel old-channel)))
                          (inc-volatile-long! (:segment-roll-count state))
                          (add-volatile-long!
                           (:segment-roll-duration-ms state)
                           (- (System/currentTimeMillis) roll-start-ms))
                          (ensure-next-segment-prepared! state))
                        (.close next-ch)))
                    (catch Exception e
                      (when-not @swapped?
                        (try
                          (.close next-ch)
                          (catch Exception _)))
                      (throw e)))))))]
      (if-let [^ReentrantLock roll-lock (:segment-roll-lock state)]
        (when (.tryLock roll-lock)
          (try
            (roll-once!)
            (finally
              (.unlock roll-lock))))
        (locking state
          (roll-once!))))))

(def ^:const no-floor-lsn
  Long/MAX_VALUE)

(defn safe-inc-lsn
  [^long lsn]
  (if (= lsn Long/MAX_VALUE)
    lsn
    (inc lsn)))

(defn parse-floor-lsn
  [v floor-k]
  (if (nil? v)
    no-floor-lsn
    (let [lsn (try
                (long v)
                (catch Exception _
                  (raise "Invalid floor LSN value"
                         {:type :txlog/invalid-floor
                          :floor floor-k
                          :value v})))]
      (when (neg? lsn)
        (raise "Floor LSN must be non-negative"
               {:type :txlog/invalid-floor
                :floor floor-k
                :value v}))
      lsn)))

(defn parse-optional-floor-lsn
  [v floor-k]
  (when (some? v)
    (parse-floor-lsn v floor-k)))

(defn parse-non-negative-long
  [v field-k]
  (when (some? v)
    (let [n (try
              (long v)
              (catch Exception _
                (raise "Invalid non-negative long value"
                       {:type :txlog/invalid-floor-provider-state
                        :field field-k
                        :value v})))]
      (when (neg? n)
        (raise "Negative value is not allowed"
               {:type :txlog/invalid-floor-provider-state
                :field field-k
                :value v}))
      n)))

(defn ensure-floor-provider-id
  [id kind]
  (when (nil? id)
    (raise "Floor provider id cannot be nil"
           {:type :txlog/invalid-floor-provider-state
            :kind kind
            :id id}))
  id)

(defn parse-floor-provider-map
  [v k]
  (cond
    (nil? v) {}
    (map? v) v
    :else
    (raise "Floor provider map in kv-info must be a map"
           {:type :txlog/invalid-floor-provider-state
            :key k
            :value v})))

(defn snapshot-floor-update-plan
  [snapshot-lsn previous-snapshot-lsn old-current-raw old-previous-raw
   current-floor-k previous-floor-k]
  (let [snapshot-lsn (parse-floor-lsn snapshot-lsn current-floor-k)
        old-current-lsn (parse-optional-floor-lsn old-current-raw
                                                  current-floor-k)
        old-previous-lsn (parse-optional-floor-lsn old-previous-raw
                                                   previous-floor-k)
        previous-lsn (if (some? previous-snapshot-lsn)
                       (parse-optional-floor-lsn previous-snapshot-lsn
                                                 previous-floor-k)
                       old-current-lsn)]
    (when (and (some? old-current-lsn) (< snapshot-lsn old-current-lsn))
      (raise "Snapshot current LSN cannot move backward"
             {:type :txlog/invalid-floor-provider-state
              :old-current-lsn old-current-lsn
              :new-current-lsn snapshot-lsn}))
    (when (and (some? previous-lsn) (> previous-lsn snapshot-lsn))
      (raise "Snapshot previous LSN cannot be greater than current LSN"
             {:type :txlog/invalid-floor-provider-state
              :snapshot-previous-lsn previous-lsn
              :snapshot-current-lsn snapshot-lsn}))
    (when (and (some? old-previous-lsn)
               (some? previous-lsn)
               (< previous-lsn old-previous-lsn))
      (raise "Snapshot previous LSN cannot move backward"
             {:type :txlog/invalid-floor-provider-state
              :old-previous-lsn old-previous-lsn
              :new-previous-lsn previous-lsn}))
    {:txs (cond-> [[:put current-floor-k snapshot-lsn]]
            (some? previous-lsn)
            (conj [:put previous-floor-k previous-lsn])

            (nil? previous-lsn)
            (conj [:del previous-floor-k]))
     :ok? true
     :snapshot-current-lsn snapshot-lsn
     :snapshot-previous-lsn previous-lsn
     :rotated? (and (nil? previous-snapshot-lsn)
                    (some? old-current-lsn))
     :old-current-lsn old-current-lsn
     :old-previous-lsn old-previous-lsn}))

(defn snapshot-floor-clear-plan
  [old-current-raw old-previous-raw current-floor-k previous-floor-k]
  (let [old-current-lsn (parse-optional-floor-lsn old-current-raw
                                                  current-floor-k)
        old-previous-lsn (parse-optional-floor-lsn old-previous-raw
                                                   previous-floor-k)]
    {:txs [[:del current-floor-k]
           [:del previous-floor-k]]
     :ok? true
     :cleared? (boolean (or (some? old-current-lsn)
                            (some? old-previous-lsn)))
     :old-current-lsn old-current-lsn
     :old-previous-lsn old-previous-lsn}))

(defn- replica-entry-lsn
  [replica-state replica-id]
  (let [old (get replica-state replica-id)
        old-lsn-raw (if (map? old)
                      (or (:applied-lsn old)
                          (:ack-lsn old)
                          (:lsn old)
                          (:floor-lsn old))
                      old)]
    (parse-optional-floor-lsn old-lsn-raw [:replica replica-id :lsn])))

(defn replica-floor-update-plan
  [replica-id applied-lsn now-ms entries entries-k]
  (let [replica-id (ensure-floor-provider-id replica-id :replica)
        lsn (parse-floor-lsn applied-lsn [:replica replica-id :lsn])
        now-ms (long now-ms)
        m0 (parse-floor-provider-map entries entries-k)
        old-lsn (replica-entry-lsn m0 replica-id)]
    (when (and (some? old-lsn) (< lsn old-lsn))
      (raise "Replica floor LSN cannot move backward"
             {:type :txlog/invalid-floor-provider-state
              :replica-id replica-id
              :old-lsn old-lsn
              :new-lsn lsn}))
    (let [m1 (assoc m0 replica-id {:applied-lsn lsn
                                   :updated-ms now-ms})]
      {:entries m1
       :ok? true
       :replica-id replica-id
       :applied-lsn lsn
       :updated-ms now-ms
       :replica-count (count m1)})))

(defn replica-floor-clear-plan
  [replica-id entries entries-k]
  (let [replica-id (ensure-floor-provider-id replica-id :replica)
        m0 (parse-floor-provider-map entries entries-k)
        existed? (contains? m0 replica-id)
        m1 (dissoc m0 replica-id)]
    {:entries m1
     :ok? true
     :replica-id replica-id
     :removed? existed?
     :replica-count (count m1)}))

(defn backup-pin-floor-update-plan
  [pin-id floor-lsn expires-ms now-ms entries entries-k]
  (let [pin-id (ensure-floor-provider-id pin-id :backup-pin)
        floor-lsn (parse-floor-lsn floor-lsn
                                   [:backup-pin pin-id :floor-lsn])
        expires-ms (parse-non-negative-long expires-ms
                                            [:backup-pin pin-id :expires-ms])
        now-ms (long now-ms)
        m0 (parse-floor-provider-map entries entries-k)
        pin (cond-> {:floor-lsn floor-lsn
                     :updated-ms now-ms}
              (some? expires-ms) (assoc :expires-ms expires-ms))
        m1 (assoc m0 pin-id pin)]
    {:entries m1
     :ok? true
     :pin-id pin-id
     :floor-lsn floor-lsn
     :expires-ms expires-ms
     :updated-ms now-ms
     :pin-count (count m1)}))

(defn backup-pin-floor-clear-plan
  [pin-id entries entries-k]
  (let [pin-id (ensure-floor-provider-id pin-id :backup-pin)
        m0 (parse-floor-provider-map entries entries-k)
        existed? (contains? m0 pin-id)
        m1 (dissoc m0 pin-id)]
    {:entries m1
     :ok? true
     :pin-id pin-id
     :removed? existed?
     :pin-count (count m1)}))

(defn min-floor-lsn
  [coll]
  (if-let [xs (seq coll)]
    (reduce min no-floor-lsn xs)
    no-floor-lsn))

(defn snapshot-floor-state
  ([current-raw previous-raw applied-lsn]
   (snapshot-floor-state current-raw previous-raw applied-lsn
                         :wal-snapshot-current-lsn
                         :wal-snapshot-previous-lsn))
  ([current-raw previous-raw applied-lsn current-floor-k previous-floor-k]
   (let [current-lsn (parse-optional-floor-lsn current-raw current-floor-k)
         previous-lsn (parse-optional-floor-lsn previous-raw previous-floor-k)
         [floor-lsn source]
         (cond
           (some? previous-lsn)
           [(safe-inc-lsn previous-lsn) :snapshot-previous]

           (some? current-lsn)
           [(safe-inc-lsn current-lsn) :snapshot-current]

           :else
           [(safe-inc-lsn applied-lsn) :applied-lsn])]
     {:floor-lsn floor-lsn
      :source source
      :snapshot-current-lsn current-lsn
      :snapshot-previous-lsn previous-lsn})))

(defn vector-domain-floor-state
  [domain meta]
  (let [previous-raw (or (:previous-snapshot-lsn meta)
                         (:vec-previous-snapshot-lsn meta))
        current-raw (or (:current-snapshot-lsn meta)
                        (:vec-current-snapshot-lsn meta))
        replay-raw (:vec-replay-floor-lsn meta)
        previous-lsn (parse-optional-floor-lsn previous-raw
                                               [:vec-meta domain
                                                :previous-snapshot-lsn])
        current-lsn (parse-optional-floor-lsn current-raw
                                              [:vec-meta domain
                                               :current-snapshot-lsn])
        replay-lsn (parse-optional-floor-lsn replay-raw
                                             [:vec-meta domain
                                              :vec-replay-floor-lsn])]
    (cond
      (some? previous-lsn)
      {:floor-lsn (safe-inc-lsn previous-lsn)
       :source :previous-snapshot
       :snapshot-lsn previous-lsn}

      (some? current-lsn)
      {:floor-lsn (safe-inc-lsn current-lsn)
       :source :current-snapshot
       :snapshot-lsn current-lsn}

      (some? replay-lsn)
      {:floor-lsn replay-lsn
       :source :replay-floor
       :replay-floor-lsn replay-lsn}

      :else
      {:floor-lsn no-floor-lsn
       :source :none})))

(defn vector-floor-state
  [meta-by-domain configured-domains legacy-floor-raw legacy-floor-k]
  (let [meta-by-domain (or meta-by-domain {})
        domains (into (set (keys meta-by-domain))
                      (or configured-domains #{}))
        domain-floors (into {}
                            (map (fn [domain]
                                   [domain
                                    (vector-domain-floor-state
                                     domain (get meta-by-domain domain {}))]))
                            domains)
        computed-floor (min-floor-lsn (map (comp :floor-lsn val)
                                           domain-floors))
        legacy-floor (parse-floor-lsn legacy-floor-raw legacy-floor-k)
        floor-lsn (min computed-floor legacy-floor)]
    {:floor-lsn floor-lsn
     :computed-floor-lsn computed-floor
     :legacy-floor-lsn legacy-floor
     :domain-count (count domains)
     :domains domain-floors}))

(defn replica-floor-state
  [entries ttl-ms now-ms legacy-floor-raw legacy-floor-k]
  (let [ttl-ms (long ttl-ms)
        entries (if (map? entries) entries {})
        parsed (->> entries
                    (keep
                     (fn [[replica-id state]]
                       (let [lsn-raw (if (map? state)
                                       (or (:applied-lsn state)
                                           (:ack-lsn state)
                                           (:lsn state)
                                           (:floor-lsn state))
                                       state)]
                         (when (some? lsn-raw)
                           (let [lsn (parse-floor-lsn
                                      lsn-raw
                                      [:replica replica-id :lsn])
                                 updated-ms (when (map? state)
                                              (parse-non-negative-long
                                               (or (:updated-ms state)
                                                   (:heartbeat-ms state))
                                               [:replica replica-id
                                                :updated-ms]))
                                 stale? (and (some? updated-ms)
                                             (pos? ttl-ms)
                                             (> (- now-ms updated-ms)
                                                ttl-ms))]
                             {:replica-id replica-id
                              :floor-lsn lsn
                              :updated-ms updated-ms
                              :stale? stale?}))))))
        active (->> parsed (remove :stale?) vec)
        computed-floor (min-floor-lsn (map :floor-lsn active))
        legacy-floor (parse-floor-lsn legacy-floor-raw legacy-floor-k)
        floor-lsn (min computed-floor legacy-floor)]
    {:floor-lsn floor-lsn
     :computed-floor-lsn computed-floor
     :legacy-floor-lsn legacy-floor
     :ttl-ms ttl-ms
     :active-count (count active)
     :stale-count (- (count parsed) (count active))
     :replicas parsed}))

(defn backup-pin-floor-state
  [entries now-ms legacy-floor-raw legacy-floor-k]
  (let [entries (if (map? entries) entries {})
        parsed (->> entries
                    (keep
                     (fn [[pin-id state]]
                       (let [lsn-raw (if (map? state)
                                       (or (:floor-lsn state)
                                           (:lsn state))
                                       state)]
                         (when (some? lsn-raw)
                           (let [floor-lsn (parse-floor-lsn
                                            lsn-raw
                                            [:backup-pin pin-id
                                             :floor-lsn])
                                 expires-ms (when (map? state)
                                              (parse-non-negative-long
                                               (:expires-ms state)
                                               [:backup-pin pin-id
                                                :expires-ms]))
                                 expired? (and (some? expires-ms)
                                               (< expires-ms now-ms))]
                             {:pin-id pin-id
                              :floor-lsn floor-lsn
                              :expires-ms expires-ms
                              :expired? expired?}))))))
        active (->> parsed (remove :expired?) vec)
        computed-floor (min-floor-lsn (map :floor-lsn active))
        legacy-floor (parse-floor-lsn legacy-floor-raw legacy-floor-k)
        floor-lsn (min computed-floor legacy-floor)]
    {:floor-lsn floor-lsn
     :computed-floor-lsn computed-floor
     :legacy-floor-lsn legacy-floor
     :active-count (count active)
     :expired-count (- (count parsed) (count active))
     :pins parsed}))

(defn annotate-gc-segments
  [segments active-segment-id newest-segment-id retention-ms gc-safety-watermark]
  (mapv
   (fn [segment]
     (let [segment-id (:segment-id segment)
           max-lsn (:max-lsn segment)
           active? (= segment-id active-segment-id)
           newest? (= segment-id newest-segment-id)
           age-exceeded? (>= ^long (:age-ms segment) retention-ms)
           safety-deletable? (and (some? max-lsn)
                                  (<= ^long max-lsn gc-safety-watermark)
                                  (not active?)
                                  (not newest?))]
       (assoc segment
              :active? active?
              :newest? newest?
              :age-exceeded? age-exceeded?
              :safety-deletable? safety-deletable?)))
   segments))

(defn select-gc-target-segments
  [segments total-bytes retention-bytes explicit-gc?]
  (let [safe-segments (->> segments (filter :safety-deletable?) vec)]
    (if explicit-gc?
      safe-segments
      (let [age-target-ids
            (into #{} (map :segment-id (filter :age-exceeded? safe-segments)))
            byte-target-ids
            (if (> total-bytes retention-bytes)
              (loop [remaining total-bytes
                     [segment & more] safe-segments
                     ids #{}]
                (if (and segment (> remaining retention-bytes))
                  (recur (- remaining ^long (:bytes segment))
                         more
                         (conj ids (:segment-id segment)))
                  ids))
              #{})
            target-ids (into age-target-ids byte-target-ids)]
        (->> safe-segments
             (filter #(contains? target-ids (:segment-id %)))
             vec)))))

(defn- segment-summary-from-cache-entry
  [entry now-ms]
  (let [created-ms (long (:created-ms entry))]
    {:segment-id (long (:segment-id entry))
     :path (:path entry)
     :bytes (long (:bytes entry))
     :created-ms created-ms
     :age-ms (max 0 (- now-ms created-ms))
     :record-count (long (:record-count entry))
     :min-lsn (some-> (:min-lsn entry) long)
     :max-lsn (some-> (:max-lsn entry) long)
     :partial-tail? (boolean (:partial-tail? entry))
     :preallocated-tail? (boolean (:preallocated-tail? entry))}))

(defn- scan-segment-summary-entry
  [segment-id ^File file allow-preallocated-tail? record->lsn marker-offset]
  (let [path (.getPath file)
        file-bytes (long (.length file))
        created-ms (long (.lastModified file))
        record-count-v (volatile! 0)
        first-record-v (volatile! nil)
        last-record-v (volatile! nil)
        marker-rec-v (volatile! nil)
        on-record (fn [record]
                    (vreset! record-count-v (inc (long @record-count-v)))
                    (when (nil? @first-record-v)
                      (vreset! first-record-v record))
                    (vreset! last-record-v record)
                    (when (and (some? marker-offset)
                               (= (long marker-offset)
                                  (long (:offset record))))
                      (vreset! marker-rec-v record)))
        scan (scan-segment
              path
              {:allow-preallocated-tail? allow-preallocated-tail?
               :collect-records? false
               :on-record on-record})
        bytes (if (:partial-tail? scan)
                (long (:valid-end scan))
                file-bytes)
        first-record @first-record-v
        last-record @last-record-v
        marker-record* @marker-rec-v
        min-lsn (some-> first-record record->lsn long)
        max-lsn (some-> last-record record->lsn long)
        marker-rec (when marker-record*
                     {:segment-id segment-id
                      :offset (long (:offset marker-record*))
                      :checksum (long (:checksum marker-record*))
                      :lsn (some-> marker-record* record->lsn long)})]
    {:entry {:segment-id segment-id
             :path path
             :file-bytes file-bytes
             :created-ms created-ms
             :bytes bytes
             :record-count (long @record-count-v)
             :min-lsn min-lsn
             :max-lsn max-lsn
             :partial-tail? (boolean (:partial-tail? scan))
             :preallocated-tail? (boolean (:preallocated-tail? scan))
             :marker-resolved? (some? marker-offset)
             :marker-offset marker-offset
             :marker-record marker-rec}
     :marker-record marker-rec}))

(defn segment-summaries
  [^String dir
   {:keys [allow-preallocated-tail?
           record->lsn
           marker-segment-id
           marker-offset
           active-segment-id
           active-segment-offset
           cache-v
           cache-key
           min-retained-fallback]
    :or {allow-preallocated-tail? true
         record->lsn #(or (:lsn %) nil)}}]
  (let [now-ms (System/currentTimeMillis)
        segments (segment-files dir)
        marker-seg-id (some-> marker-segment-id long)
        active-seg-id (some-> active-segment-id long)
        active-offset (some-> active-segment-offset long)
        cache? (and (some? cache-v) (some? cache-key))
        bucket-k (when cache? [cache-key allow-preallocated-tail?])
        cache-root0 (if cache? @cache-v {})
        cache0 (if cache? (get cache-root0 bucket-k {}) {})
        [summaries marker-record cache1]
        (reduce
         (fn [[acc marker cache*] {:keys [id file]}]
           (let [segment-id (long id)
                 file ^File file
                 path (.getPath file)
                 marker-offset* (when (and (some? marker-seg-id)
                                           (some? marker-offset)
                                           (= segment-id marker-seg-id))
                                  (long marker-offset))
                 cached-entry (get cache0 segment-id)
                 path-ok? (and cached-entry
                               (= path (:path cached-entry)))
                 marker-ok? (or (nil? marker-offset*)
                                (and cached-entry
                                     (:marker-resolved? cached-entry)
                                     (= marker-offset*
                                        (:marker-offset
                                         cached-entry))))
                 active-id-known? (some? active-seg-id)
                 active-segment? (and active-id-known?
                                      (= segment-id active-seg-id))
                 reuse-closed? (and path-ok?
                                    marker-ok?
                                    active-id-known?
                                    (not active-segment?))
                 reuse-active-by-offset?
                 (and path-ok?
                      marker-ok?
                      active-segment?
                      (some? active-offset)
                      (= active-offset
                         (long (or (:bytes cached-entry) -1))))
                 file-bytes* (delay (long (.length file)))
                 created-ms* (delay (long (.lastModified file)))
                 reuse-by-metadata?
                 (and path-ok?
                      marker-ok?
                      (= @file-bytes*
                         (long (:file-bytes cached-entry)))
                      (= @created-ms*
                         (long (:created-ms cached-entry))))
                 reusable-entry? (or reuse-closed?
                                     reuse-active-by-offset?
                                     reuse-by-metadata?)
                 {:keys [entry marker-record]}
                 (if reusable-entry?
                   {:entry cached-entry
                    :marker-record
                    (when (some? marker-offset*)
                      (:marker-record cached-entry))}
                   (scan-segment-summary-entry segment-id
                                               file
                                               allow-preallocated-tail?
                                               record->lsn
                                               marker-offset*))]
             [(conj acc (segment-summary-from-cache-entry entry now-ms))
              (or marker marker-record)
              (assoc cache* segment-id entry)]))
         [[] nil {}]
         segments)
        _ (when cache?
            (let [cache-root1 (if (seq cache1)
                                (assoc cache-root0 bucket-k cache1)
                                (dissoc cache-root0 bucket-k))]
              (when-not (= cache-root0 cache-root1)
                (vreset! cache-v cache-root1))))
        min-retained (or (some :min-lsn summaries) min-retained-fallback)
        total-bytes (reduce (fn [acc {:keys [bytes]}] (+ ^long acc ^long bytes))
                            0 summaries)
        newest-id (some-> (peek summaries) :segment-id)]
    {:segments summaries
     :marker-record marker-record
     :min-retained-lsn (some-> min-retained long)
     :total-bytes total-bytes
     :newest-segment-id (some-> newest-id long)}))

(defn retention-state
  [{:keys [segments
           total-bytes
           retention-bytes
           retention-ms
           active-segment-id
           newest-segment-id
           floors
           explicit-gc?]}]
  (let [required-floor-lsn (min-floor-lsn (vals floors))
        gc-safety-watermark (dec ^long required-floor-lsn)
        segments* (annotate-gc-segments
                   segments active-segment-id newest-segment-id
                   retention-ms gc-safety-watermark)
        safety-deletable (->> segments* (filter :safety-deletable?) vec)
        safety-bytes (reduce (fn [acc {:keys [bytes]}]
                               (+ ^long acc ^long bytes))
                             0 safety-deletable)
        bytes-exceeded? (> total-bytes retention-bytes)
        age-exceeded? (boolean
                       (some (fn [{:keys [active? age-exceeded?]}]
                               (and (not active?) age-exceeded?))
                             segments*))
        pressure? (or bytes-exceeded? age-exceeded?)
        gc-targets (select-gc-target-segments
                    segments* total-bytes retention-bytes explicit-gc?)
        gc-target-bytes (reduce (fn [acc {:keys [bytes]}]
                                  (+ ^long acc ^long bytes))
                                0 gc-targets)
        retained-bytes-after (- ^long total-bytes ^long gc-target-bytes)
        floor-limiters (->> floors
                            (filter (fn [[_ floor]]
                                      (= ^long floor ^long required-floor-lsn)))
                            (map key)
                            sort
                            vec)
        degraded? (and pressure? (empty? gc-targets))]
    {:segments segments*
     :required-retained-floor-lsn required-floor-lsn
     :gc-safety-watermark-lsn gc-safety-watermark
     :floor-limiters floor-limiters
     :safety-deletable-segment-count (count safety-deletable)
     :safety-deletable-segment-bytes safety-bytes
     :pressure {:bytes-exceeded? bytes-exceeded?
                :age-exceeded? age-exceeded?
                :pressure? pressure?
                :degraded? degraded?}
     :gc-target-segments gc-targets
     :gc-target-segment-ids (mapv :segment-id gc-targets)
     :gc-target-segment-count (count gc-targets)
     :gc-target-segment-bytes gc-target-bytes
     :retained-bytes-after-gc retained-bytes-after
     :explicit-gc? explicit-gc?}))

(defn valid-commit-marker
  [marker marker-record]
  (when (and marker marker-record
             (= (long (:applied-lsn marker))
                (long (:lsn marker-record)))
             (= (long (:txlog-record-crc marker))
                (long (:checksum marker-record))))
    marker))

(defn commit-marker-key-for-revision
  [revision]
  (if (zero? (bit-and (long revision) 0x1))
    c/wal-marker-a
    c/wal-marker-b))

(defn newer-commit-marker
  [slot-a slot-b]
  (cond
    (and slot-a slot-b)
    (if (>= ^long (:revision slot-a) ^long (:revision slot-b))
      slot-a
      slot-b)

    slot-a slot-a
    slot-b slot-b
    :else nil))

(defn marker-record-by-reference
  [marker records]
  (when marker
    (let [segment-id (long (:txlog-segment-id marker))
          offset (long (:txlog-record-offset marker))]
      (some (fn [r]
              (when (and (= segment-id (long (:segment-id r)))
                         (= offset (long (:offset r))))
                r))
            records))))

(defn validate-commit-marker-reference
  [marker records]
  (valid-commit-marker marker (marker-record-by-reference marker records)))

(defn resolve-applied-lsn
  [{:keys [commit-marker?
           marker
           marker-record
           has-records?
           meta-last-applied-lsn
           min-retained-lsn]}]
  (let [valid-marker (valid-commit-marker marker marker-record)
        applied-lsn (if commit-marker?
                      (long (or (:applied-lsn valid-marker) 0))
                      (long meta-last-applied-lsn))
        min-retained-lsn (long (or min-retained-lsn 0))]
    (when (and commit-marker? has-records? (nil? valid-marker))
      (raise
       "Commit marker is missing or invalid for existing txn-log records"
       {:type :txlog/recovery-marker-invalid}))
    (when (> min-retained-lsn (safe-inc-lsn applied-lsn))
      (raise
       "Retained txn-log floor exceeds recovery cursor coverage"
       {:type :txlog/recovery-floor-gap}
       :min-retained-lsn min-retained-lsn
       :applied-lsn applied-lsn))
    {:valid-marker valid-marker
     :applied-lsn applied-lsn}))

(defn recovery-state
  [{:keys [commit-marker?
           marker-state
           records
           meta-last-applied-lsn
           next-lsn]}]
  (let [marker (:current marker-state)
        marker-record (marker-record-by-reference marker records)
        has-records? (boolean (seq records))
        min-retained (if (seq records)
                       (long (:lsn (first records)))
                       (long (or next-lsn 0)))
        {:keys [valid-marker applied-lsn]}
        (resolve-applied-lsn
         {:commit-marker? commit-marker?
          :marker marker
          :marker-record marker-record
          :has-records? has-records?
          :meta-last-applied-lsn meta-last-applied-lsn
          :min-retained-lsn min-retained})]
    {:marker marker
     :marker-record marker-record
     :has-records? has-records?
     :min-retained-lsn min-retained
     :applied-lsn (long applied-lsn)
     :valid-marker valid-marker}))

(defn select-open-records
  [records from-lsn upto-lsn]
  (let [from (max 0 (long (or from-lsn 0)))
        upto (when (some? upto-lsn) (long upto-lsn))]
    (when (and upto (< upto from))
      (raise "Invalid txlog range: upto-lsn is smaller than from-lsn"
             {:type :txlog/invalid-range
              :from-lsn from
              :upto-lsn upto}))
    (->> records
         (filter #(>= (long (:lsn %)) from))
         (filter #(if upto
                    (<= (long (:lsn %)) upto)
                    true))
         (mapv (fn [{:keys [rows] :as r}]
                 (-> r
                     (dissoc :rows :path)
                     (assoc :ops rows)))))))

(defn retention-floors
  [{:keys [snapshot-state
           vector-state
           replica-state
           backup-state
           operator-retain-floor-lsn]}]
  {:snapshot-floor-lsn (:floor-lsn snapshot-state)
   :vector-global-floor-lsn (:floor-lsn vector-state)
   :replica-floor-lsn (:floor-lsn replica-state)
   :backup-pin-floor-lsn (:floor-lsn backup-state)
   :operator-retain-floor-lsn
   (parse-floor-lsn operator-retain-floor-lsn :operator-retain-floor-lsn)})

(defn retention-state-report
  [{:keys [dir
           retention-bytes
           retention-ms
           segments
           total-bytes
           active-segment-id
           newest-segment-id
           min-retained-lsn
           applied-lsn
           marker-state
           valid-marker
           floors
           floor-providers
           explicit-gc?]}]
  (let [core-state (retention-state
                    {:segments segments
                     :total-bytes total-bytes
                     :retention-bytes retention-bytes
                     :retention-ms retention-ms
                     :active-segment-id active-segment-id
                     :newest-segment-id newest-segment-id
                     :floors floors
                     :explicit-gc? explicit-gc?})]
    {:wal? true
     :dir dir
     :retention-bytes retention-bytes
     :retention-ms retention-ms
     :segment-count (count (:segments core-state))
     :segment-bytes total-bytes
     :segments (:segments core-state)
     :active-segment-id active-segment-id
     :newest-segment-id newest-segment-id
     :min-retained-lsn min-retained-lsn
     :applied-lsn applied-lsn
     :commit-marker marker-state
     :valid-marker valid-marker
     :floors floors
     :floor-providers floor-providers
     :required-retained-floor-lsn
     (:required-retained-floor-lsn core-state)
     :gc-safety-watermark-lsn
     (:gc-safety-watermark-lsn core-state)
     :floor-limiters (:floor-limiters core-state)
     :safety-deletable-segment-count
     (:safety-deletable-segment-count core-state)
     :safety-deletable-segment-bytes
     (:safety-deletable-segment-bytes core-state)
     :pressure (:pressure core-state)
     :gc-target-segments (:gc-target-segments core-state)
     :gc-target-segment-ids (:gc-target-segment-ids core-state)
     :gc-target-segment-count (:gc-target-segment-count core-state)
     :gc-target-segment-bytes (:gc-target-segment-bytes core-state)
     :retained-bytes-after-gc (:retained-bytes-after-gc core-state)
     :explicit-gc? (:explicit-gc? core-state)}))

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
  (let [payload (doto (ByteBuffer/allocate meta-slot-payload-size)
                  (.order ByteOrder/BIG_ENDIAN))]
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
          slot (doto (ByteBuffer/allocate meta-slot-size)
                 (.order ByteOrder/BIG_ENDIAN))]
      (.put slot payload-bytes)
      (put-u32 slot (crc32c payload-bytes))
      (.array slot))))

(defn decode-meta-slot-bytes
  [^bytes slot-bytes]
  (when-not (= (alength slot-bytes) meta-slot-size)
    (raise "Invalid txn-log meta slot size"
           {:size (alength slot-bytes) :expected meta-slot-size}))
  (let [payload (Arrays/copyOfRange slot-bytes 0 meta-slot-payload-size)
        checksum-buf (doto (ByteBuffer/wrap slot-bytes
                                            meta-slot-payload-size
                                            4)
                       (.order ByteOrder/BIG_ENDIAN))
        expected-check (long (read-int-unsigned checksum-buf))
        actual-check (long (bit-and 0xffffffff (crc32c payload)))]
    (when-not (= expected-check actual-check)
      (raise "Txn-log meta slot checksum mismatch"
             {:expected expected-check :actual actual-check}))
    (let [bf (doto (ByteBuffer/wrap payload) (.order ByteOrder/BIG_ENDIAN))
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

(defn- read-meta-slot-at
  [^FileChannel ch ^long offset]
  (let [size (.size ch)]
    (when (<= (+ offset meta-slot-size) size)
      (let [^ByteBuffer slot-bf (bf/get-array-buffer meta-slot-size)]
        (try
          (.clear slot-bf)
          (.limit slot-bf meta-slot-size)
          (read-fully-at! ch offset slot-bf)
          (.flip slot-bf)
          (let [slot-bytes (byte-array meta-slot-size)]
            (.get slot-bf slot-bytes)
            (decode-meta-slot-bytes slot-bytes))
          (catch Exception _
            nil)
          (finally
            (bf/return-array-buffer slot-bf)))))))

(defn read-meta-file
  [^String path]
  (let [f (io/file path)]
    (when (.exists f)
      (with-open [^FileChannel ch (FileChannel/open
                                   (.toPath f)
                                   (into-array StandardOpenOption
                                               [StandardOpenOption/READ]))]
        (let [slot-a (read-meta-slot-at ch 0)
              slot-b (read-meta-slot-at ch meta-slot-size)
              current (newer-commit-marker slot-a slot-b)]
          (when (or slot-a slot-b)
            {:slot-a slot-a
             :slot-b slot-b
             :current current}))))))

(defn write-meta-file!
  ([^String path meta]
   (write-meta-file! path meta {}))
  ([^String path meta {:keys [sync-mode]
                       :or {sync-mode :fdatasync}}]
   (let [revision (long
                   (if-some [rev (:revision meta)]
                     rev
                     (let [existing (read-meta-file path)
                           prev-revision
                           (long (or (get-in existing [:current :revision]) -1))]
                       (inc prev-revision))))
         slot-index (int (bit-and revision 0x1))
         slot-offset (long (* slot-index meta-slot-size))
         payload (assoc meta :revision revision)
         slot-bytes (encode-meta-slot payload)
         f (io/file path)]
     (when-let [^File parent (.getParentFile f)]
       (u/create-dirs (.getPath parent)))
     (with-open [^FileChannel ch (FileChannel/open
                                  (.toPath f)
                                  (into-array StandardOpenOption
                                              [StandardOpenOption/CREATE
                                               StandardOpenOption/READ
                                               StandardOpenOption/WRITE]))]
       (.position ch slot-offset)
       (write-fully! ch (ByteBuffer/wrap slot-bytes))
       (force-channel! ch sync-mode))
     (assoc payload :slot (if (zero? slot-index) :a :b)))))

(declare sync-manager-state
         sync-manager-pending?
         record-fsync-ms!
         record-commit-wait-ms!
         request-sync-now!)

(defn publish-meta-best-effort!
  [state {:keys [lsn segment-id offset]}]
  (try
    (let [sync-state (sync-manager-state (:sync-manager state))
          meta-revision-v (:meta-revision state)
          revision (when meta-revision-v
                     (long (vswap! meta-revision-v inc)))]
      (write-meta-file!
       (:meta-path state)
       (cond-> {:last-committed-lsn lsn
                :last-durable-lsn (:last-durable-lsn sync-state)
                :last-applied-lsn lsn
                :segment-id segment-id
                :segment-offset offset
                :updated-ms (System/currentTimeMillis)}
         (some? revision) (assoc :revision revision))
       {:sync-mode :none}))
    (catch Exception _)))

(defn maybe-publish-meta-best-effort!
  ([state append-res]
   (maybe-publish-meta-best-effort! state append-res publish-meta-best-effort!))
  ([state append-res publish-fn]
   (let [publish? (locking (or (:meta-publish-lock state) state)
                    (let [pending-v (:meta-pending-txs state)
                          last-flush-v (:meta-last-flush-ms state)
                          now-ms (System/currentTimeMillis)
                          flush-txs (max 1 (long (or (:meta-flush-max-txs state)
                                                     c/*wal-meta-flush-max-txs*)))
                          flush-ms (max 0 (long (or (:meta-flush-max-ms state)
                                                    c/*wal-meta-flush-max-ms*)))
                          pending (if pending-v
                                    (let [v (inc (long @pending-v))]
                                      (vreset! pending-v v)
                                      v)
                                    1)
                          last-flush (if last-flush-v
                                       (long @last-flush-v)
                                       0)
                          due? (or (>= pending flush-txs)
                                   (>= (- now-ms last-flush) flush-ms))]
                      (when due?
                        ;; Reserve this flush while under lock, then do I/O outside lock.
                        (when pending-v
                          (vreset! pending-v 0))
                        (when last-flush-v
                          (vreset! last-flush-v now-ms))
                        true)))]
     (when publish?
       (publish-fn state append-res)))))

(defn note-gc-deleted-bytes!
  [state deleted-bytes]
  (when-let [total-bytes-v (:retention-total-bytes state)]
    (let [remaining (- ^long @total-bytes-v ^long (max 0 (long deleted-bytes)))]
      (vreset! total-bytes-v (max 0 remaining)))))

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
  (let [payload (doto (ByteBuffer/allocate commit-marker-slot-payload-size)
                  (.order ByteOrder/BIG_ENDIAN))]
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
          slot (doto (ByteBuffer/allocate commit-marker-slot-size)
                 (.order ByteOrder/BIG_ENDIAN))]
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
        checksum-buf (doto (ByteBuffer/wrap slot-bytes
                                            commit-marker-slot-payload-size
                                            4)
                       (.order ByteOrder/BIG_ENDIAN))
        expected-check (long (read-int-unsigned checksum-buf))
        actual-check (long (bit-and 0xffffffff (crc32c payload)))]
    (when-not (= expected-check actual-check)
      (raise "Txn-log commit marker slot checksum mismatch"
             {:expected expected-check :actual actual-check}))
    (let [bf (doto (ByteBuffer/wrap payload) (.order ByteOrder/BIG_ENDIAN))
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

(defn next-commit-marker-entry
  [commit-state append-info]
  (when (:commit-marker? commit-state)
    (let [revision (inc (long (:marker-revision commit-state)))
          marker {:revision revision
                  :applied-lsn (long (:lsn append-info))
                  :txlog-segment-id (long (:segment-id append-info))
                  :txlog-record-offset (long (:offset append-info))
                  :txlog-record-crc (long (or (:checksum append-info) 0))
                  :updated-ms (long (or (:now-ms append-info)
                                        (System/currentTimeMillis)))}
          slot (encode-commit-marker-slot marker)
          row [:put c/kv-info
               (commit-marker-key-for-revision revision)
               slot :keyword :bytes]]
      {:revision revision
       :marker marker
       :row row})))

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

(def ^:private commit-payload-format-major 1)
(def ^:private commit-payload-magic-bytes
  (byte-array [(byte 0x44) (byte 0x4c) (byte 0x54) (byte 0x58)])) ; DLTX
(def ^:private commit-payload-lsn-offset 8)
(def ^:private commit-payload-tx-time-offset 16)

(def ^:const op-kv-put (byte 0x10))
(def ^:const op-kv-del (byte 0x11))
(def ^:const op-kv-put-list (byte 0x16))
(def ^:const op-kv-del-list (byte 0x17))

(def ^:const txlog-type-tuple-tag (byte 0x00))

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

(def ^:private ^ThreadLocal tl-commit-body-buffer
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_] (ByteBuffer/allocate tl-commit-body-buffer-initial-cap)))))

(def ^:private ^ThreadLocal tl-bits-buffer
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_] (ByteBuffer/allocate tl-bits-buffer-initial-cap)))))

(def ^:private ^ThreadLocal tl-row-encode-buffer
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_] (ByteBuffer/allocate tl-row-encode-buffer-initial-cap)))))

(def ^:private ^ThreadLocal tl-dbi-name-cache
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_] (HashMap. 8)))))

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
        used (long (max 0 used-bytes))]
    (when (and (>= cap tl-buffer-shrink-trigger-cap)
               (<= used tl-buffer-max-retained-cap))
      (let [new-cap (next-capacity-at-least initial-cap (max 1 used))]
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
  (let [bf (ensure-room! bf tl 1)]
    (.put bf (byte v))
    bf))

(defn- bb-put-u16!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^long v]
  (when (or (neg? v) (> v 0xffff))
    (raise "Txn-log u16 field out of range"
           {:value v :type :txlog/corrupt}))
  (let [bf (ensure-room! bf tl Short/BYTES)]
    (.putShort bf (short v))
    bf))

(defn- bb-put-u32!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^long v]
  (when (or (neg? v) (> v 0xffffffff))
    (raise "Txn-log u32 field out of range"
           {:value v :type :txlog/corrupt}))
  (let [bf (ensure-room! bf tl Integer/BYTES)]
    (.putInt bf (unchecked-int v))
    bf))

(defn- bb-put-long!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^long v]
  (let [bf (ensure-room! bf tl Long/BYTES)]
    (.putLong bf v)
    bf))

(defn- bb-put-bytes!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^bytes bs]
  (let [len (alength bs)
        bf (ensure-room! bf tl len)]
    (.put bf bs 0 len)
    bf))

(defn- bb-put-buffer!
  ^ByteBuffer [^ByteBuffer bf ^ThreadLocal tl ^ByteBuffer src]
  (let [len (.remaining src)
        bf (ensure-room! bf tl len)]
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

(defn- body-slice-buffer
  ^ByteBuffer [^bytes body ^long offset ^long len]
  (.slice ^ByteBuffer (ByteBuffer/wrap body (int offset) (int len))))

(deftype ^:private LMDBRowSpan [op
                                ^String dbi-name
                                v
                                vt
                                flags
                                ^long k-offset
                                ^long k-len
                                ^long v-offset
                                ^long v-len])

(defn- lmdb-row-from-span
  [^LMDBRowSpan span k-raw v-raw]
  (let [op (.-op span)
        ^String dbi-name (.-dbi-name span)
        v (.-v span)
        vt (.-vt span)
        flags (.-flags span)]
    (case op
      :put
      (datalevin.lmdb.KVTxData. op dbi-name k-raw v-raw :raw :raw flags)

      :del
      (datalevin.lmdb.KVTxData. op dbi-name k-raw nil :raw nil flags)

      ;; List ops still need per-element LMDB processing, but key encoding can be reused.
      :put-list
      (datalevin.lmdb.KVTxData. op dbi-name k-raw v :raw (or vt :data) flags)

      :del-list
      (datalevin.lmdb.KVTxData. op dbi-name k-raw v :raw (or vt :data) flags)

      (raise "Unsupported txn-log KV op"
             {:type :txlog/corrupt
              :op op}))))

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
               ^HashMap dbi-cache
               ^FastList lmdb-row-spans]
  (let [^String dbi-name (dbi-name->string dbi)
        ^bytes dbi-bs (dbi-name-bytes dbi-name dbi-cache)
        dbi-len (alength dbi-bs)
        k-type (or kt :data)
        ^ByteBuffer k-bf (encode-to-buf k k-type)
        k-len (.remaining k-bf)]
    (case op
      :put
      (let [bf1 (-> bf
                    (bb-put-byte! tl (int op-kv-put))
                    (bb-put-u16! tl dbi-len)
                    (bb-put-bytes! tl dbi-bs)
                    (bb-write-type! tl k-type)
                    (bb-put-u16! tl k-len))
            k-offset (.position bf1)
            bf2 (bb-put-buffer! bf1 tl k-bf)
            v-type (or vt :data)
            ^ByteBuffer v-bf (encode-to-buf v v-type)
            v-len (.remaining v-bf)
            bf3 (-> bf2
                    (bb-write-type! tl v-type)
                    (bb-put-u32! tl v-len))
            v-offset (.position bf3)
            bf' (-> bf3
                    (bb-put-buffer! tl v-bf)
                    (bb-write-flags! tl flags))]
        (when lmdb-row-spans
          (.add lmdb-row-spans
                (LMDBRowSpan. op
                              dbi-name
                              nil
                              nil
                              flags
                              (long k-offset)
                              (long k-len)
                              (long v-offset)
                              (long v-len))))
        bf')

      :del
      (let [bf1 (-> bf
                    (bb-put-byte! tl (int op-kv-del))
                    (bb-put-u16! tl dbi-len)
                    (bb-put-bytes! tl dbi-bs)
                    (bb-write-type! tl k-type)
                    (bb-put-u16! tl k-len))
            k-offset (.position bf1)
            bf' (-> bf1
                    (bb-put-buffer! tl k-bf)
                    (bb-write-flags! tl flags))]
        (when lmdb-row-spans
          (.add lmdb-row-spans
                (LMDBRowSpan. op
                              dbi-name
                              nil
                              nil
                              flags
                              (long k-offset)
                              (long k-len)
                              -1
                              -1)))
        bf')

      :put-list
      (let [bf1 (-> bf
                    (bb-put-byte! tl (int op-kv-put-list))
                    (bb-put-u16! tl dbi-len)
                    (bb-put-bytes! tl dbi-bs)
                    (bb-write-type! tl k-type)
                    (bb-put-u16! tl k-len))
            k-offset (.position bf1)
            bf2 (bb-put-buffer! bf1 tl k-bf)
            v-type (or vt :data)
            ;; Keep list payload as :data for compact, fast decode.
            ^ByteBuffer v-bf (encode-to-buf v :data)
            v-len (.remaining v-bf)
            bf' (-> bf2
                    (bb-write-type! tl v-type)
                    (bb-put-u32! tl v-len)
                    (bb-put-buffer! tl v-bf)
                    (bb-write-flags! tl flags))]
        (when lmdb-row-spans
          (.add lmdb-row-spans
                (LMDBRowSpan. op
                              dbi-name
                              v
                              v-type
                              flags
                              (long k-offset)
                              (long k-len)
                              -1
                              -1)))
        bf')

      :del-list
      (let [bf1 (-> bf
                    (bb-put-byte! tl (int op-kv-del-list))
                    (bb-put-u16! tl dbi-len)
                    (bb-put-bytes! tl dbi-bs)
                    (bb-write-type! tl k-type)
                    (bb-put-u16! tl k-len))
            k-offset (.position bf1)
            bf2 (bb-put-buffer! bf1 tl k-bf)
            v-type (or vt :data)
            ;; Keep list payload as :data for compact, fast decode.
            ^ByteBuffer v-bf (encode-to-buf v :data)
            v-len (.remaining v-bf)
            bf' (-> bf2
                    (bb-write-type! tl v-type)
                    (bb-put-u32! tl v-len)
                    (bb-put-buffer! tl v-bf)
                    (bb-write-flags! tl flags))]
        (when lmdb-row-spans
          (.add lmdb-row-spans
                (LMDBRowSpan. op
                              dbi-name
                              v
                              v-type
                              flags
                              (long k-offset)
                              (long k-len)
                              -1
                              -1)))
        bf')

      (raise "Unsupported txn-log KV op"
             {:type :txlog/corrupt
              :op op
              :row row}))))

(defn- write-kv-row!
  ([^ThreadLocal tl ^ByteBuffer bf row]
   (write-kv-row! tl bf row nil nil))
  ([^ThreadLocal tl ^ByteBuffer bf row ^HashMap dbi-cache]
   (write-kv-row! tl bf row dbi-cache nil))
  ([^ThreadLocal tl
    ^ByteBuffer bf
    row
    ^HashMap dbi-cache
    ^FastList lmdb-row-spans]
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
                             dbi-cache
                             lmdb-row-spans))

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
                           dbi-cache
                           lmdb-row-spans)

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
                             dbi-cache
                             lmdb-row-spans))

     :else
     (raise "Txn-log row must be a vector or KVTxData"
            {:type :txlog/corrupt
             :row row}))))

(defn- lmdb-rows-from-spans
  ^FastList [^bytes body ^FastList lmdb-row-spans]
  (let [n (.size lmdb-row-spans)
        ^FastList lmdb-rows (FastList. n)]
    (dotimes [i n]
      (let [^LMDBRowSpan span (.get lmdb-row-spans i)
            k-offset (long (.-k-offset span))
            k-len (long (.-k-len span))
            v-offset (long (.-v-offset span))
            v-len (long (.-v-len span))
            k-raw (body-slice-buffer body k-offset k-len)
            v-raw (when (>= v-offset 0)
                    (body-slice-buffer body v-offset v-len))]
        (.add lmdb-rows (lmdb-row-from-span span k-raw v-raw))))
    lmdb-rows))

(defn- use-parallel-row-encoding?
  [^long row-count]
  (and (>= row-count (long parallel-row-encode-min-rows))
       (> (.availableProcessors (Runtime/getRuntime)) 1)))

(defn- parallel-row-encode-chunk-size
  ^long [^long row-count]
  (let [cores (long (max 1 (.availableProcessors (Runtime/getRuntime))))
        target-task-count (long (max 1
                                    (* parallel-row-encode-target-tasks-per-core
                                       cores)))
        ceil-size (long (quot (+ row-count (dec target-task-count))
                              target-task-count))]
    (long (max parallel-row-encode-min-chunk-size ceil-size))))

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
    ;; Chunk encoding runs on worker threads, so keep worker-local bits
    ;; buffers bounded.
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
  [lsn tx-time rows]
  (let [^FastList rowsv (ensure-fast-list rows)
        row-count (long (.size rowsv))
        parallel-rows? (use-parallel-row-encoding? row-count)
        ^ThreadLocal tl tl-commit-body-buffer
        ^ByteBuffer bf0 (.clear ^ByteBuffer (.get tl))
        ^HashMap dbi-cache (when-not parallel-rows?
                             (.get tl-dbi-name-cache))
        _ (when dbi-cache (.clear dbi-cache))
        ^ByteBuffer bf1 (-> bf0
                            (bb-put-bytes! tl commit-payload-magic-bytes)
                            (bb-put-byte! tl commit-payload-format-major)
                            (bb-put-byte! tl 0)
                            (bb-put-u16! tl 0)
                            (bb-put-long! tl (long lsn))
                            (bb-put-long! tl (long tx-time))
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
    out))

(defn- encode-commit-row-payload+lmdb-rows
  [lsn tx-time rows]
  (let [^FastList rowsv (ensure-fast-list rows)
        row-count (long (.size rowsv))
        ^ThreadLocal tl tl-commit-body-buffer
        ^ByteBuffer bf0 (.clear ^ByteBuffer (.get tl))
        ^HashMap dbi-cache (.get tl-dbi-name-cache)
        _ (.clear dbi-cache)
        ^FastList lmdb-row-spans (FastList. (.size rowsv))
        ^ByteBuffer bfN
        (loop [i 0
               ^ByteBuffer bf (-> bf0
                                  (bb-put-bytes! tl commit-payload-magic-bytes)
                                  (bb-put-byte! tl commit-payload-format-major)
                                  (bb-put-byte! tl 0)
                                  (bb-put-u16! tl 0)
                                  (bb-put-long! tl (long lsn))
                                  (bb-put-long! tl (long tx-time))
                                  (bb-put-u32! tl row-count))]
          (if (< i row-count)
            (recur (unchecked-inc-int i)
                   (write-kv-row! tl bf (.get rowsv i) dbi-cache lmdb-row-spans))
            bf))
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
    {:body out
     :lmdb-rows (lmdb-rows-from-spans out lmdb-row-spans)}))

(defn- patch-commit-row-payload-header!
  [^bytes body ^long lsn ^long tx-time]
  (let [^ByteBuffer bf (ByteBuffer/wrap body)]
    (.position bf (int commit-payload-lsn-offset))
    (.putLong bf (long lsn))
    (.position bf (int commit-payload-tx-time-offset))
    (.putLong bf (long tx-time)))
  body)

(defn decode-commit-row-payload
  "Decode raw binary txn-log payload bytes."
  [^bytes body]
  (let [bf (ByteBuffer/wrap body)
        ^bytes magic (bb-get-bytes bf 4 {:field :magic})
        major (bb-get-u8 bf {:field :major})
        _flags (bb-get-u8 bf {:field :flags})
        _reserved (bb-get-u16 bf {:field :reserved})
        lsn (bb-get-long bf {:field :lsn})
        tx-time (bb-get-long bf {:field :tx-time})
        op-count (bb-get-u32 bf {:field :op-count})]
    (when-not (Arrays/equals magic ^bytes commit-payload-magic-bytes)
      (raise "Invalid txn-log payload magic"
             {:type :txlog/corrupt}))
    (when-not (= major commit-payload-format-major)
      (raise "Unsupported txn-log payload format major"
             {:type :txlog/corrupt
              :major major
              :expected commit-payload-format-major}))
    (when (> op-count Integer/MAX_VALUE)
      (raise "Txn-log payload op count overflow"
             {:type :txlog/corrupt
              :op-count op-count}))
    (let [ops (loop [i (int 0)
                     acc []]
                (if (< i ^long op-count)
                  (recur (unchecked-inc-int i) (conj acc (decode-kv-row bf)))
                  acc))]
      {:lsn lsn
       :ts tx-time
       :ops ops})))

(defn prepare-commit-rows
  [commit-state append-info rows]
  (let [^FastList rows0 (ensure-fast-list rows)
        applied-prefix-count (.size rows0)
        marker-entry (next-commit-marker-entry commit-state append-info)]
    (when marker-entry
      (.add rows0 (:row marker-entry)))
    {:rows rows0
     :applied-prefix-count applied-prefix-count
     :marker-entry marker-entry}))

(defn state
  [db]
  (some-> (i/kv-info db) deref :txlog-state))

(defn enabled-state
  [db]
  (or (state db)
      (raise "Txn-log is not enabled for this LMDB"
             {:type :txlog/not-enabled})))

(defn- append-record-under-lock!
  [state prepared-payload throw-if-fatal!]
  (when throw-if-fatal!
    (throw-if-fatal! state))
  (let [lsn-v (:next-lsn state)
        lsn (long @lsn-v)
        now (System/currentTimeMillis)
        sync-manager (:sync-manager state)
        _ (when-not sync-manager
            (raise "Txn-log sync manager is not available"
                   {:type :txlog/no-sync-manager}))
        ^long sid @(:segment-id state)
        ^FileChannel ch @(:segment-channel state)
        _ (when-not ch
            (raise "Txn-log segment channel is not available"
                   {:type :txlog/no-segment-channel}))
        segment-offset (:segment-offset state)
        offset (long (if segment-offset
                       @segment-offset
                       (.size ch)))
        append-start-ms now
        near-roll? (near-roll-append? state offset)
        ^bytes body (:body prepared-payload)
        lmdb-rows (:lmdb-rows prepared-payload)
        _ (patch-commit-row-payload-header! body lsn now)
        append-res (append-record-at! ch offset body)
        next-offset (+ offset (long (:size append-res)))]
    (when segment-offset
      (vreset! segment-offset next-offset))
    (when-let [total-bytes-v (:retention-total-bytes state)]
      (vreset! total-bytes-v (+ ^long @total-bytes-v
                                ^long (:size append-res))))
    (vreset! lsn-v (inc lsn))
    {:append-res append-res
     :append-start-ms append-start-ms
     :ch ch
     :lmdb-rows lmdb-rows
     :lsn lsn
     :near-roll? near-roll?
     :sid sid
     :sync-manager sync-manager
     :timeout-ms (long (:commit-wait-ms state))}))

(defn- perform-sync-round!
  [state ^FileChannel ch sync-manager sync-begin mark-fatal!]
  (when-let [target-lsn (:target-lsn sync-begin)]
    (try
      (let [force-start-ms (System/currentTimeMillis)]
        (force-segment! state ch (:sync-mode state))
        (let [force-end-ms (System/currentTimeMillis)
              reason (:reason sync-begin)]
          (record-fsync-ms! sync-manager (- force-end-ms force-start-ms) false)
          (complete-sync-success! sync-manager
                                  target-lsn
                                  force-end-ms
                                  reason
                                  false)
          {:target-lsn target-lsn
           :sync-done-ms force-end-ms
           :sync-reason reason}))
      (catch Exception e
        (complete-sync-failure! sync-manager e false)
        (when mark-fatal!
          (mark-fatal! state e))
        (throw e)))))

(defn- wait-strict-durable!
  [state ^FileChannel ch sync-manager lsn timeout-ms mark-fatal!]
  (let [lsn (long lsn)
        timeout-ms (long timeout-ms)
        start-ms (System/currentTimeMillis)
        deadline (+ ^long start-ms (max 0 timeout-ms))
        ^Object monitor (:monitor sync-manager)]
    (loop [last-sync-ms nil
           last-sync-reason nil]
      (let [durable (long @(:last-durable-lsn sync-manager))]
        (if (<= ^long lsn ^long durable)
          {:sync-done-ms last-sync-ms
           :sync-reason (or last-sync-reason @(:last-sync-reason sync-manager))}
          (let [now (System/currentTimeMillis)
                remaining (- ^long deadline ^long now)]
            (when-not (pos? remaining)
              (await-durable-lsn! sync-manager lsn 0 now))
            (if-let [sync-res
                     (perform-sync-round! state
                                          ch
                                          sync-manager
                                          (begin-sync! sync-manager lsn)
                                          mark-fatal!)]
              (recur (:sync-done-ms sync-res)
                     (:sync-reason sync-res))
              (do
                (locking monitor
                  (let [durable-now (long @(:last-durable-lsn sync-manager))
                        healthy? (boolean @(:healthy? sync-manager))
                        failure @(:failure sync-manager)]
                    (cond
                      (<= ^long lsn ^long durable-now)
                      nil

                      (not healthy?)
                      (throw (ex-info "Txn-log sync manager is unhealthy"
                                      {:type :txlog/unhealthy
                                       :lsn lsn}
                                      failure))

                      :else
                      (let [remaining-ms
                            (max 1 (- ^long deadline
                                      ^long (System/currentTimeMillis)))]
                        (.wait monitor (long remaining-ms))))))
                (recur last-sync-ms last-sync-reason)))))))))

(defn- append-durable-relaxed!
  [state rows {:keys [throw-if-fatal! mark-fatal!]}]
  (let [prepared-payload (encode-commit-row-payload+lmdb-rows 0 0 rows)
        append-lock (or (:append-lock state) state)
        {:keys [append-res append-start-ms ch lsn lmdb-rows near-roll?
                sid sync-manager]}
        (locking append-lock
          (append-record-under-lock! state prepared-payload throw-if-fatal!))
        sync-request (request-sync-on-append! sync-manager lsn append-start-ms)
        sync-res (when sync-request
                   (perform-sync-round! state
                                        ch
                                        sync-manager
                                        (begin-sync! sync-manager)
                                        mark-fatal!))
        synced? (or (some? sync-res)
                    (<= ^long lsn
                        ^long @(:last-durable-lsn sync-manager)))
        sync-done-ms (:sync-done-ms sync-res)]
    (when near-roll?
      (record-append-near-roll-ms!
       state
       (- (or sync-done-ms
              (System/currentTimeMillis))
          append-start-ms)))
    (assoc append-res
           :lsn lsn
           :segment-id sid
           :synced? synced?
           :lmdb-rows lmdb-rows)))

(defn- append-durable-strict!
  [state rows {:keys [throw-if-fatal! mark-fatal!]}]
  (let [prepared-payload (encode-commit-row-payload+lmdb-rows 0 0 rows)
        append-lock (or (:append-lock state) state)
        {:keys [append-res append-start-ms ch lsn lmdb-rows near-roll?
                sid sync-manager timeout-ms]}
        (locking append-lock
          (append-record-under-lock! state prepared-payload throw-if-fatal!))
        _ (request-sync-on-append! sync-manager lsn append-start-ms)
        _ (request-sync-now! sync-manager)
        {:keys [sync-done-ms sync-reason]}
        (wait-strict-durable! state ch sync-manager lsn timeout-ms mark-fatal!)
        done-ms (or sync-done-ms (System/currentTimeMillis))]
    (record-commit-wait-ms! sync-manager
                            (- done-ms append-start-ms)
                            sync-reason
                            false)
    (when near-roll?
      (record-append-near-roll-ms! state (- done-ms append-start-ms)))
    (assoc append-res
           :lsn lsn
           :segment-id sid
           :synced? true
           :lmdb-rows lmdb-rows)))

(defn- strict-profile-state?
  [state]
  (= :strict (:durability-profile state)))

(defn append-durable!
  [state rows hooks]
  (maybe-roll-segment! state (System/currentTimeMillis))
  (if (strict-profile-state? state)
    (append-durable-strict! state rows hooks)
    (append-durable-relaxed! state rows hooks)))

(defn commit-finished!
  [state marker-entry]
  (when marker-entry
    (vreset! (:marker-revision state) (long (:revision marker-entry)))))

(defn now-ms
  []
  (System/currentTimeMillis))

(def ^:private sync-reasons
  [:batch-count :batch-time :forced :unknown])

(def ^:private sync-reason-set
  (set sync-reasons))

(def ^:private sync-reason->idx
  {:batch-count 0
   :batch-time 1
   :forced 2
   :unknown 3})

(def ^:private sync-reason-batch-count-idx
  (long (sync-reason->idx :batch-count)))

(def ^:private sync-reason-batch-time-idx
  (long (sync-reason->idx :batch-time)))

(def ^:private sync-reason-forced-idx
  (long (sync-reason->idx :forced)))

(defn- normalize-sync-reason
  [reason]
  (if (contains? sync-reason-set reason)
    reason
    :unknown))

(defn- sync-reason-idx
  ^long [reason]
  (long (or (get sync-reason->idx (normalize-sync-reason reason))
            (sync-reason->idx :unknown))))

(defn- zero-sync-reason-array
  ^longs []
  (long-array (count sync-reasons)))

(defn- sync-reason-array->map
  [^longs arr]
  (persistent!
    (reduce-kv (fn [acc idx reason]
                 (assoc! acc reason (long (aget arr idx))))
               (transient {})
               sync-reasons)))

(defn- avg-ms
  [total count]
  (when (pos? (long (or count 0)))
    (/ (double (or total 0)) (double count))))

(defn- avg-by-reason
  [totals counts]
  (into {}
        (map (fn [reason]
               [reason
                (avg-ms (long (or (get totals reason) 0))
                        (long (or (get counts reason) 0)))]))
        sync-reasons))

(defn- avg-by-mode
  [totals counts]
  (let [batch-total (+ (long (or (get totals :batch-count) 0))
                       (long (or (get totals :batch-time) 0)))
        batch-count (+ (long (or (get counts :batch-count) 0))
                       (long (or (get counts :batch-time) 0)))
        forced-total (long (or (get totals :forced) 0))
        forced-count (long (or (get counts :forced) 0))
        unknown-total (long (or (get totals :unknown) 0))
        unknown-count (long (or (get counts :unknown) 0))]
    {:batched (avg-ms batch-total batch-count)
     :forced (avg-ms forced-total forced-count)
     :unknown (avg-ms unknown-total unknown-count)}))

(def ^:private pending-lsn-queue-initial-capacity 256)

(defn- enqueue-pending-lsn!
  [{:keys [pending-lsn-queue
           pending-lsn-head
           pending-lsn-tail
           pending-lsn-size]}
   ^long lsn]
  (let [size (long @pending-lsn-size)
        ^longs queue0 @pending-lsn-queue
        capacity0 (alength queue0)
        [^longs queue ^long capacity]
        (if (< size capacity0)
          [queue0 capacity0]
          (let [new-capacity (int (max (inc capacity0) (* 2 capacity0)))
                queue1 (long-array new-capacity)
                head0 (long @pending-lsn-head)]
            (dotimes [i (int size)]
              (aset-long queue1
                         i
                         (aget queue0
                               (int (mod (+ head0 i) capacity0)))))
            (vreset! pending-lsn-queue queue1)
            (vreset! pending-lsn-head 0)
            (vreset! pending-lsn-tail size)
            [queue1 (long new-capacity)]))
        tail (long @pending-lsn-tail)
        next-tail (long (if (= (inc tail) capacity) 0 (inc tail)))]
    (aset-long queue (int tail) lsn)
    (vreset! pending-lsn-tail next-tail)
    (vreset! pending-lsn-size (inc size))
    (long @pending-lsn-size)))

(defn- pending-trailing-lsn
  [{:keys [pending-lsn-queue
           pending-lsn-head
           pending-lsn-size]}]
  (let [size (long @pending-lsn-size)]
    (when (pos? size)
      (let [^longs queue @pending-lsn-queue
            capacity (long (alength queue))
            head (long @pending-lsn-head)
            idx (long (mod (+ head (dec size)) capacity))]
        (long (aget queue (int idx)))))))

(defn- drop-pending-through!
  [{:keys [pending-lsn-queue
           pending-lsn-head
           pending-lsn-size]}
   ^long durable-lsn]
  (let [^longs queue @pending-lsn-queue
        capacity (long (alength queue))]
    (loop [head (long @pending-lsn-head)
           size (long @pending-lsn-size)]
      (if (and (pos? size)
               (<= (long (aget queue (int head))) durable-lsn))
        (recur (long (if (= (inc head) capacity) 0 (inc head)))
               (dec size))
        (do
          (vreset! pending-lsn-head head)
          (vreset! pending-lsn-size size)
          size)))))

(defn new-sync-manager
  [{:keys [last-durable-lsn
           last-appended-lsn
           last-sync-ms
           group-commit
           group-commit-ms
           sync-adaptive?
           track-trailing?]
    :or {last-durable-lsn 0
         last-appended-lsn 0
         last-sync-ms 0
         group-commit 100
         group-commit-ms 100
         sync-adaptive? true
         track-trailing? true}}]
  (let [last-durable-lsn* (long last-durable-lsn)
        last-appended-lsn* (long last-appended-lsn)
        pending0 (max 0 (- last-appended-lsn* last-durable-lsn*))]
    {:monitor (Object.)
   :last-durable-lsn (volatile! last-durable-lsn*)
   :last-appended-lsn (volatile! last-appended-lsn*)
   :last-sync-ms (volatile! (long last-sync-ms))
   :last-fsync-ms (volatile! 0)
   :last-fsync-at-ms (volatile! 0)
   :last-commit-wait-ms (volatile! 0)
   :last-commit-wait-at-ms (volatile! 0)
   :group-commit (volatile! (long group-commit))
   :group-commit-ms (volatile! (long group-commit-ms))
   :sync-adaptive? (boolean sync-adaptive?)
   :track-trailing? (boolean track-trailing?)
   :sync-count-by-reason (zero-sync-reason-array)
   :batched-sync-count (volatile! 0)
   :forced-sync-count (volatile! 0)
   :last-sync-reason (volatile! nil)
   :unsynced-count (volatile! pending0)
   :pending-lsn-queue (volatile! (long-array pending-lsn-queue-initial-capacity))
   :pending-lsn-head (volatile! 0)
   :pending-lsn-tail (volatile! 0)
   :pending-lsn-size (volatile! 0)
   :sync-requested? (volatile! false)
   :sync-request-reason (volatile! nil)
   :sync-in-progress? (volatile! false)
   :commit-wait-ms-total (volatile! 0)
   :commit-wait-sample-count (volatile! 0)
   :commit-wait-ms-total-by-reason (zero-sync-reason-array)
   :commit-wait-count-by-reason (zero-sync-reason-array)
   :healthy? (volatile! true)
   :failure (volatile! nil)}))

(defn- pending-count
  [^long last-appended-lsn ^long last-durable-lsn]
  (max 0 (- ^long last-appended-lsn ^long last-durable-lsn)))

(defn sync-manager-state
  [{:keys [last-durable-lsn
           last-appended-lsn
           last-sync-ms
           last-fsync-ms
           last-fsync-at-ms
           last-commit-wait-ms
           last-commit-wait-at-ms
           group-commit
           group-commit-ms
           sync-adaptive?
           sync-count-by-reason
           batched-sync-count
           forced-sync-count
           last-sync-reason
           unsynced-count
           pending-lsn-size
           sync-requested?
           sync-request-reason
           sync-in-progress?
           commit-wait-ms-total
           commit-wait-sample-count
           commit-wait-ms-total-by-reason
           commit-wait-count-by-reason
           healthy?
           failure]}]
  (let [last-durable-lsn* (long @last-durable-lsn)
        last-appended-lsn* (long @last-appended-lsn)
        totals (sync-reason-array->map commit-wait-ms-total-by-reason)
        counts (sync-reason-array->map commit-wait-count-by-reason)
        commit-wait-ms-total* (long @commit-wait-ms-total)
        commit-wait-sample-count* (long @commit-wait-sample-count)]
    {:last-durable-lsn last-durable-lsn*
     :last-appended-lsn last-appended-lsn*
     :last-sync-ms (long @last-sync-ms)
     :last-fsync-ms (long @last-fsync-ms)
     :last-fsync-at-ms (long @last-fsync-at-ms)
     :last-commit-wait-ms (long @last-commit-wait-ms)
     :last-commit-wait-at-ms (long @last-commit-wait-at-ms)
     :group-commit (long @group-commit)
     :group-commit-ms (long @group-commit-ms)
     :sync-adaptive? sync-adaptive?
     :sync-count-by-reason (sync-reason-array->map sync-count-by-reason)
     :batched-sync-count (long @batched-sync-count)
     :forced-sync-count (long @forced-sync-count)
     :last-sync-reason @last-sync-reason
     :unsynced-count (long @unsynced-count)
     :pending-queue-size (long @pending-lsn-size)
     :sync-requested? (boolean @sync-requested?)
     :sync-request-reason @sync-request-reason
     :sync-in-progress? (boolean @sync-in-progress?)
     :commit-wait-ms-total commit-wait-ms-total*
     :commit-wait-sample-count commit-wait-sample-count*
     :commit-wait-ms-total-by-reason totals
     :commit-wait-count-by-reason counts
     :healthy? (boolean @healthy?)
     :failure @failure
     :pending-count (pending-count last-appended-lsn* last-durable-lsn*)
     :avg-commit-wait-ms
     (avg-ms commit-wait-ms-total* commit-wait-sample-count*)
     :avg-commit-wait-ms-by-reason (avg-by-reason totals counts)
     :avg-commit-wait-ms-by-mode (avg-by-mode totals counts)}))

(defn sync-manager-pending?
  [sync-manager]
  (> ^long @(:last-appended-lsn sync-manager)
     ^long @(:last-durable-lsn sync-manager)))

(defn record-fsync-ms!
  ([manager duration-ms]
   (record-fsync-ms! manager duration-ms true))
  ([{:keys [monitor] :as manager} duration-ms snapshot?]
   (let [v (long (max 0 (or duration-ms 0)))
         now (now-ms)]
     (locking monitor
       (vreset! (:last-fsync-ms manager) v)
       (vreset! (:last-fsync-at-ms manager) now)
       (when snapshot?
         (sync-manager-state manager))))))

(defn record-commit-wait-ms!
  ([manager duration-ms]
   (record-commit-wait-ms! manager duration-ms nil true))
  ([manager duration-ms reason]
   (record-commit-wait-ms! manager duration-ms reason true))
  ([{:keys [monitor] :as manager} duration-ms reason snapshot?]
   (let [v (long (max 0 (or duration-ms 0)))
         now (now-ms)]
     (locking monitor
       (let [reason* (normalize-sync-reason
                      (or reason
                          @(:last-sync-reason manager)
                          :unknown))
             idx (sync-reason-idx reason*)
             ^longs wait-totals (:commit-wait-ms-total-by-reason manager)
             ^longs wait-counts (:commit-wait-count-by-reason manager)
             total-v (:commit-wait-ms-total manager)
             sample-count-v (:commit-wait-sample-count manager)]
         (vreset! (:last-commit-wait-ms manager) v)
         (vreset! (:last-commit-wait-at-ms manager) now)
         (vreset! total-v (+ ^long @total-v v))
         (vreset! sample-count-v (long (inc (long @sample-count-v))))
         (aset-long wait-totals idx (+ ^long (aget wait-totals idx) v))
         (aset-long wait-counts idx
                    (long (inc (long (aget wait-counts idx))))))
       (when snapshot?
         (sync-manager-state manager))))))

(defn reset-sync-health!
  ([manager]
   (reset-sync-health! manager true))
  ([{:keys [monitor] :as manager} snapshot?]
   (locking monitor
     (vreset! (:healthy? manager) true)
     (vreset! (:failure manager) nil)
     (.notifyAll monitor)
     (when snapshot?
       (sync-manager-state manager)))))

(defn- mark-unhealthy!
  ([manager ex]
   (mark-unhealthy! manager ex true))
  ([{:keys [monitor] :as manager} ex snapshot?]
   (locking monitor
     (vreset! (:sync-in-progress? manager) false)
     (vreset! (:sync-requested? manager) false)
     (vreset! (:sync-request-reason manager) nil)
     (vreset! (:healthy? manager) false)
     (vreset! (:failure manager) ex)
     (.notifyAll monitor)
     (when snapshot?
       (sync-manager-state manager)))))

(defn request-sync-on-append!
  ([manager lsn] (request-sync-on-append! manager lsn (now-ms)))
  ([{:keys [monitor] :as manager} lsn now]
   (locking monitor
     (let [healthy? (boolean @(:healthy? manager))]
       (when-not healthy?
         (raise "Txn-log sync manager is unhealthy"
                {:type :txlog/unhealthy
                 :failure @(:failure manager)}))
       (let [last-appended-lsn (long @(:last-appended-lsn manager))
             unsynced-count (long @(:unsynced-count manager))
             sync-requested? (boolean @(:sync-requested? manager))
             group-commit (long @(:group-commit manager))
             group-commit-ms (long @(:group-commit-ms manager))
             last-sync-ms (long @(:last-sync-ms manager))
             lsn* (long lsn)
             new-appended (max last-appended-lsn lsn*)
             appended-delta (max 0 (- ^long new-appended ^long last-appended-lsn))
             unsynced-after (+ ^long unsynced-count ^long appended-delta)
             elapsed (max 0 (- ^long (long now) ^long last-sync-ms))
             count? (>= ^long unsynced-after ^long group-commit)
             time? (and (pos? unsynced-after)
                        (pos? group-commit-ms)
                        (>= elapsed group-commit-ms))
             reason (cond
                      count? :batch-count
                      time? :batch-time
                      :else nil)]
         (vreset! (:last-appended-lsn manager) new-appended)
         (vreset! (:unsynced-count manager) unsynced-after)
         (when (and reason (not sync-requested?))
           (vreset! (:sync-requested? manager) true)
           (vreset! (:sync-request-reason manager) reason)
           {:request? true
            :reason reason}))))))

(defn request-sync-if-needed!
  ([manager] (request-sync-if-needed! manager (now-ms)))
  ([{:keys [monitor] :as manager} now]
   (locking monitor
     (let [healthy? (boolean @(:healthy? manager))]
       (when-not healthy?
         (raise "Txn-log sync manager is unhealthy"
                {:type :txlog/unhealthy
                 :failure @(:failure manager)}))
       (let [pending (max 0 (long @(:unsynced-count manager)))
             sync-requested? (boolean @(:sync-requested? manager))
             group-commit (long @(:group-commit manager))
             group-commit-ms (long @(:group-commit-ms manager))
             last-sync-ms (long @(:last-sync-ms manager))
             elapsed (max 0 (- ^long (long now) ^long last-sync-ms))]
         (when (and (pos? pending)
                    (not sync-requested?)
                    (or (>= pending group-commit)
                        (and (pos? group-commit-ms)
                             (>= elapsed group-commit-ms))))
           (let [reason (if (>= pending group-commit)
                          :batch-count
                          :batch-time)]
             (vreset! (:sync-requested? manager) true)
             (vreset! (:sync-request-reason manager) reason)
             {:request? true :reason reason})))))))

(defn request-sync-now!
  [{:keys [monitor] :as manager}]
  (locking monitor
    (let [healthy? (boolean @(:healthy? manager))]
      (when-not healthy?
        (raise "Txn-log sync manager is unhealthy"
               {:type :txlog/unhealthy
                :failure @(:failure manager)}))
      (let [pending (max 0 (long @(:unsynced-count manager)))
            sync-requested? (boolean @(:sync-requested? manager))]
        (when (and (pos? pending) (not sync-requested?))
          (vreset! (:sync-requested? manager) true)
          (vreset! (:sync-request-reason manager) :forced)
          {:request? true :reason :forced})))))

(defn begin-sync!
  ([manager]
   (begin-sync! manager nil))
  ([{:keys [monitor] :as manager} lsn]
   (locking monitor
     (let [healthy? (boolean @(:healthy? manager))
           sync-in-progress? (boolean @(:sync-in-progress? manager))
           last-appended-lsn (long @(:last-appended-lsn manager))
           last-durable-lsn (long @(:last-durable-lsn manager))
           sync-requested? (boolean @(:sync-requested? manager))
           sync-request-reason @(:sync-request-reason manager)
           track-trailing? (boolean (:track-trailing? manager))]
       (when-not healthy?
         (raise "Txn-log sync manager is unhealthy"
                {:type :txlog/unhealthy
                 :failure @(:failure manager)}))
       (if sync-in-progress?
         nil
         (if (> ^long last-appended-lsn ^long last-durable-lsn)
           (let [target-lsn (long (if track-trailing?
                                    (or (pending-trailing-lsn manager)
                                        last-appended-lsn)
                                    last-appended-lsn))
                 lsn* (when (some? lsn) (long lsn))]
            (if (and lsn* (> lsn* target-lsn))
               nil
               (let [reason (if sync-requested?
                              (or sync-request-reason :unknown)
                              :forced)]
                 (vreset! (:sync-requested? manager) false)
                 (vreset! (:sync-request-reason manager) nil)
                 (vreset! (:sync-in-progress? manager) true)
                 (vreset! (:last-sync-reason manager) reason)
                 {:target-lsn target-lsn
                  :reason reason})))
           (when sync-requested?
             (vreset! (:sync-requested? manager) false)
             (vreset! (:sync-request-reason manager) nil)
             nil)))))))

(defn complete-sync-success!
  ([manager] (complete-sync-success! manager nil (now-ms) nil true))
  ([manager target-lsn now]
   (complete-sync-success! manager target-lsn now nil true))
  ([manager target-lsn now reason]
   (complete-sync-success! manager target-lsn now reason true))
  ([{:keys [monitor] :as manager} target-lsn now reason snapshot?]
   (locking monitor
     (let [last-durable-lsn (long @(:last-durable-lsn manager))
           last-appended-lsn (long @(:last-appended-lsn manager))
           sync-requested? (boolean @(:sync-requested? manager))
           sync-request-reason @(:sync-request-reason manager)
           target (long (or target-lsn last-appended-lsn last-durable-lsn))
           durable (max ^long last-durable-lsn target)
           pending-after (max 0 (- ^long last-appended-lsn ^long durable))
           reason* (normalize-sync-reason
                    (or reason
                        @(:last-sync-reason manager)
                        :forced))
           reason-idx (sync-reason-idx reason*)
           keep-request? (and sync-requested? (pos? pending-after))
           next-request-reason (when keep-request?
                                 (or sync-request-reason :forced))]
       (vreset! (:last-sync-reason manager) reason*)
       (vreset! (:last-durable-lsn manager) durable)
       (vreset! (:last-sync-ms manager) (long now))
       (when (boolean (:track-trailing? manager))
         (drop-pending-through! manager durable))
       (vreset! (:unsynced-count manager) pending-after)
       (vreset! (:sync-in-progress? manager) false)
       (vreset! (:sync-requested? manager) keep-request?)
       (vreset! (:sync-request-reason manager) next-request-reason)
       (vreset! (:healthy? manager) true)
       (vreset! (:failure manager) nil)
       (let [^longs sync-count-by-reason (:sync-count-by-reason manager)]
         (aset-long sync-count-by-reason
                    reason-idx
                    (long (inc (long (aget sync-count-by-reason reason-idx))))))
       (when (or (= reason-idx sync-reason-batch-count-idx)
                 (= reason-idx sync-reason-batch-time-idx))
         (vreset! (:batched-sync-count manager)
                  (long (inc (long @(:batched-sync-count manager))))))
       (when (= reason-idx sync-reason-forced-idx)
         (vreset! (:forced-sync-count manager)
                  (long (inc (long @(:forced-sync-count manager))))))
       (.notifyAll monitor)
       (when snapshot?
         (sync-manager-state manager))))))

(defn complete-sync-failure!
  ([manager ex]
   (complete-sync-failure! manager ex true))
  ([manager ex snapshot?]
   (let [failure (or ex (ex-info "Txn-log sync failed" {:type :txlog/sync-failed}))]
     (mark-unhealthy! manager failure snapshot?))))

(defn await-durable-lsn!
  ([manager lsn timeout-ms]
   (await-durable-lsn! manager lsn timeout-ms (now-ms)))
  ([{:keys [monitor] :as manager} lsn timeout-ms start-ms]
   (locking monitor
     (loop [deadline (+ ^long start-ms (max 0 ^long timeout-ms))]
       (let [last-durable-lsn (long @(:last-durable-lsn manager))
             healthy? (boolean @(:healthy? manager))
             failure @(:failure manager)]
         (cond
           (<= ^long lsn ^long last-durable-lsn)
           {:durable? true :last-durable-lsn last-durable-lsn}

           (not healthy?)
           (throw (ex-info "Txn-log sync manager is unhealthy"
                           {:type :txlog/unhealthy
                            :lsn lsn}
                           failure))

           :else
           (let [now (now-ms)
                 remaining (- ^long deadline ^long now)]
             (if (pos? remaining)
               (do
                 (.wait monitor remaining)
                 (recur deadline))
               (let [timeout-ex
                     (ex-info "Timed out waiting for durable LSN"
                              {:type :txlog/commit-timeout
                               :lsn lsn
                               :timeout-ms timeout-ms})]
                 (mark-unhealthy! manager timeout-ex false)
                 (throw timeout-ex))))))))))
