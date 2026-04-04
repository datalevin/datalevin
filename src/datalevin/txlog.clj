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
   [datalevin.binding.cpp]
   [datalevin.buffer :as bf]
   [datalevin.bits :as b]
   [clojure.java.io :as io]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.lmdb]
   [datalevin.txlog.codec :as tcodec]
   [datalevin.txlog.meta :as tmeta]
   [datalevin.txlog.recovery :as trec]
   [datalevin.txlog.segment :as tseg]
   [datalevin.util :as u :refer [raise map+]])
  (:import
   [datalevin.io PosixFsync]
   [java.io File]
   [java.nio ByteBuffer ByteOrder BufferOverflowException]
   [java.nio.channels FileChannel FileLock OverlappingFileLockException]
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
(def ^:private ^"[Ljava.nio.file.StandardOpenOption;"
  open-lock-create-write-options
  (into-array StandardOpenOption
              [StandardOpenOption/CREATE
               StandardOpenOption/WRITE]))

(def ^:const meta-slot-payload-size 64)
(def ^:const meta-slot-size (+ meta-slot-payload-size 4))
(def ^:const meta-format-major 1)
(def ^:private meta-file-name "meta")
(def ^:private meta-lock-file-name "meta.lock")
(def ^:private sync-lock-file-name "sync.lock")
(def ^:private recovery-lock-file-name "recovery.lock")
(def ^:private maintenance-lock-file-name "maintenance.lock")
(def ^:private meta-magic-bytes
  (byte-array [(byte 0x44) (byte 0x4c) (byte 0x54) (byte 0x4d)])) ; DLTM

(def ^:const commit-marker-slot-payload-size 60)
(def ^:const commit-marker-slot-size (+ commit-marker-slot-payload-size 4))
(def ^:const commit-marker-format-major 1)
(def ^:private commit-marker-magic-bytes
  (byte-array [(byte 0x44) (byte 0x4c) (byte 0x43) (byte 0x4d)])) ; DLCM
(def ^:private commit-payload-ha-term-flag 0x01)

(def ^:dynamic *commit-payload-ha-term*
  nil)

(def ^:private sync-mode-values #{:fsync :fdatasync :extra :none})
(def ^:private segment-prealloc-mode-values #{:native :none})

(declare segment-files
         append-near-roll-sample-max
         append-near-roll-stats-array-size
         scan-segment
         segment-path
         meta-lock-path
         sync-lock-path
         recovery-lock-path
         maintenance-lock-path
         open-segment-channel
         segment-end-offset
         truncate-partial-tail!
         read-meta-file
         meta-path
         preallocation-enabled-state?
         activate-next-segment!
         force-channel!
         ensure-next-segment-prepared!
         new-sync-manager
         sync-manager-pending?
         request-sync-on-append!
         append-sync-transition!
         begin-sync!
         complete-sync-success!
         complete-sync-failure!
         await-durable-lsn!
         sync-manager-state
         record-fsync-ms!
         record-commit-wait-ms!
         request-sync-now!
         classify-record-kind
         decode-commit-row-payload
         durability-profile)

(defn enabled? [info] (true? (:wal? info)))

(defn sync-mode
  [info]
  (let [mode (or (:wal-sync-mode info) c/*wal-sync-mode*)
        profile (durability-profile info)]
    (if (= :extra profile)
      (if (= :none mode) :none :extra)
      mode)))

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
        allowed #{:strict :relaxed :extra}]
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

(def ^:private decode-scanned-record-entry trec/decode-scanned-record-entry)

(def ^:private txlog-records-cache-entry trec/txlog-records-cache-entry)

(def ^:private scan-segment-records-cache-entry
  trec/scan-segment-records-cache-entry)

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
        active-last-record-summary-v (volatile! nil)
        active-last-lsn-v            (volatile! 0)
        active-scan       (truncate-partial-tail!
                           active-path
                           {:allow-preallocated-tail? true
                            :collect-records? false
                            :on-record
                            (fn [record]
                              (let [payload
                                    (tcodec/decode-commit-row-payload-header
                                     ^bytes (:body record))
                                    lsn (long (or (:lsn payload) 0))]
                                (when-not (pos? lsn)
                                  (raise "Txn-log payload missing valid positive LSN"
                                         {:type :txlog/corrupt
                                          :segment-id (long active-id)
                                          :path active-path
                                          :offset (long (:offset record))
                                          :record record}))
                                (vreset! active-last-lsn-v lsn)
                                (vreset! active-last-record-summary-v
                                         {:lsn lsn
                                          :segment-id (long active-id)
                                          :offset (long (:offset record))
                                          :checksum (long (:checksum record))})))})
        active-offset     (segment-end-offset active-scan)
        active-file       (io/file active-path)
        txlog-records-cache closed-record-cache
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
        last-from-active  (long @active-last-lsn-v)
        last-from-seg     (long (max last-from-active
                                     (long last-from-closed)))
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
         :fault-context          (select-keys info [:db-identity
                                                    :ha-node-id
                                                    :db-name])
         :meta-path              meta-path
         :meta-lock-path         (meta-lock-path dir)
         :sync-lock-path         (sync-lock-path dir)
         :recovery-lock-path     (recovery-lock-path dir)
         :maintenance-lock-path  (maintenance-lock-path dir)
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
         :meta-last-applied-lsn  (volatile! last-applied)
         :meta-revision          (volatile! (long (or (:revision meta-cur) -1)))
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
                          :track-trailing?   (not= :relaxed profile)})

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
         ;; Preserve the tail record summary from the active segment scan so a
         ;; clean reopen can validate the current commit marker without walking
         ;; the full retained WAL again.
         :last-record-summary                     @active-last-record-summary-v
         :fatal-error                             (volatile! nil)}]
    {:dir dir :state state}))

(def segment-file-name tseg/segment-file-name)

(def segment-path tseg/segment-path)

(def prepared-segment-file-name tseg/prepared-segment-file-name)

(def prepared-segment-path tseg/prepared-segment-path)

(def meta-path tmeta/meta-path)

(def meta-lock-path tmeta/meta-lock-path)

(def sync-lock-path tmeta/sync-lock-path)

(def recovery-lock-path tmeta/recovery-lock-path)

(def maintenance-lock-path tmeta/maintenance-lock-path)

(def parse-segment-id tseg/parse-segment-id)

(def parse-prepared-segment-id tseg/parse-prepared-segment-id)

(def segment-files tseg/segment-files)

(def prepared-segment-files tseg/prepared-segment-files)

(def encode-record tcodec/encode-record)

(def decode-record-bytes tcodec/decode-record-bytes)

(def scan-segment tseg/scan-segment)

(def truncate-partial-tail! tseg/truncate-partial-tail!)

(def segment-end-offset tseg/segment-end-offset)

(def open-segment-channel tseg/open-segment-channel)

(def append-record-at! tseg/append-record-at!)

(def append-record! tseg/append-record!)

(def force-segment! tseg/force-segment!)

(def prepare-segment! tseg/prepare-segment!)

(def activate-prepared-segment! tseg/activate-prepared-segment!)

(def prepare-next-segment! tseg/prepare-next-segment!)

(def activate-next-segment! tseg/activate-next-segment!)

(def ensure-next-segment-prepared! tseg/ensure-next-segment-prepared!)

(def ^:private inc-volatile-long! tseg/inc-volatile-long!)

(def ^:private add-volatile-long! tseg/add-volatile-long!)

(def ^:private activated-segment-offset tseg/activated-segment-offset)

(def ^:private append-near-roll-sample-max 512)
(def ^:private append-near-roll-linear-bucket-max-ms 255)
(def ^:private append-near-roll-linear-bucket-count
  (long (inc (long append-near-roll-linear-bucket-max-ms))))
(def ^:private append-near-roll-tail-bucket-count 32)
(def ^:private append-near-roll-tail-base-shift 8) ; 2^8 = 256ms
(def ^:private append-near-roll-hist-bucket-count
  (long (+ (long append-near-roll-linear-bucket-count)
           (long append-near-roll-tail-bucket-count))))
(def ^:private append-near-roll-stats-head-idx 0)
(def ^:private append-near-roll-stats-size-idx 1)
(def ^:private append-near-roll-stats-hist-offset 2)
(def ^:private append-near-roll-stats-array-size
  (long (+ (long append-near-roll-stats-hist-offset)
           (long append-near-roll-hist-bucket-count))))
(def ^:private long-array-class (class (long-array 0)))

(defn- near-roll-threshold
  [^long segment-max-bytes]
  (if (pos? segment-max-bytes)
    (max 1 ^long (min ^long (quot ^long segment-max-bytes 20)
                      (long (* 16 1024 1024))))
    0))

(defn- near-roll-append?
  [state ^long segment-offset]
  (let [max-bytes (long (or (:segment-max-bytes state) 0))
        threshold (near-roll-threshold max-bytes)]
    (and (pos? max-bytes)
         (>= segment-offset
             (long (max 0 (- max-bytes (long threshold))))))))

(defn- append-near-roll-bucket-idx
  ^long [^long duration-ms]
  (if (<= duration-ms (long append-near-roll-linear-bucket-max-ms))
    duration-ms
    (let [lg (long (- 63 (Long/numberOfLeadingZeros duration-ms)))
          tail-idx (long (max 0 (- lg
                                   (long append-near-roll-tail-base-shift))))]
      (long (+ (long append-near-roll-linear-bucket-count)
               (min (long (dec (long append-near-roll-tail-bucket-count)))
                    tail-idx))))))

(defn- append-near-roll-bucket-upper-ms
  ^long [^long bucket-idx]
  (if (< bucket-idx (long append-near-roll-linear-bucket-count))
    bucket-idx
    (let [tail-idx (long (max 0
                              (- bucket-idx
                                 (long append-near-roll-linear-bucket-count))))
          shift (long (+ (inc (long append-near-roll-tail-base-shift))
                         tail-idx))]
      (if (>= shift 63)
        Long/MAX_VALUE
        (dec (bit-shift-left 1 shift))))))

(defn- append-near-roll-hist-inc!
  [^longs stats ^long duration-ms]
  (let [bucket-idx (append-near-roll-bucket-idx duration-ms)
        hist-idx (+ (long append-near-roll-stats-hist-offset) bucket-idx)]
    (aset-long stats hist-idx
               (long (inc (long (aget stats hist-idx)))))))

(defn- append-near-roll-hist-dec!
  [^longs stats ^long duration-ms]
  (let [bucket-idx (append-near-roll-bucket-idx duration-ms)
        hist-idx (+ (long append-near-roll-stats-hist-offset) bucket-idx)
        current (long (aget stats hist-idx))]
    (when (pos? current)
      (aset-long stats hist-idx (dec current)))))

(defn- append-near-roll-p99-from-hist
  ^long [^longs stats ^long sample-size]
  (if (pos? sample-size)
    (let [rank (long (inc (quot (* 99 (dec sample-size)) 100)))]
      (loop [bucket-idx 0
             seen 0]
        (if (< bucket-idx (long append-near-roll-hist-bucket-count))
          (let [cnt (long (aget stats (+ (long append-near-roll-stats-hist-offset)
                                         bucket-idx)))
                seen* (+ ^long seen ^long cnt)]
            (if (>= seen* rank)
              (append-near-roll-bucket-upper-ms bucket-idx)
              (recur (inc bucket-idx) seen*)))
          (append-near-roll-bucket-upper-ms
           (dec (long append-near-roll-hist-bucket-count))))))
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
      (let [duration-ms  (long (max 0 (long (or duration-ms 0))))
            p99-v        (:append-p99-near-roll-ms state)
            metrics-lock (or (:append-lock state) state)]
        (locking metrics-lock
          (let [[^longs ring ^longs stats]
                (ensure-append-near-roll-structures! samples-v sorted-v)
                head (long (aget stats append-near-roll-stats-head-idx))
                size (long (aget stats append-near-roll-stats-size-idx))
                full? (>= size (long append-near-roll-sample-max))
                dropped (when full?
                          (long (aget ring (int head))))
                size* (if full? size (inc size))
                next-head (if (= (inc head) (long append-near-roll-sample-max))
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
  (let [segment-max-bytes (long segment-max-bytes)
        segment-max-ms (long segment-max-ms)]
    (or (>= segment-bytes segment-max-bytes)
        (>= (- now-ms segment-created-ms) segment-max-ms))))

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

(def force-channel! tseg/force-channel!)

(def ^:private with-file-lock tseg/with-file-lock)

(def ^:private try-acquire-file-lock! tseg/try-acquire-file-lock!)

(def ^:private release-file-lock! tseg/release-file-lock!)

(def preallocation-enabled-state? tseg/preallocation-enabled-state?)

(def no-floor-lsn trec/no-floor-lsn)

(def safe-inc-lsn trec/safe-inc-lsn)

(def parse-floor-lsn trec/parse-floor-lsn)

(def parse-optional-floor-lsn trec/parse-optional-floor-lsn)

(def parse-non-negative-long trec/parse-non-negative-long)

(def ensure-floor-provider-id trec/ensure-floor-provider-id)

(def parse-floor-provider-map trec/parse-floor-provider-map)

(def snapshot-floor-update-plan trec/snapshot-floor-update-plan)

(def snapshot-floor-clear-plan trec/snapshot-floor-clear-plan)

(def replica-floor-update-plan trec/replica-floor-update-plan)

(def replica-floor-clear-plan trec/replica-floor-clear-plan)

(def backup-pin-floor-update-plan trec/backup-pin-floor-update-plan)

(def ^:redef backup-pin-floor-clear-plan trec/backup-pin-floor-clear-plan)

(def min-floor-lsn trec/min-floor-lsn)

(def snapshot-floor-state trec/snapshot-floor-state)

(def vector-domain-floor-state trec/vector-domain-floor-state)

(def vector-floor-state trec/vector-floor-state)

(def replica-floor-state trec/replica-floor-state)

(def backup-pin-floor-state trec/backup-pin-floor-state)

(def annotate-gc-segments trec/annotate-gc-segments)

(def select-gc-target-segments trec/select-gc-target-segments)

(def segment-summaries trec/segment-summaries)

(def retention-state trec/retention-state)

(def valid-commit-marker trec/valid-commit-marker)

(def newer-commit-marker trec/newer-commit-marker)

(def validate-commit-marker-reference trec/validate-commit-marker-reference)

(def resolve-applied-lsn trec/resolve-applied-lsn)

(def recovery-state trec/recovery-state)

(def select-open-records trec/select-open-records)

(def select-open-record-rows trec/select-open-record-rows)

(def retention-floors trec/retention-floors)

(def retention-state-report trec/retention-state-report)

(def encode-meta-slot tcodec/encode-meta-slot)

(def decode-meta-slot-bytes tcodec/decode-meta-slot-bytes)

(def read-meta-file tmeta/read-meta-file)

(def write-meta-file! tmeta/write-meta-file!)

(def refresh-shared-state! tmeta/refresh-shared-state!)

(def ^:private refresh-shared-watermarks! tmeta/refresh-shared-watermarks!)

(def publish-meta-append! tmeta/publish-meta-append!)

(def publish-meta-commit! tmeta/publish-meta-commit!)

(def publish-meta-durable! tmeta/publish-meta-durable!)

(def try-with-maintenance-lock tmeta/try-with-maintenance-lock)

(def with-recovery-lock tmeta/with-recovery-lock)

(def note-gc-deleted-bytes! tmeta/note-gc-deleted-bytes!)

(def encode-commit-marker-slot tcodec/encode-commit-marker-slot)

(def decode-commit-marker-slot-bytes tcodec/decode-commit-marker-slot-bytes)

(def vector-checkpoint-op? tcodec/vector-checkpoint-op?)

(def classify-record-kind tcodec/classify-record-kind)

(def use-parallel-row-encoding? tcodec/use-parallel-row-encoding?)

(def tl-commit-body-buffer tcodec/tl-commit-body-buffer)

(def tl-bits-buffer tcodec/tl-bits-buffer)

(def tl-row-encode-buffer tcodec/tl-row-encode-buffer)

(def decode-commit-row-payload tcodec/decode-commit-row-payload)

(def ^:private patch-commit-row-payload-header!
  tcodec/patch-commit-row-payload-header!)

(defn commit-marker-key-for-revision
  [revision]
  (if (zero? (bit-and (long revision) 0x1))
    c/wal-marker-a
    c/wal-marker-b))

(defn next-commit-marker-entry
  ([commit-state append-info]
   (next-commit-marker-entry
    (:commit-marker? commit-state)
    (:marker-revision commit-state)
    append-info))
  ([commit-marker? marker-revision append-info]
   (when commit-marker?
     (let [revision (inc (long marker-revision))
           marker {:revision revision
                   :applied-lsn (long (:lsn append-info))
                   :txlog-segment-id (long (:segment-id append-info))
                   :txlog-record-offset (long (:offset append-info))
                   :txlog-record-crc (long (or (:checksum append-info) 0))
                   :updated-ms (long (or (:now-ms append-info)
                                         (System/currentTimeMillis)))}
           slot (tcodec/encode-commit-marker-slot marker)
           row [:put c/kv-info
                (commit-marker-key-for-revision revision)
                slot :keyword :bytes]]
       {:revision revision
        :marker marker
        :row row}))))

(defn encode-commit-row-payload
  "Encode canonical txn-log payload as raw binary bytes."
  [lsn tx-time rows]
  (tcodec/encode-commit-row-payload
   lsn tx-time rows {:ha-term *commit-payload-ha-term*}))

(defn prepare-commit-rows
  [commit-state append-info rows]
  (let [^FastList rows0 (tcodec/ensure-fast-list rows)
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
  [state prepared-payload {:keys [throw-if-fatal! before-append!]}]
  (when throw-if-fatal!
    (throw-if-fatal! state))
  (when before-append!
    (before-append! state))
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
        _ (patch-commit-row-payload-header! body lsn now)
        append-res (append-record-at! ch offset body)
        next-offset (+ offset (long (:size append-res)))]
    (when segment-offset
      (vreset! segment-offset next-offset))
    (when-let [total-bytes-v (:retention-total-bytes state)]
      (vreset! total-bytes-v (+ ^long @total-bytes-v
                                ^long (:size append-res))))
    (vreset! lsn-v (inc lsn))
    (publish-meta-append! state
                          {:lsn lsn
                           :segment-id sid
                           :offset next-offset})
    {:append-res append-res
     :append-start-ms append-start-ms
     :ch ch
     :lsn lsn
     :near-roll? near-roll?
     :sid sid
     :sync-manager sync-manager
     :timeout-ms (long (:commit-wait-ms state))}))

(defn- perform-sync-round!
  [state ^FileChannel ch sync-manager sync-begin
   {:keys [mark-fatal! before-sync!]}]
  (when-let [target-lsn (:target-lsn sync-begin)]
    (if-let [lock-state (try-acquire-file-lock! (:sync-lock-path state))]
      (try
        (refresh-shared-watermarks! state)
        (let [reason (:reason sync-begin)
              durable-before (long @(:last-durable-lsn sync-manager))]
          (if (<= ^long target-lsn ^long durable-before)
            (let [done-ms (System/currentTimeMillis)]
              (complete-sync-success! sync-manager
                                      target-lsn
                                      done-ms
                                      reason
                                      false)
              {:target-lsn target-lsn
               :sync-done-ms done-ms
               :sync-reason reason})
            (do
              (when before-sync!
                (before-sync! state sync-begin))
              (let [force-start-ms (System/currentTimeMillis)]
                (force-segment! state ch (:sync-mode state))
                (let [force-end-ms (System/currentTimeMillis)]
                  (publish-meta-durable! state target-lsn)
                  (record-fsync-ms! sync-manager
                                    (- force-end-ms force-start-ms)
                                    false)
                  (complete-sync-success! sync-manager
                                          target-lsn
                                          force-end-ms
                                          reason
                                          false)
                  {:target-lsn target-lsn
                   :sync-done-ms force-end-ms
                   :sync-reason reason})))))
        (catch Exception e
          (complete-sync-failure! sync-manager e false)
          (when mark-fatal!
            (mark-fatal! state e))
          (throw e))
        (finally
          (release-file-lock! lock-state)))
      (do
        (refresh-shared-watermarks! state)
        nil))))

(defn- wait-strict-durable!
  ([state ^FileChannel ch sync-manager lsn timeout-ms hooks]
   (wait-strict-durable! state ch sync-manager lsn timeout-ms hooks nil))
  ([state ^FileChannel ch sync-manager lsn timeout-ms hooks initial-sync-begin]
   (let [lsn (long lsn)
         timeout-ms (long timeout-ms)
         start-ms (System/currentTimeMillis)
         deadline (+ ^long start-ms (max 0 timeout-ms))]
     (loop [last-sync-ms nil
            last-sync-reason nil
            sync-begin initial-sync-begin]
       (refresh-shared-watermarks! state)
       (let [durable (long @(:last-durable-lsn sync-manager))]
         (if (<= ^long lsn ^long durable)
           {:sync-done-ms last-sync-ms
            :sync-reason (or last-sync-reason @(:last-sync-reason sync-manager))}
           (let [now (System/currentTimeMillis)
                 remaining (- ^long deadline ^long now)]
             (when-not (pos? remaining)
               (throw (ex-info "Timed out waiting for durable LSN"
                               {:type :txlog/commit-timeout
                                :lsn lsn
                                :timeout-ms timeout-ms})))
             (if-let [sync-res
                      (perform-sync-round! state
                                           ch
                                           sync-manager
                                           ;; Reuse the first sync begin from append path
                                           ;; so strict mode avoids an immediate extra
                                           ;; monitor-lock round-trip.
                                           (or sync-begin
                                               (begin-sync! sync-manager lsn))
                                           hooks)]
               (recur (:sync-done-ms sync-res)
                      (:sync-reason sync-res)
                      nil)
               (do
                 (Thread/sleep (long (min 5 remaining)))
                 (recur last-sync-ms last-sync-reason nil))))))))))

(defn- append-durable-relaxed!
  [state rows {:keys [mark-fatal!] :as hooks}]
  (let [prepared-payload {:body (encode-commit-row-payload 0 0 rows)}
        append-lock (or (:append-lock state) state)
        {:keys [append-res append-start-ms ch lsn near-roll?
                sid sync-manager]}
        (locking append-lock
          (append-record-under-lock! state prepared-payload hooks))
        sync-begin (append-sync-transition! sync-manager lsn append-start-ms)
        sync-res (when sync-begin
                   (perform-sync-round! state
                                        ch
                                        sync-manager
                                        sync-begin
                                        hooks))
        _ (when-not sync-res
            (refresh-shared-watermarks! state))
        synced? (or (some? sync-res)
                    (<= ^long lsn
                        ^long @(:last-durable-lsn sync-manager)))
        sync-done-ms (:sync-done-ms sync-res)]
    (when near-roll?
      (record-append-near-roll-ms!
       state
       (- (long (or sync-done-ms
                    (System/currentTimeMillis)))
          (long append-start-ms))))
    (assoc append-res
           :lsn lsn
           :segment-id sid
           :synced? synced?)))

(defn- append-durable-strict!
  [state rows {:as hooks}]
  (let [prepared-payload {:body (encode-commit-row-payload 0 0 rows)}
        append-lock (or (:append-lock state) state)
        {:keys [append-res append-start-ms ch lsn near-roll?
                sid sync-manager timeout-ms]}
        (locking append-lock
          (append-record-under-lock! state prepared-payload hooks))
        sync-begin (append-sync-transition! sync-manager lsn append-start-ms
                                            {:force? true :begin-lsn lsn})
        {:keys [sync-done-ms sync-reason]}
        (wait-strict-durable! state ch sync-manager lsn timeout-ms hooks
                              sync-begin)
        done-ms (or sync-done-ms (System/currentTimeMillis))]
    (record-commit-wait-ms! sync-manager
                            (- (long done-ms) (long append-start-ms))
                            sync-reason
                            false)
    (when near-roll?
      (record-append-near-roll-ms! state (- (long done-ms)
                                            (long append-start-ms))))
    (assoc append-res
           :lsn lsn
           :segment-id sid
           :synced? true)))

(defn- per-tx-durable-profile-state?
  [state]
  (not= :relaxed (:durability-profile state)))

(defn append-durable!
  [state rows hooks]
  (refresh-shared-state! state)
  (maybe-roll-segment! state (System/currentTimeMillis))
  (if (per-tx-durable-profile-state? state)
    (append-durable-strict! state rows hooks)
    (append-durable-relaxed! state rows hooks)))

(defn force-sync!
  [state hooks]
  (refresh-shared-state! state)
  (let [sync-manager (:sync-manager state)
        timeout-ms (long (:commit-wait-ms state))
        before (sync-manager-state sync-manager)
        target-lsn (long (:last-appended-lsn before))
        durable-lsn (long (:last-durable-lsn before))]
    (when (> target-lsn durable-lsn)
      (let [^FileChannel ch @(:segment-channel state)
            _ (when-not ch
                (raise "Txn-log segment channel is not available"
                       {:type :txlog/no-segment-channel}))
            sync-begin (append-sync-transition! sync-manager
                                                target-lsn
                                                (System/currentTimeMillis)
                                                {:force? true
                                                 :begin-lsn target-lsn})]
        (wait-strict-durable! state ch sync-manager target-lsn timeout-ms hooks
                              sync-begin)))
    (let [after (sync-manager-state sync-manager)]
      {:target-lsn target-lsn
       :last-appended-lsn (long (:last-appended-lsn after))
       :last-durable-lsn (long (:last-durable-lsn after))
       :pending-count (long (:pending-count after))
       :synced? (<= target-lsn (long (:last-durable-lsn after)))})))

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
   (let [v (long (max 0 (long (or duration-ms 0))))
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
   (let [v (long (max 0 (long (or duration-ms 0))))
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

(defn- ensure-sync-manager-healthy!
  [manager]
  (when-not (boolean @(:healthy? manager))
    (raise "Txn-log sync manager is unhealthy"
           {:type :txlog/unhealthy
            :failure @(:failure manager)})))

(defn- request-sync-on-append-under-monitor!
  [manager lsn now]
  (ensure-sync-manager-healthy! manager)
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
       :reason reason})))

(defn request-sync-on-append!
  ([manager lsn] (request-sync-on-append! manager lsn (now-ms)))
  ([{:keys [monitor] :as manager} lsn now]
   (locking monitor
     (request-sync-on-append-under-monitor! manager lsn now))))

(defn request-sync-if-needed!
  ([manager] (request-sync-if-needed! manager (now-ms)))
  ([{:keys [monitor] :as manager} now]
   (locking monitor
     (ensure-sync-manager-healthy! manager)
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
           {:request? true :reason reason}))))))

(defn- request-sync-now-under-monitor!
  [manager]
  (ensure-sync-manager-healthy! manager)
  (let [pending (max 0 (long @(:unsynced-count manager)))
        sync-requested? (boolean @(:sync-requested? manager))]
    (when (and (pos? pending) (not sync-requested?))
      (vreset! (:sync-requested? manager) true)
      (vreset! (:sync-request-reason manager) :forced)
      {:request? true :reason :forced})))

(defn request-sync-now!
  [{:keys [monitor] :as manager}]
  (locking monitor
    (request-sync-now-under-monitor! manager)))

(defn- begin-sync-under-monitor!
  [manager lsn]
  (ensure-sync-manager-healthy! manager)
  (let [sync-in-progress? (boolean @(:sync-in-progress? manager))
        last-appended-lsn (long @(:last-appended-lsn manager))
        last-durable-lsn (long @(:last-durable-lsn manager))
        sync-requested? (boolean @(:sync-requested? manager))
        sync-request-reason @(:sync-request-reason manager)
        track-trailing? (boolean (:track-trailing? manager))]
    (if sync-in-progress?
      nil
      (if (> ^long last-appended-lsn ^long last-durable-lsn)
        (let [target-lsn (long (if track-trailing?
                                 (or (pending-trailing-lsn manager)
                                     last-appended-lsn)
                                 last-appended-lsn))
              lsn* (when (some? lsn) (long lsn))]
          (if (and lsn* (> (long lsn*) (long target-lsn)))
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
          nil)))))

(defn begin-sync!
  ([manager]
   (begin-sync! manager nil))
  ([{:keys [monitor] :as manager} lsn]
   (locking monitor
     (begin-sync-under-monitor! manager lsn))))

(defn append-sync-transition!
  "Run append-side sync-manager transitions under one monitor lock.
   Returns the optional begin-sync payload for the caller to perform fsync."
  ([sync-manager lsn now]
   (append-sync-transition! sync-manager lsn now {}))
  ([{:keys [monitor] :as sync-manager} lsn now
    {:keys [force? begin-lsn]
     :or {force? false begin-lsn nil}}]
   (locking monitor
     (let [requested? (boolean
                       (request-sync-on-append-under-monitor! sync-manager
                                                              lsn
                                                              now))
           _ (when force?
               (request-sync-now-under-monitor! sync-manager))
           sync-begin (when (or force? requested?)
                        (begin-sync-under-monitor! sync-manager begin-lsn))]
       sync-begin))))

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
               (throw (ex-info "Timed out waiting for durable LSN"
                             {:type :txlog/commit-timeout
                              :lsn lsn
                              :timeout-ms timeout-ms}))))))))))
