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
   [clojure.java.io :as io]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.lmdb]
   [datalevin.tx-state.lifetime :as lifetime]
   [datalevin.tx-group.phase :as phase]
   [datalevin.txlog.append :as append]
   [datalevin.txlog.codec :as tcodec]
   [datalevin.txlog.meta :as tmeta]
   [datalevin.txlog.recovery :as trec]
   [datalevin.txlog.segment :as tseg]
   [datalevin.txlog.transfer :as transfer]
   [datalevin.util :as u :refer [raise]]
   [taoensso.timbre :as log])
  (:import
   [java.io File]
   [java.nio ByteBuffer]
   [java.nio.channels FileChannel FileLock OverlappingFileLockException]
   [java.nio.file StandardOpenOption]
   [java.util ArrayDeque]
   [java.util.concurrent.locks LockSupport ReentrantLock]
   [org.eclipse.collections.impl.list.mutable FastList]
))

(def ^:const record-header-size 14)
(def ^:const format-major 2)
(def ^:const compressed-flag 0x01)
(def ^:private ^"[Ljava.nio.file.StandardOpenOption;"
  open-lock-create-write-options
  (into-array StandardOpenOption
              [StandardOpenOption/CREATE
               StandardOpenOption/WRITE]))

(def ^:const meta-slot-payload-size 64)
(def ^:const meta-slot-size (+ meta-slot-payload-size 4))
(def ^:const meta-format-major 1)

(def ^:const commit-marker-slot-payload-size 60)
(def ^:const commit-marker-slot-size (+ commit-marker-slot-payload-size 4))
(def ^:const commit-marker-format-major 1)
(def ^:private segment-prealloc-mode-values #{:native :none})

(defmacro ^:private with-wal-use [state & body]
  `(if-let [guard# (:io-lifetime (control ~state))]
     (lifetime/with-use guard# ~@body)
     (do ~@body)))

(declare segment-files
         ensure-sync-manager-healthy!
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
         complete-sync-on-write!
         complete-sync-failure!
         await-durable-lsn!
         await-sync-collection!
         more-work-predicate
         open-reusable-file-lock-channel!
         now-ms
         sync-manager-state
         record-fsync-ms!
         record-commit-wait-ms!
         request-sync-now!
         ensure-sync-manager-healthy!
         classify-record-kind
         decode-commit-row-payload
         decode-commit-row-payload-header
         durability-profile)

(defn- control
  "Active WAL runtime control supplies I/O lifetime, append-admission checks
  and terminal failure notification."
  [state]
  (some-> (:runtime-control state) deref))

(defn enabled? [info] (true? (:wal? info)))

(defn sync-mode
  [info]
  (let [mode (or (:wal-sync-mode info) c/*wal-sync-mode*)
        profile (durability-profile info)]
    (if (= :extra profile)
      (if (= :none mode) :none :extra)
      mode)))

(defn- sync-on-write?
  [profile mode]
  ;; O_DSYNC makes each segment append durable as the write returns. Keep this
  ;; out of relaxed mode, where explicit group-commit fsyncs are the point.
  (and (not= :relaxed profile)
       (= :fdatasync mode)))

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

(def sync-collect-window-ns
  "Upper bound on adaptive sync collection for a decoupled buffered private
  force. The engine's pending-work predicate ends the wait as soon as the
  collector is idle; this bound only guards a stuck predicate."
  1000000)

(def sync-collect-stall-ns
  "Park slice between re-checks of the engine's pending-work predicate."
  20000)

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

(defn- scanned-record-summary
  [^long segment-id ^String path record]
  (let [payload (tcodec/decode-commit-row-payload-header
                 ^bytes (:body record))
        lsn (long (or (:lsn payload) 0))]
    (when-not (pos? lsn)
      (raise "Txn-log payload missing valid positive LSN"
             {:type :txlog/corrupt
              :segment-id segment-id
              :path path
              :offset (long (:offset record))
              :record record}))
    {:lsn lsn
     :segment-id segment-id
     :offset (long (:offset record))
     :checksum (long (:checksum record))}))

(defn- scan-closed-segment-last-record-summary-once
  [^long segment-id ^File file allow-preallocated-tail?]
  (let [path (.getPath file)
        last-record-summary-v (volatile! nil)
        scan (scan-segment
              path
              {:allow-preallocated-tail? allow-preallocated-tail?
               :collect-records? false
               :on-record
               (fn [record]
                 (vreset! last-record-summary-v
                          (scanned-record-summary
                           segment-id path record)))})]
    (assoc scan :last-record-summary @last-record-summary-v)))

(defn- repair-closed-segment-tail-summary!
  [^long segment-id ^File file cause]
  (let [path (.getPath file)
        repaired
        (try
          (scan-closed-segment-last-record-summary-once
           segment-id file true)
          (catch Exception e
            (if cause
              (throw cause)
              (throw e))))]
    (if (:preallocated-tail? repaired)
      (do
        (truncate-partial-tail!
         path
         {:allow-preallocated-tail? true
          :collect-records? false})
        (scan-closed-segment-last-record-summary-once
         segment-id file false))
      (if cause
        (throw cause)
        (raise "Partial tail found on closed txn-log segment"
               {:type :txlog/corrupt
                :path path})))))

(defn- scan-closed-segment-last-record-summary!
  [^long segment-id ^File file]
  (try
    (let [{:keys [partial-tail?] :as result}
          (scan-closed-segment-last-record-summary-once
           segment-id file false)]
      (if partial-tail?
        (repair-closed-segment-tail-summary! segment-id file nil)
        result))
    (catch Exception e
      (repair-closed-segment-tail-summary! segment-id file e))))

(defn- latest-closed-record-summary!
  [closed-segments]
  (loop [segments (seq (rseq (vec closed-segments)))]
    (when-let [{:keys [id file]} (first segments)]
      (let [summary (:last-record-summary
                     (scan-closed-segment-last-record-summary!
                      (long id)
                      ^File file))]
        (if summary
          summary
          (recur (next segments)))))))

(defn init-runtime-state
  [info marker-state]
  (when (:wal-shared? info)
    (raise "Shared-WAL mode is no longer supported" {:option :wal-shared? :value true}))
  (validate-runtime-config! info)
  (let [dir               (or (:wal-dir info)
                              (str (:dir info) u/+separator+ "txlog"))
        _                 (u/create-dirs dir)
        segments          (segment-files dir)
        closed-segments   (vec (butlast segments))
        active-id         (if (seq segments) (:id (last segments)) 1)
        active-path       (segment-path dir active-id)
        profile           (durability-profile info)
        sync-mode*        (sync-mode info)
        sync-on-write?    (sync-on-write? profile sync-mode*)
        _                 (when-not (.exists (io/file active-path))
                            (let [^FileChannel tmp-ch
                                  (open-segment-channel
                                   active-path
                                   sync-on-write?)]
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
        txlog-records-cache {}
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
        last-from-closed-summary (when (zero? last-from-active)
                                   (latest-closed-record-summary!
                                    closed-segments))
        last-record-summary (or @active-last-record-summary-v
                                last-from-closed-summary)
        last-from-closed (long (or (:lsn last-from-closed-summary) 0))
        last-from-seg     (long (max last-from-active
                                     last-from-closed))
        meta-committed-lsn (long (or (:last-committed-lsn meta-cur) 0))
        meta-durable-lsn   (long (or (:last-durable-lsn meta-cur) 0))
        meta-applied-lsn   (long (or (:last-applied-lsn meta-cur) 0))
        ;; Only the private snapshot/WAL opener supplies this already verified
        ;; floor. It preserves LSNs when retention has removed covered segments.
        last-committed    (long (max last-from-seg
                                     (long (or (:wal-recovery-floor info) 0))))
        last-durable      (long (min last-committed
                                     (max meta-durable-lsn last-from-seg
                                          (long (or (:wal-recovery-floor info) 0)))))
        last-applied      (long (min last-committed
                                     (max 0 meta-applied-lsn)))
        startup-watermark-warning
        (when (or (> meta-committed-lsn last-from-seg)
                  (> meta-durable-lsn last-from-seg)
                  (> meta-applied-lsn last-from-seg))
          {:type :txlog/meta-watermark-overclaim
           :dir dir
           :meta-last-committed-lsn meta-committed-lsn
           :meta-last-durable-lsn meta-durable-lsn
           :meta-last-applied-lsn meta-applied-lsn
           :scanned-last-lsn last-from-seg
           :recovered-last-committed-lsn last-committed
           :recovered-last-durable-lsn last-durable
           :recovered-last-applied-lsn last-applied})
        _                 (when startup-watermark-warning
                            (log/warn
                             "Txn-log meta watermarks exceed scanned WAL; using segment scan"
                             startup-watermark-warning))
        last-sync-ms      (long (or (:updated-ms meta-cur)
                                    (System/currentTimeMillis)))
        now               (System/currentTimeMillis)
        ch                (open-segment-channel active-path sync-on-write?)
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
         :sync-lock-channel      (volatile! nil)
         :recovery-lock-path     (recovery-lock-path dir)
         :maintenance-lock-path  (maintenance-lock-path dir)
         :segment-id             (volatile! active-id)
         :segment-created-ms     (volatile! now)
         :segment-channel        (volatile! ch)
         :segment-offset         (volatile! active-offset)
         :append-lock            (Object.)
         :runtime-control        (volatile! nil)
         :segment-roll-lock      (ReentrantLock.)
         :next-lsn               (volatile! (inc last-committed))
         :segment-max-bytes      (long (segment-max-bytes info))
         :segment-max-ms         (long (segment-max-ms info))
         :segment-prealloc?      prealloc?
         :segment-prealloc-mode  prealloc-mode
         :segment-prealloc-bytes (long (segment-prealloc-bytes info))
         :durability-profile     profile
         :sync-mode              sync-mode*
         :sync-on-write?         sync-on-write?
         :commit-marker?         (commit-marker? info)
         :commit-marker-version  (long (commit-marker-version info))
         :meta-last-applied-lsn  (volatile! last-applied)
         :meta-revision          (volatile! (long (or (:revision meta-cur) -1)))
         :meta-flush-max-txs     (long (meta-flush-max-txs info))
         :meta-flush-max-ms      (long (meta-flush-max-ms info))
         :meta-dirty?            (volatile! false)
         :meta-commits-since-flush (volatile! 0)
         :meta-last-flush-ms     (volatile! last-sync-ms)
         :marker-revision        (volatile! (or (:revision marker-cur)
                                                -1))
         ;; Last successful LMDB commit: transaction ID, marker revision,
         ;; payload LSN. Accessed only while holding the environment write lock.
         :lmdb-commit-metadata   (long-array [-1 -1 0])
         :commit-metadata-write (tcodec/new-commit-metadata)
         ;; Committed LMDB snapshot ID and its full recovery floor, including
         ;; snapshot metadata. Also guarded by the environment write lock.
         :lmdb-runtime-floor    (long-array [-1 0])
         :kv-encode-buffer      (volatile! nil)
         :commit-wait-ms         (long (commit-wait-ms info))

         :sync-manager (new-sync-manager
                         {:last-durable-lsn  last-durable
                          :last-appended-lsn last-committed
                          :last-sync-ms      last-sync-ms
                          :group-commit      (long (group-commit info))
                          :group-commit-ms   (long (group-commit-ms info))
                          :sync-adaptive?    (sync-adaptive? info)
                          ;; Buffered private fsync can combine adjacent
                          ;; records in one force. DSYNC already pays the
                          ;; durability cost at append, so it does not collect here.
                          :collect-window-ns (if sync-on-write?
                                               0 sync-collect-window-ns)
                          :track-trailing?   (not= :relaxed profile)
                           ;; Prepared WAL batches use full-prefix accounting.
                           :full-prefix?      (boolean (:wal-full-prefix? info))})

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
         :txlog-transfer-cache                    (transfer/create-cache)
         :retention-total-bytes                   (volatile! total-bytes)
         ;; Preserve the tail record summary so a clean reopen can validate the
         ;; current commit marker without walking the full retained WAL again.
         :last-record-summary                     last-record-summary
         :startup-warnings                        (cond-> []
                                                    startup-watermark-warning
                                                    (conj startup-watermark-warning))
         :fatal-error                             (volatile! nil)}]
    (when startup-watermark-warning
      (try
        (tseg/with-file-lock
          (tmeta/meta-lock-path dir)
          (fn []
            (let [written (tmeta/write-meta-file!
                           meta-path
                           {:last-committed-lsn last-committed
                            :last-durable-lsn last-durable
                            :last-applied-lsn last-applied
                            :segment-id active-id
                            :segment-offset active-offset
                            :updated-ms now}
                           {:sync-mode :none})]
              (when-let [meta-revision-v (:meta-revision state)]
                (vreset! meta-revision-v (long (:revision written))))
              (when-let [last-applied-v (:meta-last-applied-lsn state)]
                (vreset! last-applied-v
                         (long (:last-applied-lsn written)))))))
        (catch Exception e
          (log/warn e
                    "Failed to repair txn-log meta watermarks after WAL scan"
                    startup-watermark-warning))))
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
                      ^FileChannel next-ch (open-segment-channel
                                            next-path
                                            (boolean (:sync-on-write? state)))
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

(defn- open-reusable-file-lock-channel!
  [^String path]
  (FileChannel/open
   (.toPath (io/file path))
   open-lock-create-write-options))

(defn- try-acquire-reusable-file-lock!
  [^String path ^FileChannel ch]
  (when-let [lock (try
                    (.tryLock ch)
                    (catch OverlappingFileLockException _
                      nil))]
    {:channel ch
     :lock lock
     :path path
     :close-channel? false}))

(defn- try-acquire-sync-lock!
  [state]
  (let [path (:sync-lock-path state)]
    (if-let [channel-v (:sync-lock-channel state)]
      (let [^FileChannel ch (locking channel-v
                              (or @channel-v
                                  (let [ch (open-reusable-file-lock-channel!
                                            path)]
                                    (vreset! channel-v ch)
                                    ch)))]
        (try-acquire-reusable-file-lock! path ch))
      (try-acquire-file-lock! path))))

(defn- release-sync-lock!
  [{:keys [lock close-channel?] :as lock-state}]
  (if (= false close-channel?)
    (when lock
      (try
        (.release ^FileLock lock)
        (catch Exception _)))
    (release-file-lock! lock-state)))

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

(def publish-meta-append! tmeta/publish-meta-append!)

(def publish-meta-commit! tmeta/publish-meta-commit!)

(def publish-meta-durable! tmeta/publish-meta-durable!)

(def publish-meta-current! tmeta/publish-meta-current!)

(def try-with-maintenance-lock tmeta/try-with-maintenance-lock)

(def with-recovery-lock tmeta/with-recovery-lock)

(def note-gc-deleted-bytes! tmeta/note-gc-deleted-bytes!)

(defn- mark-meta-dirty!
  [state]
  (when-let [dirty-v (:meta-dirty? state)]
    (vreset! dirty-v true)))

(defn flush-meta!
  ([state] (flush-meta! state true))
  ([state force?]
   (let [dirty-v (:meta-dirty? state)
         commits-v (:meta-commits-since-flush state)
         commits (long (or (some-> commits-v deref) 0))
         max-txs (long (or (:meta-flush-max-txs state) 0))
         max-ms (long (or (:meta-flush-max-ms state) 0))
         last-flush-v (:meta-last-flush-ms state)
         now-ms (System/currentTimeMillis)
         elapsed-ms (max 0 (- ^long now-ms
                              ^long (long (or (some-> last-flush-v deref)
                                              now-ms))))
         txs-due? (and (pos? max-txs)
                       (>= ^long commits ^long max-txs))
         time-due? (and (pos? max-ms)
                        (>= ^long elapsed-ms ^long max-ms))]
     (when (and (some-> dirty-v deref)
                (or force? txs-due? time-due?))
       (let [written (publish-meta-current! state)]
         (when dirty-v
           (vreset! dirty-v false))
         (when commits-v
           (vreset! commits-v 0))
         (when last-flush-v
           (vreset! last-flush-v now-ms))
         written)))))

(defn note-commit-applied!
  [state {:keys [lsn]}]
  (when-let [last-applied-v (:meta-last-applied-lsn state)]
    (when (> (long lsn) (long @last-applied-v))
      (vreset! last-applied-v (long lsn))
      ;; Append and sync completion also dirty metadata. Count the applied
      ;; transaction once, independently of those watermark changes.
      (when-let [commits-v (:meta-commits-since-flush state)]
        (vreset! commits-v (inc (long @commits-v))))))
  (mark-meta-dirty! state)
  (flush-meta! state false))

(def encode-commit-marker-slot tcodec/encode-commit-marker-slot)

(def decode-commit-marker-slot-bytes tcodec/decode-commit-marker-slot-bytes)

(def vector-checkpoint-op? tcodec/vector-checkpoint-op?)

(def classify-record-kind tcodec/classify-record-kind)

(def use-parallel-row-encoding? tcodec/use-parallel-row-encoding?)

(def tl-commit-body-buffer tcodec/tl-commit-body-buffer)

(def tl-bits-buffer tcodec/tl-bits-buffer)

(def tl-row-encode-buffer tcodec/tl-row-encode-buffer)

(def decode-commit-row-payload tcodec/decode-commit-row-payload)

(def decode-commit-row-payload-header tcodec/decode-commit-row-payload-header)

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
  ([lsn tx-time rows] (tcodec/encode-commit-row-payload lsn tx-time rows))
  ([lsn tx-time rows opts] (tcodec/encode-commit-row-payload lsn tx-time rows opts)))

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

(defn bind-runtime-control!
  "Bind the WAL-only runtime-control reference for the new write protocol.

  The reference supplies only WAL I/O lifetime, append-admission checking and
  terminal failure notification; it cannot capture pending roots, application
  ranges or completion indexes. Returns the previous reference."
  [state control]
  (let [slot (:runtime-control state)
        previous @slot]
    (vreset! slot control)
    previous))

(defn runtime-control
  "The currently bound WAL-only runtime-control reference, or nil."
  [state]
  (some-> (:runtime-control state) deref))

(defn- notify-runtime-failure! [state error]
  (when-let [failed (:on-failure! (control state))]
    (failed error)))

(defn- append-record-under-lock!
  [state ^ByteBuffer body {:keys [throw-if-fatal! before-append! mark-fatal!]}]
  (when-let [check (:check-admission! (control state))]
    (check))
  (when-let [failure (some-> (:fatal-error state) deref)]
    (throw (ex-info "Txn-log runtime is in fatal state" {:type :txlog/fatal}
                    failure)))
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
        _ (ensure-sync-manager-healthy! sync-manager)
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
        _ (tcodec/patch-commit-row-payload-buffer-header! body lsn now)
        append-res (try
                     (tseg/write-record-at! ch offset body)
                     (catch Throwable e
                       ;; A partial record cannot be reused by another caller.
                       ;; Recovery owns the tail once any append I/O fails.
                       (when-let [fatal (:fatal-error state)] (vreset! fatal e))
                       (notify-runtime-failure! state e)
                       (when mark-fatal! (mark-fatal! state e))
                       (throw e)))
        next-offset (+ offset (long (:size append-res)))]
    (when segment-offset
      (vreset! segment-offset next-offset))
    (when-let [total-bytes-v (:retention-total-bytes state)]
      (vreset! total-bytes-v (+ ^long @total-bytes-v
                                ^long (:size append-res))))
    (vreset! lsn-v (inc lsn))
    (mark-meta-dirty! state)
    (append/create lsn sid ch sync-manager append-start-ms
                   (lifetime/deadline (:commit-wait-ms state))
                   (:commit-wait-ms state) near-roll? [append-res])))

(defn- register-append!
  [state batch request-count]
  ;; Publish under the insertion lock, before rotation or another appender can
  ;; observe the new tail. Durability ownership is claimed after handoff.
  (append-sync-transition!
   (append/sync-manager batch) (append/last-lsn batch) (append/started-ms batch)
   {:force? (not= :relaxed (:durability-profile state))
    :begin? false
    :request-count (long (if (not= :relaxed (:durability-profile state))
                           1 request-count))})
  batch)

(defn- append-prepared-record!
  [state rows hooks]
  (let [^ByteBuffer body (tcodec/encode-commit-row-payload-buffer
                         0 0 rows {:ha-term (:ha-term hooks)})
        append-lock (or (:append-lock state) state)]
    (try
      (locking append-lock
        (let [batch (append-record-under-lock! state body hooks)]
          ;; Engine-owned pending state must become visible before another
          ;; caller can capture this LSN in a flush target. No durability or
          ;; native work is permitted in this callback.
          (try
            (when-let [publish (:register-append! hooks)] (publish state batch))
            (register-append! state batch (long (or (:request-count hooks) 1)))
            (catch Throwable e
              (when-let [fatal (:fatal-error state)] (vreset! fatal e))
              (notify-runtime-failure! state e)
              (throw e)))))
      (finally
        (tcodec/release-commit-row-payload-buffer! body)))))

(defn- defer-sync-attempt!
  [{:keys [monitor] :as sync-manager}]
  (locking monitor
    (vreset! (:sync-in-progress? sync-manager) false)
    (.notifyAll monitor)))

(defn- before-sync-round! [state round hook]
  (let [f (:before-sync! (control state))]
    (when f (f state round))
    ;; Standalone admin writes share these hooks with the batch runtime.
    (when (and hook (not (identical? f hook))) (hook state round))))

(defn- after-sync-round! [state round hook]
  (when-let [f (:after-sync! (control state))]
    (f state round))
  (when hook (hook state round)))

(defn- capture-sync-round-prefix
  [state ch round]
  (if-let [capture (:capture-sync-prefix!
                    (control state))]
    (let [target (long (capture state round ch))]
      (when (or (< target (long (:target-lsn round)))
                (> target (long @(:last-appended-lsn (:sync-manager state)))))
        (raise "Sync prefix is outside the appended range"
               {:type :txlog/invalid-sync-prefix
                :target-lsn target :claimed-lsn (:target-lsn round)}))
      (if (= target (long (:target-lsn round)))
        round
        (assoc round :target-lsn target)))
    round))

(defn- perform-sync-round*
  [state ^FileChannel ch sync-manager sync-begin
   {:keys [mark-fatal! before-sync! after-sync!]}]
  (when (:target-lsn sync-begin)
    (if (:sync-on-write? state)
      (try
        (let [sync-begin (capture-sync-round-prefix state ch sync-begin)
              target-lsn (:target-lsn sync-begin)
              reason (:reason sync-begin)
              done-ms (System/currentTimeMillis)]
          (before-sync-round! state sync-begin before-sync!)
          (mark-meta-dirty! state)
          (record-fsync-ms! sync-manager 0 false)
          (complete-sync-success! sync-manager
                                  target-lsn
                                  done-ms
                                  reason
                                  false)
          (after-sync-round! state sync-begin after-sync!)
          {:target-lsn target-lsn
           :sync-done-ms done-ms
           :sync-reason reason})
        (catch Throwable e
          (try
            (when mark-fatal!
              (mark-fatal! state e))
            (finally
              (complete-sync-failure! sync-manager e false)
              (notify-runtime-failure! state e)))
          (throw e)))
      (if-let [lock-state (try-acquire-sync-lock! state)]
        (try
          (let [sync-begin (capture-sync-round-prefix state ch sync-begin)
                target-lsn (:target-lsn sync-begin)
                reason (:reason sync-begin)
                durable-before (long @(:last-durable-lsn sync-manager))]
            (if (<= ^long target-lsn ^long durable-before)
              (let [done-ms (System/currentTimeMillis)]
                (complete-sync-success! sync-manager
                                        target-lsn
                                        done-ms
                                        reason
                                        false)
                (after-sync-round! state sync-begin after-sync!)
                {:target-lsn target-lsn
                 :sync-done-ms done-ms
                 :sync-reason reason})
              (do
                (before-sync-round! state sync-begin before-sync!)
                (let [force-start-ms (System/currentTimeMillis)]
                  (force-segment! state ch (:sync-mode state))
                  (let [force-end-ms (System/currentTimeMillis)]
                    (mark-meta-dirty! state)
                    (record-fsync-ms! sync-manager
                                      (- force-end-ms force-start-ms)
                                      false)
                    (complete-sync-success! sync-manager
                                            target-lsn
                                            force-end-ms
                                            reason
                                            false)
                    (after-sync-round! state sync-begin after-sync!)
                    {:target-lsn target-lsn
                     :sync-done-ms force-end-ms
                     :sync-reason reason})))))
          (catch Throwable e
            (try
              (when mark-fatal!
                (mark-fatal! state e))
              (finally
                (complete-sync-failure! sync-manager e false)
                (notify-runtime-failure! state e)))
            (throw e))
          (finally
            (release-sync-lock! lock-state)))
        (do
          (defer-sync-attempt! sync-manager)
          nil)))))

(defn- perform-sync-round!
  [state ^FileChannel ch sync-manager round hooks]
  ;; Collection and request preparation happen before append publication. A
  ;; durability owner only flushes the appended prefix; it never runs arbitrary
  ;; later request bodies or waits for a collector leader to finish them.
  (try
    (when-let [check (:check-admission! (control state))]
      (check))
    (catch Throwable e
      (try
        (when-let [mark (:mark-fatal! hooks)] (mark state e))
        (finally
          (complete-sync-failure! sync-manager e false)
          (notify-runtime-failure! state e)))
      (throw e)))
  (perform-sync-round* state ch sync-manager round hooks))

(defn- commit-timeout?
  [e]
  (= :txlog/commit-timeout (:type (ex-data e))))

(defn- await-durable-or-sync-available!
  [{:keys [monitor] :as manager} lsn timeout-ms deadline-ns]
  (locking monitor
    (loop []
      (let [last-durable-lsn (long @(:last-durable-lsn manager))
            healthy? (boolean @(:healthy? manager))
            failure @(:failure manager)
            sync-in-progress? (boolean @(:sync-in-progress? manager))]
        (cond
          (<= ^long lsn ^long last-durable-lsn)
          {:durable? true :last-durable-lsn last-durable-lsn}

          (not healthy?)
          (throw (ex-info "Txn-log sync manager is unhealthy"
                          {:type :txlog/unhealthy
                           :lsn lsn}
                          failure))

          (not sync-in-progress?)
          {:durable? false :last-durable-lsn last-durable-lsn}

          :else
          (let [remaining (- (long deadline-ns) (lifetime/nano-time))]
            (if (pos? remaining)
              (do
                (.wait monitor (max 1 (quot remaining 1000000)))
                (recur))
              (raise "Timed out waiting for durable LSN"
                              {:type :txlog/commit-timeout
                               :lsn lsn
                               :timeout-ms timeout-ms}))))))))

(defn- await-durable-retry-window!
  [sync-manager lsn wait-ms]
  (try
    (await-durable-lsn! sync-manager lsn wait-ms (now-ms))
    (catch Exception e
      (if (commit-timeout? e)
        {:durable? false}
        (throw e)))))

(defn- wait-strict-durable!
  ([state ^FileChannel ch sync-manager lsn timeout-ms hooks]
   (wait-strict-durable! state ch sync-manager lsn timeout-ms hooks nil))
  ([state ^FileChannel ch sync-manager lsn timeout-ms hooks initial-sync-begin]
   (let [lsn (long lsn)
         timeout-ms (long timeout-ms)
         deadline (long (or (::wait-deadline-ns hooks)
                            (lifetime/deadline timeout-ms)))]
     (loop [last-sync-ms nil last-sync-reason nil sync-begin initial-sync-begin]
       ;; Settle a claimed sync round before returning or timing out.
       (if sync-begin
         (let [sync-res (perform-sync-round! state ch sync-manager sync-begin hooks)]
           (when-not sync-res
             (await-durable-retry-window!
              sync-manager lsn
              (max 0 (min 5 (quot (- deadline (lifetime/nano-time)) 1000000)))))
           (recur (or (:sync-done-ms sync-res) last-sync-ms)
                  (or (:sync-reason sync-res) last-sync-reason) nil))
         (if (<= lsn (long @(:last-durable-lsn sync-manager)))
           {:sync-done-ms last-sync-ms
            :sync-reason (or last-sync-reason @(:last-sync-reason sync-manager))}
           (let [remaining-ns (- deadline (lifetime/nano-time))
                 remaining (max 0 (quot (+ remaining-ns 999999) 1000000))]
             (when-not (pos? remaining-ns)
               (raise "Timed out waiting for durable LSN"
                      {:type :txlog/commit-timeout :lsn lsn :timeout-ms timeout-ms}))
             (if-let [round (do (await-sync-collection!
                                 sync-manager (more-work-predicate state))
                                (begin-sync! sync-manager lsn))]
               (recur last-sync-ms last-sync-reason round)
               (do
                 (await-durable-or-sync-available!
                  sync-manager lsn remaining deadline)
                 (recur last-sync-ms last-sync-reason nil))))))))))

(defn- await-sync-collection!
  "Wait for adjacent appended records while the engine still reports pending
  work, then let the force claim sync ownership. The predicate is the engine's
  collector activity, so this is adaptive: it batches exactly as long as more
  records are coming and flushes the moment the pipeline is idle. The bounded
  timer only guards a stuck predicate."
  [manager more-work?]
  (when (and more-work?
             (not (boolean @(:sync-in-progress? manager)))
             (> (long @(:last-appended-lsn manager))
                (long @(:last-durable-lsn manager))))
    (let [stall (max 1 (long @(:collect-stall-ns manager)))
          deadline (+ (lifetime/nano-time)
                      (max 1 (long @(:collect-window-ns manager))))]
      (loop []
        (when (and (more-work?)
                   (not (boolean @(:sync-in-progress? manager)))
                   (< (lifetime/nano-time) deadline))
          ;; Park with a precise timeout; an appender unparks this thread after
          ;; publishing a record, so a filling queue wakes us immediately.
          (vreset! (:collect-waiter manager) (Thread/currentThread))
          (try
            (LockSupport/parkNanos (max 1 (min stall
                                                (- deadline (lifetime/nano-time)))))
            (finally (vreset! (:collect-waiter manager) nil)))
          (recur))))))

(defn- more-work-predicate
  "Engine hook: true while the collector still has queued or in-flight work.
  Nil when the engine has not opted into adaptive collection."
  [state]
  (:sync-more-work? (control state)))

(defn- begin-append-sync!
  [manager lsn force? more-work?]
  (await-sync-collection! manager more-work?)
  (locking (:monitor manager)
    ;; An old receipt may outlive its segment. Do not claim a flush of newer
    ;; records using that receipt's channel after its own LSN is durable.
    (when (> (long lsn) (long @(:last-durable-lsn manager)))
      (when force? (request-sync-now! manager))
      (when (or force? @(:sync-requested? manager))
        (begin-sync! manager lsn)))))

(defn- append-durable-relaxed!
  [state batch lsn hooks]
  (let [ch (append/channel batch)
        sync-manager (append/sync-manager batch)
        sync-begin (begin-append-sync! sync-manager lsn false nil)
        sync-res (when sync-begin
                   (perform-sync-round! state
                                        ch
                                        sync-manager
                                        sync-begin
                                        hooks))
        synced? (or (some? sync-res)
                    (<= ^long lsn
                        ^long @(:last-durable-lsn sync-manager)))
        sync-done-ms (:sync-done-ms sync-res)]
    (when (append/near-roll? batch)
      (record-append-near-roll-ms!
       state
       (- (long (or sync-done-ms
                    (System/currentTimeMillis)))
          (long (append/started-ms batch)))))
    synced?))

(defn- append-durable-strict!
  [state batch lsn deadline-ns hooks]
  (let [ch (append/channel batch)
        sync-manager (append/sync-manager batch)
        timeout-ms (long (append/timeout-ms batch))
        append-start-ms (long (append/started-ms batch))
        hooks (assoc hooks ::wait-deadline-ns deadline-ns)
        _ (when (and (>= (lifetime/nano-time) (long deadline-ns))
                     (> (long lsn) (long @(:last-durable-lsn sync-manager))))
            (raise "Timed out waiting for durable LSN"
                   {:type :txlog/commit-timeout :lsn lsn :timeout-ms timeout-ms}))
        sync-begin (begin-append-sync! sync-manager lsn true
                                       (more-work-predicate state))
        done-ms
        (if (and (:sync-on-write? state)
                 (pos? (long timeout-ms))
                 sync-begin)
          ;; This caller owns sync completion and its DSYNC append has already
          ;; returned. Another sync owner still uses waiting below. Hooks remain
          ;; outside both WAL/manager locks.
          (complete-sync-on-write! state sync-manager sync-begin
                                   append-start-ms hooks)
          (let [{:keys [sync-done-ms sync-reason]}
                (wait-strict-durable! state ch sync-manager lsn timeout-ms hooks
                                      sync-begin)
                done-ms (or sync-done-ms (System/currentTimeMillis))]
            (record-commit-wait-ms! sync-manager
                                  (- (long done-ms) (long append-start-ms))
                                  sync-reason false)
            done-ms))]
    (when (append/near-roll? batch)
      (record-append-near-roll-ms! state (- (long done-ms)
                                            (long append-start-ms))))
    true))

(defn- per-tx-durable-profile-state?
  [state]
  (not= :relaxed (:durability-profile state)))

(defn append-pending!
  "Insert a transaction and return a singleton append batch without a sync wait.
  Encoding is outside the insertion lock; LSN, file order, and appended progress
  are published together inside it. Synchronous-write channels still perform
  their configured I/O during append. Shared-WAL callers must retain the existing
  external process-ownership protocol."
  [state rows hooks]
  (with-wal-use state
    (maybe-roll-segment! state (System/currentTimeMillis))
    (append-prepared-record! state rows hooks)))

(defn complete-prefix!
  "Complete an appended LSN under the WAL durability policy, using shared batch
  context and the caller's deadline. Returns whether that LSN is durable. No
  per-request receipt is constructed; call outside insertion/collector locks."
  [state batch lsn deadline-ns hooks]
  (when-not (identical? (:sync-manager state) (append/sync-manager batch))
    (raise "Txn-log append batch belongs to another runtime"
           {:type :txlog/foreign-receipt}))
  (append/record-info batch lsn)
  (with-wal-use state
    (if (per-tx-durable-profile-state? state)
      (append-durable-strict! state batch lsn deadline-ns hooks)
      (append-durable-relaxed! state batch lsn hooks))))

(defn- wal-ownership-deadline
  ^long [state deadline-ns]
  (if (pos? (long (or deadline-ns 0)))
    (long deadline-ns)
    (lifetime/deadline (:commit-wait-ms state))))

(defn- claim-wal-ownership!
  "Claim the runtime's exclusive WAL execution ownership.

  The inline leader and the worker both claim this before append or force, so no
  append can run during a force. Waits under the manager monitor until the
  current owner releases, bounded by `deadline-ns` (zero uses the configured
  commit wait). Returns the owner token."
  [state deadline-ns]
  (let [manager (:sync-manager state)
        ^Object monitor (:monitor manager)
        deadline (wal-ownership-deadline state deadline-ns)
        token (Object.)]
    (locking monitor
      (loop []
        (ensure-sync-manager-healthy! manager)
        (let [remaining (- deadline (lifetime/nano-time))]
          (cond
            (not (pos? remaining))
            (raise "Timed out waiting for WAL ownership"
                   {:type :txlog/commit-timeout
                    :error :txlog/wal-ownership-timeout})

            (nil? @(:wal-owner manager))
            (do (vreset! (:wal-owner manager) token) token)

            :else
            (do (.wait monitor (max 1 (quot remaining 1000000)))
                (recur))))))))

(defn- release-wal-ownership!
  [state token]
  (let [manager (:sync-manager state)
        ^Object monitor (:monitor manager)]
    (locking monitor
      (when (identical? token @(:wal-owner manager))
        (vreset! (:wal-owner manager) nil)
        (.notifyAll monitor)))))

(defn with-wal-owner!
  "Run lifecycle maintenance under the same exclusive owner as append/force.
  Used by private snapshot retention; no native ownership is acquired here."
  [state deadline-ns f]
  (let [token (claim-wal-ownership! state deadline-ns)]
    (try (f) (finally (release-wal-ownership! state token)))))

(defn- release-held-wal-ownership!
  "Release the ownership currently held by this runtime's append/policy span.

  One span is active per environment, so the current owner is that span's own
  claim. No-op when nothing is held."
  [state]
  (let [manager (:sync-manager state)
        ^Object monitor (:monitor manager)]
    (locking monitor
      (when (some? @(:wal-owner manager))
        (vreset! (:wal-owner manager) nil)
        (.notifyAll monitor)))))

(defn- complete-policy-holding!
  "Policy body for a caller that already holds exclusive WAL ownership."
  [state batch deadline-ns]
  (if (per-tx-durable-profile-state? state)
    (let [deadline (if (pos? (long deadline-ns))
                     (long deadline-ns)
                     (lifetime/deadline (:commit-wait-ms state)))]
      (complete-prefix! state batch (append/last-lsn batch) deadline nil))
    (with-wal-use state
      (<= (long (append/last-lsn batch))
          (long @(:last-durable-lsn (:sync-manager state)))))))

(defn- policy-outcome-error
  "Classify a WAL policy-completion failure by the durability actually reached.

  A failure before D reached this record's LSN leaves durability unknown
  (`:indeterminate`). A failure after D advanced, such as an after-sync hook,
  still means the record is durable (`:committed`). The original cause, its data
  and the record's identity (segment, offset, checksum) are preserved."
  [batch cause]
  (let [lsn (long (append/last-lsn batch))
        manager (append/sync-manager batch)
        durable? (boolean
                  (and manager
                       (<= lsn (long @(:last-durable-lsn manager)))))
        identity (merge {:lsn lsn}
                        (try
                          (let [info (append/record-info batch lsn)]
                            {:segment-id (append/segment-id batch)
                             :offset (:offset info)
                             :checksum (:checksum info)})
                          (catch Throwable _ nil)))]
    (ex-info (if durable?
               "WAL durability confirmed but policy completion failed"
               "WAL policy completion failed; durability unconfirmed")
             (merge (ex-data cause)
                    identity
                    {:error (if durable? :txlog/write-committed
                                :txlog/write-indeterminate)
                     :outcome (if durable? :committed :indeterminate)
                     :wal-status (if durable? :durable :appended)
                     :txlog-lsn lsn
                     :retryable? false})
             cause)))

(defn complete-policy!
  "WAL-only completion for one appended group under its configured policy.

  Returns true when the record is policy-complete: strict/extra require
  durability at the configured strength; relaxed follows the count/time policy.
  `deadline-ns` is the caller's absolute monotonic deadline, or zero for the
  runtime's configured commit-wait window. The append batch must belong to this
  runtime; call outside insertion/collector locks.

  This standalone form claims ownership for the policy step alone. Production
  WAL branches append and complete policy as one span (`begin-prepared-group!` /
  `finish-prepared-group!`) so a force cannot claim ownership in between.

  Relaxed completion never forces or waits: the append already registered the
  record and, when a count/time trigger was due, left the manager's
  pending-force flag set for the maintenance owner. It returns whether the
  record is already durable, so native commit can proceed on an appended but
  not-yet-durable record per the relaxed loss window."
  ([state batch]
   (complete-policy! state batch 0))
  ([state batch deadline-ns]
   (when-not (identical? (:sync-manager state) (append/sync-manager batch))
     (raise "Txn-log append batch belongs to another runtime"
            {:type :txlog/foreign-receipt}))
   (append/record-info batch (append/last-lsn batch))
   (let [token (claim-wal-ownership! state deadline-ns)]
     (try
       (complete-policy-holding! state batch deadline-ns)
       (catch Throwable t
         (throw (policy-outcome-error batch t)))
       (finally
         (release-wal-ownership! state token))))))

(defn complete-append!
  "Complete through the batch's last LSN and return its commit metadata for
  native/legacy callers. Engine waiters use complete-prefix! directly."
  [state batch hooks]
  (let [lsn (append/last-lsn batch)
        synced? (complete-prefix! state batch lsn (append/deadline-ns batch) hooks)]
    (assoc (append/commit-info batch lsn) :synced? synced?)))

(defn append-identity
  "Stable record coordinates from a successful append, for reconciliation."
  ([batch]
   (when batch (append-identity batch (append/last-lsn batch))))
  ([batch lsn]
   (when batch
     (let [result (append/record-info batch lsn)]
       {:lsn (long lsn) :segment-id (append/segment-id batch)
        :offset (:offset result) :checksum (:checksum result)}))))

(defn durable-append?
  "Verify a failed request's own record, not just its tentative LSN. This
  bounded read is only on the error/reconciliation path, never normal commit."
  ([state batch]
   (boolean (and batch (durable-append? state batch (append/last-lsn batch)))))
  ([state batch lsn]
   (boolean
    (when (and batch
               (identical? (:sync-manager state) (append/sync-manager batch))
               (<= (long lsn)
                   (long @(:last-durable-lsn (:sync-manager state)))))
      (try
        ;; Own a separate read handle: runtime I/O may already be fenced while
        ;; an application owner is unwinding. No live append handle is used.
        (let [identity (append-identity batch lsn)
              offset (long (:offset identity))
              size (long (:size (append/record-info batch lsn)))
              path (segment-path (:dir state) (:segment-id identity))
              record (first (:records
                             (tseg/scan-segment path
                                                {:start-offset offset
                                                 :max-offset (+ offset size)})))]
          (and record
               (= identity (scanned-record-summary (:segment-id identity) path record))))
        (catch Exception _ false))))))

(defn ^:redef prepare-append-body
  "Serialize canonical rows without assigning their ordered LSN or timestamp.
  Returns independently owned bytes, safe to hand to another thread after the
  encoder's thread-local scratch is reused. Input validation belongs to the
  producer and must precede this call; preparation does not append anything."
  ^bytes [rows hooks]
  (tcodec/encode-commit-row-payload 0 0 rows {:ha-term (:ha-term hooks)}))

(defn- append-write-error
  "Preserve the attempted record's coordinates when writing may have started.
  No append descriptor or complete-record/durability claim is available yet."
  [lsn segment-id offset cause]
  (ex-info "WAL append failed; record completion and durability are unconfirmed"
           (merge (ex-data cause)
                  {:error :txlog/write-indeterminate :outcome :indeterminate
                   :lsn lsn :txlog-lsn lsn :segment-id segment-id :offset offset
                   :retryable? false})
           cause))

(defn append-prepared-batch-pending!
  "Append a collected group as one record/LSN. Exclusively owned bodies from
  prepare-append-body contribute their encoded rows in request order. The
  preparation owner fixes expected-lsn; row serialization precedes collection
  for blind requests, and only the combined header is stamped at insertion.
  Byte writes stay serialized; durability ownership is acquired later.
  Return one shared append descriptor; register-appends! publishes it with all
  roots/entries before advertising the prefix. Bodies must not be reused or
  modified after handoff. No per-transaction durability receipts."
  [state expected-lsn bodies {:keys [before-append! register-appends!] :as hooks}]
  (when (zero? (count bodies))
    (throw (IllegalArgumentException. "A prepared WAL batch cannot be empty")))
  (with-wal-use state
    (let [expected-lsn (long expected-lsn)
          single? (= 1 (count bodies))
          body (when single? (nth bodies 0))
          group (when-not single? (tcodec/prepare-commit-row-group bodies))
          now (System/currentTimeMillis)]
      (maybe-roll-segment! state now)
      (locking (or (:append-lock state) state)
        (when-let [check (:check-admission! (control state))]
          (check))
        (when-let [error (some-> (:fatal-error state) deref)]
          (throw (ex-info "Txn-log runtime is in fatal state" {:type :txlog/fatal} error)))
        (when-let [check (:throw-if-fatal! hooks)] (check state))
        (when-not (= expected-lsn (long @(:next-lsn state)))
          (raise "Prepared WAL prefix changed before insertion"
                 {:type :txlog/stale-preparation :outcome :not-committed}))
        (let [^FileChannel ch @(:segment-channel state)
              sid (long @(:segment-id state))
              offset (long @(:segment-offset state))
              manager (:sync-manager state)]
          (ensure-sync-manager-healthy! manager)
          (when before-append! (before-append! state))
          (when single?
            (tcodec/patch-commit-row-payload-header! body expected-lsn now))
          (try
            (let [result (if single?
                           (tseg/write-record-at! ch offset body)
                           (tseg/write-prepared-record-at! ch offset group expected-lsn now))
                  end (+ (long (:offset result)) (long (:size result)))
                  deadline (lifetime/deadline (:commit-wait-ms state))
                  batch (append/create expected-lsn sid ch manager now deadline
                                       (:commit-wait-ms state) false [result])]
              (vreset! (:segment-offset state) end)
              (when-let [total (:retention-total-bytes state)]
                (vreset! total (+ (long @total) (- end offset))))
              (vreset! (:next-lsn state) (inc expected-lsn))
              (mark-meta-dirty! state)
              (when register-appends! (register-appends! state batch))
              ;; One physical record still represents every logical request
              ;; for the relaxed/adaptive group-commit threshold.
              (append-sync-transition! manager expected-lsn now
                                       {:force? (not= :relaxed (:durability-profile state))
                                        :begin? false
                                        :request-count (or (:request-count hooks)
                                                           (count bodies))})
              batch)
            (catch Throwable e
              (let [error (if (runtime-control state)
                            (append-write-error expected-lsn sid offset e)
                            e)]
                (when-let [fatal (:fatal-error state)] (vreset! fatal e))
                (try
                  (notify-runtime-failure! state error)
                  (when-let [mark (:mark-fatal! hooks)] (mark state e))
                  ;; A notification failure cannot turn attempted WAL I/O into
                  ;; a pre-append rejection or replace its original cause.
                  (finally (throw error)))))))))))

(defn append-batch-pending!
  "Serialize and append a sealed preparation group as one record and LSN.
  Producers with state-independent input should prepare bodies before taking
  preparation ownership and use append-prepared-batch-pending! directly."
  [state expected-lsn rows hooks]
  (append-prepared-batch-pending!
   state expected-lsn (mapv #(prepare-append-body % hooks) rows) hooks))

(defn append-prepared-group!
  "WAL-only insertion for the new write protocol.

  Writes one complete group record at `expected-lsn` and registers its logical
  weight exactly once, with no pending-engine callback, native application or
  durability wait. `bodies` are independently owned prepared row bodies, one per
  logical request, in dispatch order. Returns the shared append descriptor; pass
  it to `complete-policy!`. Requires a private WAL and a bound runtime control."
  [state expected-lsn bodies]
  (when-not (some? @(:runtime-control state))
    (raise "WAL-only insertion requires a bound runtime control"
           {:type :txlog/no-runtime-control}))
  (let [ctl (runtime-control state)
        token (claim-wal-ownership! state 0)]
    (try
      (append-prepared-batch-pending!
       state expected-lsn bodies
       {:throw-if-fatal! (:throw-if-fatal! ctl)
        :before-append! (:before-append! ctl)
        :mark-fatal! (:mark-fatal! ctl)})
      (finally
        (release-wal-ownership! state token)))))

(defn begin-prepared-group!
  "Claim WAL ownership and append one prepared group, retaining ownership for the
  matching `finish-prepared-group!`.

  The pair is one ownership span, so a maintenance force cannot claim ownership
  between the append and its policy completion: it waits for the span to finish,
  and relaxed policy completion is published before the force runs. Returns the
  append descriptor. Requires a private WAL and a bound runtime control.

  `deadline-ns` bounds the ownership wait with the batch's original deadline.
  When supplied, `phase-context` observes :wal-appended inside the ownership
  guard, so even an after-append observer failure releases the claim.
  `request-count` preserves logical sync-threshold weight when encoding has
  combined several requests into fewer bodies; it defaults to the body count."
  ([state expected-lsn bodies]
   (begin-prepared-group! state expected-lsn bodies 0 nil))
  ([state expected-lsn bodies deadline-ns]
   (begin-prepared-group! state expected-lsn bodies deadline-ns nil))
  ([state expected-lsn bodies deadline-ns phase-context]
   (begin-prepared-group! state expected-lsn bodies deadline-ns phase-context
                          (count bodies)))
  ([state expected-lsn bodies deadline-ns phase-context request-count]
   (when-not (some? @(:runtime-control state))
     (raise "WAL-only insertion requires a bound runtime control"
            {:type :txlog/no-runtime-control}))
   (let [ctl (runtime-control state)
         token (try
                 (claim-wal-ownership! state deadline-ns)
                 (catch Throwable t
                   ;; Unlike policy/force waits, this wait precedes any append.
                   (throw (ex-info "WAL append ownership was not acquired"
                                   (assoc (ex-data t) :outcome :not-committed
                                          :retryable? false)
                                   t))))]
     (try
       (let [batch (append-prepared-batch-pending!
                    state expected-lsn bodies
                    {:request-count request-count
                     :throw-if-fatal! (:throw-if-fatal! ctl)
                     :before-append! (:before-append! ctl)
                     :mark-fatal! (:mark-fatal! ctl)})]
         (when phase-context
           (try (phase/phase! :wal-appended phase-context)
                (catch Throwable t
                  (throw (ex-info "WAL appended but policy completion failed"
                                  {:error :txlog/write-indeterminate
                                   :outcome :indeterminate
                                   :txlog-lsn expected-lsn :retryable? false}
                                  t)))))
         batch)
       (catch Throwable t
         (release-wal-ownership! state token)
         ;; Record-writing failures already carry their indeterminate outcome.
         ;; All other failures here precede this request's record write.
         (throw (if (:outcome (ex-data t))
                  t
                  (ex-info "WAL append rejected before record writing"
                           (assoc (ex-data t) :outcome :not-committed
                                  :retryable? false)
                           t))))))))

(defn finish-prepared-group!
  "Complete policy for an append from `begin-prepared-group!` and release the
  span's WAL ownership. Returns the durability status; see `complete-policy!`.
  Ownership is released even when policy completion throws."
  [state batch deadline-ns]
  (when-not (identical? (:sync-manager state) (append/sync-manager batch))
    (raise "Txn-log append batch belongs to another runtime"
           {:type :txlog/foreign-receipt}))
  (append/record-info batch (append/last-lsn batch))
  (try
    (complete-policy-holding! state batch deadline-ns)
    (catch Throwable t
      (throw (policy-outcome-error batch t)))
    (finally
      (release-held-wal-ownership! state))))

(defn append-durable!
  [state rows hooks]
  (complete-append! state (append-pending! state rows hooks) hooks))

(defn append-replay-batch!
  "Append consecutive source records to a private WAL, sharing its durability
  boundary while retaining each LSN and term. Returns the final record's append
  info. The caller holds the store writer lock through materialization. Segment
  size is a soft limit: a fetched batch, like a single record, can cross it."
  [state records {:keys [throw-if-fatal! before-append! mark-fatal!] :as hooks}]
  (when (seq records)
    (maybe-roll-segment! state (System/currentTimeMillis))
    (let [append
          (locking (or (:append-lock state) state)
            (when throw-if-fatal! (throw-if-fatal! state))
            (let [first-lsn (long @(:next-lsn state))
                  now (System/currentTimeMillis)
                  bodies
                  (mapv (fn [^long index record]
                          (let [lsn (+ first-lsn index)]
                            (when-not (= lsn (long (:lsn record)))
                              (raise "Follower replay batch has a nonconsecutive LSN"
                                     {:type :txlog/ha-replay-lsn-mismatch
                                      :expected-lsn lsn :record-lsn (:lsn record)}))
                            (tcodec/encode-commit-row-payload
                             lsn (long (or (:tx-time record) (:ts record) now))
                             (tcodec/compact-replay-rows (:rows record))
                             {:ha-term (:ha-term record)})))
                        (range (count records)) records)
                  ^FileChannel ch @(:segment-channel state)
                  sid (long @(:segment-id state))
                  offset (long @(:segment-offset state))
                  manager (:sync-manager state)]
              (when-not (and ch manager)
                (raise "Txn-log replay runtime is not available"
                       {:type :txlog/no-replay-runtime}))
              (when before-append! (before-append! state))
              (let [results (try
                              (tseg/write-records-at! ch offset bodies)
                              (catch Exception e
                                ;; A partial gathered write may contain complete
                                ;; records. Only recovery may reuse this tail.
                                (when mark-fatal! (mark-fatal! state e))
                                (throw e)))
                    last-result (peek results)
                    end (+ (long (:offset last-result))
                           (long (:size last-result)))
                    lsn (long (:lsn (peek records)))]
                (vreset! (:segment-offset state) end)
                (when-let [total (:retention-total-bytes state)]
                  (vreset! total (+ (long @total) (- end offset))))
                (vreset! (:next-lsn state) (inc lsn))
                (mark-meta-dirty! state)
                (register-append!
                 state
                 (append/create first-lsn sid ch manager now
                                (lifetime/deadline (:commit-wait-ms state))
                                (:commit-wait-ms state)
                                (near-roll-append? state offset) results)
                 1))))]
      (complete-append! state append hooks))))

(defn force-sync!
  [state hooks]
  (with-wal-use state
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
            _ (when-let [before-claim (:before-force-claim! hooks)]
                (before-claim state target-lsn))
            sync-begin (begin-append-sync! sync-manager target-lsn true nil)]
        (wait-strict-durable! state ch sync-manager target-lsn timeout-ms hooks
                              sync-begin)))
    (flush-meta! state true)
    (let [after (sync-manager-state sync-manager)]
      {:target-lsn target-lsn
       :last-appended-lsn (long (:last-appended-lsn after))
       :last-durable-lsn (long (:last-durable-lsn after))
       :pending-count (long (:pending-count after))
       :synced? (<= target-lsn (long (:last-durable-lsn after)))}))))

(defn force-through!
  "Force the WAL through `target-lsn` under an absolute deadline.

  Used by explicit sync, snapshots, rotation and graceful close. Existing
  appended work may finish; the returned map reports durable progress. The force
  is a no-op when `target-lsn` is already durable. `deadline-ns` is a monotonic
  absolute deadline, or zero for the runtime's configured commit-wait window."
  ([state target-lsn deadline-ns]
   (force-through! state target-lsn deadline-ns true))
  ([state target-lsn deadline-ns force-meta?]
  (when-not (some? @(:runtime-control state))
    (raise "WAL-only force requires a bound runtime control"
           {:type :txlog/no-runtime-control}))
  (let [token (claim-wal-ownership! state deadline-ns)]
    (try
      (with-wal-use state
        (let [sync-manager (:sync-manager state)
              timeout-ms (long (:commit-wait-ms state))
              target-lsn (long target-lsn)
              deadline (if (pos? (long deadline-ns))
                         (long deadline-ns)
                         (lifetime/deadline timeout-ms))
              before (sync-manager-state sync-manager)
              durable-lsn (long (:last-durable-lsn before))]
          (when (> target-lsn durable-lsn)
            (let [^FileChannel ch @(:segment-channel state)
                  _ (when-not ch
                      (raise "Txn-log segment channel is not available"
                             {:type :txlog/no-segment-channel}))
                  sync-begin (begin-append-sync! sync-manager target-lsn true nil)]
              (wait-strict-durable! state ch sync-manager target-lsn timeout-ms
                                    {::wait-deadline-ns deadline}
                                    sync-begin)))
          (flush-meta! state force-meta?)
          (let [after (sync-manager-state sync-manager)]
            {:target-lsn target-lsn
             :last-appended-lsn (long (:last-appended-lsn after))
             :last-durable-lsn (long (:last-durable-lsn after))
             :pending-count (long (:pending-count after))
             :synced? (<= target-lsn (long (:last-durable-lsn after)))})))
      (finally
        (release-wal-ownership! state token))))))

(defn pending-sync?
  "WAL-only diagnostic: whether appends left a force armed for maintenance.

  Reads the manager's append-policy flag; it never forces, waits or inspects
  application state."
  [state]
  (boolean (some-> ^clojure.lang.IDeref (:sync-requested? (:sync-manager state))
                   deref)))

(defn service-pending-sync!
  "Service an armed WAL force from the maintenance owner.

  Returns the force result map when a force was armed and performed, or nil when
  nothing was armed or the prefix was already durable. A commandeered-but-stale
  flag is cleared when the whole appended prefix is already durable, so a worker
  cannot spin on a settled request. `deadline-ns` is an absolute monotonic
  deadline, or zero for the runtime's configured commit-wait window. Requires a
  bound runtime control and runs outside insertion/collector locks."
  [state deadline-ns]
  (when (pending-sync? state)
    (let [manager (:sync-manager state)
          target (long @(:last-appended-lsn manager))]
      (if (<= target (long @(:last-durable-lsn manager)))
        (do (locking (:monitor manager)
              (vreset! (:sync-requested? manager) false)
              (vreset! (:sync-request-reason manager) nil))
            nil)
        (force-through! state target deadline-ns false)))))

(declare request-sync-if-needed!)

(defn maintenance-deadline-ns
  "Absolute monotonic deadline for the next relaxed WAL maintenance, or 0.

  Due now when a force is already armed; otherwise the relaxed idle time
  trigger's deadline while unsynced work remains. Returns 0 when nothing is
  pending, when the runtime is unhealthy (a force failure fences waiting), or
  when the time trigger is disabled (`:wal-group-commit-ms 0`). Pure read of
  manager state; the maintenance owner uses it to bound its timed wait."
  ^long [state]
  (let [manager (:sync-manager state)]
    (if-not (and manager (boolean @(:healthy? manager)))
      0
      (let [appended (long @(:last-appended-lsn manager))
            durable (long @(:last-durable-lsn manager))
            unsynced (long @(:unsynced-count manager))
            group-commit-ms (long @(:group-commit-ms manager))]
        (cond
          (<= appended durable) 0
          (boolean @(:sync-requested? manager)) (lifetime/nano-time)
          (and (pos? unsynced) (pos? group-commit-ms))
          (let [due-in-ms (- (+ (long @(:last-sync-ms manager)) group-commit-ms)
                             (System/currentTimeMillis))]
            (+ (lifetime/nano-time) (* 1000000 due-in-ms)))
          :else 0)))))

(defn service-maintenance!
  "Drive relaxed WAL maintenance and force any due prefix.

  Re-evaluates the count/time trigger, then forces the armed prefix. Returns the
  force result map, or nil when nothing was due. The maintenance owner calls
  this between batch tasks and after its timed wait; it requires a bound runtime
  control and runs outside insertion/collector locks."
  [state deadline-ns]
  (request-sync-if-needed! (:sync-manager state))
  (service-pending-sync! state deadline-ns))

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
           track-trailing?
           full-prefix?
           collect-window-ns
           collect-stall-ns]
    :or {last-durable-lsn 0
         last-appended-lsn 0
         last-sync-ms 0
         group-commit 100
         group-commit-ms 100
         sync-adaptive? true
         track-trailing? true
         full-prefix? false
         collect-window-ns 0
         collect-stall-ns sync-collect-stall-ns}}]
  (let [last-durable-lsn* (long last-durable-lsn)
        last-appended-lsn* (long last-appended-lsn)
        full-prefix? (boolean full-prefix?)
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
   :collect-window-ns (volatile! (long collect-window-ns))
   :collect-stall-ns (volatile! (long collect-stall-ns))
   :collect-waiter (volatile! nil)
   :sync-adaptive? (boolean sync-adaptive?)
   ;; Full-prefix mode owns D as an LSN prefix and clears U when a captured-A
   ;; force confirms, so it retains no per-LSN weight tail.
   :track-trailing? (boolean (and track-trailing? (not full-prefix?)))
   :full-prefix? full-prefix?
   :sync-count-by-reason (zero-sync-reason-array)
   :batched-sync-count (volatile! 0)
   :forced-sync-count (volatile! 0)
   :last-sync-reason (volatile! nil)
   :unsynced-count (volatile! pending0)
   ;; Physical LSNs remain unchanged. Only grouped private-WAL appends need
   ;; extra weights, retained until their own LSN is durably synced.
   :pending-group-counts (ArrayDeque.)
   :pending-group-extra-count (volatile! 0)
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
   :failure (volatile! nil)
   ;; New-mode exclusive WAL execution owner. Append and force claim it so an
   ;; append can never run during a force (which would invalidate full-prefix
   ;; accounting). Compatibility never claims it.
   :wal-owner (volatile! nil)}))

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

(defn- record-commit-wait-under-monitor!
  [manager duration-ms now reason]
  (let [v (long (max 0 (long (or duration-ms 0))))
        reason* (normalize-sync-reason
                 (or reason @(:last-sync-reason manager) :unknown))
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
               (long (inc (long (aget wait-counts idx)))))))

(defn record-commit-wait-ms!
  ([manager duration-ms]
   (record-commit-wait-ms! manager duration-ms nil true))
  ([manager duration-ms reason]
   (record-commit-wait-ms! manager duration-ms reason true))
  ([{:keys [monitor] :as manager} duration-ms reason snapshot?]
   (let [now (now-ms)]
     (locking monitor
       (record-commit-wait-under-monitor! manager duration-ms now reason)
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

(defn reset-group-counts!
  "Clear logical request weights when resetting the WAL recovery floor.
  The caller owns the sync manager or the runtime state guard."
  [manager]
  (.clear ^ArrayDeque (:pending-group-counts manager))
  (vreset! (:pending-group-extra-count manager) 0))

(defn- drop-durable-group-counts!
  [manager ^long durable-lsn]
  (let [^ArrayDeque queue (:pending-group-counts manager)
        extra-count (:pending-group-extra-count manager)]
    (loop [remaining (long @extra-count)]
      (if-let [^longs entry (.peekFirst queue)]
        (if (<= (aget entry 0) durable-lsn)
          (do (.removeFirst queue)
              (recur (- remaining (aget entry 1))))
          (vreset! extra-count remaining))
        (vreset! extra-count remaining)))))

(defn- request-sync-on-append-under-monitor!
  [manager lsn now request-count]
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
        extra-count (if (pos? appended-delta)
                      (max 0 (dec (long request-count))) 0)
        unsynced-after (+ unsynced-count appended-delta extra-count)
        elapsed (max 0 (- ^long (long now) ^long last-sync-ms))
        count? (>= ^long unsynced-after ^long group-commit)
        time? (and (pos? unsynced-after)
                   (pos? group-commit-ms)
                   (>= elapsed group-commit-ms))
        reason (cond
                 count? :batch-count
                 time? :batch-time
                 :else nil)]
    (when (and (not (:full-prefix? manager)) (pos? extra-count))
      (.addLast ^ArrayDeque (:pending-group-counts manager)
                (long-array [lsn* extra-count]))
      (vreset! (:pending-group-extra-count manager)
               (+ (long @(:pending-group-extra-count manager)) extra-count)))
    (vreset! (:last-appended-lsn manager) new-appended)
    (vreset! (:unsynced-count manager) unsynced-after)
    ;; Wake a collection wait so it can cover this record before claiming.
    (when-let [waiter @(:collect-waiter manager)]
      (LockSupport/unpark ^Thread waiter))
    (.notifyAll ^Object (:monitor manager))
    (when (and reason (not sync-requested?))
      (vreset! (:sync-requested? manager) true)
      (vreset! (:sync-request-reason manager) reason)
      {:request? true
       :reason reason})))

(defn request-sync-on-append!
  ([manager lsn] (request-sync-on-append! manager lsn (now-ms)))
  ([{:keys [monitor] :as manager} lsn now]
   (locking monitor
     (request-sync-on-append-under-monitor! manager lsn now 1))))

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
      (if (and (> last-appended-lsn last-durable-lsn)
               (or (nil? lsn) (> (long lsn) last-durable-lsn)))
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
        (when (and sync-requested? (<= last-appended-lsn last-durable-lsn))
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
   :request-count weights this record's logical requests for the sync threshold.
   Returns the optional begin-sync payload for the caller to perform fsync.
   :begin? false publishes progress without reserving a sync owner."
  ([sync-manager lsn now]
   (append-sync-transition! sync-manager lsn now {}))
  ([{:keys [monitor] :as sync-manager} lsn now
    {:keys [force? begin-lsn request-count begin?]
     :or {force? false begin-lsn nil request-count 1 begin? true}}]
   (locking monitor
     (let [requested? (boolean
                       (request-sync-on-append-under-monitor! sync-manager
                                                              lsn
                                                              now
                                                              request-count))
           _ (when force?
               (request-sync-now-under-monitor! sync-manager))
           sync-begin (when (and begin? (or force? requested?))
                        (begin-sync-under-monitor! sync-manager begin-lsn))]
       sync-begin))))

(defn- complete-sync-success-under-monitor!
  [{:keys [monitor] :as manager} target-lsn now reason]
  (let [last-durable-lsn (long @(:last-durable-lsn manager))
        last-appended-lsn (long @(:last-appended-lsn manager))
        full-prefix? (boolean (:full-prefix? manager))
        sync-requested? (boolean @(:sync-requested? manager))
        sync-request-reason @(:sync-request-reason manager)
        target (long (or target-lsn last-appended-lsn last-durable-lsn))
        ;; Full-prefix mode confirms the whole captured append prefix, so D
        ;; covers A and U clears. Compatibility keeps weighted-tail accounting.
        durable (if full-prefix?
                  (max ^long last-durable-lsn ^long last-appended-lsn)
                  (max ^long last-durable-lsn target))
        _ (when-not full-prefix? (drop-durable-group-counts! manager durable))
        pending-after (if full-prefix?
                        0
                        (+ (max 0 (- last-appended-lsn durable))
                           (long @(:pending-group-extra-count manager))))
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
    (.notifyAll monitor)))

(defn complete-sync-success!
  ([manager] (complete-sync-success! manager nil (now-ms) nil true))
  ([manager target-lsn now]
   (complete-sync-success! manager target-lsn now nil true))
  ([manager target-lsn now reason]
   (complete-sync-success! manager target-lsn now reason true))
  ([{:keys [monitor] :as manager} target-lsn now reason snapshot?]
   (locking monitor
     (complete-sync-success-under-monitor! manager target-lsn now reason)
     (when snapshot?
       (sync-manager-state manager)))))

(defn- complete-sync-on-write!
  "Complete an owned private-WAL DSYNC append without a durability wait loop."
  [state manager sync-begin append-start-ms
   {:keys [before-sync! after-sync! mark-fatal!]}]
  (try
    (before-sync-round! state sync-begin before-sync!)
    (let [done-ms (System/currentTimeMillis)
          reason (:reason sync-begin)]
      (mark-meta-dirty! state)
      (locking (:monitor manager)
        (vreset! (:last-fsync-ms manager) 0)
        (vreset! (:last-fsync-at-ms manager) done-ms)
        (complete-sync-success-under-monitor!
         manager (:target-lsn sync-begin) done-ms reason)
        (record-commit-wait-under-monitor!
         manager (- done-ms (long append-start-ms)) done-ms reason))
      (after-sync-round! state sync-begin after-sync!)
      done-ms)
    (catch Throwable e
      (try
        (when mark-fatal!
          (mark-fatal! state e))
        (finally
          (complete-sync-failure! manager e false)
          (notify-runtime-failure! state e)))
      (throw e))))

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
               (raise "Timed out waiting for durable LSN"
                             {:type :txlog/commit-timeout
                              :lsn lsn
                              :timeout-ms timeout-ms})))))))))
