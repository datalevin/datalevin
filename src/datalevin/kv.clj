;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.kv
  "KV-layer helpers for txn-log APIs and floor-provider bookkeeping."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.binding.cpp :as cpp]
   [datalevin.constants :as c]
   [datalevin.custom-kv :as custom-kv]
   [datalevin.interface :as i]
   [datalevin.kv.txlog :refer [ensure-fast-list rows-vector txlog-record-lsn
                               txlog-records txlog-records-for-recovery]]
   [datalevin.lmdb :as l]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u :refer [deftype+ raise]])
  (:import
   [datalevin.lmdb DatomKVTxData]
   [org.eclipse.collections.impl.list.mutable FastList]))

(declare txlog-retention-state-map
         delete-txlog-segment!
         snapshot-current-lsn
         txlog-snapshot-floor-state
         txlog-recovery-context
         txlog-force-sync!
         txlog-config-enabled?
         txlog-rollout-mode
         txlog-rollout-watermarks
         ensure-txlog-ready!
         txlog-write-path-enabled?
         write-txn-open?
         txlog-watermarks-map
         txlog-update-snapshot-floor-state!
         txlog-clear-snapshot-floor-state!
         txlog-update-replica-floor-state!
         txlog-clear-replica-floor-state!
         txlog-pin-backup-floor-state!
         txlog-unpin-backup-floor-state!
         reset-txlog-runtime-for-snapshot!
         txlog-mark-recovery-source!
         persisted-payload-floor-lsn
         persisted-runtime-floor-lsn
         ->KVLMDB)

(declare raw-lmdb)

(def ^:dynamic *after-txlog-append-fn*
  nil)

(defn- run-after-txlog-append!
  [context]
  (when-let [f *after-txlog-append-fn*]
    (f context)))

(defn txlog-watermarks
  [db]
  (try
    (i/txlog-watermarks db)
    (catch IllegalArgumentException _
      (if-let [state (txlog/state db)]
        (txlog-watermarks-map db state)
        (if (txlog-config-enabled? db)
          (txlog-rollout-watermarks db (txlog-rollout-mode db))
          {:wal? false})))))

(defn open-tx-log
  ([db from-lsn]
   (i/open-tx-log db from-lsn))
  ([db from-lsn upto-lsn]
   (i/open-tx-log db from-lsn upto-lsn)))

(defn open-tx-log-rows
  ([db from-lsn]
   (open-tx-log-rows db from-lsn nil))
  ([db from-lsn upto-lsn]
   (if-let [state (or (txlog/state db)
                      (when (txlog-write-path-enabled? db)
                        (ensure-txlog-ready! db)))]
     (do
       (txlog/refresh-shared-state! state)
       (txlog/select-open-record-rows
        (txlog-records state from-lsn upto-lsn)
        from-lsn
        upto-lsn))
     (if (txlog-config-enabled? db)
       []
       (txlog/select-open-record-rows
        (txlog-records (txlog/enabled-state db) from-lsn upto-lsn)
        from-lsn
        upto-lsn)))))

(defn force-txlog-sync!
  [db]
  (i/force-txlog-sync! db))

(defn force-lmdb-sync!
  [db]
  (i/force-lmdb-sync! db))

(defn create-snapshot!
  [db]
  (i/create-snapshot! db))

(defn list-snapshots
  [db]
  (i/list-snapshots db))

(defn snapshot-scheduler-state
  [db]
  (i/snapshot-scheduler-state db))

(defn read-commit-marker
  [db]
  (i/read-commit-marker db))

(defn verify-commit-marker!
  [db]
  (i/verify-commit-marker! db))

(defn txlog-retention-state
  [db]
  (if-let [state (txlog-retention-state-map db nil false)]
    (dissoc state :gc-target-segments)
    (i/txlog-retention-state db)))

(defn gc-txlog-segments!
  ([db]
   (gc-txlog-segments! db nil))
  ([db retain-floor-lsn]
   (if-let [before (txlog-retention-state-map db retain-floor-lsn true)]
     (let [targets (:gc-target-segments before)
           deleted (mapv delete-txlog-segment! targets)
           deleted-bytes (reduce (fn [acc {:keys [bytes]}]
                                   (+ ^long acc ^long bytes))
                                 0 targets)
           _ (when-let [state (txlog/state db)]
               (txlog/note-gc-deleted-bytes! state deleted-bytes))
           after (txlog-retention-state-map db retain-floor-lsn false)]
       {:ok? true
        :deleted-count (count deleted)
        :deleted-bytes deleted-bytes
        :deleted-segment-ids (mapv :segment-id targets)
        :deleted-segments deleted
        :operator-retain-floor-lsn
        (get-in before [:floors :operator-retain-floor-lsn])
        :before (dissoc before :gc-target-segments)
        :after (dissoc after :gc-target-segments)})
     (i/gc-txlog-segments! db retain-floor-lsn))))

(declare with-runtime-txlog-state-guard
         with-write-txn-lock-before-runtime-txlog-state)

(defn- txlog-retention-state-local
  [db]
  (with-runtime-txlog-state-guard
    db
    (fn []
      (if-let [state (txlog-retention-state-map db nil false)]
        (dissoc state :gc-target-segments)
        (if (txlog-config-enabled? db)
          (let [rollout-mode (txlog-rollout-mode db)]
            {:wal? true
             :skipped? true
             :reason :rollback
             :watermarks (txlog-rollout-watermarks db rollout-mode)})
          {:wal? false})))))

(defn- gc-txlog-segments-local!
  [db retain-floor-lsn]
  (with-runtime-txlog-state-guard
    db
    (fn []
      (if-let [before (txlog-retention-state-map db retain-floor-lsn true)]
        (let [targets (:gc-target-segments before)
              deleted (mapv delete-txlog-segment! targets)
              deleted-bytes (reduce (fn [acc {:keys [bytes]}]
                                      (+ ^long acc ^long bytes))
                                    0 targets)
              _ (when-let [state (txlog/state db)]
                  (txlog/note-gc-deleted-bytes! state deleted-bytes))
              after (txlog-retention-state-map db retain-floor-lsn false)]
          {:ok? true
           :deleted-count (count deleted)
           :deleted-bytes deleted-bytes
           :deleted-segment-ids (mapv :segment-id targets)
           :deleted-segments deleted
           :operator-retain-floor-lsn
           (get-in before [:floors :operator-retain-floor-lsn])
           :before (dissoc before :gc-target-segments)
           :after (dissoc after :gc-target-segments)})
        (if (txlog-config-enabled? db)
          (let [rollout-mode (txlog-rollout-mode db)]
            {:ok? false
             :skipped? true
             :reason :rollback
             :retain-floor-lsn retain-floor-lsn
             :watermarks (txlog-rollout-watermarks db rollout-mode)})
          {:ok? false
           :skipped? true
           :reason :wal-disabled
           :retain-floor-lsn retain-floor-lsn
           :watermarks {:wal? false}})))))

(defn txlog-update-snapshot-floor!
  ([db snapshot-lsn]
   (txlog-update-snapshot-floor-state! db snapshot-lsn nil))
  ([db snapshot-lsn previous-snapshot-lsn]
   (txlog-update-snapshot-floor-state! db snapshot-lsn previous-snapshot-lsn)))

(defn txlog-clear-snapshot-floor!
  [db]
  (txlog-clear-snapshot-floor-state! db))

(defn txlog-update-replica-floor!
  [db replica-id applied-lsn]
  (txlog-update-replica-floor-state! db replica-id applied-lsn))

(defn txlog-clear-replica-floor!
  [db replica-id]
  (txlog-clear-replica-floor-state! db replica-id))

(defn txlog-pin-backup-floor!
  ([db pin-id floor-lsn]
   (txlog-pin-backup-floor-state! db pin-id floor-lsn nil))
  ([db pin-id floor-lsn expires-ms]
   (txlog-pin-backup-floor-state! db pin-id floor-lsn expires-ms)))

(defn txlog-unpin-backup-floor!
  [db pin-id]
  (txlog-unpin-backup-floor-state! db pin-id))

(def ^:private snapshot-meta-file-name "snapshot.edn")
(def ^:private snapshot-current-slot "current")
(def ^:private snapshot-previous-slot "previous")
(def ^:private snapshot-default-interval-ms 3600000)
(def ^:private snapshot-default-max-lsn-delta 1000000)
(def ^:private snapshot-default-max-log-bytes-delta (* 4 1024 1024 1024))
(def ^:private snapshot-default-max-age-ms 21600000)
(def ^:private snapshot-default-defer-on-contention? true)
(def ^:private snapshot-default-contention-thresholds
  {:commit-wait-p99-ms 25
   :queue-depth 1024
   :fsync-p99-ms 20})
(def ^:private snapshot-default-contention-sample-max-age-ms 30000)
(def ^:private snapshot-default-defer-backoff-min-ms 1000)
(def ^:private snapshot-default-defer-backoff-max-ms 60000)

(def ^:dynamic *wal-snapshot-copy-failpoint*
  nil)

(defn- maybe-run-txn-log-snapshot-copy-failpoint!
  [context]
  (when (fn? *wal-snapshot-copy-failpoint*)
    (*wal-snapshot-copy-failpoint* context)))

(def ^:dynamic *wal-copy-backup-pin-failpoint*
  nil)

(def ^:dynamic *wal-copy-backup-pin-observer*
  nil)

(def ^:dynamic *wal-copy-backup-pin-enabled?*
  true)

(defn- maybe-notify-txn-log-copy-backup-pin-observer!
  [context]
  (when (fn? *wal-copy-backup-pin-observer*)
    (*wal-copy-backup-pin-observer* context)))

(defn- maybe-run-txn-log-copy-backup-pin-failpoint!
  [context]
  (when (fn? *wal-copy-backup-pin-failpoint*)
    (*wal-copy-backup-pin-failpoint* context)))

(defn- lmdb-sync-interval-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})
        interval-ms (:lmdb-sync-interval-ms opts)
        interval-s (:lmdb-sync-interval opts)
        fallback-ms (long (* 1000 (long c/lmdb-sync-interval)))]
    (long (max 1
               (long
                (cond
                  (number? interval-ms) (long interval-ms)
                  (number? interval-s) (long (* 1000 (long interval-s)))
                  :else fallback-ms))))))

(defn- checkpoint-stale-threshold-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})
        configured (:wal-checkpoint-stale-threshold-ms opts)
        fallback (long (* 2 (long (lmdb-sync-interval-ms lmdb))))]
    (long (max 1
               (if (number? configured)
                 (long configured)
                 fallback)))))

(defn- txlog-lag-alert-threshold-lsn
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})
        configured (:wal-lag-alert-threshold-lsn opts)
        fallback (* 2 (long (or (:wal-group-commit opts)
                                c/*wal-group-commit*)))]
    (long (max 1
               (if (number? configured)
                 (long configured)
                 fallback)))))

(defn- mark-lmdb-checkpoint!
  [lmdb]
  (let [now-ms (System/currentTimeMillis)]
    (when-let [info-v (i/kv-info lmdb)]
      (vswap! info-v
              (fn [m]
                (-> m
                    (assoc :txlog-last-checkpoint-ms now-ms
                           :txlog-last-lmdb-sync-ms now-ms
                           :txlog-last-lmdb-sync-error nil)
                    (update :txlog-lmdb-sync-count (fnil inc 0))))))
    now-ms))

(defonce ^:private storage-fault-hook* (atom nil))

(defn- force-lmdb-sync-now!
  [lmdb]
  (try
    (when-let [f @storage-fault-hook*]
      (f (merge (select-keys (or (i/env-opts lmdb) {})
                             [:db-identity :ha-node-id :db-name])
                {:stage :lmdb-sync})))
    (i/sync lmdb 1)
    (mark-lmdb-checkpoint! lmdb)
    (catch Exception e
      (when-let [info-v (i/kv-info lmdb)]
        (vswap! info-v assoc :txlog-last-lmdb-sync-error (.getMessage e)))
      (throw e))))

(defn set-storage-fault-hook!
  "Install a testing hook that can block or fail specific write-path stages.
   The hook receives a context map with at least `:stage`, `:db-identity`,
   `:ha-node-id`, and `:db-name` when available."
  [f]
  (reset! storage-fault-hook* f))

(defn clear-storage-fault-hook!
  []
  (reset! storage-fault-hook* nil))

(defn- maybe-run-storage-fault-context!
  [fault-context stage extra]
  (when-let [f @storage-fault-hook*]
    (f (merge fault-context {:stage stage} extra))))

(defn- maybe-run-storage-fault-lmdb!
  [lmdb stage extra]
  (maybe-run-storage-fault-context!
   (select-keys (or (i/env-opts lmdb) {})
                [:db-identity :ha-node-id :db-name])
   stage
   extra))

(defn- snapshot-root-dir
  [lmdb]
  (let [opts (i/env-opts lmdb)]
    (or (:snapshot-dir opts)
        (str (i/env-dir lmdb) u/+separator+ "snapshots"))))

(defn- snapshot-slot-path
  [root-dir slot-name]
  (str root-dir u/+separator+ slot-name))

(defn- snapshot-meta-path
  [slot-path]
  (str slot-path u/+separator+ snapshot-meta-file-name))

(defn- snapshot-compact?
  [lmdb]
  (let [opts (i/env-opts lmdb)]
    (if (contains? opts :snapshot-compact?)
      (boolean (:snapshot-compact? opts))
      true)))

(defn- read-snapshot-meta
  [slot-path]
  (let [path (snapshot-meta-path slot-path)]
    (when (u/file-exists path)
      (try
        (let [v (edn/read-string (slurp path))]
          (if (map? v) v {:value v}))
        (catch Exception e
          {:corrupt? true
           :error (.getMessage e)})))))

(defn- write-snapshot-meta!
  [slot-path meta]
  (spit (snapshot-meta-path slot-path) (str (pr-str meta) "\n")))

(defn- move-dir!
  [src dst]
  (java.nio.file.Files/move
   (.toPath (io/file src))
   (.toPath (io/file dst))
   (into-array java.nio.file.CopyOption
               [java.nio.file.StandardCopyOption/REPLACE_EXISTING])))

(defn- copy-dir-contents!
  [src-dir dest-dir]
  (u/create-dirs dest-dir)
  (doseq [^java.io.File f (or (u/list-files src-dir) [])]
    (let [dst (str dest-dir u/+separator+ (.getName f))]
      (if (.isDirectory f)
        (copy-dir-contents! (.getPath f) dst)
        (u/copy-file (.getPath f) dst)))))

(defn- update-snapshot-slot-meta!
  [slot-path slot]
  (when-let [meta (read-snapshot-meta slot-path)]
    (when-not (:corrupt? meta)
      (write-snapshot-meta! slot-path (assoc meta :slot slot)))))

(defn- snapshot-entry
  [slot slot-path]
  (when (u/file-exists slot-path)
    (let [f (io/file slot-path)
          meta (or (read-snapshot-meta slot-path) {})
          created-ms (long (or (:created-ms meta)
                               (.lastModified ^java.io.File f)))
          bytes (u/dir-size f)]
      (-> meta
          (assoc :slot slot
                 :path slot-path
                 :exists? true
                 :created-ms created-ms
                 :bytes bytes)
          (update :snapshot-id #(or % (str (name slot) "-" created-ms)))))))

(defn- list-snapshot-entries
  [lmdb]
  (let [root-dir (snapshot-root-dir lmdb)
        current-path (snapshot-slot-path root-dir snapshot-current-slot)
        previous-path (snapshot-slot-path root-dir snapshot-previous-slot)]
    (->> [(snapshot-entry :current current-path)
          (snapshot-entry :previous previous-path)]
         (remove nil?)
         vec)))

(defn- snapshot-scheduler-enabled?
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (if (contains? opts :snapshot-scheduler?)
      (boolean (:snapshot-scheduler? opts))
      false)))

(defn- snapshot-interval-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (or (:snapshot-interval-ms opts)
              snapshot-default-interval-ms))))

(defn- snapshot-max-lsn-delta
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (or (:snapshot-max-lsn-delta opts)
              snapshot-default-max-lsn-delta))))

(defn- snapshot-max-log-bytes-delta
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (or (:snapshot-max-log-bytes-delta opts)
              snapshot-default-max-log-bytes-delta))))

(defn- snapshot-max-age-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (or (:snapshot-max-age-ms opts)
              snapshot-default-max-age-ms))))

(defn- snapshot-defer-on-contention?
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (if (contains? opts :snapshot-defer-on-contention?)
      (boolean (:snapshot-defer-on-contention? opts))
      snapshot-default-defer-on-contention?)))

(defn- snapshot-contention-thresholds
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})
        thresholds (merge snapshot-default-contention-thresholds
                          (or (:snapshot-contention-thresholds opts) {}))]
    {:commit-wait-p99-ms
     (long (max 0 (long (or (:commit-wait-p99-ms thresholds) 0))))
     :queue-depth
     (long (max 0 (long (or (:queue-depth thresholds) 0))))
     :fsync-p99-ms
     (long (max 0 (long (or (:fsync-p99-ms thresholds) 0))))}))

(defn- snapshot-contention-sample-max-age-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (max 0 (long (or (:snapshot-contention-sample-max-age-ms opts)
                           snapshot-default-contention-sample-max-age-ms))))))

(defn- snapshot-defer-backoff-min-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (max 0 (long (or (:snapshot-defer-backoff-min-ms opts)
                           snapshot-default-defer-backoff-min-ms))))))

(defn- snapshot-defer-backoff-max-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})
        min-ms (long (snapshot-defer-backoff-min-ms lmdb))
        requested (long (max 0 (long (or (:snapshot-defer-backoff-max-ms opts)
                                         snapshot-default-defer-backoff-max-ms))))]
    (long (max min-ms requested))))

(defn- parse-offpeak-minute
  [v]
  (cond
    (number? v)
    (let [n (long v)]
      (when (and (<= 0 n) (< n (* 24 60)))
        n))

    (string? v)
    (when-let [[_ hh mm] (re-matches #"^(\d{1,2}):(\d{2})$" v)]
      (let [h (Long/parseLong hh)
            m (Long/parseLong mm)]
        (when (and (<= 0 h) (< h 24)
                   (<= 0 m) (< m 60))
          (+ (* 60 h) m))))

    :else nil))

(defn- parse-offpeak-window
  [window]
  (let [[start-raw end-raw]
        (cond
          (and (map? window)
               (or (contains? window :start)
                   (contains? window :from))
               (or (contains? window :end)
                   (contains? window :to)))
          [(or (:start window) (:from window))
           (or (:end window) (:to window))]

          (and (vector? window) (= 2 (count window)))
          [(nth window 0) (nth window 1)]

          :else nil)
        start-min (parse-offpeak-minute start-raw)
        end-min (parse-offpeak-minute end-raw)]
    (when (and (some? start-min) (some? end-min))
      {:start-min start-min
       :end-min end-min
       :start start-raw
       :end end-raw})))

(defn- snapshot-offpeak-windows
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (vec (keep parse-offpeak-window
               (or (:snapshot-offpeak-windows opts) [])))))

(defn- local-minute-of-day
  [now-ms]
  (let [now (java.time.Instant/ofEpochMilli (long now-ms))
        zdt (.atZone now (java.time.ZoneId/systemDefault))
        time (.toLocalTime zdt)]
    (+ (* 60 (.getHour ^java.time.LocalTime time))
       (.getMinute ^java.time.LocalTime time))))

(defn- in-offpeak-window?
  [windows now-ms]
  (if (empty? windows)
    true
    (let [minute (long (local-minute-of-day now-ms))]
      (boolean
       (some
        (fn [{:keys [start-min end-min]}]
          (let [start-min (long start-min)
                end-min (long end-min)]
            (cond
              (= start-min end-min)
              true

              (< start-min end-min)
              (and (<= start-min minute)
                   (< minute end-min))

              :else
              (or (<= start-min minute)
                  (< minute end-min)))))
        windows)))))

(defn- snapshot-scheduler-contention-state
  [lmdb now-ms]
  (when-let [state (txlog/state lmdb)]
    (let [sync-state (txlog/sync-manager-state (:sync-manager state))
          sample-age-ms (long (snapshot-contention-sample-max-age-ms lmdb))
          recent-sample? (fn [sample-at]
                           (and (number? sample-at)
                                (<= (long (max 0
                                               (- (long now-ms)
                                                  (long sample-at))))
                                    sample-age-ms)))
          commit-wait-ms (when (recent-sample?
                                (:last-commit-wait-at-ms sync-state))
                           (long (or (:last-commit-wait-ms sync-state) 0)))
          fsync-ms (when (recent-sample? (:last-fsync-at-ms sync-state))
                     (long (or (:last-fsync-ms sync-state) 0)))]
      {:queue-depth (long (or (:pending-count sync-state) 0))
       :commit-wait-ms commit-wait-ms
       :fsync-ms fsync-ms
       :sample-max-age-ms sample-age-ms
       :sync-state sync-state})))

(defn- snapshot-scheduler-poll-ms
  [lmdb]
  (let [interval-ms (long (snapshot-interval-ms lmdb))
        quarter-ms (if (pos? interval-ms)
                     (quot interval-ms 4)
                     1000)]
    (long (max 50 (min 60000 quarter-ms)))))

(defn- ensure-snapshot-scheduler-runtime!
  [lmdb]
  (let [info-v (i/kv-info lmdb)
        m @info-v]
    (if (and (:snapshot-scheduler-lock m)
             (:snapshot-scheduler-future m))
      {:lock (:snapshot-scheduler-lock m)
       :future-cell (:snapshot-scheduler-future m)}
      (let [lock (Object.)
            future-cell (volatile! nil)]
        (vswap! info-v
                (fn [info]
                  (if (and (:snapshot-scheduler-lock info)
                           (:snapshot-scheduler-future info))
                    info
                    (assoc info
                           :snapshot-scheduler-lock lock
                           :snapshot-scheduler-future future-cell))))
        (let [updated @info-v]
          {:lock (:snapshot-scheduler-lock updated)
           :future-cell (:snapshot-scheduler-future updated)})))))

(defn- snapshot-scheduler-running?
  [future-cell]
  (when-let [future (some-> future-cell deref)]
    (and (not (.isCancelled ^java.util.concurrent.Future future))
         (not (.isDone ^java.util.concurrent.Future future)))))

(defn- snapshot-scheduler-defer-reason
  [lmdb trigger now-ms]
  (let [trigger-k (:trigger trigger)
        max-age-trigger? (= trigger-k :max-age)
        offpeak-windows (snapshot-offpeak-windows lmdb)
        in-offpeak? (in-offpeak-window? offpeak-windows now-ms)
        defer-contention? (snapshot-defer-on-contention? lmdb)
        thresholds (snapshot-contention-thresholds lmdb)
        contention-state (when defer-contention?
                           (snapshot-scheduler-contention-state lmdb now-ms))
        queue-depth (long (or (:queue-depth contention-state) 0))
        commit-wait-ms (:commit-wait-ms contention-state)
        fsync-ms (:fsync-ms contention-state)
        queue-hit? (> queue-depth ^long (:queue-depth thresholds))
        commit-hit? (and (number? commit-wait-ms)
                         (> ^long (long commit-wait-ms)
                            ^long (:commit-wait-p99-ms thresholds)))
        fsync-hit? (and (number? fsync-ms)
                        (> ^long (long fsync-ms)
                           ^long (:fsync-p99-ms thresholds)))
        contented? (or queue-hit? commit-hit? fsync-hit?)]
    (cond
      (and (seq offpeak-windows)
           (not in-offpeak?)
           (not max-age-trigger?))
      {:reason :offpeak-window
       :trigger trigger-k
       :offpeak-windows offpeak-windows
       :in-offpeak-window? in-offpeak?}

      (and defer-contention?
           contented?
           (not max-age-trigger?))
      {:reason :contention
       :trigger trigger-k
       :contention-state contention-state
       :thresholds thresholds
       :hits {:queue-depth queue-hit?
              :commit-wait-p99-ms commit-hit?
              :fsync-p99-ms fsync-hit?}}

      :else nil)))

(defn- snapshot-scheduler-state-map
  [lmdb]
  (let [info-v (i/kv-info lmdb)
        info (if info-v @info-v {})
        snapshots (list-snapshot-entries lmdb)
        enabled? (snapshot-scheduler-enabled? lmdb)
        future-cell (:snapshot-scheduler-future info)
        running? (boolean (snapshot-scheduler-running? future-cell))
        now-ms (System/currentTimeMillis)
        defer-since-ms (:snapshot-scheduler-defer-since-ms info)
        current-defer-ms (when (number? defer-since-ms)
                           (max 0 (- now-ms (long defer-since-ms))))
        offpeak-windows (snapshot-offpeak-windows lmdb)
        in-offpeak? (in-offpeak-window? offpeak-windows now-ms)
        defer-contention? (snapshot-defer-on-contention? lmdb)
        thresholds (snapshot-contention-thresholds lmdb)
        latest-snapshot (first snapshots)
        latest-created-ms (when-let [v (or (:completed-ms latest-snapshot)
                                           (:created-ms latest-snapshot))]
                            (long v))
        latest-lsn (snapshot-current-lsn latest-snapshot)
        max-age-ms (long (snapshot-max-age-ms lmdb))
        watermarks (when-let [state (txlog/state lmdb)]
                     (txlog-watermarks-map lmdb state))
        applied-lsn (long (or (:last-applied-lsn watermarks) 0))
        snapshot-age-ms (when (some? latest-created-ms)
                          (long (max 0 (- (long now-ms)
                                          (long latest-created-ms)))))
        failure-count (long (or (:snapshot-scheduler-failure-count info) 0))
        consecutive-failure-count
        (long (or (:snapshot-scheduler-consecutive-failure-count info) 0))
        snapshot-age-alert? (and (some? snapshot-age-ms)
                                 (pos? max-age-ms)
                                 (>= ^long snapshot-age-ms ^long max-age-ms))
        snapshot-build-failure-alert? (pos? consecutive-failure-count)
        bytes-state (when-let [state (txlog/state lmdb)]
                      (let [dir (:dir state)]
                        (when (and (string? dir)
                                   (.isDirectory (io/file dir)))
                          (try
                            (let [summary (txlog/segment-summaries
                                           dir
                                           {:record->lsn txlog-record-lsn
                                            :cache-v (:segment-summaries-cache
                                                      state)
                                            :cache-key :txlog-record-lsn
                                            :active-segment-id
                                            (some-> state :segment-id deref long)
                                            :active-segment-offset
                                            (some-> state
                                                    :segment-offset
                                                    deref
                                                    long)})
                                  bytes-since
                                  (if (some? latest-lsn)
                                    (reduce
                                     (fn [acc {:keys [max-lsn bytes]}]
                                       (if (and (some? max-lsn)
                                                (> ^long (long max-lsn)
                                                   ^long latest-lsn))
                                         (+ ^long acc ^long bytes)
                                         acc))
                                     0
                                     (:segments summary))
                                    (:total-bytes summary))]
                              {:txlog-total-bytes (long (:total-bytes
                                                         summary))
                               :txlog-segment-count
                               (count (:segments summary))
                               :txlog-bytes-since-snapshot
                               (long bytes-since)})
                            (catch Exception e
                              {:txlog-bytes-error (.getMessage e)})))))]
    {:enabled? enabled?
     :running? running?
     :mode (if enabled? :auto :manual)
     :snapshot-dir (snapshot-root-dir lmdb)
     :snapshot-count (count snapshots)
     :latest-snapshot latest-snapshot
     :snapshot-current-lsn latest-lsn
     :last-applied-lsn applied-lsn
     :snapshot-age-ms snapshot-age-ms
     :snapshot-interval-ms (snapshot-interval-ms lmdb)
     :snapshot-max-lsn-delta (snapshot-max-lsn-delta lmdb)
     :snapshot-max-log-bytes-delta (snapshot-max-log-bytes-delta lmdb)
     :snapshot-max-age-ms max-age-ms
     :snapshot-age-alert? snapshot-age-alert?
     :snapshot-offpeak-windows offpeak-windows
     :in-offpeak-window? in-offpeak?
     :snapshot-defer-on-contention? defer-contention?
     :snapshot-contention-thresholds thresholds
     :snapshot-defer-backoff-min-ms (snapshot-defer-backoff-min-ms lmdb)
     :snapshot-defer-backoff-max-ms (snapshot-defer-backoff-max-ms lmdb)
     :txlog-total-bytes (:txlog-total-bytes bytes-state)
     :txlog-segment-count (:txlog-segment-count bytes-state)
     :txlog-bytes-since-snapshot (:txlog-bytes-since-snapshot bytes-state)
     :txlog-bytes-error (:txlog-bytes-error bytes-state)
     :last-run-ms (:snapshot-scheduler-last-run-ms info)
     :last-success-ms (:snapshot-scheduler-last-success-ms info)
     :last-trigger (:snapshot-scheduler-last-trigger info)
     :last-trigger-details (:snapshot-scheduler-last-trigger-details
                            info)
     :last-defer-ms (:snapshot-scheduler-last-defer-ms info)
     :last-defer-reason (:snapshot-scheduler-last-defer-reason info)
     :last-defer-trigger (:snapshot-scheduler-last-defer-trigger info)
     :last-defer-details (:snapshot-scheduler-last-defer-details info)
     :next-eligible-ms (:snapshot-scheduler-next-eligible-ms info)
     :defer-backoff-ms (:snapshot-scheduler-defer-backoff-ms info)
     :defer-since-ms defer-since-ms
     :current-defer-duration-ms current-defer-ms
     :last-defer-duration-ms (:snapshot-scheduler-last-defer-duration-ms
                              info)
     :defer-duration-ms (:snapshot-scheduler-defer-duration-ms info)
     :defer-count (:snapshot-scheduler-defer-count info)
     :run-count (:snapshot-scheduler-run-count info)
     :last-run-start-ms (:snapshot-scheduler-last-run-start-ms info)
     :last-run-finished-ms (:snapshot-scheduler-last-run-finished-ms
                            info)
     :last-run-duration-ms (:snapshot-scheduler-last-run-duration-ms
                            info)
     :run-duration-ms (:snapshot-scheduler-run-duration-ms info)
     :max-age-breach-count (:snapshot-scheduler-max-age-breach-count
                            info)
     :last-max-age-breach-ms (:snapshot-scheduler-last-max-age-breach-ms
                              info)
     ;; Phase-4 metric names (retain short aliases above for compatibility).
     :snapshot-defer-count (:snapshot-scheduler-defer-count info)
     :snapshot-defer-duration-ms
     (:snapshot-scheduler-defer-duration-ms info)
     :snapshot-run-duration-ms (:snapshot-scheduler-run-duration-ms info)
     :snapshot-max-age-breach-count
     (:snapshot-scheduler-max-age-breach-count info)
     :snapshot-failure-count failure-count
     :snapshot-consecutive-failure-count consecutive-failure-count
     :snapshot-last-failure-ms (:snapshot-scheduler-last-failure-ms info)
     :snapshot-build-failure-alert? snapshot-build-failure-alert?
     :last-error (:snapshot-scheduler-last-error info)}))

(defn- snapshot-bootstrap-force?
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (if (contains? opts :snapshot-bootstrap-force?)
      (boolean (:snapshot-bootstrap-force? opts))
      true)))

(defn- rdonly-env?
  [lmdb]
  (let [flags (or (:flags (i/env-opts lmdb)) #{})]
    (boolean (some #{:rdonly} flags))))

(defn- ensure-txlog-nosync-flag!
  [lmdb info-v]
  (let [info @info-v
        flags (or (i/get-env-flags lmdb) #{})]
    (when (and (txlog/enabled? info)
               (not (contains? flags :nosync))
               (not (contains? flags :rdonly)))
      (i/set-env-flags lmdb #{:nosync} true)
      true)))

(defn- snapshot-source-ready?
  [lmdb]
  (let [dir (i/env-dir lmdb)]
    (and (u/file-exists (str dir u/+separator+ c/data-file-name))
         (u/file-exists (str dir u/+separator+ c/version-file-name)))))

(defn- create-snapshot-now!
  [lmdb]
  (let [state (txlog/enabled-state lmdb)
        _ (txlog-force-sync! state)
        _ (force-lmdb-sync-now! lmdb)
        watermarks (txlog-watermarks-map lmdb state)
        applied-lsn (long (or (:last-applied-lsn watermarks) 0))
        root-dir (snapshot-root-dir lmdb)
        compact? (snapshot-compact? lmdb)
        current-path (snapshot-slot-path root-dir snapshot-current-slot)
        previous-path (snapshot-slot-path root-dir snapshot-previous-slot)
        snapshot-id (str (System/currentTimeMillis) "-"
                         (java.util.UUID/randomUUID))
        started-ms (System/currentTimeMillis)
        tmp-path (snapshot-slot-path root-dir (str ".tmp-" snapshot-id))
        snapshot-pin-floor-state (txlog-snapshot-floor-state
                                  lmdb
                                  (or (i/env-opts lmdb) {})
                                  applied-lsn)
        pin-floor-lsn (long (:floor-lsn snapshot-pin-floor-state))
        pin-id (str "snapshot-build/" snapshot-id)
        pin-ttl-ms (long (max 60000 (long (snapshot-max-age-ms lmdb))))
        pin-expires-ms (long (+ (long started-ms) pin-ttl-ms))]
    (u/create-dirs root-dir)
    (when (u/file-exists tmp-path)
      (u/delete-files tmp-path))
    (txlog-pin-backup-floor-state! lmdb pin-id pin-floor-lsn pin-expires-ms)
    (try
      (maybe-run-txn-log-snapshot-copy-failpoint!
       {:lmdb lmdb
        :snapshot-id snapshot-id
        :pin-id pin-id
        :pin-floor-lsn pin-floor-lsn
        :pin-expires-ms pin-expires-ms
        :applied-lsn applied-lsn})
      (binding [*wal-copy-backup-pin-enabled?* false]
        (i/copy lmdb tmp-path compact?))
      (let [completed-ms (System/currentTimeMillis)
            meta {:snapshot-id snapshot-id
                  :slot :current
                  :created-ms completed-ms
                  :started-ms started-ms
                  :completed-ms completed-ms
                  :source-dir (i/env-dir lmdb)
                  :applied-lsn applied-lsn
                  :last-durable-lsn (long (or (:last-durable-lsn
                                               watermarks)
                                              0))
                  :last-committed-lsn (long (or (:last-committed-lsn
                                                 watermarks)
                                                0))
                  :snapshot-compact? compact?}]
        (write-snapshot-meta! tmp-path meta)
        (when (u/file-exists previous-path)
          (u/delete-files previous-path))
        (when (u/file-exists current-path)
          (move-dir! current-path previous-path)
          (update-snapshot-slot-meta! previous-path :previous))
        (move-dir! tmp-path current-path)
        (let [snapshot-floor (txlog-update-snapshot-floor-state!
                              lmdb applied-lsn nil)
              snapshots (list-snapshot-entries lmdb)]
          {:ok? true
           :snapshot-dir root-dir
           :snapshot (first snapshots)
           :snapshots snapshots
           :snapshot-floor snapshot-floor
           :backup-pin {:pin-id pin-id
                        :floor-lsn pin-floor-lsn
                        :expires-ms pin-expires-ms}
           :compact? compact?
           :watermarks watermarks}))
      (catch Exception e
        (when (u/file-exists tmp-path)
          (u/delete-files tmp-path))
        (throw e))
      (finally
        (try
          (txlog-unpin-backup-floor-state! lmdb pin-id)
          (catch Exception e
            (when-let [info-v (i/kv-info lmdb)]
              (vswap! info-v assoc :snapshot-last-backup-unpin-error
                      (.getMessage e)))))))))

(defn- snapshot-current-lsn
  [snapshot]
  (let [v (:applied-lsn snapshot)]
    (when (number? v)
      (long v))))

(defn- txlog-bytes-since-snapshot
  [lmdb snapshot-lsn]
  (when-let [state (txlog/state lmdb)]
    (let [dir (:dir state)]
      (when (and (string? dir)
                 (.isDirectory (io/file dir)))
        (let [summary (txlog/segment-summaries
                       dir
                       {:record->lsn txlog-record-lsn
                        :cache-v (:segment-summaries-cache state)
                        :cache-key :txlog-record-lsn
                        :active-segment-id
                        (some-> state :segment-id deref long)
                        :active-segment-offset
                        (some-> state :segment-offset deref long)})
              bytes-since (if (some? snapshot-lsn)
                            (reduce
                             (fn [acc {:keys [max-lsn bytes]}]
                               (if (and (some? max-lsn)
                                        (> ^long (long max-lsn)
                                           ^long snapshot-lsn))
                                 (+ ^long acc ^long bytes)
                                 acc))
                             0
                             (:segments summary))
                            (:total-bytes summary))]
          {:txlog-total-bytes (long (:total-bytes summary))
           :txlog-segment-count (count (:segments summary))
           :txlog-bytes-since-snapshot (long bytes-since)})))))

(defn- snapshot-scheduler-gc-safety-state
  [lmdb]
  (try
    (txlog-retention-state-map lmdb nil false)
    (catch Exception e
      {:txlog-retention-error (.getMessage e)})))

(defn- snapshot-scheduler-trigger
  [lmdb now-ms]
  (let [snapshots (list-snapshot-entries lmdb)
        snapshot-count (count snapshots)
        latest-snapshot (first snapshots)
        latest-created (when-let [v (or (:completed-ms latest-snapshot)
                                        (:created-ms latest-snapshot))]
                         (long v))
        latest-lsn (snapshot-current-lsn latest-snapshot)
        watermarks (when-let [state (txlog/state lmdb)]
                     (txlog-watermarks-map lmdb state))
        applied-lsn (long (or (:last-applied-lsn watermarks) 0))
        snapshot-age-ms (when (some? latest-created)
                          (long (max 0 (- (long now-ms)
                                          (long latest-created)))))
        interval-ms (long (snapshot-interval-ms lmdb))
        max-lsn-delta (long (snapshot-max-lsn-delta lmdb))
        max-log-bytes-delta (long (snapshot-max-log-bytes-delta lmdb))
        max-age-ms (long (snapshot-max-age-ms lmdb))
        bytes-state (when (and (some? latest-lsn)
                               (pos? max-log-bytes-delta))
                      (try
                        (txlog-bytes-since-snapshot lmdb latest-lsn)
                        (catch Exception e
                          {:txlog-bytes-error (.getMessage e)})))
        txlog-total-bytes (when (number? (:txlog-total-bytes bytes-state))
                            (long (:txlog-total-bytes bytes-state)))
        bytes-since-snapshot
        (when (number? (:txlog-bytes-since-snapshot bytes-state))
          (long (:txlog-bytes-since-snapshot bytes-state)))
        retention-state (when (>= snapshot-count 2)
                          (snapshot-scheduler-gc-safety-state lmdb))
        floor-limiters (set (or (:floor-limiters retention-state) []))
        pressure (:pressure retention-state)
        snapshot-floor-limited?
        (contains? floor-limiters :snapshot-floor-lsn)
        gc-safety-due? (and snapshot-floor-limited?
                            (true? (:pressure? pressure))
                            (true? (:degraded? pressure)))
        interval-due? (and (pos? interval-ms)
                           (some? latest-created)
                           (>= (- (long now-ms) (long latest-created))
                               interval-ms))
        lsn-delta-due? (and (some? latest-lsn)
                            (pos? max-lsn-delta)
                            (>= (- applied-lsn (long latest-lsn))
                                max-lsn-delta))
        log-bytes-due? (and (number? bytes-since-snapshot)
                            (pos? max-log-bytes-delta)
                            (>= ^long bytes-since-snapshot
                                ^long max-log-bytes-delta))
        max-age-due? (and (some? snapshot-age-ms)
                          (pos? max-age-ms)
                          (>= ^long snapshot-age-ms ^long max-age-ms))]
    (cond
      (< snapshot-count 2)
      {:trigger :bootstrap
       :snapshot-count snapshot-count
       :applied-lsn applied-lsn
       :latest-snapshot-lsn latest-lsn}

      max-age-due?
      {:trigger :max-age
       :snapshot-count snapshot-count
       :applied-lsn applied-lsn
       :latest-snapshot-lsn latest-lsn
       :latest-snapshot-ms latest-created
       :snapshot-age-ms snapshot-age-ms
       :max-age-ms max-age-ms}

      gc-safety-due?
      {:trigger :gc-safety
       :snapshot-count snapshot-count
       :applied-lsn applied-lsn
       :latest-snapshot-lsn latest-lsn
       :pressure pressure
       :floor-limiters (vec (sort floor-limiters))
       :required-retained-floor-lsn
       (:required-retained-floor-lsn retention-state)
       :gc-safety-watermark-lsn
       (:gc-safety-watermark-lsn retention-state)}

      interval-due?
      {:trigger :interval
       :snapshot-count snapshot-count
       :applied-lsn applied-lsn
       :latest-snapshot-lsn latest-lsn
       :latest-snapshot-ms latest-created
       :interval-ms interval-ms}

      lsn-delta-due?
      {:trigger :lsn-delta
       :snapshot-count snapshot-count
       :applied-lsn applied-lsn
       :latest-snapshot-lsn latest-lsn
       :max-lsn-delta max-lsn-delta}

      log-bytes-due?
      {:trigger :log-bytes-delta
       :snapshot-count snapshot-count
       :applied-lsn applied-lsn
       :latest-snapshot-lsn latest-lsn
       :txlog-total-bytes txlog-total-bytes
       :txlog-bytes-since-snapshot bytes-since-snapshot
       :max-log-bytes-delta max-log-bytes-delta}

      :else nil)))

(defn- note-snapshot-max-age-breach!
  [info-v trigger-k now-ms]
  (when (= trigger-k :max-age)
    (vswap! info-v
            (fn [m]
              (-> m
                  (update :snapshot-scheduler-max-age-breach-count
                          (fnil inc 0))
                  (assoc :snapshot-scheduler-last-max-age-breach-ms
                         now-ms))))))

(defn- defer-snapshot-run!
  "When `trigger` must be deferred, record the defer state and return the
   deferred result map; otherwise return nil."
  [lmdb info-v trigger trigger-k now-ms]
  (when-let [defer (snapshot-scheduler-defer-reason lmdb trigger now-ms)]
    (let [min-backoff-ms (long (snapshot-defer-backoff-min-ms lmdb))
          max-backoff-ms (long (snapshot-defer-backoff-max-ms lmdb))
          m (vswap! info-v
                    (fn [m]
                      (let [reason (:reason defer)
                            defer-count (or (:snapshot-scheduler-defer-count m)
                                            {})
                            defer-since-ms
                            (long (or (:snapshot-scheduler-defer-since-ms m)
                                      now-ms))
                            defer-ms (max 0 (- now-ms defer-since-ms))
                            prev-backoff-ms
                            (long (or (:snapshot-scheduler-defer-backoff-ms m)
                                      0))
                            backoff-ms (if (pos? prev-backoff-ms)
                                         (long (min max-backoff-ms
                                                    (max min-backoff-ms
                                                         (long (* 2
                                                                  prev-backoff-ms)))))
                                         min-backoff-ms)
                            next-ms (long (+ (long now-ms) backoff-ms))
                            defer* (assoc defer
                                          :backoff-ms backoff-ms
                                          :next-eligible-ms next-ms)]
                        (assoc m
                               :snapshot-scheduler-last-trigger trigger-k
                               :snapshot-scheduler-last-trigger-details
                               trigger
                               :snapshot-scheduler-last-defer-ms now-ms
                               :snapshot-scheduler-last-defer-reason reason
                               :snapshot-scheduler-last-defer-trigger
                               trigger-k
                               :snapshot-scheduler-last-defer-details defer*
                               :snapshot-scheduler-defer-since-ms
                               defer-since-ms
                               :snapshot-scheduler-last-defer-duration-ms
                               defer-ms
                               :snapshot-scheduler-next-eligible-ms next-ms
                               :snapshot-scheduler-defer-backoff-ms backoff-ms
                               :snapshot-scheduler-defer-count
                               (update defer-count reason (fnil inc 0))
                               :snapshot-scheduler-last-error nil))))]
      {:deferred? true
       :trigger trigger-k
       :defer (:snapshot-scheduler-last-defer-details m)
       :defer-duration-ms (:snapshot-scheduler-last-defer-duration-ms m)
       :backoff-ms (:snapshot-scheduler-defer-backoff-ms m)
       :next-eligible-ms (:snapshot-scheduler-next-eligible-ms m)})))

(defn- run-snapshot-now!
  "Run a scheduled snapshot and record success or failure bookkeeping."
  [lmdb info-v trigger trigger-k]
  (let [run-start-ms (System/currentTimeMillis)]
    (vswap! info-v
            (fn [m]
              (let [defer-since-ms (:snapshot-scheduler-defer-since-ms m)
                    defer-ms (when (number? defer-since-ms)
                               (max 0 (- run-start-ms
                                         (long defer-since-ms))))
                    m' (-> m
                           (assoc :snapshot-scheduler-last-run-start-ms
                                  run-start-ms)
                           (dissoc :snapshot-scheduler-next-eligible-ms
                                   :snapshot-scheduler-defer-backoff-ms
                                   :snapshot-scheduler-defer-since-ms))]
                (if (number? defer-ms)
                  (-> m'
                      (update :snapshot-scheduler-defer-duration-ms
                              (fnil + 0)
                              (long defer-ms))
                      (assoc :snapshot-scheduler-last-defer-duration-ms
                             (long defer-ms)))
                  m'))))
    (try
      (let [res (create-snapshot-now! lmdb)
            run-finished-ms (System/currentTimeMillis)
            run-duration-ms (max 0 (- run-finished-ms run-start-ms))]
        (vswap! info-v
                (fn [m]
                  (-> m
                      (update :snapshot-scheduler-run-count (fnil inc 0))
                      (update :snapshot-scheduler-run-duration-ms
                              (fnil + 0)
                              run-duration-ms)
                      (assoc
                       :snapshot-scheduler-last-run-finished-ms run-finished-ms
                       :snapshot-scheduler-last-run-duration-ms run-duration-ms
                       :snapshot-scheduler-last-success-ms run-finished-ms
                       :snapshot-scheduler-last-trigger trigger-k
                       :snapshot-scheduler-last-trigger-details trigger
                       :snapshot-scheduler-consecutive-failure-count 0
                       :snapshot-scheduler-last-error nil))))
        (assoc res
               :trigger trigger-k
               :run-duration-ms run-duration-ms))
      (catch Exception e
        (let [run-finished-ms (System/currentTimeMillis)
              run-duration-ms (max 0 (- run-finished-ms run-start-ms))]
          (vswap! info-v
                  (fn [m]
                    (-> m
                        (update :snapshot-scheduler-run-count (fnil inc 0))
                        (update :snapshot-scheduler-failure-count (fnil inc 0))
                        (update :snapshot-scheduler-consecutive-failure-count
                                (fnil inc 0))
                        (update :snapshot-scheduler-run-duration-ms
                                (fnil + 0)
                                run-duration-ms)
                        (assoc
                         :snapshot-scheduler-last-run-finished-ms
                         run-finished-ms
                         :snapshot-scheduler-last-run-duration-ms
                         run-duration-ms
                         :snapshot-scheduler-last-failure-ms run-finished-ms
                         :snapshot-scheduler-last-trigger trigger-k
                         :snapshot-scheduler-last-trigger-details trigger
                         :snapshot-scheduler-last-error (.getMessage e)))))
          nil)))))

(defn- maybe-run-snapshot-scheduler!
  [lmdb]
  (when (and (snapshot-scheduler-enabled? lmdb)
             (not (rdonly-env? lmdb))
             (snapshot-source-ready? lmdb))
    (let [tx-v (l/write-txn lmdb)]
      (when-not (and (some? tx-v) (some? @tx-v))
        (when-let [state (txlog/state lmdb)]
          (txlog/try-with-maintenance-lock
           state
           (fn []
             (let [info-v (i/kv-info lmdb)
                   {:keys [lock]} (ensure-snapshot-scheduler-runtime! lmdb)
                   now-ms (System/currentTimeMillis)]
               (locking lock
                 (vswap! info-v assoc :snapshot-scheduler-last-run-ms now-ms)
                 (let [next-eligible-ms
                       (:snapshot-scheduler-next-eligible-ms @info-v)]
                   (when-not (and (number? next-eligible-ms)
                                  (< now-ms (long next-eligible-ms)))
                     (if-let [trigger (snapshot-scheduler-trigger lmdb now-ms)]
                       (let [trigger-k (:trigger trigger)]
                         (note-snapshot-max-age-breach! info-v trigger-k
                                                        now-ms)
                         (or (defer-snapshot-run! lmdb info-v trigger
                                                  trigger-k now-ms)
                             (run-snapshot-now! lmdb info-v trigger
                                                trigger-k)))
                       (vswap! info-v dissoc
                               :snapshot-scheduler-defer-since-ms
                               :snapshot-scheduler-next-eligible-ms
                               :snapshot-scheduler-defer-backoff-ms)))))))))))))

(defn- start-snapshot-scheduler!
  [lmdb]
  (when (and (snapshot-scheduler-enabled? lmdb)
             (not (rdonly-env? lmdb)))
    (let [info-v (i/kv-info lmdb)
          info @info-v]
      (when (txlog/enabled? info)
        (let [{:keys [future-cell]} (ensure-snapshot-scheduler-runtime! lmdb)
              running? (snapshot-scheduler-running? future-cell)]
          (when-not running?
            (let [scheduler (u/get-scheduler)
                  poll-ms (snapshot-scheduler-poll-ms lmdb)
                  future (.scheduleWithFixedDelay
                          ^java.util.concurrent.ScheduledExecutorService
                          scheduler
                          ^Runnable #(try
                                       (maybe-run-snapshot-scheduler! lmdb)
                                       (catch Exception _))
                          ^long poll-ms
                          ^long poll-ms
                          java.util.concurrent.TimeUnit/MILLISECONDS)]
              (vreset! future-cell future))))))))

(defn- stop-snapshot-scheduler!
  [info-v]
  (when-let [future-cell (:snapshot-scheduler-future @info-v)]
    (when-let [future @future-cell]
      (.cancel ^java.util.concurrent.Future future true))
    (vreset! future-cell nil)))

(defn kv-info-value
  [lmdb k]
  (try
    (i/get-value lmdb c/kv-info k :keyword :data)
    (catch Exception _
      nil)))

(defn- txlog-read-marker-slot
  [lmdb marker-key]
  (when-let [slot (i/get-value lmdb c/kv-info marker-key :keyword :bytes)]
    (try
      (txlog/decode-commit-marker-slot-bytes slot)
      (catch Exception _
        nil))))

(defn read-commit-marker-state
  [lmdb]
  (let [slot-a (txlog-read-marker-slot lmdb c/wal-marker-a)
        slot-b (txlog-read-marker-slot lmdb c/wal-marker-b)
        current (txlog/newer-commit-marker slot-a slot-b)]
    {:slot-a slot-a
     :slot-b slot-b
     :current current}))

(defn- commit-marker-applied-lsn
  [lmdb]
  (long (or (some-> (read-commit-marker-state lmdb)
                    :current
                    :applied-lsn)
            0)))

(defn- refresh-runtime-marker-revision!
  [lmdb state]
  (when (:commit-marker? state)
    (let [marker-state (read-commit-marker-state lmdb)]
      (vreset! (:marker-revision state)
               (long (or (get-in marker-state [:current :revision]) -1)))))
  state)

(defn- sanitize-kv-info!
  [info-v]
  (vswap! info-v dissoc c/wal-marker-a c/wal-marker-b)
  info-v)

(defn- ensure-pending-fast-list!
  ^FastList [info-v]
  (let [pending (:txlog-pending-ops @info-v)]
    (if (instance? FastList pending)
      pending
      (let [^FastList normalized (ensure-fast-list pending)]
        (vswap! info-v assoc :txlog-pending-ops normalized)
        normalized))))

(defn txlog-reset-pending!
  [info-v]
  (vswap! info-v assoc :txlog-pending-ops (FastList.)))

(defn txlog-add-pending!
  [info-v rows]
  (let [^FastList pending (ensure-pending-fast-list! info-v)]
    (if (instance? java.util.Collection rows)
      (.addAll pending ^java.util.Collection rows)
      (doseq [row rows]
        (.add pending row)))
    pending))

(defn txlog-pending-rows
  [info-v]
  (ensure-pending-fast-list! info-v))

(defn txlog-mark-fatal!
  [state ex]
  (let [retryable?
        (loop [e ex]
          (when e
            (if (or (l/resized? e)
                    (= :ha/write-rejected
                       (:error (ex-data ^Throwable e))))
              true
              (recur (.getCause ^Throwable e)))))]
    (when (and (not retryable?) (:fatal-error state))
      (vreset! (:fatal-error state) ex))))

(defn txlog-throw-if-fatal!
  [state]
  (when-let [fatal @(:fatal-error state)]
    (raise "Txn-log runtime is in fatal state"
           {:type :txlog/fatal
            :cause (.getMessage ^Exception fatal)})))

(defn- nonneg-long
  [x]
  (long (max 0 (long (or x 0)))))

(defn- txlog-vector-domain
  [db-dir fname index-info]
  (or (:domain index-info)
      (let [prefix (str db-dir u/+separator+)
            suffix c/vector-index-suffix]
        (when (and (string? fname)
                   (str/starts-with? fname prefix)
                   (str/ends-with? fname suffix)
                   (> (count fname) (+ (count prefix) (count suffix))))
          (subs fname (count prefix) (- (count fname) (count suffix)))))
      fname))

(defn- txlog-vector-checkpoint-summary
  [lmdb]
  (let [db-dir (i/env-dir lmdb)
        prefix (str db-dir u/+separator+)
        init {:vec-checkpoint-count 0
              :vec-checkpoint-duration-ms 0
              :vec-checkpoint-bytes 0
              :vec-checkpoint-failure-count 0
              :vec-replay-lag-lsn 0
              :vec-checkpoint-domains {}}]
    (reduce-kv
     (fn [acc fname vec-index]
       (if (and (string? fname) (str/starts-with? fname prefix))
         (let [index-info (try
                            (i/vecs-info vec-index)
                            (catch Throwable _ nil))
               cp-state (if (map? (:checkpoint index-info))
                          (:checkpoint index-info)
                          {})
               count (nonneg-long (:vec-checkpoint-count cp-state))
               duration (nonneg-long (:vec-checkpoint-duration-ms cp-state))
               bytes (nonneg-long (:vec-checkpoint-bytes cp-state))
               failures (nonneg-long (:vec-checkpoint-failure-count cp-state))
               replay-lag (nonneg-long (:vec-replay-lag-lsn cp-state))
               domain (txlog-vector-domain db-dir fname index-info)]
           (-> acc
               (update :vec-checkpoint-count + count)
               (update :vec-checkpoint-duration-ms + duration)
               (update :vec-checkpoint-bytes + bytes)
               (update :vec-checkpoint-failure-count + failures)
               (update :vec-replay-lag-lsn max replay-lag)
               (assoc-in [:vec-checkpoint-domains domain]
                         {:vec-checkpoint-count count
                          :vec-checkpoint-duration-ms duration
                          :vec-checkpoint-bytes bytes
                          :vec-checkpoint-failure-count failures
                          :vec-replay-lag-lsn replay-lag})))
         acc))
     init
     @l/vector-indices)))

(defn- txlog-rollout-watermarks
  [lmdb rollout-mode]
  (let [write-path-enabled? (= :active rollout-mode)]
    (merge
     {:wal? true
      :rollout-mode rollout-mode
      :write-path-enabled? write-path-enabled?
      :rollback? (not write-path-enabled?)}
     (txlog-vector-checkpoint-summary lmdb))))

(defn txlog-watermarks-map
  [lmdb state]
  (txlog/refresh-shared-state! state)
  (let [sync-state (txlog/sync-manager-state (:sync-manager state))
        vec-summary (txlog-vector-checkpoint-summary lmdb)
        rollout-mode (txlog-rollout-mode lmdb)
        write-path-enabled? (= :active rollout-mode)
        marker-state (read-commit-marker-state lmdb)
        marker-current (:current marker-state)
        info-v (i/kv-info lmdb)
        info (if info-v @info-v {})
        now-ms (System/currentTimeMillis)
        next-lsn (long @(:next-lsn state))
        last-committed (max 0 (dec next-lsn))
        last-durable (long (:last-durable-lsn sync-state))
        last-applied (long (or (:applied-lsn marker-current)
                               (some-> (:meta-last-applied-lsn state) deref)
                               0))
        durable-applied-lag-lsn (max 0 (- last-durable last-applied))
        durable-applied-lag-threshold-lsn (txlog-lag-alert-threshold-lsn lmdb)
        last-checkpoint-ms (some-> (:txlog-last-checkpoint-ms info) long)
        checkpoint-staleness-ms (when (some? last-checkpoint-ms)
                                  (long (max 0 (- (long now-ms)
                                                  (long last-checkpoint-ms)))))
        checkpoint-stale-threshold-ms (checkpoint-stale-threshold-ms lmdb)
        checkpoint-stale? (and (some? checkpoint-staleness-ms)
                               (>= ^long checkpoint-staleness-ms
                                   ^long checkpoint-stale-threshold-ms))]
    {:wal? true
     :rollout-mode rollout-mode
     :write-path-enabled? write-path-enabled?
     :rollback? (not write-path-enabled?)
     :durability-profile (:durability-profile state)
     :wal-shared? (true? (:wal-shared? state))
     :dir (:dir state)
     :segment-id (long @(:segment-id state))
     :next-lsn next-lsn
     :last-committed-lsn last-committed
     :last-appended-lsn (long (:last-appended-lsn sync-state))
     :last-durable-lsn last-durable
     :last-applied-lsn last-applied
     :last-sync-ms (long (:last-sync-ms sync-state))
     :last-checkpoint-ms last-checkpoint-ms
     :checkpoint-staleness-ms checkpoint-staleness-ms
     :checkpoint-stale-threshold-ms checkpoint-stale-threshold-ms
     :checkpoint-stale? (boolean checkpoint-stale?)
     :durable-applied-lag-lsn durable-applied-lag-lsn
     :durable-applied-lag-threshold-lsn durable-applied-lag-threshold-lsn
     :durable-applied-lag-alert?
     (>= ^long durable-applied-lag-lsn
         ^long durable-applied-lag-threshold-lsn)
     :lmdb-sync-count (long (or (:txlog-lmdb-sync-count info) 0))
     :pending-count (long (:pending-count sync-state))
     :batched-sync-count (long (or (:batched-sync-count sync-state) 0))
     :avg-commit-wait-ms (:avg-commit-wait-ms sync-state)
     :avg-commit-wait-ms-by-mode (:avg-commit-wait-ms-by-mode sync-state)
     :avg-commit-wait-ms-by-reason (:avg-commit-wait-ms-by-reason sync-state)
     :segment-roll-count
     (long (or (some-> (:segment-roll-count state) deref) 0))
     :segment-roll-duration-ms
     (long (or (some-> (:segment-roll-duration-ms state) deref) 0))
     :segment-prealloc-success-count
     (long (or (some-> (:segment-prealloc-success-count state) deref) 0))
     :segment-prealloc-failure-count
     (long (or (some-> (:segment-prealloc-failure-count state) deref) 0))
     :append-p99-near-roll-ms
     (some-> (:append-p99-near-roll-ms state) deref long)
     :vec-checkpoint-count (:vec-checkpoint-count vec-summary)
     :vec-checkpoint-duration-ms (:vec-checkpoint-duration-ms vec-summary)
     :vec-checkpoint-bytes (:vec-checkpoint-bytes vec-summary)
     :vec-checkpoint-failure-count
     (:vec-checkpoint-failure-count vec-summary)
     :vec-replay-lag-lsn (:vec-replay-lag-lsn vec-summary)
     :vec-checkpoint-domains (:vec-checkpoint-domains vec-summary)
     :sync-manager sync-state
     :commit-marker marker-state
     :commit-marker? (boolean (:commit-marker? state))
     :commit-marker-revision (long @(:marker-revision state))}))

(defn txlog-force-sync!
  [state]
  (txlog-throw-if-fatal! state)
  (txlog/force-sync!
   state
   {:mark-fatal! txlog-mark-fatal!
    :before-sync!
    (fn [state sync-begin]
      (maybe-run-storage-fault-context!
       (:fault-context state)
       :txlog-force-sync
       {:target-lsn (:target-lsn sync-begin)
        :reason (:reason sync-begin)
        :sync-mode (:sync-mode state)}))}))

(defn verify-commit-marker-state
  [lmdb state]
  (let [{:keys [min-retained-lsn applied-lsn valid-marker marker-state]}
        (txlog-recovery-context lmdb state)]
    {:ok? true
     :min-retained-lsn min-retained-lsn
     :applied-lsn (long applied-lsn)
     :commit-marker marker-state
     :valid-marker valid-marker}))

(defn txlog-recovery-context
  [lmdb state]
  (txlog/refresh-shared-state! state)
  (let [marker-state (read-commit-marker-state lmdb)
        marker-applied-lsn
        (long (or (some-> marker-state :current :applied-lsn) 0))
        payload-floor-lsn (long (persisted-payload-floor-lsn lmdb))
        recovery-floor-lsn (if (and (:commit-marker? state)
                                    (pos? marker-applied-lsn))
                             marker-applied-lsn
                             (max marker-applied-lsn payload-floor-lsn))
        records (txlog-records-for-recovery state recovery-floor-lsn)
        meta-applied-lsn
        (long (or (some-> (:meta-last-applied-lsn state) deref) 0))
        meta-last-applied-lsn
        (if (and (:commit-marker? state)
                 (pos? marker-applied-lsn))
          meta-applied-lsn
          (max payload-floor-lsn meta-applied-lsn))
        recovery (txlog/recovery-state
                  {:commit-marker? (:commit-marker? state)
                   :marker-state marker-state
                   :records records
                   :meta-last-applied-lsn meta-last-applied-lsn
                   :next-lsn @(:next-lsn state)})]
    (assoc recovery
           :records records
           :marker-state marker-state
           :from-lsn (long (:applied-lsn recovery)))))

(defn init-txlog-state!
  ([lmdb]
   (when-let [info-v (i/kv-info lmdb)]
     (init-txlog-state! lmdb info-v)))
  ([lmdb info-v]
   (sanitize-kv-info! info-v)
   (let [info @info-v]
     (when (txlog/enabled? info)
       (let [marker (read-commit-marker-state lmdb)
             {:keys [dir state]}
             (txlog/init-runtime-state info marker)]
         (vswap! info-v assoc
                 :wal-dir dir
                 :txlog-state state
                 :txlog-pending-ops (FastList.))
         state)))))

(defn close-txlog-state!
  [db-or-info]
  (let [info-v (if (and (instance? clojure.lang.IDeref db-or-info)
                        (instance? clojure.lang.IAtom db-or-info))
                 db-or-info
                 (i/kv-info db-or-info))]
    (when (some? info-v)
      (when-let [state (:txlog-state @info-v)]
        (stop-snapshot-scheduler! info-v)
        (try
          (txlog/flush-meta! state)
          (catch Exception _))
        (let [append-lock (or (:append-lock state) state)
              [ch sync-lock-ch]
              (locking append-lock
                [(when-let [segment-channel-v
                            (:segment-channel state)]
                   (let [ch @segment-channel-v]
                     (when ch
                       (vreset! segment-channel-v nil))
                     ch))
                 (when-let [sync-lock-channel-v
                            (:sync-lock-channel state)]
                   (let [ch @sync-lock-channel-v]
                     (when ch
                       (vreset! sync-lock-channel-v nil))
                     ch))])]
          (when ch
            (try
              (.close ^java.io.Closeable ch)
              (catch Exception _)))
          (when sync-lock-ch
            (try
              (.close ^java.io.Closeable sync-lock-ch)
              (catch Exception _))))
        (vswap! info-v dissoc :txlog-state :txlog-pending-ops
                :txlog-recovered?
                :snapshot-scheduler-lock
                :snapshot-scheduler-future
                :snapshot-scheduler-last-run-ms
                :snapshot-scheduler-last-success-ms
                :snapshot-scheduler-last-trigger
                :snapshot-scheduler-last-trigger-details
                :snapshot-scheduler-last-defer-ms
                :snapshot-scheduler-last-defer-reason
                :snapshot-scheduler-last-defer-trigger
                :snapshot-scheduler-last-defer-details
                :snapshot-scheduler-defer-since-ms
                :snapshot-scheduler-next-eligible-ms
                :snapshot-scheduler-defer-backoff-ms
                :snapshot-scheduler-last-defer-duration-ms
                :snapshot-scheduler-defer-duration-ms
                :snapshot-scheduler-defer-count
                :snapshot-scheduler-run-count
                :snapshot-scheduler-last-run-start-ms
                :snapshot-scheduler-last-run-finished-ms
                :snapshot-scheduler-last-run-duration-ms
                :snapshot-scheduler-run-duration-ms
                :snapshot-scheduler-max-age-breach-count
                :snapshot-scheduler-last-max-age-breach-ms
                :snapshot-scheduler-failure-count
                :snapshot-scheduler-consecutive-failure-count
                :snapshot-scheduler-last-failure-ms
                :txlog-last-checkpoint-ms
                :txlog-last-lmdb-sync-ms
                :txlog-last-lmdb-sync-error
                :txlog-lmdb-sync-count
                :snapshot-scheduler-last-error)))))

(defn- close-failed-open!
  [db]
  (close-txlog-state! db)
  (try
    (i/close-kv db)
    (catch Exception _))
  nil)

(def ^:private txlog-recovery-reopen-opt-keys
  [:mapsize :max-readers :flags :max-dbs :temp? :key-compress :val-compress
   :wal?
   :wal-dir
   :wal-rollout-mode
   :wal-rollback?
   :wal-durability-profile
   :wal-commit-marker?
   :wal-commit-marker-version
   :wal-sync-mode
   :wal-group-commit
   :wal-group-commit-ms
   :wal-meta-flush-max-txs
   :wal-meta-flush-max-ms
   :wal-commit-wait-ms
   :wal-sync-adaptive?
   :wal-segment-max-bytes
   :wal-segment-max-ms
   :wal-segment-prealloc?
   :wal-segment-prealloc-mode
   :wal-segment-prealloc-bytes
   :wal-retention-bytes
   :wal-retention-ms
   :wal-retention-pin-backpressure-threshold-ms
   :wal-replica-floor-ttl-ms
   :wal-lag-alert-threshold-lsn
   :wal-checkpoint-stale-threshold-ms
   :wal-vec-checkpoint-interval-ms
   :wal-vec-max-lsn-delta
   :wal-vec-max-buffer-bytes
   :wal-vec-chunk-bytes
   :wal-replay-missing-dbi-policy
   :lmdb-sync-interval
   :lmdb-sync-interval-ms
   :snapshot-dir
   :snapshot-scheduler?
   :snapshot-compact?
   :snapshot-bootstrap-force?
   :snapshot-interval-ms
   :snapshot-max-lsn-delta
   :snapshot-max-log-bytes-delta
   :snapshot-max-age-ms
   :snapshot-offpeak-windows
   :snapshot-defer-on-contention?
   :snapshot-contention-thresholds
   :snapshot-contention-sample-max-age-ms
   :snapshot-defer-backoff-min-ms
   :snapshot-defer-backoff-max-ms])

(def ^:private txlog-recovery-lmdb-file-names
  #{c/data-file-name
    c/version-file-name
    c/keycode-file-name
    c/valcode-file-name
    "lock.mdb"})

(def ^:private txlog-recovery-fallback-types
  #{:txlog/recovery-marker-invalid
    :txlog/recovery-floor-gap
    :txlog/corrupt
    :txlog/replay-missing-dbi})

(defn- snapshot-slot-lmdb-files
  [slot-path]
  (let [slot-file (io/file slot-path)]
    (->> (or (.listFiles slot-file) [])
         (filter #(.isFile ^java.io.File %))
         (remove #(= snapshot-meta-file-name (.getName ^java.io.File %)))
         vec)))

(defn- snapshot-slot-valid?
  [{:keys [path]}]
  (and (u/file-exists path)
       (u/file-exists (str path u/+separator+ c/data-file-name))))

(defn- copy-snapshot-lmdb-files!
  [env-dir snapshot-path]
  (let [files (snapshot-slot-lmdb-files snapshot-path)
        names (set (map #(.getName ^java.io.File %) files))]
    (when-not (contains? names c/data-file-name)
      (raise "Snapshot is missing LMDB data file"
             {:type :txlog/snapshot-missing-data-file
              :snapshot-path snapshot-path
              :required-file c/data-file-name}))
    (doseq [file-name txlog-recovery-lmdb-file-names
            :when (and (not (contains? names file-name))
                       (u/file-exists
                        (str env-dir u/+separator+ file-name)))]
      (io/delete-file (io/file env-dir file-name)))
    (doseq [^java.io.File f files]
      (u/copy-file (.getPath f)
                   (str env-dir u/+separator+ (.getName f))))))

(defn- txlog-recovery-work-root
  [env-dir snapshot]
  (str env-dir u/+separator+ ".txlog-recovery-"
       (System/currentTimeMillis) "-"
       (java.util.UUID/randomUUID) "-"
       (name (:slot snapshot))))

(defn- txlog-recovery-candidate-opts
  [reopen-opts candidate-dir]
  (assoc reopen-opts
         :wal-dir (str candidate-dir u/+separator+ "txlog")
         :snapshot-dir (str candidate-dir u/+separator+ "snapshots")
         :snapshot-scheduler? false
         :snapshot-bootstrap-force? false))

(defn- copy-txlog-dir!
  [src-env-dir dest-env-dir]
  (let [src (str src-env-dir u/+separator+ "txlog")
        dst (str dest-env-dir u/+separator+ "txlog")]
    (when (u/file-exists src)
      (copy-dir-contents! src dst))))

(defn- move-recovery-path-if-exists!
  [src dst]
  (when (u/file-exists src)
    (u/create-dirs (or (some-> (io/file dst) .getParent) dst))
    (move-dir! src dst)))

(defn- backup-current-recovery-files!
  [env-dir backup-dir]
  (u/create-dirs backup-dir)
  (doseq [file-name txlog-recovery-lmdb-file-names]
    (move-recovery-path-if-exists!
     (str env-dir u/+separator+ file-name)
     (str backup-dir u/+separator+ file-name)))
  (move-recovery-path-if-exists!
   (str env-dir u/+separator+ "txlog")
   (str backup-dir u/+separator+ "txlog")))

(defn- restore-recovery-backup!
  [env-dir backup-dir]
  (doseq [file-name txlog-recovery-lmdb-file-names]
    (let [dst (str env-dir u/+separator+ file-name)]
      (when (u/file-exists dst)
        (io/delete-file (io/file dst) true))))
  (let [txlog-dir (str env-dir u/+separator+ "txlog")]
    (when (u/file-exists txlog-dir)
      (u/delete-files txlog-dir)))
  (doseq [^java.io.File f (or (u/list-files backup-dir) [])]
    (move-dir! (.getPath f)
               (str env-dir u/+separator+ (.getName f)))))

(defn- install-recovery-candidate!
  [env-dir candidate-dir]
  (doseq [file-name txlog-recovery-lmdb-file-names
          :let [src (str candidate-dir u/+separator+ file-name)]
          :when (u/file-exists src)]
    (u/copy-file src (str env-dir u/+separator+ file-name)))
  (copy-txlog-dir! candidate-dir env-dir))

(defn- promote-recovery-candidate!
  [env-dir reopen-opts snapshot cause candidate-dir work-root]
  (let [backup-dir (str work-root u/+separator+ "pre-recovery")]
    (backup-current-recovery-files! env-dir backup-dir)
    (try
      (install-recovery-candidate! env-dir candidate-dir)
      (try
        (let [reopened (txlog-mark-recovery-source!
                        (l/open-kv env-dir reopen-opts)
                        snapshot
                        cause)]
          (when (u/file-exists candidate-dir)
            (u/delete-files candidate-dir))
          reopened)
        (catch Exception e
          (restore-recovery-backup! env-dir backup-dir)
          (throw e)))
      (catch Exception e
        (when (u/file-exists backup-dir)
          (restore-recovery-backup! env-dir backup-dir))
        (throw e)))))

(defn- validate-snapshot-recovery-candidate!
  [env-dir reopen-opts snapshot work-root]
  (let [candidate-dir (str work-root u/+separator+ "candidate")
        candidate-opts (txlog-recovery-candidate-opts reopen-opts candidate-dir)]
    (u/create-dirs candidate-dir)
    (copy-snapshot-lmdb-files! candidate-dir (:path snapshot))
    (copy-txlog-dir! env-dir candidate-dir)
    (reset-txlog-runtime-for-snapshot! candidate-dir candidate-opts snapshot)
    (let [candidate (l/open-kv candidate-dir candidate-opts)]
      (try
        true
        (finally
          (try
            (i/close-kv candidate)
            (catch Exception _)))))
    candidate-dir))

(defn- scan-txlog-segment-range
  [{:keys [id file]}]
  (let [path (.getPath ^java.io.File file)
        scan (txlog/truncate-partial-tail!
              path {:allow-preallocated-tail? true})
        records (:records scan)
        first-record (first records)
        last-record (peek records)]
    {:segment-id (long id)
     :path path
     :file file
     :min-lsn (some-> first-record txlog-record-lsn long)
     :max-lsn (some-> last-record txlog-record-lsn long)
     :end-offset (txlog/segment-end-offset scan)}))

(defn- replayable-txlog-tail-for-snapshot
  [txlog-dir snapshot-lsn]
  (let [segments (vec (txlog/segment-files txlog-dir))
        target-lsn (unchecked-inc (long snapshot-lsn))]
    (loop [remaining (seq (rseq segments))
           kept []
           expected-next-lsn nil]
      (if-let [segment (first remaining)]
        (let [{:keys [range error]}
              (try
                {:range (scan-txlog-segment-range segment)}
                (catch Exception e
                  {:error e}))]
          (cond
            error
            {:kept []}

            (nil? (:max-lsn range))
            (recur (next remaining) kept expected-next-lsn)

            (nil? expected-next-lsn)
            (if (<= (long (:max-lsn range)) (long snapshot-lsn))
              {:kept (vec (reverse kept))}
              (let [kept' (conj kept range)]
                (if (<= (long (:min-lsn range)) target-lsn)
                  {:kept (vec (reverse kept'))}
                  (recur (next remaining)
                         kept'
                         (long (:min-lsn range))))))

            (= (long (:max-lsn range))
               (dec ^long expected-next-lsn))
            (let [kept' (conj kept range)]
              (if (<= (long (:min-lsn range)) target-lsn)
                {:kept (vec (reverse kept'))}
                (recur (next remaining)
                       kept'
                       (long (:min-lsn range)))))

            :else
            {:kept []}))
        (if (seq kept)
          {:kept []}
          {:kept []})))))

(defn- reset-txlog-runtime-for-snapshot!
  [env-dir reopen-opts snapshot]
  (let [txlog-dir (or (:wal-dir reopen-opts)
                      (str env-dir u/+separator+ "txlog"))
        applied-lsn (long (or (:applied-lsn snapshot) 0))
        {:keys [kept]} (replayable-txlog-tail-for-snapshot
                        txlog-dir applied-lsn)
        keep-ids (into #{} (map :segment-id) kept)
        active-segment (peek kept)
        active-segment-id (long (or (:segment-id active-segment) 1))
        active-segment-offset (long (or (:end-offset active-segment) 0))]
    (u/create-dirs txlog-dir)
    ;; Snapshot fallback should keep only a contiguous WAL suffix that starts at
    ;; the restored snapshot floor. Anything older is already covered by the
    ;; snapshot, and any gapped/corrupt suffix is safer to discard entirely.
    (doseq [{:keys [id file]} (txlog/segment-files txlog-dir)
            :when (not (contains? keep-ids (long id)))]
      (io/delete-file ^java.io.File file true))
    (doseq [{:keys [file]} (txlog/prepared-segment-files txlog-dir)]
      (io/delete-file ^java.io.File file true))
    (txlog/write-meta-file!
     (txlog/meta-path txlog-dir)
     {:last-committed-lsn applied-lsn
      :last-durable-lsn applied-lsn
      :last-applied-lsn applied-lsn
      :segment-id active-segment-id
      :segment-offset active-segment-offset
      :updated-ms (System/currentTimeMillis)})
    txlog-dir))

(defn- txlog-reopen-opts
  [env-opts]
  (into {:txlog-recovery-fallback-attempted? true}
        (keep (fn [k]
                (when-let [v (get env-opts k)]
                  [k v])))
        txlog-recovery-reopen-opt-keys))

(defn- txlog-recovery-fallback-eligible?
  [e]
  (loop [t e]
    (if t
      (let [typ (:type (ex-data t))]
        (if (contains? txlog-recovery-fallback-types typ)
          true
          (recur (.getCause ^Throwable t))))
      false)))

(defn- txlog-mark-recovery-source!
  [db snapshot cause]
  (when-let [info-v (i/kv-info db)]
    (vswap! info-v assoc
            :txlog-recovery-source
            (keyword (str "snapshot-" (name (:slot snapshot))))
            :txlog-recovery-snapshot-slot (:slot snapshot)
            :txlog-recovery-snapshot-path (:path snapshot)
            :txlog-recovery-cause-type
            (or (:type (ex-data cause))
                :unknown)))
  db)

(def ^:private txlog-replay-missing-dbi-policies
  #{:error :open-default})

(defn- txlog-replay-missing-dbi-policy
  [lmdb]
  (let [policy (or (:wal-replay-missing-dbi-policy (i/env-opts lmdb))
                   :error)]
    (if (contains? txlog-replay-missing-dbi-policies policy)
      policy
      :error)))

(defn- txlog-note-replay-warning!
  [lmdb warning]
  (when-let [info-v (i/kv-info lmdb)]
    (vswap! info-v update :txlog-replay-warnings (fnil conj []) warning)))

(defn- recover-from-snapshot-open!
  [db cause]
  (let [opts (i/env-opts db)
        attempted? (true? (:txlog-recovery-fallback-attempted? opts))]
    (when (and (txlog-config-enabled? db)
               (not attempted?)
               (txlog-recovery-fallback-eligible? cause))
      (let [snapshots (->> (list-snapshot-entries db)
                           (filter snapshot-slot-valid?)
                           vec)
            env-dir (i/env-dir db)
            reopen-opts (txlog-reopen-opts opts)
            snapshot-info (mapv #(select-keys % [:slot :path :applied-lsn])
                                snapshots)]
        (when (seq snapshots)
          (close-txlog-state! db)
          (try
            (i/close-kv db)
            (catch Exception _))
          (loop [[snapshot & more] snapshots
                 failures []]
            (if snapshot
              (let [work-root (txlog-recovery-work-root env-dir snapshot)
                    attempt
                    (try
                      (let [candidate-dir
                            (validate-snapshot-recovery-candidate!
                             env-dir
                             reopen-opts
                             snapshot
                             work-root)]
                        {:reopened
                         (promote-recovery-candidate!
                          env-dir
                          reopen-opts
                          snapshot
                          cause
                          candidate-dir
                          work-root)})
                      (catch Exception e
                        (when (u/file-exists work-root)
                          (u/delete-files work-root))
                        {:error e}))]
                (if-let [reopened (:reopened attempt)]
                  reopened
                  (recur more
                         (conj failures
                               {:slot (:slot snapshot)
                                :path (:path snapshot)
                                :cause
                                (.getMessage ^Exception (:error attempt))}))))
              (raise "Txn-log recovery failed after snapshot restore attempts"
                     cause
                     {:type :txlog/recovery-snapshot-failed
                      :snapshots snapshot-info
                      :failures failures}))))))))

(defn- txlog-replay-dbi!
  [lmdb dbi-name info]
  (or (try
        (i/get-dbi lmdb dbi-name false)
        (catch Exception _
          nil))
      (if-let [opts (get-in info [:dbis dbi-name])]
        (i/open-dbi lmdb dbi-name opts)
        (case (txlog-replay-missing-dbi-policy lmdb)
          :open-default
          (do
            (txlog-note-replay-warning!
             lmdb
             {:type :txlog/replay-missing-dbi-open-default
              :dbi dbi-name})
            (i/open-dbi lmdb dbi-name))

          (raise "DBI is not available for txn-log replay"
                 {:dbi dbi-name :type :txlog/replay-missing-dbi})))))

(defn- replay-note-kv-info-update!
  [info-v ^datalevin.lmdb.KVTxData tx]
  (when (= c/kv-info (.-dbi-name tx))
    (let [op (.-op tx)
          k (.-k tx)
          v (.-v tx)]
      (cond
        (and (= op :put)
             (vector? k)
             (= 2 (count k))
             (= :dbis (nth k 0))
             (string? (nth k 1))
             (map? v))
        (do (vswap! info-v assoc-in [:dbis (nth k 1)] v)
            (when (or (:key-type v) (:value-type v))
              (vswap! info-v update :custom-dbis (fnil conj #{}) (nth k 1))))

        (and (= op :del)
             (vector? k)
             (= 2 (count k))
             (= :dbis (nth k 0))
             (string? (nth k 1)))
        (do (vswap! info-v update :dbis dissoc (nth k 1))
            (vswap! info-v update :custom-dbis disj (nth k 1)))

        (and (= op :put) (= k :max-val-size))
        (vswap! info-v assoc :max-val-size v)

        :else nil))))

(defn- txlog-prepare-replay-dbis!
  [lmdb records from-lsn]
  (let [info-v (i/kv-info lmdb)
        replay-records (drop-while #(<= (long (:lsn %)) (long from-lsn))
                                   records)]
    (doseq [record replay-records
            t (:rows record)]
      (let [^datalevin.lmdb.KVTxData tx (l/->kv-tx-data t)
            dbi-name (.-dbi-name tx)]
        (replay-note-kv-info-update! info-v tx)
        (when (not= dbi-name c/kv-info)
          (txlog-replay-dbi! lmdb dbi-name @info-v))))))

(declare append-payload-lsn-row)
(declare append-monotonic-payload-lsn-row)
(declare align-runtime-txlog-payload-floor!)

(declare with-runtime-txlog-rollback)

(defn ^:no-doc replay-txlog-rows!
  "Apply physical txlog payload rows without consuming a new local txlog LSN."
  [lmdb rows lsn]
  (let [rows (rows-vector rows)
        record {:lsn (long lsn) :rows rows}
        result
        (binding [l/*raw-kv?* true]
          (with-runtime-txlog-rollback
            lmdb
            (fn []
              (txlog-prepare-replay-dbis! lmdb [record] (dec (long lsn)))
              (when (= "1" (System/getenv "HA_REPLAY_DEBUG"))
                (binding [*out* *err*]
                  (prn {:ha-replay-debug true
                        :lsn (long lsn)
                        :row-count (count rows)
                        :row-types (mapv class rows)
                        :rows rows})))
              (i/transact-kv
                lmdb (append-monotonic-payload-lsn-row lmdb rows (long lsn))))))]
    (custom-kv/initialize! lmdb (raw-lmdb lmdb))
    result))

(defn- txlog-replay-record!
  [lmdb state record]
  (let [append-res {:lsn (:lsn record)
                    :segment-id (:segment-id record)
                    :offset (:offset record)
                    :checksum (:checksum record)}
        {:keys [rows marker-entry]}
        (txlog/prepare-commit-rows
         {:commit-marker? (:commit-marker? state)
          :marker-revision (long @(:marker-revision state))}
         append-res
         (:rows record))
        rows (append-monotonic-payload-lsn-row
              lmdb
              rows
              (:lsn record))]
    (maybe-run-storage-fault-lmdb! lmdb
                                   :txlog-replay
                                   {:lsn (:lsn record)
                                    :segment-id (:segment-id record)})
    (i/transact-kv lmdb rows)
    (txlog/commit-finished! state marker-entry)
    record))

(defn- clean-open-recovery-fast-path
  [lmdb state]
  (let [last-record (:last-record-summary state)
        marker (some-> (read-commit-marker-state lmdb) :current)
        applied-lsn (long (or (some-> (:meta-last-applied-lsn state) deref) 0))
        committed-lsn (long (max 0 (dec (long @(:next-lsn state)))))]
    (when (and (:commit-marker? state)
               last-record
               marker
               (= applied-lsn committed-lsn)
               (= committed-lsn (long (:lsn last-record)))
               (= committed-lsn (long (:applied-lsn marker)))
               (= (long (:segment-id last-record))
                  (long (:txlog-segment-id marker)))
               (= (long (:offset last-record))
                  (long (:txlog-record-offset marker)))
               (= (long (:checksum last-record))
                  (long (:txlog-record-crc marker))))
      (vreset! (:marker-revision state) (long (:revision marker)))
      {:from-lsn committed-lsn
       :last-record nil
       :replayed 0})))

(defn txlog-recover-on-open!
  [lmdb]
  (when-let [state (txlog/state lmdb)]
    (or (clean-open-recovery-fast-path lmdb state)
        (let [{:keys [records valid-marker from-lsn]}
              (txlog-recovery-context lmdb state)]
          (when (and (:commit-marker? state) valid-marker)
            (vreset! (:marker-revision state) (long (:revision valid-marker))))
          (txlog-prepare-replay-dbis! lmdb records from-lsn)
          (let [replayed (->> records
                              (drop-while #(<= (long (:lsn %))
                                               (long from-lsn)))
                              (mapv #(txlog-replay-record! lmdb state %)))]
            (i/set-max-val-size lmdb (i/max-val-size lmdb))
            {:from-lsn (long from-lsn)
             :last-record (peek replayed)
             :replayed (count replayed)})))))

(defn- txlog-recover-under-write-transaction!
  [lmdb state]
  (txlog/with-recovery-lock
    state
    (fn []
      (txlog/refresh-shared-state! state)
      (let [{:keys [last-record] :as recovery} (txlog-recover-on-open! lmdb)]
        (when last-record
          (txlog/publish-meta-commit! state
                                      {:lsn (:lsn last-record)
                                       :segment-id (:segment-id last-record)
                                       :offset (:offset last-record)
                                       :synced? true}))
        (dissoc recovery :last-record)))))

(defn- ensure-snapshot-bootstrap!
  [lmdb state]
  (when (and (snapshot-bootstrap-force? lmdb)
             (not (rdonly-env? lmdb))
             (snapshot-source-ready? lmdb))
    (let [snapshots (list-snapshot-entries lmdb)
          persisted-current-lsn (some-> (kv-info-value
                                         lmdb
                                         c/wal-snapshot-current-lsn)
                                        long)
          applied-lsn (long (or (:last-applied-lsn
                                 (txlog-watermarks-map lmdb state))
                                0))
          restored-snapshot? (and (some? persisted-current-lsn)
                                  (or (empty? snapshots)
                                      (> (long persisted-current-lsn)
                                         applied-lsn)))]
      ;; Restored HA snapshot copies persist the snapshot floor in kv-info but
      ;; can reopen without a matching txlog history. Treat an already-persisted
      ;; floor that is ahead of the recovered local LSN as bootstrapped rather
      ;; than synthesizing a new snapshot at a lower applied LSN during open.
      (when (and (not restored-snapshot?)
                 (< (count snapshots) 1))
        (create-snapshot-now! lmdb))
      (when (and (not restored-snapshot?)
                 (< (count (list-snapshot-entries lmdb)) 2))
        (create-snapshot-now! lmdb)))))

(defn ensure-txlog-ready!
  [lmdb]
  (when-let [info-v (i/kv-info lmdb)]
    (sanitize-kv-info! info-v)
    (ensure-txlog-nosync-flag! lmdb info-v)
    (when (and (txlog/enabled? @info-v)
               (txlog-write-path-enabled? lmdb))
      (let [state (or (txlog/state lmdb)
                      (init-txlog-state! lmdb info-v))]
        (when-not (:txlog-recovered? @info-v)
          (txlog-recover-under-write-transaction! lmdb state)
          (align-runtime-txlog-payload-floor! lmdb)
          (ensure-snapshot-bootstrap! lmdb state)
          (vswap! info-v assoc :txlog-recovered? true))
        (txlog/refresh-shared-state! state)
        (align-runtime-txlog-payload-floor! lmdb)
        (start-snapshot-scheduler! lmdb)
        (or (txlog/state lmdb) state)))))

(defn- ensure-tx-dbi
  [dbi-name tx]
  (let [^datalevin.lmdb.KVTxData tx tx]
    (if (or (nil? dbi-name) (some? (.-dbi-name tx)))
      tx
      (datalevin.lmdb.KVTxData.
       (.-op tx) dbi-name (.-k tx) (.-v tx)
       (.-kt tx) (.-vt tx) (.-flags tx)))))

(defn- add-canonical-kvtx!
  [^FastList out dbi-name t kt vt]
  (if (instance? DatomKVTxData t)
    (do
      (when dbi-name
        (raise "Internal datom transaction cannot use an explicit DBI"
               {:dbi-name dbi-name}))
      (l/add-datom-kv-txs! out t))
    (let [tx (if (instance? datalevin.lmdb.KVTxData t)
               t
               (if dbi-name
                 (l/->kv-tx-data t kt vt)
                 (l/->kv-tx-data t)))]
      (.add out (ensure-tx-dbi dbi-name tx))))
  out)

(defn- canonicalize-input-kvtxs
  [dbi-name txs kt vt]
  (letfn [(canonical-kvtx-list?
            [xs]
            (when (instance? java.util.List xs)
              (let [^java.util.List lst xs
                    n (.size lst)]
                (loop [i 0]
                  (if (< i n)
                    (let [x (.get lst i)]
                      (if (instance? datalevin.lmdb.KVTxData x)
                        (let [^datalevin.lmdb.KVTxData tx x]
                          (if (or (nil? dbi-name) (some? (.-dbi-name tx)))
                            (recur (inc i))
                            false))
                        false))
                    true)))))]
    (if (canonical-kvtx-list? txs)
      txs
      (let [^FastList out
            (if (instance? java.util.Collection txs)
              ;; An internal datom becomes one canonical AVE row and one EAV
              ;; row in the transaction log. Size for that representation so
              ;; ingestion batches do not repeatedly grow the backing array.
              (let [n (.size ^java.util.Collection txs)]
                (FastList. (int (if (l/datom-kv-txs? txs) (* 2 n) n))))
              (FastList.))]
        (if (instance? java.util.List txs)
          (let [^java.util.List tx-list txs
                n (.size tx-list)]
            (dotimes [i n]
              (add-canonical-kvtx! out dbi-name (.get tx-list i) kt vt)))
          (doseq [t txs]
            (add-canonical-kvtx! out dbi-name t kt vt)))
        out))))

(defn- payload-lsn-row
  [lsn]
  (l/kv-tx :put c/kv-info c/wal-local-payload-lsn (long lsn) :keyword :data))

(defn- append-payload-lsn-row
  (^FastList [rows lsn]
   (append-payload-lsn-row rows lsn true))
  (^FastList [rows lsn persist?]
   (let [^FastList rows* (ensure-fast-list rows)]
     (when persist?
       (.add rows* (payload-lsn-row lsn)))
     rows*)))

(def ^:private txlog-append-hooks
  {:throw-if-fatal! txlog-throw-if-fatal!
   :mark-fatal! txlog-mark-fatal!
   :before-append!
   (fn [state]
     (maybe-run-storage-fault-context! (:fault-context state)
                                       :txlog-append
                                       {:sync-mode (:sync-mode state)}))
   :before-sync!
   (fn [state sync-begin]
     (maybe-run-storage-fault-context! (:fault-context state)
                                       :txlog-sync
                                       {:target-lsn (:target-lsn sync-begin)
                                        :reason (:reason sync-begin)
                                        :sync-mode (:sync-mode state)}))})

(declare close-with-txlog!)

(defn- write-txn-open?
  [lmdb]
  (let [tx-v (l/write-txn lmdb)]
    (and (some? tx-v)
         (some? @tx-v))))

(defn- txlog-config-enabled?
  [lmdb]
  (when-let [info-v (i/kv-info lmdb)]
    (txlog/enabled? @info-v)))

(def ^:private txlog-rollout-mode-values #{:active :rollback})

(defn- normalize-txlog-rollout-mode
  [v]
  (cond
    (keyword? v) v
    (string? v) (keyword v)
    :else nil))

(defn- txlog-rollout-mode
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})
        rollout-specified? (contains? opts :wal-rollout-mode)
        rollback-specified? (contains? opts :wal-rollback?)
        rollback? (if (contains? opts :wal-rollback?)
                    (boolean (:wal-rollback? opts))
                    (boolean c/*wal-rollback?*))
        requested (or (:wal-rollout-mode opts)
                      (when rollback? :rollback)
                      c/*wal-rollout-mode*)
        mode (normalize-txlog-rollout-mode requested)]
    (when-not (contains? txlog-rollout-mode-values mode)
      (raise "Unsupported txn-log rollout mode"
             {:wal-rollout-mode requested
              :allowed txlog-rollout-mode-values}))
    (when (and rollout-specified?
               rollback-specified?
               rollback?
               (not= :rollback mode))
      (raise "Conflicting txn-log rollout options"
             {:wal-rollout-mode (:wal-rollout-mode opts)
              :wal-rollback? (:wal-rollback? opts)
              :expected-mode :rollback}))
    mode))

(defn- txlog-write-path-enabled?
  [lmdb]
  (and (txlog-config-enabled? lmdb)
       (= :active (txlog-rollout-mode lmdb))))

(defn- txlog-runtime-state
  [lmdb]
  (when (txlog-write-path-enabled? lmdb)
    (or (txlog/state lmdb)
        (ensure-txlog-ready! lmdb))))

(defn- persisted-payload-floor-lsn
  [lmdb]
  (long
   (max
    (long (or (try
                (i/get-value lmdb
                             c/kv-info
                             c/wal-local-payload-lsn
                             :keyword
                             :data)
                (catch Throwable _
                  nil))
              0))
    (long (or (try
                (i/get-value lmdb
                             c/kv-info
                             c/wal-snapshot-current-lsn
                             :keyword
                             :data)
                (catch Throwable _
                  nil))
              0)))))

(defn- persisted-local-payload-lsn
  [lmdb]
  (long
   (or (try
         (i/get-value lmdb
                      c/kv-info
                      c/wal-local-payload-lsn
                      :keyword
                      :data)
         (catch Throwable _
           nil))
       0)))

(defn- payload-marker-lsn
  [lmdb lsn]
  (long (max (long lsn)
             (long (persisted-local-payload-lsn lmdb)))))

(defn- append-monotonic-payload-lsn-row
  [lmdb rows lsn]
  (append-payload-lsn-row rows (payload-marker-lsn lmdb lsn)))

(defn- persisted-runtime-floor-lsn
  [lmdb]
  (long
   (max
    (long (commit-marker-applied-lsn lmdb))
    (long (persisted-payload-floor-lsn lmdb)))))

(defn- align-runtime-txlog-payload-floor!
  [lmdb]
  (with-runtime-txlog-state-guard
    lmdb
    (fn []
      (when-let [info-v (i/kv-info lmdb)]
        (when-let [state (txlog/state lmdb)]
          (let [floor-lsn (long (persisted-runtime-floor-lsn lmdb))
                target-next-lsn (unchecked-inc floor-lsn)
                current-next-lsn (long @(:next-lsn state))]
            (when (> target-next-lsn current-next-lsn)
              ;; Snapshot-installed followers can restore LMDB payload state
              ;; ahead of their local txlog files. Align the runtime cursor to
              ;; the restored payload floor so the next mirrored HA record can
              ;; append contiguously without inventing placeholder WAL rows.
              (let [last-applied-v (:meta-last-applied-lsn state)
                    _ (when (and last-applied-v
                                 (> floor-lsn (long @last-applied-v)))
                        (vreset! last-applied-v floor-lsn))
                    sync-manager (:sync-manager state)
                    now-ms (System/currentTimeMillis)]
                (vreset! (:next-lsn state) target-next-lsn)
                (vreset! (:last-appended-lsn sync-manager) floor-lsn)
                (vreset! (:last-durable-lsn sync-manager) floor-lsn)
                (vreset! (:last-sync-ms sync-manager)
                         (long (max now-ms
                                    (long @(:last-sync-ms sync-manager)))))
                (vreset! (:unsynced-count sync-manager) 0)
                (vreset! (:pending-lsn-head sync-manager) 0)
                (vreset! (:pending-lsn-tail sync-manager) 0)
                (vreset! (:pending-lsn-size sync-manager) 0)
                (vreset! (:sync-requested? sync-manager) false)
                (vreset! (:sync-request-reason sync-manager) nil)
                (vreset! (:sync-in-progress? sync-manager) false)
                (vreset! (:healthy? sync-manager) true)
                (vreset! (:failure sync-manager) nil)))))))))

(defn- apply-lmdb-after-txlog-append!
  [lmdb state rows]
  ;; These rows have already been assigned a durable WAL position. Apply them
  ;; to LMDB without routing back through the live txlog path, otherwise we
  ;; consume an extra local LSN while materializing the same record.
  (with-runtime-txlog-rollback
    lmdb
    (fn []
      (try
        (u/repeat-try-catch
         c/+in-tx-overflow-times+
         l/resized?
         (i/transact-kv lmdb rows))
        (catch Exception e
          (txlog-mark-fatal! state e)
          (throw e))))))

(defn- txlog-record-at-lsn
  [state record-lsn]
  (let [record-lsn (long record-lsn)]
    (some (fn [record]
            (when (= record-lsn (long (:lsn record)))
              record))
          (txlog-records state record-lsn record-lsn))))

(defn- txlog-record-replay-summary
  [record]
  (select-keys record [:lsn :segment-id :offset :checksum :ha-term
                       :tx-kind :payload-bytes :path]))

(defn- comparable-txlog-record?
  [local incoming]
  (let [local-checksum (some-> (:checksum local) long)
        incoming-checksum (some-> (:checksum incoming) long)
        local-term (some-> (:ha-term local) long)
        incoming-term (some-> (:ha-term incoming) long)]
    (and (= (long (:lsn local)) (long (:lsn incoming)))
         (= local-term incoming-term)
         (if (and (some? local-checksum) (some? incoming-checksum))
           (= local-checksum incoming-checksum)
           (= (rows-vector (:rows local))
              (rows-vector (or (:rows incoming) (:ops incoming))))))))

(defn- assert-skipped-replay-record-matches-local!
  [lmdb state record expected-lsn]
  (let [record-lsn (long (:lsn record))
        local-record (txlog-record-at-lsn state record-lsn)
        local-term (some-> (:ha-term local-record) long)
        incoming-term (some-> (:ha-term record) long)
        local-payload-lsn (long (persisted-local-payload-lsn lmdb))
        materialized-without-local-wal?
        (and (nil? local-record)
             (<= record-lsn local-payload-lsn))]
    (when (and (or (some? local-term) (some? incoming-term))
               (not materialized-without-local-wal?)
               (not (and local-record
                         (comparable-txlog-record? local-record record))))
      (raise "Follower replay found divergent local txn-log record"
             {:type :txlog/ha-replay-divergent-local-record
              :error :ha/txlog-divergent-local-record
              :expected-lsn expected-lsn
              :record-lsn record-lsn
              :local-payload-lsn local-payload-lsn
              :local-record (some-> local-record
                                    txlog-record-replay-summary)
              :incoming-record (txlog-record-replay-summary record)}))))

(defn ^:no-doc mirror-replayed-txlog-record!
  "Append a replicated HA record into the local txlog at the same LSN and
  apply its payload to LMDB. This keeps promoted followers on the same WAL
  sequence and preserves source records for downstream followers.

  `preapply-rows` may be a collection or a zero-argument function. Functions
  are evaluated under the replay write lock so read-dependent cleanup rows are
  derived from the state immediately preceding this record. Set
  `:replay-skipped?` when an existing local LSN may represent a different
  local-only record and an incoming payload newer than the materialized floor
  must still be applied."
  ([lmdb record]
   (mirror-replayed-txlog-record! lmdb record nil))
  ([lmdb record preapply-rows]
   (mirror-replayed-txlog-record! lmdb record preapply-rows nil))
  ([lmdb record preapply-rows {:keys [replay-skipped?]}]
   ;; Replay mutates both the runtime txlog metadata and LMDB itself. Keep the
   ;; same lock order as close/transact paths so follower replay cannot deadlock
   ;; with concurrent write-txn close on the same store.
   (with-write-txn-lock-before-runtime-txlog-state
     lmdb
     (fn []
       (let [preapply-rows (if (fn? preapply-rows)
                             (preapply-rows)
                             preapply-rows)
             record-rows (cond
                           (vector? (:rows record)) (:rows record)
                           (sequential? (:rows record)) (vec (:rows record))
                           (vector? (:ops record)) (:ops record)
                           (sequential? (:ops record)) (vec (:ops record))
                           :else nil)
             preapply-rows (rows-vector preapply-rows)
             materialize-rows (if (seq preapply-rows)
                                (into preapply-rows record-rows)
                                record-rows)]
         (when-not record-rows
           (raise "Follower replay record is missing payload rows"
                  {:type :txlog/ha-replay-missing-rows
                   :record (select-keys record [:lsn :segment-id :offset])}))
         (if-let [state (txlog-runtime-state lmdb)]
           (let [_ (txlog/refresh-shared-state! state)
                 record-lsn (long (:lsn record))
                 local-payload-lsn (long (persisted-local-payload-lsn lmdb))
                 expected-lsn0 (long @(:next-lsn state))
                 expected-lsn (if (> record-lsn expected-lsn0)
                                (do
                                  ;; Snapshot bootstrap and reopen paths can
                                  ;; advance the persisted payload floor before
                                  ;; the in-memory txlog cursor catches up.
                                  ;; Realign once from kv-info before treating a
                                  ;; one-step-ahead follower record as a hard
                                  ;; replay gap.
                                  (align-runtime-txlog-payload-floor! lmdb)
                                  (long @(:next-lsn state)))
                                expected-lsn0)]
             (when (> record-lsn expected-lsn)
               (raise "Follower replay local txn-log cursor does not match record LSN"
                      {:type :txlog/ha-replay-lsn-mismatch
                       :expected-lsn expected-lsn
                       :record-lsn record-lsn}))
             (if (< record-lsn expected-lsn)
               (do
                 (assert-skipped-replay-record-matches-local!
                  lmdb
                  state
                  record
                  expected-lsn)
                 ;; A lagging advisory follower cursor can ask for a record
                 ;; whose payload is already materialized. Reapplying that
                 ;; older physical payload would regress cardinality-one
                 ;; values even though the payload floor remains monotonic.
                 ;; Only repair the crash window where WAL is ahead of LMDB.
                 (if (and replay-skipped?
                          (> record-lsn local-payload-lsn))
                   (do
                     (replay-txlog-rows! lmdb materialize-rows record-lsn)
                     {:lsn record-lsn
                      :skipped? true
                      :replayed? true})
                   {:lsn record-lsn
                    :skipped? true}))
               (do
                 (txlog-prepare-replay-dbis!
                  lmdb
                  [(assoc record :rows materialize-rows)]
                  (dec record-lsn))
                 (let [record-rows (rows-vector record-rows)
                       materialized-rows
                       (if (seq preapply-rows)
                         (rows-vector materialize-rows)
                         record-rows)
                       append-res (binding [txlog/*commit-payload-ha-term*
                                            (some-> (:ha-term record) long)]
                                    (txlog/append-durable! state
                                                           record-rows
                                                           txlog-append-hooks))]
                   (when-not (= record-lsn (long (:lsn append-res)))
                     (raise "Follower replay appended unexpected txn-log LSN"
                            {:type :txlog/ha-replay-lsn-mismatch
                             :expected-lsn record-lsn
                             :actual-lsn (:lsn append-res)}))
                   (let [state (refresh-runtime-marker-revision! lmdb state)
                         marker-entry
                         (txlog/next-commit-marker-entry
                          (:commit-marker? state)
                          (long @(:marker-revision state))
                          append-res)
                         rows
                     (append-monotonic-payload-lsn-row
                      lmdb
                      materialized-rows
                      record-lsn)]
                     (when marker-entry
                       (.add ^FastList rows (:row marker-entry)))
                     (apply-lmdb-after-txlog-append! lmdb state rows)
                     (txlog/commit-finished! state marker-entry)
                     (txlog/note-commit-applied! state append-res)
                     append-res)))))
           (replay-txlog-rows! lmdb materialize-rows (:lsn record))))))))

(defn- transact-with-txlog!
  [lmdb state dbi-name txs k-type v-type]
  (let [datom-txs? (l/datom-kv-txs? txs)
        tx-data    (canonicalize-input-kvtxs dbi-name txs k-type v-type)
        lmdb-txs   (if datom-txs? txs tx-data)]
    (if (pos? (.size ^java.util.List tx-data))
      (if (write-txn-open? lmdb)
        (let [res (i/transact-kv lmdb lmdb-txs)]
          (txlog-add-pending! (i/kv-info lmdb) tx-data)
          res)
        (let [wdb (i/open-transact-kv lmdb)]
          (try
            (let [res (i/transact-kv wdb lmdb-txs)]
              (when-not (= :transacted res)
                (raise "Unexpected LMDB transactional write result"
                       {:type :txlog/unexpected-transact-result
                        :result res}))
              (txlog-add-pending! (i/kv-info lmdb) tx-data)
              (let [status (close-with-txlog! lmdb state)]
                (when-not (= :committed status)
                  (raise "Unexpected LMDB transactional close result"
                         {:type :txlog/unexpected-close-result
                          :result status}))
                :transacted))
            (catch Exception e
              (try
                (i/abort-transact-kv lmdb)
                (catch Exception _))
              (try
                (i/close-transact-kv lmdb)
                (catch Exception _))
              (throw e)))))
      (i/transact-kv lmdb dbi-name txs k-type v-type))))

(defn- close-with-txlog!
  [lmdb state]
  (let [info-v (i/kv-info lmdb)]
    (if (write-txn-open? lmdb)
      (let [pending (txlog-pending-rows info-v)]
        (try
          (if (pos? (.size ^java.util.List pending))
            ;; HA commit admission cannot safely reject after we append durable
            ;; txn-log payload, because followers may replay the payload even if
            ;; the local LMDB commit is later aborted. Run that check before the
            ;; append and suppress the lower-level duplicate invocation. The
            ;; post-append publish hook runs only after LMDB close commits below,
            ;; so authority watermarks never advertise uncommitted local state.
            (let [_ (when-let [f cpp/*before-write-commit-fn*]
                      (f {:operation :close-transact-kv}))
                  append-res (txlog/append-durable!
                              state pending txlog-append-hooks)
                  state (refresh-runtime-marker-revision! lmdb state)
                  marker-entry (txlog/next-commit-marker-entry
                                (:commit-marker? state)
                                (long @(:marker-revision state))
                                append-res)
                  commit-rows (append-monotonic-payload-lsn-row
                               lmdb
                               nil
                               (:lsn append-res))
                  _ (when marker-entry
                      (.add ^FastList commit-rows (:row marker-entry)))
                  status
                  (try
                    (when (pos? (.size ^FastList commit-rows))
                      (apply-lmdb-after-txlog-append! lmdb state
                                                      commit-rows))
                    (let [status (binding [cpp/*before-write-commit-fn* nil]
                                   (i/close-transact-kv lmdb))]
                      (when (= status :committed)
                        (txlog/commit-finished! state marker-entry)
                        (txlog/note-commit-applied! state append-res))
                      (when-not (write-txn-open? lmdb)
                        (txlog-reset-pending! info-v))
                      status)
                    (catch Exception e
                      (txlog-mark-fatal! state e)
                      (throw e)))]
              (when (= status :committed)
                (run-after-txlog-append!
                 {:operation :close-transact-kv
                  :txlog-lsn (long (:lsn append-res))
                  :append-res append-res}))
              status)
            (let [status (i/close-transact-kv lmdb)]
              (when-not (write-txn-open? lmdb)
                (txlog-reset-pending! info-v))
              status))
          (catch Exception e
            (when (write-txn-open? lmdb)
              (try
                (i/abort-transact-kv lmdb)
                (catch Exception _))
              (try
                (i/close-transact-kv lmdb)
                (catch Exception _)))
            (when-not (write-txn-open? lmdb)
              (txlog-reset-pending! info-v))
            (throw e))))
      (i/close-transact-kv lmdb))))

(defn- maybe-domain-name
  [x]
  (cond
    (string? x) x
    (keyword? x) (u/keyword->string x)
    (symbol? x) (str x)
    :else nil))

(defn- dbi-open?
  [lmdb dbi-name]
  (try
    (boolean (i/get-dbi lmdb dbi-name false))
    (catch Exception _
      false)))

(defn txlog-snapshot-floor-state
  [lmdb info applied-lsn]
  (txlog/snapshot-floor-state
   (or (kv-info-value lmdb c/wal-snapshot-current-lsn)
       (get info :wal-snapshot-current-lsn))
   (or (kv-info-value lmdb c/wal-snapshot-previous-lsn)
       (get info :wal-snapshot-previous-lsn))
   applied-lsn
   c/wal-snapshot-current-lsn
   c/wal-snapshot-previous-lsn))

(defn- configured-vector-domains
  [lmdb]
  (if (dbi-open? lmdb c/opts)
    (try
      (let [domains (i/get-value lmdb c/opts :vector-domains :attr :data)]
        (cond
          (map? domains)
          (->> (keys domains)
               (keep maybe-domain-name)
               set)

          (sequential? domains)
          (->> domains
               (keep maybe-domain-name)
               set)

          :else #{}))
      (catch Exception _
        #{}))
    #{}))

(defn- vector-meta-state
  [lmdb]
  (if (dbi-open? lmdb c/vec-meta-dbi)
    (try
      (->> (i/get-range lmdb c/vec-meta-dbi [:all] :string :data)
           (keep
            (fn [[domain meta]]
              (when (and (string? domain) (map? meta))
                [domain meta])))
           (into {}))
      (catch Exception _
        {}))
    {}))

(defn txlog-vector-floor-state
  [lmdb info]
  (txlog/vector-floor-state
   (vector-meta-state lmdb)
   (configured-vector-domains lmdb)
   (or (kv-info-value lmdb :wal-vector-floor-lsn)
       (get info :wal-vector-floor-lsn))
   :wal-vector-floor-lsn))

(defn txlog-replica-floor-state
  [lmdb info]
  (txlog/replica-floor-state
   (or (kv-info-value lmdb c/wal-replica-floors)
       (get info :wal-replica-floors))
   (long (or (:wal-replica-floor-ttl-ms info)
             c/*wal-replica-floor-ttl-ms*))
   (System/currentTimeMillis)
   (or (kv-info-value lmdb :wal-replica-floor-lsn)
       (get info :wal-replica-floor-lsn))
   :wal-replica-floor-lsn))

(defn txlog-backup-pin-floor-state
  [lmdb info]
  (txlog/backup-pin-floor-state
   (or (kv-info-value lmdb c/wal-backup-pins)
       (get info :wal-backup-pins))
   (System/currentTimeMillis)
   (or (kv-info-value lmdb :wal-backup-pin-floor-lsn)
       (get info :wal-backup-pin-floor-lsn))
   :wal-backup-pin-floor-lsn))

(defn- ^:redef kv-info-map-value
  [lmdb k]
  (txlog/parse-floor-provider-map (kv-info-value lmdb k) k))

(defn- ^:redef with-runtime-txlog-state-guard
  [lmdb f]
  (if-let [info-v (i/kv-info lmdb)]
    (locking info-v
      (f))
    (f)))

(defn- ^:redef with-write-txn-lock-before-runtime-txlog-state
  [lmdb f]
  (let [tx-v (l/write-txn lmdb)]
    (if (Thread/holdsLock tx-v)
      (with-runtime-txlog-state-guard lmdb f)
      (locking tx-v
        (with-runtime-txlog-state-guard lmdb f)))))

(defn- with-runtime-txlog-rollback
  [lmdb f]
  ;; This helper eventually routes through LMDB write APIs. Acquire the write
  ;; transaction lock before the runtime txlog metadata guard so all write-capable
  ;; paths use a consistent lock order.
  (with-write-txn-lock-before-runtime-txlog-state
    lmdb
    (fn []
      (if-let [info-v (i/kv-info lmdb)]
        (let [info @info-v]
          (if (true? (:wal? info))
            (let [had-rollout? (contains? info :wal-rollout-mode)
                  had-rollback? (contains? info :wal-rollback?)
                  prev-rollout (:wal-rollout-mode info)
                  prev-rollback (:wal-rollback? info)]
              ;; Local metadata bookkeeping should not consume replicated WAL
              ;; LSNs used by HA promotion and follower catch-up checks. Guard
              ;; the override so admin APIs do not observe the transient
              ;; rollback mode.
              (vswap! info-v assoc :wal-rollout-mode :rollback :wal-rollback? true)
              (try
                (f)
                (finally
                  (vswap! info-v
                          (fn [m]
                            (cond-> m
                              had-rollout?
                              (assoc :wal-rollout-mode prev-rollout)

                              (not had-rollout?)
                              (dissoc :wal-rollout-mode)

                              had-rollback?
                              (assoc :wal-rollback? prev-rollback)

                              (not had-rollback?)
                              (dissoc :wal-rollback?)))))))
            (f)))
        (f)))))

(defn ^:no-doc transact-kv-without-txlog!
  "Apply local metadata rows without consuming a local txlog LSN."
  [lmdb tx-data]
  (with-runtime-txlog-rollback
    lmdb
    #(i/transact-kv lmdb tx-data)))

(defn- ^:redef write-kv-info-map!
  [lmdb k m]
  (with-runtime-txlog-rollback
    lmdb
    #(if (seq m)
       (i/transact-kv lmdb c/kv-info [[:put k m]] :keyword :data)
       (i/transact-kv lmdb c/kv-info [[:del k]] :keyword :data)))
  m)

(defn- update-kv-info-map-plan!
  [lmdb k plan-f]
  ;; Floor-provider bookkeeping writes through the wrapped LMDB path, so it must
  ;; share the same tx-v -> kv-info lock order as normal transaction close.
  (with-write-txn-lock-before-runtime-txlog-state
    lmdb
    (fn []
      (let [res (plan-f (kv-info-map-value lmdb k))]
        (write-kv-info-map! lmdb k (:entries res))
        (dissoc res :entries)))))

(defn txlog-update-snapshot-floor-state!
  [lmdb snapshot-lsn previous-snapshot-lsn]
  (let [res (txlog/snapshot-floor-update-plan
             snapshot-lsn
             previous-snapshot-lsn
             (kv-info-value lmdb c/wal-snapshot-current-lsn)
             (kv-info-value lmdb c/wal-snapshot-previous-lsn)
             c/wal-snapshot-current-lsn
             c/wal-snapshot-previous-lsn)]
    (i/transact-kv lmdb c/kv-info (:txs res) :keyword :data)
    (dissoc res :txs)))

(defn txlog-clear-snapshot-floor-state!
  [lmdb]
  (let [res (txlog/snapshot-floor-clear-plan
             (kv-info-value lmdb c/wal-snapshot-current-lsn)
             (kv-info-value lmdb c/wal-snapshot-previous-lsn)
             c/wal-snapshot-current-lsn
             c/wal-snapshot-previous-lsn)]
    (i/transact-kv lmdb c/kv-info (:txs res) :keyword :data)
    (dissoc res :txs)))

(defn txlog-update-replica-floor-state!
  [lmdb replica-id applied-lsn]
  (update-kv-info-map-plan!
   lmdb
   c/wal-replica-floors
   (fn [entries]
     (txlog/replica-floor-update-plan
      replica-id
      applied-lsn
      (System/currentTimeMillis)
      entries
      c/wal-replica-floors))))

(defn txlog-clear-replica-floor-state!
  [lmdb replica-id]
  (update-kv-info-map-plan!
   lmdb
   c/wal-replica-floors
   (fn [entries]
     (txlog/replica-floor-clear-plan
      replica-id
      entries
      c/wal-replica-floors))))

(defn txlog-pin-backup-floor-state!
  [lmdb pin-id floor-lsn expires-ms]
  (update-kv-info-map-plan!
   lmdb
   c/wal-backup-pins
   (fn [entries]
     (txlog/backup-pin-floor-update-plan
      pin-id
      floor-lsn
      expires-ms
      (System/currentTimeMillis)
      entries
      c/wal-backup-pins))))

(defn txlog-unpin-backup-floor-state!
  [lmdb pin-id]
  (update-kv-info-map-plan!
   lmdb
   c/wal-backup-pins
   (fn [entries]
     (txlog/backup-pin-floor-clear-plan
      pin-id
      entries
      c/wal-backup-pins))))

(defn- local-dir?
  [dir]
  (and (string? dir)
       (.isDirectory (java.io.File. ^String dir))))

(defn- delete-txlog-segment!
  [{:keys [segment-id path]}]
  (try
    (io/delete-file path)
    {:segment-id segment-id :path path}
    (catch Exception e
      (raise "Failed to delete txn-log segment"
             e
             {:type :txlog/gc-delete-failed
              :segment-id segment-id
              :path path}))))

(defn- txlog-retention-state-map
  [db operator-retain-floor-lsn explicit-gc?]
  (let [runtime-state (txlog/state db)
        watermarks (txlog-watermarks db)
        txlog? (:wal? watermarks)
        dir (:dir watermarks)]
    (when (and txlog? (local-dir? dir))
      (let [info (or (i/env-opts db) {})
            retention-bytes (long (or (:wal-retention-bytes info)
                                      c/*wal-retention-bytes*))
            retention-ms (long (or (:wal-retention-ms info)
                                   c/*wal-retention-ms*))
            marker-state (or (:commit-marker watermarks)
                             (read-commit-marker-state db))
            marker (:current marker-state)
            {:keys [segments marker-record min-retained-lsn total-bytes
                    newest-segment-id]}
            (txlog/segment-summaries
             dir
             {:allow-preallocated-tail? true
              :record->lsn txlog-record-lsn
              :cache-v (some-> runtime-state :segment-summaries-cache)
              :cache-key :txlog-record-lsn
              :active-segment-id
              (some-> runtime-state :segment-id deref long)
              :active-segment-offset
              (some-> runtime-state :segment-offset deref long)
              :marker-segment-id (some-> marker :txlog-segment-id long)
              :marker-offset (some-> marker :txlog-record-offset long)
              :min-retained-fallback (long (or (:next-lsn watermarks) 0))})
            has-records? (boolean (some (comp pos? :record-count) segments))
            {:keys [valid-marker applied-lsn]}
            (txlog/resolve-applied-lsn
             {:commit-marker? (:commit-marker? watermarks)
              :marker marker
              :marker-record marker-record
              :has-records? has-records?
              :meta-last-applied-lsn (:last-applied-lsn watermarks)
              :min-retained-lsn min-retained-lsn})
            snapshot-state (txlog-snapshot-floor-state db info applied-lsn)
            vector-state (txlog-vector-floor-state db info)
            replica-state (txlog-replica-floor-state db info)
            backup-state (txlog-backup-pin-floor-state db info)
            floors (txlog/retention-floors
                    {:snapshot-state snapshot-state
                     :vector-state vector-state
                     :replica-state replica-state
                     :backup-state backup-state
                     :operator-retain-floor-lsn
                     operator-retain-floor-lsn})
            active-segment-id (long (or (:segment-id watermarks) 0))]
        (txlog/retention-state-report
         {:dir dir
          :retention-bytes retention-bytes
          :retention-ms retention-ms
          :segments segments
          :total-bytes total-bytes
          :active-segment-id active-segment-id
          :newest-segment-id newest-segment-id
          :min-retained-lsn min-retained-lsn
          :applied-lsn applied-lsn
          :marker-state marker-state
          :valid-marker valid-marker
          :floors floors
          :floor-providers {:snapshot snapshot-state
                            :vector vector-state
                            :replica replica-state
                            :backup backup-state}
          :explicit-gc? explicit-gc?})))))

(defn- txlog-copy-with-backup-pin!
  [lmdb raw-lmdb dest compact?]
  (if (and *wal-copy-backup-pin-enabled?*
           (txlog-write-path-enabled? raw-lmdb))
    (let [compact? (boolean compact?)
          context
          (with-write-txn-lock-before-runtime-txlog-state
            raw-lmdb
            (fn []
              (when (write-txn-open? raw-lmdb)
                (raise "Cannot copy LMDB while write transaction is open"
                       {:type :txlog/copy-write-transaction-open
                        :dest dest
                        :compact? compact?}))
              (let [state (txlog/enabled-state raw-lmdb)
                    _ (txlog-force-sync! state)
                    _ (force-lmdb-sync-now! raw-lmdb)
                    watermarks (txlog-watermarks-map raw-lmdb state)
                    applied-lsn (long (or (:last-applied-lsn watermarks) 0))
                    snapshot-pin-floor-state
                    (txlog-snapshot-floor-state raw-lmdb
                                                (or (i/env-opts raw-lmdb) {})
                                                applied-lsn)
                    pin-floor-lsn (long (:floor-lsn
                                         snapshot-pin-floor-state))
                    started-ms (System/currentTimeMillis)
                    pin-id (str "backup-copy/" started-ms "-"
                                (java.util.UUID/randomUUID))
                    pin-ttl-ms (long (max 60000
                                          (long (snapshot-max-age-ms
                                                 raw-lmdb))))
                    pin-expires-ms (long (+ (long started-ms) pin-ttl-ms))]
                (txlog-pin-backup-floor-state! raw-lmdb
                                               pin-id
                                               pin-floor-lsn
                                               pin-expires-ms)
                {:lmdb lmdb
                 :dest dest
                 :compact? compact?
                 :pin-id pin-id
                 :pin-floor-lsn pin-floor-lsn
                 :pin-expires-ms pin-expires-ms
                 :applied-lsn applied-lsn
                 :started-ms started-ms})))]
      (try
        (maybe-notify-txn-log-copy-backup-pin-observer! context)
        (maybe-run-txn-log-copy-backup-pin-failpoint! context)
        (i/copy raw-lmdb dest compact?)
        (let [completed-ms (System/currentTimeMillis)]
          {:started-ms (:started-ms context)
           :completed-ms completed-ms
           :duration-ms (max 0 (- completed-ms
                                   (long (:started-ms context))))
           :compact? compact?
           :backup-pin {:pin-id (:pin-id context)
                        :floor-lsn (:pin-floor-lsn context)
                        :expires-ms (:pin-expires-ms context)}})
        (finally
          (try
            (txlog-unpin-backup-floor-state! raw-lmdb (:pin-id context))
            (catch Exception e
              (when-let [info-v (i/kv-info raw-lmdb)]
                (vswap! info-v assoc :copy-last-backup-unpin-error
                        (.getMessage e))))))))
    (i/copy raw-lmdb dest compact?)))

(declare wrap-lmdb)

(defn- txlog-log-dbi-registration!
  [wrapped raw-db dbi-name before after]
  (when (and (not= dbi-name c/kv-info)
             (not= before after)
             (txlog-write-path-enabled? raw-db))
    (i/transact-kv wrapped
                   c/kv-info
                   [[:put [:dbis dbi-name] after]]
                   [:keyword :string]
                   :data)))

(defn- txlog-log-dbi-drop!
  [wrapped raw-db dbi-name before]
  (when (and (not= dbi-name c/kv-info)
             (some? before)
             (txlog-write-path-enabled? raw-db))
    (i/transact-kv wrapped
                   c/kv-info
                   [[:del [:dbis dbi-name]]]
                   [:keyword :string])))

(defmacro def-read-kv-forwarders
  "Expand to KVLMDB methods that forward to `custom-kv/read-kv`.

  `store` is the receiver symbol and `lmdb` the wrapped LMDB expression
  passed to `read-kv`. Each spec is `(method-name [args...])`, with the
  receiver omitted, and expands to
  `(method-name [store args...] (custom-kv/read-kv :method-name store lmdb args...))`."
  [store lmdb & specs]
  (cons 'do
        (map (fn [[mname args]]
               (list mname (into [store] args)
                     (concat ['custom-kv/read-kv (keyword (name mname))
                              store lmdb]
                             args)))
             specs)))

(deftype+ KVLMDB [db]
  l/IWriting
  (writing? [_] (l/writing? db))
  (write-txn [_] (l/write-txn db))
  (mark-write [_] (wrap-lmdb (l/mark-write db)))
  (reset-write [_] (wrap-lmdb (l/reset-write db)))

  i/IList
  (del-list-items [this list-name k k-type]
    (i/transact-kv this [(l/kv-tx :del list-name k k-type)]))
  (del-list-items [this list-name k vs k-type v-type]
    (i/transact-kv this [(l/kv-tx :del-list list-name k vs k-type v-type)]))
  (list-dbi? [this dbi-name] (i/list-dbi? db dbi-name))
  (put-list-items [this list-name k vs k-type v-type]
    (i/transact-kv this [(l/kv-tx :put-list list-name k vs k-type v-type)]))

  (def-read-kv-forwarders this db
    (get-list [list-name k k-type v-type])
    (in-list? [list-name k v k-type v-type])
    (list-count [list-name k k-type])
    (list-range [list-name k-range k-type v-range v-type])
    (list-range-count [list-name k-range k-type])
    (list-range-filter [list-name pred k-range k-type v-range v-type])
    (list-range-filter [list-name pred k-range k-type v-range v-type raw-pred?])
    (list-range-filter-count [list-name pred k-range k-type v-range v-type])
    (list-range-filter-count
      [list-name pred k-range k-type v-range v-type raw-pred?])
    (list-range-first [list-name k-range k-type v-range v-type])
    (list-range-first-n [list-name n k-range k-type v-range v-type])
    (list-range-keep [list-name pred k-range k-type v-range v-type])
    (list-range-keep [list-name pred k-range k-type v-range v-type raw-pred?])
    (list-range-some [list-name pred k-range k-type v-range v-type])
    (list-range-some [list-name pred k-range k-type v-range v-type raw-pred?])
    (near-list [list-name k v k-type v-type])
    (visit-list [list-name visitor k k-type])
    (visit-list [list-name visitor k k-type v-type])
    (visit-list [list-name visitor k k-type v-type raw-pred?])
    (visit-list-key-range [list-name visitor k-range k-type v-type])
    (visit-list-key-range [list-name visitor k-range k-type v-type raw-pred?])
    (visit-list-range [list-name visitor k-range k-type v-range v-type])
    (visit-list-range [list-name visitor k-range k-type v-range v-type raw-pred?])
    (visit-list-sample [list-name indices visitor k-range k-type v-type])
    (visit-list-sample
      [list-name indices visitor k-range k-type v-type raw-pred?]))

  i/IAdmin
  (re-index [this opts] (i/re-index db opts))
  (re-index [this schema opts] (i/re-index db schema opts))

  i/ITxLog
  (txlog-watermarks [_]
    (if-let [state (txlog/state db)]
      (txlog-watermarks-map db state)
      (if (txlog-config-enabled? db)
        (txlog-rollout-watermarks db (txlog-rollout-mode db))
        {:wal? false})))

  (open-tx-log [this from-lsn]
    (.open-tx-log this from-lsn nil))
  (open-tx-log [_ from-lsn upto-lsn]
    (if-let [state (or (txlog/state db)
                       (when (txlog-write-path-enabled? db)
                         (ensure-txlog-ready! db)))]
      (do
      (txlog/refresh-shared-state! state)
      (txlog/select-open-records
       (txlog-records state from-lsn upto-lsn)
       from-lsn
       upto-lsn))
     (if (txlog-config-enabled? db)
       []
       (txlog/select-open-records
        (txlog-records (txlog/enabled-state db) from-lsn upto-lsn)
        from-lsn
        upto-lsn))))

  (force-txlog-sync! [_]
    (with-runtime-txlog-state-guard
      db
      (fn []
        (cond
          (not (txlog-config-enabled? db))
          (txlog/enabled-state db)

          (not (txlog-write-path-enabled? db))
          (let [rollout-mode (txlog-rollout-mode db)]
            {:synced? false
             :skipped? true
             :reason :rollback
             :watermarks (txlog-rollout-watermarks db rollout-mode)})

          :else
          (let [state (txlog/enabled-state db)]
            (assoc (txlog-force-sync! state)
                   :watermarks (txlog-watermarks-map db state)))))))

  (force-lmdb-sync! [_]
    (if (txlog-config-enabled? db)
      (do
        (force-lmdb-sync-now! db)
        {:synced? true
         :watermarks (if-let [state (txlog/state db)]
                       (txlog-watermarks-map db state)
                       (txlog-rollout-watermarks db
                                                 (txlog-rollout-mode db)))})
      (txlog/enabled-state db)))

  (create-snapshot! [_]
    ;; Snapshot creation updates backup-pin floor metadata before copying the
    ;; environment, so it participates in the same write-capable lock ordering
    ;; as replica-floor bookkeeping and transaction close.
    (with-write-txn-lock-before-runtime-txlog-state
      db
      (fn []
        (if (txlog-write-path-enabled? db)
          (create-snapshot-now! db)
          (if (txlog-config-enabled? db)
            (let [rollout-mode (txlog-rollout-mode db)]
              {:ok? false
               :skipped? true
               :reason :rollback
               :watermarks (txlog-rollout-watermarks db rollout-mode)})
            (txlog/enabled-state db))))))

  (list-snapshots [_]
    (list-snapshot-entries db))

  (snapshot-scheduler-state [_]
    (snapshot-scheduler-state-map db))

  (read-commit-marker [_]
    (if-let [state (txlog/state db)]
      (do
        (refresh-runtime-marker-revision! db state)
        (assoc (read-commit-marker-state db)
               :commit-marker? (boolean (:commit-marker? state))
               :marker-revision (long @(:marker-revision state))))
      {:commit-marker? false
       :slot-a nil
       :slot-b nil
       :current nil}))

  (verify-commit-marker! [_]
    (with-runtime-txlog-state-guard
      db
      (fn []
        (if-let [state (txlog/state db)]
          (verify-commit-marker-state db state)
          (if (txlog-config-enabled? db)
            (let [rollout-mode (txlog-rollout-mode db)]
              {:ok? false
               :skipped? true
               :reason :rollback
               :watermarks (txlog-rollout-watermarks db rollout-mode)})
            (verify-commit-marker-state db (txlog/enabled-state db)))))))

  (txlog-retention-state [this]
    (txlog-retention-state-local db))

  (gc-txlog-segments! [this]
    (.gc-txlog-segments! this nil))
  (gc-txlog-segments! [this retain-floor-lsn]
    (gc-txlog-segments-local! db retain-floor-lsn))

  (txlog-update-snapshot-floor! [this snapshot-lsn]
    (.txlog-update-snapshot-floor! this snapshot-lsn nil))
  (txlog-update-snapshot-floor! [_ snapshot-lsn previous-snapshot-lsn]
    (txlog-update-snapshot-floor! db snapshot-lsn previous-snapshot-lsn))

  (txlog-clear-snapshot-floor! [_]
    (txlog-clear-snapshot-floor! db))

  (txlog-update-replica-floor! [_ replica-id applied-lsn]
    (txlog-update-replica-floor! db replica-id applied-lsn))

  (txlog-clear-replica-floor! [_ replica-id]
    (txlog-clear-replica-floor! db replica-id))

  (txlog-pin-backup-floor! [this pin-id floor-lsn]
    (.txlog-pin-backup-floor! this pin-id floor-lsn nil))
  (txlog-pin-backup-floor! [_ pin-id floor-lsn expires-ms]
    (txlog-pin-backup-floor! db pin-id floor-lsn expires-ms))

  (txlog-unpin-backup-floor! [_ pin-id]
    (txlog-unpin-backup-floor! db pin-id))

  i/ILMDB
  (open-transact-kv [_]
    (when (txlog-write-path-enabled? db)
      (ensure-txlog-ready! db))
    (let [wdb (i/open-transact-kv db)]
      (when (txlog-write-path-enabled? db)
        (txlog-reset-pending! (i/kv-info db)))
      (->KVLMDB wdb)))
  (abort-transact-kv [_]
    (when (txlog-config-enabled? db)
      (txlog-reset-pending! (i/kv-info db)))
    (i/abort-transact-kv db))
  (check-ready [this] (i/check-ready db))
  (clear-dbi [this dbi-name]
    (custom-kv/guard-internal! db dbi-name)
    (if (custom-kv/custom-dbi? db dbi-name)
      (custom-kv/clear! this db dbi-name)
      (i/clear-dbi db dbi-name)))
  (close-kv [_]
    (try
      (i/close-kv db)
      (finally
        (close-txlog-state! db))))
  (close-transact-kv [_]
    (with-write-txn-lock-before-runtime-txlog-state
      db
      (fn []
        (if (txlog-write-path-enabled? db)
          (if-let [state (txlog-runtime-state db)]
            (close-with-txlog! db state)
            (i/close-transact-kv db))
          (i/close-transact-kv db)))))
  (closed-kv? [this] (i/closed-kv? db))
  (copy [this dest] (.copy this dest false))
  (copy [this dest compact?] (txlog-copy-with-backup-pin! this db dest compact?))
  (dbi-opts [this dbi-name] (i/dbi-opts db dbi-name))
  (drop-dbi [this dbi-name]
    (custom-kv/guard-internal! db dbi-name)
    (locking (l/write-txn db)
      (when (and (custom-kv/custom-dbi? db dbi-name) (some? @(l/write-txn db)))
        (raise "Drop a custom DBI outside an explicit transaction"
               {:error :custom-type/drop-transaction :dbi dbi-name}))
      (when (custom-kv/custom-dbi? db dbi-name)
        (custom-kv/clear! this db dbi-name))
      (let [before (try
                     (i/dbi-opts db dbi-name)
                     (catch Exception _ nil))
            res    (i/drop-dbi db dbi-name)]
        (txlog-log-dbi-drop! this db dbi-name before)
        res)))
  (entries [this dbi-name] (i/entries db dbi-name))
  (env-dir [this] (i/env-dir db))
  (kv-info [this] (i/kv-info db))
  (env-opts [this] (i/env-opts db))
  (get-dbi [this dbi-name] (i/get-dbi db dbi-name))
  (get-dbi [this dbi-name create?] (i/get-dbi db dbi-name create?))
  (get-env-flags [this] (i/get-env-flags db))
  (get-rtx [this] (i/get-rtx db))
  (key-compressor [this] (i/key-compressor db))
  (list-dbis [this] (i/list-dbis db))
  (max-val-size [this] (i/max-val-size db))
  (open-dbi [this dbi-name]
    (.open-dbi this dbi-name nil))
  (open-dbi [this dbi-name opts]
    (let [before   (try
                     (i/dbi-opts db dbi-name)
                     (catch Exception _ nil))
          prepared (if l/*raw-kv?* opts
                       (custom-kv/prepare-dbi! this db dbi-name opts))
          res      (i/open-dbi db dbi-name prepared)
          after    (i/dbi-opts db dbi-name)]
      (txlog-log-dbi-registration! this db dbi-name before after)
      res))
  (open-list-dbi [this list-name]
    (.open-list-dbi this list-name nil))
  (open-list-dbi [this list-name opts]
    (let [before   (try
                     (i/dbi-opts db list-name)
                     (catch Exception _ nil))
          supplied (assoc opts :flags
                          (conj (set (or (:flags opts) (:flags before)
                                         c/default-dbi-flags))
                                :dupsort))
          prepared (if l/*raw-kv?* supplied
                       (custom-kv/prepare-dbi! this db list-name supplied))
          res      (i/open-list-dbi db list-name prepared)
          after    (i/dbi-opts db list-name)]
      (txlog-log-dbi-registration! this db list-name before after)
      res))
  (return-rtx [this rtx] (i/return-rtx db rtx))
  (set-env-flags [this ks on-off] (i/set-env-flags db ks on-off))
  (set-key-compressor [this c] (i/set-key-compressor db c))
  (set-max-val-size [this size] (i/set-max-val-size db size))
  (set-val-compressor [this c] (i/set-val-compressor db c))
  (stat [this] (i/stat db))
  (stat [this dbi-name] (i/stat db dbi-name))
  (sync [this] (i/sync db))
  (sync [this force] (i/sync db force))
  (transact-kv [this txs] (.transact-kv this nil txs))
  (transact-kv [this dbi-name txs]
    (.transact-kv this dbi-name txs :data :data))
  (transact-kv [this dbi-name txs k-type]
    (.transact-kv this dbi-name txs k-type :data))
  (transact-kv [this dbi-name txs k-type v-type]
    (if (custom-kv/custom-txs? db dbi-name txs)
      (custom-kv/transact! this db dbi-name txs k-type v-type)
      (with-write-txn-lock-before-runtime-txlog-state
        db
        (fn []
          (if (txlog-write-path-enabled? db)
            (if-let [state (txlog-runtime-state db)]
              (transact-with-txlog! db state dbi-name txs k-type v-type)
              (i/transact-kv db dbi-name txs k-type v-type))
            (i/transact-kv db dbi-name txs k-type v-type))))))
  (val-compressor [this] (i/val-compressor db))

  (def-read-kv-forwarders this db
    (get-by-rank [dbi-name rank])
    (get-by-rank [dbi-name rank k-type])
    (get-by-rank [dbi-name rank k-type v-type])
    (get-by-rank [dbi-name rank k-type v-type ignore-key?])
    (get-first [dbi-name k-range])
    (get-first [dbi-name k-range k-type])
    (get-first [dbi-name k-range k-type v-type])
    (get-first [dbi-name k-range k-type v-type ignore-key?])
    (get-first-n [dbi-name n k-range])
    (get-first-n [dbi-name n k-range k-type])
    (get-first-n [dbi-name n k-range k-type v-type])
    (get-first-n [dbi-name n k-range k-type v-type ignore-key?])
    (get-range [dbi-name k-range])
    (get-range [dbi-name k-range k-type])
    (get-range [dbi-name k-range k-type v-type])
    (get-range [dbi-name k-range k-type v-type ignore-key?])
    (get-rank [dbi-name k])
    (get-rank [dbi-name k k-type])
    (get-some [dbi-name pred k-range])
    (get-some [dbi-name pred k-range k-type])
    (get-some [dbi-name pred k-range k-type v-type])
    (get-some [dbi-name pred k-range k-type v-type ignore-key?])
    (get-some [dbi-name pred k-range k-type v-type ignore-key? raw-pred?])
    (get-value [dbi-name k])
    (get-value [dbi-name k k-type])
    (get-value [dbi-name k k-type v-type])
    (get-value [dbi-name k k-type v-type ignore-key?])
    (key-range [dbi-name k-range])
    (key-range [dbi-name k-range k-type])
    (key-range-count [dbi-name k-range])
    (key-range-count [dbi-name k-range k-type])
    (key-range-list-count [dbi-name k-range k-type])
    (range-count [dbi-name k-range])
    (range-count [dbi-name k-range k-type])
    (range-filter [dbi-name pred k-range])
    (range-filter [dbi-name pred k-range k-type])
    (range-filter [dbi-name pred k-range k-type v-type])
    (range-filter [dbi-name pred k-range k-type v-type ignore-key?])
    (range-filter [dbi-name pred k-range k-type v-type ignore-key? raw-pred?])
    (range-filter-count [dbi-name pred k-range])
    (range-filter-count [dbi-name pred k-range k-type])
    (range-filter-count [dbi-name pred k-range k-type v-type])
    (range-filter-count [dbi-name pred k-range k-type v-type raw-pred?])
    (range-keep [dbi-name pred k-range])
    (range-keep [dbi-name pred k-range k-type])
    (range-keep [dbi-name pred k-range k-type v-type])
    (range-keep [dbi-name pred k-range k-type v-type raw-pred?])
    (range-seq [dbi-name k-range])
    (range-seq [dbi-name k-range k-type])
    (range-seq [dbi-name k-range k-type v-type])
    (range-seq [dbi-name k-range k-type v-type ignore-key?])
    (range-seq [dbi-name k-range k-type v-type ignore-key? opts])
    (range-some [dbi-name pred k-range])
    (range-some [dbi-name pred k-range k-type])
    (range-some [dbi-name pred k-range k-type v-type])
    (range-some [dbi-name pred k-range k-type v-type raw-pred?])
    (sample-kv [dbi-name n])
    (sample-kv [dbi-name n k-type])
    (sample-kv [dbi-name n k-type v-type])
    (sample-kv [dbi-name n k-type v-type ignore-key?])
    (visit [dbi-name visitor k-range])
    (visit [dbi-name visitor k-range k-type])
    (visit [dbi-name visitor k-range k-type v-type])
    (visit [dbi-name visitor k-range k-type v-type raw-pred?])
    (visit-key-range [dbi-name visitor k-range])
    (visit-key-range [dbi-name visitor k-range k-type])
    (visit-key-range [dbi-name visitor k-range k-type raw-pred?])
    (visit-key-sample [dbi-name indices visitor k-range k-type])
    (visit-key-sample [dbi-name indices visitor k-range k-type raw-pred?])))

(defn raw-lmdb
  [db]
  (if (instance? KVLMDB db)
    (.-db ^KVLMDB db)
    db))

(defn wrap-lmdb
  [db]
  (if (instance? KVLMDB db)
    db
    (let [fallback-attempted?
          (true? (:txlog-recovery-fallback-attempted?
                  (i/env-opts db)))]
      (try
        (ensure-txlog-ready! db)
        (let [wrapped (->KVLMDB db)]
          (when-not (l/writing? db) (custom-kv/initialize! wrapped db))
          wrapped)
        (catch Exception e
          (if fallback-attempted?
            (do
              (close-failed-open! db)
              (throw e))
            (if-let [recovered (recover-from-snapshot-open! db e)]
              recovered
              (do
                (close-failed-open! db)
                (throw e)))))))))

(l/set-open-kv-wrapper! wrap-lmdb)
