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
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u :refer [raise]])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]))

(declare txlog-retention-state-map
         delete-txlog-segment!
         txlog-records
         txlog-record-lsn
         snapshot-current-lsn
         txlog-snapshot-floor-state
         txlog-recovery-context
         txlog-force-sync!
         txlog-config-enabled?
         txlog-rollout-mode
         txlog-rollout-watermarks
         txlog-write-path-enabled?
         txlog-watermarks-map
         txlog-update-snapshot-floor-state!
         txlog-clear-snapshot-floor-state!
         txlog-update-replica-floor-state!
         txlog-clear-replica-floor-state!
         txlog-pin-backup-floor-state!
         txlog-unpin-backup-floor-state!)

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

(defn- txlog-retention-state-local
  [db]
  (if-let [state (txlog-retention-state-map db nil false)]
    (dissoc state :gc-target-segments)
    (if (txlog-config-enabled? db)
      (let [rollout-mode (txlog-rollout-mode db)]
        {:wal? true
         :skipped? true
         :reason :rollback
         :watermarks (txlog-rollout-watermarks db rollout-mode)})
      {:wal? false})))

(defn- gc-txlog-segments-local!
  [db retain-floor-lsn]
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
       :watermarks {:wal? false}})))

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

(defn- force-lmdb-sync-now!
  [lmdb]
  (try
    (i/sync lmdb 1)
    (mark-lmdb-checkpoint! lmdb)
    (catch Exception e
      (when-let [info-v (i/kv-info lmdb)]
        (vswap! info-v assoc :txlog-last-lmdb-sync-error (.getMessage e)))
      (throw e))))

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
  (let [info  @info-v
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

(defn- maybe-run-snapshot-scheduler!
  [lmdb]
  (when (and (snapshot-scheduler-enabled? lmdb)
             (not (rdonly-env? lmdb))
             (snapshot-source-ready? lmdb))
    (let [tx-v (l/write-txn lmdb)]
      (when-not (and (some? tx-v) (some? @tx-v))
        (let [info-v (i/kv-info lmdb)
              {:keys [lock]} (ensure-snapshot-scheduler-runtime! lmdb)
              now-ms (System/currentTimeMillis)]
          (locking lock
            (vswap! info-v assoc :snapshot-scheduler-last-run-ms now-ms)
            (let [next-eligible-ms (:snapshot-scheduler-next-eligible-ms @info-v)]
              (when-not (and (number? next-eligible-ms)
                             (< now-ms (long next-eligible-ms)))
                (if-let [trigger (snapshot-scheduler-trigger lmdb now-ms)]
                  (let [trigger-k (:trigger trigger)]
                    (when (= trigger-k :max-age)
                      (vswap! info-v
                              (fn [m]
                                (-> m
                                    (update :snapshot-scheduler-max-age-breach-count
                                            (fnil inc 0))
                                    (assoc
                                     :snapshot-scheduler-last-max-age-breach-ms
                                     now-ms)))))
                    (if-let [defer (snapshot-scheduler-defer-reason
                                    lmdb trigger now-ms)]
                      (let [min-backoff-ms (long
                                             (snapshot-defer-backoff-min-ms lmdb))
                            max-backoff-ms (long
                                             (snapshot-defer-backoff-max-ms lmdb))
                            m (vswap! info-v
                                      (fn [m]
                                        (let [reason (:reason defer)
                                              defer-count (or
                                                           (:snapshot-scheduler-defer-count
                                                            m)
                                                           {})
                                              defer-since-ms (long
                                                              (or (:snapshot-scheduler-defer-since-ms
                                                                   m)
                                                                  now-ms))
                                              defer-ms (max 0
                                                            (- now-ms
                                                               defer-since-ms))
                                              prev-backoff-ms (long
                                                               (or (:snapshot-scheduler-defer-backoff-ms
                                                                    m)
                                                                   0))
                                              backoff-ms (if (pos?
                                                              prev-backoff-ms)
                                                           (long
                                                            (min
                                                             max-backoff-ms
                                                             (max
                                                              min-backoff-ms
                                                              (long (* 2
                                                                       prev-backoff-ms)))))
                                                           min-backoff-ms)
                                              next-ms (long (+ (long now-ms)
                                                               backoff-ms))
                                              defer* (assoc defer
                                                            :backoff-ms
                                                            backoff-ms
                                                            :next-eligible-ms
                                                            next-ms)]
                                          (assoc m
                                                 :snapshot-scheduler-last-trigger
                                                 trigger-k
                                                 :snapshot-scheduler-last-trigger-details
                                                 trigger
                                                 :snapshot-scheduler-last-defer-ms
                                                 now-ms
                                                 :snapshot-scheduler-last-defer-reason
                                                 reason
                                                 :snapshot-scheduler-last-defer-trigger
                                                 trigger-k
                                                 :snapshot-scheduler-last-defer-details
                                                 defer*
                                                 :snapshot-scheduler-defer-since-ms
                                                 defer-since-ms
                                                 :snapshot-scheduler-last-defer-duration-ms
                                                 defer-ms
                                                 :snapshot-scheduler-next-eligible-ms
                                                 next-ms
                                                 :snapshot-scheduler-defer-backoff-ms
                                                 backoff-ms
                                                 :snapshot-scheduler-defer-count
                                                 (update defer-count
                                                         reason
                                                         (fnil inc 0))
                                                 :snapshot-scheduler-last-error
                                                 nil))))]
                        {:deferred? true
                         :trigger trigger-k
                         :defer (:snapshot-scheduler-last-defer-details m)
                         :defer-duration-ms
                         (:snapshot-scheduler-last-defer-duration-ms m)
                         :backoff-ms (:snapshot-scheduler-defer-backoff-ms m)
                         :next-eligible-ms
                         (:snapshot-scheduler-next-eligible-ms m)})
                      (let [run-start-ms (System/currentTimeMillis)]
                        (vswap! info-v
                                (fn [m]
                                  (let [defer-since-ms
                                        (:snapshot-scheduler-defer-since-ms m)
                                        defer-ms (when (number? defer-since-ms)
                                                   (max 0
                                                        (- run-start-ms
                                                           (long defer-since-ms))))
                                        m' (-> m
                                               (assoc
                                                :snapshot-scheduler-last-run-start-ms
                                                run-start-ms)
                                               (dissoc
                                                :snapshot-scheduler-next-eligible-ms
                                                :snapshot-scheduler-defer-backoff-ms
                                                :snapshot-scheduler-defer-since-ms))]
                                    (if (number? defer-ms)
                                      (-> m'
                                          (update :snapshot-scheduler-defer-duration-ms
                                                  (fnil + 0)
                                                  (long defer-ms))
                                          (assoc
                                           :snapshot-scheduler-last-defer-duration-ms
                                           (long defer-ms)))
                                      m'))))
                        (try
                          (let [res (create-snapshot-now! lmdb)
                                run-finished-ms (System/currentTimeMillis)
                                run-duration-ms (max 0
                                                     (- run-finished-ms
                                                        run-start-ms))]
                            (vswap! info-v
                                    (fn [m]
                                      (-> m
                                          (update :snapshot-scheduler-run-count
                                                  (fnil inc 0))
                                          (update :snapshot-scheduler-run-duration-ms
                                                  (fnil + 0)
                                                  run-duration-ms)
                                          (assoc
                                           :snapshot-scheduler-last-run-finished-ms
                                           run-finished-ms
                                           :snapshot-scheduler-last-run-duration-ms
                                           run-duration-ms
                                           :snapshot-scheduler-last-success-ms
                                           run-finished-ms
                                           :snapshot-scheduler-last-trigger
                                           trigger-k
                                           :snapshot-scheduler-last-trigger-details
                                           trigger
                                           :snapshot-scheduler-consecutive-failure-count
                                           0
                                           :snapshot-scheduler-last-error nil))))
                            (assoc res
                                   :trigger trigger-k
                                   :run-duration-ms run-duration-ms))
                          (catch Exception e
                            (let [run-finished-ms (System/currentTimeMillis)
                                  run-duration-ms (max 0
                                                       (- run-finished-ms
                                                          run-start-ms))]
                              (vswap! info-v
                                      (fn [m]
                                        (-> m
                                            (update :snapshot-scheduler-run-count
                                                    (fnil inc 0))
                                            (update :snapshot-scheduler-failure-count
                                                    (fnil inc 0))
                                            (update
                                             :snapshot-scheduler-consecutive-failure-count
                                             (fnil inc 0))
                                            (update
                                             :snapshot-scheduler-run-duration-ms
                                             (fnil + 0)
                                             run-duration-ms)
                                            (assoc
                                             :snapshot-scheduler-last-run-finished-ms
                                             run-finished-ms
                                             :snapshot-scheduler-last-run-duration-ms
                                             run-duration-ms
                                             :snapshot-scheduler-last-failure-ms
                                             run-finished-ms
                                             :snapshot-scheduler-last-trigger
                                             trigger-k
                                             :snapshot-scheduler-last-trigger-details
                                             trigger
                                             :snapshot-scheduler-last-error
                                             (.getMessage e)))))
                              nil))))))
                  (vswap! info-v dissoc
                          :snapshot-scheduler-defer-since-ms
                          :snapshot-scheduler-next-eligible-ms
                          :snapshot-scheduler-defer-backoff-ms))))))))))

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

(defn- sanitize-kv-info!
  [info-v]
  (vswap! info-v dissoc c/wal-marker-a c/wal-marker-b)
  info-v)

(defn- ensure-fast-list
  ^FastList [x]
  (cond
    (instance? FastList x)
    x

    (instance? java.util.Collection x)
    (FastList. ^java.util.Collection x)

    (nil? x)
    (FastList.)

    :else
    (let [^FastList out (FastList.)]
      (doseq [v x]
        (.add out v))
      out)))

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
            (if (l/resized? e)
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
        init   {:vec-checkpoint-count 0
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
               cp-state   (if (map? (:checkpoint index-info))
                            (:checkpoint index-info)
                            {})
               count      (nonneg-long (:vec-checkpoint-count cp-state))
               duration   (nonneg-long (:vec-checkpoint-duration-ms cp-state))
               bytes      (nonneg-long (:vec-checkpoint-bytes cp-state))
               failures   (nonneg-long (:vec-checkpoint-failure-count cp-state))
               replay-lag (nonneg-long (:vec-replay-lag-lsn cp-state))
               domain     (txlog-vector-domain db-dir fname index-info)]
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
                               (:meta-last-applied-lsn state)
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
  (let [sync-manager (:sync-manager state)
        timeout-ms (long (:commit-wait-ms state))
        before (txlog/sync-manager-state sync-manager)
        target-lsn (long (:last-appended-lsn before))
        durable-lsn (long (:last-durable-lsn before))]
    (when (> target-lsn durable-lsn)
      (when-let [{:keys [target-lsn reason]} (txlog/begin-sync! sync-manager)]
        (let [ch @(:segment-channel state)]
          (when-not ch
            (raise "Txn-log segment channel is not available"
                   {:type :txlog/no-segment-channel}))
          (try
            (txlog/force-segment! state ch (:sync-mode state))
            (txlog/complete-sync-success! sync-manager
                                          target-lsn
                                          (System/currentTimeMillis)
                                          reason
                                          false)
            (catch Exception e
              (txlog/complete-sync-failure! sync-manager e false)
              (txlog-mark-fatal! state e)
              (throw e)))))
      (txlog/await-durable-lsn! sync-manager target-lsn timeout-ms
                                (System/currentTimeMillis)))
    (let [after (txlog/sync-manager-state sync-manager)]
      {:target-lsn target-lsn
       :last-appended-lsn (long (:last-appended-lsn after))
       :last-durable-lsn (long (:last-durable-lsn after))
       :pending-count (long (:pending-count after))
       :synced? (<= target-lsn (long (:last-durable-lsn after)))})))

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
  (let [records (txlog-records state)
        marker-state (read-commit-marker-state lmdb)
        recovery (txlog/recovery-state
                  {:commit-marker? (:commit-marker? state)
                   :marker-state marker-state
                   :records records
                   :meta-last-applied-lsn (:meta-last-applied-lsn state)
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
        (let [append-lock (or (:append-lock state) state)
              ch          (locking append-lock
                            (when-let [segment-channel-v
                                       (:segment-channel state)]
                              (let [ch @segment-channel-v]
                                (when ch
                                  (vreset! segment-channel-v nil))
                                ch)))]
          (when ch
            (try
              (.close ^java.io.Closeable ch)
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
      (let [typ (:type (ex-data t))
            msg (.getMessage ^Throwable t)]
        (if (or (contains? txlog-recovery-fallback-types typ)
                (and (keyword? typ) (= "txlog" (namespace typ)))
                (and (string? msg) (.contains ^String msg "Txn-log")))
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
              (let [attempt
                    (try
                      (copy-snapshot-lmdb-files! env-dir (:path snapshot))
                      {:reopened
                       (txlog-mark-recovery-source!
                        (l/open-kv env-dir reopen-opts)
                        snapshot
                        cause)}
                      (catch Exception e
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
      (let [opts (or (get-in info [:dbis dbi-name])
                     (raise "DBI is not available for txn-log replay"
                            {:dbi dbi-name :type :txlog/replay-missing-dbi}))]
        (i/open-dbi lmdb dbi-name opts))))

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
        (vswap! info-v assoc-in [:dbis (nth k 1)] v)

        (and (= op :del)
             (vector? k)
             (= 2 (count k))
             (= :dbis (nth k 0))
             (string? (nth k 1)))
        (vswap! info-v update :dbis dissoc (nth k 1))

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
         (:rows record))]
    (i/transact-kv lmdb rows)
    (txlog/commit-finished! state marker-entry)
    record))

(defn txlog-recover-on-open!
  [lmdb]
  (when-let [state (txlog/state lmdb)]
    (let [{:keys [records valid-marker from-lsn]}
          (txlog-recovery-context lmdb state)]
      (when (and (:commit-marker? state) valid-marker)
        (vreset! (:marker-revision state) (long (:revision valid-marker))))
      (txlog-prepare-replay-dbis! lmdb records from-lsn)
      (let [replayed (->> records
                          (drop-while #(<= (long (:lsn %))
                                           (long from-lsn)))
                          (mapv #(txlog-replay-record! lmdb state %)))]
        (when-let [last-record (peek replayed)]
          (txlog/publish-meta-best-effort! state
                                           {:lsn (:lsn last-record)
                                            :segment-id (:segment-id last-record)
                                            :offset (:offset last-record)}))
        (i/set-max-val-size lmdb (i/max-val-size lmdb))
        {:from-lsn (long from-lsn)
         :replayed (count replayed)}))))

(defn- ensure-snapshot-bootstrap!
  [lmdb]
  (when (and (snapshot-bootstrap-force? lmdb)
             (not (rdonly-env? lmdb))
             (snapshot-source-ready? lmdb))
    (let [snapshots (list-snapshot-entries lmdb)]
      (when (< (count snapshots) 1)
        (create-snapshot-now! lmdb))
      (when (< (count (list-snapshot-entries lmdb)) 2)
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
          (txlog-recover-on-open! lmdb)
          (ensure-snapshot-bootstrap! lmdb)
          (vswap! info-v assoc :txlog-recovered? true))
        (start-snapshot-scheduler! lmdb)
        state))))

(defn- ensure-tx-dbi
  [dbi-name tx]
  (let [^datalevin.lmdb.KVTxData tx tx]
    (if (or (nil? dbi-name) (some? (.-dbi-name tx)))
      tx
      (datalevin.lmdb.KVTxData.
       (.-op tx) dbi-name (.-k tx) (.-v tx)
       (.-kt tx) (.-vt tx) (.-flags tx)))))

(defn- canonicalize-input-kvtxs
  [dbi-name txs kt vt]
  (let [^FastList out
        (if (instance? java.util.Collection txs)
          (FastList. (.size ^java.util.Collection txs))
          (FastList.))]
    (if (instance? java.util.List txs)
      (let [^java.util.List tx-list txs
            n (.size tx-list)]
        (dotimes [i n]
          (let [t (.get tx-list i)
                tx (if (instance? datalevin.lmdb.KVTxData t)
                     t
                     (if dbi-name
                       (l/->kv-tx-data t kt vt)
                       (l/->kv-tx-data t)))]
            (.add out (ensure-tx-dbi dbi-name tx)))))
      (doseq [t txs]
        (let [tx (if (instance? datalevin.lmdb.KVTxData t)
                   t
                   (if dbi-name
                     (l/->kv-tx-data t kt vt)
                     (l/->kv-tx-data t)))]
          (.add out (ensure-tx-dbi dbi-name tx)))))
    out))

(def ^:private txlog-append-hooks
  {:throw-if-fatal! txlog-throw-if-fatal!
   :mark-fatal! txlog-mark-fatal!})

(defn- write-txn-open?
  [lmdb]
  (let [tx-v (l/write-txn lmdb)]
    (and (some? tx-v)
         (some? @tx-v))))

(defn- txlog-commit-state
  [state]
  {:commit-marker? (:commit-marker? state)
   :marker-revision (long @(:marker-revision state))})

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

(def ^:private txlog-retention-backpressure-check-interval-ms
  1000)

(def ^:private txlog-retention-backpressure-idle-check-interval-ms
  60000)

(def ^:private txlog-retention-pin-floor-limiters
  #{:replica-floor-lsn :backup-pin-floor-lsn :operator-retain-floor-lsn})

(defn- txlog-retention-pin-backpressure-threshold-ms
  [lmdb]
  (let [info (or (i/env-opts lmdb) {})
        configured (:wal-retention-pin-backpressure-threshold-ms info)]
    (long (or configured
              c/*wal-retention-pin-backpressure-threshold-ms*))))

(defn- txlog-retention-backpressure-state
  [lmdb state]
  (let [check-v  (:retention-backpressure-last-check-ms state)
        cached-v (:retention-backpressure-state state)
        blocked-v (:retention-backpressure-blocked-since-ms state)
        total-bytes-v (:retention-total-bytes state)
        now-ms   (System/currentTimeMillis)
        info     (or (i/env-opts lmdb) {})
        retention-bytes (long (or (:wal-retention-bytes info)
                                  c/*wal-retention-bytes*))
        total-bytes (when (some? total-bytes-v) (long @total-bytes-v))
        cached-report (when (some? cached-v) @cached-v)
        bytes-pressure?
        (and (number? total-bytes)
             (> ^long total-bytes ^long retention-bytes))
        tighten? (or bytes-pressure?
                     (true? (:degraded-now? cached-report))
                     (true? (:degraded? cached-report)))
        interval-ms (long (if tighten?
                            txlog-retention-backpressure-check-interval-ms
                            txlog-retention-backpressure-idle-check-interval-ms))
        cached?  (and (some? check-v)
                      (some? cached-v)
                      (< (- now-ms (long @check-v))
                         interval-ms))]
    (if cached?
      cached-report
      (let [state-map (txlog-retention-state-map lmdb nil false)
            pressure  (:pressure state-map)
            floor-limiters (vec (:floor-limiters state-map))
            degraded-now? (true? (:degraded? pressure))
            pin-limited? (boolean (some txlog-retention-pin-floor-limiters
                                        floor-limiters))
            threshold-ms (txlog-retention-pin-backpressure-threshold-ms lmdb)
            blocked-since-old (when (some? blocked-v) @blocked-v)
            blocked-since-ms (cond
                               (not (map? state-map))
                               nil

                               (and degraded-now? pin-limited?
                                    (number? blocked-since-old))
                               (long blocked-since-old)

                               (and degraded-now? pin-limited?)
                               now-ms

                               :else
                               nil)
            blocked-for-ms (when (some? blocked-since-ms)
                             (long (max 0 (- (long now-ms)
                                             (long blocked-since-ms)))))
            degraded? (if (and degraded-now? pin-limited?)
                        (and blocked-for-ms
                             (>= ^long blocked-for-ms ^long threshold-ms))
                        degraded-now?)
            report    (when (map? state-map)
                        {:degraded?                 degraded?
                         :degraded-now?             degraded-now?
                         :pin-limited?              pin-limited?
                         :pin-backpressure-threshold-ms
                         (when pin-limited? threshold-ms)
                         :pin-blocked-since-ms      blocked-since-ms
                         :pin-blocked-for-ms        blocked-for-ms
                         :pressure                  pressure
                         :required-retained-floor-lsn
                         (:required-retained-floor-lsn state-map)
                         :gc-safety-watermark-lsn
                         (:gc-safety-watermark-lsn state-map)
                         :floor-limiters            floor-limiters})]
        (when (some? check-v)
          (vreset! check-v now-ms))
        (when (some? cached-v)
          (vreset! cached-v report))
        (when (some? blocked-v)
          (vreset! blocked-v blocked-since-ms))
        report))))

(defn- txlog-throw-if-retention-backpressure!
  [lmdb state]
  (when-let [report (txlog-retention-backpressure-state lmdb state)]
    (when (:degraded? report)
      (raise "Txn-log retention pressure is degraded; rejecting writes"
             (assoc report :type :txlog/retention-degraded)))))

(defn- apply-lmdb-after-txlog-append!
  [lmdb state rows]
  (try
    (u/repeat-try-catch
        c/+in-tx-overflow-times+
        l/resized?
      (i/transact-kv lmdb rows))
    (catch Exception e
      (txlog-mark-fatal! state e)
      (throw e))))

(defn- transact-with-txlog!
  [lmdb state dbi-name txs k-type v-type]
  (let [tx-data (canonicalize-input-kvtxs dbi-name txs k-type v-type)]
    (if (pos? (.size ^java.util.List tx-data))
      (if (write-txn-open? lmdb)
        (let [res (i/transact-kv lmdb tx-data)]
          (txlog-add-pending! (i/kv-info lmdb) tx-data)
          res)
        (let [append-res (txlog/append-durable!
                          state tx-data txlog-append-hooks)
              lmdb-base-rows (or (:lmdb-rows append-res) tx-data)
              marker-entry (txlog/next-commit-marker-entry
                            (txlog-commit-state state)
                            append-res)
              rows (if marker-entry
                     (doto (ensure-fast-list lmdb-base-rows)
                       (.add (:row marker-entry)))
                     lmdb-base-rows)]
          (try
            (apply-lmdb-after-txlog-append! lmdb state rows)
            (txlog/commit-finished! state marker-entry)
            (when (:synced? append-res)
              (txlog/maybe-publish-meta-best-effort! state append-res))
            :transacted
            (catch Exception e
              (txlog-mark-fatal! state e)
              (throw e)))))
      (i/transact-kv lmdb dbi-name txs k-type v-type))))

(defn- close-with-txlog!
  [lmdb state]
  (let [info-v (i/kv-info lmdb)]
    (if (write-txn-open? lmdb)
      (let [pending (txlog-pending-rows info-v)]
        (try
          (if (pos? (.size ^java.util.List pending))
            (let [append-res (txlog/append-durable!
                              state pending txlog-append-hooks)
                  marker-entry (txlog/next-commit-marker-entry
                                (txlog-commit-state state)
                                append-res)
                  commit-rows (when marker-entry
                                (doto (FastList. 1)
                                  (.add (:row marker-entry))))]
              (try
                (when commit-rows
                  (apply-lmdb-after-txlog-append! lmdb state commit-rows))
                (let [status (i/close-transact-kv lmdb)]
                  (when (= status :committed)
                    (txlog/commit-finished! state marker-entry)
                    (when (:synced? append-res)
                      (txlog/maybe-publish-meta-best-effort! state append-res)))
                  (when-not (write-txn-open? lmdb)
                    (txlog-reset-pending! info-v))
                  status)
                (catch Exception e
                  (txlog-mark-fatal! state e)
                  (throw e))))
            (let [status (i/close-transact-kv lmdb)]
              (when-not (write-txn-open? lmdb)
                (txlog-reset-pending! info-v))
              status))
          (catch Exception e
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

(defn- kv-info-map-value
  [lmdb k]
  (txlog/parse-floor-provider-map (kv-info-value lmdb k) k))

(defn- write-kv-info-map!
  [lmdb k m]
  (if (seq m)
    (i/transact-kv lmdb c/kv-info [[:put k m]] :keyword :data)
    (i/transact-kv lmdb c/kv-info [[:del k]] :keyword :data))
  m)

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
  (let [res (txlog/replica-floor-update-plan
             replica-id
             applied-lsn
             (System/currentTimeMillis)
             (kv-info-map-value lmdb c/wal-replica-floors)
             c/wal-replica-floors)]
    (write-kv-info-map! lmdb c/wal-replica-floors (:entries res))
    (dissoc res :entries)))

(defn txlog-clear-replica-floor-state!
  [lmdb replica-id]
  (let [res (txlog/replica-floor-clear-plan
             replica-id
             (kv-info-map-value lmdb c/wal-replica-floors)
             c/wal-replica-floors)]
    (write-kv-info-map! lmdb c/wal-replica-floors (:entries res))
    (dissoc res :entries)))

(defn txlog-pin-backup-floor-state!
  [lmdb pin-id floor-lsn expires-ms]
  (let [res (txlog/backup-pin-floor-update-plan
             pin-id
             floor-lsn
             expires-ms
             (System/currentTimeMillis)
             (kv-info-map-value lmdb c/wal-backup-pins)
             c/wal-backup-pins)]
    (write-kv-info-map! lmdb c/wal-backup-pins (:entries res))
    (dissoc res :entries)))

(defn txlog-unpin-backup-floor-state!
  [lmdb pin-id]
  (let [res (txlog/backup-pin-floor-clear-plan
             pin-id
             (kv-info-map-value lmdb c/wal-backup-pins)
             c/wal-backup-pins)]
    (write-kv-info-map! lmdb c/wal-backup-pins (:entries res))
    (dissoc res :entries)))

(defn- txlog-record-payload
  [record]
  (try
    (let [payload (txlog/decode-commit-row-payload ^bytes (:body record))]
      (when-not (map? payload)
        (raise "Malformed txn-log payload" {:record record}))
      payload)
    (catch Exception e
      (raise "Malformed txn-log payload" e {}))))

(defn txlog-record-lsn
  [record]
  (let [payload (txlog-record-payload record)
        lsn (long (or (:lsn payload) 0))]
    (when-not (pos? lsn)
      (raise "Txn-log payload missing valid positive LSN"
             {:type :txlog/corrupt
              :record record}))
    lsn))

(defn- txlog-record-entry
  [segment-id path record]
  (let [payload (txlog-record-payload record)
        lsn     (long (or (:lsn payload) 0))
        tx-time (long (or (:tx-time payload)
                          (:ts payload)
                          0))
        rows    (vec (or (:ops payload) []))
        tx-kind (txlog/classify-record-kind rows)]
    {:lsn lsn
     :tx-kind tx-kind
     :tx-time tx-time
     :rows rows
     :segment-id (long segment-id)
     :offset (long (:offset record))
     :checksum (long (:checksum record))
     :path path}))

(defn- txlog-segment-records
  [segment]
  (let [{:keys [id file]} segment
        segment-id (long id)
        path (.getPath ^java.io.File file)
        acc  (FastList.)]
    (txlog/scan-segment
     path
     {:allow-preallocated-tail? true
      :collect-records? false
      :on-record (fn [record]
                   (.add acc (txlog-record-entry segment-id path record)))})
    (vec acc)))

(defn- txlog-segment-cache-valid?
  [entry path file-bytes modified-ms]
  (and entry
       (= path (:path entry))
       (= file-bytes (long (:file-bytes entry)))
       (= modified-ms (long (:modified-ms entry)))))

(defn- txlog-segment-cache-entry
  [segment]
  (let [records (txlog-segment-records segment)
        {:keys [id file]} segment
        path (.getPath ^java.io.File file)
        file-bytes (long (.length ^java.io.File file))
        modified-ms (long (.lastModified ^java.io.File file))]
    {:segment-id (long id)
     :path path
     :file-bytes file-bytes
     :modified-ms modified-ms
     :min-lsn (some-> records first :lsn long)
     :records records}))

(defn- txlog-segment-records-entry
  [segment cache-v]
  (if cache-v
    (let [{:keys [id file]} segment
          segment-id (long id)
          path (.getPath ^java.io.File file)
          file-bytes (long (.length ^java.io.File file))
          modified-ms (long (.lastModified ^java.io.File file))
          cache0 (or @cache-v {})
          cached (get cache0 segment-id)]
      (if (txlog-segment-cache-valid? cached path file-bytes modified-ms)
        cached
        (let [entry (txlog-segment-cache-entry segment)]
          (vreset! cache-v (assoc cache0 segment-id entry))
          entry)))
    (txlog-segment-cache-entry segment)))

(defn- prune-txlog-records-cache!
  [cache-v segments]
  (when cache-v
    (let [cache0 (or @cache-v {})]
      (when (seq cache0)
        (let [segment-ids (into #{} (map (comp long :id)) segments)
              cache1 (reduce-kv
                      (fn [acc sid entry]
                        (if (contains? segment-ids sid)
                          (assoc acc sid entry)
                          acc))
                      {}
                      cache0)]
          (when (not= cache0 cache1)
            (vreset! cache-v cache1)))))))

(defn txlog-records
  ([state]
   (txlog-records state nil))
  ([state from-lsn]
  (let [dir        (:dir state)
         segments   (vec (txlog/segment-files dir))
         cache-v    (:txlog-records-cache state)
         _          (prune-txlog-records-cache! cache-v segments)
         from       (long (max 0 (long (or from-lsn 0))))
         tail-scan? (pos? from)
         records    (if tail-scan?
                      (loop [remaining  (seq (rseq segments))
                             collected  '()]
                        (if-let [segment (first remaining)]
                          (let [{:keys [records min-lsn]}
                                (txlog-segment-records-entry segment cache-v)
                                collected'      (if (seq records)
                                                  (cons records collected)
                                                  collected)]
                            (if (and (some? min-lsn)
                                     (<= (long min-lsn) from))
                              (->> collected' (mapcat identity) vec)
                              (recur (next remaining) collected')))
                          (->> collected (mapcat identity) vec)))
                      (->> segments
                           (mapcat #(-> (txlog-segment-records-entry
                                         %
                                         cache-v)
                                        :records))
                           vec))]
    (loop [prev nil
           [record & more] records]
      (when record
        (let [lsn (:lsn record)]
          (when (or (not (pos? ^long lsn))
                    (and prev (not= lsn (inc ^long prev))))
            (raise "Txn-log sequence is invalid"
                   {:type :txlog/corrupt
                    :previous-lsn prev
                    :lsn lsn
                    :record record}))
          (recur lsn more))))
    records)))

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
    (let [state (txlog/enabled-state raw-lmdb)
          watermarks (txlog-watermarks-map raw-lmdb state)
          applied-lsn (long (or (:last-applied-lsn watermarks) 0))
          snapshot-pin-floor-state (txlog-snapshot-floor-state
                                    raw-lmdb
                                    (or (i/env-opts raw-lmdb) {})
                                    applied-lsn)
          pin-floor-lsn (long (:floor-lsn snapshot-pin-floor-state))
          compact? (boolean compact?)
          started-ms (System/currentTimeMillis)
          pin-id (str "backup-copy/" started-ms "-"
                      (java.util.UUID/randomUUID))
          pin-ttl-ms (long (max 60000
                                (long (snapshot-max-age-ms raw-lmdb))))
          pin-expires-ms (long (+ (long started-ms) pin-ttl-ms))]
      (txlog-pin-backup-floor-state! raw-lmdb
                                     pin-id
                                     pin-floor-lsn
                                     pin-expires-ms)
      (try
        (let [context {:lmdb lmdb
                       :dest dest
                       :compact? compact?
                       :pin-id pin-id
                       :pin-floor-lsn pin-floor-lsn
                       :pin-expires-ms pin-expires-ms
                       :applied-lsn applied-lsn}]
          (maybe-notify-txn-log-copy-backup-pin-observer! context)
          (maybe-run-txn-log-copy-backup-pin-failpoint! context)
          (i/copy raw-lmdb dest compact?)
          (let [completed-ms (System/currentTimeMillis)]
            {:started-ms started-ms
             :completed-ms completed-ms
             :duration-ms (max 0 (- completed-ms started-ms))
             :compact? compact?
             :backup-pin {:pin-id pin-id
                          :floor-lsn pin-floor-lsn
                          :expires-ms pin-expires-ms}}))
        (finally
          (try
            (txlog-unpin-backup-floor-state! raw-lmdb pin-id)
            (catch Exception e
              (when-let [info-v (i/kv-info raw-lmdb)]
                (vswap! info-v assoc :copy-last-backup-unpin-error
                        (.getMessage e))))))))
    (i/copy raw-lmdb dest compact?)))

(declare wrap-lmdb)

(deftype KVLMDB [db]
  l/IWriting
  (writing? [_] (l/writing? db))
  (write-txn [_] (l/write-txn db))
  (mark-write [_] (wrap-lmdb (l/mark-write db)))
  (reset-write [_] (wrap-lmdb (l/reset-write db)))

  i/IList
  (del-list-items [this a0 a1 a2] (i/del-list-items db a0 a1 a2))
  (del-list-items [this a0 a1 a2 a3 a4] (i/del-list-items db a0 a1 a2 a3 a4))
  (get-list [this a0 a1 a2 a3] (i/get-list db a0 a1 a2 a3))
  (in-list? [this a0 a1 a2 a3 a4] (i/in-list? db a0 a1 a2 a3 a4))
  (list-count [this a0 a1 a2] (i/list-count db a0 a1 a2))
  (list-dbi? [this a0] (i/list-dbi? db a0))
  (list-range [this a0 a1 a2 a3 a4] (i/list-range db a0 a1 a2 a3 a4))
  (list-range-count [this a0 a1 a2 a3 a4] (i/list-range-count db a0 a1 a2 a3 a4))
  (list-range-filter [this a0 a1 a2 a3 a4 a5] (i/list-range-filter db a0 a1 a2 a3 a4 a5))
  (list-range-filter [this a0 a1 a2 a3 a4 a5 a6] (i/list-range-filter db a0 a1 a2 a3 a4 a5 a6))
  (list-range-filter-count [this a0 a1 a2 a3 a4 a5] (i/list-range-filter-count db a0 a1 a2 a3 a4 a5))
  (list-range-filter-count [this a0 a1 a2 a3 a4 a5 a6] (i/list-range-filter-count db a0 a1 a2 a3 a4 a5 a6))
  (list-range-first [this a0 a1 a2 a3 a4] (i/list-range-first db a0 a1 a2 a3 a4))
  (list-range-first-n [this a0 a1 a2 a3 a4 a5] (i/list-range-first-n db a0 a1 a2 a3 a4 a5))
  (list-range-keep [this a0 a1 a2 a3 a4 a5] (i/list-range-keep db a0 a1 a2 a3 a4 a5))
  (list-range-keep [this a0 a1 a2 a3 a4 a5 a6] (i/list-range-keep db a0 a1 a2 a3 a4 a5 a6))
  (list-range-some [this a0 a1 a2 a3 a4 a5] (i/list-range-some db a0 a1 a2 a3 a4 a5))
  (list-range-some [this a0 a1 a2 a3 a4 a5 a6] (i/list-range-some db a0 a1 a2 a3 a4 a5 a6))
  (near-list [this a0 a1 a2 a3 a4] (i/near-list db a0 a1 a2 a3 a4))
  (put-list-items [this a0 a1 a2 a3 a4] (i/put-list-items db a0 a1 a2 a3 a4))
  (visit-list [this a0 a1 a2 a3] (i/visit-list db a0 a1 a2 a3))
  (visit-list [this a0 a1 a2 a3 a4] (i/visit-list db a0 a1 a2 a3 a4))
  (visit-list [this a0 a1 a2 a3 a4 a5] (i/visit-list db a0 a1 a2 a3 a4 a5))
  (visit-list-key-range [this a0 a1 a2 a3 a4] (i/visit-list-key-range db a0 a1 a2 a3 a4))
  (visit-list-key-range [this a0 a1 a2 a3 a4 a5] (i/visit-list-key-range db a0 a1 a2 a3 a4 a5))
  (visit-list-range [this a0 a1 a2 a3 a4 a5] (i/visit-list-range db a0 a1 a2 a3 a4 a5))
  (visit-list-range [this a0 a1 a2 a3 a4 a5 a6] (i/visit-list-range db a0 a1 a2 a3 a4 a5 a6))
  (visit-list-sample [this a0 a1 a2 a3 a4 a5] (i/visit-list-sample db a0 a1 a2 a3 a4 a5))
  (visit-list-sample [this a0 a1 a2 a3 a4 a5 a6] (i/visit-list-sample db a0 a1 a2 a3 a4 a5 a6))

  i/IAdmin
  (re-index [this a0] (i/re-index db a0))
  (re-index [this a0 a1] (i/re-index db a0 a1))

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
      (txlog/select-open-records
        (txlog-records state from-lsn)
        from-lsn
        upto-lsn)
      (if (txlog-config-enabled? db)
        []
        (txlog/select-open-records
          (txlog-records (txlog/enabled-state db) from-lsn)
          from-lsn
          upto-lsn))))

  (force-txlog-sync! [_]
    (cond
      (not (txlog-config-enabled? db))
      (txlog/enabled-state db)

      (not (txlog-write-path-enabled? db))
      (let [rollout-mode (txlog-rollout-mode db)]
        {:synced?    false
         :skipped?   true
         :reason     :rollback
         :watermarks (txlog-rollout-watermarks db rollout-mode)})

      :else
      (let [state (txlog/enabled-state db)]
        (assoc (txlog-force-sync! state)
               :watermarks (txlog-watermarks-map db state)))))

  (force-lmdb-sync! [_]
    (if (txlog-config-enabled? db)
      (do
        (force-lmdb-sync-now! db)
        {:synced?    true
         :watermarks (if-let [state (txlog/state db)]
                       (txlog-watermarks-map db state)
                       (txlog-rollout-watermarks db
                                                 (txlog-rollout-mode db)))})
      (txlog/enabled-state db)))

  (create-snapshot! [_]
    (if (txlog-write-path-enabled? db)
      (create-snapshot-now! db)
      (if (txlog-config-enabled? db)
        (let [rollout-mode (txlog-rollout-mode db)]
          {:ok?        false
           :skipped?   true
           :reason     :rollback
           :watermarks (txlog-rollout-watermarks db rollout-mode)})
        (txlog/enabled-state db))))

  (list-snapshots [_]
    (list-snapshot-entries db))

  (snapshot-scheduler-state [_]
    (snapshot-scheduler-state-map db))

  (read-commit-marker [_]
    (if-let [state (txlog/state db)]
      (assoc (read-commit-marker-state db)
             :commit-marker? (boolean (:commit-marker? state))
             :marker-revision (long @(:marker-revision state)))
      {:commit-marker? false
       :slot-a         nil
       :slot-b         nil
       :current        nil}))

  (verify-commit-marker! [_]
    (if-let [state (txlog/state db)]
      (verify-commit-marker-state db state)
      (if (txlog-config-enabled? db)
        (let [rollout-mode (txlog-rollout-mode db)]
          {:ok?        false
           :skipped?   true
           :reason     :rollback
           :watermarks (txlog-rollout-watermarks db rollout-mode)})
        (verify-commit-marker-state db (txlog/enabled-state db)))))

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
    (let [wdb (i/open-transact-kv db)]
      (when (ensure-txlog-ready! db)
        (txlog-reset-pending! (i/kv-info db)))
      (wrap-lmdb wdb)))
  (abort-transact-kv [_]
    (when (txlog-config-enabled? db)
      (txlog-reset-pending! (i/kv-info db)))
    (i/abort-transact-kv db))
  (check-ready [this] (i/check-ready db))
  (clear-dbi [this a0] (i/clear-dbi db a0))
  (close-kv [_]
    (try
      (i/close-kv db)
      (finally
        (close-txlog-state! db))))
  (close-transact-kv [_]
    (if-let [state (txlog-runtime-state db)]
      (close-with-txlog! db state)
      (i/close-transact-kv db)))
  (closed-kv? [this] (i/closed-kv? db))
  (copy [this a0] (.copy this a0 false))
  (copy [this a0 a1] (txlog-copy-with-backup-pin! this db a0 a1))
  (dbi-opts [this a0] (i/dbi-opts db a0))
  (drop-dbi [this a0] (i/drop-dbi db a0))
  (entries [this a0] (i/entries db a0))
  (env-dir [this] (i/env-dir db))
  (kv-info [this] (i/kv-info db))
  (env-opts [this] (i/env-opts db))
  (get-by-rank [this a0 a1] (i/get-by-rank db a0 a1))
  (get-by-rank [this a0 a1 a2] (i/get-by-rank db a0 a1 a2))
  (get-by-rank [this a0 a1 a2 a3] (i/get-by-rank db a0 a1 a2 a3))
  (get-by-rank [this a0 a1 a2 a3 a4] (i/get-by-rank db a0 a1 a2 a3 a4))
  (get-dbi [this a0] (i/get-dbi db a0))
  (get-dbi [this a0 a1] (i/get-dbi db a0 a1))
  (get-env-flags [this] (i/get-env-flags db))
  (get-first [this a0 a1] (i/get-first db a0 a1))
  (get-first [this a0 a1 a2] (i/get-first db a0 a1 a2))
  (get-first [this a0 a1 a2 a3] (i/get-first db a0 a1 a2 a3))
  (get-first [this a0 a1 a2 a3 a4] (i/get-first db a0 a1 a2 a3 a4))
  (get-first-n [this a0 a1 a2] (i/get-first-n db a0 a1 a2))
  (get-first-n [this a0 a1 a2 a3] (i/get-first-n db a0 a1 a2 a3))
  (get-first-n [this a0 a1 a2 a3 a4] (i/get-first-n db a0 a1 a2 a3 a4))
  (get-first-n [this a0 a1 a2 a3 a4 a5] (i/get-first-n db a0 a1 a2 a3 a4 a5))
  (get-range [this a0 a1] (i/get-range db a0 a1))
  (get-range [this a0 a1 a2] (i/get-range db a0 a1 a2))
  (get-range [this a0 a1 a2 a3] (i/get-range db a0 a1 a2 a3))
  (get-range [this a0 a1 a2 a3 a4] (i/get-range db a0 a1 a2 a3 a4))
  (get-rank [this a0 a1] (i/get-rank db a0 a1))
  (get-rank [this a0 a1 a2] (i/get-rank db a0 a1 a2))
  (get-rtx [this] (i/get-rtx db))
  (get-some [this a0 a1 a2] (i/get-some db a0 a1 a2))
  (get-some [this a0 a1 a2 a3] (i/get-some db a0 a1 a2 a3))
  (get-some [this a0 a1 a2 a3 a4] (i/get-some db a0 a1 a2 a3 a4))
  (get-some [this a0 a1 a2 a3 a4 a5] (i/get-some db a0 a1 a2 a3 a4 a5))
  (get-some [this a0 a1 a2 a3 a4 a5 a6] (i/get-some db a0 a1 a2 a3 a4 a5 a6))
  (get-value [this a0 a1] (i/get-value db a0 a1))
  (get-value [this a0 a1 a2] (i/get-value db a0 a1 a2))
  (get-value [this a0 a1 a2 a3] (i/get-value db a0 a1 a2 a3))
  (get-value [this a0 a1 a2 a3 a4] (i/get-value db a0 a1 a2 a3 a4))
  (key-compressor [this] (i/key-compressor db))
  (key-range [this a0 a1] (i/key-range db a0 a1))
  (key-range [this a0 a1 a2] (i/key-range db a0 a1 a2))
  (key-range-count [this a0 a1] (i/key-range-count db a0 a1))
  (key-range-count [this a0 a1 a2] (i/key-range-count db a0 a1 a2))
  (key-range-list-count [this a0 a1 a2] (i/key-range-list-count db a0 a1 a2))
  (list-dbis [this] (i/list-dbis db))
  (max-val-size [this] (i/max-val-size db))
  (open-dbi [this a0] (i/open-dbi db a0))
  (open-dbi [this a0 a1] (i/open-dbi db a0 a1))
  (open-list-dbi [this a0] (i/open-list-dbi db a0))
  (open-list-dbi [this a0 a1] (i/open-list-dbi db a0 a1))
  (range-count [this a0 a1] (i/range-count db a0 a1))
  (range-count [this a0 a1 a2] (i/range-count db a0 a1 a2))
  (range-filter [this a0 a1 a2] (i/range-filter db a0 a1 a2))
  (range-filter [this a0 a1 a2 a3] (i/range-filter db a0 a1 a2 a3))
  (range-filter [this a0 a1 a2 a3 a4] (i/range-filter db a0 a1 a2 a3 a4))
  (range-filter [this a0 a1 a2 a3 a4 a5] (i/range-filter db a0 a1 a2 a3 a4 a5))
  (range-filter [this a0 a1 a2 a3 a4 a5 a6] (i/range-filter db a0 a1 a2 a3 a4 a5 a6))
  (range-filter-count [this a0 a1 a2] (i/range-filter-count db a0 a1 a2))
  (range-filter-count [this a0 a1 a2 a3] (i/range-filter-count db a0 a1 a2 a3))
  (range-filter-count [this a0 a1 a2 a3 a4] (i/range-filter-count db a0 a1 a2 a3 a4))
  (range-filter-count [this a0 a1 a2 a3 a4 a5] (i/range-filter-count db a0 a1 a2 a3 a4 a5))
  (range-keep [this a0 a1 a2] (i/range-keep db a0 a1 a2))
  (range-keep [this a0 a1 a2 a3] (i/range-keep db a0 a1 a2 a3))
  (range-keep [this a0 a1 a2 a3 a4] (i/range-keep db a0 a1 a2 a3 a4))
  (range-keep [this a0 a1 a2 a3 a4 a5] (i/range-keep db a0 a1 a2 a3 a4 a5))
  (range-seq [this a0 a1] (i/range-seq db a0 a1))
  (range-seq [this a0 a1 a2] (i/range-seq db a0 a1 a2))
  (range-seq [this a0 a1 a2 a3] (i/range-seq db a0 a1 a2 a3))
  (range-seq [this a0 a1 a2 a3 a4] (i/range-seq db a0 a1 a2 a3 a4))
  (range-seq [this a0 a1 a2 a3 a4 a5] (i/range-seq db a0 a1 a2 a3 a4 a5))
  (range-some [this a0 a1 a2] (i/range-some db a0 a1 a2))
  (range-some [this a0 a1 a2 a3] (i/range-some db a0 a1 a2 a3))
  (range-some [this a0 a1 a2 a3 a4] (i/range-some db a0 a1 a2 a3 a4))
  (range-some [this a0 a1 a2 a3 a4 a5] (i/range-some db a0 a1 a2 a3 a4 a5))
  (return-rtx [this a0] (i/return-rtx db a0))
  (sample-kv [this a0 a1] (i/sample-kv db a0 a1))
  (sample-kv [this a0 a1 a2] (i/sample-kv db a0 a1 a2))
  (sample-kv [this a0 a1 a2 a3] (i/sample-kv db a0 a1 a2 a3))
  (sample-kv [this a0 a1 a2 a3 a4] (i/sample-kv db a0 a1 a2 a3 a4))
  (set-env-flags [this a0 a1] (i/set-env-flags db a0 a1))
  (set-key-compressor [this a0] (i/set-key-compressor db a0))
  (set-max-val-size [this a0] (i/set-max-val-size db a0))
  (set-val-compressor [this a0] (i/set-val-compressor db a0))
  (stat [this] (i/stat db))
  (stat [this a0] (i/stat db a0))
  (sync [this] (i/sync db))
  (sync [this a0] (i/sync db a0))
  (transact-kv [this a0] (.transact-kv this nil a0))
  (transact-kv [this a0 a1] (.transact-kv this a0 a1 :data :data))
  (transact-kv [this a0 a1 a2] (.transact-kv this a0 a1 a2 :data))
  (transact-kv [_ a0 a1 a2 a3]
    (if-let [state (txlog-runtime-state db)]
      (transact-with-txlog! db state a0 a1 a2 a3)
      (i/transact-kv db a0 a1 a2 a3)))
  (val-compressor [this] (i/val-compressor db))
  (visit [this a0 a1 a2] (i/visit db a0 a1 a2))
  (visit [this a0 a1 a2 a3] (i/visit db a0 a1 a2 a3))
  (visit [this a0 a1 a2 a3 a4] (i/visit db a0 a1 a2 a3 a4))
  (visit [this a0 a1 a2 a3 a4 a5] (i/visit db a0 a1 a2 a3 a4 a5))
  (visit-key-range [this a0 a1 a2] (i/visit-key-range db a0 a1 a2))
  (visit-key-range [this a0 a1 a2 a3] (i/visit-key-range db a0 a1 a2 a3))
  (visit-key-range [this a0 a1 a2 a3 a4] (i/visit-key-range db a0 a1 a2 a3 a4))
  (visit-key-sample [this a0 a1 a2 a3 a4] (i/visit-key-sample db a0 a1 a2 a3 a4))
  (visit-key-sample [this a0 a1 a2 a3 a4 a5] (i/visit-key-sample db a0 a1 a2 a3 a4 a5)))

(defn wrap-lmdb
  [db]
  (if (instance? KVLMDB db)
    db
    (let [fallback-attempted?
          (true? (:txlog-recovery-fallback-attempted?
                  (i/env-opts db)))]
      (try
        (ensure-txlog-ready! db)
        (->KVLMDB db)
        (catch Exception e
          (if fallback-attempted?
            (do
              (close-txlog-state! db)
              (try
                (i/close-kv db)
                (catch Exception _))
              (throw e))
            (if-let [recovered (recover-from-snapshot-open! db e)]
              recovered
              (throw e))))))))

(l/set-open-kv-wrapper! wrap-lmdb)
