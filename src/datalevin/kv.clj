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
   [clojure.java.io :as io]
   [datalevin.constants :as c]
   [datalevin.custom-kv :as custom-kv]
   [datalevin.interface :as i]
   [datalevin.kv.snapshot :refer [ensure-snapshot-scheduler-runtime!
                                  in-offpeak-window? list-snapshot-entries
                                  snapshot-contention-thresholds
                                  snapshot-defer-backoff-max-ms
                                  snapshot-defer-backoff-min-ms
                                  snapshot-defer-on-contention?
                                  snapshot-interval-ms snapshot-max-age-ms
                                  snapshot-max-log-bytes-delta
                                  snapshot-max-lsn-delta
                                  snapshot-offpeak-windows snapshot-root-dir
                                  snapshot-scheduler-contention-state
                                  snapshot-scheduler-enabled?
                                  snapshot-scheduler-poll-ms
                                  snapshot-scheduler-running?]]
   [datalevin.kv.retention :refer [delete-txlog-segment!
                                   txlog-log-dbi-drop!
                                   txlog-log-dbi-registration!
                                   txlog-retention-state-map]]
   [datalevin.kv.txlog :as kvtx :refer [*wal-copy-backup-pin-enabled?*
                                        close-failed-open!
                                        close-txlog-state!
                                        close-with-txlog!
                                        read-commit-marker-state
                                        refresh-runtime-marker-revision!
                                        transact-with-txlog!
                                        txlog-reset-pending!
                                        txlog-runtime-state
                                        verify-commit-marker-state
                                        create-snapshot-now!
                                        force-lmdb-sync-now!
                                        maybe-notify-txn-log-copy-backup-pin-observer!
                                        maybe-run-txn-log-copy-backup-pin-failpoint!
                                        rdonly-env? recover-from-snapshot-open!
                                        snapshot-source-ready?
                                        txlog-clear-replica-floor-state!
                                        txlog-clear-snapshot-floor-state!
                                        txlog-config-enabled? txlog-force-sync!
                                        txlog-pin-backup-floor-state!
                                        txlog-rollout-mode
                                        txlog-rollout-watermarks
                                        txlog-record-lsn txlog-records
                                        txlog-snapshot-floor-state
                                        txlog-unpin-backup-floor-state!
                                        txlog-update-replica-floor-state!
                                        txlog-update-snapshot-floor-state!
                                        txlog-watermarks-map
                                        txlog-write-path-enabled?
                                        with-runtime-txlog-state-guard
                                        with-write-txn-lock-before-runtime-txlog-state
                                        write-txn-open?]]
   [datalevin.lmdb :as l]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u :refer [deftype+ raise]])
  )

(declare snapshot-current-lsn ->KVLMDB)

(declare raw-lmdb)

(def ensure-txlog-ready! kvtx/ensure-txlog-ready!)

(def transact-kv-without-txlog! kvtx/transact-kv-without-txlog!)

(def mirror-replayed-txlog-record! kvtx/mirror-replayed-txlog-record!)

(def replay-txlog-rows! kvtx/replay-txlog-rows!)



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

(defn txlog-watermarks
  [db]
  (kvtx/txlog-watermarks db))

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

(kvtx/set-snapshot-scheduler-hooks! start-snapshot-scheduler!
                                     stop-snapshot-scheduler!)
