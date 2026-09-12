;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.kv.scheduler
  "Snapshot scheduler runtime: trigger detection, deferral, and bookkeeping."
  (:require
   [clojure.java.io :as io]
   [datalevin.interface :as i]
   [datalevin.kv.retention :refer [txlog-retention-state-map]]
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
   [datalevin.kv.txlog :as kvtx :refer [create-snapshot-now!
                                        rdonly-env?
                                        snapshot-source-ready?
                                        txlog-record-lsn
                                        txlog-watermarks-map]]
   [datalevin.lmdb :as l]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u]))

(declare snapshot-current-lsn)

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

(defn snapshot-scheduler-state-map
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

(defn maybe-run-snapshot-scheduler!
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

(defn start-snapshot-scheduler!
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

(defn stop-snapshot-scheduler!
  [info-v]
  (when-let [future-cell (:snapshot-scheduler-future @info-v)]
    (when-let [future @future-cell]
      (.cancel ^java.util.concurrent.Future future true))
    (vreset! future-cell nil)))
