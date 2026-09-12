;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.kv.snapshot
  "Snapshot file layout, metadata, and scheduler configuration."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [datalevin.interface :as i]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u]))

(def snapshot-meta-file-name "snapshot.edn")
(def snapshot-current-slot "current")
(def snapshot-previous-slot "previous")
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

(defn snapshot-root-dir
  [lmdb]
  (let [opts (i/env-opts lmdb)]
    (or (:snapshot-dir opts)
        (str (i/env-dir lmdb) u/+separator+ "snapshots"))))

(defn snapshot-slot-path
  [root-dir slot-name]
  (str root-dir u/+separator+ slot-name))

(defn- snapshot-meta-path
  [slot-path]
  (str slot-path u/+separator+ snapshot-meta-file-name))

(defn snapshot-compact?
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

(defn write-snapshot-meta!
  [slot-path meta]
  (spit (snapshot-meta-path slot-path) (str (pr-str meta) "\n")))

(defn move-dir!
  [src dst]
  (java.nio.file.Files/move
   (.toPath (io/file src))
   (.toPath (io/file dst))
   (into-array java.nio.file.CopyOption
               [java.nio.file.StandardCopyOption/REPLACE_EXISTING])))

(defn copy-dir-contents!
  [src-dir dest-dir]
  (u/create-dirs dest-dir)
  (doseq [^java.io.File f (or (u/list-files src-dir) [])]
    (let [dst (str dest-dir u/+separator+ (.getName f))]
      (if (.isDirectory f)
        (copy-dir-contents! (.getPath f) dst)
        (u/copy-file (.getPath f) dst)))))

(defn update-snapshot-slot-meta!
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

(defn list-snapshot-entries
  [lmdb]
  (let [root-dir (snapshot-root-dir lmdb)
        current-path (snapshot-slot-path root-dir snapshot-current-slot)
        previous-path (snapshot-slot-path root-dir snapshot-previous-slot)]
    (->> [(snapshot-entry :current current-path)
          (snapshot-entry :previous previous-path)]
         (remove nil?)
         vec)))

(defn snapshot-scheduler-enabled?
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (if (contains? opts :snapshot-scheduler?)
      (boolean (:snapshot-scheduler? opts))
      false)))

(defn snapshot-interval-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (or (:snapshot-interval-ms opts)
              snapshot-default-interval-ms))))

(defn snapshot-max-lsn-delta
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (or (:snapshot-max-lsn-delta opts)
              snapshot-default-max-lsn-delta))))

(defn snapshot-max-log-bytes-delta
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (or (:snapshot-max-log-bytes-delta opts)
              snapshot-default-max-log-bytes-delta))))

(defn snapshot-max-age-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (or (:snapshot-max-age-ms opts)
              snapshot-default-max-age-ms))))

(defn snapshot-defer-on-contention?
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (if (contains? opts :snapshot-defer-on-contention?)
      (boolean (:snapshot-defer-on-contention? opts))
      snapshot-default-defer-on-contention?)))

(defn snapshot-contention-thresholds
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

(defn snapshot-defer-backoff-min-ms
  [lmdb]
  (let [opts (or (i/env-opts lmdb) {})]
    (long (max 0 (long (or (:snapshot-defer-backoff-min-ms opts)
                           snapshot-default-defer-backoff-min-ms))))))

(defn snapshot-defer-backoff-max-ms
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

(defn snapshot-offpeak-windows
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

(defn in-offpeak-window?
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

(defn snapshot-scheduler-contention-state
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

(defn snapshot-scheduler-poll-ms
  [lmdb]
  (let [interval-ms (long (snapshot-interval-ms lmdb))
        quarter-ms (if (pos? interval-ms)
                     (quot interval-ms 4)
                     1000)]
    (long (max 50 (min 60000 quarter-ms)))))

(defn ensure-snapshot-scheduler-runtime!
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

(defn snapshot-scheduler-running?
  [future-cell]
  (when-let [future (some-> future-cell deref)]
    (and (not (.isCancelled ^java.util.concurrent.Future future))
         (not (.isDone ^java.util.concurrent.Future future)))))
