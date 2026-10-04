;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.private
  "Private M0 opener. The lease precedes recovery/native/WAL open and all private
  handles share the collector. Public APIs remain on compatibility."
  (:require [clojure.java.io :as io]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.kv.snapshot :as snapshot]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.factory :as factory]
            [datalevin.tx-group.batch.recovery :as recovery]
            [datalevin.tx-state.lifetime :as lifetime]
            [datalevin.txlog :as wal])
  (:import [datalevin.lmdb KVTxData]
           [java.io Closeable]
           [java.util.concurrent Executors ScheduledExecutorService ThreadFactory TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent.locks ReentrantLock]))

(defn- close-wal! [state]
  (doseq [key [:segment-channel :sync-lock-channel]]
    (when-let [^Closeable channel (some-> (get state key) deref)]
      (.close channel))))

(defn- rows-cost
  "Conservative encoded-size estimate for rows about to become one WAL body.

  Charged before the encoder runs, so a request that cannot fit is rejected
  before allocating the encoding."
  ^long [rows]
  (reduce (fn [^long total row]
            (let [record? (instance? KVTxData row)
                  op (if record? (.-op ^KVTxData row) (nth row 0))
                  name (if record? (.-dbi-name ^KVTxData row) (nth row 1))
                  key (if record? (.-k ^KVTxData row) (nth row 2))
                  value (if record? (.-v ^KVTxData row) (nth row 3 nil))]
              (+ total (long charge/encoded-row-descriptor)
               ;; UTF-8 takes at most three bytes per UTF-16 code unit. Bound
               ;; it without allocating a temporary name encoding first.
               (* 3 (long (.length ^String name)))
               (alength ^bytes key)
               (if (= :del op)
                 0
                 (alength ^bytes value)))))
          charge/buffer-wrapper
          rows))

(defn- check-dbi! [declared name]
  (when-not (contains? declared name)
    (throw (ex-info "DBI is outside the fixed private M0 catalog"
                    {:error :txlog/unsupported-private-operation
                     :outcome :not-committed :dbi name :retryable? false}))))

(defn- check-single-value! [declared name]
  (check-dbi! declared name)
  (when (some #{:dupsort} (:flags (get declared name)))
    (throw (ex-info "Private native RMW requires a single-value DBI"
                    {:error :txlog/unsupported-private-operation
                     :outcome :not-committed :dbi name :retryable? false}))))

(defn- check-physical! [declared name op k v]
  (check-dbi! declared name)
  (let [dbi (get declared name)
        duplicates? (some #{:dupsort} (:flags dbi))]
    (when-not (and (bytes? k) (<= 1 (alength ^bytes k)
                                  (long (get dbi :key-size c/+max-key-size+)))
                   (or (= op :del)
                       (and (bytes? v)
                            (or (not duplicates?)
                                (<= 1 (alength ^bytes v) c/+max-key-size+)))))
      (throw (ex-info "Invalid physical private KV row"
                      {:error :kv/invalid-encoded-size :outcome :not-committed
                       :dbi name :retryable? false})))))

(defn- check-catalog! [declared]
  (when-not (and (map? declared)
                 (every? (fn [[name opts]]
                           (and (string? name) (map? opts)
                                (every? #{:create :dupsort :counted :prefix-compression}
                                        (:flags opts))))
                         declared))
    (throw (ex-info "Unsupported private M0 catalog/comparator"
                    {:error :txlog/unsupported-private-operation :retryable? false}))))

(defn- rmw-adapters
  "Write capture and WAL encoding for the native batch transaction."
  [declared]
  {:check-row! (fn [name op key value]
                 (check-single-value! declared name)
                 (check-physical! declared name op key value))
   :body-cost rows-cost
   :encode-body (fn [rows hooks] (wal/prepare-append-body rows hooks))})

(defn- check-prepared!
  "The private M0 catalog/operation boundary. Physical raw puts and key deletes
  are replayable from a conservative snapshot floor. Flags with position or
  conditional semantics and catalog changes remain compatibility-only."
  [declared descriptor]
    (let [rows ^java.util.List (:rows (batch/data descriptor))]
      (when rows
        (dotimes [row-idx (.size rows)]
          (let [row (.get rows row-idx)
                record? (instance? KVTxData row)
                op (if record? (.-op ^KVTxData row) (nth row 0))
                name (if record? (.-dbi-name ^KVTxData row) (nth row 1))
                k (if record? (.-k ^KVTxData row) (nth row 2))
                v (when (= op :put)
                    (if record? (.-v ^KVTxData row) (nth row 3)))
                kt (if record? (.-kt ^KVTxData row)
                       (nth row (if (= :put op) 4 3) nil))
                vt (when (= :put op)
                     (if record? (.-vt ^KVTxData row) (nth row 5 nil)))
                flags (if record? (.-flags ^KVTxData row)
                          (nth row (if (= :put op) 6 4) nil))]
            (when-not (and (contains? declared name) (#{:put :del} op)
                           (or record? (= :put op))
                           (= :raw kt) (or (= :del op) (= :raw vt))
                           (empty? flags))
              (throw
               (ex-info "Operation is outside private M0 coverage"
                        {:error :txlog/unsupported-private-operation
                         :outcome :not-committed :dbi name :operation op
                         :retryable? false})))
            (check-physical! declared name op k v))))))

(defn- snapshot-due? [raw state collector manifest]
  (let [age (- (System/currentTimeMillis) (long (:created-ms manifest)))
        urgent? (or @(:retention-backpressure-state state)
                    (>= age (snapshot/snapshot-max-age-ms raw)))
        due? (or urgent?
                 (>= age (snapshot/snapshot-interval-ms raw))
                 (>= (- (batch/published-lsn collector) (long (:floor-lsn manifest)))
                     (snapshot/snapshot-max-lsn-delta raw))
                 (>= (- (long @(:retention-total-bytes state))
                        (long (or (:retained-byte-floor manifest) 0)))
                     (snapshot/snapshot-max-log-bytes-delta raw)))
        sync (when due? (wal/sync-manager-state (:sync-manager state)))
        thresholds (when due? (snapshot/snapshot-contention-thresholds raw))
        contended? (and due?
                        (or (> (long (or (:last-commit-wait-ms sync) 0))
                               (:commit-wait-p99-ms thresholds))
                            (> (long (or (:last-fsync-ms sync) 0))
                               (:fsync-p99-ms thresholds))))]
    (and due?
         (or urgent?
             (and (snapshot/in-offpeak-window? (snapshot/snapshot-offpeak-windows raw)
                                                (System/currentTimeMillis))
                  (or (not (snapshot/snapshot-defer-on-contention? raw))
                      (not contended?)))))))

(defn open!
  "Open/attach private KV. WAL mode restores a verified snapshot plus contiguous
  WAL on every start. Native-only mode retains ordinary native reopen semantics.
  :dbis declares the fixed private catalog (default {\"data\" {}}); scheduled
  snapshots default on for WAL, with a mandatory C=0 baseline even when disabled."
  [{:keys [dir] :as requested}]
  (let [opts (assoc requested :wal? (get requested :wal? true))
        wal? (:wal? opts)]
    (check-catalog! (recovery/dbis opts))
    (when wal? (recovery/validate-limits! opts))
    (when (and wal? (:wal-shared? opts))
      (throw (ex-info "Private M0 does not support shared WAL"
                      {:error :txlog/write-protocol-mismatch})))
    (env/open-batch!
     (assoc opts :open-runtime!
            (fn []
              (when (and (not wal?)
                         (or (.exists (io/file (recovery/root opts)))
                             (.exists (io/file (recovery/wal-dir opts)))))
                (throw (ex-info "Cannot open a WAL overlay as native-only"
                                {:error :txlog/write-protocol-mismatch})))
              (let [restored (when wal? (recovery/restore! opts))
                    db (l/open-kv dir (recovery/native-options opts))
                  raw (kv/raw-lmdb db)
                    native-lifetime (lifetime/create)
                    _ (vswap! (i/kv-info raw) assoc :native-lifetime native-lifetime)]
                (try
                  (doseq [[name dbi-opts] (recovery/dbis opts)]
                    (i/open-dbi raw name dbi-opts))
                  (let [state (when wal?
                                (:state (wal/init-runtime-state
                                         (assoc opts :wal-shared? false
                                                :wal-full-prefix? true
                                                :wal-recovery-floor (long (or (:last-lsn restored) 0)))
                                         nil)))]
                    (try
                      (let [collector (volatile! nil)
                            snapshot-lock (ReentrantLock.)
                            stopped (AtomicBoolean. false)
                            closing-force? (AtomicBoolean. false)
                            scheduler (volatile! nil)
                            latest (volatile! (when restored
                                                (assoc (:snapshot restored)
                                                       :retained-byte-floor
                                                       (- (long @(:retention-total-bytes state))
                                                          (long (:tail-bytes restored))))))
                            snapshot-error (volatile! nil)
                            runtime (factory/executor
                                     state raw
                                     {:runtime-control
                                      {:check-admission! #(when (and @collector
                                                                     (not (.get closing-force?)))
                                                            (batch/check-serving! @collector))
                                       :on-failure! #(when @collector
                                                       (batch/fence! @collector %))}
                                      :check-batch! (when state
                                                      #(recovery/check-capacity! opts state %))
                                      :rmw-opts (rmw-adapters (recovery/dbis opts))})
                            take-snapshot!
                            (fn []
                              (.lockInterruptibly snapshot-lock)
                              (try
                                (when (.get stopped)
                                  (throw (ex-info "Snapshot runtime is closed"
                                                  {:error :txlog/runtime-closed})))
                                (when @collector (batch/check-serving! @collector))
                                (let [manifest (recovery/snapshot! opts raw state @collector)
                                      removed (recovery/collect-covered-segments! opts state)]
                                  (vreset! latest
                                           (update manifest :retained-byte-floor - removed))
                                  (vreset! snapshot-error nil)
                                  manifest)
                                (finally (.unlock snapshot-lock))))]
                        (try
                          (when (and state (nil? restored)) (take-snapshot!))
                          {:executor (:executor runtime)
                           :check-prepared! #(check-prepared! (recovery/dbis opts) %)
                           :bind!
                           (fn [c]
                             (batch/initialize-prefix! c (long (or (:last-lsn restored) 0)))
                             (vreset! collector c)
                             (when (and state (get opts :snapshot-scheduler? true))
                               ;; A single reusable scheduled task, no queued
                               ;; copies. It cannot inherit request thread locals
                               ;; and never holds the active execution slot.
                               (let [pool (Executors/newSingleThreadScheduledExecutor
                                           (reify ThreadFactory
                                             (newThread [_ task]
                                               (doto (Thread. nil task "datalevin-m0-snapshot" 0 false)
                                                 (.setDaemon true)))))
                                     poll (snapshot/snapshot-scheduler-poll-ms raw)]
                                 (vreset! scheduler pool)
                                 (.scheduleWithFixedDelay
                                  ^ScheduledExecutorService pool
                                  ^Runnable
                                  (fn []
                                    (try
                                      (when (and (not (.get stopped)) (batch/serving? c)
                                                 (snapshot-due? raw state c @latest))
                                        (take-snapshot!))
                                      (catch Throwable t (vreset! snapshot-error t))))
                                  poll poll TimeUnit/MILLISECONDS))))
                           :on-failure! (fn [_] (lifetime/fence! native-lifetime))
                           :resources {:raw raw :wal-state state :wal? wal?
                                       :recovery restored :snapshot! (when state take-snapshot!)
                                       :snapshot-state latest :snapshot-error snapshot-error}
                           :close!
                           (fn []
                             ;; The collector is already closed and quiescent
                             ;; before this hook runs. Permit the authorized
                             ;; final WAL force to finish its durability round.
                             (.set closing-force? true)
                             (.set stopped true)
                             (when-let [^ScheduledExecutorService pool @scheduler]
                               (.shutdown pool))
                             (when (.tryLock snapshot-lock
                                             (long (get opts :write-close-timeout-ms 30000))
                                             TimeUnit/MILLISECONDS)
                               (try
                                 (when ((:close! runtime))
                                   (when (and state @(:healthy? (:sync-manager state)))
                                     (wal/force-through! state (dec (long @(:next-lsn state))) 0))
                                   (i/close-kv db)
                                   (when state (close-wal! state))
                                   true)
                                 (finally (.unlock snapshot-lock)))))}
                          (catch Throwable t
                            ((:close! runtime))
                            (throw t))))
                      (catch Throwable t (when state (close-wal! state)) (throw t))))
                  (catch Throwable t (i/close-kv db) (throw t)))))))))
