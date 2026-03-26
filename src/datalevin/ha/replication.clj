;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.replication
  "Consensus-lease follower replication, probe, and local-store helpers."
  (:require
   [clojure.string :as s]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.ha.client-cache :as cache]
   [datalevin.ha.lease :as lease]
   [datalevin.ha.snapshot :as snap]
   [datalevin.ha.util :as hu]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.remote :as r]
   [datalevin.storage :as st]
   [datalevin.util :as u]
   [datalevin.validate :as vld]
   [taoensso.timbre :as log])
  (:import
   [datalevin.db DB]
   [datalevin.interface IStore ILMDB]
   [datalevin.storage Store]
   [java.net ConnectException URI]
   [java.nio.channels ClosedChannelException]
   [java.util UUID]
   [java.util.concurrent Callable ExecutorCompletionService
    ExecutorService Executors ForkJoinPool Future ThreadFactory TimeUnit]
   [java.util.concurrent.atomic AtomicLong]))

(defn ^:redef ha-now-ms
  []
  (System/currentTimeMillis))

(defn ^:redef ha-now-nanos
  []
  (System/nanoTime))

(def ^:private long-max2 hu/long-max2)
(def ^:private long-max3 hu/long-max3)
(def ^:private long-max4 hu/long-max4)
(def ^:private long-min2 hu/long-min2)
(def ^:private saturated-long-add hu/saturated-long-add)
(def ^:private nonnegative-long-diff hu/nonnegative-long-diff)
(def ^:private ordered-ha-members hu/ordered-ha-members)
(def ^:private shutdown-ha-executor! hu/shutdown-ha-executor!)
(def ^:private ha-thread-label hu/ha-thread-label)
(def ^:private ha-request-timeout-ms hu/ha-request-timeout-ms)
(def ^:private bootstrap-empty-lease? lease/bootstrap-empty-lease?)
(def effective-ha-runtime-local-opts hu/effective-ha-runtime-local-opts)
(def merge-ha-runtime-local-opts hu/merge-ha-runtime-local-opts)

(defn- closed-kv-message-race?
  [t]
  (and t
       (s/includes? (or (ex-message t) "")
                    "LMDB env is closed")))

(declare closed-kv-store?)

(def ^:dynamic *ha-current-state-fn*
  (fn []
    nil))

(defn- current-ha-state
  [m]
  (let [current-m (try
                    (*ha-current-state-fn*)
                    (catch Throwable _
                      nil))]
    (if (map? current-m)
      current-m
      m)))

(defn- kv-store-candidates
  [m]
  (let [current-m (current-ha-state m)
        current-dt-db (:dt-db current-m)
        stale-dt-db   (:dt-db m)]
    (distinct
      (remove nil?
              (concat
                [(:store current-m)
                 (when (instance? DB current-dt-db)
                   (.-store ^DB current-dt-db))]
                (when-not (identical? current-m m)
                  [(:store m)
                   (when (instance? DB stale-dt-db)
                     (.-store ^DB stale-dt-db))]))))))

(defn- local-kv-store
  [m]
  (some
    (fn [store]
      (let [kv-store (cond
                       (nil? store) nil
                       (instance? Store store) (.-lmdb ^Store store)
                       (instance? ILMDB store) store
                       :else nil)]
        (when-not (closed-kv-store? kv-store)
          kv-store)))
    (kv-store-candidates m)))

(defn- raw-local-kv-store
  [m]
  (when-let [kv-store (local-kv-store m)]
    (if (instance? datalevin.kv.KVLMDB kv-store)
      (.-db ^datalevin.kv.KVLMDB kv-store)
      kv-store)))

(defn- closed-kv-store?
  [kv-store]
  (try
    (cond
      (nil? kv-store) true
      (instance? IStore kv-store) (i/closed? kv-store)
      (instance? ILMDB kv-store) (i/closed-kv? kv-store)
      :else false)
    (catch Exception e
      (if (closed-kv-message-race? e)
        true
        (throw e)))))

(defn- closed-kv-race?
  [t kv-store]
  (or (closed-kv-store? kv-store)
      (closed-kv-message-race? t)))

(defn- read-ha-local-persisted-lsn
  [kv-store]
  (long (or (try
              (when-not (closed-kv-store? kv-store)
                (i/get-value kv-store c/kv-info
                             c/ha-local-applied-lsn
                             :keyword :data))
              (catch Throwable t
                (when-not (closed-kv-race? t kv-store)
                  (throw t))
                nil))
            0)))

(defn- ^:redef persist-ha-local-applied-lsn!
  [m applied-lsn]
  (when-let [kv-store (raw-local-kv-store m)]
    (try
      (i/transact-kv kv-store c/kv-info
                     [[:put c/ha-local-applied-lsn (long applied-lsn)]]
                     :keyword :data)
      (catch Throwable t
        (when-not (closed-kv-race? t kv-store)
          (throw t)))))
  applied-lsn)

(defn- ha-follower-persist-every-batches
  [m]
  (long-max2 1
             (long (or (:ha-follower-persist-every-batches m)
                       c/*ha-follower-persist-every-batches*))))

(defn- ha-follower-persist-interval-ms
  [m]
  (max 0
       (long (or (:ha-follower-persist-interval-ms m)
                 c/*ha-follower-persist-interval-ms*))))

(defn- seed-ha-local-persist-tracking
  [m now-ms]
  (if (integer? (:ha-local-persisted-lsn m))
    m
    (assoc m
           :ha-local-persisted-lsn
           (long (if-let [kv-store (local-kv-store m)]
                   (read-ha-local-persisted-lsn kv-store)
                   0))
           :ha-local-last-persisted-applied-ms (long now-ms)
           :ha-follower-batches-since-persist 0)))

(defn- note-ha-local-applied-lsn-persisted
  [m applied-lsn now-ms]
  (assoc m
         :ha-local-persisted-lsn (long applied-lsn)
         :ha-local-last-persisted-applied-ms (long now-ms)
         :ha-follower-batches-since-persist 0))

(defn- maybe-persist-ha-follower-local-applied-lsn
  [m applied-lsn now-ms]
  (let [tracked-m (seed-ha-local-persist-tracking m now-ms)
        persisted-lsn (long (or (:ha-local-persisted-lsn tracked-m) 0))
        next-batch-count
        (long (inc (long (or (:ha-follower-batches-since-persist tracked-m)
                             0))))
        persist-interval-ms (ha-follower-persist-interval-ms tracked-m)
        last-persisted-ms
        (long (or (:ha-local-last-persisted-applied-ms tracked-m)
                  now-ms))
        elapsed-ms (nonnegative-long-diff now-ms last-persisted-ms)
        persist-every-batches (ha-follower-persist-every-batches tracked-m)
        interval-elapsed?
        (and (pos? (long persist-interval-ms))
             (>= (long elapsed-ms)
                 (long persist-interval-ms)))
        batch-threshold-reached?
        (>= (long next-batch-count)
            (long persist-every-batches))]
    ;; `:ha/local-applied-lsn` is a conservative follower floor used for
    ;; promotion/rejoin decisions. The exact replayed payload watermark is
    ;; tracked separately via `:wal/local-payload-lsn`.
    (if (and (> (long applied-lsn) persisted-lsn)
             (or interval-elapsed?
                 batch-threshold-reached?))
      (do
        (persist-ha-local-applied-lsn! tracked-m applied-lsn)
        (note-ha-local-applied-lsn-persisted
         tracked-m applied-lsn now-ms))
      (assoc tracked-m :ha-follower-batches-since-persist next-batch-count))))

(defn- read-ha-local-snapshot-current-lsn
  [kv-store]
  (long (or (try
              (when-not (closed-kv-store? kv-store)
                (i/get-value kv-store c/kv-info
                             c/wal-snapshot-current-lsn
                             :keyword :data))
              (catch Throwable t
                (when-not (closed-kv-race? t kv-store)
                  (throw t))
                nil))
            0)))

(defn- read-ha-local-payload-lsn
  [kv-store]
  (long (or (try
              (when-not (closed-kv-store? kv-store)
                (i/get-value kv-store c/kv-info
                             c/wal-local-payload-lsn
                             :keyword :data))
              (catch Throwable t
                (when-not (closed-kv-race? t kv-store)
                  (throw t))
                nil))
            0)))

(def ^:private ha-local-watermark-snapshot-key
  ::ha-local-watermark-snapshot)

(def ^:private ha-local-store-transient-state-keys
  [ha-local-watermark-snapshot-key
   :wdt-db
   :wlmdb
   :wstore
   :runner
   :engine
   :index])

(defn- clear-ha-local-store-transient-state
  [m]
  (apply dissoc m ha-local-store-transient-state-keys))

(declare read-ha-local-watermark-lsn)

(defn- ha-local-data-lsn-ceiling
  [m kv-store]
  (let [role (:ha-role m)
        watermark-lsn (read-ha-local-watermark-lsn m)
        snapshot-lsn (read-ha-local-snapshot-current-lsn kv-store)
        payload-lsn (if (= :leader role)
                      (read-ha-local-payload-lsn kv-store)
                      0)]
    {:snapshot-lsn snapshot-lsn
     :watermark-lsn watermark-lsn
     :payload-lsn payload-lsn
     :ceiling-lsn (long (if (= :leader role)
                          (long-max3 watermark-lsn snapshot-lsn payload-lsn)
                          (long-max2 watermark-lsn snapshot-lsn)))}))

(defn- ha-clamped-follower-floor-lsn
  [persisted-lsn snapshot-lsn ceiling-lsn]
  (let [tracked-lsn (long-max2 persisted-lsn snapshot-lsn)]
    (if (pos? (long ceiling-lsn))
      (if (pos? tracked-lsn)
        (long-min2 ceiling-lsn tracked-lsn)
        ceiling-lsn)
      tracked-lsn)))

(declare read-ha-local-last-applied-lsn)
(declare read-ha-local-watermark-lsn)
(declare with-ha-local-store-read)

(defn- read-ha-local-watermark-lsn*
  [kv-store]
  (long (or (try
              (get (kv/txlog-watermarks kv-store) :last-applied-lsn)
              (catch Throwable t
                (when-not (closed-kv-race? t kv-store)
                  (throw t))
                nil))
            0)))

(defn- ^:redef fresh-ha-local-watermark-snapshot
  [m kv-store]
  (let [state-lsn (long (or (:ha-local-last-applied-lsn m) 0))
        role (:ha-role m)
        watermark-lsn (read-ha-local-watermark-lsn* kv-store)
        snapshot-lsn (read-ha-local-snapshot-current-lsn kv-store)
        payload-lsn (if (= :leader role)
                      (read-ha-local-payload-lsn kv-store)
                      0)
        persisted-lsn (long (read-ha-local-persisted-lsn kv-store))
        ceiling-lsn (long (if (= :leader role)
                            (long-max3 watermark-lsn
                                       snapshot-lsn
                                       payload-lsn)
                            (long-max2 watermark-lsn
                                       snapshot-lsn)))
        follower-floor-lsn
        (ha-clamped-follower-floor-lsn
         persisted-lsn snapshot-lsn ceiling-lsn)
        local-last-applied-lsn
        (long (if (= :leader role)
                ceiling-lsn
                (if (pos? (long ceiling-lsn))
                  follower-floor-lsn
                  state-lsn)))]
    {:role role
     :kv-store kv-store
     :state-lsn state-lsn
     :watermark-lsn watermark-lsn
     :snapshot-lsn snapshot-lsn
     :payload-lsn payload-lsn
     :persisted-lsn persisted-lsn
     :ceiling-lsn ceiling-lsn
     :follower-floor-lsn follower-floor-lsn
     :local-last-applied-lsn local-last-applied-lsn}))

(defn- cached-ha-local-watermark-snapshot
  [m kv-store]
  (let [snapshot (get m ha-local-watermark-snapshot-key)]
    (when (and (map? snapshot)
               (= (:role snapshot) (:ha-role m))
               (identical? (:kv-store snapshot) kv-store))
      snapshot)))

(defn- ha-local-watermark-snapshot
  [m kv-store]
  (or (cached-ha-local-watermark-snapshot m kv-store)
      (fresh-ha-local-watermark-snapshot m kv-store)))

(defn ^:redef read-ha-snapshot-payload-lsn
  [m]
  (with-ha-local-store-read
    #(if-let [kv-store (local-kv-store m)]
       (read-ha-local-payload-lsn kv-store)
       0)))

(defn ^:redef read-ha-local-last-applied-lsn
  [m]
  (with-ha-local-store-read
    #(let [state-lsn (long (or (:ha-local-last-applied-lsn m) 0))]
       (try
         (if-let [kv-store (local-kv-store m)]
           (:local-last-applied-lsn
            (ha-local-watermark-snapshot m kv-store))
           state-lsn)
         (catch Throwable e
           (when-not (closed-kv-race? e (local-kv-store m))
             (log/warn e "Unable to read local txlog watermarks for HA lag guard"
                       {:db-name (some-> (:store m) i/opts :db-name)}))
           state-lsn)))))

(defn- ^:redef read-ha-local-watermark-lsn
  [m]
  (with-ha-local-store-read
    #(if-let [kv-store (local-kv-store m)]
       (if-let [snapshot (cached-ha-local-watermark-snapshot m kv-store)]
         (long (:watermark-lsn snapshot))
         (read-ha-local-watermark-lsn* kv-store))
       0)))

(defn persist-ha-runtime-local-applied-lsn!
  [m]
  (with-ha-local-store-read
    #(when-let [kv-store (local-kv-store m)]
       (let [{:keys [ceiling-lsn]} (ha-local-data-lsn-ceiling m kv-store)
             persisted-lsn (long (read-ha-local-persisted-lsn kv-store))
             state-lsn (long (or (:ha-local-last-applied-lsn m) 0))
             lease-lsn (long (or (get-in m
                                         [:ha-authority-lease
                                          :leader-last-applied-lsn])
                                 0))
             ;; `:ha/local-applied-lsn` is follower replay state. Persisting a
             ;; former leader's raw local txlog watermark here can overstate how
             ;; much replicated history that node actually shares with the current
             ;; leader on rejoin.
             target-lsn (long (if (= :leader (:ha-role m))
                                (long-max2 persisted-lsn lease-lsn)
                                (long-max2 persisted-lsn state-lsn)))
             applied-lsn (long (if (pos? (long ceiling-lsn))
                                 (long-min2 ceiling-lsn target-lsn)
                                 target-lsn))]
         (when (> applied-lsn persisted-lsn)
           (persist-ha-local-applied-lsn! m applied-lsn))
         applied-lsn))))

(defn- ha-local-last-applied-lsn
  [m]
  (let [state-lsn (:ha-local-last-applied-lsn m)]
    (if (local-kv-store m)
      (read-ha-local-last-applied-lsn m)
      (if (integer? state-lsn)
        (long state-lsn)
        0))))

(defn- refresh-ha-local-watermarks
  [m]
  (with-ha-local-store-read
    #(try
       (if-let [kv-store (local-kv-store m)]
         (let [{:keys [role local-last-applied-lsn] :as snapshot}
               (ha-local-watermark-snapshot m kv-store)]
           (cond-> (assoc m
                          ha-local-watermark-snapshot-key snapshot
                          :ha-local-last-applied-lsn local-last-applied-lsn)
             (= :leader role)
             (assoc :ha-leader-last-applied-lsn local-last-applied-lsn)))
         m)
       (catch Throwable e
         (when-not (closed-kv-race? e (local-kv-store m))
           (log/warn e "Unable to refresh local txlog watermarks for HA renew"
                     {:db-name (some-> (:store m) i/opts :db-name)}))
         m))))

(declare fresh-ha-promotion-local-last-applied-lsn)

(defn- ha-promotion-lag-guard
  ([m lease]
   (ha-promotion-lag-guard m lease
                           (fresh-ha-promotion-local-last-applied-lsn
                            m lease)))
  ([m lease local-last-applied-lsn]
   (let [leader-last-applied-lsn (long (or (:leader-last-applied-lsn lease) 0))
         local-last-applied-lsn (long (or local-last-applied-lsn 0))
        lag-lsn (nonnegative-long-diff leader-last-applied-lsn
                                       local-last-applied-lsn)
        max-lag-lsn (long (or (:ha-max-promotion-lag-lsn m) 0))]
     {:ok? (<= lag-lsn max-lag-lsn)
      :leader-last-applied-lsn leader-last-applied-lsn
      :local-last-applied-lsn local-last-applied-lsn
      :lag-lsn lag-lsn
      :max-lag-lsn max-lag-lsn})))

(defn- fresh-ha-promotion-local-last-applied-lsn
  [m lease]
  (with-ha-local-store-read
    #(let [state-lsn (long (or (:ha-local-last-applied-lsn m) 0))]
       (try
         (if-let [kv-store (local-kv-store m)]
           (:local-last-applied-lsn
            (fresh-ha-local-watermark-snapshot m kv-store))
           (if (and (bootstrap-empty-lease? lease)
                    (zero? state-lsn))
             (long-max2 state-lsn (read-ha-local-watermark-lsn m))
             state-lsn))
         (catch Throwable e
           (when-not (closed-kv-race? e (local-kv-store m))
             (log/warn e "Unable to read fresh local txlog watermarks for HA promotion lag guard"
                       {:db-name (some-> (:store m) i/opts :db-name)}))
           state-lsn)))))

(def ^:private endpoint-pattern
  #"^(.+):([0-9]+)$")

(defn- parse-endpoint
  [endpoint]
  (when-let [[_ host port-str] (and (string? endpoint)
                                    (re-matches endpoint-pattern endpoint))]
    (let [port (parse-long port-str)]
      (when (and (not (s/blank? host))
                 (integer? port)
                 (<= 1 (long port) 65535))
        {:host host :port (long port)}))))

(declare default-ha-probe-executor)

(defn- stop-ha-probe-executor!
  [db-name executor]
  (when (and executor
             (not (identical? executor default-ha-probe-executor)))
    (shutdown-ha-executor! executor
                           "HA probe executor"
                           {:db-name db-name})))

(defn- ha-client-credentials
  [m]
  (or (:ha-client-credentials m)
      {:username c/default-username
       :password c/default-password}))

(defn- ha-endpoint-uri
  [db-name endpoint m]
  (when-let [{:keys [host port]} (parse-endpoint endpoint)]
    (let [{:keys [username password]} (ha-client-credentials m)]
      (str (URI. "dtlv"
                 (str username ":" password)
                 host
                 (int port)
                 (str "/" db-name)
                 nil
                 nil)))))

(def ^:redef sync-ha-snapshot-install-target!
  snap/sync-ha-snapshot-install-target!)
(def ha-snapshot-install-marker-path
  snap/ha-snapshot-install-marker-path)
(def ^:private copy-dir-contents! snap/copy-dir-contents!)
(def ^:private move-path! snap/move-path!)
(def ^:private write-ha-snapshot-install-marker!
  snap/write-ha-snapshot-install-marker!)
(def ^:private delete-ha-snapshot-install-marker!
  snap/delete-ha-snapshot-install-marker!)
(def ^:private recover-ha-local-snapshot-install!
  snap/recover-ha-local-snapshot-install!)
(def recover-ha-local-store-dir-if-needed!
  snap/recover-ha-local-store-dir-if-needed!)
(def ^:private close-ha-local-store! snap/close-ha-local-store!)
(def ^:private refresh-ha-local-dt-db snap/refresh-ha-local-dt-db)

(declare open-ha-store-dbis!)
(def ^:private ha-local-store-reopen-info-key
  ::ha-local-store-reopen-info)
(declare ha-local-store-reopen-info)
(declare reopen-ha-local-store-from-info)
(declare with-ha-local-store-swap)

(def ^:dynamic *ha-with-local-store-swap-fn*
  (fn [f]
    (f)))

(defn- with-ha-local-store-swap
  [f]
  (*ha-with-local-store-swap-fn* f))

(def ^:dynamic *ha-with-local-store-read-fn*
  (fn [f]
    (f)))

(defn- with-ha-local-store-read
  [f]
  (*ha-with-local-store-read-fn* f))

(defn- ha-local-store-open-opts
  [m store]
  (let [store-opts (if (instance? IStore store)
                     (or (i/opts store) {})
                     {})
        runtime-opts (effective-ha-runtime-local-opts m)
        db-name (or (:db-name store-opts)
                    (some-> store i/db-name))
        db-identity (or (:ha-db-identity m)
                        (:db-identity store-opts))
        open-opts (merge-ha-runtime-local-opts store-opts runtime-opts)]
    (cond-> open-opts
      db-name (assoc :db-name db-name)
      db-identity (assoc :db-identity db-identity))))

(defn recover-ha-local-store-if-needed
  ([store]
   (recover-ha-local-store-if-needed store nil))
  ([store open-opts]
   (if-not (instance? IStore store)
     store
     (let [env-dir (i/dir store)]
       (if-not (recover-ha-local-store-dir-if-needed! env-dir)
         store
         (let [schema (i/schema store)
               opts (merge (or (i/opts store) {})
                           (or open-opts {}))]
           (when-not (i/closed? store)
             (i/close store))
           (-> (st/open env-dir schema opts)
               open-ha-store-dbis!)))))))

(defn- reopen-ha-local-store-if-needed
  [m]
  (let [store (:store m)
        open-opts (ha-local-store-open-opts m store)
        recovered-store (recover-ha-local-store-if-needed store open-opts)
        m (if (identical? recovered-store store)
            m
            (-> m
                clear-ha-local-store-transient-state
                (assoc :store recovered-store
                       :dt-db nil)
                (dissoc ha-local-store-reopen-info-key)
                refresh-ha-local-dt-db))]
    (if (local-kv-store m)
      m
      (if-let [reopen-info (ha-local-store-reopen-info m)]
        (try
          (reopen-ha-local-store-from-info m reopen-info)
          (catch Throwable e
            (u/raise "HA local store reopen failed"
                     {:error :ha/follower-local-store-reopen-failed
                      :env-dir (:env-dir reopen-info)
                      :reopen-info reopen-info
                      :message (ex-message e)
                      :data (ex-data e)
                      :state (-> m
                                 (assoc ha-local-store-reopen-info-key
                                        reopen-info
                                        :dt-db nil)
                                 (dissoc :engine :index))})))
        m))))

(defn- ha-local-store-reopen-info
  [m]
  (let [current-m (current-ha-state m)
        candidates (if (identical? current-m m)
                     [m]
                     [current-m m])]
    (or (some #(get % ha-local-store-reopen-info-key) candidates)
        (some (fn [state]
                (let [store (:store state)]
        (when (instance? IStore store)
          (try
            {:env-dir (i/dir store)
             :store-opts (ha-local-store-open-opts state store)}
                      (catch Throwable _
                        nil)))))
              candidates))))

(defn- ^:redef reopen-ha-local-store-from-info
  [m {:keys [env-dir store-opts]}]
  (when (and (string? env-dir)
             (not (s/blank? env-dir))
             (map? store-opts))
    (-> m
        ;; A saved reopen recipe can be applied after partial snapshot install
        ;; recovery while the previous store handle is still present in state.
        ;; Always retire that handle first; otherwise we can leak multiple LMDB
        ;; connections to the same env and poison later copy/watermark reads.
        ((fn [state]
           (close-ha-local-store! state)
           state))
        clear-ha-local-store-transient-state
        (assoc :store (-> (st/open env-dir nil store-opts)
                          open-ha-store-dbis!)
               :dt-db nil)
        (dissoc ha-local-store-reopen-info-key)
        refresh-ha-local-dt-db)))

(defn- normalize-ha-bootstrap-retry-state
  [candidate-m fallback-m reopen-info]
  (or (try
        (let [state (reopen-ha-local-store-if-needed candidate-m)]
          (when (local-kv-store state)
            state))
        (catch Throwable _
          nil))
      (try
        (let [state (reopen-ha-local-store-if-needed fallback-m)]
          (when (local-kv-store state)
            state))
        (catch Throwable _
          nil))
      (try
        ;; Snapshot install can leave only a reopen recipe behind after a
        ;; failed restore; use it before giving up on the next source.
        (let [state (with-ha-local-store-swap
                      #(reopen-ha-local-store-from-info fallback-m
                                                        reopen-info))]
          (when (local-kv-store state)
            state))
        (catch Throwable _
          nil))
      candidate-m))

(defn- ha-local-contiguous-txlog-tail
  [kv-store from-lsn upto-lsn]
  (if (or (nil? kv-store)
          (> (long from-lsn) (long upto-lsn)))
    []
    (loop [expected (long from-lsn)
           remaining (seq (kv/open-tx-log-rows kv-store from-lsn upto-lsn))
           acc []]
      (if-let [record (first remaining)]
        (let [record-lsn (long (:lsn record))
              rows (or (:rows record) (:ops record))]
          (if (and (= record-lsn expected)
                   (sequential? rows))
            (recur (unchecked-inc expected)
                   (next remaining)
                   (conj acc record))
            acc))
        acc))))

(defn- inspect-ha-local-bootstrap-tail
  [m snapshot-lsn]
  (if-let [kv-store (raw-local-kv-store m)]
    (let [persisted-floor-lsn
          (read-ha-local-persisted-lsn kv-store)
          candidate-floor-lsn
          (long-max4 snapshot-lsn
                     persisted-floor-lsn
                     (read-ha-local-watermark-lsn m)
                     (read-ha-snapshot-payload-lsn m))
          records
          (ha-local-contiguous-txlog-tail
           kv-store
           (unchecked-inc (long snapshot-lsn))
           candidate-floor-lsn)
          verified-floor-lsn
          (long (if (or (= candidate-floor-lsn (long snapshot-lsn))
                        (seq records))
                  candidate-floor-lsn
                  snapshot-lsn))]
      {:verified-floor-lsn verified-floor-lsn
       :persisted-floor-lsn persisted-floor-lsn
       :candidate-floor-lsn candidate-floor-lsn
       :tail-record-count (count records)})
    {:verified-floor-lsn (long snapshot-lsn)
     :persisted-floor-lsn (long snapshot-lsn)
     :candidate-floor-lsn (long snapshot-lsn)
     :tail-record-count 0}))

(defn- persist-ha-local-bootstrap-floor!
  [m applied-lsn]
  (when-let [kv-store (raw-local-kv-store m)]
    (i/transact-kv kv-store
                   c/kv-info
                   [[:put c/wal-local-payload-lsn (long applied-lsn)]
                    [:put c/ha-local-applied-lsn (long applied-lsn)]]
                   :keyword
                   :data))
  (long applied-lsn))

(defn- reconcile-ha-installed-snapshot-state
  [m snapshot-lsn]
  (let [{:keys [verified-floor-lsn] :as replay}
        (inspect-ha-local-bootstrap-tail m snapshot-lsn)
        reopen-info (ha-local-store-reopen-info m)
        clamped? (< (long verified-floor-lsn)
                    (long (:candidate-floor-lsn replay)))
        _ (when clamped?
            (persist-ha-local-bootstrap-floor! m verified-floor-lsn))
        next-m (if (and clamped? reopen-info)
                 (with-ha-local-store-swap
                   #(do
                      (close-ha-local-store! m)
                      (reopen-ha-local-store-from-info m reopen-info)))
                 m)]
    (assoc replay
           :installed-lsn (long verified-floor-lsn)
           :state next-m)))

(def ^:private open-ha-store-dbis! snap/open-ha-store-dbis!)

(defn- ha-snapshot-open-opts
  [m db-name db-identity]
  (let [store (:store m)
        base-opts (ha-local-store-open-opts m store)]
    (assoc (or base-opts {})
           ::st/raw-persist-open-opts? true
           :db-name db-name
           :db-identity db-identity)))

(defn ^:redef fetch-ha-endpoint-watermark-lsn
  [db-name m endpoint]
  (let [local-endpoint (:ha-local-endpoint m)]
    (cond
      (or (nil? endpoint) (s/blank? endpoint))
      {:reachable? false
       :reason :missing-endpoint}

      (= endpoint local-endpoint)
      {:reachable? true
       :last-applied-lsn (ha-local-last-applied-lsn m)
       :ha-authority-owner-node-id (:ha-authority-owner-node-id m)
       :ha-authority-term (:ha-authority-term m)
       :ha-role (:ha-role m)
       :ha-runtime? (boolean (:ha-authority m))
       :source :local}

      :else
      (if-let [uri (ha-endpoint-uri db-name endpoint m)]
        (let [client-opts
              {:pool-size 1
               :time-out (ha-request-timeout-ms m 5000)}]
          (try
            (cache/with-cached-ha-client
              m uri db-name client-opts
              (fn [client]
                (let [watermarks
                      (try
                        (cache/ha-client-request
                         client
                         :ha-watermark
                         [db-name]
                         false)
                        (catch Exception e
                          (if (cache/unknown-ha-watermark-command? e)
                            (cache/ha-client-request
                             client
                             :txlog-watermarks
                             [db-name]
                             false)
                            (throw e))))]
                  {:reachable? true
                   :last-applied-lsn
                   (long (or (:last-applied-lsn watermarks) 0))
                   :txlog-last-applied-lsn
                   (long (or (:txlog-last-applied-lsn watermarks)
                             (:last-applied-lsn watermarks)
                             0))
                   :ha-local-last-applied-lsn
                   (some-> (:ha-local-last-applied-lsn watermarks) long)
                   :ha-authority-owner-node-id
                   (some-> (:ha-authority-owner-node-id watermarks) long)
                   :ha-authority-term
                   (some-> (:ha-authority-term watermarks) long)
                   :ha-role (:ha-role watermarks)
                   :ha-runtime? (:ha-runtime? watermarks)
                   :ha-control-node-leader?
                   (:ha-control-node-leader? watermarks)
                   :ha-control-node-state
                   (:ha-control-node-state watermarks)
                   :source (if (:ha-runtime? watermarks)
                             :remote-ha-runtime
                             :remote)})))
            (catch Exception e
              {:reachable? false
               :reason :endpoint-watermark-fetch-failed
               :message (ex-message e)})))
        {:reachable? false
         :reason :invalid-endpoint
         :endpoint endpoint}))))

(def ^:redef open-ha-snapshot-remote-store!
  r/open-kv)

(def ^:redef copy-ha-remote-store!
  i/copy)

(def ^:redef unpin-ha-remote-store-backup-floor!
  i/txlog-unpin-backup-floor!)

(def ^:redef close-ha-snapshot-remote-store!
  i/close-kv)

(defn- safe-fetch-ha-endpoint-watermark-lsn
  [db-name m endpoint]
  (try
    (fetch-ha-endpoint-watermark-lsn db-name m endpoint)
    (catch Exception e
      {:reachable? false
       :reason :endpoint-watermark-fetch-failed
       :endpoint endpoint
       :message (ex-message e)})))

(def ^:private ha-probe-max-threads
  (-> (.availableProcessors (Runtime/getRuntime))
      (* 2)
      (max 4)
      (min 16)))

(def ^:private ^AtomicLong ha-probe-thread-seq
  (AtomicLong. 0))

(defn- new-ha-probe-thread-factory
  ([] (new-ha-probe-thread-factory nil))
  ([label]
   (let [^ThreadFactory delegate (Executors/defaultThreadFactory)]
     (reify ThreadFactory
       (newThread [_ runnable]
         (doto (.newThread delegate runnable)
           (.setName
            (str "datalevin-ha-probe"
                 (when label (str "-" label))
                 "-"
                 (.incrementAndGet ^AtomicLong ha-probe-thread-seq)))
           (.setDaemon true)))))))

(def ^:private ^ExecutorService default-ha-probe-executor
  (ForkJoinPool/commonPool))

(defn- new-ha-probe-executor
  [db-name]
  (Executors/newFixedThreadPool
   (int ha-probe-max-threads)
   (new-ha-probe-thread-factory (ha-thread-label db-name))))

(defn- ha-probe-executor-for
  [m]
  (or (:ha-probe-executor m)
      default-ha-probe-executor))

(defn ^:redef ha-probe-round-timeout-ms
  [m]
  (long (max 1
             (long (or (:ha-probe-round-timeout-ms m)
                       (ha-request-timeout-ms m 1000))))))

(deftype ^:private HaParallelProbeTask [^int index
                                        item
                                        ^clojure.lang.IFn f
                                        values
                                        errors]
  Callable
  (call [_]
    (let [^objects values values
          ^objects errors  errors
          idx              (int index)]
      (try
        (aset values idx (f item))
        (Integer/valueOf idx)
        (catch Throwable t
          (aset errors idx t)
          (Integer/valueOf idx))))))

(defn- ha-parallel-mapv
  ([f coll]
   (ha-parallel-mapv nil f coll))
  ([m f coll]
   (ha-parallel-mapv m f coll nil))
  ([m f coll timeout-value-fn]
   (let [items (if (vector? coll)
                 coll
                 (vec coll))
         n (count items)]
     (cond
       (zero? n)
       []

       (= 1 n)
       [(f (first items))]

       :else
       (let [^ExecutorService executor (ha-probe-executor-for m)
             ^ExecutorCompletionService completion
             (ExecutorCompletionService. executor)
             ^objects results (object-array n)
             ^objects values (object-array n)
             ^objects errors (object-array n)
             ^objects futures (object-array n)
             completed (boolean-array n)
             timeout-ms (long (ha-probe-round-timeout-ms m))
             deadline-nanos
             (saturated-long-add
              (long (ha-now-nanos))
              (.toNanos TimeUnit/MILLISECONDS timeout-ms))]
         (dotimes [idx n]
           (aset futures idx
                 (.submit completion
                          ^Callable
                          (HaParallelProbeTask.
                           (int idx)
                           (nth items idx)
                           f
                           values
                           errors))))
         (letfn [(finish [^long first-failure]
                   (if (neg? first-failure)
                     (loop [idx 0
                            acc (transient [])]
                       (if (< idx n)
                         (recur (unchecked-inc-int idx)
                                (conj! acc (aget results idx)))
                         (persistent! acc)))
                     (let [index (int first-failure)
                           item (nth items index)
                           error (aget errors index)
                           failure-count
                           (loop [idx 0
                                  count (long 0)]
                             (if (< idx n)
                               (recur (unchecked-inc-int idx)
                                      (if (and (aget completed idx)
                                               (some? (aget errors idx)))
                                        (unchecked-inc count)
                                        count))
                               count))]
                       (throw
                        (ex-info "HA parallel probe task failed"
                                 {:error :ha/parallel-probe-failed
                                  :index index
                                  :item item
                                  :failure-count failure-count}
                                 error)))))
                 (record-completion [^long remaining
                                     ^long first-failure
                                     ^Future completed-future]
                   (let [idx (int ^Integer (.get completed-future))]
                     (if (aget completed idx)
                       [remaining first-failure]
                       (do
                         (aset completed idx true)
                         (when-not (some? (aget errors idx))
                           (aset results idx (aget values idx)))
                         [(unchecked-dec remaining)
                          (if (and (neg? first-failure)
                                   (some? (aget errors idx)))
                            (long idx)
                            first-failure)]))))
                 (drain-ready [^long remaining ^long first-failure]
                   (loop [remaining remaining
                          first-failure first-failure]
                     (if-let [completed-future (.poll completion)]
                       (let [[next-remaining next-first-failure]
                             (record-completion
                              remaining
                              first-failure
                              completed-future)
                             remaining (long next-remaining)
                             first-failure (long next-first-failure)]
                         (recur remaining first-failure))
                       [remaining first-failure])))
                 (timeout-items []
                   (loop [idx 0
                          acc (transient [])]
                     (if (< idx n)
                       (recur (unchecked-inc-int idx)
                              (if (aget completed idx)
                                acc
                                (conj! acc (nth items idx))))
                       (persistent! acc))))
                 (assign-timeout-results! []
                   (dotimes [idx n]
                     (when-not (aget completed idx)
                       (aset results idx
                             (timeout-value-fn (nth items idx))))))
                 (cancel-pending! []
                   (dotimes [idx n]
                     (when-not (aget completed idx)
                       (when-let [^Future future (aget futures idx)]
                         (.cancel future true)))))]
           (loop [remaining (long n)
                  first-failure (long -1)]
             (let [remaining (long remaining)
                   first-failure (long first-failure)]
               (if (zero? remaining)
                 (finish first-failure)
                 (let [remaining-nanos
                       (unchecked-subtract
                        (long deadline-nanos)
                        (long (ha-now-nanos)))
                       completed-future
                       (when (pos? remaining-nanos)
                         (.poll completion
                                (long (max 1
                                           (quot remaining-nanos
                                                 1000000)))
                                TimeUnit/MILLISECONDS))]
                   (if completed-future
                     (let [[next-remaining next-first-failure]
                           (record-completion
                            remaining
                            first-failure
                            completed-future)
                           remaining (long next-remaining)
                           first-failure (long next-first-failure)]
                       (recur remaining first-failure))
                     (let [[next-remaining next-first-failure]
                           (drain-ready remaining first-failure)
                           remaining (long next-remaining)
                           first-failure (long next-first-failure)]
                       (if (zero? remaining)
                         (finish first-failure)
                         (do
                           (cancel-pending!)
                           (if timeout-value-fn
                             (do
                               (assign-timeout-results!)
                               (finish first-failure))
                             (throw (ex-info "HA parallel probe timed out"
                                             {:error :ha/parallel-probe-timeout
                                              :timeout-ms timeout-ms
                                              :pending-count remaining
                                              :items (timeout-items)})))))))))))))))))

(defn- normalize-leader-watermark-result
  [lease result]
  (let [leader-endpoint (:leader-endpoint lease)
        authority-lsn (long (or (:leader-last-applied-lsn lease) 0))]
    (if (:reachable? result)
      (update result
              :last-applied-lsn
              #(long (or % authority-lsn)))
      (cond-> result
        (= :missing-endpoint (:reason result))
        (assoc :reason :missing-leader-endpoint)

        (= :invalid-endpoint (:reason result))
        (assoc :reason :invalid-leader-endpoint
               :leader-endpoint leader-endpoint)

        (= :endpoint-watermark-fetch-failed (:reason result))
        (assoc :reason :leader-watermark-fetch-failed)))))

(defn ^:redef fetch-leader-watermark-lsn
  [db-name m lease]
  (let [leader-endpoint (:leader-endpoint lease)
        result (safe-fetch-ha-endpoint-watermark-lsn
                db-name m leader-endpoint)]
    (normalize-leader-watermark-result lease result)))

(defn- ha-member-watermark-endpoints
  ([m]
   (ha-member-watermark-endpoints m nil))
  ([m extra-endpoints]
   (->> (concat [(:ha-local-endpoint m)]
                extra-endpoints
                (map :endpoint (:ha-members m)))
        (filter #(and (string? %)
                      (not (s/blank? %))))
        distinct
        vec)))

(defn- ha-member-watermarks
  ([db-name m]
   (ha-member-watermarks db-name m nil {}))
  ([db-name m extra-endpoints]
   (ha-member-watermarks db-name m extra-endpoints {}))
  ([db-name m extra-endpoints prefetched-watermarks]
   (let [endpoints (ha-member-watermark-endpoints m extra-endpoints)]
     (into {}
           (ha-parallel-mapv
            m
            (fn [endpoint]
              [endpoint
               (if (contains? prefetched-watermarks endpoint)
                 (get prefetched-watermarks endpoint)
                 (safe-fetch-ha-endpoint-watermark-lsn
                  db-name m endpoint))])
            endpoints
            (fn [endpoint]
              [endpoint
               {:reachable? false
                :reason :endpoint-watermark-fetch-timed-out}]))))))

(defn- highest-reachable-ha-member-watermark*
  [m watermarks]
  (let [local-endpoint (:ha-local-endpoint m)]
    (reduce-kv
     (fn [best endpoint watermark]
       (if-not (:reachable? watermark)
         best
         (let [last-applied-lsn
               (long (or (:last-applied-lsn watermark) 0))
               candidate {:endpoint endpoint
                          :last-applied-lsn last-applied-lsn
                          :watermark watermark}]
           (cond
             (nil? best)
             candidate

             (> last-applied-lsn
                (long (:last-applied-lsn best)))
             candidate

             (and (= last-applied-lsn
                     (long (:last-applied-lsn best)))
                  (= endpoint local-endpoint)
                  (not= endpoint (:endpoint best)))
             candidate

             :else
             best))))
     nil
     watermarks)))

(defn- highest-reachable-ha-member-watermark
  ([db-name m]
   (highest-reachable-ha-member-watermark db-name m {}))
  ([db-name m prefetched-watermarks]
   (highest-reachable-ha-member-watermark*
    m
    (ha-member-watermarks
     db-name
     m
     (keys prefetched-watermarks)
     prefetched-watermarks))))

(defn- ha-follower-max-batch-records
  [m]
  (long (or (:ha-follower-max-batch-records m)
            c/*ha-follower-max-batch-records*)))

(defn- ha-follower-target-batch-bytes
  [m]
  (long (or (:ha-follower-target-batch-bytes m)
            c/*ha-follower-target-batch-bytes*)))

(def ^:private ha-follower-adaptive-batch-assumed-record-bytes 4096)
(def ^:private ha-follower-adaptive-batch-sample-limit 16)
(def ^:private ha-follower-adaptive-batch-value-max-depth 3)
(def ^:private ha-follower-adaptive-batch-value-sample-limit 8)
(def ^:private ha-follower-adaptive-batch-byte-array-class
  (class (byte-array 0)))
(def ^:private ha-follower-adaptive-batch-value-overhead-bytes 16)
(def ^:private ha-follower-adaptive-batch-record-overhead-bytes 32)

(declare estimate-ha-follower-value-bytes*)

(defn- estimate-ha-follower-truncated-map-bytes ^long
  [value]
  (let [entry-count (if (counted? value)
                      (long (count value))
                      1)]
    (long (+ 32
             (* 2
                (long ha-follower-adaptive-batch-value-overhead-bytes)
                entry-count)))))

(defn- estimate-ha-follower-truncated-coll-bytes ^long
  [value]
  (let [item-count (if (counted? value)
                     (long (count value))
                     1)]
    (long (+ 16
             (* (long ha-follower-adaptive-batch-value-overhead-bytes)
                item-count)))))

(defn- extrapolate-ha-follower-sampled-bytes ^long
  [^long base-bytes ^long sample-bytes ^long sampled-count ^long total-count]
  (let [limit (long ha-follower-adaptive-batch-value-sample-limit)
        avg-item-bytes (if (pos? sampled-count)
                         (long-max2
                          1
                          (long (Math/ceil
                                 (/ (double (- sample-bytes base-bytes))
                                    (double sampled-count)))))
                         (long ha-follower-adaptive-batch-value-overhead-bytes))
        known-total? (not= total-count -1)]
    (cond
      (and known-total? (> total-count sampled-count))
      (let [remaining-count (unchecked-subtract total-count sampled-count)]
        (long (+ sample-bytes
                 (* avg-item-bytes remaining-count))))

      (and (not known-total?)
           (= sampled-count limit))
      (long (+ sample-bytes avg-item-bytes))

      :else
      sample-bytes)))

(defn- estimate-ha-follower-map-bytes ^long
  [value ^long remaining-depth]
  (if (<= remaining-depth 0)
    (estimate-ha-follower-truncated-map-bytes value)
    (let [limit (long ha-follower-adaptive-batch-value-sample-limit)
          next-depth (unchecked-dec remaining-depth)
          total-count (long (if (counted? value)
                              (count value)
                              -1))]
      (loop [entries (seq value)
             sampled-count (long 0)
             sample-bytes (long 32)]
        (if (and (< sampled-count limit)
                 (seq entries))
          (let [entry (first entries)
                key-bytes (estimate-ha-follower-value-bytes*
                           (key entry)
                           next-depth)
                val-bytes (estimate-ha-follower-value-bytes*
                           (val entry)
                           next-depth)]
            (recur (rest entries)
                   (unchecked-inc sampled-count)
                   (+ sample-bytes
                      (long key-bytes)
                      (long val-bytes))))
          (extrapolate-ha-follower-sampled-bytes
           32
           sample-bytes
           sampled-count
           total-count))))))

(defn- estimate-ha-follower-coll-bytes ^long
  [value ^long remaining-depth]
  (if (<= remaining-depth 0)
    (estimate-ha-follower-truncated-coll-bytes value)
    (let [limit (long ha-follower-adaptive-batch-value-sample-limit)
          next-depth (unchecked-dec remaining-depth)
          total-count (long (if (counted? value)
                              (count value)
                              -1))]
      (loop [items (seq value)
             sampled-count (long 0)
             sample-bytes (long 16)]
        (if (and (< sampled-count limit)
                 (seq items))
          (let [^long item-bytes (estimate-ha-follower-value-bytes*
                                  (first items)
                                  next-depth)]
            (recur (rest items)
                   (unchecked-inc sampled-count)
                   (+ sample-bytes item-bytes)))
          (extrapolate-ha-follower-sampled-bytes
           16
           sample-bytes
           sampled-count
           total-count))))))

(defn- estimate-ha-follower-value-bytes* ^long
  [value ^long remaining-depth]
  (cond
    (nil? value)
    1

    (string? value)
    (+ 16 (* 2 (long (count value))))

    (keyword? value)
    (let [name-len (long (count (name value)))
          ns-len   (if-let [ns (namespace value)]
                     (long (count ns))
                     0)]
      (+ 24 (* 2 (+ name-len ns-len))))

    (symbol? value)
    (let [name-len (long (count (name value)))
          ns-len   (if-let [ns (namespace value)]
                     (long (count ns))
                     0)]
      (+ 24 (* 2 (+ name-len ns-len))))

    (number? value)
    8

    (boolean? value)
    1

    (char? value)
    2

    (instance? java.util.UUID value)
    16

    (instance? ha-follower-adaptive-batch-byte-array-class value)
    (alength ^bytes value)

    (instance? java.nio.ByteBuffer value)
    (.remaining ^java.nio.ByteBuffer value)

    (map? value)
    (estimate-ha-follower-map-bytes value remaining-depth)

    (vector? value)
    (estimate-ha-follower-coll-bytes value remaining-depth)

    (sequential? value)
    (estimate-ha-follower-coll-bytes value remaining-depth)

    (coll? value)
    (estimate-ha-follower-coll-bytes value remaining-depth)

    :else
    (long ha-follower-adaptive-batch-value-overhead-bytes)))

(defn- estimate-ha-follower-value-bytes ^long
  [value]
  (estimate-ha-follower-value-bytes*
   value
   (long ha-follower-adaptive-batch-value-max-depth)))

(defn- estimate-ha-follower-record-bytes ^long
  [record]
  (if-let [payload-bytes (some-> (:payload-bytes record) long)]
    (long-max2 1 payload-bytes)
    (long-max2
     1
     (+ (long ha-follower-adaptive-batch-record-overhead-bytes)
        (long (estimate-ha-follower-value-bytes (:lsn record)))
        (long (estimate-ha-follower-value-bytes (:ha-term record)))
        (long (estimate-ha-follower-value-bytes (:rows record)))
        (long (estimate-ha-follower-value-bytes (:ops record)))))))

(defn- estimate-ha-follower-batch-bytes
  [records]
  (when (seq records)
    (let [sample (vec (take ha-follower-adaptive-batch-sample-limit records))
          sample-count (count sample)]
      (when (pos? sample-count)
        (let [sample-bytes (reduce
                            (fn [^long total record]
                              (+ total
                                 (long-max2 1
                                            (estimate-ha-follower-record-bytes
                                             record))))
                            0
                            sample)
              avg-record-bytes (long-max2
                                1
                                (long (Math/ceil
                                       (/ (double sample-bytes)
                                          (double sample-count)))))]
          (long (* avg-record-bytes (long (count records)))))))))

(defn- summarize-ha-follower-batch-record
  [record]
  (cond-> {:lsn (long (:lsn record))
           :row-count (count (:rows record))
           :op-count (count (:ops record))}
    (some? (:payload-bytes record))
    (assoc :payload-bytes (long (:payload-bytes record)))))

(defn- ha-follower-request-batch-records
  [m]
  (let [max-records (long-max2 1 (ha-follower-max-batch-records m))
        target-bytes (long-max2 1 (ha-follower-target-batch-bytes m))
        last-batch-size (long (or (:ha-follower-last-batch-size m) 0))
        last-batch-bytes (long (or (:ha-follower-last-batch-estimated-bytes m)
                                   0))
        avg-record-bytes (if (and (pos? last-batch-size)
                                  (pos? last-batch-bytes))
                           (long-max2 1
                                      (long (Math/ceil
                                             (/ (double last-batch-bytes)
                                                (double last-batch-size)))))
                           (long ha-follower-adaptive-batch-assumed-record-bytes))
        target-records (long-max2 1
                                  (quot (long target-bytes)
                                        avg-record-bytes))]
    (long-min2 max-records target-records)))

(def ^:private ha-follower-max-sync-backoff-ms 30000)

(defn- next-ha-follower-sync-backoff-ms
  [m]
  (let [renew-ms (long-max2 100
                            (or (:ha-lease-renew-ms m)
                                c/*ha-lease-renew-ms*))
        base-ms (long-max2 250 (quot renew-ms 2))
        prev-ms (long (or (:ha-follower-sync-backoff-ms m) 0))
        max-ms (long-max2 ha-follower-max-sync-backoff-ms
                          (unchecked-multiply (long 6) renew-ms))
        candidate (if (pos? prev-ms)
                    (unchecked-multiply (long 2) prev-ms)
                    base-ms)]
    (long-min2 max-ms candidate)))

(defn- ha-follower-gap-error?
  [e]
  (contains? #{:ha/txlog-gap
               :ha/txlog-source-behind
               :ha/txlog-source-authority-mismatch
               :ha/txlog-non-contiguous
               :ha/txlog-record-invalid-term
               :ha/txlog-gap-unresolved}
             (:error (ex-data e))))

(defn- ha-leader-endpoint
  [m lease]
  (or (:leader-endpoint lease)
      (some->> (:ha-members m)
               (filter #(= (:leader-node-id lease) (:node-id %)))
               first
               :endpoint)))

(defn- ha-default-follower-source-endpoints
  [m lease]
  (let [local-endpoint (:ha-local-endpoint m)
        leader-endpoint (ha-leader-endpoint m lease)
        ordered-members (ordered-ha-members m)
        fallback-endpoints
        (into []
              (comp
               (map :endpoint)
               (remove nil?)
               (remove s/blank?)
               (remove #(= % local-endpoint))
               (remove #(= % leader-endpoint)))
              ordered-members)]
    (->> (cond-> []
           (and (string? leader-endpoint)
                (not (s/blank? leader-endpoint))
                (not= leader-endpoint local-endpoint))
           (conj leader-endpoint)
           :always
           (into fallback-endpoints))
         distinct
         vec)))

(defn- reuse-ha-follower-source-order?
  [m]
  (and (true? (:ha-follower-source-order-dynamic? m))
       (vector? (:ha-follower-source-order m))
       (= (:ha-follower-source-order-authority-version m)
          (:ha-authority-version m))))

(defn- ha-follower-source-endpoints
  [m lease]
  (let [default-sources (ha-default-follower-source-endpoints m lease)]
    (if-not (reuse-ha-follower-source-order? m)
      default-sources
      (let [leader-endpoint (ha-leader-endpoint m lease)
            valid-endpoints (set default-sources)
            cached-tail
            (->> (:ha-follower-source-order m)
                 (filter valid-endpoints)
                 (remove #(= % leader-endpoint))
                 distinct
                 vec)
            merged
            (->> (concat
                  (when (and (string? leader-endpoint)
                             (not (s/blank? leader-endpoint))
                             (valid-endpoints leader-endpoint))
                    [leader-endpoint])
                  cached-tail
                  (remove (set cached-tail) default-sources))
                 (filter valid-endpoints)
                 distinct
                 vec)]
        (if (seq merged)
          merged
          default-sources)))))

(defn- ha-source-watermark-lsn
  [leader-endpoint source-endpoint watermark]
  (when (:reachable? watermark)
    (let [last-lsn (some-> (:last-applied-lsn watermark) long)
          txlog-lsn (some-> (or (:txlog-last-applied-lsn watermark)
                                (:last-applied-lsn watermark))
                            long)]
      (if (= source-endpoint leader-endpoint)
        (or last-lsn txlog-lsn)
        txlog-lsn))))

(defn- ha-source-authority-check
  [lease source-endpoint watermark]
  (let [leader-endpoint (:leader-endpoint lease)
        leader-node-id (:leader-node-id lease)
        lease-term (:term lease)]
    (cond
      (= source-endpoint leader-endpoint)
      {:ok? true}

      (not (:reachable? watermark))
      {:ok? false
       :reason :source-watermark-unreachable
       :watermark watermark}

      (not (:ha-runtime? watermark))
      {:ok? false
       :reason :source-not-ha-runtime
       :watermark watermark}

      (not (integer? leader-node-id))
      {:ok? false
       :reason :lease-missing-leader-node-id
       :leader-node-id leader-node-id}

      (not (integer? lease-term))
      {:ok? false
       :reason :lease-missing-term
       :lease-term lease-term}

      (not (integer? (:ha-authority-owner-node-id watermark)))
      {:ok? false
       :reason :source-missing-authority-owner-node-id
       :watermark watermark}

      (not (integer? (:ha-authority-term watermark)))
      {:ok? false
       :reason :source-missing-authority-term
       :watermark watermark}

      (not= (long leader-node-id)
            (long (:ha-authority-owner-node-id watermark)))
      {:ok? false
       :reason :source-authority-owner-mismatch
       :leader-node-id (long leader-node-id)
       :source-authority-owner-node-id
       (long (:ha-authority-owner-node-id watermark))
       :watermark watermark}

      (not= (long lease-term)
            (long (:ha-authority-term watermark)))
      {:ok? false
       :reason :source-authority-term-mismatch
       :lease-term (long lease-term)
       :source-authority-term (long (:ha-authority-term watermark))
       :watermark watermark}

      :else
      {:ok? true
       :watermark watermark})))

(defn- assert-ha-source-authority!
  [lease source-endpoint watermark]
  (let [check (ha-source-authority-check lease source-endpoint watermark)]
    (when-not (:ok? check)
      (u/raise "Follower txlog replay source is not aligned with current authority"
               {:error :ha/txlog-source-authority-mismatch
                :source-endpoint source-endpoint
                :leader-endpoint (:leader-endpoint lease)
                :leader-node-id (:leader-node-id lease)
                :lease-term (:term lease)
                :source-check (dissoc check :ok?)
                :watermark watermark}))))

(declare ha-leader-safe-lsn)

(defn- ha-gap-fallback-follower-probe
  [db-name m lease leader-endpoint leader-safe-lsn required-lsn
   {:keys [endpoint node-id]}]
  (try
    (let [watermark (safe-fetch-ha-endpoint-watermark-lsn
                     db-name m endpoint)
          authority-check (ha-source-authority-check
                           lease endpoint watermark)
          raw-last-lsn (ha-source-watermark-lsn
                        leader-endpoint
                        endpoint
                        watermark)
          last-lsn (when (some? raw-last-lsn)
                     (long-min2 raw-last-lsn
                                leader-safe-lsn))
          eligible? (and (:ok? authority-check)
                         (some? last-lsn)
                         (not (neg? (Long/compare
                                     (long last-lsn)
                                     (long required-lsn)))))]
      {:endpoint endpoint
       :node-id node-id
       :watermark watermark
       :authority-ok? (:ok? authority-check)
       :authority-check authority-check
       :last-applied-lsn last-lsn
       :raw-last-applied-lsn raw-last-lsn
       :leader-safe-lsn leader-safe-lsn
       :eligible? eligible?})
    (catch Throwable e
      {:endpoint endpoint
       :node-id node-id
       :watermark nil
       :authority-ok? false
       :authority-check {:ok? false
                         :reason :probe-exception
                         :message (ex-message e)
                         :data (ex-data e)}
       :last-applied-lsn nil
       :raw-last-applied-lsn nil
       :leader-safe-lsn leader-safe-lsn
       :eligible? false})))

(defn- ha-gap-fallback-follower-timeout
  [leader-safe-lsn {:keys [endpoint node-id]}]
  {:endpoint endpoint
   :node-id node-id
   :watermark nil
   :authority-ok? false
   :authority-check {:ok? false
                     :reason :probe-timeout}
   :last-applied-lsn nil
   :raw-last-applied-lsn nil
   :leader-safe-lsn leader-safe-lsn
   :eligible? false})

(defn- ha-gap-fallback-source-selection
  ([db-name m lease next-lsn]
   (ha-gap-fallback-source-selection
    db-name
    m
    lease
    next-lsn
    (fetch-leader-watermark-lsn db-name m lease)))
  ([db-name m lease next-lsn leader-watermark]
   (let [required-lsn (long (max 0 (dec (long next-lsn))))
         leader-safe-lsn (ha-leader-safe-lsn lease leader-watermark)
         local-endpoint (:ha-local-endpoint m)
         leader-endpoint (ha-leader-endpoint m lease)
         follower-members
         (->> (ordered-ha-members m)
              (filter (fn [{:keys [endpoint]}]
                        (and (string? endpoint)
                             (not (s/blank? endpoint))
                             (not= endpoint local-endpoint)
                             (not= endpoint leader-endpoint))))
              vec)
         followers
         (ha-parallel-mapv
          m
          (fn [member]
            (ha-gap-fallback-follower-probe
             db-name m lease leader-endpoint leader-safe-lsn required-lsn
             member))
          follower-members
          (fn [member]
            (ha-gap-fallback-follower-timeout leader-safe-lsn member)))
         eligible-followers
         (sort-by (juxt (comp - :last-applied-lsn) :node-id)
                  (filter :eligible? followers))
         unknown-followers
         (sort-by :node-id
                  (filter :authority-ok? (remove :eligible? followers)))
         source-endpoints
         (->> (concat (when (and (string? leader-endpoint)
                                 (not (s/blank? leader-endpoint))
                                 (not= leader-endpoint local-endpoint))
                        [leader-endpoint])
                      (map :endpoint eligible-followers)
                      (map :endpoint unknown-followers))
              distinct
              vec)
         source-watermarks
         (->> followers
              (filter :authority-ok?)
              (keep (fn [{:keys [endpoint watermark]}]
                      (when (some? watermark)
                        [endpoint watermark])))
              (into {}))]
     {:source-endpoints source-endpoints
      :source-watermarks source-watermarks})))

(defn- ha-gap-fallback-source-endpoints
  ([db-name m lease next-lsn]
   (:source-endpoints
    (ha-gap-fallback-source-selection db-name m lease next-lsn)))
  ([db-name m lease next-lsn leader-watermark]
   (:source-endpoints
    (ha-gap-fallback-source-selection
     db-name m lease next-lsn leader-watermark))))

(declare fetch-ha-leader-txlog-batch)
(declare assert-contiguous-lsn!)
(declare assert-ha-follower-record-terms!)

(defn- ha-leader-safe-lsn
  [lease leader-watermark]
  (let [authority-lsn (long (or (:leader-last-applied-lsn lease) 0))]
    (long-max2 authority-lsn
               (if (:reachable? leader-watermark)
                 (long (or (:last-applied-lsn leader-watermark) 0))
                 0))))

(defn- ha-source-advertised-last-applied-lsn
  ([db-name m lease source-endpoint]
   (ha-source-advertised-last-applied-lsn
    db-name
    m
    lease
    source-endpoint
    (fetch-leader-watermark-lsn db-name m lease)
    nil))
  ([db-name m lease source-endpoint leader-watermark]
   (ha-source-advertised-last-applied-lsn
    db-name
    m
    lease
    source-endpoint
    leader-watermark
    nil))
  ([db-name m lease source-endpoint leader-watermark source-watermark]
   (let [leader-endpoint (ha-leader-endpoint m lease)
         authority-lsn (long (or (:leader-last-applied-lsn lease) 0))
         leader-safe-lsn (ha-leader-safe-lsn lease leader-watermark)
         watermark (or source-watermark
                       (if (= source-endpoint leader-endpoint)
                         leader-watermark
                         (safe-fetch-ha-endpoint-watermark-lsn
                          db-name m source-endpoint)))]
     (if (= source-endpoint leader-endpoint)
       (let [remote-lsn (ha-source-watermark-lsn
                         leader-endpoint
                         source-endpoint
                         watermark)]
         {:known? (or (some? remote-lsn)
                      (pos? authority-lsn))
          ;; Use the source's fresh watermark when we have it. The cached lease
          ;; observation can be ahead of what the leader currently serves during
          ;; follower catch-up, and treating that stale authority LSN as
          ;; authoritative turns speculative cursor overshoot into a false gap.
          :last-applied-lsn (when (or (some? remote-lsn)
                                      (pos? authority-lsn))
                              (long (or remote-lsn authority-lsn)))
          :authority-last-applied-lsn authority-lsn
          :watermark watermark})
       (let [raw-last-lsn (ha-source-watermark-lsn
                           leader-endpoint
                           source-endpoint
                           watermark)
             last-lsn (when (some? raw-last-lsn)
                        (long-min2 raw-last-lsn leader-safe-lsn))]
         {:known? (some? last-lsn)
          :last-applied-lsn last-lsn
          :raw-last-applied-lsn raw-last-lsn
          :leader-safe-lsn leader-safe-lsn
          :watermark watermark})))))

(defn- ^:redef fetch-ha-follower-records-with-gap-fallback
  [db-name m lease next-lsn upto-lsn]
  (let [sources (ha-follower-source-endpoints m lease)
        leader-endpoint (ha-leader-endpoint m lease)
        cached-source-order? (reuse-ha-follower-source-order? m)
        leader-watermark* (delay (fetch-leader-watermark-lsn
                                  db-name m lease))]
    (loop [remaining sources
           source-order sources
           reordered? cached-source-order?
           source-watermarks {}
           gap-errors []]
      (if-let [source-endpoint (first remaining)]
        (let [attempt
              (try
                (let [leader-source? (= source-endpoint leader-endpoint)
                      source-watermark (when-not leader-source?
                                         (or (get source-watermarks
                                                  source-endpoint)
                                             (safe-fetch-ha-endpoint-watermark-lsn
                                              db-name m source-endpoint)))
                      _ (when source-watermark
                          (assert-ha-source-authority!
                           lease source-endpoint source-watermark))
                      records (vec (or (fetch-ha-leader-txlog-batch
                                        db-name
                                        m
                                        source-endpoint
                                        next-lsn
                                        upto-lsn)
                                       []))]
                  (if (seq records)
                    (let [last-record-term
                          (do
                            (assert-contiguous-lsn! next-lsn records)
                            (assert-ha-follower-record-terms!
                             lease source-endpoint records
                             (:ha-follower-last-applied-term m)))]
                      {:ok? true
                       :value {:source-endpoint source-endpoint
                               :records records
                               :last-record-term last-record-term
                               :source-order source-order
                               :source-order-dynamic? reordered?
                               :source-last-applied-lsn-known? false
                               :source-last-applied-lsn nil}})
                    (let [{:keys [known? last-applied-lsn]}
                          (ha-source-advertised-last-applied-lsn
                           db-name
                           m
                           lease
                           source-endpoint
                           @leader-watermark*
                           source-watermark)
                          source-last-applied-lsn
                          (long (or last-applied-lsn 0))
                          local-last-applied-lsn
                          (long (max 0
                                     (long (or (:ha-local-last-applied-lsn m)
                                               0))))
                          speculative-cursor?
                          (> (long next-lsn)
                             (unchecked-inc local-last-applied-lsn))]
                      (cond
                        (and known?
                             (< source-last-applied-lsn
                                (long (max 0
                                           (dec (long next-lsn))))))
                        (if (and speculative-cursor?
                                 (>= source-last-applied-lsn
                                     local-last-applied-lsn))
                          ;; The tracked replay cursor got ahead of what the
                          ;; chosen source can currently prove, but the source
                          ;; is still caught up through the follower's
                          ;; authoritative local floor. Treat this as a stale
                          ;; speculative cursor so the caller clamps back to
                          ;; `(inc local-last-applied-lsn)` instead of
                          ;; triggering snapshot bootstrap.
                          {:ok? true
                           :value {:source-endpoint source-endpoint
                                   :records records
                                   :source-order source-order
                                   :source-order-dynamic? reordered?
                                   :source-last-applied-lsn-known? true
                                   :source-last-applied-lsn
                                   source-last-applied-lsn}}
                          {:ok? false
                           :gap-error
                           {:source-endpoint source-endpoint
                            :message
                            "Follower txlog replay source is behind local cursor"
                            :data {:error :ha/txlog-source-behind
                                   :expected-lsn (long next-lsn)
                                   :actual-lsn nil
                                   :source-last-applied-lsn
                                   source-last-applied-lsn}}})

                        (and known?
                             (>= source-last-applied-lsn
                                 (long next-lsn)))
                        {:ok? false
                         :gap-error
                         {:source-endpoint source-endpoint
                          :message
                          "Follower txlog replay detected empty source gap"
                          :data {:error :ha/txlog-gap
                                 :expected-lsn (long next-lsn)
                                 :actual-lsn nil
                                 :source-last-applied-lsn
                                 source-last-applied-lsn}}}

                        reordered?
                        {:ok? false
                         :skip? true}

                        :else
                        {:ok? true
                         :value {:source-endpoint source-endpoint
                                 :records records
                                 :source-order source-order
                                 :source-order-dynamic? reordered?
                                 :source-last-applied-lsn-known? known?
                                 :source-last-applied-lsn
                                 (when known?
                                   (long (or last-applied-lsn 0)))}}))))
                (catch Exception e
                  (if (ha-follower-gap-error? e)
                    {:ok? false
                     :gap-error {:source-endpoint source-endpoint
                                 :message (ex-message e)
                                 :data (ex-data e)}}
                    (if reordered?
                      {:ok? false
                       :gap-error {:source-endpoint source-endpoint
                                   :message (ex-message e)
                                   :data (assoc (or (ex-data e) {})
                                                :error
                                                (or (:error (ex-data e))
                                                    :ha/txlog-source-unavailable))}}
                      (throw e)))))]
          (cond
            (:ok? attempt)
            (:value attempt)

            (:skip? attempt)
            (recur (rest remaining)
                   source-order
                   reordered?
                   source-watermarks
                   gap-errors)

            :else
            (let [{fallback-sources :source-endpoints
                   prefetched-source-watermarks :source-watermarks}
                  (when-not reordered?
                    (ha-gap-fallback-source-selection
                     db-name
                     m
                     lease
                     next-lsn
                     @leader-watermark*))
                  remaining' (if reordered?
                               (rest remaining)
                               (->> fallback-sources
                                    (remove #{source-endpoint})
                                    vec))
                  source-order' (if reordered?
                                  source-order
                                  (vec (cons source-endpoint remaining')))]
              (recur remaining'
                     source-order'
                     true
                     (if reordered?
                       source-watermarks
                       (merge source-watermarks
                              prefetched-source-watermarks))
                     (conj gap-errors (:gap-error attempt))))))
        (u/raise "Follower txlog replay gap unresolved across deterministic sources"
                 {:error :ha/txlog-gap-unresolved
                  :expected-lsn next-lsn
                  :upto-lsn upto-lsn
                  :source-order source-order
                  :source-order-dynamic? reordered?
                  :gap-errors gap-errors})))))

(defn ^:redef fetch-ha-leader-txlog-batch
  [db-name m leader-endpoint from-lsn upto-lsn]
  (if-let [uri (ha-endpoint-uri db-name leader-endpoint m)]
    (let [client-opts
          {:pool-size 1
           :time-out (ha-request-timeout-ms m 10000)}]
      (cache/with-cached-ha-client
        m uri db-name client-opts
        (fn [client]
          (cache/ha-client-request
           client
           :open-tx-log-rows
           [db-name (long from-lsn) (long upto-lsn)]
           false))))
    (u/raise "Invalid HA leader endpoint for txlog fetch"
             {:error :ha/follower-invalid-leader-endpoint
              :leader-endpoint leader-endpoint})))

(defn- assert-contiguous-lsn!
  [expected-from records]
  (let [expected-from (long expected-from)]
    (when (seq records)
      (let [first-lsn (long (:lsn (first records)))]
        (when (not= first-lsn expected-from)
          (u/raise "Follower txlog replay detected LSN gap"
                   {:error :ha/txlog-gap
                    :expected-lsn expected-from
                    :actual-lsn first-lsn})))
      (loop [prev expected-from
             rs (rest records)]
        (when-let [record (first rs)]
          (let [actual (long (:lsn record))
                want (unchecked-inc (long prev))]
            (when (not= actual want)
              (u/raise "Follower txlog replay detected non-contiguous LSN"
                       {:error :ha/txlog-non-contiguous
                        :expected-lsn want
                        :actual-lsn actual}))
            (recur actual (rest rs))))))))

(defn- assert-ha-follower-record-terms!
  ([lease source-endpoint records]
   (assert-ha-follower-record-terms! lease source-endpoint records nil))
  ([lease source-endpoint records last-applied-term]
   (when-some [lease-term* (:term lease)]
     (let [lease-term (long lease-term*)
           last-applied-term (some-> last-applied-term long)]
       (reduce
         (fn [prev-term record]
           (if-some [record-term* (:ha-term record)]
             (let [record-term (long record-term*)]
               (when-not (pos? record-term)
                 (u/raise "Follower txlog replay record has invalid HA term"
                          {:error           :ha/txlog-record-invalid-term
                           :source-endpoint source-endpoint
                           :lease-term      lease-term
                           :record-lsn      (:lsn record)
                           :record-term     record-term}))
               ;; Catch-up can legitimately span multiple committed leadership
               ;; terms after failover, so older record terms remain valid
               ;; until the follower has actually replayed past them. Once the
               ;; local committed prefix has advanced through a later term,
               ;; later batches must not fall back below that term even if the
               ;; sync source changes.
               (when (> record-term lease-term)
                 (u/raise "Follower txlog replay record term is ahead of current lease"
                          {:error           :ha/txlog-record-invalid-term
                           :source-endpoint source-endpoint
                           :lease-term      lease-term
                           :record-lsn      (:lsn record)
                           :record-term     record-term}))
               (when (and prev-term
                          (< record-term (long prev-term)))
                 (u/raise "Follower txlog replay record terms regressed"
                          {:error                :ha/txlog-record-invalid-term
                           :source-endpoint      source-endpoint
                           :lease-term           lease-term
                           :previous-record-term prev-term
                           :record-lsn           (:lsn record)
                           :record-term          record-term}))
               record-term)
             prev-term))
         last-applied-term
         records)))))

(defn- assert-ha-follower-record-term!
  [m record]
  (when-some [record-term* (:ha-term record)]
    (let [record-term (long record-term*)]
      (when-not (pos? record-term)
        (u/raise "Follower txlog replay record has invalid HA term"
                 {:error :ha/txlog-record-invalid-term
                  :record-lsn (:lsn record)
                  :record-term record-term}))
      (when-some [authority-term* (:ha-authority-term m)]
        (let [authority-term (long authority-term*)]
          ;; Single-record apply shares the same rule as batch validation:
          ;; historical committed records may trail the current authority term,
          ;; but replay must never advance into a future term.
          (when (> record-term authority-term)
            (u/raise "Follower txlog replay record term is ahead of current authority"
                     {:error :ha/txlog-record-invalid-term
                      :record-lsn (:lsn record)
                      :record-term record-term
                      :authority-term authority-term})))))))

(defn- advance-store-max-tx-to-target!
  ([store target-max-tx]
   (advance-store-max-tx-to-target!
    #(long (i/max-tx store))
    #(i/advance-max-tx store)
    target-max-tx))
  ([read-max-tx! advance-max-tx! target-max-tx]
   (let [target-max-tx (long target-max-tx)]
     (loop [cur (long (read-max-tx!))]
       (when (< cur target-max-tx)
         (advance-max-tx!)
         (let [next-cur (long (read-max-tx!))]
           (when (<= next-cur cur)
             (u/raise "HA follower max-tx sync failed to make progress"
                      {:error :ha/follower-max-tx-stalled
                       :current-max-tx cur
                       :next-max-tx next-cur
                       :target-max-tx target-max-tx}))
           (recur next-cur)))))))

(defn ^:redef apply-ha-follower-txlog-record!
  [m record]
  (let [store       (:store m)
        kv-store    (raw-local-kv-store m)
        rows        (:rows record)
        ops         (:ops record)
        replay-rows (cond
                      (sequential? rows) (vec rows)
                      (sequential? ops)  (vec ops)
                      :else              nil)]
    (when-not kv-store
      (u/raise "Follower txlog replay requires a local KV store"
               {:error :ha/follower-missing-store}))
    (assert-ha-follower-record-term! m record)
    (cond
      replay-rows
      (let [schema-overrides
            (reduce
              (fn [overrides [op dbi attr props]]
                (if (= dbi c/schema)
                  (case op
                    :put (assoc overrides attr props)
                    :del (dissoc overrides attr)
                    overrides)
                  overrides))
              {}
              replay-rows)
            replayed-max-gt
            (reduce
              (fn [^long acc [op dbi k]]
                (if (and (= op :put)
                         (= dbi c/giants)
                         (integer? k))
                  (long-max2 acc (unchecked-inc (long k)))
                  acc))
              0
              replay-rows)
            next-state
            (do
              (kv/mirror-replayed-txlog-record! kv-store record)
              (when (and (instance? IStore store)
                         (pos? (long replayed-max-gt)))
                (st/sync-max-gt-floor! store replayed-max-gt))
              (if (and (instance? IStore store)
                       (seq schema-overrides))
                (let [reopen-info (ha-local-store-reopen-info m)]
                  (with-ha-local-store-swap
                    (fn []
                      (close-ha-local-store! m)
                      (try
                        (reopen-ha-local-store-from-info m reopen-info)
                        (catch Throwable e
                          (u/raise
                           "HA follower schema replay failed to reopen local store"
                           {:error       :ha/follower-schema-reopen-failed
                            :record-lsn  (:lsn record)
                            :reopen-info reopen-info
                            :message     (ex-message e)
                            :data        (ex-data e)
                            :state       (-> m
                                             (assoc ha-local-store-reopen-info-key
                                                    reopen-info
                                                    :dt-db nil)
                                             (dissoc :engine :index))}))))))
                 m))
            readback-kv-store (raw-local-kv-store next-state)
            probe-eid         (some->> replay-rows
                                       (keep (fn [[op dbi k]]
                                               (when (and (= op :put)
                                                          (= dbi c/eav)
                                                          (integer? k))
                                                 (long k))))
                                       first)
            readback
            (when readback-kv-store
              {:lsn         (long (:lsn record))
               :payload-lsn (read-ha-local-payload-lsn readback-kv-store)
               :meta-max-tx (long (or (try
                                        (i/get-value readback-kv-store
                                                     c/meta
                                                     :max-tx
                                                     :attr
                                                     :long)
                                        (catch Throwable _
                                          nil))
                                      0))
               :probe-eid   probe-eid
               :probe-eav-range
               (when probe-eid
                 (try
                   (vec (i/get-range readback-kv-store
                                     c/eav
                                     [probe-eid]
                                     :id
                                     :avg))
                   (catch Throwable _
                     nil)))})
            next-state        (assoc next-state
                                     :ha-follower-last-apply-readback readback)]
        (when (instance? IStore (:store next-state))
          (when-let [target-max-tx
                     (some->> replay-rows
                              (keep (fn [[op dbi k v]]
                                      (when (and (= op :put)
                                                 (= dbi c/meta)
                                                 (= k :max-tx)
                                                 (integer? v))
                                        (long v))))
                              last)]
            (advance-store-max-tx-to-target!
             (:store next-state)
             (long target-max-tx))))
        next-state)

      :else
      (u/raise "Follower txlog replay record is missing rows"
               {:error  :ha/follower-invalid-record
                :record record}))))

(def ^:dynamic *ha-follower-apply-record-fn* nil)

(defn- ha-follower-stale-state-error?
  [e]
  (= :ha/follower-stale-state
     (or (:error (ex-data e))
         (:type (ex-data e)))))

(defn ^:redef report-ha-replica-floor!
  [db-name m leader-endpoint applied-lsn]
  (if-let [uri (ha-endpoint-uri db-name leader-endpoint m)]
    (let [client-opts
          {:pool-size 1
           :time-out (ha-request-timeout-ms m 10000)}]
      (cache/with-cached-ha-client
        m uri db-name client-opts
        (fn [client]
          (cache/ha-client-request
           client
           :txlog-update-replica-floor!
           [db-name (:ha-node-id m) (long applied-lsn)]
           false))))
    (u/raise "Invalid HA leader endpoint for replica-floor update"
             {:error :ha/follower-invalid-leader-endpoint
              :leader-endpoint leader-endpoint
              :applied-lsn applied-lsn})))

(defn ^:redef clear-ha-replica-floor!
  [db-name m leader-endpoint]
  (if-let [uri (ha-endpoint-uri db-name leader-endpoint m)]
    (let [client-opts
          {:pool-size 1
           :time-out (long (max 500
                                (min 10000
                                     (long (or (:ha-lease-renew-ms m)
                                               c/*ha-lease-renew-ms*)))))}]
      (cache/with-cached-ha-client
        m uri db-name client-opts
        (fn [client]
          (cache/ha-client-request
           client
           :txlog-clear-replica-floor!
           [db-name (:ha-node-id m)]
           false))))
    (u/raise "Invalid HA leader endpoint for replica-floor clear"
             {:error :ha/follower-invalid-leader-endpoint
              :leader-endpoint leader-endpoint})))

(defn- ha-replica-floor-reset-required?
  [e]
  (boolean
   (some
    (fn [cause]
      (let [data (ex-data cause)
            err-data (:err-data data)
            type* (or (:type err-data) (:type data))
            old-lsn (or (:old-lsn data) (:old-lsn err-data))
            new-lsn (or (:new-lsn data) (:new-lsn err-data))
            message (ex-message cause)]
        (and (= :txlog/invalid-floor-provider-state type*)
             (or (and (integer? old-lsn)
                      (integer? new-lsn)
                      (< (long new-lsn) (long old-lsn)))
                 (and (string? message)
                      (s/includes? message
                                   "Replica floor LSN cannot move backward"))))))
    (take-while some? (iterate ex-cause e)))))

(defn- ha-replica-floor-transport-failure?
  [e]
  (boolean
   (some
    (fn [cause]
      (let [message (ex-message cause)]
        (or (instance? ClosedChannelException cause)
            (instance? ConnectException cause)
            (and (string? message)
                 (or (s/includes? message "Socket channel is closed.")
                     (s/includes? message "ClosedChannelException")
                     (s/includes? message "Unable to connect to server:")
                     (s/includes? message "Connection refused")
                     (s/includes? message "Connection reset by peer")
                     (s/includes? message "Broken pipe"))))))
    (take-while some? (iterate ex-cause e)))))

(defn ^:redef fetch-ha-endpoint-snapshot-copy!
  [db-name m endpoint dest-dir]
  (if-let [uri (ha-endpoint-uri db-name endpoint m)]
    (let [timeout-ms (long (max 1000
                                (min 60000
                                     (* 4
                                        (long (or (:ha-lease-renew-ms m)
                                                  c/*ha-lease-renew-ms*))))))
          remote-store (open-ha-snapshot-remote-store!
                        uri
                        {:client-opts {:pool-size 1
                                       :time-out timeout-ms}})]
      (try
        (let [copy-meta (copy-ha-remote-store! remote-store dest-dir false)]
          (when-let [pin-id (get-in copy-meta [:backup-pin :pin-id])]
            (try
              (unpin-ha-remote-store-backup-floor! remote-store pin-id)
              (catch Exception e
                (log/debug e
                           "Best-effort HA snapshot-copy backup pin cleanup failed"
                           {:db-name db-name
                            :source-endpoint endpoint
                            :pin-id pin-id}))))
          {:copy-meta copy-meta})
        (finally
          (close-ha-snapshot-remote-store! remote-store))))
    (u/raise "Invalid HA endpoint for snapshot copy"
             {:error :ha/follower-invalid-snapshot-endpoint
              :db-name db-name
              :source-endpoint endpoint})))

(defn- validate-ha-snapshot-copy!
  [db-name m source-endpoint snapshot-dir copy-meta required-lsn]
  (let [snapshot-db-name (:db-name copy-meta)
        snapshot-db-identity (:db-identity copy-meta)
        snapshot-last-lsn (:snapshot-last-applied-lsn copy-meta)
        payload-last-lsn (:payload-last-applied-lsn copy-meta)]
    (when (not= db-name snapshot-db-name)
      (u/raise "HA snapshot copy DB name mismatch"
               {:error :ha/follower-snapshot-db-name-mismatch
                :db-name db-name
                :snapshot-db-name snapshot-db-name
                :source-endpoint source-endpoint}))
    (when (or (nil? snapshot-db-identity) (s/blank? snapshot-db-identity))
      (u/raise "HA snapshot copy is missing DB identity"
               {:error :ha/follower-snapshot-missing-db-identity
                :db-name db-name
                :source-endpoint source-endpoint}))
    (when (not= (:ha-db-identity m) snapshot-db-identity)
      (u/raise "HA snapshot copy DB identity mismatch"
               {:error :ha/follower-snapshot-db-identity-mismatch
                :db-name db-name
                :local-db-identity (:ha-db-identity m)
                :snapshot-db-identity snapshot-db-identity
                :source-endpoint source-endpoint}))
    (when-not (or (integer? snapshot-last-lsn)
                  (integer? payload-last-lsn))
      (u/raise "HA snapshot copy is missing payload last applied LSN"
               {:error :ha/follower-snapshot-missing-last-applied-lsn
                :db-name db-name
                :source-endpoint source-endpoint
                :copy-meta copy-meta}))
    (let [snapshot-last-lsn (when (integer? snapshot-last-lsn)
                              (long snapshot-last-lsn))
          payload-last-lsn (when (integer? payload-last-lsn)
                             (long payload-last-lsn))
          install-last-lsn (long-max2 (or payload-last-lsn 0)
                                      (or snapshot-last-lsn 0))]
      (when (< install-last-lsn (long required-lsn))
        (u/raise "HA snapshot copy payload is older than the required follower floor"
                 {:error :ha/follower-snapshot-too-stale
                  :db-name db-name
                  :required-lsn (long required-lsn)
                  :snapshot-last-applied-lsn snapshot-last-lsn
                  :payload-last-applied-lsn payload-last-lsn
                  :source-endpoint source-endpoint}))
      {:db-name db-name
       :db-identity snapshot-db-identity
       :snapshot-last-applied-lsn (or snapshot-last-lsn install-last-lsn)
       :payload-last-applied-lsn install-last-lsn})))

(defn ^:redef install-ha-local-snapshot!
  [m snapshot-dir]
  (let [store (:store m)]
    (if-not (instance? IStore store)
      {:ok? false
       :state m
       :error {:error :ha/follower-missing-store
               :message "HA follower snapshot install requires a local store"}}
      (let [env-dir (i/dir store)
            backup-dir (str env-dir ".ha-backup-" (UUID/randomUUID))
            stage-dir (str env-dir ".ha-install-" (UUID/randomUUID))
            install-marker {:backup-dir backup-dir
                            :stage-dir stage-dir
                            :db-name (some-> store i/db-name)
                            :created-at-ms (System/currentTimeMillis)}
            open-opts (ha-snapshot-open-opts
                       m
                       (:db-name (i/opts store))
                       (:ha-db-identity m))]
        ;; Validate that the copied snapshot is openable before swapping paths.
        (let [snapshot-store (st/open snapshot-dir nil open-opts)]
          (try
            (i/opts snapshot-store)
            (finally
              (i/close snapshot-store))))
        (with-ha-local-store-swap
          (fn []
            (try
              (close-ha-local-store! m)
              (when (u/file-exists backup-dir)
                (u/delete-files backup-dir))
              (when (u/file-exists stage-dir)
                (u/delete-files stage-dir))
              (write-ha-snapshot-install-marker!
               env-dir
               (assoc install-marker :stage :backup-moving))
              (move-path! env-dir backup-dir)
              (write-ha-snapshot-install-marker!
               env-dir
               (assoc install-marker :stage :backup-moved))
              (copy-dir-contents! snapshot-dir stage-dir)
              (sync-ha-snapshot-install-target! stage-dir)
              (write-ha-snapshot-install-marker!
               env-dir
               (assoc install-marker :stage :snapshot-staged))
              (move-path! stage-dir env-dir)
              (sync-ha-snapshot-install-target! env-dir)
              (let [new-store (open-ha-store-dbis!
                               (st/open env-dir nil open-opts))
                    new-db (db/new-db new-store)]
                (delete-ha-snapshot-install-marker! env-dir)
                (when (u/file-exists backup-dir)
                  (u/delete-files backup-dir))
                {:ok? true
                 :state (-> m
                            clear-ha-local-store-transient-state
                            (assoc :store new-store
                                   :dt-db new-db))})
              (catch Exception e
                (let [log-context {:db-name (some-> store i/db-name)
                                   :env-dir env-dir}]
                  (try
                    (recover-ha-local-snapshot-install! env-dir)
                    (log/warn e
                              "HA follower snapshot install failed; restored local store"
                              log-context)
                    (let [restored-store (open-ha-store-dbis!
                                          (st/open env-dir nil open-opts))
                          restored-db (db/new-db restored-store)]
                      {:ok? false
                       :state (-> m
                                  clear-ha-local-store-transient-state
                                  (assoc :store restored-store
                                         :dt-db restored-db))
                       :error {:error :ha/follower-snapshot-install-failed
                               :message (ex-message e)
                               :data (ex-data e)}})
                    (catch Exception restore-e
                      (log/error restore-e
                                 "HA follower snapshot install restore failed"
                                 (merge log-context
                                        {:install-message (ex-message e)
                                         :install-data (ex-data e)}))
                      {:ok? false
                       :state (-> m
                                  ;; Preserve the closed store handle so the next
                                  ;; renew step can recover and reopen from its
                                  ;; original env dir instead of getting stuck with
                                  ;; no local store at all.
                                  clear-ha-local-store-transient-state
                                  (assoc :store store
                                         :dt-db nil))
                       :error {:error :ha/follower-snapshot-install-restore-failed
                               :message (ex-message e)
                               :data (merge (or (ex-data e) {})
                                            {:restore-message (ex-message restore-e)
                                             :restore-data (ex-data restore-e)})}})))))))))))

(defn- sync-ha-follower-batch
  [db-name m lease next-lsn now-ms]
  (let [m (reopen-ha-local-store-if-needed m)
        leader-endpoint (ha-leader-endpoint m lease)
        local-node-id (:ha-node-id m)
        requested-batch-records (long (ha-follower-request-batch-records m))]
    (when (or (nil? leader-endpoint) (s/blank? leader-endpoint))
      (u/raise "HA follower is missing leader endpoint for txlog sync"
               {:error :ha/follower-missing-leader-endpoint
                :lease lease}))
    (let [upto-lsn (long (+ (long next-lsn)
                            (dec requested-batch-records)))
          {:keys [records last-record-term source-endpoint source-order
                  source-order-dynamic?
                  source-last-applied-lsn-known?
                  source-last-applied-lsn]}
          (fetch-ha-follower-records-with-gap-fallback
           db-name m lease next-lsn upto-lsn)
          apply-record-fn (or *ha-follower-apply-record-fn*
                              apply-ha-follower-txlog-record!)
          next-m (reduce apply-record-fn m records)
          _ (when (and (seq records)
                       (instance? IStore (:store next-m)))
              ;; Follower replay writes raw KV rows and bypasses the normal
              ;; datalog transaction path, so clear any cached query misses
              ;; before publishing a refreshed dt-db view.
              (db/refresh-cache (:store next-m)))
          last-record-lsn (when-let [record (peek records)]
                            (long (:lsn record)))
          ;; Empty batches prove only that the chosen source returned no new
          ;; replicated rows. Preserve the tracked replay cursor only when the
          ;; source explicitly advertised that it is caught up through
          ;; `(dec next-lsn)`; otherwise fall back to the local floor.
          current-local-floor-lsn
          (long (max 0
                     (long (or (:ha-local-last-applied-lsn next-m)
                               (:ha-local-last-applied-lsn m)
                               0))))
          applied-lsn (long (or last-record-lsn
                                current-local-floor-lsn))
          next-fetch-lsn
          (long
           (if (seq records)
             (unchecked-inc applied-lsn)
             (let [leader-last-applied-lsn
                   (long (or (:leader-last-applied-lsn lease) 0))
                   empty-batch-proven-cursor?
                   (and (true? source-last-applied-lsn-known?)
                        (= (long (or source-last-applied-lsn 0))
                           (long (max 0 (dec (long next-lsn))))))
                   preserve-tracked-cursor?
                   (and empty-batch-proven-cursor?
                        (> (long next-lsn)
                           (long (unchecked-inc current-local-floor-lsn)))
                        (> leader-last-applied-lsn
                           current-local-floor-lsn))]
               (if preserve-tracked-cursor?
                 (long next-lsn)
                 (unchecked-inc applied-lsn)))))
          batch-estimated-bytes (estimate-ha-follower-batch-bytes records)
          next-m (if (seq records)
                   (maybe-persist-ha-follower-local-applied-lsn
                    next-m applied-lsn now-ms)
                   next-m)]
      (try
        (try
          (report-ha-replica-floor!
           db-name next-m leader-endpoint applied-lsn)
          (catch Exception e
            (if (ha-replica-floor-reset-required? e)
              (do
                (log/info "HA follower cleared stale leader replica floor after local reset"
                          {:db-name db-name
                           :ha-node-id local-node-id
                           :leader-endpoint leader-endpoint
                           :applied-lsn applied-lsn})
                (clear-ha-replica-floor! db-name next-m leader-endpoint)
                (report-ha-replica-floor!
                 db-name next-m leader-endpoint applied-lsn))
              (throw e))))
        (catch Exception e
          (if (ha-replica-floor-transport-failure? e)
            (log/debug "HA follower skipped replica-floor update because the leader endpoint is unavailable"
                       {:db-name db-name
                        :ha-node-id local-node-id
                        :leader-endpoint leader-endpoint
                        :applied-lsn applied-lsn
                        :message (ex-message e)})
            (log/warn e "HA follower failed to update leader replica floor"
                      {:db-name db-name
                       :ha-node-id local-node-id
                       :leader-endpoint leader-endpoint
                       :applied-lsn applied-lsn}))))
      {:records records
       :applied-lsn applied-lsn
       :leader-endpoint leader-endpoint
       :source-endpoint source-endpoint
       :source-order source-order
       :state (-> (assoc next-m
                         :ha-local-last-applied-lsn applied-lsn
                         :ha-follower-last-applied-term
                         (or last-record-term
                             (:ha-follower-last-applied-term m))
                         :ha-follower-requested-batch-records
                         requested-batch-records
                         :ha-follower-last-batch-records
                         (when (seq records)
                           (mapv summarize-ha-follower-batch-record
                                 records))
                         :ha-follower-last-batch-estimated-bytes
                         (or batch-estimated-bytes
                             (:ha-follower-last-batch-estimated-bytes m))
                         :ha-follower-next-lsn next-fetch-lsn
                         :ha-follower-last-batch-size (count records)
                         :ha-follower-last-sync-ms now-ms
                         :ha-follower-leader-endpoint leader-endpoint
                         :ha-follower-source-endpoint source-endpoint
                         :ha-follower-source-order source-order
                         :ha-follower-source-order-dynamic?
                         source-order-dynamic?
                         :ha-follower-source-order-authority-version
                         (:ha-authority-version next-m)
                         :ha-follower-sync-backoff-ms nil
                         :ha-follower-next-sync-not-before-ms nil
                         :ha-follower-degraded? nil
                         :ha-follower-degraded-reason nil
                         :ha-follower-degraded-details nil
                         :ha-follower-degraded-since-ms nil
                         :ha-follower-last-error nil
                         :ha-follower-last-error-details nil
                         :ha-follower-last-error-ms nil)
                  refresh-ha-local-dt-db)})))

(defn- ^:redef bootstrap-ha-follower-from-snapshot
  [db-name m lease source-order next-lsn now-ms]
  (let [required-lsn (long (max 0 (dec (long next-lsn))))]
    (loop [remaining source-order
           current-m (normalize-ha-bootstrap-retry-state
                      m m (ha-local-store-reopen-info m))
           current-reopen-info (ha-local-store-reopen-info m)
           errors []]
      (if-let [source-endpoint (first remaining)]
        (let [current-m (normalize-ha-bootstrap-retry-state
                         current-m current-m current-reopen-info)
              current-reopen-info (or (ha-local-store-reopen-info current-m)
                                      current-reopen-info)
              snapshot-dir (u/tmp-dir (str "ha-snapshot-copy-"
                                           (UUID/randomUUID)))
              attempt
              (try
                (let [{:keys [copy-meta]}
                      (fetch-ha-endpoint-snapshot-copy!
                       db-name current-m source-endpoint snapshot-dir)
                      manifest
                      (validate-ha-snapshot-copy!
                       db-name current-m source-endpoint snapshot-dir
                       copy-meta required-lsn)
                      install-res
                      (install-ha-local-snapshot! current-m snapshot-dir)]
                  (if (:ok? install-res)
                    (let [installed-state (:state install-res)
                          snapshot-lsn
                          (long (max 0
                                     (long (or (:snapshot-last-applied-lsn
                                                manifest)
                                               0))
                                     (long (if-let [kv-store
                                                    (raw-local-kv-store
                                                     installed-state)]
                                             (read-ha-local-snapshot-current-lsn
                                              kv-store)
                                             0))))
                          {:keys [state installed-lsn]}
                          (reconcile-ha-installed-snapshot-state
                           installed-state
                           snapshot-lsn)
                          resume-next-lsn (unchecked-inc (long installed-lsn))
                          _ (persist-ha-local-applied-lsn!
                             state
                             installed-lsn)
                          installed-state
                          (-> state
                              (assoc
                               :ha-local-last-applied-lsn installed-lsn
                               :ha-local-persisted-lsn installed-lsn
                               :ha-local-last-persisted-applied-ms
                               (long now-ms)
                               :ha-follower-last-applied-term nil
                               :ha-follower-batches-since-persist 0
                               :ha-follower-next-lsn resume-next-lsn
                               :ha-follower-last-bootstrap-ms now-ms
                               :ha-follower-bootstrap-source-endpoint
                               source-endpoint
                               :ha-follower-bootstrap-snapshot-last-applied-lsn
                               snapshot-lsn))]
                      (try
                        (let [sync-res (sync-ha-follower-batch
                                        db-name installed-state lease
                                        resume-next-lsn now-ms)
                              next-state (-> (:state sync-res)
                                             (assoc
                                              :ha-follower-last-bootstrap-ms
                                              now-ms
                                              :ha-follower-bootstrap-source-endpoint
                                              source-endpoint
                                              :ha-follower-bootstrap-snapshot-last-applied-lsn
                                              snapshot-lsn))]
                          {:ok? true
                           :state next-state})
                        (catch Exception e
                          {:ok? false
                           :state installed-state
                           :error {:error (or (:error (ex-data e))
                                              :ha/follower-snapshot-resume-failed)
                                   :message (ex-message e)
                                   :data (merge
                                          (or (ex-data e) {})
                                          {:snapshot-last-applied-lsn
                                           snapshot-lsn
                                           :installed-last-applied-lsn
                                           installed-lsn
                                           :resume-next-lsn
                                           resume-next-lsn})}})))
                    {:ok? false
                     :state (:state install-res)
                     :error (:error install-res)}))
                (catch Exception e
                  {:ok? false
                   :state current-m
                   :error {:error (or (:error (ex-data e))
                                      :ha/follower-snapshot-bootstrap-failed)
                           :message (ex-message e)
                           :data (ex-data e)}})
                (finally
                  (when (u/file-exists snapshot-dir)
                    (u/delete-files snapshot-dir))))]
          (if (:ok? attempt)
            attempt
            (let [next-m (normalize-ha-bootstrap-retry-state
                          (:state attempt)
                          current-m
                          current-reopen-info)
                  next-reopen-info (or (ha-local-store-reopen-info next-m)
                                       current-reopen-info)]
              (recur (rest remaining)
                     next-m
                     next-reopen-info
                     (conj errors
                           (assoc (:error attempt)
                                  :source-endpoint source-endpoint))))))
        {:ok? false
         :state current-m
         :source-order (vec source-order)
         :errors errors}))))

(defn- sync-ha-follower-state
  [db-name m now-ms]
  (if (not= :follower (:ha-role m))
    m
    (let [lease (:ha-authority-lease m)
          local-node-id (:ha-node-id m)
          leader-node-id (:leader-node-id lease)]
      (cond
        (or (nil? lease) (nil? leader-node-id))
        m

        (= leader-node-id local-node-id)
        m

        (lease/lease-expired? lease now-ms)
        m

        (and (integer? (:ha-follower-next-sync-not-before-ms m))
             (< (long now-ms)
                (long (:ha-follower-next-sync-not-before-ms m))))
        m

        :else
        (let [leader-endpoint (ha-leader-endpoint m lease)]
          (if (or (nil? leader-endpoint) (s/blank? leader-endpoint))
            (assoc m
                   :ha-follower-last-error :missing-leader-endpoint
                   :ha-follower-last-error-details {:lease lease}
                   :ha-follower-last-error-ms now-ms)
            (try
              (let [m (reopen-ha-local-store-if-needed m)
                    local-next-lsn
                    (long (max 1
                               (unchecked-inc
                                (long (ha-local-last-applied-lsn m)))))
                    local-last-applied-lsn
                    (long (max 0
                               (dec local-next-lsn)))
                    leader-last-applied-lsn
                    (long (or (:leader-last-applied-lsn lease)
                              0))
                    tracked-next-lsn
                    (when (integer? (:ha-follower-next-lsn m))
                      (long (:ha-follower-next-lsn m)))
                    ;; Fresh follower stores can expose internal LMDB txlog
                    ;; watermarks before they have replayed any replicated rows.
                    ;; Honor the tracked follower cursor when the local watermark
                    ;; floor is behind the leader, such as after snapshot install
                    ;; resets. Otherwise clamp speculative cursors to the local
                    ;; floor.
                    next-lsn
                    (cond
                      (and tracked-next-lsn
                           (pos? (long tracked-next-lsn))
                           (> (long tracked-next-lsn)
                              local-next-lsn)
                           (> leader-last-applied-lsn
                              local-last-applied-lsn))
                      tracked-next-lsn

                      (and tracked-next-lsn
                           (pos? (long tracked-next-lsn)))
                      (long-min2 tracked-next-lsn local-next-lsn)

                      :else
                      local-next-lsn)]
                (:state (sync-ha-follower-batch
                         db-name m lease next-lsn now-ms)))
              (catch Exception e
                (let [error-state (or (:state (ex-data e)) m)
                      fallback-next-lsn
                      (long (max 1
                                 (unchecked-inc
                                  (long
                                   (ha-local-last-applied-lsn
                                    error-state)))))]
                  (cond
                    (ha-follower-stale-state-error? e)
                    error-state

                    (ha-follower-gap-error? e)
                    (let [source-order (vec (or (:source-order (ex-data e))
                                                (ha-gap-fallback-source-endpoints
                                                 db-name error-state lease
                                                 fallback-next-lsn)))
                          source-order-dynamic?
                          (if (contains? (ex-data e) :source-order-dynamic?)
                            (true? (:source-order-dynamic? (ex-data e)))
                            true)
                          bootstrap (bootstrap-ha-follower-from-snapshot
                                     db-name error-state lease source-order
                                     fallback-next-lsn
                                     now-ms)]
                      (if (:ok? bootstrap)
                        (:state bootstrap)
                        (let [details {:message
                                       "Follower txlog gap unresolved and snapshot bootstrap failed"
                                       :data
                                       {:error :ha/follower-snapshot-bootstrap-failed
                                       :gap-error {:message (ex-message e)
                                                   :data (ex-data e)}
                                        :snapshot-errors (:errors bootstrap)}
                                       :leader-endpoint leader-endpoint
                                       :source-order source-order}]
                          (assoc (:state bootstrap)
                                 :ha-follower-source-order source-order
                                 :ha-follower-source-order-dynamic?
                                 source-order-dynamic?
                                 :ha-follower-source-order-authority-version
                                 (:ha-authority-version error-state)
                                 :ha-follower-last-error :sync-failed
                                 :ha-follower-last-error-details details
                                 :ha-follower-last-error-ms now-ms
                                 :ha-follower-degraded? true
                                 :ha-follower-degraded-reason :wal-gap
                                 :ha-follower-degraded-details details
                                 :ha-follower-degraded-since-ms
                                 (or (:ha-follower-degraded-since-ms
                                      (:state bootstrap))
                                     now-ms)
                                 :ha-follower-sync-backoff-ms nil
                                 :ha-follower-next-sync-not-before-ms nil))))

                    :else
                    (let [details {:message (ex-message e)
                                   :data (ex-data e)
                                   :leader-endpoint leader-endpoint
                                   :source-order
                                   (ha-follower-source-endpoints error-state
                                                                 lease)}
                          backoff-ms (next-ha-follower-sync-backoff-ms
                                      error-state)]
                      (assoc error-state
                             :ha-follower-last-error :sync-failed
                             :ha-follower-last-error-details details
                             :ha-follower-last-error-ms now-ms
                             :ha-follower-sync-backoff-ms backoff-ms
                             :ha-follower-next-sync-not-before-ms
                             (+ (long now-ms)
                                (long backoff-ms))))))))))))))
