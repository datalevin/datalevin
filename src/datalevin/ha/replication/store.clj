;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.replication.store
  "Local-store, watermark, and reopen helpers for HA replication."
  (:require
   [clojure.string :as s]
   [datalevin.constants :as c]
   [datalevin.ha.lease :as lease]
   [datalevin.ha.snapshot :as snap]
   [datalevin.ha.util :as hu]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.storage :as st]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [datalevin.db DB]
   [datalevin.interface IStore ILMDB]
   [datalevin.storage Store]))

(def ^:private long-max2 hu/long-max2)
(def ^:private long-max3 hu/long-max3)
(def ^:private long-max4 hu/long-max4)
(def ^:private long-min2 hu/long-min2)
(def ^:private nonnegative-long-diff hu/nonnegative-long-diff)
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

(def ^:dynamic *ha-with-local-store-swap-fn*
  (fn [f]
    (f)))

(def ^:dynamic *ha-with-local-store-read-fn*
  (fn [f]
    (f)))

(defn with-ha-local-store-swap
  [f]
  (*ha-with-local-store-swap-fn* f))

(defn with-ha-local-store-read
  [f]
  (*ha-with-local-store-read-fn* f))

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

(defn local-kv-store
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

(defn raw-local-kv-store
  [m]
  (when-let [kv-store (local-kv-store m)]
    (if (instance? datalevin.kv.KVLMDB kv-store)
      (.-db ^datalevin.kv.KVLMDB kv-store)
      kv-store)))

(defn closed-kv-store?
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

(defn read-ha-local-persisted-lsn
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

(defn persist-ha-local-applied-lsn!
  [m applied-lsn]
  (when-let [kv-store (raw-local-kv-store m)]
    (try
      (i/transact-kv kv-store c/kv-info
                     [[:put c/ha-local-applied-lsn (long applied-lsn)]]
                     :keyword :data)
      (long applied-lsn)
      (catch Throwable t
        (when-not (closed-kv-race? t kv-store)
          (throw t))
        nil))))

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

(defn note-ha-bootstrap-installed-state
  [m installed-lsn source-endpoint snapshot-lsn now-ms persisted-installed-lsn]
  (let [resume-next-lsn (unchecked-inc (long installed-lsn))]
    (cond-> (assoc m
                   :ha-local-last-applied-lsn (long installed-lsn)
                   :ha-follower-last-applied-term nil
                   :ha-follower-batches-since-persist 0
                   :ha-follower-next-lsn resume-next-lsn
                   :ha-follower-last-bootstrap-ms (long now-ms)
                   :ha-follower-bootstrap-source-endpoint source-endpoint
                   :ha-follower-bootstrap-snapshot-last-applied-lsn
                   (long snapshot-lsn))
      persisted-installed-lsn
      (assoc :ha-local-persisted-lsn (long persisted-installed-lsn)
             :ha-local-last-persisted-applied-ms (long now-ms)))))

(defn maybe-persist-ha-follower-local-applied-lsn*
  [persist-ha-local-applied-lsn!-fn m applied-lsn now-ms]
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
      (if-let [persisted-applied-lsn
               (persist-ha-local-applied-lsn!-fn tracked-m applied-lsn)]
        (note-ha-local-applied-lsn-persisted
         tracked-m persisted-applied-lsn now-ms)
        (assoc tracked-m :ha-follower-batches-since-persist next-batch-count))
      (assoc tracked-m :ha-follower-batches-since-persist next-batch-count))))

(defn read-ha-local-snapshot-current-lsn
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

(defn read-ha-local-payload-lsn
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

(def ha-local-watermark-snapshot-key
  ::ha-local-watermark-snapshot)

(def ^:private ha-local-store-transient-state-keys
  [ha-local-watermark-snapshot-key
   :wdt-db
   :wlmdb
   :wstore
   :runner
   :engine
   :index])

(defn clear-ha-local-store-transient-state
  [m]
  (apply dissoc m ha-local-store-transient-state-keys))

(declare read-ha-local-watermark-lsn)
(declare read-ha-local-last-applied-lsn)

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

(defn- read-ha-local-watermark-lsn*
  [kv-store]
  (long (or (try
              (get (kv/txlog-watermarks kv-store) :last-applied-lsn)
              (catch Throwable t
                (when-not (closed-kv-race? t kv-store)
                  (throw t))
                nil))
            0)))

(defn fresh-ha-local-watermark-snapshot
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
        tracked-follower-next-lsn
        (when (integer? (:ha-follower-next-lsn m))
          (long (:ha-follower-next-lsn m)))
        tracked-follower-applied-lsn
        (when (and tracked-follower-next-lsn
                   (pos? (long tracked-follower-next-lsn)))
          (long (max 0
                     (dec (long tracked-follower-next-lsn)))))
        replayed-follower-lsn
        (if (integer? tracked-follower-applied-lsn)
          (long-max2 follower-floor-lsn
                     (long-min2 state-lsn
                                tracked-follower-applied-lsn))
          follower-floor-lsn)
        local-last-applied-lsn
        (long (if (= :leader role)
                ceiling-lsn
                (if (pos? (long ceiling-lsn))
                  (long-min2 ceiling-lsn replayed-follower-lsn)
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
     :tracked-follower-next-lsn tracked-follower-next-lsn
     :tracked-follower-applied-lsn tracked-follower-applied-lsn
     :replayed-follower-lsn replayed-follower-lsn
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

(defn read-ha-snapshot-payload-lsn
  [m]
  (with-ha-local-store-read
    #(if-let [kv-store (local-kv-store m)]
       (read-ha-local-payload-lsn kv-store)
       0)))

(defn read-ha-local-last-applied-lsn
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

(defn read-ha-local-watermark-lsn
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
             target-lsn (long (if (= :leader (:ha-role m))
                                (long-max2 persisted-lsn lease-lsn)
                                (long-max2 persisted-lsn state-lsn)))
             applied-lsn (long (if (pos? (long ceiling-lsn))
                                 (long-min2 ceiling-lsn target-lsn)
                                 target-lsn))]
         (when (> applied-lsn persisted-lsn)
           (persist-ha-local-applied-lsn! m applied-lsn))
         applied-lsn))))

(defn ha-local-last-applied-lsn
  [m]
  (let [state-lsn (:ha-local-last-applied-lsn m)]
    (if (local-kv-store m)
      (read-ha-local-last-applied-lsn m)
      (if (integer? state-lsn)
        (long state-lsn)
        0))))

(defn refresh-ha-local-watermarks
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

(defn fresh-ha-promotion-local-last-applied-lsn
  [m observed-lease]
  (with-ha-local-store-read
    #(let [state-lsn (long (or (:ha-local-last-applied-lsn m) 0))]
       (try
         (if-let [kv-store (local-kv-store m)]
           (:local-last-applied-lsn
            (fresh-ha-local-watermark-snapshot m kv-store))
           (if (and (bootstrap-empty-lease? observed-lease)
                    (zero? state-lsn))
             (long-max2 state-lsn (read-ha-local-watermark-lsn m))
             state-lsn))
         (catch Throwable e
           (when-not (closed-kv-race? e (local-kv-store m))
             (log/warn e "Unable to read fresh local txlog watermarks for HA promotion lag guard"
                       {:db-name (some-> (:store m) i/opts :db-name)}))
           state-lsn)))))

(defn ha-promotion-lag-guard
  ([m observed-lease]
   (ha-promotion-lag-guard m observed-lease
                           (fresh-ha-promotion-local-last-applied-lsn
                            m observed-lease)))
  ([m observed-lease local-last-applied-lsn]
   (let [leader-last-applied-lsn
         (long (or (:leader-last-applied-lsn observed-lease) 0))
         local-last-applied-lsn (long (or local-last-applied-lsn 0))
         lag-lsn (nonnegative-long-diff leader-last-applied-lsn
                                        local-last-applied-lsn)
         max-lag-lsn (long (or (:ha-max-promotion-lag-lsn m) 0))]
     {:ok? (<= lag-lsn max-lag-lsn)
      :leader-last-applied-lsn leader-last-applied-lsn
      :local-last-applied-lsn local-last-applied-lsn
      :lag-lsn lag-lsn
      :max-lag-lsn max-lag-lsn})))

(def ^:private close-ha-local-store! snap/close-ha-local-store!)
(def ^:private refresh-ha-local-dt-db snap/refresh-ha-local-dt-db)
(def ^:private open-ha-store-dbis! snap/open-ha-store-dbis!)
(def recover-ha-local-store-dir-if-needed!
  snap/recover-ha-local-store-dir-if-needed!)

(def ha-local-store-reopen-info-key
  ::ha-local-store-reopen-info)

(defn ha-local-store-open-opts
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

(declare ha-local-store-reopen-info)
(declare reopen-ha-local-store-from-info)

(defn reopen-ha-local-store-if-needed
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

(defn ha-local-store-reopen-info
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

(defn reopen-ha-local-store-from-info
  [m {:keys [env-dir store-opts]}]
  (when (and (string? env-dir)
             (not (s/blank? env-dir))
             (map? store-opts))
    (-> m
        ((fn [state]
           (close-ha-local-store! state)
           state))
        clear-ha-local-store-transient-state
        (assoc :store (-> (st/open env-dir nil store-opts)
                          open-ha-store-dbis!)
               :dt-db nil)
        (dissoc ha-local-store-reopen-info-key)
        refresh-ha-local-dt-db)))
