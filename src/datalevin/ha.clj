;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha
  "Consensus-lease HA runtime helpers shared by server runtime."
  (:require
   [clojure.string :as s]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.ha.client-cache :as cache]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.lease :as lease]
   [datalevin.ha.snapshot :as snap]
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
   [java.util.concurrent ExecutorService Executors Future ThreadFactory
    TimeUnit]
   [java.util.concurrent.atomic AtomicLong]))

(defn consensus-ha-opts
  [store]
  (when (instance? IStore store)
    (let [opts (i/opts store)]
      (when (= :consensus-lease (:ha-mode opts))
        opts))))

(def ^:private ha-runtime-local-option-keys
  [:ha-node-id
   :ha-client-credentials
   :ha-fencing-hook
   :ha-clock-skew-hook])

(def ^:private ha-runtime-local-control-plane-option-keys
  [:local-peer-id
   :raft-dir])

(defn select-ha-runtime-local-opts
  "Select the node-local HA runtime config from a full HA opts map."
  [ha-opts]
  (let [ha-opts (or ha-opts {})
        local-opts (select-keys ha-opts ha-runtime-local-option-keys)
        runtime-control-plane (some-> (:ha-control-plane ha-opts)
                                      (select-keys
                                       ha-runtime-local-control-plane-option-keys))]
    (cond-> local-opts
      (seq runtime-control-plane)
      (assoc :ha-control-plane runtime-control-plane))))

(defn merge-ha-runtime-local-opts
  "Merge node-local HA runtime config back onto a store/runtime opts map without
  changing the shared persisted HA configuration."
  [store-opts runtime-opts]
  (let [store-opts            (or store-opts {})
        runtime-opts          (or runtime-opts {})
        local-runtime-opts    (select-ha-runtime-local-opts runtime-opts)
        local-opts            (select-keys local-runtime-opts
                                           ha-runtime-local-option-keys)
        runtime-control-plane (:ha-control-plane local-runtime-opts)
        store-control-plane   (:ha-control-plane store-opts)]
    (cond-> (merge store-opts local-opts)
      (seq runtime-control-plane)
      (assoc :ha-control-plane
             (merge (or store-control-plane {})
                    runtime-control-plane)))))

(defn effective-ha-runtime-local-opts
  "Return the best available node-local HA config from a runtime state map.
  This survives HA runtime restarts where the active `:ha-runtime-opts` may be
  temporarily cleared while the preserved local config remains available."
  [m]
  (merge-ha-runtime-local-opts
   (:ha-runtime-local-opts (or m {}))
   (some-> m :ha-runtime-opts select-ha-runtime-local-opts)))

(defn- local-ha-endpoint
  [ha-opts]
  (let [node-id (:ha-node-id ha-opts)]
    (:endpoint (first (filter #(= node-id (:node-id %))
                              (:ha-members ha-opts))))))

(defn- ordered-ha-members
  [m]
  (or (:ha-members-sorted m)
      (some->> (:ha-members m)
               (sort-by :node-id)
               vec)))

(defn- ha-request-timeout-ms
  [m max-ms]
  (let [renew-ms (long (or (:ha-lease-renew-ms m)
                           c/*ha-lease-renew-ms*))
        rpc-timeout-ms (long (or (get-in m [:ha-control-plane :rpc-timeout-ms])
                                 0))
        budget-ms (max 1000 renew-ms rpc-timeout-ms)]
    (long (min (long max-ms) budget-ms))))

(defn- ha-lease-local-remaining-ms
  [m now-ms now-nanos]
  (cond
    (integer? (:ha-lease-local-deadline-nanos m))
    (max 0
         (.toMillis TimeUnit/NANOSECONDS
                    (max 0
                         (- (long (:ha-lease-local-deadline-nanos m))
                            (long now-nanos)))))

    (integer? (:ha-lease-local-deadline-ms m))
    (max 0
         (- (long (:ha-lease-local-deadline-ms m))
            (long now-ms)))

    (integer? (:ha-lease-until-ms m))
    (max 0
         (- (long (:ha-lease-until-ms m))
            (long now-ms)))

    :else
    nil))

(defn- ha-renew-timeout-ms ^long
  [m now-ms now-nanos]
  (let [lease-timeout-ms   (long (or (:ha-lease-timeout-ms m)
                                     c/*ha-lease-timeout-ms*))
        request-timeout-ms (long (ha-request-timeout-ms m lease-timeout-ms))
        remaining-ms       (ha-lease-local-remaining-ms m now-ms now-nanos)
        timeout-ms         (long (if (integer? remaining-ms)
                                   (let [remaining-ms (long remaining-ms)]
                                     (if (< remaining-ms request-timeout-ms)
                                       remaining-ms
                                       request-timeout-ms))
                                   request-timeout-ms))]
    (if (< timeout-ms 1) 1 timeout-ms)))

(defn- long-max2 ^long
  [a b]
  (let [a (long a)
        b (long b)]
    (if (> a b) a b)))

(defn- long-max3 ^long
  [a b c]
  (long-max2 a (long-max2 b c)))

(defn- long-max4 ^long
  [a b c d]
  (long-max2 (long-max2 a b) (long-max2 c d)))

(defn- long-min2 ^long
  [a b]
  (let [a (long a)
        b (long b)]
    (if (< a b) a b)))

(defn- nonnegative-long-diff ^long
  [a b]
  (let [a (long a)
        b (long b)]
    (if (> a b) (- a b) 0)))

(defn- demote-ha-leader
  [db-name m reason details now-ms]
  (if (= :leader (:ha-role m))
    (let [drain-ms (long (or (:ha-demotion-drain-ms m)
                             c/*ha-demotion-drain-ms*))]
      (log/warn "Demoting HA leader for DB" db-name
                {:reason reason
                 :details details
                 :drain-ms drain-ms})
      (-> m
          (assoc :ha-role :demoting
                 :ha-demotion-reason reason
                 :ha-demotion-details details
                 :ha-demotion-drain-ms drain-ms
                 :ha-demoted-at-ms now-ms
                 :ha-demotion-drain-until-ms (+ (long now-ms) drain-ms))
          (dissoc :ha-leader-term
                  :ha-leader-last-applied-lsn
                  :ha-leader-fencing-pending?
                  :ha-leader-fencing-observed-lease
                  :ha-leader-fencing-last-error)))
    m))

(defn- ha-demotion-deadline-ms
  [m]
  (cond
    (integer? (:ha-demotion-drain-until-ms m))
    (long (:ha-demotion-drain-until-ms m))

    (integer? (:ha-demoted-at-ms m))
    (long (:ha-demoted-at-ms m))

    :else nil))

(defn- ha-demotion-draining?
  [m now-ms]
  (let [deadline-ms (ha-demotion-deadline-ms m)]
    (or (= :demoting (:ha-role m))
        (and (integer? deadline-ms)
             (< (long now-ms) (long deadline-ms))))))

(defn- maybe-finish-ha-demotion
  [m now-ms started-demoting?]
  (let [deadline-ms (ha-demotion-deadline-ms m)]
    (if (and started-demoting?
             (= :demoting (:ha-role m))
             (integer? deadline-ms)
             (>= (long now-ms) (long deadline-ms)))
      (assoc m :ha-role :follower)
      m)))

(defn- clear-ha-candidate-state
  [m]
  (dissoc m
          :ha-candidate-since-ms
          :ha-candidate-delay-ms
          :ha-candidate-rank-index
          :ha-candidate-pre-cas-wait-until-ms
          :ha-candidate-pre-cas-observed-version
          :ha-promotion-wait-before-cas-ms))

(defn- clear-ha-leader-fencing-state
  [m]
  (dissoc m
          :ha-leader-fencing-pending?
          :ha-leader-fencing-observed-lease
          :ha-leader-fencing-last-error))

(defn- ha-promotion-rank-index
  [m]
  (let [node-id (:ha-node-id m)
        members (ordered-ha-members m)]
    (first
     (keep-indexed
      (fn [idx member]
        (when (= node-id (:node-id member))
          idx))
      members))))

(defn- ha-promotion-delay-ms
  [m]
  (let [base-ms (long (or (:ha-promotion-base-delay-ms m)
                          c/*ha-promotion-base-delay-ms*))
        rank-ms (long (or (:ha-promotion-rank-delay-ms m)
                          c/*ha-promotion-rank-delay-ms*))
        rank-idx (ha-promotion-rank-index m)]
    (when (integer? rank-idx)
      (+ base-ms (* (long rank-idx) rank-ms)))))

(defn ^:redef ha-now-ms
  []
  (System/currentTimeMillis))

(defn ^:redef ha-now-nanos
  []
  (System/nanoTime))

(defn- local-kv-store
  [m]
  (let [dt-db (:dt-db m)
        candidates (cond-> [(:store m)]
                     (instance? DB dt-db)
                     (conj (.-store ^DB dt-db)))]
    (some
     (fn [store]
       (let [kv-store (cond
                        (nil? store) nil
                        (instance? Store store) (.-lmdb ^Store store)
                        (instance? ILMDB store) store
                        :else nil)]
         (when-not (try
                     (cond
                       (nil? kv-store) true
                       (instance? IStore kv-store) (i/closed? kv-store)
                       (instance? ILMDB kv-store) (i/closed-kv? kv-store)
                       :else false)
                     (catch Throwable _
                       true))
           kv-store)))
     candidates)))

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
    (catch Throwable _
      true)))

(defn- closed-kv-race?
  [t kv-store]
  (or (closed-kv-store? kv-store)
      (and t
           (s/includes? (or (ex-message t) "")
                        "LMDB env is closed"))))

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

(defn- persist-ha-local-applied-lsn!
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
  (if-let [kv-store (local-kv-store m)]
    (read-ha-local-payload-lsn kv-store)
    0))

(defn ^:redef read-ha-local-last-applied-lsn
  [m]
  (let [state-lsn (long (or (:ha-local-last-applied-lsn m) 0))]
    (try
      (if-let [kv-store (local-kv-store m)]
        (:local-last-applied-lsn
         (ha-local-watermark-snapshot m kv-store))
        state-lsn)
      (catch Throwable e
        (when-not (closed-kv-race? e (local-kv-store m))
          (log/warn e "Unable to read local txlog watermarks for HA lag guard"
                    {:db-name (some-> (:store m) i/opts :db-name)}))
        state-lsn))))

(defn- ^:redef read-ha-local-watermark-lsn
  [m]
  (if-let [kv-store (local-kv-store m)]
    (if-let [snapshot (cached-ha-local-watermark-snapshot m kv-store)]
      (long (:watermark-lsn snapshot))
      (read-ha-local-watermark-lsn* kv-store))
    0))

(defn persist-ha-runtime-local-applied-lsn!
  [m]
  (when-let [kv-store (local-kv-store m)]
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
      applied-lsn)))

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
  (if-let [kv-store (local-kv-store m)]
    (let [{:keys [role local-last-applied-lsn] :as snapshot}
          (ha-local-watermark-snapshot m kv-store)]
      (cond-> (assoc m
                     ha-local-watermark-snapshot-key snapshot
                     :ha-local-last-applied-lsn local-last-applied-lsn)
        (= :leader role)
        (assoc :ha-leader-last-applied-lsn local-last-applied-lsn)))
    m))

(declare bootstrap-empty-lease?)
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
  (let [state-lsn (long (or (:ha-local-last-applied-lsn m) 0))]
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
        state-lsn))))

(defn- bootstrap-empty-lease?
  [lease]
  (and (or (nil? lease) (nil? (:leader-node-id lease)))
       (zero? (long (lease/observed-term lease)))))

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

(declare ha-probe-executor)

(defn- shutdown-ha-executor!
  [^ExecutorService executor description context]
  (when executor
    (try
      (.shutdown executor)
      (when-not (.awaitTermination executor 100 TimeUnit/MILLISECONDS)
        (.shutdownNow executor))
      (catch InterruptedException e
        (.interrupt (Thread/currentThread))
        (.shutdownNow executor)
        (log/warn e (str "Interrupted while stopping " description) context))
      (catch Throwable e
        (log/warn e (str "Failed to stop " description) context)))))

(defn- stop-ha-probe-executor!
  [db-name executor]
  (when (and executor
             (not (identical? executor ha-probe-executor)))
    (shutdown-ha-executor! executor
                           "HA probe executor"
                           {:db-name db-name})))

(defn- ha-thread-label
  [db-name]
  (when (some? db-name)
    (let [label (-> (str db-name)
                    (s/replace #"[^A-Za-z0-9._-]+" "-")
                    (s/replace #"(^-+|-+$)" ""))]
      (when-not (s/blank? label)
        label))))

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
                (assoc :store recovered-store
                       :dt-db nil)
                (dissoc :engine :index
                        ha-local-store-reopen-info-key)
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
  (or (get m ha-local-store-reopen-info-key)
      (let [store (:store m)]
        (when (instance? IStore store)
          (try
            {:env-dir (i/dir store)
             :store-opts (ha-local-store-open-opts m store)}
            (catch Throwable _
              nil))))))

(defn- ^:redef reopen-ha-local-store-from-info
  [m {:keys [env-dir store-opts]}]
  (when (and (string? env-dir)
             (not (s/blank? env-dir))
             (map? store-opts))
    (-> m
        (assoc :store (-> (st/open env-dir nil store-opts)
                          open-ha-store-dbis!)
               :dt-db nil)
        (dissoc :engine :index
                ha-local-store-reopen-info-key)
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
        (let [state (reopen-ha-local-store-from-info fallback-m reopen-info)]
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
                 (do
                   (close-ha-local-store! m)
                   (reopen-ha-local-store-from-info m reopen-info))
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

(defonce ^:private ^ExecutorService ha-probe-executor
  (Executors/newFixedThreadPool
   (int ha-probe-max-threads)
   (new-ha-probe-thread-factory)))

(defn- new-ha-probe-executor
  [db-name]
  (Executors/newFixedThreadPool
   (int ha-probe-max-threads)
   (new-ha-probe-thread-factory (ha-thread-label db-name))))

(defn- ha-probe-executor-for
  [m]
  (or (:ha-probe-executor m)
      ha-probe-executor))

(defn- ha-parallel-mapv
  ([f coll]
   (ha-parallel-mapv nil f coll))
  ([m f coll]
   (let [items (vec coll)
         n (count items)]
     (cond
       (zero? n)
       []

       (= 1 n)
       [(f (first items))]

       :else
       (->> (.invokeAll ^ExecutorService (ha-probe-executor-for m)
                        (mapv (fn [item] #(f item)) items))
            (mapv #(.get ^Future %)))))))

(defn ^:redef fetch-leader-watermark-lsn
  [db-name m lease]
  (let [leader-endpoint (:leader-endpoint lease)
        authority-lsn (long (or (:leader-last-applied-lsn lease) 0))
        result (safe-fetch-ha-endpoint-watermark-lsn
                db-name m leader-endpoint)]
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

(defn- highest-reachable-ha-member-watermark
  ([db-name m]
   (highest-reachable-ha-member-watermark db-name m {}))
  ([db-name m prefetched-watermarks]
   (let [local-endpoint (:ha-local-endpoint m)
         endpoints (->> (concat [local-endpoint]
                                (map :endpoint (:ha-members m)))
                        (filter #(and (string? %)
                                      (not (s/blank? %))))
                        distinct
                        vec)
         watermarks (ha-parallel-mapv
                     m
                     (fn [endpoint]
                       {:endpoint endpoint
                        :watermark
                        (if (contains? prefetched-watermarks endpoint)
                          (get prefetched-watermarks endpoint)
                          (safe-fetch-ha-endpoint-watermark-lsn
                           db-name m endpoint))})
                     endpoints)]
     (reduce (fn [best {:keys [endpoint watermark]}]
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
             watermarks))))

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
(def ^:private ha-follower-adaptive-batch-byte-array-class
  (class (byte-array 0)))
(def ^:private ha-follower-adaptive-batch-value-overhead-bytes 16)
(def ^:private ha-follower-adaptive-batch-record-overhead-bytes 32)

(defn- estimate-ha-follower-value-bytes ^long
  [value]
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
    (reduce-kv (fn [^long total k v]
                 (+ total
                    (long (estimate-ha-follower-value-bytes k))
                    (long (estimate-ha-follower-value-bytes v))))
               32
               value)

    (vector? value)
    (reduce (fn [^long total item]
              (+ total (long (estimate-ha-follower-value-bytes item))))
            16
            value)

    (sequential? value)
    (reduce (fn [^long total item]
              (+ total (long (estimate-ha-follower-value-bytes item))))
            16
            value)

    (coll? value)
    (reduce (fn [^long total item]
              (+ total (long (estimate-ha-follower-value-bytes item))))
            16
            value)

    :else
    (long ha-follower-adaptive-batch-value-overhead-bytes)))

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

(defn- ha-follower-source-endpoints
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

(defn- ha-gap-fallback-source-endpoints
  ([db-name m lease next-lsn]
   (ha-gap-fallback-source-endpoints
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
          (fn [{:keys [endpoint node-id]}]
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
                                 (>= (long last-lsn)
                                     required-lsn))]
              {:endpoint endpoint
               :node-id node-id
               :authority-ok? (:ok? authority-check)
               :authority-check authority-check
               :last-applied-lsn last-lsn
               :raw-last-applied-lsn raw-last-lsn
               :leader-safe-lsn leader-safe-lsn
               :eligible? eligible?}))
          follower-members)
         eligible-followers
         (sort-by (juxt (comp - :last-applied-lsn) :node-id)
                  (filter :eligible? followers))
         unknown-followers
         (sort-by :node-id
                  (filter :authority-ok? (remove :eligible? followers)))]
     (->> (concat (when (and (string? leader-endpoint)
                             (not (s/blank? leader-endpoint))
                             (not= leader-endpoint local-endpoint))
                    [leader-endpoint])
                  (map :endpoint eligible-followers)
                  (map :endpoint unknown-followers))
          distinct
          vec))))

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
        leader-watermark* (delay (fetch-leader-watermark-lsn
                                  db-name m lease))]
    (loop [remaining sources
           source-order sources
           reordered? false
           gap-errors []]
      (if-let [source-endpoint (first remaining)]
        (let [attempt
              (try
                (let [leader-source? (= source-endpoint leader-endpoint)
                      source-watermark (when-not leader-source?
                                         (safe-fetch-ha-endpoint-watermark-lsn
                                          db-name m source-endpoint))
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
                    (do
                      (assert-contiguous-lsn! next-lsn records)
                      (assert-ha-follower-record-terms!
                       lease source-endpoint records)
                      {:ok? true
                       :value {:source-endpoint source-endpoint
                               :records records
                               :source-order source-order
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
            (recur (rest remaining) source-order reordered? gap-errors)

            :else
            (let [remaining' (if reordered?
                               (rest remaining)
                               (->> (ha-gap-fallback-source-endpoints
                                     db-name
                                     m
                                     lease
                                     next-lsn
                                     @leader-watermark*)
                                    (remove #{source-endpoint})
                                    vec))
                  source-order' (if reordered?
                                  source-order
                                  (vec (cons source-endpoint remaining')))]
              (recur remaining'
                     source-order'
                     true
                     (conj gap-errors (:gap-error attempt))))))
        (u/raise "Follower txlog replay gap unresolved across deterministic sources"
                 {:error :ha/txlog-gap-unresolved
                  :expected-lsn next-lsn
                  :upto-lsn upto-lsn
                  :source-order source-order
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
  [lease source-endpoint records]
  (when-some [lease-term* (:term lease)]
    (let [lease-term (long lease-term*)]
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
              ;; terms after failover, so older record terms remain valid here.
              ;; Source freshness is enforced separately via
              ;; `assert-ha-source-authority!`; the per-record guard only
              ;; rejects future terms and regressions within a contiguous batch.
              (when (> record-term lease-term)
                (u/raise "Follower txlog replay record term is ahead of current lease"
                         {:error           :ha/txlog-record-invalid-term
                          :source-endpoint source-endpoint
                          :lease-term      lease-term
                          :record-lsn      (:lsn record)
                          :record-term     record-term}))
              (when (and prev-term
                         (< record-term (long prev-term)))
                (u/raise "Follower txlog replay record terms regressed within batch"
                         {:error                :ha/txlog-record-invalid-term
                          :source-endpoint      source-endpoint
                          :lease-term           lease-term
                          :previous-record-term prev-term
                          :record-lsn           (:lsn record)
                          :record-term          record-term}))
              record-term)
            prev-term))
        nil
        records))))

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
                                          (dissoc :engine :index))}))))
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
            (let [target-max-tx (long target-max-tx)]
              (loop [cur (long (i/max-tx (:store next-state)))]
                (when (< cur target-max-tx)
                  (i/advance-max-tx (:store next-state))
                  (recur (long (i/max-tx (:store next-state)))))))))
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
            install-marker {:backup-dir backup-dir
                            :db-name (some-> store i/db-name)
                            :created-at-ms (System/currentTimeMillis)}
            open-opts (ha-snapshot-open-opts
                       m
                       (:db-name (i/opts store))
                       (:ha-db-identity m))]
        (try
          ;; Validate that the copied snapshot is openable before swapping paths.
          (let [snapshot-store (st/open snapshot-dir nil open-opts)]
            (try
              (i/opts snapshot-store)
              (finally
                (i/close snapshot-store))))
          (close-ha-local-store! m)
          (when (u/file-exists backup-dir)
            (u/delete-files backup-dir))
          (write-ha-snapshot-install-marker!
           env-dir
           (assoc install-marker :stage :prepare))
          (move-path! env-dir backup-dir)
          (write-ha-snapshot-install-marker!
           env-dir
           (assoc install-marker :stage :backup-moved))
          (u/create-dirs env-dir)
          (copy-dir-contents! snapshot-dir env-dir)
          (sync-ha-snapshot-install-target! env-dir)
          (let [new-store (open-ha-store-dbis!
                           (st/open env-dir nil open-opts))
                new-db (db/new-db new-store)]
            (delete-ha-snapshot-install-marker! env-dir)
            (when (u/file-exists backup-dir)
              (u/delete-files backup-dir))
            {:ok? true
             :state (-> m
                        (assoc :store new-store
                               :dt-db new-db)
                        (dissoc :engine :index))})
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
                              (assoc :store restored-store
                                     :dt-db restored-db)
                              (dissoc :engine :index))
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
                              (assoc :store store
                                     :dt-db nil)
                              (dissoc :engine :index))
                   :error {:error :ha/follower-snapshot-install-restore-failed
                           :message (ex-message e)
                           :data (merge (or (ex-data e) {})
                                        {:restore-message (ex-message restore-e)
                                         :restore-data (ex-data restore-e)})}})))))))))

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
          {:keys [records source-endpoint source-order
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
          _ (when (seq records)
              (persist-ha-local-applied-lsn! next-m applied-lsn))]
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

(defn ^:redef maybe-wait-unreachable-leader-before-pre-cas!
  [m lease]
  (let [renew-ms (long (or (:ha-lease-renew-ms m) c/*ha-lease-renew-ms*))
        lease-until-ms (long (or (:lease-until-ms lease) 0))
        wait-until-ms (+ lease-until-ms renew-ms)
        now-ms (System/currentTimeMillis)
        wait-ms (long (max 0 (- wait-until-ms now-ms)))]
    {:wait-ms wait-ms
     :wait-until-ms wait-until-ms}))

(defn- pre-cas-lag-input
  [db-name m lease now-ms]
  (let [authority-lsn (long (or (:leader-last-applied-lsn lease) 0))
        lease-expired? (lease/lease-expired? lease now-ms)
        local-last-applied-lsn (fresh-ha-promotion-local-last-applied-lsn
                                m lease)
        leader-watermark (fetch-leader-watermark-lsn db-name m lease)
        reachable? (true? (:reachable? leader-watermark))
        leader-lsn (if reachable?
                     (long (or (:last-applied-lsn leader-watermark)
                               authority-lsn))
                     0)
        reachable-member-watermark
        (when (or (not reachable?)
                  (and lease-expired?
                       (< leader-lsn authority-lsn)))
          (highest-reachable-ha-member-watermark
           db-name
           m
           (cond-> {}
             (string? (:leader-endpoint lease))
             (assoc (:leader-endpoint lease) leader-watermark))))
        reachable-member-lsn
        (when reachable-member-watermark
          (long (or (:last-applied-lsn reachable-member-watermark) 0)))
        max-local-member-lsn
        (long-max2 (or reachable-member-lsn 0)
                   local-last-applied-lsn)
        effective-lsn
        (cond
          (and lease-expired?
               reachable?
               (< leader-lsn authority-lsn))
          (long-max2 leader-lsn max-local-member-lsn)

          reachable?
          (long-max3 authority-lsn leader-lsn max-local-member-lsn)

          :else
          max-local-member-lsn)]
    {:effective-lease (assoc lease :leader-last-applied-lsn effective-lsn)
     :local-last-applied-lsn local-last-applied-lsn
     :lease-expired? lease-expired?
     :leader-endpoint-reachable? reachable?
     :authority-last-applied-lsn authority-lsn
     :leader-watermark-last-applied-lsn (when reachable? leader-lsn)
     :leader-watermark leader-watermark
     :reachable-member-last-applied-lsn
     reachable-member-lsn
     :reachable-member-watermark reachable-member-watermark}))

(defn- authority-observation-from-state
  [m]
  {:lease (:ha-authority-lease m)
   :version (:ha-authority-version m)
   :authority-now-ms (:ha-authority-now-ms m)
   :lease-local-deadline-ms (:ha-lease-local-deadline-ms m)
   :lease-local-deadline-nanos (:ha-lease-local-deadline-nanos m)
   :authority-membership-hash (:ha-authority-membership-hash m)
   :db-identity-mismatch? (true? (:ha-db-identity-mismatch? m))
   :membership-mismatch? (true? (:ha-membership-mismatch? m))
   :observed-at-ms (:ha-last-authority-refresh-ms m)})

(defn- authority-lease-local-deadline-ms
  [lease authority-now-ms local-start-ms]
  (when (and (integer? authority-now-ms)
             (integer? local-start-ms))
    (let [lease-until-ms (:lease-until-ms lease)]
      (when (integer? lease-until-ms)
        (+ (long local-start-ms)
           (max 0
                (- (long lease-until-ms)
                   (long authority-now-ms))))))))

(defn- authority-lease-local-deadline-nanos
  [lease authority-now-ms local-start-nanos]
  (when (and (integer? authority-now-ms)
             (integer? local-start-nanos))
    (let [lease-until-ms (:lease-until-ms lease)]
      (when (integer? lease-until-ms)
        (+ (long local-start-nanos)
           (.toNanos TimeUnit/MILLISECONDS
                     (max 0
                          (- (long lease-until-ms)
                             (long authority-now-ms)))))))))

(defn ^:redef observe-authority-state
  [m]
  (let [authority (:ha-authority m)
        db-identity (:ha-db-identity m)
        local-start-ms (ha-now-ms)
        local-start-nanos (ha-now-nanos)
        {:keys [lease version membership-hash authority-now-ms]}
        (ctrl/read-state authority db-identity)
        authority-membership-hash membership-hash
        db-identity-mismatch?
        (and lease (not= db-identity (:db-identity lease)))
        membership-mismatch?
        (and authority-membership-hash
             (not= authority-membership-hash (:ha-membership-hash m)))
        observed-at-ms (ha-now-ms)]
    {:lease lease
     :version version
     :authority-now-ms authority-now-ms
     :lease-local-deadline-ms
     (authority-lease-local-deadline-ms
      lease authority-now-ms local-start-ms)
     :lease-local-deadline-nanos
     (authority-lease-local-deadline-nanos
      lease authority-now-ms local-start-nanos)
     :authority-membership-hash authority-membership-hash
     :db-identity-mismatch? db-identity-mismatch?
     :membership-mismatch? membership-mismatch?
     :observed-at-ms observed-at-ms}))

(defn- apply-authority-observation
  [m {:keys [lease version authority-now-ms
             lease-local-deadline-ms lease-local-deadline-nanos
             authority-membership-hash
             db-identity-mismatch? membership-mismatch?
             observed-at-ms]}
   now-ms]
  (let [refresh-ms (or observed-at-ms now-ms)]
  (-> m
      (assoc :ha-authority-lease lease
             :ha-authority-version version
             :ha-authority-now-ms authority-now-ms
             :ha-lease-local-deadline-ms lease-local-deadline-ms
             :ha-lease-local-deadline-nanos lease-local-deadline-nanos
             :ha-authority-owner-node-id (:leader-node-id lease)
             :ha-authority-term (:term lease)
             :ha-lease-until-ms (:lease-until-ms lease)
             :ha-authority-membership-hash authority-membership-hash
             :ha-db-identity-mismatch? db-identity-mismatch?
             :ha-membership-mismatch? membership-mismatch?
             :ha-last-authority-refresh-ms refresh-ms))))

(defn- authority-read-error
  [e]
  {:reason :authority-read-failed
   :message (ex-message e)
   :data (ex-data e)})

(defn- apply-authority-read-failure
  [m error]
  (assoc m
         :ha-authority-read-ok? false
         :ha-authority-read-error error))

(defn- apply-authority-read-success
  [m]
  (assoc m
         :ha-authority-read-ok? true
         :ha-authority-read-error nil))

(defn- run-command-with-timeout
  [cmd env timeout-ms]
  (try
    (let [process-builder (ProcessBuilder. ^java.util.List (mapv str cmd))
          env-map (.environment process-builder)
          _ (doseq [[k v] env]
              (.put env-map (str k) (str v)))
          _ (.redirectErrorStream process-builder true)
          process (.start process-builder)
          finished? (.waitFor process
                              (long timeout-ms)
                              TimeUnit/MILLISECONDS)]
      (if finished?
        (let [exit (.exitValue process)
              output (try
                       (slurp (.getInputStream process) :encoding "UTF-8")
                       (catch Exception _
                         ""))]
          {:ok? (zero? exit)
           :exit exit
           :output output})
        (do
          (.destroy process)
          (when-not (.waitFor process 200 TimeUnit/MILLISECONDS)
            (.destroyForcibly process))
          {:ok? false
           :reason :timeout
           :timeout-ms timeout-ms})))
    (catch Exception e
      {:ok? false
       :reason :exception
       :message (ex-message e)})))

(defn ^:redef run-ha-fencing-hook
  [db-name m observed-lease]
  (if (bootstrap-empty-lease? observed-lease)
    {:ok? true
     :skipped? true
     :reason :bootstrap-empty-lease}
    (let [{:keys [cmd timeout-ms retries retry-delay-ms]} (:ha-fencing-hook m)]
      (if-not (and (vector? cmd) (seq cmd))
        {:ok? false
         :reason :fencing-hook-unconfigured}
        (let [observed-term (lease/observed-term observed-lease)
              candidate-term (lease/next-term observed-lease)
              candidate-id (:ha-node-id m)
              fence-op-id (str db-name ":" observed-term ":" candidate-id)
              fence-shared-op-id (str db-name ":" observed-term)
              env {"DTLV_DB_NAME" db-name
                   "DTLV_OLD_LEADER_NODE_ID"
                   (str (or (:leader-node-id observed-lease) ""))
                   "DTLV_OLD_LEADER_ENDPOINT"
                   (str (or (:leader-endpoint observed-lease) ""))
                   "DTLV_NEW_LEADER_NODE_ID" (str candidate-id)
                   "DTLV_TERM_CANDIDATE" (str candidate-term)
                   "DTLV_TERM_OBSERVED" (str observed-term)
                   "DTLV_FENCE_OP_ID" fence-op-id
                   "DTLV_FENCE_SHARED_OP_ID" fence-shared-op-id}
              max-attempts (inc (long (or retries 0)))
              timeout-ms (long (or timeout-ms 3000))
              retry-delay-ms (long (or retry-delay-ms 1000))]
          (loop [attempt 1]
            (let [result (run-command-with-timeout cmd env timeout-ms)]
              (if (:ok? result)
                (assoc result
                       :attempt attempt
                       :fence-op-id fence-op-id
                       :fence-shared-op-id fence-shared-op-id)
                (if (< attempt max-attempts)
                  (do
                    (Thread/sleep retry-delay-ms)
                    (recur (u/long-inc attempt)))
                  (assoc result
                         :attempt attempt
                         :fence-op-id fence-op-id
                         :fence-shared-op-id fence-shared-op-id))))))))))

(defn- maybe-complete-ha-leader-fencing
  [m db-name]
  (let [observed-lease (:ha-leader-fencing-observed-lease m)]
    (if-not (and (= :leader (:ha-role m))
                 (true? (:ha-leader-fencing-pending? m)))
      m
      (let [fence-result (try
                           (run-ha-fencing-hook db-name m observed-lease)
                           (catch Exception e
                             {:ok? false
                              :reason :exception
                              :message (ex-message e)
                              :data (ex-data e)}))]
        (if (:ok? fence-result)
          (-> m
              clear-ha-leader-fencing-state
              (assoc :ha-leader-fencing-last-error nil))
          (do
            (log/warn "HA leader fencing incomplete"
                      {:db-name db-name
                       :ha-node-id (:ha-node-id m)
                       :ha-authority-term (:ha-authority-term m)
                       :fence-result fence-result})
            (assoc m
                   :ha-leader-fencing-pending? true
                   :ha-leader-fencing-last-error fence-result)))))))

(defn- parse-ha-clock-skew-output
  [output]
  (let [trimmed (some-> output s/trim)]
    (cond
      (s/blank? trimmed)
      {:ok? false
       :reason :invalid-output
       :message "Clock skew hook returned blank output"
       :output output}

      :else
      (try
        {:ok? true
         :clock-skew-ms (Math/abs (long (Long/parseLong trimmed)))}
        (catch NumberFormatException _
          {:ok? false
           :reason :invalid-output
           :message "Clock skew hook output must be an integer millisecond value"
           :output output})))))

(defn ^:redef run-ha-clock-skew-hook
  [db-name m]
  (let [budget-ms (long (or (:ha-clock-skew-budget-ms m)
                            c/*ha-clock-skew-budget-ms*))
        {:keys [cmd timeout-ms retries retry-delay-ms]} (:ha-clock-skew-hook m)]
    (if-not (and (vector? cmd) (seq cmd))
      {:ok? true
       :skipped? true
       :paused? false
       :reason :clock-skew-hook-unconfigured
       :budget-ms budget-ms}
      (let [env {"DTLV_DB_NAME" db-name
                 "DTLV_HA_NODE_ID" (str (or (:ha-node-id m) ""))
                 "DTLV_HA_ENDPOINT"
                 (str (or (:ha-local-endpoint m) ""))
                 "DTLV_CLOCK_SKEW_BUDGET_MS" (str budget-ms)}
            max-attempts (inc (long (or retries 0)))
            timeout-ms (long (or timeout-ms 3000))
            retry-delay-ms (long (or retry-delay-ms 1000))]
        (loop [attempt 1]
          (let [result (run-command-with-timeout cmd env timeout-ms)
                parsed (if (:ok? result)
                         (merge result
                                (parse-ha-clock-skew-output
                                 (:output result)))
                         result)
                paused? (if (:ok? parsed)
                          (> (long (or (:clock-skew-ms parsed) 0))
                             budget-ms)
                          true)]
            (if (:ok? parsed)
              (assoc parsed
                     :attempt attempt
                     :budget-ms budget-ms
                     :paused? paused?
                     :reason (if paused?
                               :clock-skew-budget-breached
                               :clock-skew-within-budget))
              (if (< attempt max-attempts)
                (do
                  (Thread/sleep retry-delay-ms)
                  (recur (u/long-inc attempt)))
                (assoc parsed
                       :attempt attempt
                       :budget-ms budget-ms
                       :paused? true)))))))))

(defn- refresh-ha-clock-skew-state
  [db-name m]
  (let [budget-ms (long (or (:ha-clock-skew-budget-ms m)
                            c/*ha-clock-skew-budget-ms*))
        role (:ha-role m)
        clock-skew-hook (:ha-clock-skew-hook m)
        hook-configured?
        (let [cmd (:cmd clock-skew-hook)]
          (and (vector? cmd) (seq cmd)))]
    (if (or (#{:follower :candidate} role)
            (and (= :leader role) hook-configured?))
      (let [result (try
                     (run-ha-clock-skew-hook db-name m)
                     (catch Exception e
                       {:ok? false
                        :paused? true
                        :reason :exception
                        :budget-ms budget-ms
                        :message (ex-message e)}))
            now-ms (System/currentTimeMillis)
            next-m (assoc m
                          :ha-clock-skew-budget-ms budget-ms
                          :ha-clock-skew-paused? (true? (:paused? result))
                          :ha-clock-skew-last-check-ms now-ms
                          :ha-clock-skew-last-observed-ms (:clock-skew-ms result)
                          :ha-clock-skew-last-result result)]
        (if (and (= :leader role)
                 (true? (:paused? result)))
          (demote-ha-leader db-name next-m
                            :clock-skew-paused
                            {:budget-ms budget-ms
                             :clock-skew-result result}
                            now-ms)
          next-m))
      (assoc m
             :ha-clock-skew-budget-ms budget-ms
             :ha-clock-skew-paused? false))))

(defn- ha-clock-skew-promotion-failure-details
  [m]
  (let [budget-ms (long (or (:ha-clock-skew-budget-ms m)
                            c/*ha-clock-skew-budget-ms*))
        result (:ha-clock-skew-last-result m)
        base {:budget-ms budget-ms
              :last-check-ms (:ha-clock-skew-last-check-ms m)
              :check result}]
    (if (= :clock-skew-budget-breached (:reason result))
      (assoc base
             :reason :clock-skew-budget-breached
             :clock-skew-ms (:clock-skew-ms result))
      (assoc base
             :reason :clock-skew-check-failed))))

(defn- ha-authority-read-fresh?
  [m now-ms]
  (let [ok? (true? (:ha-authority-read-ok? m))
        last-ms (:ha-last-authority-refresh-ms m)
        timeout-ms (long (or (:ha-lease-timeout-ms m)
                             c/*ha-lease-timeout-ms*))]
    (and ok?
         (or (not (integer? last-ms))
             (< (- (long now-ms) (long last-ms))
                timeout-ms)))))

(defn- current-authority-observation
  [m now-ms]
  (when (ha-authority-read-fresh? m now-ms)
    (authority-observation-from-state m)))

(defn- promotion-authority-observation
  [m now-ms]
  (or (current-authority-observation m now-ms)
      (observe-authority-state m)))

(defn- ha-authority-read-failure-details
  ([m]
   (ha-authority-read-failure-details m (ha-now-ms)))
  ([m now-ms]
   (or (when (and (true? (:ha-authority-read-ok? m))
                  (integer? (:ha-last-authority-refresh-ms m))
                  (not (ha-authority-read-fresh? m now-ms)))
         {:reason :authority-read-stale
          :last-authority-refresh-ms (:ha-last-authority-refresh-ms m)
          :timeout-ms (long (or (:ha-lease-timeout-ms m)
                                c/*ha-lease-timeout-ms*))})
       (:ha-authority-read-error m)
       {:reason :authority-read-failed})))

(defn- ha-rejoin-promotion-failure-details
  [m]
  {:reason :rejoin-in-progress
   :blocked-until-ms (:ha-rejoin-promotion-blocked-until-ms m)
   :authority-owner-node-id (:ha-authority-owner-node-id m)
   :local-last-applied-lsn (ha-local-last-applied-lsn m)
   :leader-last-applied-lsn
   (long (or (get-in m [:ha-authority-lease :leader-last-applied-lsn]) 0))
   :ha-follower-last-error (:ha-follower-last-error m)
   :ha-follower-degraded? (:ha-follower-degraded? m)})

(defn- clear-ha-rejoin-promotion-block
  [m]
  (-> m
      (assoc :ha-rejoin-promotion-blocked? false
             :ha-rejoin-promotion-blocked-until-ms nil
             :ha-rejoin-promotion-cleared-ms (System/currentTimeMillis))
      (dissoc :ha-rejoin-started-at-ms)))

(defn- maybe-clear-ha-rejoin-promotion-block
  [m now-ms]
  (if-not (:ha-rejoin-promotion-blocked? m)
    m
    (let [blocked-until-ms (long (or (:ha-rejoin-promotion-blocked-until-ms m) 0))
          owner-node-id (:ha-authority-owner-node-id m)
          local-node-id (:ha-node-id m)
          leader-lsn (long (or (get-in m [:ha-authority-lease
                                          :leader-last-applied-lsn])
                               0))
          local-lsn (ha-local-last-applied-lsn m)
          authority-fresh? (ha-authority-read-fresh? m now-ms)
          lag-lsn (nonnegative-long-diff leader-lsn local-lsn)
          lag-ok? (<= lag-lsn
                      (long (or (:ha-max-promotion-lag-lsn m) 0)))
          synced? (and authority-fresh?
                       (integer? owner-node-id)
                       (not= owner-node-id local-node-id)
                       (not (:ha-follower-degraded? m))
                       (nil? (:ha-follower-last-error m))
                       lag-ok?)]
      (cond
        synced?
        (clear-ha-rejoin-promotion-block m)

        (and (pos? blocked-until-ms)
             (>= (long now-ms) blocked-until-ms))
        (clear-ha-rejoin-promotion-block m)

        :else
        m))))

(defn- fail-ha-candidate
  [m reason details]
  (-> m
      clear-ha-candidate-state
      (assoc :ha-role :follower
             :ha-promotion-last-failure reason
             :ha-promotion-failure-details details)))

(defn- maybe-enter-ha-candidate
  [m now-ms]
  (cond
    (not (contains? #{:follower :candidate} (:ha-role m)))
    m

    (and (= :candidate (:ha-role m))
         (not (lease/lease-expired? (:ha-authority-lease m) now-ms)))
    (-> m
        clear-ha-candidate-state
        (assoc :ha-role :follower))

    (not= :follower (:ha-role m))
    m

    (not (lease/lease-expired? (:ha-authority-lease m) now-ms))
    m

    (or (true? (:ha-db-identity-mismatch? m))
        (true? (:ha-membership-mismatch? m))
        (not= (:ha-membership-hash m)
              (:ha-authority-membership-hash m)))
    m

    (true? (:ha-clock-skew-paused? m))
    (assoc m
           :ha-promotion-last-failure :clock-skew-paused
           :ha-promotion-failure-details
           (ha-clock-skew-promotion-failure-details m))

    (true? (:ha-follower-degraded? m))
    (assoc m
           :ha-promotion-last-failure :follower-degraded
           :ha-promotion-failure-details
           {:reason (:ha-follower-degraded-reason m)
            :details (:ha-follower-degraded-details m)})

    (true? (:ha-rejoin-promotion-blocked? m))
    (assoc m
           :ha-promotion-last-failure :rejoin-in-progress
           :ha-promotion-failure-details
           (ha-rejoin-promotion-failure-details m))

    (not (ha-authority-read-fresh? m now-ms))
    (assoc m
           :ha-promotion-last-failure :authority-read-failed
           :ha-promotion-failure-details
           (ha-authority-read-failure-details m now-ms))

    :else
    (if-let [delay-ms (ha-promotion-delay-ms m)]
      (assoc m :ha-role :candidate
             :ha-candidate-since-ms now-ms
             :ha-candidate-delay-ms delay-ms
             :ha-candidate-rank-index (ha-promotion-rank-index m))
      (fail-ha-candidate m :missing-promotion-rank
                         {:ha-node-id (:ha-node-id m)
                          :ha-members (:ha-members m)}))))

(defn- try-promote-with-cas
  [db-name m authority db-identity observed-lease version now-ms lag-check]
  (let [local-start-ms now-ms
        local-start-nanos (ha-now-nanos)
        acquire
        (ctrl/try-acquire-lease
         authority
         {:db-identity db-identity
          :leader-node-id (:ha-node-id m)
          :leader-endpoint (:ha-local-endpoint m)
          :lease-renew-ms (:ha-lease-renew-ms m)
          :lease-timeout-ms (:ha-lease-timeout-ms m)
          :leader-last-applied-lsn (:local-last-applied-lsn lag-check)
          :now-ms local-start-ms
          :observed-version version
          :observed-lease observed-lease})]
    (if (:ok? acquire)
      (let [{acquired-lease :lease
             acquired-version :version
             :keys [term authority-now-ms]} acquire
            observed-at-ms (ha-now-ms)]
        (-> m
            clear-ha-candidate-state
            (assoc :ha-role :leader
                   :ha-leader-term term
                   :ha-authority-lease acquired-lease
                   :ha-authority-version acquired-version
                   :ha-authority-now-ms authority-now-ms
                   :ha-lease-local-deadline-ms
                   (authority-lease-local-deadline-ms
                    acquired-lease authority-now-ms local-start-ms)
                   :ha-lease-local-deadline-nanos
                   (authority-lease-local-deadline-nanos
                    acquired-lease authority-now-ms local-start-nanos)
                   :ha-authority-owner-node-id (:leader-node-id acquired-lease)
                   :ha-authority-term (:term acquired-lease)
                   :ha-lease-until-ms (:lease-until-ms acquired-lease)
                   :ha-last-authority-refresh-ms observed-at-ms
                   :ha-db-identity-mismatch? false
                   :ha-membership-mismatch? false
                   :ha-promotion-last-failure nil
                   :ha-promotion-failure-details nil
                   :ha-leader-fencing-pending? true
                   :ha-leader-fencing-observed-lease observed-lease
                   :ha-leader-fencing-last-error nil)
            (maybe-complete-ha-leader-fencing db-name)))
      (fail-ha-candidate m :lease-cas-failed {:acquire acquire}))))

(defn- finalize-ha-candidate-promotion
  [db-name m authority db-identity lease version now-ms lag-input]
  (if-not (lease/lease-expired? lease now-ms)
    (fail-ha-candidate m :lease-not-expired
                       {:lease lease})
    (let [lag-check
          (ha-promotion-lag-guard m
                                  (:effective-lease lag-input)
                                  (:local-last-applied-lsn lag-input))]
      (if-not (:ok? lag-check)
        (fail-ha-candidate m :lag-guard-failed
                           {:phase :pre-cas
                            :lag lag-check
                            :leader-lag-input lag-input})
        (try-promote-with-cas
         db-name
         m
         authority
         db-identity
         lease
         version
         now-ms
         lag-check)))))

(defn- maybe-promote-after-authority-observation
  [db-name m authority db-identity obs now-ms]
  (let [m1 (-> (apply-authority-observation m obs now-ms)
               (dissoc :ha-candidate-pre-cas-wait-until-ms
                       :ha-candidate-pre-cas-observed-version
                       :ha-promotion-wait-before-cas-ms))
        lease (:lease obs)
        version (:version obs)
        db-identity-mismatch? (:db-identity-mismatch? obs)
        membership-mismatch? (:membership-mismatch? obs)]
    (cond
      db-identity-mismatch?
      (fail-ha-candidate m1 :db-identity-mismatch
                         {:local-db-identity db-identity
                          :authority-lease lease})

      membership-mismatch?
      (fail-ha-candidate m1 :membership-hash-mismatch
                         {:ha-membership-hash (:ha-membership-hash m1)
                          :ha-authority-membership-hash
                          (:authority-membership-hash obs)})

      (not (lease/lease-expired? lease now-ms))
      (fail-ha-candidate m1 :lease-not-expired
                         {:lease lease})

      :else
      (let [lag-input (pre-cas-lag-input db-name m1 lease now-ms)
            reachable? (:leader-endpoint-reachable? lag-input)]
        (if (or reachable? (bootstrap-empty-lease? lease))
          (finalize-ha-candidate-promotion
           db-name m1 authority db-identity lease version now-ms lag-input)
          (let [wait-info (maybe-wait-unreachable-leader-before-pre-cas!
                           m1 lease)
                wait-ms (long (or (:wait-ms wait-info)
                                  (:slept-ms wait-info)
                                  0))]
            (if (pos? wait-ms)
              (assoc m1
                     :ha-candidate-pre-cas-wait-until-ms
                     (:wait-until-ms wait-info)
                     :ha-candidate-pre-cas-observed-version version
                     :ha-promotion-wait-before-cas-ms wait-ms)
              (finalize-ha-candidate-promotion
               db-name m1 authority db-identity lease version now-ms lag-input))))))))

(def ^:private restart-ha-candidate-promotion
  ::restart-ha-candidate-promotion)

(defn- maybe-resume-ha-candidate-pre-cas-wait
  [db-name m now-ms]
  (when-let [wait-until-ms (:ha-candidate-pre-cas-wait-until-ms m)]
    (let [remaining-ms (long (max 0 (- (long wait-until-ms) (long now-ms))))]
      (if (pos? remaining-ms)
        (assoc m :ha-promotion-wait-before-cas-ms remaining-ms)
        (let [observed-version (:ha-candidate-pre-cas-observed-version m)
              current-obs (promotion-authority-observation m now-ms)
              m1 (dissoc m
                         :ha-candidate-pre-cas-wait-until-ms
                         :ha-candidate-pre-cas-observed-version
                         :ha-promotion-wait-before-cas-ms)
              now-ms-2 (System/currentTimeMillis)]
          (if (= observed-version (:version current-obs))
            (maybe-promote-after-authority-observation
             db-name
             m1
             (:ha-authority m1)
             (:ha-db-identity m1)
             current-obs
             now-ms-2)
            restart-ha-candidate-promotion))))))

(defn- attempt-ha-candidate-promotion
  [db-name m now-ms]
  (let [observed-lease (:ha-authority-lease m)]
    (cond
      (or (true? (:ha-db-identity-mismatch? m))
          (true? (:ha-membership-mismatch? m))
          (not= (:ha-membership-hash m)
                (:ha-authority-membership-hash m)))
      (fail-ha-candidate m :membership-hash-mismatch
                         {:ha-membership-hash (:ha-membership-hash m)
                          :ha-authority-membership-hash
                          (:ha-authority-membership-hash m)})

      (not (lease/lease-expired? observed-lease now-ms))
      (fail-ha-candidate m :lease-not-expired
                         {:lease observed-lease})

      (true? (:ha-clock-skew-paused? m))
      (fail-ha-candidate m :clock-skew-paused
                         (ha-clock-skew-promotion-failure-details m))

      (false? (:ha-authority-read-ok? m))
      (fail-ha-candidate m :authority-read-failed
                         (ha-authority-read-failure-details m))

      :else
      (let [promotion-now-ms (System/currentTimeMillis)]
        (maybe-promote-after-authority-observation
         db-name
         m
         (:ha-authority m)
         (:ha-db-identity m)
         (promotion-authority-observation
          m
          promotion-now-ms)
         promotion-now-ms)))))

(defn- maybe-promote-ha-candidate
  [db-name m now-ms]
  (if (not= :candidate (:ha-role m))
    m
    (try
      (let [candidate-since-ms (long (or (:ha-candidate-since-ms m) now-ms))
            candidate-delay-ms (long (or (:ha-candidate-delay-ms m) 0))
            elapsed-ms (max 0 (- (long now-ms) candidate-since-ms))]
        (if (< elapsed-ms candidate-delay-ms)
          m
          (let [resume-result (maybe-resume-ha-candidate-pre-cas-wait
                               db-name m now-ms)]
            (cond
              (map? resume-result)
              resume-result

              (= restart-ha-candidate-promotion resume-result)
              (attempt-ha-candidate-promotion
               db-name
               (dissoc m
                       :ha-candidate-pre-cas-wait-until-ms
                       :ha-candidate-pre-cas-observed-version
                       :ha-promotion-wait-before-cas-ms)
               now-ms)

              :else
              (attempt-ha-candidate-promotion db-name m now-ms)))))
      (catch Exception e
        (fail-ha-candidate m :promotion-exception
                           {:message (ex-message e)})))))

(defn ^:redef ha-follower-sync-step
  [db-name m]
  (if-not (:ha-authority m)
    m
    (let [m0 (refresh-ha-local-watermarks m)]
      (cond-> (if (= :follower (:ha-role m0))
                (sync-ha-follower-state db-name m0 (ha-now-ms))
                m0)
        :always
        (dissoc ha-local-watermark-snapshot-key)))))

(defn- advance-ha-follower-or-candidate
  [db-name m]
  (let [state-now-ms (ha-now-ms)]
    (if (or (ha-demotion-draining? m state-now-ms)
            (not (contains? #{:follower :candidate} (:ha-role m))))
      m
      (let [m1 (maybe-clear-ha-rejoin-promotion-block m state-now-ms)
            m2 (maybe-enter-ha-candidate m1 state-now-ms)]
        (maybe-promote-ha-candidate db-name m2 (ha-now-ms))))))

(defn refresh-ha-authority-state
  [db-name m]
  (if-not (:ha-authority m)
    m
    (try
      (let [db-identity (:ha-db-identity m)
            now-ms (ha-now-ms)
            observation (observe-authority-state m)
            {:keys [lease authority-membership-hash
                    db-identity-mismatch? membership-mismatch?]}
            observation
            m1 (-> m
                   (apply-authority-observation observation now-ms)
                   apply-authority-read-success)]
        (cond
          (and db-identity-mismatch? (= :leader (:ha-role m1)))
          (demote-ha-leader db-name m1
                            :db-identity-mismatch
                            {:local-db-identity db-identity
                             :authority-lease lease}
                            now-ms)

          (and membership-mismatch? (= :leader (:ha-role m1)))
          (demote-ha-leader db-name m1
                            :membership-hash-mismatch
                            {:local-membership-hash (:ha-membership-hash m1)
                             :authority-membership-hash
                             authority-membership-hash}
                            now-ms)

          :else
          m1))
      (catch Exception e
        (let [error (authority-read-error e)
              failed-m (apply-authority-read-failure m error)]
          (log/warn e "HA read-lease failed"
                    {:db-name db-name
                     :ha-role (:ha-role m)})
          failed-m)))))

(defn- renew-ha-leader-state
  [db-name m]
  (if (not= :leader (:ha-role m))
    m
    (let [local-start-ms (ha-now-ms)
          local-start-nanos (ha-now-nanos)
          renew-timeout-ms (ha-renew-timeout-ms
                            m
                            local-start-ms
                            local-start-nanos)
          term (:ha-leader-term m)]
      (if-not (and (integer? term) (pos? ^long term))
        (demote-ha-leader db-name m :missing-leader-term nil local-start-ms)
        (let [result (ctrl/renew-lease
                      (:ha-authority m)
                      {:db-identity (:ha-db-identity m)
                       :leader-node-id (:ha-node-id m)
                       :leader-endpoint (:ha-local-endpoint m)
                       :term term
                       :lease-renew-ms (:ha-lease-renew-ms m)
                       :lease-timeout-ms (:ha-lease-timeout-ms m)
                       :leader-last-applied-lsn (long (or (:ha-leader-last-applied-lsn m) 0))
                       :now-ms local-start-ms
                       :timeout-ms renew-timeout-ms})]
          (if (:ok? result)
            (let [{:keys [lease version authority-now-ms]} result
                  observed-at-ms (ha-now-ms)]
              (-> m
                  apply-authority-read-success
                  (assoc :ha-authority-lease lease
                         :ha-authority-version version
                         :ha-authority-now-ms authority-now-ms
                         :ha-lease-local-deadline-ms
                         (authority-lease-local-deadline-ms
                          lease authority-now-ms local-start-ms)
                         :ha-lease-local-deadline-nanos
                         (authority-lease-local-deadline-nanos
                          lease authority-now-ms local-start-nanos)
                         :ha-authority-owner-node-id (:leader-node-id lease)
                         :ha-authority-term (:term lease)
                         :ha-lease-until-ms (:lease-until-ms lease)
                         :ha-last-authority-refresh-ms observed-at-ms)
                  (maybe-complete-ha-leader-fencing db-name)))
            (demote-ha-leader db-name m
                              :renew-failed
                              {:reason (:reason result)}
                              (ha-now-ms))))))))

(defn- maybe-demote-on-refresh-timeout
  [db-name m now-ms]
  (let [timeout-ms (:ha-lease-timeout-ms m)
        last-ms (:ha-last-authority-refresh-ms m)]
    (if (and (= :leader (:ha-role m))
             (integer? timeout-ms)
             (pos? ^long timeout-ms)
             (integer? last-ms)
             (>= (- (long now-ms) (long last-ms))
                 (long timeout-ms)))
      (demote-ha-leader db-name m :authority-refresh-timeout nil now-ms)
      m)))

(defn ha-renew-step
  [db-name m]
  (if-not (:ha-authority m)
    m
    (let [started-demoting? (= :demoting (:ha-role m))
          m0 (refresh-ha-local-watermarks m)
          m1 (refresh-ha-authority-state db-name m0)
          m2 (try
               (renew-ha-leader-state db-name m1)
               (catch Exception e
                 (demote-ha-leader db-name m1
                                   :renew-exception
                                   {:message (ex-message e)}
                                   (ha-now-ms))))
          m3 (refresh-ha-clock-skew-state db-name m2)
          m4 (advance-ha-follower-or-candidate db-name m3)
          end-now-ms (ha-now-ms)]
      (-> (maybe-demote-on-refresh-timeout db-name m4 end-now-ms)
          (maybe-finish-ha-demotion end-now-ms started-demoting?)
          (dissoc ha-local-watermark-snapshot-key)))))

(defn clear-ha-runtime-state
  [m]
  (dissoc m
          ha-local-watermark-snapshot-key
          :ha-authority :ha-db-identity :ha-membership-hash
          :ha-authority-membership-hash
          :ha-db-identity-mismatch? :ha-membership-mismatch?
          :ha-members :ha-members-sorted
          :ha-node-id :ha-local-endpoint
          :ha-lease-renew-ms :ha-lease-timeout-ms
          :ha-promotion-base-delay-ms :ha-promotion-rank-delay-ms
          :ha-max-promotion-lag-lsn :ha-demotion-drain-ms
          :ha-follower-max-batch-records
          :ha-follower-target-batch-bytes
          :ha-fencing-hook
          :ha-clock-skew-budget-ms :ha-clock-skew-hook
          :ha-authority-lease :ha-authority-version
          :ha-authority-now-ms
          :ha-authority-owner-node-id :ha-authority-term
          :ha-lease-until-ms
          :ha-lease-local-deadline-ms
          :ha-lease-local-deadline-nanos
          :ha-last-authority-refresh-ms
          :ha-authority-read-ok?
          :ha-authority-read-error
          :ha-client-cache-state
          :ha-clock-skew-paused?
          :ha-clock-skew-last-check-ms
          :ha-clock-skew-last-observed-ms
          :ha-clock-skew-last-result
          :ha-demotion-drain-until-ms
          :ha-role :ha-leader-term
          :ha-leader-fencing-pending?
          :ha-leader-fencing-observed-lease
          :ha-leader-fencing-last-error
          :ha-local-last-applied-lsn
          :ha-follower-next-lsn :ha-follower-last-batch-size
          :ha-follower-last-batch-estimated-bytes
          :ha-follower-requested-batch-records
          :ha-follower-last-sync-ms :ha-follower-leader-endpoint
          :ha-follower-source-endpoint :ha-follower-source-order
          :ha-follower-last-bootstrap-ms
          :ha-follower-bootstrap-source-endpoint
          :ha-follower-bootstrap-snapshot-last-applied-lsn
          :ha-follower-sync-backoff-ms
          :ha-follower-next-sync-not-before-ms
          :ha-follower-degraded? :ha-follower-degraded-reason
          :ha-follower-degraded-details :ha-follower-degraded-since-ms
          :ha-follower-last-error :ha-follower-last-error-details
          :ha-follower-last-error-ms
          :ha-promotion-last-failure :ha-promotion-failure-details
          :ha-candidate-since-ms :ha-candidate-delay-ms
          :ha-candidate-rank-index
          :ha-candidate-pre-cas-wait-until-ms
          :ha-candidate-pre-cas-observed-version
          :ha-promotion-wait-before-cas-ms
          :ha-rejoin-promotion-blocked?
          :ha-rejoin-promotion-blocked-until-ms
          :ha-rejoin-promotion-cleared-ms
          :ha-rejoin-started-at-ms
          :ha-probe-executor
          :ha-renew-loop-running?
          :ha-follower-loop-running?))

(def ^:private ha-write-command-types
  #{:set-schema
    :swap-attr
    :del-attr
    :rename-attr
    :load-datoms
    :tx-data
    :tx-data+db-info
    :open-transact
    :close-transact
    :abort-transact
    :open-transact-kv
    :close-transact-kv
    :abort-transact-kv
    :transact-kv
    :open-dbi
    :clear-dbi
    :drop-dbi
    :set-env-flags
    :add-doc
    :remove-doc
    :clear-docs
    :add-vec
    :remove-vec
    :persist-vecs
    :clear-vecs
    :kv-re-index
    :datalog-re-index})

(defn ha-write-message?
  [{:keys [type]}]
  (boolean (ha-write-command-types type)))

(defn ha-write-admission-error
  [dbs {:keys [type args]}]
  (when (ha-write-message? {:type type})
    (let [db-name (nth args 0 nil)
          m (and db-name (get dbs db-name))]
      (when (and m (:ha-authority m))
        (let [now-ms (ha-now-ms)
              role (:ha-role m)
              local-node-id (:ha-node-id m)
              owner-node-id (:ha-authority-owner-node-id m)
              owner-endpoint (or (get-in m [:ha-authority-lease :leader-endpoint])
                                 (some->> (:ha-members m)
                                          (filter #(= owner-node-id
                                                      (:node-id %)))
                                          first
                                          :endpoint))
              ordered-endpoints
              (into []
                    (comp
                     (map :endpoint)
                     (remove nil?)
                     (remove s/blank?))
                    (ordered-ha-members m))
              retry-endpoints
              (->> (cond-> []
                     (and (string? owner-endpoint)
                          (not (s/blank? owner-endpoint)))
                     (conj owner-endpoint)
                     :always
                     (into ordered-endpoints))
                   distinct
                   vec)
              common-meta
              {:db-name db-name
               :ha-retry-endpoints retry-endpoints
               :ha-authoritative-leader-endpoint owner-endpoint
               :ha-authoritative-leader-node-id owner-node-id}
              authority-read-failure
              (when-not (ha-authority-read-fresh? m now-ms)
                (ha-authority-read-failure-details m now-ms))
              db-id-mismatch? (true? (:ha-db-identity-mismatch? m))
              membership-mismatch? (true? (:ha-membership-mismatch? m))
              lease-until-ms (:ha-lease-until-ms m)
              lease-local-deadline-ms (:ha-lease-local-deadline-ms m)
              lease-local-deadline-nanos (:ha-lease-local-deadline-nanos m)
              leader-term (:ha-leader-term m)
              authority-term (:ha-authority-term m)
              now-nanos (ha-now-nanos)
              leader-fencing-pending? (true? (:ha-leader-fencing-pending? m))
              leader-fencing-error (:ha-leader-fencing-last-error m)]
          (cond
            (= role :demoting)
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :demoting
                    :ha-role role
                    :retryable? true})

            (not= role :leader)
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :not-leader
                    :ha-role role
                    :retryable? true})

            (true? (:ha-clock-skew-paused? m))
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :clock-skew-paused
                    :ha-clock-skew-last-result
                    (:ha-clock-skew-last-result m)
                    :ha-clock-skew-last-check-ms
                    (:ha-clock-skew-last-check-ms m)
                    :ha-clock-skew-last-observed-ms
                    (:ha-clock-skew-last-observed-ms m)
                    :retryable? true})

            authority-read-failure
            (merge common-meta
                   authority-read-failure
                   {:error :ha/write-rejected
                    :reason (:reason authority-read-failure)
                    :ha-authority-read-error authority-read-failure
                    :retryable? true})

            leader-fencing-pending?
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :fencing-pending
                    :ha-fencing-error leader-fencing-error
                    :retryable? true})

            db-id-mismatch?
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :db-identity-mismatch
                    :retryable? false})

            membership-mismatch?
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :membership-hash-mismatch
                    :ha-membership-hash (:ha-membership-hash m)
                    :ha-authority-membership-hash
                    (:ha-authority-membership-hash m)
                    :retryable? false})

            (not= local-node-id owner-node-id)
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :owner-mismatch
                    :ha-node-id local-node-id
                    :ha-authority-owner-node-id owner-node-id
                    :retryable? true})

            (or (and (integer? lease-local-deadline-nanos)
                     (>= (long now-nanos)
                         (long lease-local-deadline-nanos)))
                (and (not (integer? lease-local-deadline-nanos))
                     (integer? lease-local-deadline-ms)
                     (>= (long now-ms) (long lease-local-deadline-ms)))
                (and (not (integer? lease-local-deadline-nanos))
                     (not (integer? lease-local-deadline-ms))
                     (or (nil? lease-until-ms)
                         (>= (long now-ms) (long lease-until-ms)))))
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :lease-expired
                    :lease-until-ms lease-until-ms
                    :lease-local-deadline-ms lease-local-deadline-ms
                    :lease-local-deadline-nanos lease-local-deadline-nanos
                    :ha-authority-now-ms (:ha-authority-now-ms m)
                    :now-ms now-ms
                    :now-nanos now-nanos
                    :retryable? true})

            (or (nil? leader-term)
                (nil? authority-term)
                (not= leader-term authority-term))
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :term-mismatch
                    :ha-leader-term leader-term
                    :ha-authority-term authority-term
                    :retryable? true})

            :else nil))))))

(defn- startup-read-ha-authority-state
  [db-name authority db-identity]
  (try
    (let [{:keys [lease version authority-now-ms]}
          (ctrl/read-state-for-startup authority db-identity)]
      {:ok? true
       :lease lease
       :version version
       :authority-now-ms authority-now-ms
       :error nil})
    (catch Exception e
      (log/warn e "HA startup read-lease failed; deferring to renew loop"
                {:db-name db-name})
      {:ok? false
       :lease nil
       :version nil
       :authority-now-ms nil
       :error {:reason :startup-authority-read-failed
               :message (ex-message e)
               :data (ex-data e)}})))

(defn start-ha-authority
  [db-name ha-opts]
  (let [validation-opts (cond-> ha-opts
                          (= :in-memory
                             (get-in ha-opts [:ha-control-plane :backend]))
                          (assoc-in [:ha-control-plane :backend]
                                    :sofa-jraft)

                          (nil? (get-in ha-opts [:ha-control-plane :rpc-timeout-ms]))
                          (assoc-in [:ha-control-plane :rpc-timeout-ms] 2000)

                          (nil? (get-in ha-opts [:ha-control-plane :election-timeout-ms]))
                          (assoc-in [:ha-control-plane :election-timeout-ms]
                                    3000)

                          (nil? (get-in ha-opts [:ha-control-plane :operation-timeout-ms]))
                          (assoc-in [:ha-control-plane :operation-timeout-ms]
                                    5000))
        _ (vld/validate-ha-options validation-opts)
        db-identity (:db-identity ha-opts)
        node-id (:ha-node-id ha-opts)
        members (:ha-members ha-opts)
        ordered-members (some->> members (sort-by :node-id) vec)
        renew-ms (:ha-lease-renew-ms ha-opts)
        timeout-ms (:ha-lease-timeout-ms ha-opts)
        promotion-base-delay-ms
        (long (or (:ha-promotion-base-delay-ms ha-opts)
                  c/*ha-promotion-base-delay-ms*))
        promotion-rank-delay-ms
        (long (or (:ha-promotion-rank-delay-ms ha-opts)
                  c/*ha-promotion-rank-delay-ms*))
        max-promotion-lag-lsn
        (long (or (:ha-max-promotion-lag-lsn ha-opts)
                  c/*ha-max-promotion-lag-lsn*))
        demotion-drain-ms
        (long (or (:ha-demotion-drain-ms ha-opts)
                  c/*ha-demotion-drain-ms*))
        clock-skew-budget-ms
        (long (or (:ha-clock-skew-budget-ms ha-opts)
                  c/*ha-clock-skew-budget-ms*))
        follower-max-batch-records
        (long (or (:ha-follower-max-batch-records ha-opts)
                  c/*ha-follower-max-batch-records*))
        follower-target-batch-bytes
        (long (or (:ha-follower-target-batch-bytes ha-opts)
                  c/*ha-follower-target-batch-bytes*))
        cp (assoc (:ha-control-plane ha-opts)
                  :clock-skew-budget-ms clock-skew-budget-ms)
        fencing-hook (:ha-fencing-hook ha-opts)
        clock-skew-hook (:ha-clock-skew-hook ha-opts)
        local-endpoint (local-ha-endpoint ha-opts)
        client-cache-state (cache/new-ha-client-cache-state db-name)
        probe-executor (new-ha-probe-executor db-name)
        authority (ctrl/new-authority cp)]
    (try
      (ctrl/start-authority! authority)
      (when (or (nil? db-identity) (s/blank? db-identity))
        (u/raise "HA db identity is missing for consensus mode"
                 {:error :ha/missing-db-identity
                  :db-name db-name}))
      (when (or (nil? local-endpoint) (s/blank? local-endpoint))
        (u/raise "HA local endpoint is missing for consensus mode"
                 {:error :ha/missing-local-endpoint
                  :db-name db-name
                  :ha-node-id node-id}))
      (let [local-start-ms (ha-now-ms)
            local-start-nanos (ha-now-nanos)
            derived-hash (vld/derive-ha-membership-hash ha-opts)
            init-result (ctrl/init-membership-hash! authority derived-hash)
            startup-read
            (startup-read-ha-authority-state db-name authority db-identity)
            observed-at-ms (ha-now-ms)
            {:keys [lease version authority-now-ms error]} startup-read
            _ (when (and lease (not= db-identity (:db-identity lease)))
                (u/raise "HA lease db identity mismatch at startup"
                         {:error :ha/db-identity-mismatch
                          :db-name db-name
                          :local-db-identity db-identity
                          :authority-lease lease}))
            lease-local-deadline-ms
            (authority-lease-local-deadline-ms
             lease authority-now-ms local-start-ms)
            lease-local-deadline-nanos
            (authority-lease-local-deadline-nanos
             lease authority-now-ms local-start-nanos)
            local-authority-owner? (and lease
                                        (= node-id (:leader-node-id lease))
                                        (not (lease/lease-expired?
                                              lease
                                              observed-at-ms))
                                        (= db-identity (:db-identity lease)))
            rejoin-promotion-blocked?
            (and lease
                 (integer? (:leader-node-id lease))
                 (not= node-id (:leader-node-id lease))
                 (not (lease/lease-expired? lease observed-at-ms))
                 (= db-identity (:db-identity lease)))
            rejoin-promotion-blocked-until-ms
            (when rejoin-promotion-blocked?
              (long (max (+ (long observed-at-ms) (long renew-ms))
                         (+ (long (or (:lease-until-ms lease)
                                      observed-at-ms))
                            (long renew-ms)))))]
        (when-not (:ok? init-result)
          (u/raise "HA membership hash mismatch with authoritative control plane"
                   {:error :ha/membership-hash-mismatch
                    :db-name db-name
                    :derived-hash derived-hash
                    :authority init-result}))
        (when local-authority-owner?
          (log/info "HA startup resumed local authority ownership as leader"
                    {:db-name db-name
                     :ha-node-id node-id
                     :term (:term lease)
                     :lease-until-ms (:lease-until-ms lease)}))
        (cond-> {:ha-authority authority
                 :ha-db-identity db-identity
                 :ha-membership-hash derived-hash
                 :ha-authority-membership-hash (:membership-hash init-result)
                 :ha-db-identity-mismatch? false
                 :ha-membership-mismatch? false
                 :ha-client-cache-state client-cache-state
                 :ha-members members
                 :ha-members-sorted ordered-members
                 :ha-node-id node-id
                 :ha-local-endpoint local-endpoint
                 :ha-lease-renew-ms renew-ms
                 :ha-lease-timeout-ms timeout-ms
                 :ha-promotion-base-delay-ms promotion-base-delay-ms
                 :ha-promotion-rank-delay-ms promotion-rank-delay-ms
                 :ha-max-promotion-lag-lsn max-promotion-lag-lsn
                 :ha-client-credentials (:ha-client-credentials ha-opts)
                 :ha-demotion-drain-ms demotion-drain-ms
                 :ha-follower-max-batch-records follower-max-batch-records
                 :ha-follower-target-batch-bytes follower-target-batch-bytes
                 :ha-fencing-hook fencing-hook
                 :ha-clock-skew-budget-ms clock-skew-budget-ms
                 :ha-clock-skew-hook clock-skew-hook
                 :ha-clock-skew-paused? false
                 :ha-clock-skew-last-check-ms nil
                 :ha-clock-skew-last-observed-ms nil
                 :ha-clock-skew-last-result nil
                 :ha-authority-lease lease
                 :ha-authority-version version
                 :ha-authority-now-ms authority-now-ms
                 :ha-authority-owner-node-id (:leader-node-id lease)
                 :ha-authority-term (:term lease)
                 :ha-lease-until-ms (:lease-until-ms lease)
                 :ha-lease-local-deadline-ms lease-local-deadline-ms
                 :ha-lease-local-deadline-nanos lease-local-deadline-nanos
                 :ha-authority-read-ok? (:ok? startup-read)
                 :ha-authority-read-error error
                 :ha-last-authority-refresh-ms observed-at-ms
                 :ha-leader-fencing-pending? false
                 :ha-leader-fencing-last-error nil
                 :ha-probe-executor probe-executor
                 :ha-rejoin-promotion-blocked? rejoin-promotion-blocked?
                 :ha-rejoin-promotion-blocked-until-ms
                 rejoin-promotion-blocked-until-ms
                 :ha-rejoin-started-at-ms
                 (when rejoin-promotion-blocked? observed-at-ms)
                 :ha-role :follower
                 :ha-leader-term nil}
          local-authority-owner?
          (assoc :ha-role :leader
                 :ha-leader-term (:term lease))))
      (catch Exception e
        (try
          (ctrl/stop-authority! authority)
          (catch Exception stop-e
            (log/warn stop-e "Failed to stop HA authority after startup failure"
                      {:db-name db-name})))
        (cache/stop-ha-client-cache-state! db-name client-cache-state)
        (stop-ha-probe-executor! db-name probe-executor)
        (throw e)))))

(defn stop-ha-authority
  [db-name m]
  (try
    (when-let [authority (:ha-authority m)]
      (try
        (ctrl/stop-authority! authority)
        (catch Exception e
          (log/warn e "Failed to stop HA authority" {:db-name db-name}))))
    (finally
      (cache/stop-ha-client-cache-state! db-name (:ha-client-cache-state m))
      (stop-ha-probe-executor! db-name (:ha-probe-executor m)))))
