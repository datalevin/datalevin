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
   [clojure.edn :as edn]
   [clojure.string :as s]
   [datalevin.bits :as b]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.lease :as lease]
   [datalevin.index :as idx]
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
   [java.io File]
   [java.net ConnectException URI]
   [java.nio.channels ClosedChannelException]
   [java.nio.file Files Paths StandardCopyOption]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap TimeUnit]
   [java.util.function BiFunction Function]))

(defn consensus-ha-opts
  [store]
  (when (instance? IStore store)
    (let [opts (i/opts store)]
      (when (= :consensus-lease (:ha-mode opts))
        opts))))

(defn- local-ha-endpoint
  [ha-opts]
  (let [node-id (:ha-node-id ha-opts)]
    (:endpoint (first (filter #(= node-id (:node-id %))
                              (:ha-members ha-opts))))))

(defn- ha-request-timeout-ms
  [m max-ms]
  (let [renew-ms (long (or (:ha-lease-renew-ms m)
                           c/*ha-lease-renew-ms*))
        rpc-timeout-ms (long (or (get-in m [:ha-control-plane :rpc-timeout-ms])
                                 0))
        budget-ms (max 1000 renew-ms rpc-timeout-ms)]
    (long (min (long max-ms) budget-ms))))

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
                  :ha-leader-last-applied-lsn)))
    m))

(defn- ha-demotion-deadline-ms
  [m]
  (cond
    (integer? (:ha-demotion-drain-until-ms m))
    (long (:ha-demotion-drain-until-ms m))

    (integer? (:ha-demoted-at-ms m))
    (long (:ha-demoted-at-ms m))

    :else nil))

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

(defn- ha-promotion-rank-index
  [m]
  (let [node-id (:ha-node-id m)
        members (sort-by :node-id (:ha-members m))]
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
                        :else (or (try
                                    (.-lmdb store)
                                    (catch Throwable _
                                      nil))
                                  store))]
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
      (nil? kv-store) false
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

(declare read-ha-local-last-applied-lsn)

(defn ^:redef read-ha-snapshot-payload-lsn
  [m]
  (if-let [kv-store (local-kv-store m)]
    (read-ha-local-payload-lsn kv-store)
    0))

(defn ^:redef read-ha-local-last-applied-lsn
  [m]
  (let [state-lsn (long (or (:ha-local-last-applied-lsn m) 0))
        role (:ha-role m)]
    (try
      (if-let [kv-store (local-kv-store m)]
        (let [watermark-lsn (long (or (try
                                        (get (kv/txlog-watermarks kv-store)
                                             :last-applied-lsn)
                                        (catch Throwable t
                                          (when-not (closed-kv-race? t kv-store)
                                            (throw t))
                                          nil))
                                      0))
              snapshot-lsn (read-ha-local-snapshot-current-lsn kv-store)
              payload-lsn (if (= :leader role)
                            (read-ha-local-payload-lsn kv-store)
                            0)
              persisted-lsn (read-ha-local-persisted-lsn kv-store)
              ha-lsn (long (max persisted-lsn state-lsn))
              follower-floor-lsn (long (max persisted-lsn snapshot-lsn))
              local-truth (long (if (= :leader role)
                                  (max watermark-lsn snapshot-lsn payload-lsn)
                                  (max watermark-lsn snapshot-lsn)))]
          (cond
            (= :leader role)
            (long (max ha-lsn local-truth))

            (pos? follower-floor-lsn)
            follower-floor-lsn

            (pos? ha-lsn)
            ha-lsn

            (pos? local-truth)
            local-truth

            :else
            0))
        state-lsn)
      (catch Throwable e
        (when-not (closed-kv-race? e (local-kv-store m))
          (log/warn e "Unable to read local txlog watermarks for HA lag guard"
                    {:db-name (some-> (:store m) i/opts :db-name)}))
        state-lsn))))

(defn- ^:redef read-ha-local-watermark-lsn
  [m]
  (if-let [kv-store (local-kv-store m)]
    (long (or (try
                (get (kv/txlog-watermarks kv-store) :last-applied-lsn)
                (catch Throwable t
                  (when-not (closed-kv-race? t kv-store)
                    (throw t))
                  nil))
              0))
    0))

(defn persist-ha-runtime-local-applied-lsn!
  [m]
  (when-let [kv-store (local-kv-store m)]
    (let [persisted-lsn (read-ha-local-persisted-lsn kv-store)
          state-lsn (long (or (:ha-local-last-applied-lsn m) 0))
          lease-lsn (long (or (get-in m
                                      [:ha-authority-lease
                                       :leader-last-applied-lsn])
                              0))
          ;; `:ha/local-applied-lsn` is follower replay state. Persisting a
          ;; former leader's raw local txlog watermark here can overstate how
          ;; much replicated history that node actually shares with the current
          ;; leader on rejoin.
          applied-lsn (long (if (= :leader (:ha-role m))
                              (max persisted-lsn lease-lsn)
                              (max persisted-lsn state-lsn)))]
      (when (> applied-lsn persisted-lsn)
        (persist-ha-local-applied-lsn! m applied-lsn))
      applied-lsn)))

(defn- ha-local-last-applied-lsn
  [m]
  (if (integer? (:ha-local-last-applied-lsn m))
    (long (:ha-local-last-applied-lsn m))
    (read-ha-local-last-applied-lsn m)))

(defn- refresh-ha-local-watermarks
  [m]
  (if (local-kv-store m)
    (let [state-lsn (long (or (:ha-local-last-applied-lsn m) 0))
          role (:ha-role m)
          kv-store (local-kv-store m)
          persisted-lsn (long (read-ha-local-persisted-lsn kv-store))
          watermark-lsn (read-ha-local-watermark-lsn m)
          snapshot-lsn (read-ha-local-snapshot-current-lsn kv-store)
          payload-lsn (if (= :leader role)
                        (read-ha-local-payload-lsn kv-store)
                        0)
          follower-floor-lsn (long (max persisted-lsn snapshot-lsn))
          local-truth (long (if (= :leader role)
                              (max watermark-lsn snapshot-lsn payload-lsn)
                              (max watermark-lsn snapshot-lsn)))
          local-lsn (long (if (= :leader role)
                            (max state-lsn persisted-lsn local-truth)
                            (if (pos? follower-floor-lsn)
                              follower-floor-lsn
                              (if (pos? local-truth)
                                local-truth
                                (max state-lsn persisted-lsn)))))]
      (cond-> (assoc m :ha-local-last-applied-lsn local-lsn)
        (= :leader role)
        (assoc :ha-leader-last-applied-lsn local-lsn)))
    m))

(defn- transact-ha-follower-local!
  [m rows]
  (if-let [kv-store (local-kv-store m)]
    (#'kv/with-runtime-txlog-rollback
     kv-store
     #(i/transact-kv kv-store rows))
    (u/raise "Follower txlog replay requires a local KV store"
             {:error :ha/follower-missing-store})))

(declare bootstrap-empty-lease?)

(defn- ha-promotion-lag-guard
  [m lease]
  (let [leader-last-applied-lsn (long (or (:leader-last-applied-lsn lease) 0))
        tracked-local-lsn (ha-local-last-applied-lsn m)
        bootstrap-watermark-lsn
        (when (and (bootstrap-empty-lease? lease)
                   (zero? tracked-local-lsn))
          (read-ha-local-watermark-lsn m))
        local-last-applied-lsn (long (max tracked-local-lsn
                                          (long (or bootstrap-watermark-lsn
                                                    0))))
        lag-lsn (max 0
                     (- leader-last-applied-lsn
                        local-last-applied-lsn))
        max-lag-lsn (long (or (:ha-max-promotion-lag-lsn m) 0))]
    {:ok? (<= lag-lsn max-lag-lsn)
     :leader-last-applied-lsn leader-last-applied-lsn
     :local-last-applied-lsn local-last-applied-lsn
     :lag-lsn lag-lsn
     :max-lag-lsn max-lag-lsn}))

(defn- bootstrap-empty-lease?
  [lease]
  (and (or (nil? lease) (nil? (:leader-node-id lease)))
       (zero? (lease/observed-term lease))))

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

(defn- ^:redef close-ha-client!
  [client]
  (when client
    (try
      ;; Internal HA probes may reuse pooled clients; when we evict one from the
      ;; cache, close the pool directly instead of running the network
      ;; disconnect handshake, which reacquires a pooled connection and can mask
      ;; a successful request with a pool timeout.
      (cl/close-pool (cl/get-pool client))
      (catch Throwable _
        (try
          (cl/disconnect client)
          (catch Throwable _ nil))))))

(defonce ^:private ^ConcurrentHashMap ha-client-cache
  (ConcurrentHashMap.))

(defn- ha-client-cache-key
  [uri client-opts]
  [uri
   (long (or (:pool-size client-opts) 1))
   (long (or (:time-out client-opts) c/default-connection-timeout))])

(defn- ^:redef live-ha-client?
  [client]
  (and client
       (try
         (not (cl/disconnected? client))
         (catch Throwable _
           false))))

(defn- ^:redef open-ha-client
  [uri db-name client-opts]
  (let [client (cl/new-client uri client-opts)]
    (cl/open-database client db-name c/db-store-kv)
    client))

(defn- ^:redef ha-client-request
  [client type args writing?]
  (cl/normal-request client type args writing?))

(defn- cached-ha-client
  [uri db-name client-opts]
  (let [cache-key (ha-client-cache-key uri client-opts)
        ^ConcurrentHashMap cache ha-client-cache]
    (if-let [client (.get cache cache-key)]
      (if (live-ha-client? client)
        client
        (let [stale-client-v (volatile! nil)
              client (.compute cache cache-key
                               (reify BiFunction
                                 (apply [_ _ existing]
                                   (if (live-ha-client? existing)
                                     existing
                                     (do
                                       (when existing
                                         (vreset! stale-client-v existing))
                                       (open-ha-client uri db-name
                                                       client-opts))))))]
          (when-let [stale-client @stale-client-v]
            (when-not (identical? stale-client client)
              (close-ha-client! stale-client)))
          client))
      (.computeIfAbsent cache cache-key
                        (reify Function
                          (apply [_ _]
                            (open-ha-client uri db-name client-opts)))))))

(defn- invalidate-cached-ha-client!
  [uri client-opts client]
  (let [cache-key (ha-client-cache-key uri client-opts)
        ^ConcurrentHashMap cache ha-client-cache]
    (when (.remove cache cache-key client)
      (close-ha-client! client))))

(defn- with-cached-ha-client
  [uri db-name client-opts f]
  (let [client (cached-ha-client uri db-name client-opts)]
    (try
      (f client)
      (catch Exception e
        (invalidate-cached-ha-client! uri client-opts client)
        (throw e)))))

(defn- unknown-ha-watermark-command?
  [e]
  (let [message (or (ex-message e) "")]
    (or (= :unknown-message-type (:error (ex-data e)))
        (s/includes? message "Unknown message type :ha-watermark"))))

(defn- clear-ha-client-cache!
  []
  (let [^ConcurrentHashMap cache ha-client-cache]
    (loop []
      (let [entries (vec (.entrySet cache))]
        (when (seq entries)
          (doseq [entry entries]
            (let [entry ^java.util.Map$Entry entry
                  cache-key (.getKey entry)
                  client (.getValue entry)]
              (when (.remove cache cache-key client)
                (close-ha-client! client))))
          (when-not (.isEmpty cache)
            (recur)))))))

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

(defn- copy-dir-contents!
  [src-dir dest-dir]
  (u/create-dirs dest-dir)
  (doseq [^File f (or (u/list-files src-dir) [])]
    (let [dst (str dest-dir u/+separator+ (.getName f))]
      (if (.isDirectory f)
        (copy-dir-contents! (.getPath f) dst)
        (u/copy-file (.getPath f) dst)))))

(defn- move-path!
  [src dst]
  (let [src-path (Paths/get src (into-array String []))
        dst-path (Paths/get dst (into-array String []))
        opts (into-array java.nio.file.CopyOption
                         [StandardCopyOption/REPLACE_EXISTING
                          StandardCopyOption/ATOMIC_MOVE])]
    (try
      (Files/move src-path dst-path opts)
      (catch Exception _
        (Files/move src-path dst-path
                    (into-array java.nio.file.CopyOption
                                [StandardCopyOption/REPLACE_EXISTING]))))))

(def ^:private ha-snapshot-install-marker-suffix
  ".ha-snapshot-install.edn")

(defn- ha-snapshot-install-marker-path
  [env-dir]
  (str env-dir ha-snapshot-install-marker-suffix))

(defn- read-ha-snapshot-install-marker
  [env-dir]
  (let [marker-path (ha-snapshot-install-marker-path env-dir)]
    (when (u/file-exists marker-path)
      (let [marker (try
                     (edn/read-string (slurp marker-path))
                     (catch Exception e
                       (u/raise "HA snapshot install marker is unreadable"
                                e
                                {:error :ha/follower-snapshot-install-marker-invalid
                                 :env-dir env-dir
                                 :marker-path marker-path})))
            backup-dir (:backup-dir marker)
            stage (:stage marker)]
        (when-not (and (map? marker)
                       (string? backup-dir)
                       (not (s/blank? backup-dir))
                       (keyword? stage))
          (u/raise "HA snapshot install marker is invalid"
                   {:error :ha/follower-snapshot-install-marker-invalid
                    :env-dir env-dir
                    :marker-path marker-path
                    :marker marker}))
        (assoc marker :marker-path marker-path)))))

(defn- write-ha-snapshot-install-marker!
  [env-dir marker]
  (spit (ha-snapshot-install-marker-path env-dir)
        (str (pr-str marker) "\n")))

(defn- delete-ha-snapshot-install-marker!
  [env-dir]
  (let [marker-path (ha-snapshot-install-marker-path env-dir)]
    (when (u/file-exists marker-path)
      (u/delete-files marker-path))))

(defn- restore-ha-snapshot-install-backup!
  [env-dir backup-dir]
  (when (u/file-exists env-dir)
    (u/delete-files env-dir))
  (move-path! backup-dir env-dir))

(defn- recover-ha-local-snapshot-install!
  [env-dir]
  (when-let [{:keys [backup-dir stage] :as marker}
             (read-ha-snapshot-install-marker env-dir)]
    (case stage
      :prepare
      (cond
        (u/file-exists backup-dir)
        (do
          (log/warn "Recovering HA snapshot install from staged backup"
                    {:env-dir env-dir
                     :backup-dir backup-dir
                     :stage stage})
          (restore-ha-snapshot-install-backup! env-dir backup-dir)
          (delete-ha-snapshot-install-marker! env-dir)
          marker)

        (u/file-exists env-dir)
        (do
          (delete-ha-snapshot-install-marker! env-dir)
          marker)

        :else
        (u/raise "HA snapshot install marker has no recoverable store"
                 {:error :ha/follower-snapshot-install-recovery-failed
                  :env-dir env-dir
                  :backup-dir backup-dir
                  :stage stage}))

      :backup-moved
      (if (u/file-exists backup-dir)
        (do
          (log/warn "Recovering HA snapshot install after interrupted backup move"
                    {:env-dir env-dir
                     :backup-dir backup-dir
                     :stage stage})
          (restore-ha-snapshot-install-backup! env-dir backup-dir)
          (delete-ha-snapshot-install-marker! env-dir)
          marker)
        (u/raise "HA snapshot install backup is missing during recovery"
                 {:error :ha/follower-snapshot-install-recovery-failed
                  :env-dir env-dir
                  :backup-dir backup-dir
                  :stage stage}))

      (u/raise "HA snapshot install marker has an unsupported stage"
               {:error :ha/follower-snapshot-install-marker-invalid
                :env-dir env-dir
                :marker marker}))))

(defn recover-ha-local-store-dir-if-needed!
  [env-dir]
  (when (read-ha-snapshot-install-marker env-dir)
    (recover-ha-local-snapshot-install! env-dir)))

(defn- close-ha-local-store!
  [m]
  (if-let [dt-db (:dt-db m)]
    (db/close-db dt-db)
    (when-let [store (:store m)]
      (cond
        (instance? IStore store) (i/close store)
        (instance? ILMDB store) (i/close-kv store)
        :else nil))))

(defn- refresh-ha-local-dt-db
  [m]
  (let [store (:store m)]
    (if (instance? IStore store)
      (let [info {:max-eid (i/init-max-eid store)
                  :max-tx (long (i/max-tx store))
                  :last-modified (long (i/last-modified store))}]
        (assoc m :dt-db (db/new-db store info)))
      m)))

(declare open-ha-store-dbis!)

(defn recover-ha-local-store-if-needed
  [store]
  (if-not (instance? IStore store)
    store
    (let [env-dir (i/dir store)]
      (if-not (recover-ha-local-store-dir-if-needed! env-dir)
        store
        (let [schema (i/schema store)
              opts (i/opts store)]
          (when-not (i/closed? store)
            (i/close store))
          (-> (st/open env-dir schema opts)
              open-ha-store-dbis!))))))

(defn- reopen-ha-local-store-if-needed
  [m]
  (let [store (:store m)
        recovered-store (recover-ha-local-store-if-needed store)
        m (if (identical? recovered-store store)
            m
            (-> m
                (assoc :store recovered-store
                       :dt-db nil)
                (dissoc :engine :index)
                refresh-ha-local-dt-db))]
    (if (local-kv-store m)
      m
      (let [store (:store m)]
        (if (instance? IStore store)
          (let [store-dir (i/dir store)
                store-opts (i/opts store)
                reopened-store (-> (st/open store-dir nil store-opts)
                                   open-ha-store-dbis!)]
            (-> m
                (assoc :store reopened-store
                       :dt-db nil)
                (dissoc :engine :index)
                refresh-ha-local-dt-db))
          m)))))

(defn- ha-local-store-reopen-info
  [m]
  (let [store (:store m)]
    (when (instance? IStore store)
      (try
        {:env-dir (i/dir store)
         :store-opts (i/opts store)}
        (catch Throwable _
          nil)))))

(defn- reopen-ha-local-store-from-info
  [m {:keys [env-dir store-opts]}]
  (when (and (string? env-dir)
             (not (s/blank? env-dir))
             (map? store-opts))
    (-> m
        (assoc :store (-> (st/open env-dir nil store-opts)
                          open-ha-store-dbis!)
               :dt-db nil)
        (dissoc :engine :index)
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
    (let [candidate-floor-lsn
          (long (max snapshot-lsn
                     (read-ha-local-watermark-lsn m)
                     (read-ha-snapshot-payload-lsn m)))
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
       :candidate-floor-lsn candidate-floor-lsn
       :tail-record-count (count records)})
    {:verified-floor-lsn (long snapshot-lsn)
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

(defn- open-ha-store-dbis!
  [store]
  (when-let [kv-store (cond
                        (instance? IStore store)
                        (try
                          (.-lmdb store)
                          (catch Throwable _
                            nil))

                        (instance? ILMDB store)
                        store

                        :else nil)]
    (doseq [dbi-name (or (i/list-dbis kv-store) [])]
      (let [dbi-opts (try
                       (i/dbi-opts kv-store dbi-name)
                       (catch Exception _
                         nil))]
        (if (map? dbi-opts)
          (i/open-dbi kv-store dbi-name dbi-opts)
          (i/open-dbi kv-store dbi-name)))))
  store)

(defn- ha-class-name
  [x]
  (some-> x class .getName))

(defn- ha-retrieved-like?
  [x]
  (= "datalevin.bits.Retrieved" (ha-class-name x)))

(defn- ha-reflect-field
  [x field-name]
  (when x
    (let [^Class cls (class x)
          field (.getDeclaredField cls field-name)]
      (.setAccessible field true)
      (.get field x))))

(defn- ha-seq-like?
  [x]
  (or (sequential? x)
      (instance? java.util.List x)))

(defn- ha-snapshot-open-opts
  [m db-name db-identity]
  (let [store (:store m)
        base-opts (if (instance? IStore store)
                    (i/opts store)
                    {})]
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
       :source :local}

      :else
      (if-let [uri (ha-endpoint-uri db-name endpoint m)]
        (let [client-opts
              {:pool-size 1
               :time-out (ha-request-timeout-ms m 5000)}]
          (try
            (with-cached-ha-client
              uri db-name client-opts
              (fn [client]
                (let [watermarks
                      (try
                        (ha-client-request
                         client
                         :ha-watermark
                         [db-name]
                         false)
                        (catch Exception e
                          (if (unknown-ha-watermark-command? e)
                            (ha-client-request
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
  [db-name m]
  (let [local-endpoint (:ha-local-endpoint m)
        endpoints (->> (concat [local-endpoint]
                               (map :endpoint (:ha-members m)))
                       (filter #(and (string? %)
                                     (not (s/blank? %))))
                       distinct)]
    (reduce (fn [best endpoint]
              (let [watermark (safe-fetch-ha-endpoint-watermark-lsn
                               db-name m endpoint)]
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
                      best)))))
            nil
            endpoints)))

(def ^:private ha-follower-max-batch-records 256)
(def ^:private ha-follower-max-sync-backoff-ms 30000)

(defn- next-ha-follower-sync-backoff-ms
  [m]
  (let [renew-ms (long (max 100
                            (long (or (:ha-lease-renew-ms m)
                                      c/*ha-lease-renew-ms*))))
        base-ms (long (max 250 (quot renew-ms 2)))
        prev-ms (long (or (:ha-follower-sync-backoff-ms m) 0))
        max-ms (long (max ha-follower-max-sync-backoff-ms
                          (* 6 renew-ms)))
        candidate (if (pos? prev-ms)
                    (* 2 prev-ms)
                    base-ms)]
    (long (min max-ms candidate))))

(defn- ha-follower-gap-error?
  [e]
  (contains? #{:ha/txlog-gap
               :ha/txlog-source-behind
               :ha/txlog-non-contiguous
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
        ordered-members (sort-by :node-id (:ha-members m))
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

(defn- ha-gap-fallback-source-endpoints
  [db-name m lease next-lsn]
  (let [required-lsn (long (max 0 (dec (long next-lsn))))
        leader-watermark (fetch-leader-watermark-lsn db-name m lease)
        authority-lsn (long (or (:leader-last-applied-lsn lease) 0))
        leader-safe-lsn (long (max authority-lsn
                                   (if (:reachable? leader-watermark)
                                     (long (or (:last-applied-lsn
                                                leader-watermark)
                                               0))
                                     0)))
        local-endpoint (:ha-local-endpoint m)
        leader-endpoint (ha-leader-endpoint m lease)
        followers
        (->> (:ha-members m)
             (sort-by :node-id)
             (keep (fn [{:keys [endpoint node-id]}]
                     (when (and (string? endpoint)
                                (not (s/blank? endpoint))
                                (not= endpoint local-endpoint)
                                (not= endpoint leader-endpoint))
                       (let [watermark (safe-fetch-ha-endpoint-watermark-lsn
                                        db-name m endpoint)
                             raw-last-lsn (ha-source-watermark-lsn
                                           leader-endpoint
                                           endpoint
                                           watermark)
                             last-lsn (when (some? raw-last-lsn)
                                        (long (min raw-last-lsn
                                                   leader-safe-lsn)))
                             eligible? (and (some? last-lsn)
                                            (>= last-lsn required-lsn))]
                         {:endpoint endpoint
                          :node-id node-id
                          :last-applied-lsn last-lsn
                          :raw-last-applied-lsn raw-last-lsn
                          :leader-safe-lsn leader-safe-lsn
                          :eligible? eligible?})))))
        eligible-followers
        (sort-by (juxt (comp - :last-applied-lsn) :node-id)
                 (filter :eligible? followers))
        unknown-followers
        (sort-by :node-id (remove :eligible? followers))]
    (->> (concat (when (and (string? leader-endpoint)
                            (not (s/blank? leader-endpoint))
                            (not= leader-endpoint local-endpoint))
                   [leader-endpoint])
                 (map :endpoint eligible-followers)
                 (map :endpoint unknown-followers))
         distinct
         vec)))

(declare fetch-ha-leader-txlog-batch)
(declare assert-contiguous-lsn!)

(defn- ha-source-advertised-last-applied-lsn
  [db-name m lease source-endpoint]
  (let [leader-endpoint (ha-leader-endpoint m lease)
        authority-lsn (long (or (:leader-last-applied-lsn lease) 0))
        leader-watermark (fetch-leader-watermark-lsn db-name m lease)
        leader-safe-lsn (long (max authority-lsn
                                   (if (:reachable? leader-watermark)
                                     (long (or (:last-applied-lsn
                                                leader-watermark)
                                               0))
                                     0)))]
    (if (= source-endpoint leader-endpoint)
      (let [watermark (safe-fetch-ha-endpoint-watermark-lsn
                       db-name m source-endpoint)
            remote-lsn (ha-source-watermark-lsn
                        leader-endpoint
                        source-endpoint
                        watermark)]
        {:known? true
         :last-applied-lsn (long (max authority-lsn (or remote-lsn 0)))
         :watermark watermark})
      (let [watermark (safe-fetch-ha-endpoint-watermark-lsn
                       db-name m source-endpoint)
            raw-last-lsn (ha-source-watermark-lsn
                          leader-endpoint
                          source-endpoint
                          watermark)
            last-lsn (when (some? raw-last-lsn)
                       (long (min raw-last-lsn leader-safe-lsn)))]
        {:known? (some? last-lsn)
         :last-applied-lsn last-lsn
         :raw-last-applied-lsn raw-last-lsn
         :leader-safe-lsn leader-safe-lsn
         :watermark watermark}))))

(defn- ^:redef fetch-ha-follower-records-with-gap-fallback
  [db-name m lease next-lsn upto-lsn]
  (let [sources (ha-follower-source-endpoints m lease)]
    (loop [remaining sources
           source-order sources
           reordered? false
           gap-errors []]
      (if-let [source-endpoint (first remaining)]
        (let [attempt
              (try
                (let [records (vec (or (fetch-ha-leader-txlog-batch
                                        db-name
                                        m
                                        source-endpoint
                                        next-lsn
                                        upto-lsn)
                                       []))]
                  (if (seq records)
                    (do
                      (assert-contiguous-lsn! next-lsn records)
                      {:ok? true
                       :value {:source-endpoint source-endpoint
                               :records records
                               :source-order source-order}})
                    (let [{:keys [known? last-applied-lsn]}
                          (ha-source-advertised-last-applied-lsn
                           db-name m lease source-endpoint)]
                      (cond
                        (and known?
                             (< (long (or last-applied-lsn 0))
                                (long (max 0
                                           (dec (long next-lsn))))))
                        {:ok? false
                         :gap-error
                         {:source-endpoint source-endpoint
                          :message
                          "Follower txlog replay source is behind local cursor"
                          :data {:error :ha/txlog-source-behind
                                 :expected-lsn (long next-lsn)
                                 :actual-lsn nil
                                 :source-last-applied-lsn
                                 (long (or last-applied-lsn 0))}}}

                        (and known?
                             (>= (long (or last-applied-lsn 0))
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
                                 (long (or last-applied-lsn 0))}}}

                        reordered?
                        {:ok? false
                         :skip? true}

                        :else
                        {:ok? true
                         :value {:source-endpoint source-endpoint
                                 :records records
                                 :source-order source-order}}))))
                (catch Exception e
                  (if (ha-follower-gap-error? e)
                    {:ok? false
                     :gap-error {:source-endpoint source-endpoint
                                 :message (ex-message e)
                                 :data (ex-data e)}}
                    (throw e))))]
          (cond
            (:ok? attempt)
            (:value attempt)

            (:skip? attempt)
            (recur (rest remaining) source-order reordered? gap-errors)

            :else
            (let [remaining' (if reordered?
                               (rest remaining)
                               (->> (ha-gap-fallback-source-endpoints
                                     db-name m lease next-lsn)
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
      (with-cached-ha-client
        uri db-name client-opts
        (fn [client]
          (ha-client-request
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

(defn ^:redef apply-ha-follower-txlog-record!
  [m record]
  (let [store (:store m)
        kv-store (raw-local-kv-store m)
        rows (:rows record)
        ops (:ops record)
        replay-rows (cond
                      (sequential? rows) (vec rows)
                      (sequential? ops) (vec ops)
                      :else nil)]
    (when-not kv-store
      (u/raise "Follower txlog replay requires a local KV store"
               {:error :ha/follower-missing-store}))
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
             (fn [acc [op dbi k]]
               (if (and (= op :put)
                        (= dbi c/giants)
                        (integer? k))
                 (max acc (unchecked-inc (long k)))
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
                (let [store-dir (i/dir store)
                      store-opts (i/opts store)]
                  (close-ha-local-store! m)
                  (assoc m
                         :store (open-ha-store-dbis!
                                 (st/open store-dir nil store-opts))
                         :dt-db nil))
                m))
            readback-kv-store (raw-local-kv-store next-state)
            probe-eid (some->> replay-rows
                               (keep (fn [[op dbi k]]
                                       (when (and (= op :put)
                                                  (= dbi c/eav)
                                                  (integer? k))
                                         (long k))))
                               first)
            readback
            (when readback-kv-store
              {:lsn (long (:lsn record))
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
               :probe-eid probe-eid
               :probe-eav-range
               (when probe-eid
                 (try
                   (vec (i/get-range readback-kv-store
                                     c/eav
                                     [probe-eid]
                                     :id
                                     :avg))
                   (catch Throwable _
                     nil)))})]
        (let [next-state (assoc next-state
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
              (loop [cur (long (i/max-tx (:store next-state)))]
                (when (< cur target-max-tx)
                  (i/advance-max-tx (:store next-state))
                  (recur (long (i/max-tx (:store next-state))))))))
          next-state))

      :else
      (u/raise "Follower txlog replay record is missing rows"
               {:error :ha/follower-invalid-record
                :record record}))))

(defn ^:redef report-ha-replica-floor!
  [db-name m leader-endpoint applied-lsn]
  (if-let [uri (ha-endpoint-uri db-name leader-endpoint m)]
    (let [client-opts
          {:pool-size 1
           :time-out (ha-request-timeout-ms m 10000)}]
      (with-cached-ha-client
        uri db-name client-opts
        (fn [client]
          (ha-client-request
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
      (with-cached-ha-client
        uri db-name client-opts
        (fn [client]
          (ha-client-request
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
          install-last-lsn (long (max (or payload-last-lsn 0)
                                      (or snapshot-last-lsn 0)))]
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
            (log/error e "HA follower snapshot install failed"
                       {:db-name (some-> store i/db-name)
                        :env-dir env-dir})
            (try
              (recover-ha-local-snapshot-install! env-dir)
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
                                       :restore-data (ex-data restore-e)})}}))))))))

(defn- sync-ha-follower-batch
  [db-name m lease next-lsn now-ms]
  (let [m (reopen-ha-local-store-if-needed m)
        leader-endpoint (ha-leader-endpoint m lease)
        local-node-id (:ha-node-id m)]
    (when (or (nil? leader-endpoint) (s/blank? leader-endpoint))
      (u/raise "HA follower is missing leader endpoint for txlog sync"
               {:error :ha/follower-missing-leader-endpoint
                :lease lease}))
    (let [upto-lsn (long (+ (long next-lsn)
                            (dec (long ha-follower-max-batch-records))))
          {:keys [records source-endpoint source-order]}
          (fetch-ha-follower-records-with-gap-fallback
           db-name m lease next-lsn upto-lsn)
          next-m (reduce apply-ha-follower-txlog-record! m records)
          _ (when (and (seq records)
                       (instance? IStore (:store next-m)))
              ;; Follower replay writes raw KV rows and bypasses the normal
              ;; datalog transaction path, so clear any cached query misses
              ;; before publishing a refreshed dt-db view.
              (db/refresh-cache (:store next-m)))
          last-record-lsn (when-let [record (peek records)]
                            (long (:lsn record)))
          ;; Empty batches prove only that the chosen source returned no new
          ;; replicated rows. Advancing to `(dec next-lsn)` here can skip
          ;; records after a stale or speculative follower cursor.
          current-local-floor-lsn
          (long (max 0
                     (long (or (:ha-local-last-applied-lsn next-m)
                               (:ha-local-last-applied-lsn m)
                               0))))
          applied-lsn (long (or last-record-lsn
                                current-local-floor-lsn))
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
                         :ha-follower-last-batch-records
                         (when (seq records)
                           (mapv #(select-keys % [:lsn :rows :ops])
                                 records))
                         :ha-follower-next-lsn (unchecked-inc applied-lsn)
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
                          resume-next-lsn (unchecked-inc installed-lsn)
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
            (let [local-next-lsn
                  (long (max 1
                             (unchecked-inc
                              (long (ha-local-last-applied-lsn m)))))
                  tracked-next-lsn
                  (when (integer? (:ha-follower-next-lsn m))
                    (long (:ha-follower-next-lsn m)))
                  ;; Fresh follower stores can expose internal LMDB txlog
                  ;; watermarks before they have replayed any replicated rows.
                  ;; Honor the tracked follower cursor when present, but never
                  ;; allow it to advance past the local floor.
                  next-lsn
                  (if (and tracked-next-lsn (pos? tracked-next-lsn))
                    (long (min tracked-next-lsn local-next-lsn))
                    local-next-lsn)]
              (try
                (:state (sync-ha-follower-batch
                         db-name m lease next-lsn now-ms))
                (catch Exception e
                  (if (ha-follower-gap-error? e)
                    (let [source-order (vec (or (:source-order (ex-data e))
                                                (ha-gap-fallback-source-endpoints
                                                 db-name m lease next-lsn)))
                          bootstrap (bootstrap-ha-follower-from-snapshot
                                     db-name m lease source-order
                                     next-lsn now-ms)]
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
                    (let [details {:message (ex-message e)
                                   :data (ex-data e)
                                   :leader-endpoint leader-endpoint
                                   :source-order
                                   (ha-follower-source-endpoints m lease)}
                          backoff-ms (next-ha-follower-sync-backoff-ms m)]
                      (assoc m
                             :ha-follower-last-error :sync-failed
                             :ha-follower-last-error-details details
                             :ha-follower-last-error-ms now-ms
                             :ha-follower-sync-backoff-ms backoff-ms
                             :ha-follower-next-sync-not-before-ms
                             (+ (long now-ms) (long backoff-ms))))))))))))))

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
          (highest-reachable-ha-member-watermark db-name m))
        reachable-member-lsn
        (when reachable-member-watermark
          (long (or (:last-applied-lsn reachable-member-watermark) 0)))
        effective-lsn
        (cond
          (and lease-expired?
               reachable?
               (< leader-lsn authority-lsn))
          (long (max leader-lsn
                     (or reachable-member-lsn
                         (ha-local-last-applied-lsn m))))

          reachable?
          (max authority-lsn leader-lsn)

          :else
          (long (or reachable-member-lsn
                    (ha-local-last-applied-lsn m))))]
    {:effective-lease (assoc lease :leader-last-applied-lsn effective-lsn)
     :lease-expired? lease-expired?
     :leader-endpoint-reachable? reachable?
     :authority-last-applied-lsn authority-lsn
     :leader-watermark-last-applied-lsn (when reachable? leader-lsn)
     :leader-watermark leader-watermark
     :reachable-member-last-applied-lsn
     reachable-member-lsn
     :reachable-member-watermark reachable-member-watermark}))

(defn- observe-authority-state
  [m]
  (let [authority (:ha-authority m)
        db-identity (:ha-db-identity m)
        {:keys [lease version membership-hash]}
        (ctrl/read-state authority db-identity)
        authority-membership-hash membership-hash
        db-identity-mismatch?
        (and lease (not= db-identity (:db-identity lease)))
        membership-mismatch?
        (and authority-membership-hash
             (not= authority-membership-hash (:ha-membership-hash m)))]
    {:lease lease
     :version version
     :authority-membership-hash authority-membership-hash
     :db-identity-mismatch? db-identity-mismatch?
     :membership-mismatch? membership-mismatch?}))

(defn- apply-authority-observation
  [m {:keys [lease version authority-membership-hash
             db-identity-mismatch? membership-mismatch?]}
   now-ms]
  (-> m
      (assoc :ha-authority-lease lease
             :ha-authority-version version
             :ha-authority-owner-node-id (:leader-node-id lease)
             :ha-authority-term (:term lease)
             :ha-lease-until-ms (:lease-until-ms lease)
             :ha-authority-membership-hash authority-membership-hash
             :ha-db-identity-mismatch? db-identity-mismatch?
             :ha-membership-mismatch? membership-mismatch?
             :ha-last-authority-refresh-ms now-ms)))

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
        role (:ha-role m)]
    (if (#{:follower :candidate} role)
      (let [result (try
                     (run-ha-clock-skew-hook db-name m)
                     (catch Exception e
                       {:ok? false
                        :paused? true
                        :reason :exception
                        :budget-ms budget-ms
                        :message (ex-message e)}))
            now-ms (System/currentTimeMillis)]
        (assoc m
               :ha-clock-skew-budget-ms budget-ms
               :ha-clock-skew-paused? (true? (:paused? result))
               :ha-clock-skew-last-check-ms now-ms
               :ha-clock-skew-last-observed-ms (:clock-skew-ms result)
               :ha-clock-skew-last-result result))
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

(defn- ha-authority-read-failure-details
  [m]
  (or (:ha-authority-read-error m)
      {:reason :authority-read-failed}))

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
          lag-lsn (max 0 (- leader-lsn local-lsn))
          lag-ok? (<= lag-lsn
                      (long (or (:ha-max-promotion-lag-lsn m) 0)))
          synced? (and (true? (:ha-authority-read-ok? m))
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

    (true? (:ha-follower-degraded? m))
    (assoc m
           :ha-promotion-last-failure :follower-degraded
           :ha-promotion-failure-details
           {:reason (:ha-follower-degraded-reason m)
            :details (:ha-follower-degraded-details m)})

    (true? (:ha-clock-skew-paused? m))
    (assoc m
           :ha-promotion-last-failure :clock-skew-paused
           :ha-promotion-failure-details
           (ha-clock-skew-promotion-failure-details m))

    (true? (:ha-rejoin-promotion-blocked? m))
    (assoc m
           :ha-promotion-last-failure :rejoin-in-progress
           :ha-promotion-failure-details
           (ha-rejoin-promotion-failure-details m))

    (false? (:ha-authority-read-ok? m))
    (assoc m
           :ha-promotion-last-failure :authority-read-failed
           :ha-promotion-failure-details
           (ha-authority-read-failure-details m))

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
  [m authority db-identity lease version now-ms lag-check]
  (let [acquire
        (ctrl/try-acquire-lease
         authority
         {:db-identity db-identity
          :leader-node-id (:ha-node-id m)
          :leader-endpoint (:ha-local-endpoint m)
          :lease-renew-ms (:ha-lease-renew-ms m)
          :lease-timeout-ms (:ha-lease-timeout-ms m)
          :leader-last-applied-lsn (:local-last-applied-lsn lag-check)
          :now-ms now-ms
          :observed-version version
          :observed-lease lease})]
    (if (:ok? acquire)
      (let [{:keys [lease version term]} acquire]
        (-> m
            clear-ha-candidate-state
            (assoc :ha-role :leader
                   :ha-leader-term term
                   :ha-authority-lease lease
                   :ha-authority-version version
                   :ha-authority-owner-node-id (:leader-node-id lease)
                   :ha-authority-term (:term lease)
                   :ha-lease-until-ms (:lease-until-ms lease)
                   :ha-last-authority-refresh-ms now-ms
                   :ha-db-identity-mismatch? false
                   :ha-membership-mismatch? false
                   :ha-promotion-last-failure nil
                   :ha-promotion-failure-details nil)))
      (fail-ha-candidate m :lease-cas-failed {:acquire acquire}))))

(defn- finalize-ha-candidate-promotion
  [db-name m authority db-identity lease version now-ms lag-input]
  (if-not (lease/lease-expired? lease now-ms)
    (fail-ha-candidate m :lease-not-expired
                       {:lease lease})
    (let [lag-check
          (ha-promotion-lag-guard m (:effective-lease lag-input))]
      (if-not (:ok? lag-check)
        (fail-ha-candidate m :lag-guard-failed
                           {:phase :pre-cas
                            :lag lag-check
                            :leader-lag-input lag-input})
        (try-promote-with-cas
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
              (let [obs-2 (observe-authority-state m1)
                    now-ms-2 (System/currentTimeMillis)
                    m2 (apply-authority-observation m1 obs-2 now-ms-2)
                    lease-2 (:lease obs-2)
                    version-2 (:version obs-2)]
                (cond
                  (:db-identity-mismatch? obs-2)
                  (fail-ha-candidate m2 :db-identity-mismatch
                                     {:local-db-identity db-identity
                                      :authority-lease lease-2})

                  (:membership-mismatch? obs-2)
                  (fail-ha-candidate m2 :membership-hash-mismatch
                                     {:ha-membership-hash
                                      (:ha-membership-hash m2)
                                      :ha-authority-membership-hash
                                      (:authority-membership-hash obs-2)})

                  (not (lease/lease-expired? lease-2 now-ms-2))
                  (fail-ha-candidate m2 :lease-not-expired
                                     {:lease lease-2})

                  :else
                  (finalize-ha-candidate-promotion
                   db-name
                   m2
                   authority
                   db-identity
                   lease-2
                   version-2
                   now-ms-2
                   (pre-cas-lag-input db-name m2 lease-2 now-ms-2)))))))))))

(def ^:private restart-ha-candidate-promotion
  ::restart-ha-candidate-promotion)

(defn- maybe-resume-ha-candidate-pre-cas-wait
  [db-name m now-ms]
  (when-let [wait-until-ms (:ha-candidate-pre-cas-wait-until-ms m)]
    (let [remaining-ms (long (max 0 (- (long wait-until-ms) (long now-ms))))]
      (if (pos? remaining-ms)
        (assoc m :ha-promotion-wait-before-cas-ms remaining-ms)
        (let [observed-version (:ha-candidate-pre-cas-observed-version m)
              m1 (dissoc m
                         :ha-candidate-pre-cas-wait-until-ms
                         :ha-candidate-pre-cas-observed-version
                         :ha-promotion-wait-before-cas-ms)
              obs (observe-authority-state m1)
              now-ms-2 (System/currentTimeMillis)]
          (if (= observed-version (:version obs))
            (maybe-promote-after-authority-observation
             db-name
             m1
             (:ha-authority m1)
             (:ha-db-identity m1)
             obs
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
      (let [lag-input-1 (pre-cas-lag-input db-name
                                           m
                                           observed-lease
                                           now-ms)
            lag-check-1 (ha-promotion-lag-guard
                         m
                         (:effective-lease lag-input-1))]
        (if-not (:ok? lag-check-1)
          (fail-ha-candidate m :lag-guard-failed
                             {:phase :pre-fence
                              :lag lag-check-1
                              :leader-lag-input lag-input-1})
          (let [fence-result
                (run-ha-fencing-hook db-name m observed-lease)]
            (if-not (:ok? fence-result)
              (fail-ha-candidate m :fencing-failed fence-result)
              (maybe-promote-after-authority-observation
               db-name
               m
               (:ha-authority m)
               (:ha-db-identity m)
               (observe-authority-state m)
               (System/currentTimeMillis)))))))))

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

(defn- advance-ha-follower-or-candidate
  [db-name m]
  (if (contains? #{:follower :candidate} (:ha-role m))
    (let [m0 (sync-ha-follower-state db-name m (ha-now-ms))
          state-now-ms (ha-now-ms)
          m1 (maybe-clear-ha-rejoin-promotion-block m0 state-now-ms)
          m2 (maybe-enter-ha-candidate m1 state-now-ms)]
      (maybe-promote-ha-candidate db-name m2 (ha-now-ms)))
    m))

(defn- read-ha-authority-state
  [db-name m]
  (let [authority (:ha-authority m)
        db-identity (:ha-db-identity m)
        {:keys [lease version membership-hash]}
        (ctrl/read-state authority db-identity)
        now-ms (ha-now-ms)
        authority-membership-hash membership-hash
        db-identity-mismatch?
        (and lease (not= db-identity (:db-identity lease)))
        membership-mismatch?
        (and authority-membership-hash
             (not= authority-membership-hash (:ha-membership-hash m)))
        m1 (-> m
               (assoc :ha-authority-lease lease
                      :ha-authority-version version
                      :ha-authority-owner-node-id (:leader-node-id lease)
                      :ha-authority-term (:term lease)
                      :ha-lease-until-ms (:lease-until-ms lease)
                      :ha-authority-membership-hash authority-membership-hash
                      :ha-db-identity-mismatch? db-identity-mismatch?
                      :ha-membership-mismatch? membership-mismatch?
                      :ha-authority-read-ok? true
                      :ha-authority-read-error nil
                      :ha-last-authority-refresh-ms now-ms))]
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
                         :authority-membership-hash authority-membership-hash}
                        now-ms)

      :else
      m1)))

(defn- renew-ha-leader-state
  [db-name m]
  (if (not= :leader (:ha-role m))
    m
    (let [now-ms (ha-now-ms)
          term (:ha-leader-term m)]
      (if-not (and (integer? term) (pos? ^long term))
        (demote-ha-leader db-name m :missing-leader-term nil now-ms)
        (let [result (ctrl/renew-lease
                      (:ha-authority m)
                      {:db-identity (:ha-db-identity m)
                       :leader-node-id (:ha-node-id m)
                       :leader-endpoint (:ha-local-endpoint m)
                       :term term
                       :lease-renew-ms (:ha-lease-renew-ms m)
                       :lease-timeout-ms (:ha-lease-timeout-ms m)
                       :leader-last-applied-lsn (long (or (:ha-leader-last-applied-lsn m) 0))
                       :now-ms now-ms})]
          (if (:ok? result)
            (let [{:keys [lease version]} result]
              (-> m
                  (assoc :ha-authority-lease lease
                         :ha-authority-version version
                         :ha-authority-owner-node-id (:leader-node-id lease)
                         :ha-authority-term (:term lease)
                         :ha-lease-until-ms (:lease-until-ms lease)
                         :ha-last-authority-refresh-ms now-ms)))
            (demote-ha-leader db-name m
                              :renew-failed
                              {:reason (:reason result)}
                              now-ms)))))))

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
          m1 (try
               (read-ha-authority-state db-name m0)
               (catch Exception e
                 (log/warn e "HA read-lease failed"
                           {:db-name db-name})
                 (assoc m0
                        :ha-authority-read-ok? false
                        :ha-authority-read-error
                        {:reason :authority-read-failed
                         :message (ex-message e)
                         :data (ex-data e)})))
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
          (maybe-finish-ha-demotion end-now-ms started-demoting?)))))

(defn clear-ha-runtime-state
  [m]
  (dissoc m
          :ha-authority :ha-db-identity :ha-membership-hash
          :ha-authority-membership-hash
          :ha-db-identity-mismatch? :ha-membership-mismatch?
          :ha-members
          :ha-node-id :ha-local-endpoint
          :ha-lease-renew-ms :ha-lease-timeout-ms
          :ha-promotion-base-delay-ms :ha-promotion-rank-delay-ms
          :ha-max-promotion-lag-lsn :ha-demotion-drain-ms
          :ha-fencing-hook
          :ha-clock-skew-budget-ms :ha-clock-skew-hook
          :ha-authority-lease :ha-authority-version
          :ha-authority-owner-node-id :ha-authority-term
          :ha-lease-until-ms :ha-last-authority-refresh-ms
          :ha-authority-read-ok?
          :ha-authority-read-error
          :ha-clock-skew-paused?
          :ha-clock-skew-last-check-ms
          :ha-clock-skew-last-observed-ms
          :ha-clock-skew-last-result
          :ha-demotion-drain-until-ms
          :ha-role :ha-leader-term
          :ha-local-last-applied-lsn
          :ha-follower-next-lsn :ha-follower-last-batch-size
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
          :ha-renew-loop-running?))

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
        (let [now-ms (System/currentTimeMillis)
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
                    (sort-by :node-id (:ha-members m)))
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
              db-id-mismatch? (true? (:ha-db-identity-mismatch? m))
              membership-mismatch? (true? (:ha-membership-mismatch? m))
              lease-until-ms (:ha-lease-until-ms m)
              leader-term (:ha-leader-term m)
              authority-term (:ha-authority-term m)]
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

            (or (nil? lease-until-ms)
                (>= (long now-ms) (long lease-until-ms)))
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :lease-expired
                    :lease-until-ms lease-until-ms
                    :now-ms now-ms
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
    (let [{:keys [lease version]}
          (ctrl/read-state-for-startup authority db-identity)]
      {:ok? true
       :lease lease
       :version version
       :error nil})
    (catch Exception e
      (log/warn e "HA startup read-lease failed; deferring to renew loop"
                {:db-name db-name})
      {:ok? false
       :lease nil
       :version nil
       :error {:reason :startup-authority-read-failed
               :message (ex-message e)
               :data (ex-data e)}})))

(defn start-ha-authority
  [db-name ha-opts]
  (let [cp (:ha-control-plane ha-opts)
        db-identity (:db-identity ha-opts)
        node-id (:ha-node-id ha-opts)
        members (:ha-members ha-opts)
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
        fencing-hook (:ha-fencing-hook ha-opts)
        clock-skew-hook (:ha-clock-skew-hook ha-opts)
        local-endpoint (local-ha-endpoint ha-opts)
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
      (let [now-ms (System/currentTimeMillis)
            derived-hash (vld/derive-ha-membership-hash ha-opts)
            init-result (ctrl/init-membership-hash! authority derived-hash)
            startup-read
            (startup-read-ha-authority-state db-name authority db-identity)
            {:keys [lease version error]} startup-read
            _ (when (and lease (not= db-identity (:db-identity lease)))
                (u/raise "HA lease db identity mismatch at startup"
                         {:error :ha/db-identity-mismatch
                          :db-name db-name
                          :local-db-identity db-identity
                          :authority-lease lease}))
            local-authority-owner? (and lease
                                        (= node-id (:leader-node-id lease))
                                        (not (lease/lease-expired? lease now-ms))
                                        (= db-identity (:db-identity lease)))
            rejoin-promotion-blocked?
            (and lease
                 (integer? (:leader-node-id lease))
                 (not= node-id (:leader-node-id lease))
                 (not (lease/lease-expired? lease now-ms))
                 (= db-identity (:db-identity lease)))
            rejoin-promotion-blocked-until-ms
            (when rejoin-promotion-blocked?
              (long (max (+ (long now-ms) (long renew-ms))
                         (+ (long (or (:lease-until-ms lease) now-ms))
                            (long renew-ms)))))]
        (when-not (:ok? init-result)
          (u/raise "HA membership hash mismatch with authoritative control plane"
                   {:error :ha/membership-hash-mismatch
                    :db-name db-name
                    :derived-hash derived-hash
                    :authority init-result}))
        (when local-authority-owner?
          (log/info "HA startup is rejoining as follower despite local authority ownership"
                    {:db-name db-name
                     :ha-node-id node-id
                     :term (:term lease)
                     :lease-until-ms (:lease-until-ms lease)}))
        {:ha-authority authority
         :ha-db-identity db-identity
         :ha-membership-hash derived-hash
         :ha-authority-membership-hash (:membership-hash init-result)
         :ha-db-identity-mismatch? false
         :ha-membership-mismatch? false
         :ha-members members
         :ha-node-id node-id
         :ha-local-endpoint local-endpoint
         :ha-lease-renew-ms renew-ms
         :ha-lease-timeout-ms timeout-ms
         :ha-promotion-base-delay-ms promotion-base-delay-ms
         :ha-promotion-rank-delay-ms promotion-rank-delay-ms
         :ha-max-promotion-lag-lsn max-promotion-lag-lsn
         :ha-client-credentials (:ha-client-credentials ha-opts)
         :ha-demotion-drain-ms demotion-drain-ms
         :ha-fencing-hook fencing-hook
         :ha-clock-skew-budget-ms clock-skew-budget-ms
         :ha-clock-skew-hook clock-skew-hook
         :ha-clock-skew-paused? false
         :ha-clock-skew-last-check-ms nil
         :ha-clock-skew-last-observed-ms nil
         :ha-clock-skew-last-result nil
         :ha-authority-lease lease
         :ha-authority-version version
         :ha-authority-owner-node-id (:leader-node-id lease)
         :ha-authority-term (:term lease)
         :ha-lease-until-ms (:lease-until-ms lease)
         :ha-authority-read-ok? (:ok? startup-read)
         :ha-authority-read-error error
         :ha-last-authority-refresh-ms now-ms
         :ha-rejoin-promotion-blocked? rejoin-promotion-blocked?
         :ha-rejoin-promotion-blocked-until-ms
         rejoin-promotion-blocked-until-ms
         :ha-rejoin-started-at-ms (when rejoin-promotion-blocked? now-ms)
         :ha-role :follower
         :ha-leader-term nil})
      (catch Exception e
        (try
          (ctrl/stop-authority! authority)
          (catch Exception stop-e
            (log/warn stop-e "Failed to stop HA authority after startup failure"
                      {:db-name db-name})))
        (throw e)))))

(defn stop-ha-authority
  [db-name m]
  (when-let [authority (:ha-authority m)]
    (try
      (ctrl/stop-authority! authority)
      (catch Exception e
        (log/warn e "Failed to stop HA authority" {:db-name db-name})))))
