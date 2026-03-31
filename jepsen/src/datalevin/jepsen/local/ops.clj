(ns datalevin.jepsen.local.ops
  (:require
   [clojure.string :as str]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.ha :as dha]
   [datalevin.interface :as i]
   [datalevin.jepsen.local.cluster :as lcluster]
   [datalevin.jepsen.local.remote :as lremote]
   [datalevin.kv :as kv]
   [datalevin.remote :as r]
   [datalevin.server :as srv]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [datalevin.server Server]
   [datalevin.storage Store]))

(def ^:private cluster-timeout-ms 10000)
(def ^:private conn-client-opts {:pool-size 1 :time-out cluster-timeout-ms})
(def ^:private leader-connect-retry-sleep-ms 250)
(def ^:private unavailable-sentinel
  :datalevin.jepsen.local/unavailable)

(defn- now-ms
  []
  (System/currentTimeMillis))

(defn- authority-diagnostics-snapshot
  [authority]
  (when authority
    (if-let [f (try
                 (requiring-resolve 'datalevin.ha.control/authority-diagnostics)
                 (catch Throwable _
                   nil))]
      (f authority)
      {:backend :diagnostics-unavailable})))

(defn- authority-diagnostics->control-state
  [authority-diagnostics]
  {:ha-control-local-peer-id (:local-peer-id authority-diagnostics)
   :ha-control-leader-peer-id (:leader-peer-id authority-diagnostics)
   :ha-control-node-leader? (:node-leader? authority-diagnostics)
   :ha-control-node-state (some-> (:node-state authority-diagnostics) str)})

(defn- db-state
  [server db-name]
  (when server
    (get (.-dbs ^Server server) db-name)))

(defn- with-live-store-read-access
  [server db-name f]
  (if server
    (srv/with-db-runtime-store-read-access server db-name f)
    (f)))

(defn- store-open?
  [store]
  (cond
    (nil? store) false
    (instance? Store store) (not (i/closed? store))
    :else (not (i/closed-kv? store))))

(defn- local-watermarks
  [server db-name]
  (when-let [state (db-state server db-name)]
    (let [store (:store state)
          lmdb  (if (instance? Store store)
                  (.-lmdb ^Store store)
                  store)]
      (when (store-open? lmdb)
        (try
          (kv/txlog-watermarks lmdb)
          (catch Throwable _
            nil))))))

(defn- local-ha-persisted-lsn
  [state]
  (let [store (:store state)
        lmdb  (if (instance? Store store)
                (.-lmdb ^Store store)
                store)]
    (long
     (or (try
           (i/get-value lmdb c/kv-info c/ha-local-applied-lsn
                        :keyword :data)
           (catch Throwable _
             nil))
         0))))

(defn- local-snapshot-lsn
  [state]
  (let [store (:store state)
        lmdb  (if (instance? Store store)
                (.-lmdb ^Store store)
                store)]
    (long
     (or (try
           (i/get-value lmdb c/kv-info c/wal-snapshot-current-lsn
                        :keyword :data)
           (catch Throwable _
             nil))
         0))))

(declare effective-local-lsn
         node-diagnostics
         with-node-conn
         with-node-kv-store
         authority-leader-logical-node
         authority-leader-snapshot
         wait-for-single-leader!
         wait-for-authority-leader!)

(defn- remote-node-diagnostics
  [{:keys [clusters remote-deps transport-failure? storage-fault]} cluster-id logical-node]
  (try
    (when-let [state (lremote/remote-ha-watermark (remote-deps)
                                                  cluster-id
                                                  logical-node)]
      (let [effective-lsn (long (or (:last-applied-lsn state)
                                    (:ha-local-last-applied-lsn state)
                                    0))]
        (assoc state
               :jepsen-paused?
               (contains? (get-in @clusters [cluster-id :paused-nodes])
                          logical-node)
               :jepsen-storage-fault (storage-fault cluster-id logical-node)
               :ha-effective-local-lsn effective-lsn)))
    (catch Throwable e
      (if (transport-failure? e)
        nil
        (throw e)))))

(defn effective-local-lsn
  [{:keys [clusters remote-cluster?] :as deps} cluster-id logical-node]
  (if (remote-cluster? cluster-id)
    (long (or (:ha-effective-local-lsn
               (remote-node-diagnostics deps cluster-id logical-node))
              0))
    (let [{:keys [db-name servers]} (get @clusters cluster-id)
          server                    (get servers logical-node)]
      (with-live-store-read-access
        server
        db-name
        (fn []
          (if-let [state (db-state server db-name)]
            (let [txlog-lsn     (long (or (:last-applied-lsn
                                           (local-watermarks server db-name))
                                          0))
                  snapshot-lsn  (local-snapshot-lsn state)
                  runtime-lsn   (long (or (:ha-local-last-applied-lsn state) 0))
                  persisted-lsn (local-ha-persisted-lsn state)
                  comparable    (long (max runtime-lsn persisted-lsn))
                  local-truth   (long (max txlog-lsn snapshot-lsn))]
              (if (= :leader (:ha-role state))
                (long (max local-truth comparable))
                (if (pos? local-truth)
                  local-truth
                  comparable)))
            0))))))

(defn node-progress-lsn
  [{:keys [clusters remote-cluster?] :as deps} cluster-id logical-node]
  (if (remote-cluster? cluster-id)
    (long (or (:last-applied-lsn
               (remote-node-diagnostics deps cluster-id logical-node))
              0))
    (let [{:keys [db-name servers]} (get @clusters cluster-id)
          server                    (get servers logical-node)]
      (with-live-store-read-access
        server
        db-name
        (fn []
          (if-let [state (db-state server db-name)]
            (let [txlog-lsn     (long (or (:last-applied-lsn
                                           (local-watermarks server db-name))
                                          0))
                  runtime-lsn   (long (or (:ha-local-last-applied-lsn state) 0))
                  persisted-lsn (local-ha-persisted-lsn state)
                  comparable    (long (max runtime-lsn persisted-lsn))]
              (long (max txlog-lsn comparable)))
            0))))))

(defn wait-for-live-nodes-at-least-lsn!
  [deps cluster-id target-lsn timeout-ms]
  (let [deadline (+ (now-ms) (long timeout-ms))
        target   (long target-lsn)]
    (loop [last-snapshot nil]
      (let [{:keys [live-nodes]} (get @(-> deps :clusters) cluster-id)
            snapshot            (into {}
                                      (map (fn [logical-node]
                                             [logical-node
                                              (node-progress-lsn deps
                                                                 cluster-id
                                                                 logical-node)]))
                                      live-nodes)]
        (cond
          (every? (fn [[_ lsn]]
                    (>= (long lsn) target))
                  snapshot)
          snapshot

          (< (now-ms) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info "Timed out waiting for live nodes to catch up"
                          {:cluster-id cluster-id
                           :target-lsn target
                           :timeout-ms timeout-ms
                           :snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn wait-for-nodes-at-least-lsn!
  [deps cluster-id logical-nodes target-lsn timeout-ms]
  (let [deadline      (+ (now-ms) (long timeout-ms))
        target        (long target-lsn)
        logical-nodes (vec logical-nodes)]
    (loop [last-snapshot nil]
      (let [snapshot (into {}
                           (map (fn [logical-node]
                                  [logical-node
                                   (node-progress-lsn deps cluster-id logical-node)]))
                           logical-nodes)]
        (cond
          (every? (fn [[_ lsn]]
                    (>= (long lsn) target))
                  snapshot)
          snapshot

          (< (now-ms) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info "Timed out waiting for Jepsen nodes to catch up"
                          {:cluster-id cluster-id
                           :logical-nodes logical-nodes
                           :target-lsn target
                           :timeout-ms timeout-ms
                           :snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn wait-for-at-least-nodes-at-least-lsn!
  [deps cluster-id logical-nodes target-lsn required-count timeout-ms]
  (let [deadline       (+ (now-ms) (long timeout-ms))
        target         (long target-lsn)
        logical-nodes  (vec logical-nodes)
        required-count (long required-count)]
    (loop [last-snapshot nil]
      (let [snapshot (into {}
                           (map (fn [logical-node]
                                  [logical-node
                                   (node-progress-lsn deps cluster-id logical-node)]))
                           logical-nodes)
            matching (->> snapshot
                          (keep (fn [[logical-node lsn]]
                                  (when (>= (long lsn) target)
                                    logical-node)))
                          sort
                          vec)]
        (cond
          (>= (count matching) required-count)
          {:snapshot snapshot
           :matched-nodes matching}

          (< (now-ms) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info
                  "Timed out waiting for Jepsen node quorum to catch up"
                  {:cluster-id cluster-id
                   :logical-nodes logical-nodes
                   :target-lsn target
                   :required-count required-count
                   :timeout-ms timeout-ms
                   :snapshot snapshot
                   :previous-snapshot last-snapshot})))))))

(defn- probe-write-admission
  [{:keys [clusters]} cluster-id logical-node server db-name]
  (let [db-state (db-state server db-name)]
    (cond
      (contains? (get-in @clusters [cluster-id :paused-nodes]) logical-node)
      {:status :paused}

      (nil? server)
      {:status :down}

      (nil? db-state)
      {:status :missing-db-state}

      (nil? (:ha-authority db-state))
      {:status :ha-runtime-missing}

      :else
      (try
        (if-let [err (dha/ha-write-admission-error
                      (.-dbs ^Server server)
                      {:type :open-dbi
                       :args [db-name "__jepsen_probe" nil]})]
          {:status :rejected
           :reason (:reason err)
           :error (:error err)}
          {:status :leader})
        (catch Throwable e
          {:status :probe-failed
           :message (ex-message e)})))))

(defn- authority-leader-snapshot
  [{:keys [clusters]} cluster-id diagnostics-snapshot]
  (let [{:keys [control-node-names]} (get @clusters cluster-id)
        control-node-names (or control-node-names [])
        snapshot (into {}
                       (map (fn [logical-node]
                              (let [state (get diagnostics-snapshot logical-node)
                                    leader-peer-id
                                    (or (:ha-control-leader-peer-id state)
                                        (when (:ha-control-node-leader? state)
                                          (:ha-control-local-peer-id state)))]
                                [logical-node
                                 {:leader-peer-id leader-peer-id
                                  :leader (authority-leader-logical-node
                                           {:clusters clusters}
                                           cluster-id
                                           leader-peer-id)
                                  :node-leader?
                                  (:ha-control-node-leader? state)
                                  :node-state (:ha-control-node-state state)
                                  :term (:ha-authority-term state)
                                  :role (:ha-role state)}])))
                       control-node-names)
        quorum-size (inc (quot (count control-node-names) 2))
        leader-counts (frequencies (keep :leader (vals snapshot)))
        leaders (->> leader-counts
                     (keep (fn [[logical-node count]]
                             (when (and (>= count quorum-size)
                                        (true? (get-in snapshot
                                                       [logical-node
                                                        :node-leader?])))
                               logical-node)))
                     vec)]
    {:snapshot snapshot
     :leaders leaders}))

(defn wait-for-single-leader!
  [{:keys [clusters remote-cluster?] :as deps} cluster-id timeout-ms]
  (let [timeout-ms (long timeout-ms)
        deadline   (+ (now-ms) timeout-ms)]
    (loop [last-snapshot nil]
      (let [{:keys [db-name live-nodes servers]} (get @clusters cluster-id)
            snapshot (if (remote-cluster? cluster-id)
                       (into {}
                             (for [logical-node live-nodes
                                   :let [state (node-diagnostics deps
                                                                 cluster-id
                                                                 logical-node)]]
                               [logical-node
                                (cond
                                  (:jepsen-paused? state)
                                  {:status :paused}

                                  (nil? state)
                                  {:status :unavailable}

                                  (= :leader (:ha-role state))
                                  {:status :leader}

                                  :else
                                  {:status :follower
                                   :ha-role (:ha-role state)})]))
                       (into {}
                             (for [logical-node live-nodes]
                               [logical-node
                                (probe-write-admission deps
                                                       cluster-id
                                                       logical-node
                                                       (get servers logical-node)
                                                       db-name)])))
            diagnostics-snapshot
            (into {}
                  (for [logical-node live-nodes]
                    [logical-node
                     (select-keys
                      (node-diagnostics deps cluster-id logical-node)
                      [:ha-role
                       :ha-authority-owner-node-id
                       :ha-authority-term
                       :ha-control-local-peer-id
                       :ha-control-leader-peer-id
                       :ha-control-node-leader?
                       :ha-control-node-state
                       :ha-local-last-applied-lsn
                       :ha-follower-next-lsn
                       :ha-follower-last-sync-ms
                       :ha-follower-last-bootstrap-ms
                       :ha-follower-degraded?
                       :ha-follower-degraded-reason
                       :ha-follower-last-error
                       :ha-follower-last-error-details
                       :ha-clock-skew-paused?
                       :ha-clock-skew-last-observed-ms
                       :ha-clock-skew-last-result
                       :ha-promotion-last-failure
                       :ha-promotion-failure-details
                       :ha-rejoin-promotion-blocked?
                       :ha-rejoin-promotion-blocked-until-ms
                       :ha-lease-until-ms
                       :ha-last-authority-refresh-ms])]))
            leaders (->> snapshot
                         (keep (fn [[logical-node {:keys [status]}]]
                                 (when (= :leader status) logical-node)))
                         vec)]
        (cond
          (= 1 (count leaders))
          {:leader (first leaders)
           :snapshot snapshot}

          (< (now-ms) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (let [data {:timeout-ms timeout-ms
                      :probe-snapshot snapshot
                      :diagnostics-snapshot diagnostics-snapshot
                      :previous-snapshot last-snapshot}]
            (log/warn "Jepsen timed out waiting for single Datalevin leader"
                      data)
            (throw (ex-info "Timed out waiting for single leader"
                            data))))))))

(defn maybe-wait-for-single-leader
  [deps cluster-id timeout-ms]
  (try
    (wait-for-single-leader! deps cluster-id timeout-ms)
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [probe-snapshot previous-snapshot]} (ex-data e)]
        (when-not (and probe-snapshot previous-snapshot)
          (throw e))
        nil))))

(defn authority-leader-logical-node
  [{:keys [clusters]} cluster-id peer-id]
  (get-in @clusters [cluster-id :peer-id->node peer-id]))

(defn wait-for-authority-leader!
  [{:keys [clusters] :as deps} cluster-id timeout-ms]
  (let [timeout-ms (long timeout-ms)
        deadline   (+ (now-ms) timeout-ms)]
    (loop [last-snapshot nil]
      (let [{:keys [control-node-names]} (get @clusters cluster-id)
            control-node-names (or control-node-names [])
            diagnostics-snapshot
            (into {}
                  (map (fn [logical-node]
                         [logical-node (node-diagnostics deps
                                                         cluster-id
                                                         logical-node)]))
                  control-node-names)
            {:keys [snapshot leaders]}
            (authority-leader-snapshot deps cluster-id diagnostics-snapshot)]
        (cond
          (= 1 (count leaders))
          {:leader (first leaders)
           :snapshot snapshot}

          (< (now-ms) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info "Timed out waiting for HA authority leader"
                          {:timeout-ms timeout-ms
                           :authority-snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn maybe-wait-for-authority-leader
  [deps cluster-id timeout-ms]
  (try
    (wait-for-authority-leader! deps cluster-id timeout-ms)
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [authority-snapshot previous-snapshot]} (ex-data e)]
        (when-not (and authority-snapshot previous-snapshot)
          (throw e))
        nil))))

(defn node-diagnostics
  [{:keys [clusters remote-cluster? storage-fault] :as deps} cluster-id logical-node]
  (if (remote-cluster? cluster-id)
    (remote-node-diagnostics deps cluster-id logical-node)
    (let [{:keys [db-name servers control-authorities]} (get @clusters cluster-id)]
      (if-let [state (db-state (get servers logical-node) db-name)]
        (let [authority-diagnostics
              (when-let [authority (:ha-authority state)]
                (authority-diagnostics-snapshot authority))]
          (merge
           {:ha-role (:ha-role state)
            :ha-authority-owner-node-id (:ha-authority-owner-node-id state)
            :ha-authority-term (:ha-authority-term state)
            :udf-ready? (:udf-ready? state)
            :udf-missing (:udf-missing state)
            :udf-readiness-token (:udf-readiness-token state)
            :ha-local-last-applied-lsn (:ha-local-last-applied-lsn state)
            :ha-follower-next-lsn (:ha-follower-next-lsn state)
            :ha-follower-last-batch-size (:ha-follower-last-batch-size state)
            :ha-follower-last-sync-ms (:ha-follower-last-sync-ms state)
            :ha-follower-leader-endpoint (:ha-follower-leader-endpoint state)
            :ha-follower-source-endpoint (:ha-follower-source-endpoint state)
            :ha-follower-source-order (:ha-follower-source-order state)
            :ha-follower-last-bootstrap-ms (:ha-follower-last-bootstrap-ms state)
            :ha-follower-bootstrap-source-endpoint
            (:ha-follower-bootstrap-source-endpoint state)
            :ha-follower-bootstrap-snapshot-last-applied-lsn
            (:ha-follower-bootstrap-snapshot-last-applied-lsn state)
            :ha-follower-degraded? (:ha-follower-degraded? state)
            :ha-follower-degraded-reason (:ha-follower-degraded-reason state)
            :ha-follower-last-error (:ha-follower-last-error state)
            :ha-follower-last-error-details
            (:ha-follower-last-error-details state)
            :ha-follower-next-sync-not-before-ms
            (:ha-follower-next-sync-not-before-ms state)
            :ha-clock-skew-paused? (:ha-clock-skew-paused? state)
            :ha-clock-skew-last-observed-ms
            (:ha-clock-skew-last-observed-ms state)
            :ha-clock-skew-last-result (:ha-clock-skew-last-result state)
            :ha-lease-until-ms (:ha-lease-until-ms state)
            :ha-last-authority-refresh-ms
            (:ha-last-authority-refresh-ms state)
            :ha-authority-read-ok? (:ha-authority-read-ok? state)
            :ha-promotion-last-failure
            (:ha-promotion-last-failure state)
            :ha-promotion-failure-details
            (:ha-promotion-failure-details state)
            :ha-rejoin-promotion-blocked?
            (:ha-rejoin-promotion-blocked? state)
            :ha-rejoin-promotion-blocked-until-ms
            (:ha-rejoin-promotion-blocked-until-ms state)
            :ha-rejoin-promotion-cleared-ms
            (:ha-rejoin-promotion-cleared-ms state)
            :ha-candidate-since-ms (:ha-candidate-since-ms state)
            :ha-candidate-delay-ms (:ha-candidate-delay-ms state)
            :jepsen-paused? (contains? (get-in @clusters [cluster-id :paused-nodes])
                                       logical-node)
            :jepsen-storage-fault (storage-fault cluster-id logical-node)
            :ha-effective-local-lsn (effective-local-lsn deps cluster-id logical-node)}
           (authority-diagnostics->control-state authority-diagnostics)))
        (when-let [authority (get control-authorities logical-node)]
          (merge
           {:ha-role :control-only
            :jepsen-control-only? true}
           (authority-diagnostics->control-state
            (authority-diagnostics-snapshot authority))))))))

(defn local-query
  [{:keys [clusters remote-cluster?] :as deps} cluster-id logical-node q & inputs]
  (if (remote-cluster? cluster-id)
    (try
      (with-node-conn
        deps
        {:datalevin/cluster-id cluster-id
         :db-name (:db-name (get @clusters cluster-id))}
        logical-node
        nil
        (fn [conn]
          (apply d/q q @conn inputs)))
      (catch Throwable _
        unavailable-sentinel))
    (let [{:keys [db-name servers]} (get @clusters cluster-id)
          server                    (get servers logical-node)]
      (with-live-store-read-access
        server
        db-name
        (fn []
          (if-let [db (#'srv/get-db server db-name false)]
            (try
              (apply d/q q db inputs)
              (catch Throwable _
                unavailable-sentinel))
            unavailable-sentinel))))))

(defn open-leader-conn!
  [{:keys [remote-cluster? transport-failure? clusters] :as deps} test schema]
  (let [cluster-id (:datalevin/cluster-id test)
        deadline   (+ (now-ms) cluster-timeout-ms)]
    (loop []
      (let [candidate-node  (when-not (remote-cluster? cluster-id)
                              (:leader (wait-for-single-leader! deps cluster-id cluster-timeout-ms)))
            {:keys [leader snapshot]}
            (when-not (remote-cluster? cluster-id)
              (let [{:keys [live-nodes]} (get @clusters cluster-id)
                    snapshot
                    (into {}
                          (for [logical-node live-nodes
                                :let [diag          (node-diagnostics deps cluster-id logical-node)
                                      owner-node-id (:ha-authority-owner-node-id diag)
                                      owner-logical (when (some? owner-node-id)
                                                      (get-in @clusters
                                                              [cluster-id
                                                               :node-by-id
                                                               owner-node-id]))
                                      leader?       (and (= :leader (:ha-role diag))
                                                         (= logical-node owner-logical)
                                                         (true? (:ha-authority-read-ok? diag))
                                                         (not (true? (:ha-clock-skew-paused? diag))))]]
                            [logical-node
                             {:leader? leader?
                              :diagnostics diag}]))
                    leaders (->> snapshot
                                 (keep (fn [[logical-node {:keys [leader?]}]]
                                         (when leader? logical-node)))
                                 vec)]
                {:snapshot snapshot
                 :leader (when (= 1 (count leaders))
                           (first leaders))}))
            leader-node     (if (remote-cluster? cluster-id)
                              (:leader (wait-for-single-leader! deps cluster-id cluster-timeout-ms))
                              leader)]
        (cond
          (nil? leader-node)
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep (long leader-connect-retry-sleep-ms))
              (recur))
            (throw (ex-info "Timed out waiting for authoritative local leader"
                            {:cluster-id cluster-id
                             :candidate-node candidate-node
                             :authoritative-snapshot snapshot})))

          :else
          (let [leader-uri (lcluster/db-uri (get-in @clusters [cluster-id :node-by-name leader-node :endpoint])
                                            (:db-name test))
                outcome    (try
                             {:conn (lcluster/create-conn-with-timeout! leader-uri
                                                                       schema)}
                             (catch Throwable e
                               {:error e}))]
            (if-let [conn (:conn outcome)]
              conn
              (let [e (:error outcome)]
                (if (and (< (now-ms) deadline)
                         (transport-failure? e))
                  (do
                    (Thread/sleep (long leader-connect-retry-sleep-ms))
                    (recur))
                  (throw e))))))))))

(defn with-leader-conn
  [deps test schema f]
  (let [conn (open-leader-conn! deps test schema)]
    (try
      (f conn)
      (finally
        (lcluster/safe-close-conn! conn)))))

(defn open-node-conn!
  [{:keys [clusters transport-failure?]} test logical-node schema]
  (let [cluster-id (:datalevin/cluster-id test)
        deadline   (+ (now-ms) cluster-timeout-ms)]
    (loop []
      (let [node        (get-in @clusters [cluster-id :node-by-name logical-node])
            endpoint    (:endpoint node)
            outcome     (if (string? endpoint)
                          (try
                            {:conn (lcluster/create-conn-with-timeout!
                                    (lcluster/db-uri endpoint (:db-name test))
                                    schema)}
                            (catch Throwable e
                              {:error e}))
                          {:error (ex-info "Missing Jepsen node endpoint"
                                           {:cluster-id cluster-id
                                            :logical-node logical-node})})]
        (if-let [conn (:conn outcome)]
          conn
          (let [e (:error outcome)]
            (if (and (< (now-ms) deadline)
                     (transport-failure? e))
              (do
                (Thread/sleep (long leader-connect-retry-sleep-ms))
                (recur))
              (throw e))))))))

(defn with-node-conn
  [deps test logical-node schema f]
  (let [conn (open-node-conn! deps test logical-node schema)]
    (try
      (f conn)
      (finally
        (lcluster/safe-close-conn! conn)))))

(defn with-admin-node-conn
  [{:keys [clusters]} test logical-node f]
  (let [cluster-id (:datalevin/cluster-id test)
        conn       (get-in @clusters [cluster-id :admin-conns logical-node])]
    (when (d/closed? conn)
      (u/raise "Jepsen admin connection unavailable"
               {:cluster-id cluster-id
                :logical-node logical-node}))
    (f conn)))

(defn with-admin-leader-conn
  [deps test f]
  (let [cluster-id (:datalevin/cluster-id test)
        leader     (:leader (wait-for-single-leader! deps cluster-id cluster-timeout-ms))]
    (with-admin-node-conn deps test leader f)))

(defn override-node-ha-opts!
  [{:keys [clusters]} cluster-id logical-node override-opts]
  (locking clusters
    (when-let [{:keys [node-by-name remote?]} (get @clusters cluster-id)]
      (when-not (contains? node-by-name logical-node)
        (u/raise "Cannot override HA opts for unknown Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node}))
      (swap! clusters
             (fn [clusters*]
               (cond-> (assoc-in clusters*
                                 [cluster-id :node-ha-opt-overrides logical-node]
                                 override-opts)
                 remote?
                 (assoc-in [cluster-id :remote-config
                            :node-ha-opts-overrides
                            logical-node]
                           override-opts))))
      (when remote?
        (lremote/persist-remote-config! {:clusters clusters} cluster-id))
      override-opts)))

(defn clear-node-ha-opts-override!
  [{:keys [clusters]} cluster-id logical-node]
  (locking clusters
    (let [override (get-in @clusters
                           [cluster-id :node-ha-opt-overrides logical-node])
          remote?  (true? (get-in @clusters [cluster-id :remote?]))]
      (swap! clusters
             (fn [clusters*]
               (cond-> (update-in clusters*
                                  [cluster-id :node-ha-opt-overrides]
                                  dissoc logical-node)
                 remote?
                 (update-in [cluster-id :remote-config :node-ha-opts-overrides]
                            dissoc logical-node))))
      (when remote?
        (lremote/persist-remote-config! {:clusters clusters} cluster-id))
      override)))

(defn txlog-retention-state
  [{:keys [clusters remote-cluster?] :as deps} cluster-id logical-node]
  (if (remote-cluster? cluster-id)
    (with-node-kv-store
      deps
      cluster-id
      logical-node
      (fn [kv-store]
        (i/txlog-retention-state kv-store)))
    (let [{:keys [db-name servers]} (get @clusters cluster-id)
          server                    (get servers logical-node)]
      (with-live-store-read-access
        server
        db-name
        (fn []
          (when-let [state (db-state server db-name)]
            (let [store (:store state)
                  lmdb  (if (instance? Store store)
                          (.-lmdb ^Store store)
                          store)]
              (when (store-open? lmdb)
                (i/txlog-retention-state lmdb)))))))))

(defn copy-backup-pin-ids
  [deps cluster-id logical-node]
  (->> (get-in (txlog-retention-state deps cluster-id logical-node)
               [:floor-providers :backup :pins])
       (keep (fn [{:keys [pin-id expired?]}]
               (when (and (string? pin-id)
                          (not expired?)
                          (str/starts-with? pin-id "backup-copy/"))
                 pin-id)))
       sort
       vec))

(defn with-node-kv-store
  [{:keys [clusters]} cluster-id logical-node f]
  (let [{:keys [db-name node-by-name]} (get @clusters cluster-id)
        endpoint (get-in node-by-name [logical-node :endpoint])
        kv-store (r/open-kv (lcluster/db-uri endpoint db-name)
                            {:client-opts conn-client-opts})]
    (try
      (f kv-store)
      (finally
        (i/close-kv kv-store)))))

(defn assoc-opt-on-node!
  [deps cluster-id logical-node k v]
  (with-node-kv-store
    deps
    cluster-id
    logical-node
    (fn [kv-store]
      (i/assoc-opt kv-store k v))))

(defn assoc-opt-on-node-store!
  [{:keys [clusters remote-cluster?] :as deps} cluster-id logical-node k v]
  (if (remote-cluster? cluster-id)
    (assoc-opt-on-node! deps cluster-id logical-node k v)
    (let [{:keys [db-name admin-conns]} (get @clusters cluster-id)
          store (some-> (get admin-conns logical-node)
                        deref
                        .-store)]
      (when-not store
        (u/raise "Cannot update remote store opt on unavailable Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node
                  :db-name db-name}))
      (i/assoc-opt store k v))))

(defn assoc-opt-on-stopped-node-store!
  [{:keys [clusters remote-cluster? cluster-deps] :as deps}
   cluster-id logical-node k v]
  (if (remote-cluster? cluster-id)
    (let [override (merge (or (get-in @clusters
                                      [cluster-id :node-ha-opt-overrides logical-node])
                              {})
                          {k v})]
      (override-node-ha-opts! deps cluster-id logical-node override))
    (let [{:keys [db-name node-by-name servers]} (get @clusters cluster-id)
          node (get node-by-name logical-node)
          root (:root node)]
      (when-not node
        (u/raise "Cannot update local store opt on unknown Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node
                  :db-name db-name}))
      (when (get servers logical-node)
        (u/raise "Cannot update local store opt while Jepsen node is running"
                 {:cluster-id cluster-id
                  :logical-node logical-node
                  :db-name db-name}))
      (lcluster/wait-for-node-store-released! (cluster-deps)
                                              cluster-id
                                              logical-node
                                              cluster-timeout-ms)
      (let [store (#'srv/open-store root db-name nil true)]
        (try
          (i/assoc-opt store k v)
          (finally
            (i/close store)))))))

(defn clear-copy-backup-pins-on-node!
  [{:keys [clusters remote-cluster?] :as deps} cluster-id logical-node]
  (if (remote-cluster? cluster-id)
    (let [pin-ids (copy-backup-pin-ids deps cluster-id logical-node)]
      (when (seq pin-ids)
        (with-node-kv-store
          deps
          cluster-id
          logical-node
          (fn [kv-store]
            (doseq [pin-id pin-ids]
              (i/txlog-unpin-backup-floor! kv-store pin-id)))))
      {:cleared-pin-ids pin-ids
       :remaining-pin-ids (copy-backup-pin-ids deps cluster-id logical-node)})
    (let [{:keys [db-name servers]} (get @clusters cluster-id)
          state (db-state (get servers logical-node) db-name)]
      (when-not state
        (u/raise "Cannot clear copy backup pins on unavailable Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node}))
      (let [pin-ids  (copy-backup-pin-ids deps cluster-id logical-node)]
        (when (seq pin-ids)
          (with-node-kv-store
            deps
            cluster-id
            logical-node
            (fn [kv-store]
              (doseq [pin-id pin-ids]
                (i/txlog-unpin-backup-floor! kv-store pin-id)))))
        {:cleared-pin-ids pin-ids
         :remaining-pin-ids (copy-backup-pin-ids deps cluster-id logical-node)}))))

(defn create-snapshot-on-node!
  [{:keys [clusters remote-cluster?] :as deps} cluster-id logical-node]
  (let [result (if (remote-cluster? cluster-id)
                 (with-node-kv-store
                   deps
                   cluster-id
                   logical-node
                   (fn [kv-store]
                     (i/create-snapshot! kv-store)))
                 (let [{:keys [db-name servers]} (get @clusters cluster-id)
                       state (db-state (get servers logical-node) db-name)]
                   (when-not state
                     (u/raise "Cannot create snapshot on unavailable Jepsen node"
                              {:cluster-id cluster-id
                               :logical-node logical-node}))
                   (let [store  (:store state)
                         lmdb   (if (instance? Store store)
                                  (.-lmdb ^Store store)
                                  store)]
                     (i/create-snapshot! lmdb))))]
    (when-not (:ok? result)
      (u/raise "Jepsen snapshot creation failed"
               {:cluster-id cluster-id
                :logical-node logical-node
                :result result}))
    result))

(defn create-snapshots-on-nodes!
  [deps cluster-id logical-nodes]
  (into {}
        (map (fn [logical-node]
               [logical-node
                (create-snapshot-on-node! deps cluster-id logical-node)]))
        logical-nodes))

(defn gc-txlog-segments-on-node!
  [{:keys [clusters remote-cluster?] :as deps} cluster-id logical-node]
  (let [result (if (remote-cluster? cluster-id)
                 (with-node-kv-store
                   deps
                   cluster-id
                   logical-node
                   (fn [kv-store]
                     (i/gc-txlog-segments! kv-store)))
                 (let [{:keys [db-name servers]} (get @clusters cluster-id)
                       state (db-state (get servers logical-node) db-name)]
                   (when-not state
                     (u/raise "Cannot GC WAL segments on unavailable Jepsen node"
                              {:cluster-id cluster-id
                               :logical-node logical-node}))
                   (let [store  (:store state)
                         lmdb   (if (instance? Store store)
                                  (.-lmdb ^Store store)
                                  store)]
                     (i/gc-txlog-segments! lmdb))))]
    (when-not (:ok? result)
      (u/raise "Jepsen WAL GC failed"
               {:cluster-id cluster-id
                :logical-node logical-node
                :result result}))
    result))

(defn wait-for-follower-bootstrap!
  [deps cluster-id logical-node min-snapshot-lsn timeout-ms]
  (let [timeout-ms       (long timeout-ms)
        deadline         (+ (now-ms) timeout-ms)
        min-snapshot-lsn (long min-snapshot-lsn)]
    (loop [last-state nil]
      (let [state        (node-diagnostics deps cluster-id logical-node)
            applied-lsn  (long (or (:ha-local-last-applied-lsn state) 0))
            snapshot-lsn (long (or (:ha-follower-bootstrap-snapshot-last-applied-lsn
                                    state)
                                   0))]
        (cond
          (and state
               (integer? (:ha-follower-last-bootstrap-ms state))
               (string? (:ha-follower-bootstrap-source-endpoint state))
               (>= applied-lsn snapshot-lsn)
               (>= snapshot-lsn min-snapshot-lsn))
          state

          (< (now-ms) deadline)
          (do
            (Thread/sleep 250)
            (recur (or state last-state)))

          :else
          (throw (ex-info "Timed out waiting for Jepsen follower snapshot bootstrap"
                          {:cluster-id cluster-id
                           :logical-node logical-node
                           :timeout-ms timeout-ms
                           :min-snapshot-lsn min-snapshot-lsn
                           :last-state last-state})))))))
