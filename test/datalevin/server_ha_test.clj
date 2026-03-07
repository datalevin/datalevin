(ns datalevin.server-ha-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ha]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.server :as srv]
   [datalevin.storage :as st]
   [datalevin.util :as u]
   [datalevin.validate :as vld]
   [taoensso.timbre :as log])
  (:import
   [datalevin.storage Store]
   [datalevin.server Server]
   [java.net ConnectException]
   [java.nio.channels ClosedChannelException]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]
   [java.util.concurrent.atomic AtomicBoolean]))

(defn- quiet-server-ha-logs-fixture
  [f]
  (let [old-config log/*config*
        orig-create @#'srv/create]
    (log/set-min-level! :error)
    (try
      (with-redefs [srv/create
                    (fn [opts]
                      (let [server (orig-create opts)]
                        (log/set-min-level! :error)
                        server))]
        (f))
      (finally
        (log/set-config! old-config)))))

(use-fixtures :once quiet-server-ha-logs-fixture)

(defn- valid-ha-opts
  ([]
   (valid-ha-opts (str "ha-test-" (UUID/randomUUID))))
  ([group-id]
   {:ha-mode :consensus-lease
    :db-identity (str "db-" (UUID/randomUUID))
    :ha-node-id 2
    :ha-lease-renew-ms 5000
    :ha-lease-timeout-ms 15000
    :ha-promotion-base-delay-ms 300
    :ha-promotion-rank-delay-ms 700
    :ha-max-promotion-lag-lsn 0
    :ha-clock-skew-budget-ms 100
    :ha-fencing-hook {:cmd ["/bin/sh" "-c" "exit 0"]
                      :timeout-ms 1000
                      :retries 0
                      :retry-delay-ms 0}
    :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                 {:node-id 2 :endpoint "10.0.0.12:8898"}
                 {:node-id 3 :endpoint "10.0.0.13:8898"}]
    :ha-control-plane {:backend :in-memory
                       :group-id group-id
                       :local-peer-id "10.0.0.12:7801"
                       :voters [{:peer-id "10.0.0.11:7801" :ha-node-id 1 :promotable? true}
                                {:peer-id "10.0.0.12:7801" :ha-node-id 2 :promotable? true}
                                {:peer-id "10.0.0.13:7801" :ha-node-id 3 :promotable? true}]}}))

(def ^:private ha-runtime-option-keys
  [:ha-mode
   :db-identity
   :ha-node-id
   :ha-members
   :ha-lease-renew-ms
   :ha-lease-timeout-ms
   :ha-promotion-base-delay-ms
   :ha-promotion-rank-delay-ms
   :ha-max-promotion-lag-lsn
   :ha-fencing-hook
   :ha-clock-skew-budget-ms
   :ha-clock-skew-hook
   :ha-control-plane])

(defn- resolved-ha-runtime-opts
  [root db-name ha-opts]
  (-> (#'srv/with-default-ha-control-raft-dir root db-name ha-opts)
      (select-keys ha-runtime-option-keys)))

(def ^:private e2e-ha-schema
  {:drill/key {:db/valueType :db.type/string
               :db/unique :db.unique/identity}
   :drill/value {:db/valueType :db.type/string}})

(def ^:private e2e-ha-value-query
  '[:find ?v .
    :in $ ?k
    :where
    [?e :drill/key ?k]
    [?e :drill/value ?v]])

(def ^:private e2e-ha-conn-client-opts
  {:pool-size 1
   :time-out 60000})

(defn- now-ms
  []
  (System/currentTimeMillis))

(defn- random-port-candidate
  []
  (+ 35000 (rand-int 20000)))

(defn- e2e-admin-uri
  [endpoint]
  (str "dtlv://" c/default-username ":" c/default-password "@" endpoint))

(defn- e2e-db-uri
  [endpoint db-name]
  (str (e2e-admin-uri endpoint) "/" db-name))

(defn- e2e-make-nodes
  [work-dir]
  (let [ports (loop [chosen #{}]
                (if (= 6 (count chosen))
                  (vec chosen)
                  (recur (conj chosen (random-port-candidate)))))]
    (mapv
     (fn [idx]
       (let [node-id (inc idx)
             port (nth ports idx)
             peer-port (nth ports (+ 3 idx))]
         {:node-id node-id
          :port port
          :endpoint (str "127.0.0.1:" port)
          :peer-port peer-port
          :peer-id (str "127.0.0.1:" peer-port)
          :root (str work-dir u/+separator+ "node-" node-id)}))
     (range 3))))

(defn- e2e-promotable-voters
  [nodes]
  (mapv (fn [{:keys [node-id peer-id]}]
          {:peer-id peer-id
           :ha-node-id node-id
           :promotable? true})
        nodes))

(defn- e2e-base-ha-opts
  [nodes group-id db-identity control-backend]
  {:wal? true
   :db-identity db-identity
   :ha-mode :consensus-lease
   :ha-lease-renew-ms 1000
   :ha-lease-timeout-ms 3000
   :ha-promotion-base-delay-ms 100
   :ha-promotion-rank-delay-ms 200
   :ha-max-promotion-lag-lsn 0
   :ha-clock-skew-budget-ms 1000
   :ha-fencing-hook {:cmd ["/bin/sh" "-c" "exit 0"]
                     :timeout-ms 1000
                     :retries 0
                     :retry-delay-ms 0}
   :ha-members (mapv #(select-keys % [:node-id :endpoint]) nodes)
   :ha-control-plane
   {:backend (if (= :in-memory control-backend)
               :sofa-jraft
               control-backend)
    :group-id group-id
    :voters (e2e-promotable-voters nodes)
    :rpc-timeout-ms 5000
    :election-timeout-ms 5000
    :operation-timeout-ms 30000}})

(defn- e2e-node-ha-opts
  [base-opts node]
  (-> base-opts
      (assoc :ha-node-id (:node-id node))
      (assoc-in [:ha-control-plane :local-peer-id] (:peer-id node))))

(declare e2e-effective-local-lsn)
(declare e2e-write-value!)
(declare e2e-verify-value-on-nodes!)

(defn- start-e2e-ha-server!
  [node]
  (u/create-dirs (:root node))
  (let [server (binding [c/*db-background-sampling?* false]
                 (srv/create {:port (:port node)
                              :root (:root node)}))]
    (binding [c/*db-background-sampling?* false]
      (srv/start server))
    server))

(defn- open-e2e-ha-conn!
  [node db-name opts]
  (d/create-conn (e2e-db-uri (:endpoint node) db-name)
                 e2e-ha-schema
                 (assoc opts :client-opts e2e-ha-conn-client-opts)))

(defn- safe-close-e2e-ha-conn!
  [conn]
  (when conn
    (try
      (d/close conn)
      (catch Throwable _ nil))))

(defn- safe-stop-e2e-ha-server!
  [server]
  (when server
    (try
      (srv/stop server)
      (catch Throwable _ nil))))

(defn- safe-delete-dir!
  [path]
  (when path
    (try
      (u/delete-files path)
      (catch Throwable _ nil))))

(defn- e2e-db-state
  [server db-name]
  (when server
    (get (.-dbs ^Server server) db-name)))

(defn- e2e-node-diagnostics
  [server db-name]
  (if-let [db-state (e2e-db-state server db-name)]
    (let [store (:store db-state)
          store-opts (when (instance? Store store)
                       (i/opts store))]
      (merge
       (select-keys db-state
                    [:ha-role
                     :ha-local-last-applied-lsn
                     :ha-last-authority-refresh-ms
                     :ha-follower-next-lsn
                     :ha-follower-last-sync-ms
                     :ha-follower-leader-endpoint
                     :ha-follower-source-endpoint
                     :ha-follower-source-order
                     :ha-authority-owner-node-id
                     :ha-authority-term
                     :ha-authority-version
                     :ha-authority-read-ok?
                     :ha-authority-read-error
                     :ha-follower-degraded?
                     :ha-promotion-last-failure
                     :ha-promotion-failure-details
                     :ha-follower-last-error
                     :ha-follower-last-error-details])
       {:ha-authority-diagnostics
        (ha/authority-diagnostics (:ha-authority db-state))
        :ha-authority-lease
        (some-> (:ha-authority-lease db-state)
                (select-keys [:leader-node-id
                              :leader-endpoint
                              :leader-last-applied-lsn
                              :term
                              :lease-until-ms
                              :updated-ms]))
        :ha-renew-loop-running?
        (some-> (:ha-renew-loop-running? db-state) .get)
        :ha-effective-local-lsn
        (e2e-effective-local-lsn
         {:servers {(-> db-state :ha-node-id) server}
          :db-name db-name}
         (:ha-node-id db-state))
        :ha-runtime-opts
        (some-> (:ha-runtime-opts db-state)
                (select-keys [:ha-mode
                              :db-identity
                              :ha-node-id
                              :ha-members
                              :ha-control-plane]))
        :store-ha-opts
        (some-> store-opts
                (select-keys [:ha-mode
                              :db-identity
                              :ha-node-id
                              :ha-members
                              :ha-control-plane]))}))
    {:status :missing-db-state}))

(defn- e2e-node-diagnostics-by-node
  [servers db-name live-node-ids]
  (into {}
        (map (fn [node-id]
               [node-id
                (e2e-node-diagnostics (get servers node-id) db-name)]))
        live-node-ids))

(defn- e2e-probe-write-admission
  [server db-name]
  (let [db-state (e2e-db-state server db-name)
        probe (select-keys db-state
                           [:ha-role
                            :ha-authority-owner-node-id
                            :ha-authority-read-ok?
                            :ha-clock-skew-paused?
                            :ha-follower-degraded?
                            :ha-promotion-last-failure
                            :ha-promotion-failure-details
                            :ha-follower-last-error
                            :ha-follower-last-error-details])]
    (cond
      (nil? server)
      {:status :down}

      (nil? db-state)
      {:status :missing-db-state}

      (nil? (:ha-authority db-state))
      (merge probe {:status :ha-runtime-missing})

      :else
      (try
        (merge
         probe
         (if-let [err (#'srv/ha-write-admission-error
                       server
                       {:type :open-dbi
                        :args [db-name "__ha_e2e_probe" nil]})]
           {:status :rejected
            :reason (:reason err)
            :error (:error err)}
           {:status :leader}))
        (catch Throwable e
          (merge probe
                 {:status :probe-failed
                  :message (ex-message e)}))))))

(defn- e2e-probe-snapshot
  [servers db-name live-node-ids]
  (into {}
        (map (fn [node-id]
               [node-id
                (e2e-probe-write-admission (get servers node-id) db-name)]))
        live-node-ids))

(defn- e2e-wait-for-single-leader!
  [servers db-name live-node-ids timeout-ms]
  (let [deadline (+ (now-ms) timeout-ms)]
    (loop [last-snapshot nil]
      (let [snapshot (e2e-probe-snapshot servers db-name live-node-ids)
            leaders (->> snapshot
                         (keep (fn [[node-id {:keys [status]}]]
                                 (when (= :leader status) node-id)))
                         vec)]
        (cond
          (= 1 (count leaders))
          {:leader-id (first leaders)
           :snapshot snapshot}

          (> (count leaders) 1)
          (throw (ex-info "Multiple leaders detected"
                          {:probe-snapshot snapshot
                           :node-diagnostics
                           (e2e-node-diagnostics-by-node
                            servers db-name live-node-ids)}))

          (< (now-ms) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info "Timed out waiting for single leader"
                          {:label "single leader"
                           :timeout-ms timeout-ms
                           :probe-snapshot snapshot
                           :previous-snapshot last-snapshot
                           :node-diagnostics
                           (e2e-node-diagnostics-by-node
                            servers db-name live-node-ids)})))))))

(defn- e2e-read-value
  [conn key]
  (d/q e2e-ha-value-query @conn key))

(defn- e2e-store-open?
  [store]
  (cond
    (nil? store) false
    (instance? Store store) (not (i/closed? store))
    :else (not (i/closed-kv? store))))

(defn- e2e-local-read-value
  [ctx node-id key]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    (let [store (:store db-state)]
      (when (e2e-store-open? store)
        (try
          (d/q e2e-ha-value-query
               (db/new-db store)
               key)
          (catch Throwable _
            ;; Snapshot install closes the old LMDB before the reopened store is
            ;; published into the server map. Treat that swap window as retryable.
            nil))))))

(defn- e2e-local-watermarks
  [ctx node-id]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    (let [store (:store db-state)
          lmdb (if (instance? Store store)
                 (.-lmdb ^Store store)
                 store)]
      (when (e2e-store-open? lmdb)
        (try
          (kv/txlog-watermarks lmdb)
          (catch Throwable _
            nil))))))

(defn- e2e-local-ha-persisted-lsn
  [db-state]
  (let [store (:store db-state)
        lmdb (if (instance? Store store)
               (.-lmdb ^Store store)
               store)]
    (long (or (try
                (i/get-value lmdb c/kv-info c/ha-local-applied-lsn
                             :keyword :data)
                (catch Throwable _
                  nil))
              0))))

(defn- e2e-effective-local-lsn
  [ctx node-id]
  (if-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                  (:db-name ctx))]
    (let [txlog-lsn (long (or (:last-applied-lsn
                               (e2e-local-watermarks ctx node-id))
                              0))
          runtime-lsn (long (or (:ha-local-last-applied-lsn db-state) 0))
          persisted-lsn (e2e-local-ha-persisted-lsn db-state)
          comparable-lsn (long (max runtime-lsn persisted-lsn))]
      (if (= :leader (:ha-role db-state))
        (long (max txlog-lsn comparable-lsn))
        comparable-lsn))
    0))

(defn- e2e-retention-state-on-node
  [ctx node-id]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    (let [store (:store db-state)
          lmdb (if (instance? Store store)
                 (.-lmdb ^Store store)
                 store)]
      (i/txlog-retention-state lmdb))))

(defn- e2e-create-snapshot-on-node!
  [ctx node-id]
  (let [store (-> (get-in ctx [:conns node-id]) deref .-store)
        result (i/create-snapshot! store)]
    (when-not (:ok? result)
      (throw (ex-info "Failed to create e2e HA snapshot"
                      {:node-id node-id
                       :result result})))
    result))

(defn- e2e-create-snapshots-on-nodes!
  [ctx node-ids]
  (into {}
        (map (fn [node-id]
               [node-id (e2e-create-snapshot-on-node! ctx node-id)]))
        node-ids))

(defn- e2e-gc-txlog-segments-on-node!
  [ctx node-id]
  (let [store (-> (get-in ctx [:conns node-id]) deref .-store)
        result (i/gc-txlog-segments! store)]
    (when-not (:ok? result)
      (throw (ex-info "Failed to GC e2e HA WAL segments"
                      {:node-id node-id
                       :result result})))
    result))

(defn- e2e-write-series-with-rolls!
  [ctx leader-id prefix n sleep-ms]
  (doseq [idx (range n)]
    (e2e-write-value! (:conns ctx)
                      leader-id
                      (str prefix "-" idx)
                      (str prefix "-v-" idx))
    (Thread/sleep sleep-ms))
  (let [target-lsn (e2e-effective-local-lsn ctx leader-id)]
    (e2e-verify-value-on-nodes! ctx (:live-node-ids ctx) (str prefix "-0")
                                (str prefix "-v-0"))
    target-lsn))

(defn- e2e-clock-skew-script-path
  []
  (.getCanonicalPath (io/file "." "script" "ha" "clock-skew-file.sh")))

(defn- e2e-fence-script-path
  []
  (.getCanonicalPath (io/file "." "script" "ha" "fence-log.sh")))

(defn- e2e-fence-hook-config
  [log-file exit-code]
  {:cmd [(e2e-fence-script-path) log-file (str exit-code)]
   :timeout-ms 1000
   :retries 0
   :retry-delay-ms 0})

(defn- e2e-parse-fence-log-line
  [line]
  (let [parts (s/split line #"," 8)
        [timestamp-ms db-name fence-op-id observed-term candidate-term new-node-id
         old-node-id old-leader-endpoint]
        (if (= 6 (count parts))
          (let [[ts op observed candidate new-id old-id] parts]
            [ts nil op observed candidate new-id old-id nil])
          (into (vec parts)
                (repeat (max 0 (- 8 (count parts))) nil)))]
    {:timestamp-ms timestamp-ms
     :db-name db-name
     :fence-op-id fence-op-id
     :observed-term observed-term
     :candidate-term candidate-term
     :new-node-id new-node-id
     :old-node-id old-node-id
     :old-leader-endpoint old-leader-endpoint}))

(defn- e2e-read-fence-log
  [path]
  (let [f (io/file path)]
    (if (.exists f)
      (->> (line-seq (io/reader f))
           (remove s/blank?)
           (mapv e2e-parse-fence-log-line))
      [])))

(defn- e2e-clock-skew-hook-config
  [state-dir]
  {:cmd [(e2e-clock-skew-script-path) state-dir]
   :timeout-ms 1000
   :retries 0
   :retry-delay-ms 0})

(defn- e2e-clock-skew-state-file
  [state-dir node-id]
  (str state-dir u/+separator+ "clock-skew-" node-id ".txt"))

(defn- e2e-write-clock-skew-ms!
  [state-dir node-id skew-ms]
  (u/create-dirs state-dir)
  (spit (e2e-clock-skew-state-file state-dir node-id) (str (long skew-ms))))

(defn- e2e-clock-skew-state
  [ctx node-id]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    {:ha-role (:ha-role db-state)
     :ha-authority-owner-node-id (:ha-authority-owner-node-id db-state)
     :ha-authority-term (:ha-authority-term db-state)
     :ha-clock-skew-paused? (:ha-clock-skew-paused? db-state)
     :ha-clock-skew-last-observed-ms (:ha-clock-skew-last-observed-ms db-state)
     :ha-clock-skew-last-result (:ha-clock-skew-last-result db-state)
     :ha-promotion-last-failure (:ha-promotion-last-failure db-state)
     :ha-promotion-failure-details (:ha-promotion-failure-details db-state)}))

(defn- e2e-follower-state
  [ctx node-id]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    {:ha-role (:ha-role db-state)
     :ha-authority-owner-node-id (:ha-authority-owner-node-id db-state)
     :ha-authority-term (:ha-authority-term db-state)
     :ha-local-last-applied-lsn (:ha-local-last-applied-lsn db-state)
     :ha-follower-next-lsn (:ha-follower-next-lsn db-state)
     :ha-follower-last-bootstrap-ms (:ha-follower-last-bootstrap-ms db-state)
     :ha-follower-bootstrap-source-endpoint
     (:ha-follower-bootstrap-source-endpoint db-state)
     :ha-follower-bootstrap-snapshot-last-applied-lsn
     (:ha-follower-bootstrap-snapshot-last-applied-lsn db-state)
     :ha-follower-degraded? (:ha-follower-degraded? db-state)
     :ha-follower-degraded-reason (:ha-follower-degraded-reason db-state)
     :ha-follower-last-error (:ha-follower-last-error db-state)
     :ha-follower-last-error-details (:ha-follower-last-error-details db-state)
     :ha-promotion-last-failure (:ha-promotion-last-failure db-state)
     :ha-promotion-failure-details (:ha-promotion-failure-details db-state)
     :last-applied-lsn (e2e-effective-local-lsn ctx node-id)}))

(defn- e2e-verify-value-on-nodes!
  [ctx live-node-ids key expected]
  (let [deadline (+ (now-ms) 15000)]
    (loop []
      (if (every? (fn [node-id]
                    (= expected
                       (e2e-local-read-value ctx node-id key)))
                  live-node-ids)
        true
        (if (< (now-ms) deadline)
          (do
            (Thread/sleep 250)
            (recur))
          (throw (ex-info "Timed out waiting for replicated value"
                          {:label :replicated-value
                           :key key
                           :expected expected
                           :live-node-ids live-node-ids
                           :node-values
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (e2e-local-read-value ctx node-id key)]))
                                 live-node-ids)
                           :node-watermarks
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (select-keys
                                          (or (e2e-local-watermarks ctx node-id)
                                              {})
                                          [:last-applied-lsn
                                           :durable-lsn
                                           :replica-floor-lsn])]))
                                 live-node-ids)
                           :node-diagnostics
                           (e2e-node-diagnostics-by-node
                            (:servers ctx)
                            (:db-name ctx)
                            live-node-ids)})))))))

(defn- e2e-write-value!
  [conns leader-id key value]
  (d/transact! (get conns leader-id)
               [{:drill/key key
                 :drill/value value}]))

(defmacro with-e2e-control-backend
  [control-backend & body]
  `(with-redefs [srv/*start-ha-authority-fn*
                 (fn [db-name# ha-opts#]
                   (dha/start-ha-authority
                    db-name#
                    (assoc-in ha-opts#
                              [:ha-control-plane :backend]
                              ~control-backend)))
                 srv/*stop-ha-authority-fn*
                 dha/stop-ha-authority]
     ~@body))

(defn- with-e2e-ha-cluster
  ([control-backend f]
   (with-e2e-ha-cluster control-backend {} f))
  ([control-backend {:keys [base-opts-fn]} f]
   (let [work-dir (u/tmp-dir (str "ha-e2e-" (name control-backend) "-"
                                  (UUID/randomUUID)))
         nodes (e2e-make-nodes work-dir)
         db-name "ha-e2e"
         group-id (str "ha-e2e-" (UUID/randomUUID))
         db-identity (str "db-" (UUID/randomUUID))
         default-opts (e2e-base-ha-opts nodes
                                        group-id
                                        db-identity
                                        control-backend)
         base-opts (if base-opts-fn
                     (base-opts-fn default-opts)
                     default-opts)
         ctx-atom (atom {:work-dir work-dir
                         :nodes nodes
                         :db-name db-name
                         :control-backend control-backend
                         :base-opts base-opts
                         :group-id group-id
                         :db-identity db-identity
                         :servers {}
                         :conns {}
                         :live-node-ids #{}})]
     (try
       (with-e2e-control-backend control-backend
         (doseq [node nodes]
           (swap! ctx-atom assoc-in [:servers (:node-id node)]
                  (start-e2e-ha-server! node)))
         (let [conn-futures
               (into {}
                     (map (fn [node]
                            [(:node-id node)
                             (future
                               (open-e2e-ha-conn!
                                node
                                db-name
                                (e2e-node-ha-opts base-opts node)))]))
                     nodes)]
           (doseq [node nodes]
             (swap! ctx-atom assoc-in [:conns (:node-id node)]
                    @(get conn-futures (:node-id node)))))
         (swap! ctx-atom assoc :live-node-ids (set (map :node-id nodes)))
         (f ctx-atom))
       (finally
         (doseq [conn (vals (:conns @ctx-atom))]
           (safe-close-e2e-ha-conn! conn))
         (doseq [server (vals (:servers @ctx-atom))]
           (safe-stop-e2e-ha-server! server))
         (safe-delete-dir! work-dir))))))

(defn- e2e-stop-node
  [ctx node-id]
  (safe-close-e2e-ha-conn! (get-in ctx [:conns node-id]))
  (safe-stop-e2e-ha-server! (get-in ctx [:servers node-id]))
  (-> ctx
      (assoc-in [:conns node-id] nil)
      (assoc-in [:servers node-id] nil)
      (update :live-node-ids disj node-id)))

(defn- e2e-restart-node
  [ctx node-id]
  (let [node (get-in ctx [:nodes (dec node-id)])
        server (start-e2e-ha-server! node)
        conn (open-e2e-ha-conn! node
                                (:db-name ctx)
                                (e2e-node-ha-opts (:base-opts ctx) node))]
    (-> ctx
        (assoc-in [:servers node-id] server)
        (assoc-in [:conns node-id] conn)
        (update :live-node-ids conj node-id))))

(defn- e2e-wait-for-follower-rejoin!
  [ctx-atom node-id leader-id min-lsn]
  (let [timeout-ms 30000
        deadline (+ (now-ms) timeout-ms)]
    (loop [last-state nil]
      (let [ctx @ctx-atom
            db-state (e2e-db-state (get-in ctx [:servers node-id])
                                   (:db-name ctx))
            state (when db-state
                    {:ha-role (:ha-role db-state)
                     :ha-authority-owner-node-id
                     (:ha-authority-owner-node-id db-state)
                     :ha-authority-term (:ha-authority-term db-state)
                     :ha-follower-degraded? (:ha-follower-degraded? db-state)
                     :ha-follower-last-error (:ha-follower-last-error db-state)
                     :last-applied-lsn (e2e-effective-local-lsn ctx node-id)})]
        (if (and state
                 (= :follower (:ha-role state))
                 (= leader-id (:ha-authority-owner-node-id state))
                 (integer? (:ha-authority-term state))
                 (pos? (long (:ha-authority-term state)))
                 (>= (long (:last-applied-lsn state)) (long min-lsn))
                 (not (:ha-follower-degraded? state))
                 (nil? (:ha-follower-last-error state)))
          state
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or state last-state)))
            (throw (ex-info "Timed out waiting for follower-only rejoin"
                            {:node-id node-id
                             :leader-id leader-id
                             :min-lsn min-lsn
                             :timeout-ms timeout-ms
                             :last-state last-state
                             :node-diagnostics
                             (e2e-node-diagnostics-by-node
                              (:servers ctx)
                              (:db-name ctx)
                              (:live-node-ids ctx))}))))))))

(defn- e2e-wait-for-follower-bootstrap!
  [ctx-atom node-id min-applied-lsn]
  (let [timeout-ms 30000
        deadline (+ (now-ms) timeout-ms)]
    (loop [last-state nil]
      (let [ctx @ctx-atom
            state (e2e-follower-state ctx node-id)
            applied-lsn (long (or (:ha-local-last-applied-lsn state) 0))
            snapshot-lsn (long (or (:ha-follower-bootstrap-snapshot-last-applied-lsn
                                    state)
                                   0))]
        (if (and state
                 (integer? (:ha-follower-last-bootstrap-ms state))
                 (string? (:ha-follower-bootstrap-source-endpoint state))
                 (>= applied-lsn snapshot-lsn)
                 (pos? snapshot-lsn)
                 (>= applied-lsn (long min-applied-lsn))
                 (not (:ha-follower-degraded? state))
                 (nil? (:ha-follower-last-error state)))
          state
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or state last-state)))
            (throw (ex-info "Timed out waiting for follower snapshot bootstrap"
                            {:node-id node-id
                             :min-applied-lsn min-applied-lsn
                             :timeout-ms timeout-ms
                             :last-state last-state
                             :node-diagnostics
                             (e2e-node-diagnostics-by-node
                              (:servers ctx)
                              (:db-name ctx)
                              (:live-node-ids ctx))}))))))))

(defn- e2e-wait-for-follower-degraded!
  [ctx-atom node-id]
  (let [timeout-ms 30000
        deadline (+ (now-ms) timeout-ms)]
    (loop [last-state nil]
      (let [ctx @ctx-atom
            state (e2e-follower-state ctx node-id)]
        (if (and state
                 (= :follower (:ha-role state))
                 (true? (:ha-follower-degraded? state))
                 (= :wal-gap (:ha-follower-degraded-reason state))
                 (= :sync-failed (:ha-follower-last-error state)))
          state
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or state last-state)))
            (throw (ex-info "Timed out waiting for follower degraded mode"
                            {:node-id node-id
                             :timeout-ms timeout-ms
                             :last-state last-state
                             :node-diagnostics
                             (e2e-node-diagnostics-by-node
                              (:servers ctx)
                              (:db-name ctx)
                              (:live-node-ids ctx))}))))))))

(defn- e2e-wait-for-follower-stays-degraded!
  [ctx-atom node-id leader-id]
  (let [timeout-ms 30000
        deadline (+ (now-ms) timeout-ms)]
    (loop [last-state nil]
      (let [ctx @ctx-atom
            state (e2e-follower-state ctx node-id)]
        (if (and state
                 (= :follower (:ha-role state))
                 (= leader-id (:ha-authority-owner-node-id state))
                 (integer? (:ha-authority-term state))
                 (pos? (long (:ha-authority-term state)))
                 (true? (:ha-follower-degraded? state))
                 (= :wal-gap (:ha-follower-degraded-reason state)))
          state
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or state last-state)))
            (throw (ex-info "Timed out waiting for degraded follower state to persist"
                            {:node-id node-id
                             :leader-id leader-id
                             :timeout-ms timeout-ms
                             :last-state last-state
                             :node-diagnostics
                             (e2e-node-diagnostics-by-node
                              (:servers ctx)
                              (:db-name ctx)
                              (:live-node-ids ctx))}))))))))

(defn- e2e-wait-for-replica-floor!
  [ctx-atom leader-id follower-id min-lsn]
  (let [timeout-ms 10000
        deadline (+ (now-ms) timeout-ms)]
    (loop [last-state nil]
      (let [ctx @ctx-atom
            state (e2e-retention-state-on-node ctx leader-id)
            replicas (get-in state [:floor-providers :replica :replicas])
            replica (some #(when (= follower-id (:replica-id %)) %) replicas)]
        (if (and replica
                 (not (:stale? replica))
                 (>= (long (or (:floor-lsn replica) 0))
                     (long min-lsn)))
          {:replica replica
           :retention-state state}
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or state last-state)))
            (throw (ex-info "Timed out waiting for replica floor refresh"
                            {:leader-id leader-id
                             :follower-id follower-id
                             :min-lsn min-lsn
                             :timeout-ms timeout-ms
                             :last-state last-state}))))))))

(defn- e2e-wait-for-clock-skew-block!
  [ctx-atom paused-node-ids]
  (let [timeout-ms 15000
        deadline (+ (now-ms) timeout-ms)]
    (loop [last-data nil]
      (let [ctx @ctx-atom
            snapshot (e2e-probe-snapshot (:servers ctx)
                                         (:db-name ctx)
                                         (:live-node-ids ctx))
            leaders (->> snapshot
                         (keep (fn [[node-id {:keys [status]}]]
                                 (when (= :leader status) node-id)))
                         vec)
            states (into {}
                         (map (fn [node-id]
                                [node-id (e2e-clock-skew-state ctx node-id)]))
                         paused-node-ids)]
        (if (and (empty? leaders)
                 (every? (fn [[_ state]]
                           (and state
                                (= :follower (:ha-role state))
                                (true? (:ha-clock-skew-paused? state))
                                (= :clock-skew-paused
                                   (:ha-promotion-last-failure state))
                                (= :clock-skew-budget-breached
                                   (get-in state
                                           [:ha-promotion-failure-details
                                            :reason]))))
                         states))
          {:probe-snapshot snapshot
           :paused-states states}
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur {:probe-snapshot snapshot
                      :paused-states states}))
            (throw (ex-info "Timed out waiting for clock-skew pause to block failover"
                            {:paused-node-ids paused-node-ids
                             :timeout-ms timeout-ms
                             :last-data last-data
                             :node-diagnostics
                             (e2e-node-diagnostics-by-node
                              (:servers ctx)
                              (:db-name ctx)
                              (:live-node-ids ctx))}))))))))

(defn- e2e-wait-for-clock-skew-clear!
  [ctx-atom node-id leader-id role]
  (let [timeout-ms 15000
        deadline (+ (now-ms) timeout-ms)]
    (loop [last-state nil]
      (let [ctx @ctx-atom
            state (e2e-clock-skew-state ctx node-id)]
        (if (and state
                 (= role (:ha-role state))
                 (= leader-id (:ha-authority-owner-node-id state))
                 (integer? (:ha-authority-term state))
                 (pos? (long (:ha-authority-term state)))
                 (false? (:ha-clock-skew-paused? state))
                 (= :clock-skew-within-budget
                    (get-in state [:ha-clock-skew-last-result :reason])))
          state
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or state last-state)))
            (throw (ex-info "Timed out waiting for clock-skew pause to clear"
                            {:node-id node-id
                             :leader-id leader-id
                             :role role
                             :timeout-ms timeout-ms
                             :last-state last-state
                             :node-diagnostics
                             (e2e-node-diagnostics-by-node
                              (:servers ctx)
                              (:db-name ctx)
                              (:live-node-ids ctx))}))))))))

(defn- e2e-wait-for-fence-entry!
  [ctx-atom log-file expected-new-node-id]
  (let [timeout-ms 10000
        deadline (+ (now-ms) timeout-ms)
        expected-node-id (str expected-new-node-id)]
    (loop [last-entries nil]
      (let [entries (e2e-read-fence-log log-file)
            matching-entries (->> entries
                                  (filter #(= expected-node-id
                                              (:new-node-id %)))
                                  vec)]
        (if (seq matching-entries)
          {:entries entries
           :matching-entries matching-entries}
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 200)
              (recur entries))
            (throw (ex-info "Timed out waiting for e2e fencing hook verification entry"
                            {:log-file log-file
                             :expected-new-node-id expected-new-node-id
                             :timeout-ms timeout-ms
                             :last-entries last-entries}))))))))

(defn- e2e-verify-fencing-hook-entry
  [ctx initial-leader-id initial-endpoint new-leader-id entry]
  (let [db-name (:db-name ctx)
        observed-term (parse-long (or (:observed-term entry) ""))
        candidate-term (parse-long (or (:candidate-term entry) ""))]
    (when-not (= db-name (:db-name entry))
      (throw (ex-info "E2E fencing hook entry recorded unexpected DB name"
                      {:expected-db-name db-name
                       :entry entry})))
    (when-not (= (str initial-leader-id) (:old-node-id entry))
      (throw (ex-info "E2E fencing hook entry recorded unexpected old leader node"
                      {:expected-old-node-id initial-leader-id
                       :entry entry})))
    (when-not (= initial-endpoint (:old-leader-endpoint entry))
      (throw (ex-info "E2E fencing hook entry recorded unexpected old leader endpoint"
                      {:expected-old-leader-endpoint initial-endpoint
                       :entry entry})))
    (when-not (= (str new-leader-id) (:new-node-id entry))
      (throw (ex-info "E2E fencing hook entry recorded unexpected new leader node"
                      {:expected-new-node-id new-leader-id
                       :entry entry})))
    (when-not (and (integer? observed-term)
                   (integer? candidate-term)
                   (= (unchecked-inc (long observed-term))
                      (long candidate-term)))
      (throw (ex-info "E2E fencing hook entry recorded unexpected leader term transition"
                      {:entry entry})))
    (let [expected-fence-op-id (str db-name ":" observed-term ":" new-leader-id)]
      (when-not (= expected-fence-op-id (:fence-op-id entry))
        (throw (ex-info "E2E fencing hook entry recorded unexpected fence op id"
                        {:expected-fence-op-id expected-fence-op-id
                         :entry entry})))
      (assoc entry :expected-fence-op-id expected-fence-op-id))))

(defn- e2e-drifted-ha-members
  [members target-node-id]
  (mapv (fn [member]
          (if (= target-node-id (:node-id member))
            (assoc member :endpoint
                   (str "127.0.0.1:" (+ 29000 target-node-id)))
            member))
        members))

(defn- run-e2e-ha-failover!
  [control-backend]
  (with-e2e-ha-cluster
    control-backend
    (fn [ctx-atom]
      (let [ctx @ctx-atom
            leader-timeout-ms (if (= :sofa-jraft control-backend) 20000 10000)
            {:keys [leader-id]}
            (e2e-wait-for-single-leader! (:servers ctx)
                                         (:db-name ctx)
                                         (:live-node-ids ctx)
                                         leader-timeout-ms)
            leader-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])]
        (e2e-write-value! (:conns ctx) leader-id "seed" "v1")
        (e2e-verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1")
        (swap! ctx-atom e2e-stop-node leader-id)
        (let [ctx @ctx-atom
              {:keys [leader-id snapshot]}
              (e2e-wait-for-single-leader! (:servers ctx)
                                           (:db-name ctx)
                                           (:live-node-ids ctx)
                                           leader-timeout-ms)
              new-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])]
          (e2e-write-value! (:conns ctx) leader-id "post-failover" "v2")
          (e2e-verify-value-on-nodes! ctx
                                      (:live-node-ids ctx)
                                      "post-failover"
                                      "v2")
          {:control-backend control-backend
           :initial-leader-endpoint leader-endpoint
           :new-leader-endpoint new-endpoint
           :probe-snapshot snapshot})))))

(defn- run-e2e-ha-follower-rejoin!
  [control-backend]
  (with-e2e-ha-cluster
    control-backend
    (fn [ctx-atom]
      (let [ctx @ctx-atom
            leader-timeout-ms (if (= :sofa-jraft control-backend) 20000 10000)
            {:keys [leader-id]}
            (e2e-wait-for-single-leader! (:servers ctx)
                                         (:db-name ctx)
                                         (:live-node-ids ctx)
                                         leader-timeout-ms)
            initial-leader-id leader-id
            initial-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])]
        (e2e-write-value! (:conns ctx) leader-id "seed" "v1")
        (e2e-verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1")
        (swap! ctx-atom e2e-stop-node initial-leader-id)
        (let [ctx @ctx-atom
              {:keys [leader-id]}
              (e2e-wait-for-single-leader! (:servers ctx)
                                           (:db-name ctx)
                                           (:live-node-ids ctx)
                                           leader-timeout-ms)
              failover-leader-id leader-id]
          (e2e-write-value! (:conns ctx) leader-id "post-failover" "v2")
          (e2e-verify-value-on-nodes! ctx
                                      (:live-node-ids ctx)
                                      "post-failover"
                                      "v2")
          (swap! ctx-atom e2e-restart-node initial-leader-id)
          (let [ctx @ctx-atom
                {:keys [leader-id]}
                (e2e-wait-for-single-leader! (:servers ctx)
                                             (:db-name ctx)
                                             (:live-node-ids ctx)
                                             leader-timeout-ms)
                current-leader-id leader-id
                _ (when (= current-leader-id initial-leader-id)
                    (throw (ex-info "Rejoined node unexpectedly became leader"
                                    {:initial-leader-id initial-leader-id
                                     :probe-snapshot
                                     (e2e-probe-snapshot (:servers ctx)
                                                         (:db-name ctx)
                                                         (:live-node-ids ctx))
                                     :node-diagnostics
                                     (e2e-node-diagnostics-by-node
                                      (:servers ctx)
                                      (:db-name ctx)
                                      (:live-node-ids ctx))})))
                catch-up-lsn (e2e-effective-local-lsn ctx current-leader-id)
                rejoin-state
                (e2e-wait-for-follower-rejoin! ctx-atom
                                               initial-leader-id
                                               current-leader-id
                                               catch-up-lsn)]
            (e2e-write-value! (:conns ctx) current-leader-id "post-rejoin" "v3")
            (e2e-verify-value-on-nodes! @ctx-atom
                                        (:live-node-ids @ctx-atom)
                                        "post-rejoin"
                                        "v3")
            (let [target-lsn (e2e-effective-local-lsn @ctx-atom
                                                      current-leader-id)
                  replica-floor
                  (e2e-wait-for-replica-floor! ctx-atom
                                               current-leader-id
                                               initial-leader-id
                                               target-lsn)]
              {:control-backend control-backend
               :initial-leader-id initial-leader-id
               :initial-leader-endpoint initial-endpoint
               :failover-leader-id failover-leader-id
               :current-leader-id current-leader-id
               :current-leader-endpoint
               (get-in @ctx-atom [:nodes (dec current-leader-id) :endpoint])
               :rejoined-node-id initial-leader-id
               :rejoin-state rejoin-state
               :replica-floor replica-floor})))))))

(defn- run-e2e-ha-membership-hash-drift!
  [control-backend]
  (let [old-config log/*config*]
    (log/set-min-level! :fatal)
    (try
      (with-e2e-ha-cluster
        control-backend
        (fn [ctx-atom]
          (let [ctx @ctx-atom
                leader-timeout-ms (if (= :sofa-jraft control-backend) 20000 10000)
                {:keys [leader-id]}
                (e2e-wait-for-single-leader! (:servers ctx)
                                             (:db-name ctx)
                                             (:live-node-ids ctx)
                                             leader-timeout-ms)
                drifted-node-id leader-id
                leader-store (-> (get-in ctx [:conns leader-id]) deref .-store)
                original-members (:ha-members
                                  (e2e-db-state (get-in ctx [:servers leader-id])
                                                (:db-name ctx)))
                drifted-members (e2e-drifted-ha-members original-members
                                                        drifted-node-id)]
            (e2e-write-value! (:conns ctx) leader-id "seed" "v1")
            (e2e-verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1")
            (let [drift-error
                  (try
                    (i/assoc-opt leader-store :ha-members drifted-members)
                    nil
                    (catch Exception e
                      {:message (ex-message e)
                       :data (ex-data e)}))]
              (when-not drift-error
                (throw (ex-info "Membership drift update unexpectedly succeeded"
                                {:drifted-node-id drifted-node-id
                                 :original-members original-members
                                 :drifted-members drifted-members})))
              (i/assoc-opt leader-store :ha-members original-members)
              (let [ctx @ctx-atom
                    {:keys [leader-id snapshot]}
                    (e2e-wait-for-single-leader! (:servers ctx)
                                                 (:db-name ctx)
                                                 (:live-node-ids ctx)
                                                 leader-timeout-ms)]
                (e2e-write-value! (:conns ctx) leader-id "post-reconcile" "v2")
                (e2e-verify-value-on-nodes! ctx
                                            (:live-node-ids ctx)
                                            "post-reconcile"
                                            "v2")
                {:control-backend control-backend
                 :drifted-node-id drifted-node-id
                 :drift-error drift-error
                 :recovered-leader-id leader-id
                 :recovered-probe-snapshot snapshot
                 :recovered-node-diagnostics
                 (e2e-node-diagnostics-by-node (:servers ctx)
                                               (:db-name ctx)
                                               (:live-node-ids ctx))})))))
      (finally
        (log/set-config! old-config)))))

(defn- run-e2e-ha-fencing-hook-verify!
  [control-backend]
  (let [dir (u/tmp-dir (str "ha-e2e-fence-hook-"
                            (UUID/randomUUID)))
        log-file (str dir u/+separator+ "fence.log")
        old-config log/*config*]
    (u/create-dirs dir)
    (log/set-min-level! :fatal)
    (try
      (with-e2e-ha-cluster
        control-backend
        {:base-opts-fn
         (fn [base-opts]
           (assoc base-opts
                  :ha-fencing-hook
                  (e2e-fence-hook-config log-file 0)))}
        (fn [ctx-atom]
          (let [ctx @ctx-atom
                leader-timeout-ms (if (= :sofa-jraft control-backend) 20000 10000)
                {:keys [leader-id]}
                (e2e-wait-for-single-leader! (:servers ctx)
                                             (:db-name ctx)
                                             (:live-node-ids ctx)
                                             leader-timeout-ms)
                initial-leader-id leader-id
                initial-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])]
            (e2e-write-value! (:conns ctx) leader-id "seed" "v1")
            (e2e-verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1")
            (swap! ctx-atom e2e-stop-node initial-leader-id)
            (let [ctx @ctx-atom
                  {:keys [leader-id snapshot]}
                  (e2e-wait-for-single-leader! (:servers ctx)
                                               (:db-name ctx)
                                               (:live-node-ids ctx)
                                               leader-timeout-ms)
                  new-leader-id leader-id
                  new-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])
                  fence-result (e2e-wait-for-fence-entry! ctx-atom
                                                          log-file
                                                          new-leader-id)
                  verified-entry (e2e-verify-fencing-hook-entry
                                  @ctx-atom
                                  initial-leader-id
                                  initial-endpoint
                                  new-leader-id
                                  (first (:matching-entries fence-result)))]
              (e2e-write-value! (:conns ctx) new-leader-id "post-failover" "v2")
              (e2e-verify-value-on-nodes! ctx
                                          (:live-node-ids ctx)
                                          "post-failover"
                                          "v2")
              {:control-backend control-backend
               :initial-leader-id initial-leader-id
               :initial-leader-endpoint initial-endpoint
               :new-leader-id new-leader-id
               :new-leader-endpoint new-endpoint
               :verified-entry verified-entry
               :matching-entries (:matching-entries fence-result)
               :fence-log-entries (:entries fence-result)
               :probe-snapshot snapshot
               :node-diagnostics
               (e2e-node-diagnostics-by-node (:servers ctx)
                                             (:db-name ctx)
                                             (:live-node-ids ctx))}))))
      (finally
        (log/set-config! old-config)
        (safe-delete-dir! dir)))))

(defn- run-e2e-ha-clock-skew-pause!
  [control-backend]
  (let [clock-skew-dir (u/tmp-dir (str "ha-e2e-clock-skew-"
                                       (UUID/randomUUID)))
        old-config log/*config*]
    (doseq [node-id [1 2 3]]
      (e2e-write-clock-skew-ms! clock-skew-dir node-id 0))
    (log/set-min-level! :fatal)
    (try
      (with-e2e-ha-cluster
        control-backend
        {:base-opts-fn
         (fn [base-opts]
           (assoc base-opts
                  :ha-clock-skew-budget-ms 100
                  :ha-clock-skew-hook
                  (e2e-clock-skew-hook-config clock-skew-dir)))}
        (fn [ctx-atom]
          (let [ctx @ctx-atom
                leader-timeout-ms (if (= :sofa-jraft control-backend) 20000 10000)
                {:keys [leader-id]}
                (e2e-wait-for-single-leader! (:servers ctx)
                                             (:db-name ctx)
                                             (:live-node-ids ctx)
                                             leader-timeout-ms)
                initial-leader-id leader-id
                initial-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])
                follower-ids (->> (:live-node-ids ctx)
                                  (remove #{leader-id})
                                  sort
                                  vec)
                resume-node-id (first follower-ids)
                blocked-node-id (last follower-ids)]
            (e2e-write-value! (:conns ctx) leader-id "seed" "v1")
            (e2e-verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1")
            (doseq [node-id follower-ids]
              (e2e-write-clock-skew-ms! clock-skew-dir node-id 250))
            (swap! ctx-atom e2e-stop-node initial-leader-id)
            (let [blocked-result
                  (e2e-wait-for-clock-skew-block! ctx-atom follower-ids)]
              (e2e-write-clock-skew-ms! clock-skew-dir resume-node-id 25)
              (let [ctx @ctx-atom
                    {:keys [leader-id snapshot]}
                    (e2e-wait-for-single-leader! (:servers ctx)
                                                 (:db-name ctx)
                                                 (:live-node-ids ctx)
                                                 leader-timeout-ms)
                    _ (when (not= leader-id resume-node-id)
                        (throw (ex-info "Unexpected leader after clock-skew recovery"
                                        {:expected-leader-id resume-node-id
                                         :leader-id leader-id
                                         :probe-snapshot snapshot
                                         :blocked-result blocked-result})))
                    leader-state (e2e-wait-for-clock-skew-clear! ctx-atom
                                                                 leader-id
                                                                 leader-id
                                                                 :leader)]
                (e2e-write-value! (:conns ctx) leader-id "post-resume" "v2")
                (e2e-verify-value-on-nodes! ctx
                                            (:live-node-ids ctx)
                                            "post-resume"
                                            "v2")
                (e2e-write-clock-skew-ms! clock-skew-dir blocked-node-id 25)
                (let [resumed-follower-state
                      (e2e-wait-for-clock-skew-clear! ctx-atom
                                                      blocked-node-id
                                                      leader-id
                                                      :follower)
                      ctx @ctx-atom]
                  (e2e-write-value! (:conns ctx) leader-id "post-clear" "v3")
                  (e2e-verify-value-on-nodes! ctx
                                              (:live-node-ids ctx)
                                              "post-clear"
                                              "v3")
                  {:control-backend control-backend
                   :initial-leader-id initial-leader-id
                   :initial-leader-endpoint initial-endpoint
                   :paused-node-ids follower-ids
                   :resume-node-id resume-node-id
                   :blocked-node-id blocked-node-id
                   :blocked-result blocked-result
                   :resumed-leader-id leader-id
                   :resumed-leader-endpoint
                   (get-in ctx [:nodes (dec leader-id) :endpoint])
                   :leader-state leader-state
                   :resumed-follower-state resumed-follower-state
                   :probe-snapshot snapshot
                   :node-diagnostics
                   (e2e-node-diagnostics-by-node (:servers ctx)
                                                 (:db-name ctx)
                                                 (:live-node-ids ctx))}))))))
      (finally
        (log/set-config! old-config)
        (safe-delete-dir! clock-skew-dir)))))

(defn- run-e2e-ha-degraded-mode-no-valid-source!
  [control-backend]
  (let [segment-max-ms 100
        write-sleep-ms 150
        replica-floor-ttl-ms 500
        writes-per-batch 4
        old-config log/*config*]
    (log/set-min-level! :fatal)
    (try
      (with-e2e-ha-cluster
        control-backend
        {:base-opts-fn
         (fn [base-opts]
           (assoc base-opts
                  :wal-segment-max-ms segment-max-ms
                  :wal-segment-prealloc? false
                  :wal-segment-prealloc-mode :none
                  :wal-replica-floor-ttl-ms replica-floor-ttl-ms
                  :snapshot-scheduler? false))}
        (fn [ctx-atom]
          (let [ctx @ctx-atom
                leader-timeout-ms (if (= :sofa-jraft control-backend) 20000 10000)
                {:keys [leader-id]}
                (e2e-wait-for-single-leader! (:servers ctx)
                                             (:db-name ctx)
                                             (:live-node-ids ctx)
                                             leader-timeout-ms)
                initial-leader-id leader-id
                initial-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])
                degraded-node-id (last (remove #{leader-id}
                                               (sort (:live-node-ids ctx))))
                source-node-ids (->> (:live-node-ids ctx)
                                     (remove #{degraded-node-id})
                                     sort
                                     vec)]
            (e2e-write-value! (:conns ctx) leader-id "seed" "v1")
            (e2e-verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1")
            (let [baseline-lsn (e2e-write-series-with-rolls!
                                ctx
                                leader-id
                                "baseline"
                                writes-per-batch
                                write-sleep-ms)
                  _ (e2e-create-snapshots-on-nodes! ctx source-node-ids)
                  _ (swap! ctx-atom e2e-stop-node degraded-node-id)
                  ctx @ctx-atom
                  batch-2-lsn (e2e-write-series-with-rolls!
                               ctx
                               leader-id
                               "gap-batch-1"
                               writes-per-batch
                               write-sleep-ms)
                  _ (e2e-create-snapshots-on-nodes! ctx source-node-ids)
                  batch-3-lsn (e2e-write-series-with-rolls!
                               ctx
                               leader-id
                               "gap-batch-2"
                               writes-per-batch
                               write-sleep-ms)
                  _ (e2e-create-snapshots-on-nodes! ctx source-node-ids)
                  follower-next-lsn (unchecked-inc (long baseline-lsn))
                  _ (doseq [node-id source-node-ids
                            :let [gc-result
                                  (e2e-gc-txlog-segments-on-node!
                                   @ctx-atom node-id)
                                  min-retained-lsn
                                  (long (or (get-in gc-result
                                                    [:after
                                                     :min-retained-lsn])
                                            0))]]
                      (when (zero? (:deleted-count gc-result))
                        (throw (ex-info "Degraded-mode e2e did not delete WAL segments"
                                        {:node-id node-id
                                         :gc-result gc-result})))
                      (when (<= min-retained-lsn
                                (long follower-next-lsn))
                        (throw (ex-info "Degraded-mode e2e did not create a real WAL gap"
                                        {:node-id node-id
                                         :follower-next-lsn
                                         follower-next-lsn
                                         :gc-result gc-result}))))
                  fault-error (fn [endpoint]
                                (ex-info "forced snapshot source failure"
                                         {:error :ha/follower-snapshot-unavailable
                                          :endpoint endpoint}))
                  injected-result
                  (with-redefs-fn
                    {#'dha/fetch-ha-endpoint-snapshot-copy!
                     (fn [_ _ endpoint _]
                       (throw (fault-error endpoint)))}
                    (fn []
                      (swap! ctx-atom e2e-restart-node degraded-node-id)
                      (let [degraded-state
                            (e2e-wait-for-follower-degraded! ctx-atom
                                                             degraded-node-id)]
                        (let [ctx @ctx-atom
                              _ (e2e-write-value! (:conns ctx)
                                                  initial-leader-id
                                                  "post-gap-live"
                                                  "v-live")
                              _ (e2e-verify-value-on-nodes! ctx
                                                            source-node-ids
                                                            "post-gap-live"
                                                            "v-live")
                              blocked-state
                              (e2e-wait-for-follower-stays-degraded!
                               ctx-atom
                               degraded-node-id
                               initial-leader-id)]
                          {:post-gap-live-lsn
                           (e2e-effective-local-lsn ctx initial-leader-id)
                           :degraded-state degraded-state
                           :blocked-state blocked-state}))))
                  recovery-state (e2e-wait-for-follower-bootstrap!
                                  ctx-atom
                                  degraded-node-id
                                  (:post-gap-live-lsn injected-result))
                  ctx @ctx-atom
                  {:keys [leader-id]}
                  (e2e-wait-for-single-leader! (:servers ctx)
                                               (:db-name ctx)
                                               (:live-node-ids ctx)
                                               leader-timeout-ms)]
              (e2e-write-value! (:conns ctx)
                                leader-id
                                "post-recovery"
                                "v-final")
              (e2e-verify-value-on-nodes! ctx
                                          (:live-node-ids ctx)
                                          "post-recovery"
                                          "v-final")
              {:control-backend control-backend
               :initial-leader-id initial-leader-id
               :initial-leader-endpoint initial-endpoint
               :degraded-node-id degraded-node-id
               :follower-next-lsn follower-next-lsn
               :mid-gap-lsn batch-2-lsn
               :pre-recovery-leader-lsn batch-3-lsn
               :degraded-state (:degraded-state injected-result)
               :blocked-state (:blocked-state injected-result)
               :recovered-state recovery-state
               :node-diagnostics
               (e2e-node-diagnostics-by-node (:servers ctx)
                                             (:db-name ctx)
                                             (:live-node-ids ctx))}))))
      (finally
        (log/set-config! old-config)))))

(deftest ha-e2e-in-memory-three-node-failover-test
  (let [result (run-e2e-ha-failover! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (string? (:initial-leader-endpoint result)))
    (is (string? (:new-leader-endpoint result)))
    (is (not= (:initial-leader-endpoint result)
              (:new-leader-endpoint result)))))

(deftest ha-e2e-sofa-jraft-three-node-failover-characterization-test
  (when (= "1" (System/getenv "DTLV_RUN_HA_E2E_SOFA_JRAFT"))
    (let [result (run-e2e-ha-failover! :sofa-jraft)]
      (is (= :sofa-jraft (:control-backend result)))
      (is (string? (:initial-leader-endpoint result)))
      (is (string? (:new-leader-endpoint result)))
      (is (not= (:initial-leader-endpoint result)
                (:new-leader-endpoint result))))))

(deftest ha-e2e-in-memory-three-node-follower-rejoin-test
  (let [result (run-e2e-ha-follower-rejoin! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (= (:initial-leader-id result)
           (:rejoined-node-id result)))
    (is (not= (:current-leader-id result)
              (:rejoined-node-id result)))
    (is (= :follower (get-in result [:rejoin-state :ha-role])))
    (is (= (:current-leader-id result)
           (get-in result [:rejoin-state :ha-authority-owner-node-id])))
    (is (integer? (get-in result [:rejoin-state :ha-authority-term])))
    (is (>= (long (or (get-in result [:replica-floor :replica :floor-lsn]) 0))
            (long (or (get-in result
                              [:rejoin-state :last-applied-lsn])
                      0))))))

(deftest ha-e2e-in-memory-three-node-membership-hash-drift-recovery-test
  (let [result (run-e2e-ha-membership-hash-drift! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (integer? (:drifted-node-id result)))
    (is (= :ha/membership-hash-mismatch
           (get-in result [:drift-error :data :err-data :error])))
    (is (integer? (:recovered-leader-id result)))
    (is (true? (get-in result
                       [:recovered-node-diagnostics
                        (:drifted-node-id result)
                        :ha-authority-diagnostics
                        :running?])))))

(deftest ha-e2e-in-memory-three-node-fencing-hook-verify-test
  (let [result (run-e2e-ha-fencing-hook-verify! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (string? (:initial-leader-endpoint result)))
    (is (string? (:new-leader-endpoint result)))
    (is (not= (:initial-leader-id result)
              (:new-leader-id result)))
    (is (= (str (:initial-leader-id result))
           (get-in result [:verified-entry :old-node-id])))
    (is (= (:initial-leader-endpoint result)
           (get-in result [:verified-entry :old-leader-endpoint])))
    (is (= (str (:new-leader-id result))
           (get-in result [:verified-entry :new-node-id])))
    (is (= (str "ha-e2e:"
                (get-in result [:verified-entry :observed-term])
                ":"
                (:new-leader-id result))
           (get-in result [:verified-entry :fence-op-id])))))

(deftest ha-e2e-in-memory-three-node-clock-skew-pause-test
  (let [result (run-e2e-ha-clock-skew-pause! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (= 2 (count (:paused-node-ids result))))
    (is (= (:resume-node-id result)
           (:resumed-leader-id result)))
    (is (not= (:resume-node-id result)
              (:blocked-node-id result)))
    (is (every? true?
                (map #(true? (get-in result
                                     [:blocked-result :paused-states %
                                      :ha-clock-skew-paused?]))
                     (:paused-node-ids result))))
    (is (every? #(= :clock-skew-budget-breached
                    (get-in result
                            [:blocked-result :paused-states %
                             :ha-promotion-failure-details :reason]))
                (:paused-node-ids result)))
    (is (= :leader (get-in result [:leader-state :ha-role])))
    (is (false? (get-in result [:leader-state :ha-clock-skew-paused?])))
    (is (= :follower
           (get-in result [:resumed-follower-state :ha-role])))
    (is (false? (get-in result
                        [:resumed-follower-state :ha-clock-skew-paused?])))))

(deftest ha-e2e-in-memory-three-node-degraded-mode-no-valid-source-test
  (let [result (run-e2e-ha-degraded-mode-no-valid-source! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (integer? (:degraded-node-id result)))
    (is (true? (get-in result [:degraded-state :ha-follower-degraded?])))
    (is (= :wal-gap
           (get-in result [:degraded-state :ha-follower-degraded-reason])))
    (is (true? (get-in result [:blocked-state :ha-follower-degraded?])))
    (is (= (:initial-leader-id result)
           (get-in result [:blocked-state :ha-authority-owner-node-id])))
    (is (nil? (get-in result [:recovered-state :ha-follower-degraded?])))
    (is (nil? (get-in result [:recovered-state :ha-follower-last-error])))))

(deftest ha-e2e-sofa-jraft-three-node-follower-rejoin-characterization-test
  (when (= "1" (System/getenv "DTLV_RUN_HA_E2E_SOFA_JRAFT"))
    (let [result (run-e2e-ha-follower-rejoin! :sofa-jraft)]
      (is (= :sofa-jraft (:control-backend result)))
      (is (= (:initial-leader-id result)
             (:rejoined-node-id result)))
      (is (not= (:current-leader-id result)
                (:rejoined-node-id result)))
      (is (= :follower (get-in result [:rejoin-state :ha-role]))))))

(deftest start-ha-authority-initializes-membership-hash-test
  (let [opts (valid-ha-opts)
        expected (vld/derive-ha-membership-hash opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        authority (:ha-authority runtime)]
    (is (= expected (:ha-membership-hash runtime)))
    (is (= expected (ha/read-membership-hash authority)))
    (#'srv/stop-ha-authority "orders" runtime)))

(deftest stop-ha-authority-stops-lifecycle-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        authority (:ha-authority runtime)]
    (#'srv/stop-ha-authority "orders" runtime)
    (is (thrown? clojure.lang.ExceptionInfo
                 (ha/read-membership-hash authority)))))

(deftest start-ha-authority-supports-sofa-jraft-adapter-test
  (let [opts (-> (valid-ha-opts)
                 (assoc-in [:ha-control-plane :backend] :sofa-jraft)
                      ;; Single-voter smoke config for local JRaft startup.
                 (assoc-in [:ha-control-plane :voters]
                           [{:peer-id "10.0.0.12:7801"
                             :ha-node-id 2
                             :promotable? true}]))
        runtime (#'srv/start-ha-authority "orders" opts)
        authority (:ha-authority runtime)]
    (is (some? authority))
    (is (string? (ha/read-membership-hash authority)))
    (#'srv/stop-ha-authority "orders" runtime)))

(deftest start-ha-authority-derives-raft-dir-from-server-root-test
  (let [root "/var/lib/datalevin-test"
        opts (-> (valid-ha-opts "ha/prod")
                 (assoc-in [:ha-control-plane :backend] :sofa-jraft))
        resolved (#'srv/with-default-ha-control-raft-dir root "orders" opts)]
    (is (= (str root
                u/+separator+
                "ha-control"
                u/+separator+
                "ha_prod"
                u/+separator+
                "10.0.0.12_7801"
                u/+separator+
                (u/hexify-string "orders"))
           (get-in resolved [:ha-control-plane :raft-dir])))))

(deftest start-ha-authority-keeps-explicit-raft-dir-test
  (let [root "/var/lib/datalevin-test"
        explicit "/srv/dtlv/raft/custom-dir"
        opts (-> (valid-ha-opts "ha/prod")
                 (assoc-in [:ha-control-plane :backend] :sofa-jraft)
                 (assoc-in [:ha-control-plane :raft-dir] explicit))
        resolved (#'srv/with-default-ha-control-raft-dir root "orders" opts)]
    (is (= explicit
           (get-in resolved [:ha-control-plane :raft-dir])))))

(deftest start-ha-authority-fails-closed-on-membership-mismatch-test
  (let [group-id (str "ha-test-" (UUID/randomUUID))
        opts-a (valid-ha-opts group-id)
        opts-b (assoc-in (valid-ha-opts group-id)
                         [:ha-members 2 :endpoint]
                         "10.0.0.99:8898")
        runtime-a (#'srv/start-ha-authority "orders" opts-a)]
    (try
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"membership hash mismatch"
           (#'srv/start-ha-authority "orders" opts-b)))
      (is (string? (ha/read-membership-hash (:ha-authority runtime-a))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime-a)))))

(deftest startup-read-ha-authority-state-timeout-falls-back-test
  (let [authority (reify ha/ILeaseAuthority
                    (start-authority! [this] this)
                    (stop-authority! [this] this)
                    (read-lease [_ _]
                      (throw (ex-info "warming up"
                                      {:error :ha/control-timeout})))
                    (try-acquire-lease [_ _]
                      (throw (ex-info "unsupported" {})))
                    (renew-lease [_ _]
                      (throw (ex-info "unsupported" {})))
                    (read-membership-hash [_]
                      "unused")
                    (init-membership-hash! [_ membership-hash]
                      {:ok? true
                       :membership-hash membership-hash})
                    (read-voters [_] [])
                    (replace-voters! [_ _]
                      (throw (ex-info "unsupported" {}))))
        result (#'dha/startup-read-ha-authority-state
                "orders" authority "db-a")]
    (is (false? (:ok? result)))
    (is (nil? (:lease result)))
    (is (= :startup-authority-read-failed
           (get-in result [:error :reason])))
    (is (= :ha/control-timeout
           (get-in result [:error :data :error])))))

(deftest start-ha-authority-rejoins-local-authority-owner-as-follower-test
  (let [group-id (str "ha-test-" (UUID/randomUUID))
        opts (valid-ha-opts group-id)
        runtime-a (#'srv/start-ha-authority "orders" opts)
        runtime-b (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [acquire (ha/try-acquire-lease
                     (:ha-authority runtime-a)
                     {:db-identity (:ha-db-identity runtime-a)
                      :leader-node-id (:ha-node-id runtime-a)
                      :leader-endpoint (:ha-local-endpoint runtime-a)
                      :lease-renew-ms (:ha-lease-renew-ms runtime-a)
                      :lease-timeout-ms (:ha-lease-timeout-ms runtime-a)
                      :leader-last-applied-lsn 1
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})]
        (is (:ok? acquire))
        (#'srv/stop-ha-authority "orders" runtime-a)
        (reset! runtime-b (#'srv/start-ha-authority "orders" opts))
        (is (= :follower (:ha-role @runtime-b)))
        (is (nil? (:ha-leader-term @runtime-b)))
        (is (= (:ha-node-id opts)
               (:ha-authority-owner-node-id @runtime-b)))
        (is (= (:term acquire)
               (:ha-authority-term @runtime-b))))
      (finally
        (when-let [runtime-b' @runtime-b]
          (#'srv/stop-ha-authority "orders" runtime-b'))
        (#'srv/stop-ha-authority "orders" runtime-a)))))

(deftest ha-renew-step-keeps-leader-on-success-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)]
    (try
      (let [acquire (ha/try-acquire-lease
                     (:ha-authority runtime)
                     {:db-identity (:ha-db-identity runtime)
                      :leader-node-id (:ha-node-id runtime)
                      :leader-endpoint (:ha-local-endpoint runtime)
                      :lease-renew-ms (:ha-lease-renew-ms runtime)
                      :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                      :leader-last-applied-lsn 1
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})
            leader-runtime (-> runtime
                               (assoc :ha-role :leader
                                      :ha-leader-term (:term acquire)
                                      :ha-last-authority-refresh-ms now-ms))
            next-state (#'srv/ha-renew-step "orders" leader-runtime)]
        (is (:ok? acquire))
        (is (= :leader (:ha-role next-state)))
        (is (= (:term acquire) (:ha-authority-term next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-demotes-on-renew-term-mismatch-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)]
    (try
      (let [acquire (ha/try-acquire-lease
                     (:ha-authority runtime)
                     {:db-identity (:ha-db-identity runtime)
                      :leader-node-id (:ha-node-id runtime)
                      :leader-endpoint (:ha-local-endpoint runtime)
                      :lease-renew-ms (:ha-lease-renew-ms runtime)
                      :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                      :leader-last-applied-lsn 1
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})
            leader-runtime (-> runtime
                               (assoc :ha-role :leader
                                      :ha-leader-term (unchecked-inc (long (:term acquire)))
                                      :ha-last-authority-refresh-ms now-ms))
            demoting-state (#'srv/ha-renew-step "orders" leader-runtime)
            follower-state (#'srv/ha-renew-step "orders"
                                                (assoc demoting-state
                                                       :ha-demoted-at-ms
                                                       (dec (System/currentTimeMillis))))]
        (is (:ok? acquire))
        (is (= :demoting (:ha-role demoting-state)))
        (is (= :renew-failed (:ha-demotion-reason demoting-state)))
        (is (nil? (:ha-leader-term demoting-state)))
        (is (= :follower (:ha-role follower-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-demotes-on-membership-hash-mismatch-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)]
    (try
      (let [acquire (ha/try-acquire-lease
                     (:ha-authority runtime)
                     {:db-identity (:ha-db-identity runtime)
                      :leader-node-id (:ha-node-id runtime)
                      :leader-endpoint (:ha-local-endpoint runtime)
                      :lease-renew-ms (:ha-lease-renew-ms runtime)
                      :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                      :leader-last-applied-lsn 1
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})
            leader-runtime (-> runtime
                               (assoc :ha-role :leader
                                      :ha-membership-hash "bogus-hash"
                                      :ha-leader-term (:term acquire)
                                      :ha-last-authority-refresh-ms now-ms))
            next-state (#'srv/ha-renew-step "orders" leader-runtime)]
        (is (:ok? acquire))
        (is (= :demoting (:ha-role next-state)))
        (is (= :membership-hash-mismatch (:ha-demotion-reason next-state)))
        (is (true? (:ha-membership-mismatch? next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-promotes-follower-from-empty-lease-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-local-last-applied-lsn 0))
            next-state (#'srv/ha-renew-step "orders" follower-runtime)]
        (is (= :leader (:ha-role next-state)))
        (is (= 1 (:ha-leader-term next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-lag-guard-blocks-promotion-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 10
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn {#'dha/run-ha-fencing-hook
                                        (fn [_ _ _]
                                          (throw (ex-info "must-not-call-fencing" {})))}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :follower (:ha-role next-state)))
        (is (= :lag-guard-failed (:ha-promotion-last-failure next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-fencing-failure-blocks-promotion-test
  (let [opts (assoc (valid-ha-opts)
                    :ha-fencing-hook {:cmd ["/bin/sh" "-c" "exit 7"]
                                      :timeout-ms 1000
                                      :retries 0
                                      :retry-delay-ms 0})
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 0
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            next-state (#'srv/ha-renew-step "orders" follower-runtime)]
        (is (= :follower (:ha-role next-state)))
        (is (= :fencing-failed (:ha-promotion-last-failure next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-run-fencing-hook-retries-reuse-fence-op-id-test
  (let [dir (u/tmp-dir (str "ha-fence-hook-" (UUID/randomUUID)))
        log-file (str dir u/+separator+ "fence.log")
        cmd ["/bin/sh" "-c"
             (str "printf '%s\\n' "
                  "\"$DTLV_FENCE_OP_ID,$DTLV_TERM_CANDIDATE,$DTLV_TERM_OBSERVED\" "
                  ">> \"$1\"; "
                  "n=$(wc -l < \"$1\"); "
                  "if [ \"$n\" -lt 3 ]; then exit 7; else exit 0; fi")
             "fence-hook"
             log-file]]
    (try
      (u/create-dirs dir)
      (let [result (#'dha/run-ha-fencing-hook
                    "orders"
                    {:ha-node-id 2
                     :ha-fencing-hook {:cmd cmd
                                       :timeout-ms 1000
                                       :retries 2
                                       :retry-delay-ms 0}}
                    {:leader-node-id 1
                     :leader-endpoint "10.0.0.11:8898"
                     :term 4})
            calls (->> (slurp log-file)
                       s/split-lines)]
        (is (:ok? result))
        (is (= 3 (:attempt result)))
        (is (= 1
               (count (distinct (map #(first (s/split % #","))
                                     calls)))))
        (is (= #{"5"}
               (set (map #(second (s/split % #",")) calls))))
        (is (= #{"4"}
               (set (map #(nth (s/split % #",") 2) calls)))))
      (finally
        (try
          (u/delete-files dir)
          (catch Exception _))))))

(deftest ha-renew-step-follower-control-read-failure-blocks-promotion-test
  (let [authority (reify ha/ILeaseAuthority
                    (start-authority! [this] this)
                    (stop-authority! [this] this)
                    (read-lease [_ _]
                      (throw (ex-info "quorum lost"
                                      {:error :ha/control-timeout})))
                    (try-acquire-lease [_ _]
                      (throw (ex-info "must-not-acquire" {})))
                    (renew-lease [_ _]
                      (throw (ex-info "must-not-renew" {})))
                    (read-membership-hash [_]
                      (throw (ex-info "must-not-read-membership" {})))
                    (init-membership-hash! [_ _]
                      (throw (ex-info "unsupported" {})))
                    (read-voters [_] [])
                    (replace-voters! [_ _]
                      (throw (ex-info "unsupported" {}))))
        follower-runtime
        {:ha-authority authority
         :ha-db-identity "db-a"
         :ha-role :follower
         :ha-node-id 2
         :ha-local-endpoint "10.0.0.12:8898"
         :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                      {:node-id 2 :endpoint "10.0.0.12:8898"}]
         :ha-membership-hash "hash-a"
         :ha-authority-membership-hash "hash-a"
         :ha-lease-renew-ms 5000
         :ha-lease-timeout-ms 15000
         :ha-promotion-base-delay-ms 0
         :ha-promotion-rank-delay-ms 0
         :ha-max-promotion-lag-lsn 0
         :ha-local-last-applied-lsn 0}
        next-state (with-redefs-fn
                     {#'dha/run-ha-fencing-hook
                      (fn [_ _ _]
                        (throw (ex-info "must-not-call-fencing" {})))}
                     (fn []
                       (#'srv/ha-renew-step "orders" follower-runtime)))]
    (is (= :follower (:ha-role next-state)))
    (is (false? (:ha-authority-read-ok? next-state)))
    (is (= :authority-read-failed
           (:ha-promotion-last-failure next-state)))
    (is (= :authority-read-failed
           (get-in next-state
                   [:ha-promotion-failure-details :reason])))
    (is (= :ha/control-timeout
           (get-in next-state
                   [:ha-promotion-failure-details :data :error])))))

(deftest ha-renew-step-candidate-clock-skew-pause-resumes-after-recovery-test
  (let [opts (assoc (valid-ha-opts)
                    :ha-clock-skew-hook {:cmd ["/bin/sh" "-c" "printf 0"]
                                         :timeout-ms 1000
                                         :retries 0
                                         :retry-delay-ms 0})
        runtime (#'srv/start-ha-authority "orders" opts)
        hook-results (atom [{:ok? true
                             :paused? true
                             :reason :clock-skew-budget-breached
                             :budget-ms 100
                             :clock-skew-ms 125}
                            {:ok? true
                             :paused? false
                             :reason :clock-skew-within-budget
                             :budget-ms 100
                             :clock-skew-ms 25}])]
    (try
      (let [follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            [paused-state resumed-state]
            (with-redefs-fn
              {#'dha/run-ha-clock-skew-hook
               (fn [_ _]
                 (let [result (first @hook-results)]
                   (swap! hook-results #(vec (rest %)))
                   result))
               #'dha/run-ha-fencing-hook
               (fn [_ _ _]
                 {:ok? true})}
              (fn []
                (let [state-1 (#'srv/ha-renew-step "orders" follower-runtime)
                      state-2 (#'srv/ha-renew-step "orders" state-1)]
                  [state-1 state-2])))]
        (is (= :follower (:ha-role paused-state)))
        (is (true? (:ha-clock-skew-paused? paused-state)))
        (is (= :clock-skew-paused
               (:ha-promotion-last-failure paused-state)))
        (is (= :clock-skew-budget-breached
               (get-in paused-state
                       [:ha-promotion-failure-details :reason])))
        (is (= 125
               (get-in paused-state
                       [:ha-promotion-failure-details :clock-skew-ms])))
        (is (= :leader (:ha-role resumed-state)))
        (is (false? (:ha-clock-skew-paused? resumed-state)))
        (is (= 25 (:ha-clock-skew-last-observed-ms resumed-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-pre-cas-lag-uses-reachable-watermark-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 5
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 8))
            next-state (with-redefs-fn {#'dha/run-ha-fencing-hook
                                        (fn [_ _ _] {:ok? true})
                                        #'dha/fetch-leader-watermark-lsn
                                        (fn [_ _ _]
                                          {:reachable? true
                                           :last-applied-lsn 10
                                           :source :test})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :follower (:ha-role next-state)))
        (is (= :lag-guard-failed (:ha-promotion-last-failure next-state)))
        (is (= :pre-cas
               (get-in next-state [:ha-promotion-failure-details :phase])))
        (is (= 10
               (get-in next-state
                       [:ha-promotion-failure-details
                        :leader-lag-input
                        :leader-watermark-last-applied-lsn]))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-unreachable-leader-waits-before-cas-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        wait-calls (atom 0)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 0
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn {#'dha/run-ha-fencing-hook
                                        (fn [_ _ _] {:ok? true})
                                        #'dha/fetch-leader-watermark-lsn
                                        (fn [_ _ _]
                                          {:reachable? false
                                           :reason :test-unreachable})
                                        #'dha/maybe-wait-unreachable-leader-before-pre-cas!
                                        (fn [_ _]
                                          (swap! wait-calls inc)
                                          {:slept-ms 0
                                           :wait-until-ms 0})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= 1 @wait-calls))
        (is (= :leader (:ha-role next-state)))
        (is (= 0 (:ha-promotion-wait-before-cas-ms next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-loop-sleep-ms-respects-candidate-and-follower-deadlines-test
  (let [now-ms 1000]
    (is (= 300
           (#'srv/ha-loop-sleep-ms
            {:ha-role :candidate
             :ha-lease-renew-ms 5000
             :ha-candidate-since-ms now-ms
             :ha-candidate-delay-ms 300}
            now-ms)))
    (is (= 250
           (#'srv/ha-loop-sleep-ms
            {:ha-role :follower
             :ha-lease-renew-ms 5000
             :ha-follower-next-sync-not-before-ms (+ now-ms 250)}
            now-ms)))
    (is (= 5000
           (#'srv/ha-loop-sleep-ms
            {:ha-role :leader
             :ha-lease-renew-ms 5000}
            now-ms)))))

(deftest ha-renew-step-follower-syncs-txlog-and-updates-replica-floor-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-lsn (atom 0)
        applied (atom [])
        reported (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            leader-endpoint "10.0.0.11:8898"
            acquire (ha/try-acquire-lease
                     authority
                     {:db-identity (:ha-db-identity runtime)
                      :leader-node-id 1
                      :leader-endpoint leader-endpoint
                      :lease-renew-ms (:ha-lease-renew-ms runtime)
                      :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                      :leader-last-applied-lsn 2
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ _ from-lsn _]
                            (is (= 1 from-lsn))
                            [{:lsn 1 :ops [[:put "a" "k1" "v1"]]}
                             {:lsn 2 :ops [[:put "a" "k2" "v2"]]}])
                          #'dha/apply-ha-follower-txlog-record!
                          (fn [_ record]
                            (swap! applied conj (:lsn record))
                            (reset! local-lsn (long (:lsn record))))
                          #'dha/read-ha-local-last-applied-lsn
                          (fn [_] @local-lsn)
                          #'dha/report-ha-replica-floor!
                          (fn [db-name m endpoint applied-lsn]
                            (reset! reported
                                    {:db-name db-name
                                     :ha-node-id (:ha-node-id m)
                                     :leader-endpoint endpoint
                                     :applied-lsn applied-lsn})
                            {:ok? true})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (:ok? acquire))
        (is (= [1 2] @applied))
        (is (= :follower (:ha-role next-state)))
        (is (= 2 (:ha-local-last-applied-lsn next-state)))
        (is (= 3 (:ha-follower-next-lsn next-state)))
        (is (= 2 (:ha-follower-last-batch-size next-state)))
        (is (nil? (:ha-follower-sync-backoff-ms next-state)))
        (is (nil? (:ha-follower-next-sync-not-before-ms next-state)))
        (is (nil? (:ha-follower-degraded? next-state)))
        (is (nil? (:ha-follower-last-error next-state)))
        (is (= {:db-name "orders"
                :ha-node-id 2
                :leader-endpoint leader-endpoint
                :applied-lsn 2}
               @reported)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-replica-floor-transport-failure-classification-test
  (is (true? (#'dha/ha-replica-floor-transport-failure?
              (ex-info "Error sending message and receiving response: #error {:cause \"Socket channel is closed.\"}"
                       {:msg {:type :authentication}}))))
  (is (true? (#'dha/ha-replica-floor-transport-failure?
              (ex-info "Error sending message and receiving response"
                       {:msg {:type :authentication}}
                       (ex-info "Socket channel is closed." {})))))
  (is (true? (#'dha/ha-replica-floor-transport-failure?
              (ex-info "Unable to connect to server"
                       {:host "127.0.0.1" :port 8898}
                       (ConnectException. "Connection refused")))))
  (is (false? (#'dha/ha-replica-floor-transport-failure?
               (ex-info "replica floor update failed"
                        {:db-name "orders"}
                        (RuntimeException. "boom"))))))

(deftest server-client-disconnect-classification-test
  (is (true? (#'srv/client-disconnect?
              (ClosedChannelException.))))
  (is (true? (#'srv/client-disconnect?
              (ex-info "Error sending message and receiving response"
                       {}
                       (ex-info "Socket channel is closed." {})))))
  (is (false? (#'srv/client-disconnect?
               (ex-info "authentication failed" {}
                        (RuntimeException. "boom"))))))

(deftest ha-follower-replica-floor-keeps-leader-retention-safe-while-fresh-test
  (let [leader-dir (u/tmp-dir (str "ha-leader-retention-"
                                   (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-retention-"
                                     (UUID/randomUUID)))
        leader-endpoint "10.0.0.11:8898"
        leader-store (st/open leader-dir nil
                              {:db-name "orders"
                               :db-identity "db-retention"
                               :wal? true
                               :wal-replica-floor-ttl-ms 50})
        follower-store (st/open follower-dir nil
                                {:db-name "orders"
                                 :db-identity "db-retention"
                                 :wal? true})
        leader-kv (.-lmdb leader-store)
        follower-kv (.-lmdb follower-store)
        leader-db (kv/wrap-lmdb leader-kv)
        follower-runtime {:store follower-store
                          :ha-node-id 2
                          :ha-local-endpoint "10.0.0.12:8898"
                          :ha-lease-renew-ms 1000}
        lease {:leader-node-id 1
               :leader-endpoint leader-endpoint
               :leader-last-applied-lsn 2}
        now-ms (System/currentTimeMillis)]
    (try
      (i/open-dbi leader-kv "a")
      (i/open-dbi follower-kv "a")
      (i/transact-kv leader-kv
                     [[:put "a" "k1" "v1"]
                      [:put "a" "k2" "v2"]])
      (with-redefs [dha/fetch-ha-leader-txlog-batch
                    (fn [_ _ endpoint from-lsn upto-lsn]
                      (is (= leader-endpoint endpoint))
                      (kv/open-tx-log leader-db from-lsn upto-lsn))
                    dha/fetch-ha-endpoint-watermark-lsn
                    (fn [_ _ endpoint]
                      (is (= leader-endpoint endpoint))
                      {:reachable? true
                       :last-applied-lsn
                       (:last-applied-lsn (kv/txlog-watermarks leader-db))})
                    dha/report-ha-replica-floor!
                    (fn [_ m endpoint applied-lsn]
                      (is (= leader-endpoint endpoint))
                      (kv/txlog-update-replica-floor! leader-db
                                                      (:ha-node-id m)
                                                      applied-lsn))]
        (let [sync-1 (#'dha/sync-ha-follower-batch
                      "orders" follower-runtime lease 1 now-ms)
              state-1 (:state sync-1)
              synced-lsn (:applied-lsn sync-1)]
          (is (seq (:records sync-1)))
          (is (= synced-lsn
                 (:ha-local-last-applied-lsn state-1)))
          (is (= (inc synced-lsn)
                 (:ha-follower-next-lsn state-1)))
          (i/transact-kv leader-kv
                         [[:put "a" "k3" "v3"]
                          [:put "a" "k4" "v4"]])
          (let [retention-1 (kv/txlog-retention-state leader-db)]
            (is (= synced-lsn
                   (get-in retention-1 [:floors :replica-floor-lsn])))
            (is (= synced-lsn
                   (:required-retained-floor-lsn retention-1)))
            (is (contains? (set (:floor-limiters retention-1))
                           :replica-floor-lsn)))
          (Thread/sleep 150)
          (let [retention-stale (kv/txlog-retention-state leader-db)
                _ (kv/txlog-update-replica-floor! leader-db 2 synced-lsn)
                retention-2 (kv/txlog-retention-state leader-db)]
            (is (zero? (get-in retention-stale
                               [:floor-providers :replica :active-count])))
            (is (= (:required-retained-floor-lsn retention-stale)
                   (get-in retention-stale
                           [:floors :snapshot-floor-lsn])))
            (is (not-any? #{:replica-floor-lsn}
                          (:floor-limiters retention-stale)))
            (is (= synced-lsn
                   (:required-retained-floor-lsn retention-2)))
            (is (contains? (set (:floor-limiters retention-2))
                           :replica-floor-lsn)))))
      (finally
        (when-not (i/closed? follower-store)
          (i/close follower-store))
        (when-not (i/closed? leader-store)
          (i/close leader-store))
        (u/delete-files follower-dir)
        (u/delete-files leader-dir)))))

(deftest ha-apply-follower-txlog-record-replays-real-datalog-rows-test
  (let [schema {:drill/key {:db/valueType :db.type/string
                            :db/unique :db.unique/identity}
                :drill/value {:db/valueType :db.type/string}}
        value-query '[:find ?v .
                      :in $ ?k
                      :where
                      [?e :drill/key ?k]
                      [?e :drill/value ?v]]
        leader-dir (u/tmp-dir (str "ha-leader-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}
        leader-conn (d/get-conn leader-dir schema opts)]
    (try
      (d/transact! leader-conn [{:drill/key "seed" :drill/value "v1"}])
      (let [leader-store (st/open leader-dir nil opts)
            follower-store (st/open follower-dir schema opts)]
        (try
          (let [record (->> (kv/open-tx-log-rows (.-lmdb leader-store) 1 64)
                            ;; :tx-kind only distinguishes vector checkpoints from
                            ;; everything else, so select the actual datalog tx.
                            (filter (fn [record]
                                      (some (fn [[op _dbi k]]
                                              (and (= op :put)
                                                   (= k :max-tx)))
                                            (:rows record))))
                            last)]
            (is record)
            (is (seq (:rows record)))
            (#'dha/apply-ha-follower-txlog-record! {:store follower-store}
                                                   record)
            (is (= "v1"
                   (d/q value-query
                        (db/new-db follower-store)
                        "seed"))))
          (finally
            (i/close follower-store)
            (i/close leader-store))))
      (finally
        (d/close leader-conn)
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest ha-renew-step-follower-sync-detects-lsn-gap-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        applied (atom [])
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 3
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ _ _ _]
                            [{:lsn 3 :ops [[:put "a" "k3" "v3"]]}])
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.13:8898"
                              {:reachable? true :last-applied-lsn 3}
                              (throw (ex-info "unexpected-endpoint"
                                              {:endpoint endpoint}))))
                          #'dha/fetch-ha-endpoint-snapshot-copy!
                          (fn [_ _ endpoint _]
                            (throw (ex-info "snapshot unavailable"
                                            {:error :ha/follower-snapshot-unavailable
                                             :endpoint endpoint})))
                          #'dha/apply-ha-follower-txlog-record!
                          (fn [_ record]
                            (swap! applied conj record))
                          #'dha/read-ha-local-last-applied-lsn
                          (fn [_] 0)}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (empty? @applied))
        (is (= :follower (:ha-role next-state)))
        (is (= :sync-failed (:ha-follower-last-error next-state)))
        (is (true? (:ha-follower-degraded? next-state)))
        (is (= :wal-gap (:ha-follower-degraded-reason next-state)))
        (is (= :ha/follower-snapshot-bootstrap-failed
               (get-in next-state
                       [:ha-follower-last-error-details :data :error])))
        (is (= :ha/txlog-gap-unresolved
               (get-in next-state
                       [:ha-follower-last-error-details
                        :data
                        :gap-error
                        :data
                        :error])))
        (is (= #{"10.0.0.11:8898" "10.0.0.13:8898"}
               (into #{}
                     (map :source-endpoint)
                     (get-in next-state
                             [:ha-follower-last-error-details
                              :data
                              :snapshot-errors])))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-follower-sync-empty-batch-with-leader-ahead-detects-gap-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        next-state (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 3
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))]
        (with-redefs-fn
          {#'dha/fetch-ha-leader-txlog-batch
           (fn [_ _ _ _ _]
             [])
           #'dha/fetch-ha-endpoint-watermark-lsn
           (fn [_ _ endpoint]
             (case endpoint
               "10.0.0.11:8898"
               {:reachable? true :last-applied-lsn 3}
               "10.0.0.13:8898"
               {:reachable? true :last-applied-lsn 0}
               (throw (ex-info "unexpected-endpoint"
                               {:endpoint endpoint}))))
           #'dha/fetch-ha-endpoint-snapshot-copy!
           (fn [_ _ endpoint _]
             (throw (ex-info "snapshot unavailable"
                             {:error :ha/follower-snapshot-unavailable
                              :endpoint endpoint})))}
          (fn []
            (reset! next-state (#'srv/ha-renew-step "orders" follower-runtime)))))
      (let [state @next-state
            gap-data (get-in state
                             [:ha-follower-last-error-details
                              :data
                              :gap-error
                              :data])]
        (is (= :follower (:ha-role state)))
        (is (= :sync-failed (:ha-follower-last-error state)))
        (is (true? (:ha-follower-degraded? state)))
        (is (= :wal-gap (:ha-follower-degraded-reason state)))
        (is (= :ha/txlog-gap-unresolved (:error gap-data)))
        (is (= 3
               (get-in gap-data
                       [:gap-errors 0 :data :source-last-applied-lsn]))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-follower-sync-transient-failure-backs-off-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        fetch-calls (atom 0)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 3
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            step-state
            (fn [state]
              (with-redefs-fn
                {#'dha/fetch-ha-leader-txlog-batch
                 (fn [_ _ _ _ _]
                   (swap! fetch-calls inc)
                   (throw (ex-info "transient follower sync failure"
                                   {:error :ha/follower-sync-transient})))
                 #'dha/read-ha-local-last-applied-lsn
                 (fn [_] 0)}
                (fn []
                  (#'srv/ha-renew-step "orders" state))))
            state-1 (step-state follower-runtime)
            state-2 (step-state state-1)]
        (is (= 1 @fetch-calls))
        (is (= :sync-failed (:ha-follower-last-error state-1)))
        (is (integer? (:ha-follower-sync-backoff-ms state-1)))
        (is (integer? (:ha-follower-next-sync-not-before-ms state-1)))
        (is (= :follower (:ha-role state-2)))
        (is (= :sync-failed (:ha-follower-last-error state-2)))
        (is (= 1 @fetch-calls)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-follower-sync-gap-falls-back-to-next-source-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-lsn (atom 0)
        calls (atom [])
        reported (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 2
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint _ _]
                            (swap! calls conj endpoint)
                            (case endpoint
                              "10.0.0.11:8898"
                              [{:lsn 3 :ops [[:put "a" "k3" "v3"]]}]
                              "10.0.0.13:8898"
                              [{:lsn 1 :ops [[:put "a" "k1" "v1"]]}
                               {:lsn 2 :ops [[:put "a" "k2" "v2"]]}]
                              []))
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.13:8898"
                              {:reachable? true :last-applied-lsn 2}
                              (throw (ex-info "unexpected-endpoint"
                                              {:endpoint endpoint}))))
                          #'dha/apply-ha-follower-txlog-record!
                          (fn [_ record]
                            (reset! local-lsn (long (:lsn record))))
                          #'dha/read-ha-local-last-applied-lsn
                          (fn [_] @local-lsn)
                          #'dha/report-ha-replica-floor!
                          (fn [db-name m endpoint applied-lsn]
                            (reset! reported {:db-name db-name
                                              :ha-node-id (:ha-node-id m)
                                              :leader-endpoint endpoint
                                              :applied-lsn applied-lsn})
                            {:ok? true})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= ["10.0.0.11:8898" "10.0.0.13:8898"] @calls))
        (is (= :follower (:ha-role next-state)))
        (is (= 2 (:ha-local-last-applied-lsn next-state)))
        (is (= 3 (:ha-follower-next-lsn next-state)))
        (is (= "10.0.0.13:8898" (:ha-follower-source-endpoint next-state)))
        (is (= ["10.0.0.11:8898" "10.0.0.13:8898"]
               (:ha-follower-source-order next-state)))
        (is (nil? (:ha-follower-degraded? next-state)))
        (is (= {:db-name "orders"
                :ha-node-id 2
                :leader-endpoint "10.0.0.11:8898"
                :applied-lsn 2}
               @reported)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-gap-fallback-source-endpoints-prefers-highest-watermark-followers-test
  (let [m {:ha-local-endpoint "10.0.0.12:8898"
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}
                        {:node-id 3 :endpoint "10.0.0.13:8898"}
                        {:node-id 4 :endpoint "10.0.0.14:8898"}
                        {:node-id 5 :endpoint "10.0.0.15:8898"}]}
        lease {:leader-node-id 1
               :leader-endpoint "10.0.0.11:8898"}]
    (with-redefs-fn
      {#'dha/fetch-ha-endpoint-watermark-lsn
       (fn [_ _ endpoint]
         (case endpoint
           "10.0.0.13:8898" {:reachable? true :last-applied-lsn 25}
           "10.0.0.14:8898" {:reachable? true :last-applied-lsn 60}
           "10.0.0.15:8898" {:reachable? false
                             :reason :endpoint-watermark-fetch-failed}
           (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))}
      (fn []
        (is (= ["10.0.0.11:8898"
                "10.0.0.14:8898"
                "10.0.0.13:8898"
                "10.0.0.15:8898"]
               (#'dha/ha-gap-fallback-source-endpoints
                "orders" m lease 20)))))))

(deftest ha-renew-step-follower-sync-gap-reorders-followers-by-watermark-test
  (let [opts (assoc (valid-ha-opts)
                    :ha-members
                    [{:node-id 1 :endpoint "10.0.0.11:8898"}
                     {:node-id 2 :endpoint "10.0.0.12:8898"}
                     {:node-id 3 :endpoint "10.0.0.13:8898"}
                     {:node-id 4 :endpoint "10.0.0.14:8898"}]
                    :ha-control-plane
                    {:backend :in-memory
                     :group-id (str "ha-test-" (UUID/randomUUID))
                     :local-peer-id "10.0.0.12:7801"
                     :voters [{:peer-id "10.0.0.11:7801"
                               :ha-node-id 1
                               :promotable? true}
                              {:peer-id "10.0.0.12:7801"
                               :ha-node-id 2
                               :promotable? true}
                              {:peer-id "10.0.0.13:7801"
                               :ha-node-id 3
                               :promotable? true}
                              {:peer-id "10.0.0.14:7801"
                               :ha-node-id 4
                               :promotable? true}]})
        runtime (#'srv/start-ha-authority "orders" opts)
        local-lsn (atom 0)
        calls (atom [])
        reported (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 2
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.13:8898"
                              {:reachable? true :last-applied-lsn 1}
                              "10.0.0.14:8898"
                              {:reachable? true :last-applied-lsn 2}
                              (throw (ex-info "unexpected-endpoint"
                                              {:endpoint endpoint}))))
                          #'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint _ _]
                            (swap! calls conj endpoint)
                            (case endpoint
                              "10.0.0.11:8898"
                              [{:lsn 3 :ops [[:put "a" "k3" "v3"]]}]
                              "10.0.0.14:8898"
                              [{:lsn 1 :ops [[:put "a" "k1" "v1"]]}
                               {:lsn 2 :ops [[:put "a" "k2" "v2"]]}]
                              "10.0.0.13:8898"
                              (throw
                               (ex-info "must-not-try-lower-watermark-source"
                                        {:endpoint endpoint}))
                              []))
                          #'dha/apply-ha-follower-txlog-record!
                          (fn [_ record]
                            (reset! local-lsn (long (:lsn record))))
                          #'dha/read-ha-local-last-applied-lsn
                          (fn [_] @local-lsn)
                          #'dha/report-ha-replica-floor!
                          (fn [db-name m endpoint applied-lsn]
                            (reset! reported {:db-name db-name
                                              :ha-node-id (:ha-node-id m)
                                              :leader-endpoint endpoint
                                              :applied-lsn applied-lsn})
                            {:ok? true})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= ["10.0.0.11:8898" "10.0.0.14:8898"] @calls))
        (is (= :follower (:ha-role next-state)))
        (is (= "10.0.0.14:8898" (:ha-follower-source-endpoint next-state)))
        (is (= ["10.0.0.11:8898" "10.0.0.14:8898" "10.0.0.13:8898"]
               (:ha-follower-source-order next-state)))
        (is (= {:db-name "orders"
                :ha-node-id 2
                :leader-endpoint "10.0.0.11:8898"
                :applied-lsn 2}
               @reported)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-maybe-enter-candidate-blocked-when-follower-degraded-test
  (let [now-ms (System/currentTimeMillis)
        m {:ha-role :follower
           :ha-follower-degraded? true
           :ha-follower-degraded-reason :wal-gap
           :ha-follower-degraded-details {:error :ha/txlog-gap}
           :ha-node-id 2
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}]
           :ha-promotion-base-delay-ms 0
           :ha-promotion-rank-delay-ms 0
           :ha-membership-hash "hash-a"
           :ha-authority-membership-hash "hash-a"
           :ha-authority-lease {:lease-until-ms (dec now-ms)}}
        next-m (#'dha/maybe-enter-ha-candidate m now-ms)]
    (is (= :follower (:ha-role next-m)))
    (is (= :follower-degraded (:ha-promotion-last-failure next-m)))
    (is (= :wal-gap (get-in next-m [:ha-promotion-failure-details :reason])))))

(deftest ha-maybe-enter-candidate-blocked-when-clock-skew-paused-test
  (let [now-ms (System/currentTimeMillis)
        m {:ha-role :follower
           :ha-clock-skew-paused? true
           :ha-clock-skew-budget-ms 100
           :ha-clock-skew-last-check-ms now-ms
           :ha-clock-skew-last-result {:ok? true
                                       :paused? true
                                       :reason :clock-skew-budget-breached
                                       :budget-ms 100
                                       :clock-skew-ms 125}
           :ha-node-id 2
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}]
           :ha-promotion-base-delay-ms 0
           :ha-promotion-rank-delay-ms 0
           :ha-membership-hash "hash-a"
           :ha-authority-membership-hash "hash-a"
           :ha-authority-lease {:lease-until-ms (dec now-ms)}}
        next-m (#'dha/maybe-enter-ha-candidate m now-ms)]
    (is (= :follower (:ha-role next-m)))
    (is (= :clock-skew-paused (:ha-promotion-last-failure next-m)))
    (is (= :clock-skew-budget-breached
           (get-in next-m [:ha-promotion-failure-details :reason])))
    (is (= 125
           (get-in next-m [:ha-promotion-failure-details :clock-skew-ms])))))

(deftest ha-maybe-enter-candidate-blocked-when-authority-read-failed-test
  (let [now-ms (System/currentTimeMillis)
        m {:ha-role :follower
           :ha-authority-read-ok? false
           :ha-authority-read-error {:reason :authority-read-failed
                                     :data {:error :ha/control-timeout}}
           :ha-node-id 2
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}]
           :ha-promotion-base-delay-ms 0
           :ha-promotion-rank-delay-ms 0
           :ha-membership-hash "hash-a"
           :ha-authority-membership-hash "hash-a"
           :ha-authority-lease {:lease-until-ms (dec now-ms)}}
        next-m (#'dha/maybe-enter-ha-candidate m now-ms)]
    (is (= :follower (:ha-role next-m)))
    (is (= :authority-read-failed (:ha-promotion-last-failure next-m)))
    (is (= :authority-read-failed
           (get-in next-m [:ha-promotion-failure-details :reason])))
    (is (= :ha/control-timeout
           (get-in next-m [:ha-promotion-failure-details :data :error])))))

(deftest ha-maybe-enter-candidate-blocked-when-rejoin-in-progress-test
  (let [now-ms (System/currentTimeMillis)
        m {:ha-role :follower
           :ha-node-id 2
           :ha-authority-owner-node-id 1
           :ha-local-last-applied-lsn 3
           :ha-rejoin-promotion-blocked? true
           :ha-rejoin-promotion-blocked-until-ms (+ now-ms 5000)
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}]
           :ha-promotion-base-delay-ms 0
           :ha-promotion-rank-delay-ms 0
           :ha-max-promotion-lag-lsn 0
           :ha-membership-hash "hash-a"
           :ha-authority-membership-hash "hash-a"
           :ha-authority-read-ok? true
           :ha-authority-lease {:lease-until-ms (dec now-ms)
                                :leader-node-id 1
                                :leader-last-applied-lsn 5}}
        next-m (#'dha/maybe-enter-ha-candidate m now-ms)]
    (is (= :follower (:ha-role next-m)))
    (is (= :rejoin-in-progress (:ha-promotion-last-failure next-m)))
    (is (= 1
           (get-in next-m
                   [:ha-promotion-failure-details :authority-owner-node-id])))))

(deftest maybe-clear-ha-rejoin-promotion-block-after-catch-up-test
  (let [now-ms (System/currentTimeMillis)
        m {:ha-role :follower
           :ha-node-id 2
           :ha-authority-owner-node-id 1
           :ha-local-last-applied-lsn 5
           :ha-rejoin-promotion-blocked? true
           :ha-rejoin-promotion-blocked-until-ms (+ now-ms 5000)
           :ha-max-promotion-lag-lsn 0
           :ha-authority-read-ok? true
           :ha-authority-lease {:leader-node-id 1
                                :leader-last-applied-lsn 5}}
        next-m (#'dha/maybe-clear-ha-rejoin-promotion-block m now-ms)]
    (is (false? (:ha-rejoin-promotion-blocked? next-m)))
    (is (nil? (:ha-rejoin-promotion-blocked-until-ms next-m)))
    (is (integer? (:ha-rejoin-promotion-cleared-ms next-m)))))

(defn- fake-server-with-db-state
  [db-name state]
  (let [dbs (doto (ConcurrentHashMap.)
              (.put db-name state))]
    (srv/->Server (AtomicBoolean. false)
                  0
                  ""
                  0
                  nil
                  nil
                  (ConcurrentLinkedQueue.)
                  nil
                  nil
                  nil
                  (ConcurrentHashMap.)
                  dbs)))

(deftest ensure-ha-runtime-restarts-on-ha-config-change-test
  (let [root "/srv/dtlv"
        db-name "orders"
        old-ha-opts (valid-ha-opts "ha-restart")
        new-ha-opts (assoc old-ha-opts :ha-clock-skew-budget-ms 250)
        old-m {:ha-authority ::old-authority
               :ha-runtime-opts (resolved-ha-runtime-opts
                                 root db-name old-ha-opts)
               :keep :value}
        stopped (atom [])
        started (atom [])]
    (binding [srv/*consensus-ha-opts-fn* (constantly new-ha-opts)
              srv/*stop-ha-renew-loop-fn*
              (fn [m]
                (swap! stopped conj [:renew (:ha-authority m)]))
              srv/*stop-ha-authority-fn*
              (fn [db-name' m]
                (swap! stopped conj [:authority db-name'
                                     (:ha-authority m)]))
              srv/*start-ha-authority-fn*
              (fn [db-name' ha-opts]
                (swap! started conj [db-name' ha-opts])
                {:ha-authority ::new-authority
                 :ha-role :follower})]
      (let [next-m (#'srv/ensure-ha-runtime root db-name old-m ::store)
            expected-opts (resolved-ha-runtime-opts root db-name new-ha-opts)]
        (is (= [[:renew ::old-authority]
                [:authority db-name ::old-authority]]
               @stopped))
        (is (= [[db-name expected-opts]]
               @started))
        (is (= ::new-authority (:ha-authority next-m)))
        (is (= :value (:keep next-m)))
        (is (= expected-opts (:ha-runtime-opts next-m)))))))

(deftest ensure-ha-runtime-restarts-when-authority-is-stopped-test
  (let [root "/srv/dtlv"
        db-name "orders"
        ha-opts (valid-ha-opts "ha-stopped")
        runtime (#'srv/start-ha-authority db-name ha-opts)
        authority (:ha-authority runtime)
        state {:ha-authority authority
               :ha-runtime-opts (resolved-ha-runtime-opts
                                 root db-name ha-opts)
               :keep :value}
        stopped (atom [])
        started (atom [])]
    (try
      (ha/stop-authority! authority)
      (binding [srv/*consensus-ha-opts-fn* (constantly ha-opts)
                srv/*stop-ha-renew-loop-fn*
                (fn [m]
                  (swap! stopped conj [:renew (:ha-authority m)]))
                srv/*stop-ha-authority-fn*
                (fn [db-name' m]
                  (swap! stopped conj [:authority db-name'
                                       (:ha-authority m)]))
                srv/*start-ha-authority-fn*
                (fn [db-name' ha-opts']
                  (swap! started conj [db-name' ha-opts'])
                  {:ha-authority ::restarted-authority
                   :ha-role :follower})]
        (let [next-m (#'srv/ensure-ha-runtime root db-name state ::store)
              expected-opts (resolved-ha-runtime-opts root db-name ha-opts)]
          (is (= [[:renew authority]
                  [:authority db-name authority]]
                 @stopped))
          (is (= [[db-name expected-opts]]
                 @started))
          (is (= ::restarted-authority (:ha-authority next-m)))
          (is (= :value (:keep next-m)))
          (is (= expected-opts (:ha-runtime-opts next-m)))))
      (finally
        (when (some? authority)
          (ha/stop-authority! authority))))))

(deftest ensure-ha-runtime-ignores-non-ha-option-change-test
  (let [root "/srv/dtlv"
        db-name "orders"
        ha-opts (valid-ha-opts "ha-stable")
        state {:ha-authority ::existing-authority
               :ha-runtime-opts (resolved-ha-runtime-opts
                                 root db-name ha-opts)
               :keep :value}
        stopped (atom [])
        started (atom [])]
    (binding [srv/*consensus-ha-opts-fn*
              (constantly (assoc ha-opts :cache-limit 2048))
              srv/*stop-ha-renew-loop-fn*
              (fn [m]
                (swap! stopped conj [:renew (:ha-authority m)]))
              srv/*stop-ha-authority-fn*
              (fn [db-name' m]
                (swap! stopped conj [:authority db-name'
                                     (:ha-authority m)]))
              srv/*start-ha-authority-fn*
              (fn [db-name' ha-opts]
                (swap! started conj [db-name' ha-opts])
                {:ha-authority ::unexpected
                 :ha-role :follower})]
      (let [next-m (#'srv/ensure-ha-runtime root db-name state ::store)]
        (is (= state next-m))
        (is (empty? @stopped))
        (is (empty? @started))))))

(deftest copy-response-meta-includes-ha-snapshot-fields-test
  (let [dir (u/tmp-dir (str "ha-copy-meta-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity (str "db-" (UUID/randomUUID))}
        store (st/open dir nil opts)]
    (try
      (let [kv (.-lmdb store)
            _ (i/open-dbi kv "a")
            _ (i/transact-kv kv [[:put "a" "k1" "v1"]])
            _ (i/create-snapshot! kv)
            snapshot-lsn
            (long (or (i/get-value kv c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (i/transact-kv kv [[:put "a" "k2" "v2"]])
            watermark-lsn
            (long (or (:last-applied-lsn (i/txlog-watermarks kv)) 0))
            meta (#'srv/copy-response-meta "orders" store {:compact? false})]
        (is (= "orders" (:db-name meta)))
        (is (= (:db-identity opts) (:db-identity meta)))
        (is (= snapshot-lsn (:snapshot-last-applied-lsn meta)))
        (is (> watermark-lsn snapshot-lsn)))
      (finally
        (i/close store)
        (u/delete-files dir)))))

(deftest copy-response-meta-includes-db-identity-for-raw-lmdb-test
  (let [dir (u/tmp-dir (str "ha-copy-meta-raw-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity (str "db-" (UUID/randomUUID))}
        store (st/open dir nil opts)
        lmdb (.-lmdb store)]
    (try
      (let [_ (i/open-dbi lmdb "a")
            _ (i/transact-kv lmdb [[:put "a" "k1" "v1"]])
            _ (i/create-snapshot! lmdb)
            snapshot-lsn
            (long (or (i/get-value lmdb c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (i/transact-kv lmdb [[:put "a" "k2" "v2"]])
            watermark-lsn
            (long (or (:last-applied-lsn (i/txlog-watermarks lmdb)) 0))
            meta (#'srv/copy-response-meta "orders" lmdb {:compact? false})]
        (is (= "orders" (:db-name meta)))
        (is (= (:db-identity opts) (:db-identity meta)))
        (is (= snapshot-lsn (:snapshot-last-applied-lsn meta)))
        (is (> watermark-lsn snapshot-lsn)))
      (finally
        (i/close store)
        (u/delete-files dir)))))

(deftest read-ha-local-last-applied-lsn-uses-persisted-ha-lsn-after-install-test
  (let [db-name "orders"
        db-identity (str "db-" (UUID/randomUUID))
        source-dir (u/tmp-dir (str "ha-source-snapshot-floor-"
                                   (UUID/randomUUID)))
        local-dir (u/tmp-dir (str "ha-local-snapshot-floor-"
                                  (UUID/randomUUID)))
        copy-dir (u/tmp-dir (str "ha-copy-snapshot-floor-"
                                 (UUID/randomUUID)))
        source-store (st/open source-dir nil {:db-name db-name
                                              :db-identity db-identity
                                              :wal? true})
        local-store (st/open local-dir nil {:db-name db-name
                                            :db-identity db-identity
                                            :wal? true})
        install-state (atom nil)
        snapshot-lsn* (atom nil)]
    (try
      (let [source-kv (.-lmdb source-store)
            _ (i/open-dbi source-kv "a")
            _ (doseq [i (range 5)]
                (i/transact-kv source-kv
                               [[:put "a"
                                 (str "k" i)
                                 (str "v" i)]]))
            _ (i/create-snapshot! source-kv)
            snapshot-lsn
            (long (or (i/get-value source-kv c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (reset! snapshot-lsn* snapshot-lsn)
            _ (is (> snapshot-lsn 1))
            _ (i/copy source-kv copy-dir false)
            install-res (#'dha/install-ha-local-snapshot!
                         {:store local-store
                          :ha-db-identity db-identity}
                         copy-dir)]
        (is (:ok? install-res))
        (reset! install-state (:state install-res))
        (let [installed-store (:store @install-state)
              _ (#'dha/persist-ha-local-applied-lsn!
                 {:store installed-store}
                 @snapshot-lsn*)
              installed-kv (.-lmdb installed-store)
              watermark-lsn (long (or (:last-applied-lsn
                                       (i/txlog-watermarks installed-kv))
                                      0))]
          (is (= @snapshot-lsn*
                 (i/get-value installed-kv c/kv-info
                              c/ha-local-applied-lsn
                              :keyword :data)))
          (is (<= watermark-lsn @snapshot-lsn*))
          (is (= @snapshot-lsn*
                 (dha/read-ha-local-last-applied-lsn
                  {:store installed-store})))))
      (finally
        (when-let [store (:store @install-state)]
          (when-not (identical? store local-store)
            (when-not (i/closed? store)
              (i/close store))))
        (when-not (i/closed? local-store)
          (i/close local-store))
        (when-not (i/closed? source-store)
          (i/close source-store))
        (u/delete-files copy-dir)
        (u/delete-files local-dir)
        (u/delete-files source-dir)))))

(deftest ha-renew-step-follower-gap-bootstraps-from-snapshot-copy-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-dir (u/tmp-dir (str "ha-local-snapshot-" (UUID/randomUUID)))
        source-dir (u/tmp-dir (str "ha-source-snapshot-" (UUID/randomUUID)))
        db-identity (:ha-db-identity runtime)
        local-store (st/open local-dir nil {:db-name "orders"
                                            :db-identity db-identity
                                            :wal? true})
        source-store (st/open source-dir nil {:db-name "orders"
                                              :db-identity db-identity
                                              :wal? true})
        snapshot-lsn* (atom nil)
        reported (atom nil)
        next-state (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [local-kv (.-lmdb local-store)
            source-kv (.-lmdb source-store)
            _ (i/open-dbi source-kv "a")
            _ (i/transact-kv source-kv
                             [[:put "a" "k1" "v1"]
                              [:put "a" "k2" "v2"]])
            _ (i/create-snapshot! source-kv)
            snapshot-lsn
            (long (or (i/get-value source-kv c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (reset! snapshot-lsn* snapshot-lsn)
            authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity db-identity
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn (inc snapshot-lsn)
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :store local-store
                                        :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))]
        (with-redefs-fn
          {#'dha/fetch-ha-leader-txlog-batch
           (fn [_ _ endpoint _ _]
             (case endpoint
               "10.0.0.11:8898"
               [{:lsn (inc snapshot-lsn)
                 :ops [[:put "a" "k3" "v3"]]}]
               "10.0.0.13:8898"
               [{:lsn (inc snapshot-lsn)
                 :ops [[:put "a" "k3" "v3"]]}]
               []))
           #'dha/fetch-ha-endpoint-watermark-lsn
           (fn [_ _ endpoint]
             (case endpoint
               "10.0.0.13:8898"
               {:reachable? true :last-applied-lsn snapshot-lsn}
               (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))
           #'dha/fetch-ha-endpoint-snapshot-copy!
           (fn [_ _ endpoint dest-dir]
             (case endpoint
               "10.0.0.11:8898"
               (throw (ex-info "leader snapshot unavailable"
                               {:error :ha/follower-snapshot-unavailable}))
               "10.0.0.13:8898"
               {:copy-meta
                (assoc (i/copy source-kv dest-dir false)
                       :db-name "orders"
                       :db-identity db-identity
                       :snapshot-last-applied-lsn snapshot-lsn)}
               (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))
           #'dha/report-ha-replica-floor!
           (fn [db-name m endpoint applied-lsn]
             (reset! reported {:db-name db-name
                               :ha-node-id (:ha-node-id m)
                               :leader-endpoint endpoint
                               :applied-lsn applied-lsn})
             {:ok? true})}
          (fn []
            (reset! next-state (#'srv/ha-renew-step "orders" follower-runtime)))))
      (let [state @next-state
            next-kv (.-lmdb (:store state))]
        (i/open-dbi next-kv "a")
        (is (= :follower (:ha-role state)))
        (is (= (inc @snapshot-lsn*) (:ha-local-last-applied-lsn state)))
        (is (= (inc @snapshot-lsn*)
               (i/get-value next-kv c/kv-info
                            c/ha-local-applied-lsn
                            :keyword :data)))
        (is (= (inc @snapshot-lsn*)
               (dha/read-ha-local-last-applied-lsn
                {:store (:store state)})))
        (is (= (+ 2 @snapshot-lsn*) (:ha-follower-next-lsn state)))
        (is (= 1 (:ha-follower-last-batch-size state)))
        (is (= "10.0.0.11:8898" (:ha-follower-source-endpoint state)))
        (is (= "10.0.0.13:8898"
               (:ha-follower-bootstrap-source-endpoint state)))
        (is (= @snapshot-lsn*
               (:ha-follower-bootstrap-snapshot-last-applied-lsn state)))
        (is (integer? (:ha-follower-last-bootstrap-ms state)))
        (is (nil? (:ha-follower-degraded? state)))
        (is (= "v2" (i/get-value next-kv "a" "k2")))
        (is (= "v3" (i/get-value next-kv "a" "k3")))
        (is (= {:db-name "orders"
                :ha-node-id 2
                :leader-endpoint "10.0.0.11:8898"
                :applied-lsn (inc @snapshot-lsn*)}
               @reported)))
      (finally
        (when-let [store (:store @next-state)]
          (when-not (i/closed? store)
            (i/close store)))
        (when-not (i/closed? source-store)
          (i/close source-store))
        (when-not (i/closed? local-store)
          (i/close local-store))
        (#'srv/stop-ha-authority "orders" runtime)
        (u/delete-files local-dir)
        (u/delete-files source-dir)))))

(deftest ha-renew-step-follower-gap-snapshot-bootstrap-rejects-db-identity-mismatch-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-dir (u/tmp-dir (str "ha-local-snapshot-mismatch-"
                                  (UUID/randomUUID)))
        local-store (st/open local-dir nil {:db-name "orders"
                                            :db-identity (:ha-db-identity runtime)
                                            :wal? true})
        next-state (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 3
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :store local-store
                                        :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))]
        (with-redefs-fn
          {#'dha/fetch-ha-leader-txlog-batch
           (fn [_ _ endpoint _ _]
             (case endpoint
               "10.0.0.11:8898"
               [{:lsn 3 :ops [[:put "a" "k3" "v3"]]}]
               "10.0.0.13:8898"
               [{:lsn 3 :ops [[:put "a" "k3" "v3"]]}]
               []))
           #'dha/fetch-ha-endpoint-watermark-lsn
           (fn [_ _ endpoint]
             (case endpoint
               "10.0.0.13:8898"
               {:reachable? true :last-applied-lsn 2}
               (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))
           #'dha/fetch-ha-endpoint-snapshot-copy!
           (fn [_ _ endpoint _]
             {:copy-meta {:db-name "orders"
                          :db-identity "db-mismatch"
                          :snapshot-last-applied-lsn 2}})}
          (fn []
            (reset! next-state (#'srv/ha-renew-step "orders" follower-runtime)))))
      (let [state @next-state]
        (is (= :follower (:ha-role state)))
        (is (true? (:ha-follower-degraded? state)))
        (is (= :wal-gap (:ha-follower-degraded-reason state)))
        (is (some #(= :ha/follower-snapshot-db-identity-mismatch
                      (or (:error %)
                          (get-in % [:data :error])))
                  (get-in state
                          [:ha-follower-last-error-details
                           :data
                           :snapshot-errors])))
        (is (= local-store (:store state))))
      (finally
        (when-not (i/closed? local-store)
          (i/close local-store))
        (#'srv/stop-ha-authority "orders" runtime)
        (u/delete-files local-dir)))))

(deftest ha-write-admission-allows-valid-leader-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)]
    (try
      (let [acquire (ha/try-acquire-lease
                     (:ha-authority runtime)
                     {:db-identity (:db-identity opts)
                      :leader-node-id (:ha-node-id opts)
                      :leader-endpoint "10.0.0.12:8898"
                      :lease-renew-ms (:ha-lease-renew-ms opts)
                      :lease-timeout-ms (:ha-lease-timeout-ms opts)
                      :leader-last-applied-lsn 1
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})
            state (-> runtime
                      (assoc :ha-role :leader
                             :ha-authority-owner-node-id (:ha-node-id opts)
                             :ha-lease-until-ms (+ now-ms 10000)
                             :ha-leader-term (:term acquire)
                             :ha-authority-term (:term acquire)))
            server (fake-server-with-db-state "orders" state)]
        (is (:ok? acquire))
        (is (nil? (#'srv/ha-write-admission-error
                   server {:type :transact-kv :args ["orders"]}))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-control-quorum-loss-demotes-leader-and-blocks-writes-test
  (let [authority (reify ha/ILeaseAuthority
                    (start-authority! [this] this)
                    (stop-authority! [this] this)
                    (read-lease [_ _]
                      (throw (ex-info "quorum lost"
                                      {:error :ha/control-timeout})))
                    (try-acquire-lease [_ _]
                      (throw (ex-info "must-not-acquire" {})))
                    (renew-lease [_ _]
                      {:ok? false
                       :reason :control-timeout})
                    (read-membership-hash [_]
                      "hash-a")
                    (init-membership-hash! [_ _]
                      (throw (ex-info "unsupported" {})))
                    (read-voters [_] [])
                    (replace-voters! [_ _]
                      (throw (ex-info "unsupported" {}))))
        leader-state
        {:ha-authority authority
         :ha-db-identity "db-a"
         :ha-role :leader
         :ha-node-id 2
         :ha-local-endpoint "10.0.0.12:8898"
         :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                      {:node-id 2 :endpoint "10.0.0.12:8898"}
                      {:node-id 3 :endpoint "10.0.0.13:8898"}]
         :ha-membership-hash "hash-a"
         :ha-authority-membership-hash "hash-a"
         :ha-authority-owner-node-id 2
         :ha-authority-term 3
         :ha-lease-until-ms (+ (System/currentTimeMillis) 10000)
         :ha-lease-renew-ms 5000
         :ha-lease-timeout-ms 15000
         :ha-leader-term 3
         :ha-leader-last-applied-lsn 7
         :ha-last-authority-refresh-ms (System/currentTimeMillis)}
        demoting-state (#'srv/ha-renew-step "orders" leader-state)
        demoting-server (fake-server-with-db-state "orders" demoting-state)
        demoting-err (#'srv/ha-write-admission-error
                      demoting-server
                      {:type :transact-kv :args ["orders"]})
        follower-state (#'srv/ha-renew-step
                        "orders"
                        (assoc demoting-state
                               :ha-demoted-at-ms
                               (dec (System/currentTimeMillis))))
        follower-server (fake-server-with-db-state "orders" follower-state)
        follower-err (#'srv/ha-write-admission-error
                      follower-server
                      {:type :transact-kv :args ["orders"]})]
    (is (= :demoting (:ha-role demoting-state)))
    (is (= :renew-failed (:ha-demotion-reason demoting-state)))
    (is (= :control-timeout
           (get-in demoting-state [:ha-demotion-details :reason])))
    (is (= :ha/write-rejected (:error demoting-err)))
    (is (= :demoting (:reason demoting-err)))
    (is (true? (:retryable? demoting-err)))
    (is (= :follower (:ha-role follower-state)))
    (is (= :ha/write-rejected (:error follower-err)))
    (is (= :not-leader (:reason follower-err)))))

(deftest ha-write-admission-rejects-follower-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        state (assoc runtime :ha-role :follower)
        server (fake-server-with-db-state "orders" state)
        err (#'srv/ha-write-admission-error
             server {:type :transact-kv :args ["orders"]})]
    (try
      (is (= :ha/write-rejected (:error err)))
      (is (= :not-leader (:reason err)))
      (is (true? (:retryable? err)))
      (is (= "orders" (:db-name err)))
      (is (= ["10.0.0.11:8898" "10.0.0.12:8898" "10.0.0.13:8898"]
             (:ha-retry-endpoints err)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-write-admission-rejects-lease-expired-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)
        state (-> runtime
                  (assoc :ha-role :leader
                         :ha-authority-owner-node-id (:ha-node-id opts)
                         :ha-lease-until-ms (dec now-ms)
                         :ha-leader-term 3
                         :ha-authority-term 3))
        server (fake-server-with-db-state "orders" state)
        err (#'srv/ha-write-admission-error
             server {:type :transact-kv :args ["orders"]})]
    (try
      (is (= :ha/write-rejected (:error err)))
      (is (= :lease-expired (:reason err)))
      (is (true? (:retryable? err)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-write-admission-rejects-term-mismatch-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)
        state (-> runtime
                  (assoc :ha-role :leader
                         :ha-authority-owner-node-id (:ha-node-id opts)
                         :ha-lease-until-ms (+ now-ms 10000)
                         :ha-leader-term 7
                         :ha-authority-term 6))
        server (fake-server-with-db-state "orders" state)
        err (#'srv/ha-write-admission-error
             server {:type :transact-kv :args ["orders"]})]
    (try
      (is (= :ha/write-rejected (:error err)))
      (is (= :term-mismatch (:reason err)))
      (is (true? (:retryable? err)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-write-admission-rejects-membership-hash-mismatch-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)
        state (-> runtime
                  (assoc :ha-role :leader
                         :ha-authority-owner-node-id (:ha-node-id opts)
                         :ha-lease-until-ms (+ now-ms 10000)
                         :ha-leader-term 3
                         :ha-authority-term 3
                         :ha-membership-mismatch? true
                         :ha-authority-membership-hash "authority-hash"))
        server (fake-server-with-db-state "orders" state)
        err (#'srv/ha-write-admission-error
             server {:type :transact-kv :args ["orders"]})]
    (try
      (is (= :ha/write-rejected (:error err)))
      (is (= :membership-hash-mismatch (:reason err)))
      (is (false? (:retryable? err)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-write-admission-owner-mismatch-prioritizes-owner-endpoint-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)
        state (-> runtime
                  (assoc :ha-role :leader
                         :ha-authority-owner-node-id 1
                         :ha-authority-lease {:leader-endpoint "10.0.0.11:8898"}
                         :ha-lease-until-ms (+ now-ms 10000)
                         :ha-leader-term 7
                         :ha-authority-term 7))
        server (fake-server-with-db-state "orders" state)
        err (#'srv/ha-write-admission-error
             server {:type :transact-kv :args ["orders"]})]
    (try
      (is (= :ha/write-rejected (:error err)))
      (is (= :owner-mismatch (:reason err)))
      (is (true? (:retryable? err)))
      (is (= "10.0.0.11:8898" (:ha-authoritative-leader-endpoint err)))
      (is (= ["10.0.0.11:8898" "10.0.0.12:8898" "10.0.0.13:8898"]
             (:ha-retry-endpoints err)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))
