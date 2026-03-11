(ns datalevin.server-ha-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ha]
   [datalevin.interpret :as interp]
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

(defn- e2e-remote-read-value
  [ctx node-id key]
  (when-let [conn (get-in ctx [:conns node-id])]
    (try
      (e2e-read-value conn key)
      (catch Throwable _
        nil))))

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
    (let [dt-db (:dt-db db-state)
          store (:store db-state)]
      (or
       (when dt-db
         (try
           (d/q e2e-ha-value-query dt-db key)
           (catch Throwable _
             nil)))
       (when (e2e-store-open? store)
         (try
           (d/q e2e-ha-value-query
                (db/new-db store)
                key)
           (catch Throwable _
             ;; Snapshot install closes the old LMDB before the reopened store is
             ;; published into the server map. Treat that swap window as retryable.
             nil)))))))

(defn- e2e-local-raw-value-probe
  [ctx node-id key]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    (let [store (:store db-state)]
      (when (e2e-store-open? store)
        (try
          (when-let [e (i/av-first-e store :drill/key key)]
            {:eid e
             :value (i/ea-first-v store e :drill/value)})
          (catch Throwable _
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
                           :remote-node-values
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (e2e-remote-read-value ctx node-id key)]))
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
  ([ctx-atom node-id leader-id min-lsn]
   (e2e-wait-for-follower-rejoin! ctx-atom
                                  node-id
                                  leader-id
                                  min-lsn
                                  nil
                                  nil))
  ([ctx-atom node-id leader-id min-lsn visible-key visible-expected]
   (let [timeout-ms 30000
         deadline (+ (now-ms) timeout-ms)]
     (loop [last-state nil]
       (let [ctx @ctx-atom
             db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))
             visible-value (when visible-key
                             (e2e-local-read-value ctx node-id visible-key))
             state (when db-state
                     (cond-> {:ha-role (:ha-role db-state)
                              :ha-authority-owner-node-id
                              (:ha-authority-owner-node-id db-state)
                              :ha-authority-term (:ha-authority-term db-state)
                              :ha-follower-degraded? (:ha-follower-degraded? db-state)
                              :ha-follower-last-error (:ha-follower-last-error db-state)
                              :last-applied-lsn (e2e-effective-local-lsn ctx node-id)}
                       visible-key
                       (assoc :visible-key visible-key
                              :visible-value visible-value)))]
         (if (and state
                  (= :follower (:ha-role state))
                  (= leader-id (:ha-authority-owner-node-id state))
                  (integer? (:ha-authority-term state))
                  (pos? (long (:ha-authority-term state)))
                  (>= (long (:last-applied-lsn state)) (long min-lsn))
                  (not (:ha-follower-degraded? state))
                  (nil? (:ha-follower-last-error state))
                  (or (nil? visible-key)
                      (= visible-expected visible-value)))
           state
           (if (< (now-ms) deadline)
             (do
               (Thread/sleep 250)
               (recur (or state last-state)))
             (throw (ex-info "Timed out waiting for follower-only rejoin"
                             {:node-id node-id
                              :leader-id leader-id
                              :min-lsn min-lsn
                              :visible-key visible-key
                              :visible-expected visible-expected
                             :timeout-ms timeout-ms
                             :last-state last-state
                             :remote-value-probes
                             (into {}
                                   (map (fn [live-node-id]
                                          [live-node-id
                                           (when visible-key
                                             (e2e-remote-read-value
                                              ctx
                                              live-node-id
                                              visible-key))]))
                                   (:live-node-ids ctx))
                             :raw-value-probes
                             (into {}
                                   (map (fn [live-node-id]
                                          [live-node-id
                                           (e2e-local-raw-value-probe
                                            ctx
                                            live-node-id
                                            visible-key)]))
                                   (:live-node-ids ctx))
                             :node-diagnostics
                             (e2e-node-diagnostics-by-node
                               (:servers ctx)
                               (:db-name ctx)
                               (:live-node-ids ctx))})))))))))

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
                                               catch-up-lsn
                                               "post-failover"
                                               "v2")]
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

(defn- e2e-wal-gap-base-opts
  [base-opts]
  (assoc base-opts
         :wal-segment-max-ms 100
         :wal-segment-prealloc? false
         :wal-segment-prealloc-mode :none
         :wal-replica-floor-ttl-ms 500
         :snapshot-scheduler? false))

(defn- run-e2e-ha-rejoin-bootstrap!
  [control-backend]
  (let [write-sleep-ms 150
        writes-per-batch 4
        old-config log/*config*]
    (log/set-min-level! :fatal)
    (try
      (with-e2e-ha-cluster
        control-backend
        {:base-opts-fn e2e-wal-gap-base-opts}
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
            (let [baseline-lsn (e2e-effective-local-lsn ctx initial-leader-id)]
              (swap! ctx-atom e2e-stop-node initial-leader-id)
              (let [ctx @ctx-atom
                    {:keys [leader-id]}
                    (e2e-wait-for-single-leader! (:servers ctx)
                                                 (:db-name ctx)
                                                 (:live-node-ids ctx)
                                                 leader-timeout-ms)
                    failover-leader-id leader-id
                    failover-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])
                    source-node-ids (->> (:live-node-ids ctx)
                                         sort
                                         vec)
                    source-endpoints (->> source-node-ids
                                          (map #(get-in ctx [:nodes (dec %) :endpoint]))
                                          set)
                    _ (e2e-write-value! (:conns ctx)
                                        failover-leader-id
                                        "post-failover"
                                        "v2")
                    _ (e2e-verify-value-on-nodes! ctx
                                                  (:live-node-ids ctx)
                                                  "post-failover"
                                                  "v2")
                    _ (e2e-create-snapshots-on-nodes! ctx source-node-ids)
                    batch-2-lsn (e2e-write-series-with-rolls!
                                 ctx
                                 failover-leader-id
                                 "gap-batch-1"
                                 writes-per-batch
                                 write-sleep-ms)
                    snapshot-2 (e2e-create-snapshots-on-nodes! ctx source-node-ids)
                    batch-3-lsn (e2e-write-series-with-rolls!
                                 ctx
                                 failover-leader-id
                                 "gap-batch-2"
                                 writes-per-batch
                                 write-sleep-ms)
                    snapshot-3 (e2e-create-snapshots-on-nodes! ctx source-node-ids)
                    follower-next-lsn (unchecked-inc (long baseline-lsn))
                    retention-before
                    (let [deadline (+ (now-ms) 10000)]
                      (loop [last-state nil]
                        (let [state (e2e-retention-state-on-node @ctx-atom
                                                                 failover-leader-id)]
                          (if (and (= 1 (long (or (get-in state
                                                          [:floor-providers
                                                           :replica
                                                           :active-count])
                                                 0)))
                                   (> (long (or (:required-retained-floor-lsn state)
                                                0))
                                      (long follower-next-lsn)))
                            state
                            (if (< (now-ms) deadline)
                              (do
                                (Thread/sleep 200)
                                (recur (or state last-state)))
                              (throw (ex-info "Timed out waiting for replica floor expiry before rejoin WAL GC"
                                              {:leader-id failover-leader-id
                                               :follower-next-lsn follower-next-lsn
                                               :last-state last-state})))))))
                    gc-results
                    (into {}
                          (map (fn [node-id]
                                 [node-id
                                  (e2e-gc-txlog-segments-on-node!
                                   @ctx-atom node-id)]))
                          source-node-ids)
                    _ (doseq [[node-id gc-result] gc-results
                              :let [min-retained-lsn
                                    (long (or (get-in gc-result [:after :min-retained-lsn])
                                              0))]]
                        (when (zero? (:deleted-count gc-result))
                          (throw (ex-info "Rejoin bootstrap e2e did not delete WAL segments"
                                          {:node-id node-id
                                           :gc-result gc-result})))
                        (when (<= min-retained-lsn (long follower-next-lsn))
                          (throw (ex-info "Rejoin bootstrap e2e did not create a real WAL gap"
                                          {:node-id node-id
                                           :follower-next-lsn follower-next-lsn
                                           :gc-result gc-result}))))
                    _ (swap! ctx-atom e2e-restart-node initial-leader-id)
                    ctx @ctx-atom
                    {:keys [leader-id]}
                    (e2e-wait-for-single-leader! (:servers ctx)
                                                 (:db-name ctx)
                                                 (:live-node-ids ctx)
                                                 leader-timeout-ms)
                    current-leader-id leader-id
                    _ (when (= current-leader-id initial-leader-id)
                        (throw (ex-info "Rejoin bootstrap node unexpectedly became leader"
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
                    bootstrap-state
                    (e2e-wait-for-follower-bootstrap!
                     ctx-atom
                     initial-leader-id
                     (long (or (get-in snapshot-2
                                       [failover-leader-id :snapshot :applied-lsn])
                               0)))
                    _ (when-not (= :follower (:ha-role bootstrap-state))
                        (throw (ex-info "Rejoined node did not remain follower during bootstrap"
                                        {:node-id initial-leader-id
                                         :bootstrap-state bootstrap-state})))
                    _ (when-not (= current-leader-id
                                    (:ha-authority-owner-node-id bootstrap-state))
                        (throw (ex-info "Rejoined node did not follow the active leader after bootstrap"
                                        {:expected-leader-id current-leader-id
                                         :bootstrap-state bootstrap-state})))
                    _ (when-not (contains?
                                 source-endpoints
                                 (:ha-follower-bootstrap-source-endpoint
                                  bootstrap-state))
                        (throw (ex-info "Rejoin bootstrap used an unexpected snapshot source endpoint"
                                        {:expected-source-endpoints source-endpoints
                                         :bootstrap-state bootstrap-state})))
                    rejoin-state
                    (e2e-wait-for-follower-rejoin! ctx-atom
                                                   initial-leader-id
                                                   current-leader-id
                                                   batch-3-lsn)]
                (e2e-write-value! (:conns @ctx-atom)
                                  current-leader-id
                                  "post-rejoin-bootstrap"
                                  "v-final")
                (e2e-verify-value-on-nodes! @ctx-atom
                                            (:live-node-ids @ctx-atom)
                                            "post-rejoin-bootstrap"
                                            "v-final")
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
                   :failover-leader-endpoint failover-endpoint
                   :current-leader-id current-leader-id
                   :current-leader-endpoint
                   (get-in @ctx-atom [:nodes (dec current-leader-id) :endpoint])
                   :rejoined-node-id initial-leader-id
                   :baseline-lsn baseline-lsn
                   :follower-next-lsn follower-next-lsn
                   :retention-before retention-before
                   :gc-results gc-results
                   :source-node-ids source-node-ids
                   :source-endpoints source-endpoints
                   :bootstrap-state bootstrap-state
                   :rejoin-state rejoin-state
                   :replica-floor replica-floor
                   :snapshots {:mid-gap snapshot-2
                               :pre-rejoin snapshot-3}}))))))
      (finally
        (log/set-config! old-config)))))

(defn- run-e2e-ha-degraded-mode-no-valid-source!
  [control-backend]
  (let [write-sleep-ms 150
        writes-per-batch 4
        old-config log/*config*]
    (log/set-min-level! :fatal)
    (try
      (with-e2e-ha-cluster
        control-backend
        {:base-opts-fn e2e-wal-gap-base-opts}
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

(deftest ha-e2e-in-memory-three-node-rejoin-bootstrap-test
  (let [result (run-e2e-ha-rejoin-bootstrap! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (= (:initial-leader-id result)
           (:rejoined-node-id result)))
    (is (not= (:current-leader-id result)
              (:rejoined-node-id result)))
    (is (= :follower (get-in result [:bootstrap-state :ha-role])))
    (is (= (:current-leader-id result)
           (get-in result [:bootstrap-state :ha-authority-owner-node-id])))
    (is (integer? (get-in result [:bootstrap-state :ha-follower-last-bootstrap-ms])))
    (is (pos? (long (or (get-in result
                                [:bootstrap-state
                                 :ha-follower-bootstrap-snapshot-last-applied-lsn])
                        0))))
    (is (contains? (:source-endpoints result)
                   (get-in result
                           [:bootstrap-state
                            :ha-follower-bootstrap-source-endpoint])))
    (is (= :follower (get-in result [:rejoin-state :ha-role])))
    (is (nil? (get-in result [:rejoin-state :ha-follower-degraded?])))
    (is (nil? (get-in result [:rejoin-state :ha-follower-last-error])))
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

(deftest ha-e2e-sofa-jraft-three-node-rejoin-bootstrap-characterization-test
  (when (= "1" (System/getenv "DTLV_RUN_HA_E2E_SOFA_JRAFT"))
    (let [result (run-e2e-ha-rejoin-bootstrap! :sofa-jraft)]
      (is (= :sofa-jraft (:control-backend result)))
      (is (= (:initial-leader-id result)
             (:rejoined-node-id result)))
      (is (not= (:current-leader-id result)
                (:rejoined-node-id result)))
      (is (= :follower (get-in result [:bootstrap-state :ha-role])))
      (is (integer? (get-in result
                            [:bootstrap-state :ha-follower-last-bootstrap-ms]))))))

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

(deftest create-does-not-auto-reopen-consensus-ha-db-from-persisted-sessions-test
  (let [port       (random-port-candidate)
        root       (u/tmp-dir (str "server-ha-reopen-" (UUID/randomUUID)))
        group-id   (str "server-ha-reopen-" (UUID/randomUUID))
        local-peer "127.0.0.1:17801"
        peer-2     "127.0.0.1:17802"
        peer-3     "127.0.0.1:17803"
        db-name    "orders"
        endpoint-1 (str "127.0.0.1:" port)
        endpoint-2 "127.0.0.1:28899"
        endpoint-3 "127.0.0.1:28900"
        client-id  (UUID/randomUUID)
        ha-opts    {:wal? true
                    :ha-mode :consensus-lease
                    :db-identity (str "db-" (UUID/randomUUID))
                    :ha-node-id 1
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
                    :ha-members [{:node-id 1 :endpoint endpoint-1}
                                 {:node-id 2 :endpoint endpoint-2}
                                 {:node-id 3 :endpoint endpoint-3}]
                    :ha-control-plane
                    {:backend :sofa-jraft
                     :group-id group-id
                     :local-peer-id local-peer
                     :rpc-timeout-ms 5000
                     :election-timeout-ms 5000
                     :operation-timeout-ms 30000
                     :voters [{:peer-id local-peer
                               :ha-node-id 1
                               :promotable? true}
                              {:peer-id peer-2
                               :ha-node-id 2
                               :promotable? true}
                              {:peer-id peer-3
                               :ha-node-id 3
                               :promotable? true}]}}]
    (try
      (let [server-1 (binding [c/*db-background-sampling?* false]
                       (srv/create {:port port :root root}))]
        (try
          (let [store (st/open (#'srv/db-dir root db-name)
                               e2e-ha-schema
                               ha-opts)]
            (try
              (#'srv/add-client server-1 "127.0.0.1" client-id c/default-username)
              (#'srv/update-client server-1
                                   client-id
                                   #(-> %
                                        (update :stores assoc db-name
                                                {:datalog? true
                                                 :dbis #{}})
                                        (update :dt-dbs conj db-name)))
              (is (contains? (set (keys (.-clients ^Server server-1))) client-id))
              (finally
                (i/close store))))
          (srv/stop server-1)
          (let [server-2 (binding [c/*db-background-sampling?* false]
                           (srv/create {:port port :root root}))]
            (try
              (is (nil? (get (.-dbs ^Server server-2) db-name)))
              (is (contains? (set (keys (.-clients ^Server server-2))) client-id))
              (finally
                (srv/stop server-2))))
          (finally
            (when (.get ^AtomicBoolean (.-running ^Server server-1))
              (srv/stop server-1)))))
      (finally
        (safe-delete-dir! root)))))

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

(deftest ha-renew-step-promotes-from-empty-lease-using-local-watermark-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/read-ha-local-watermark-lsn
                          (fn [_]
                            3)
                          #'dha/run-ha-fencing-hook
                          (fn [_ _ _]
                            {:ok? true})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :leader (:ha-role next-state)))
        (is (= 1 (:ha-leader-term next-state)))
        (is (= 3
               (get-in next-state
                       [:ha-authority-lease :leader-last-applied-lsn]))))
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

(deftest ha-renew-step-candidate-lag-input-uses-reachable-watermark-test
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
        (is (= :pre-fence
               (get-in next-state [:ha-promotion-failure-details :phase])))
        (is (= 10
               (get-in next-state
                       [:ha-promotion-failure-details
                        :leader-lag-input
                        :leader-watermark-last-applied-lsn]))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-expired-reachable-leader-behind-uses-best-reachable-watermark-test
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
                :leader-last-applied-lsn 17
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 10))
            next-state (with-redefs-fn
                         {#'dha/run-ha-fencing-hook
                          (fn [_ _ _] {:ok? true})
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.11:8898"
                              {:reachable? true
                               :last-applied-lsn 4
                               :source :remote}

                              "10.0.0.12:8898"
                              {:reachable? true
                               :last-applied-lsn 10
                               :source :local}

                              "10.0.0.13:8898"
                              {:reachable? true
                               :last-applied-lsn 10
                               :source :remote}

                              {:reachable? false
                               :reason :missing-endpoint}))}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :leader (:ha-role next-state)))
        (is (= 2 (:ha-authority-owner-node-id next-state)))
        (is (= 10
               (get-in next-state
                       [:ha-authority-lease :leader-last-applied-lsn])))
        (is (= 10
               (get-in next-state
                       [:ha-local-last-applied-lsn])))
        (is (nil? (:ha-promotion-last-failure next-state))))
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

(deftest ha-renew-step-candidate-unreachable-leader-uses-reachable-member-watermark-test
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
                :leader-last-applied-lsn 17
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 10))
            next-state (with-redefs-fn
                         {#'dha/run-ha-fencing-hook
                          (fn [_ _ _] {:ok? true})
                          #'dha/maybe-wait-unreachable-leader-before-pre-cas!
                          (fn [_ _]
                            {:slept-ms 0
                             :wait-until-ms 0})
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.11:8898"
                              {:reachable? false
                               :reason :endpoint-watermark-fetch-failed}

                              "10.0.0.12:8898"
                              {:reachable? true
                               :last-applied-lsn 10
                               :source :local}

                              "10.0.0.13:8898"
                              {:reachable? true
                               :last-applied-lsn 10
                               :source :remote}

                              {:reachable? false
                               :reason :missing-endpoint}))}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :leader (:ha-role next-state)))
        (is (= 2 (:ha-authority-owner-node-id next-state)))
        (is (= 10
               (get-in next-state
                       [:ha-authority-lease :leader-last-applied-lsn])))
        (is (nil? (:ha-promotion-last-failure next-state))))
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
                          (fn [state record]
                            (swap! applied conj (:lsn record))
                            (reset! local-lsn (long (:lsn record)))
                            state)
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

(deftest ha-replica-floor-reset-required-classification-test
  (is (true? (#'dha/ha-replica-floor-reset-required?
              (ex-info
               "Request to Datalevin server failed: \"Replica floor LSN cannot move backward\""
               {:type :txlog-update-replica-floor!
                :err-data {:type :txlog/invalid-floor-provider-state
                           :replica-id 2
                           :old-lsn 50
                           :new-lsn 17}}))))
  (is (true? (#'dha/ha-replica-floor-reset-required?
              (ex-info "Replica floor LSN cannot move backward"
                       {:type :txlog/invalid-floor-provider-state
                        :replica-id 2
                        :old-lsn 50
                        :new-lsn 17}))))
  (is (false? (#'dha/ha-replica-floor-reset-required?
               (ex-info "replica floor update failed"
                        {:type :txlog-update-replica-floor!
                         :err-data {:type :txlog/io-error
                                    :replica-id 2}})))))

(deftest sync-ha-follower-batch-clears-stale-leader-replica-floor-after-reset-test
  (let [leader-endpoint "10.0.0.11:8898"
        local-lsn (atom 16)
        applied (atom [])
        calls (atom [])
        now-ms (System/currentTimeMillis)
        follower-runtime {:ha-node-id 2
                          :ha-local-endpoint "10.0.0.12:8898"
                          :ha-lease-renew-ms 1000}
        lease {:leader-node-id 1
               :leader-endpoint leader-endpoint
               :leader-last-applied-lsn 17}]
    (let [sync (with-redefs [dha/fetch-ha-leader-txlog-batch
                             (fn [_ _ endpoint from-lsn _]
                               (is (= leader-endpoint endpoint))
                               (is (= 17 from-lsn))
                               [{:lsn 17 :ops [[:put "a" "k17" "v17"]]}])
                             dha/apply-ha-follower-txlog-record!
                             (fn [state record]
                               (swap! applied conj (:lsn record))
                               (reset! local-lsn (long (:lsn record)))
                               state)
                             dha/read-ha-local-last-applied-lsn
                             (fn [_] @local-lsn)
                             dha/report-ha-replica-floor!
                             (fn [_ m endpoint applied-lsn]
                               (swap! calls conj [:report endpoint applied-lsn
                                                  (:ha-node-id m)])
                               (when (= 1 (count @calls))
                                 (throw
                                  (ex-info
                                   "Request to Datalevin server failed: \"Replica floor LSN cannot move backward\""
                                   {:type :txlog-update-replica-floor!
                                    :err-data {:type :txlog/invalid-floor-provider-state
                                               :replica-id 2
                                               :old-lsn 50
                                               :new-lsn 17}})))
                               {:ok? true})
                             dha/clear-ha-replica-floor!
                             (fn [_ m endpoint]
                               (swap! calls conj [:clear endpoint (:ha-node-id m)])
                               {:ok? true})]
                 (#'dha/sync-ha-follower-batch
                  "orders" follower-runtime lease 17 now-ms))
          state (:state sync)]
      (is (= [17] @applied))
      (is (= [[:report leader-endpoint 17 2]
              [:clear leader-endpoint 2]
              [:report leader-endpoint 17 2]]
             @calls))
      (is (= 17 (:applied-lsn sync)))
      (is (= 17 (:ha-local-last-applied-lsn state)))
      (is (= 18 (:ha-follower-next-lsn state)))
      (is (nil? (:ha-follower-last-error state))))))

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

(deftest ha-apply-follower-txlog-record-replays-schema-attr-added-in-record-test
  (let [padding (apply str (repeat 512 "bank-transfer-padding-"))
        tx-fn (interp/inter-fn [db from-id to-id amount]
                               (let [_ padding]
                                 []))
        tx-fn-query '[:find ?e .
                      :where
                      [?e :db/ident :bank/transfer]
                      [?e :db/fn ?fn]]
        leader-dir (u/tmp-dir (str "ha-leader-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}
        leader-conn (d/get-conn leader-dir nil opts)
        leader-store-v (volatile! nil)
        follower-store-v (volatile! nil)]
    (try
      (d/transact! leader-conn [{:db/ident :bank/transfer
                                 :db/fn tx-fn}])
      (vreset! leader-store-v (st/open leader-dir nil opts))
      (vreset! follower-store-v (st/open follower-dir nil opts))
      (let [record
            (->> (kv/open-tx-log-rows (.-lmdb @leader-store-v) 1 64)
                 (filter (fn [record]
                           (some (fn [[op dbi k]]
                                   (and (= op :put)
                                        (= dbi c/schema)
                                        (= k :db/fn)))
                                 (:rows record))))
                 last)]
        (is record)
        (is (seq (:rows record)))
        (is (some (fn [[op dbi]]
                    (and (= op :put)
                         (= dbi c/giants)))
                  (:rows record)))
        (let [next-state (#'dha/apply-ha-follower-txlog-record!
                          {:store @follower-store-v}
                          record)]
          (vreset! follower-store-v (:store next-state)))
        (let [tx-fn-eid (d/q tx-fn-query
                             (db/new-db @follower-store-v))]
          (is (integer? tx-fn-eid))
          (is (fn? (i/ea-first-v @follower-store-v
                                 tx-fn-eid
                                 :db/fn)))))
      (finally
        (when-let [follower-store @follower-store-v]
          (when-not (i/closed? follower-store)
            (i/close follower-store)))
        (when-let [leader-store @leader-store-v]
          (when-not (i/closed? leader-store)
            (i/close leader-store)))
        (d/close leader-conn)
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest ha-apply-follower-txlog-record-advances-max-gt-on-open-store-test
  (let [schema {:giant/key {:db/valueType :db.type/long
                            :db/unique :db.unique/identity}
                :giant/version {:db/valueType :db.type/long}
                :giant/payload {:db/valueType :db.type/string}}
        payload-1 (apply str (repeat 600 "seed-payload-"))
        payload-2 (apply str (repeat 600 "next-payload-"))
        leader-dir (u/tmp-dir (str "ha-leader-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}
        leader-conn (d/get-conn leader-dir schema opts)
        leader-store-v (volatile! nil)
        follower-store-v (volatile! nil)]
    (try
      (d/transact! leader-conn [{:db/id "giant-1"
                                 :giant/key 1
                                 :giant/version 1
                                 :giant/payload payload-1}])
      (vreset! leader-store-v (st/open leader-dir nil opts))
      (vreset! follower-store-v (st/open follower-dir schema opts))
      (let [record
            (->> (kv/open-tx-log-rows (.-lmdb @leader-store-v) 1 64)
                 (filter (fn [record]
                           (some (fn [[op dbi]]
                                   (and (= op :put)
                                        (= dbi c/giants)))
                                 (:rows record))))
                 last)]
        (is record)
        (is (seq (:rows record)))
        (let [before-max-gt (long (i/max-gt @follower-store-v))
              next-state (#'dha/apply-ha-follower-txlog-record!
                          {:store @follower-store-v}
                          record)]
          (vreset! follower-store-v (:store next-state))
          (is (> (long (i/max-gt @follower-store-v))
                 before-max-gt)))
        (is (= payload-1
               (:giant/payload
                (d/entity (db/new-db @follower-store-v)
                          [:giant/key 1]))))
        (i/load-datoms @follower-store-v
                       [(dd/datom 1001 :giant/key 2)
                        (dd/datom 1001 :giant/version 1)
                        (dd/datom 1001 :giant/payload payload-2)])
        (is (= payload-2
               (:giant/payload
                (d/entity (db/new-db @follower-store-v)
                          [:giant/key 2])))))
      (finally
        (when-let [follower-store @follower-store-v]
          (when-not (i/closed? follower-store)
            (i/close follower-store)))
        (when-let [leader-store @leader-store-v]
          (when-not (i/closed? leader-store)
            (i/close leader-store)))
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
                          (fn [state record]
                            (reset! local-lsn (long (:lsn record)))
                            state)
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

(deftest ha-renew-step-follower-sync-source-behind-triggers-snapshot-bootstrap-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        bootstrap-called (atom nil)
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
                :leader-last-applied-lsn 5
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 10
                                        :ha-local-last-applied-lsn 9))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint _ _]
                            (is (contains? #{"10.0.0.11:8898"
                                             "10.0.0.13:8898"}
                                           endpoint))
                            [])
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.11:8898"
                              {:reachable? true :last-applied-lsn 5}
                              "10.0.0.13:8898"
                              {:reachable? false
                               :reason :endpoint-watermark-fetch-failed}
                              (throw (ex-info "unexpected-endpoint"
                                              {:endpoint endpoint}))))
                          #'dha/bootstrap-ha-follower-from-snapshot
                          (fn [_ state lease source-order next-lsn bootstrap-now-ms]
                            (reset! bootstrap-called
                                    {:leader-last-applied-lsn
                                     (:leader-last-applied-lsn lease)
                                     :source-order source-order
                                     :next-lsn next-lsn})
                           {:ok? true
                             :state (assoc state
                                           :ha-local-last-applied-lsn 5
                                           :ha-follower-next-lsn 6
                                           :ha-follower-last-bootstrap-ms
                                           bootstrap-now-ms
                                           :ha-follower-bootstrap-source-endpoint
                                           "10.0.0.11:8898"
                                           :ha-follower-bootstrap-snapshot-last-applied-lsn
                                           5
                                           :ha-follower-last-error nil
                                           :ha-follower-degraded? nil)})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= {:leader-last-applied-lsn 5
                :source-order ["10.0.0.11:8898" "10.0.0.13:8898"]
                :next-lsn 10}
               @bootstrap-called))
        (is (= :follower (:ha-role next-state)))
        (is (= 5 (:ha-local-last-applied-lsn next-state)))
        (is (= 6 (:ha-follower-next-lsn next-state)))
        (is (= "10.0.0.11:8898"
               (:ha-follower-bootstrap-source-endpoint next-state)))
        (is (= 5
               (:ha-follower-bootstrap-snapshot-last-applied-lsn
                next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-follower-sync-clamps-stale-next-lsn-to-local-state-before-bootstrap-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        bootstrap-called (atom nil)
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
                :leader-last-applied-lsn 26
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 99
                                        :ha-local-last-applied-lsn 26))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint _ _]
                            (is (contains? #{"10.0.0.11:8898"
                                             "10.0.0.13:8898"}
                                           endpoint))
                            [])
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.11:8898"
                              {:reachable? true :last-applied-lsn 26}
                              "10.0.0.13:8898"
                              {:reachable? false
                               :reason :endpoint-watermark-fetch-failed}
                              (throw (ex-info "unexpected-endpoint"
                                              {:endpoint endpoint}))))
                          #'dha/bootstrap-ha-follower-from-snapshot
                          (fn [_ state lease source-order next-lsn bootstrap-now-ms]
                            (reset! bootstrap-called
                                    {:leader-last-applied-lsn
                                     (:leader-last-applied-lsn lease)
                                     :source-order source-order
                                     :next-lsn next-lsn})
                            {:ok? true
                             :state (assoc state
                                           :ha-local-last-applied-lsn 26
                                           :ha-follower-next-lsn 27
                                           :ha-follower-last-bootstrap-ms
                                           bootstrap-now-ms
                                           :ha-follower-bootstrap-source-endpoint
                                           "10.0.0.11:8898"
                                           :ha-follower-bootstrap-snapshot-last-applied-lsn
                                           26
                                           :ha-follower-last-error nil
                                           :ha-follower-degraded? nil)})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= {:leader-last-applied-lsn 26
                :source-order ["10.0.0.11:8898" "10.0.0.13:8898"]
                :next-lsn 27}
               @bootstrap-called))
        (is (= :follower (:ha-role next-state)))
        (is (= 26 (:ha-local-last-applied-lsn next-state)))
        (is (= 27 (:ha-follower-next-lsn next-state))))
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
               :leader-endpoint "10.0.0.11:8898"
               :leader-last-applied-lsn 80}]
    (with-redefs-fn
      {#'dha/fetch-ha-endpoint-watermark-lsn
       (fn [_ _ endpoint]
         (case endpoint
           "10.0.0.11:8898" {:reachable? true :last-applied-lsn 80}
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

(deftest ha-gap-fallback-source-endpoints-caps-followers-at-leader-watermark-test
  (let [m {:ha-local-endpoint "10.0.0.12:8898"
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}
                        {:node-id 3 :endpoint "10.0.0.13:8898"}
                        {:node-id 4 :endpoint "10.0.0.14:8898"}]}
        lease {:leader-node-id 1
               :leader-endpoint "10.0.0.11:8898"
               :leader-last-applied-lsn 68}]
    (with-redefs-fn
      {#'dha/fetch-ha-endpoint-watermark-lsn
       (fn [_ _ endpoint]
         (case endpoint
           "10.0.0.11:8898" {:reachable? true :last-applied-lsn 68}
           "10.0.0.13:8898" {:reachable? true :last-applied-lsn 183}
           "10.0.0.14:8898" {:reachable? true :last-applied-lsn 184}
           (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))}
      (fn []
        (is (= ["10.0.0.11:8898"
                "10.0.0.13:8898"
                "10.0.0.14:8898"]
               (#'dha/ha-gap-fallback-source-endpoints
                "orders" m lease 80)))))))

(deftest ha-source-advertised-last-applied-lsn-caps-follower-at-leader-watermark-test
  (let [m {:ha-local-endpoint "10.0.0.12:8898"
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}
                        {:node-id 3 :endpoint "10.0.0.13:8898"}]}
        lease {:leader-node-id 1
               :leader-endpoint "10.0.0.11:8898"
               :leader-last-applied-lsn 68}]
    (with-redefs-fn
      {#'dha/fetch-ha-endpoint-watermark-lsn
       (fn [_ _ endpoint]
         (case endpoint
           "10.0.0.11:8898" {:reachable? true :last-applied-lsn 68}
           "10.0.0.13:8898" {:reachable? true :last-applied-lsn 183}
           (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))}
      (fn []
        (is (= {:known? true
                :last-applied-lsn 68
                :raw-last-applied-lsn 183
                :leader-safe-lsn 68
                :watermark {:reachable? true :last-applied-lsn 183}}
               (#'dha/ha-source-advertised-last-applied-lsn
                "orders" m lease "10.0.0.13:8898")))))))

(deftest ha-watermark-transport-reuses-cached-client-test
  (#'dha/clear-ha-client-cache!)
  (let [request-clients (atom [])
        new-client-count (atom 0)
        close-count (atom 0)
        client {:id :ha-test-client
                :pool :ha-test-pool}
        m {:ha-local-endpoint "10.0.0.12:8898"
           :ha-lease-renew-ms 1000}]
    (try
      (with-redefs-fn
        {#'dha/open-ha-client
         (fn [_ _ _]
           (swap! new-client-count inc)
           client)
         #'dha/live-ha-client?
         (fn [_]
           true)
         #'dha/ha-client-request
         (fn [client' op _ _]
           (swap! request-clients conj [client' op])
           {:last-applied-lsn 42
            :ha-runtime? false})
         #'dha/close-ha-client!
         (fn [_]
           (swap! close-count inc)
           nil)}
        (fn []
          (is (= {:reachable? true
                  :last-applied-lsn 42
                  :txlog-last-applied-lsn 42
                  :ha-local-last-applied-lsn nil
                  :ha-role nil
                  :ha-runtime? false
                  :ha-control-node-leader? nil
                  :ha-control-node-state nil
                  :source :remote}
                 (#'dha/fetch-ha-endpoint-watermark-lsn
                  "orders" m "10.0.0.11:8898")))
          (is (= {:reachable? true
                  :last-applied-lsn 42
                  :txlog-last-applied-lsn 42
                  :ha-local-last-applied-lsn nil
                  :ha-role nil
                  :ha-runtime? false
                  :ha-control-node-leader? nil
                  :ha-control-node-state nil
                  :source :remote}
                 (#'dha/fetch-ha-endpoint-watermark-lsn
                  "orders" m "10.0.0.11:8898")))
          (#'dha/clear-ha-client-cache!)))
      (is (= 1 @new-client-count))
      (is (= [[client :ha-watermark]
              [client :ha-watermark]]
             @request-clients))
      (is (= 1 @close-count))
      (finally
        (#'dha/clear-ha-client-cache!)))))

(deftest ha-watermark-transport-falls-back-to-txlog-watermarks-test
  (#'dha/clear-ha-client-cache!)
  (let [request-clients (atom [])
        client {:id :ha-test-client
                :pool :ha-test-pool}]
    (try
      (with-redefs-fn
        {#'dha/open-ha-client
         (fn [_ _ _]
           client)
         #'dha/live-ha-client?
         (fn [_]
           true)
         #'dha/ha-client-request
         (fn [client' op _ _]
           (swap! request-clients conj [client' op])
           (case op
             :ha-watermark
             (throw (ex-info "Unknown message type :ha-watermark"
                             {:error :unknown-message-type}))

             :txlog-watermarks
             {:last-applied-lsn 42}))
         #'dha/close-ha-client!
         (fn [_] nil)}
        (fn []
          (is (= {:reachable? true
                  :last-applied-lsn 42
                  :txlog-last-applied-lsn 42
                  :ha-local-last-applied-lsn nil
                  :ha-role nil
                  :ha-runtime? nil
                  :ha-control-node-leader? nil
                  :ha-control-node-state nil
                  :source :remote}
                 (#'dha/fetch-ha-endpoint-watermark-lsn
                  "orders"
                  {:ha-local-endpoint "10.0.0.12:8898"
                   :ha-lease-renew-ms 1000}
                  "10.0.0.11:8898")))))
      (is (= [[client :ha-watermark]
              [client :txlog-watermarks]]
             @request-clients))
      (finally
        (#'dha/clear-ha-client-cache!)))))

(deftest ha-watermark-transport-prefers-remote-ha-local-lsn-test
  (#'dha/clear-ha-client-cache!)
  (let [client {:id :ha-test-client
                :pool :ha-test-pool}]
    (try
      (with-redefs-fn
        {#'dha/open-ha-client
         (fn [_ _ _]
           client)
         #'dha/live-ha-client?
         (fn [_]
           true)
         #'dha/ha-client-request
         (fn [_ op _ _]
           (is (= :ha-watermark op))
           {:last-applied-lsn 3
            :txlog-last-applied-lsn 37
            :ha-local-last-applied-lsn 3
            :ha-role :follower
            :ha-runtime? true
            :ha-control-node-leader? false
            :ha-control-node-state "STATE_FOLLOWER"})
         #'dha/close-ha-client!
         (fn [_] nil)}
        (fn []
          (is (= {:reachable? true
                  :last-applied-lsn 3
                  :txlog-last-applied-lsn 37
                  :ha-local-last-applied-lsn 3
                  :ha-role :follower
                  :ha-runtime? true
                  :ha-control-node-leader? false
                  :ha-control-node-state "STATE_FOLLOWER"
                  :source :remote-ha-runtime}
                 (#'dha/fetch-ha-endpoint-watermark-lsn
                  "orders"
                  {:ha-local-endpoint "10.0.0.12:8898"
                   :ha-lease-renew-ms 1000}
                  "10.0.0.11:8898")))))
      (finally
        (#'dha/clear-ha-client-cache!)))))

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
                          (fn [state record]
                            (reset! local-lsn (long (:lsn record)))
                            state)
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
        (is (= watermark-lsn (:payload-last-applied-lsn meta)))
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
        (is (= watermark-lsn (:payload-last-applied-lsn meta)))
        (is (> watermark-lsn snapshot-lsn)))
      (finally
        (i/close store)
        (u/delete-files dir)))))

(deftest copy-response-meta-prefers-lmdb-payload-lsn-over-runtime-watermark-test
  (let [dir (u/tmp-dir (str "ha-copy-meta-payload-" (UUID/randomUUID)))
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
            runtime-lsn
            (#'dha/read-ha-local-last-applied-lsn
             {:store store :ha-role :leader})
            _ (is (> runtime-lsn snapshot-lsn))
            meta (with-redefs [dha/read-ha-snapshot-payload-lsn
                               (fn [_] snapshot-lsn)]
                   (#'srv/copy-response-meta
                    "orders" store {:compact? false}))]
        (is (= runtime-lsn
               (#'dha/read-ha-local-last-applied-lsn
                {:store store :ha-role :leader})))
        (is (= snapshot-lsn (:payload-last-applied-lsn meta)))
        (is (not= runtime-lsn (:payload-last-applied-lsn meta))))
      (finally
        (i/close store)
        (u/delete-files dir)))))

(deftest persist-ha-local-applied-lsn-bypasses-local-txlog-test
  (let [dir (u/tmp-dir (str "ha-local-applied-persist-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :wal? true})]
    (try
      (let [kv (.-lmdb store)
            _ (i/open-dbi kv "a")
            _ (i/transact-kv kv [[:put "a" "k1" "v1"]])
            watermark-before
            (long (or (:last-applied-lsn (i/txlog-watermarks kv)) 0))
            _ (#'dha/persist-ha-local-applied-lsn!
               {:store store}
               watermark-before)
            watermark-after
            (long (or (:last-applied-lsn (i/txlog-watermarks kv)) 0))
            persisted-lsn
            (long (or (i/get-value kv c/kv-info
                                   c/ha-local-applied-lsn
                                   :keyword :data)
                      0))]
        (is (pos? watermark-before))
        (is (= watermark-before persisted-lsn))
        (is (= watermark-before watermark-after)))
      (finally
        (i/close store)
        (u/delete-files dir)))))

(deftest validate-ha-snapshot-copy-prefers-payload-lsn-test
  (let [db-identity (str "db-" (UUID/randomUUID))
        manifest (#'dha/validate-ha-snapshot-copy!
                  "orders"
                  {:ha-db-identity db-identity}
                  "10.0.0.13:8898"
                  "/tmp/ignored"
                  {:db-name "orders"
                   :db-identity db-identity
                   :snapshot-last-applied-lsn 7
                   :payload-last-applied-lsn 11}
                  11)]
    (is (= "orders" (:db-name manifest)))
    (is (= db-identity (:db-identity manifest)))
    (is (= 7 (:snapshot-last-applied-lsn manifest)))
    (is (= 11 (:payload-last-applied-lsn manifest)))))

(deftest fetch-ha-endpoint-snapshot-copy-unpins-backup-pin-best-effort-test
  (let [opened   (atom nil)
        unpinned (atom [])
        closed   (atom [])]
    (with-redefs-fn
      {#'dha/open-ha-snapshot-remote-store!
       (fn [uri opts]
         (reset! opened {:uri uri :opts opts})
         ::remote-store)
       #'dha/copy-ha-remote-store!
       (fn [store dest compact?]
         (is (= ::remote-store store))
         (is (= "/tmp/ignored" dest))
         (is (false? compact?))
         {:db-name "orders"
          :backup-pin {:pin-id "backup-copy/test-pin"}})
       #'dha/unpin-ha-remote-store-backup-floor!
       (fn [store pin-id]
         (swap! unpinned conj [store pin-id])
         (throw (ex-info "cleanup failed" {:pin-id pin-id})))
       #'dha/close-ha-snapshot-remote-store!
       (fn [store]
         (swap! closed conj store))}
      (fn []
        (is (= {:copy-meta {:db-name "orders"
                            :backup-pin
                            {:pin-id "backup-copy/test-pin"}}}
               (#'dha/fetch-ha-endpoint-snapshot-copy!
                "orders"
                {:ha-lease-renew-ms 1000}
                "10.0.0.11:8898"
                "/tmp/ignored")))
        (is (= [[::remote-store "backup-copy/test-pin"]]
               @unpinned))
        (is (= [::remote-store] @closed))
        (is (= "dtlv://datalevin:datalevin@10.0.0.11:8898/orders"
               (:uri @opened)))))))

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

(deftest read-ha-local-last-applied-lsn-falls-back-to-cached-state-when-store-closed-test
  (let [dir (u/tmp-dir (str "ha-closed-store-fallback-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :db-identity (str "db-" (UUID/randomUUID))
                                :wal? true})
        state {:store store
               :ha-local-last-applied-lsn 0}]
    (try
      (#'dha/persist-ha-local-applied-lsn! state 7)
      (is (= 7
             (dha/read-ha-local-last-applied-lsn state)))
      (i/close store)
      (let [closed-state (assoc state :ha-local-last-applied-lsn 9)]
        (is (= 9
               (dha/read-ha-local-last-applied-lsn closed-state)))
        (is (= 11
               (#'dha/persist-ha-local-applied-lsn! closed-state 11))))
      (finally
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest read-ha-local-last-applied-lsn-prefers-follower-local-truth-test
  (let [dir (u/tmp-dir (str "ha-follower-watermark-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :db-identity (str "db-" (UUID/randomUUID))
                                :wal? true})
        lmdb (.-lmdb ^Store store)]
    (try
      (i/open-dbi lmdb "a")
      (i/transact-kv lmdb [[:put "a" "k1" "v1"]])
      (let [initial-watermark-lsn (long (or (:last-applied-lsn
                                             (i/txlog-watermarks lmdb))
                                            0))]
        (is (pos? initial-watermark-lsn))
        (#'dha/persist-ha-local-applied-lsn!
         {:store store}
         (+ initial-watermark-lsn 7))
        (let [watermark-lsn (long (or (:last-applied-lsn
                                       (i/txlog-watermarks lmdb))
                                      0))]
          ;; Persisting the follower-applied marker itself appends to the WAL,
          ;; so the local watermark advances as part of the metadata write.
          (is (> watermark-lsn initial-watermark-lsn))
          (is (= watermark-lsn
               (dha/read-ha-local-last-applied-lsn
                {:store store
                 :ha-role :follower
                 :ha-local-last-applied-lsn (+ watermark-lsn 11)})))
          (is (= (+ initial-watermark-lsn 7)
                 (dha/read-ha-local-last-applied-lsn
                  {:store store
                   :ha-role :leader})))))
      (finally
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest stop-ha-runtime-persists-authoritative-local-applied-lsn-for-rejoin-test
  (let [dir (u/tmp-dir (str "ha-stop-runtime-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :db-identity (str "db-" (UUID/randomUUID))
                                :wal? true})
        lmdb (.-lmdb ^Store store)]
    (try
      (i/open-dbi lmdb "a")
      (i/transact-kv lmdb [[:put "a" "k1" "v1"]])
      (let [watermark-lsn (long (or (:last-applied-lsn
                                     (i/txlog-watermarks lmdb))
                                    0))
            authoritative-lsn (long (max 1 (dec watermark-lsn)))]
        (is (pos? watermark-lsn))
        (is (> watermark-lsn authoritative-lsn))
        (with-redefs [srv/*stop-ha-renew-loop-fn* (fn [_] nil)
                      srv/*stop-ha-authority-fn* (fn [_ _] nil)]
          (#'srv/stop-ha-runtime
           "orders"
           {:store store
            :ha-role :leader
            :ha-node-id 1
            :ha-local-last-applied-lsn watermark-lsn
            :ha-authority-lease {:leader-last-applied-lsn authoritative-lsn}}))
        (is (= authoritative-lsn
               (i/get-value lmdb c/kv-info
                            c/ha-local-applied-lsn
                            :keyword :data))))
      (finally
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest sync-ha-follower-batch-reopens-closed-local-store-test
  (let [leader-endpoint "10.0.0.11:8898"
        leader-dir (u/tmp-dir (str "ha-leader-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}
        follower-store-v (volatile! nil)
        recovered-store-v (volatile! nil)]
    (try
      (let [leader-store (st/open leader-dir nil opts)]
        (try
          (let [leader-kv (.-lmdb leader-store)
                follower-store (st/open follower-dir nil opts)
                follower-kv (.-lmdb follower-store)
                follower-db (db/new-db follower-store)
                follower-runtime {:store follower-store
                                  :dt-db follower-db
                                  :ha-node-id 2
                                  :ha-local-endpoint "10.0.0.12:8898"
                                  :ha-lease-renew-ms 1000}
                lease {:leader-node-id 1
                       :leader-endpoint leader-endpoint
                       :leader-last-applied-lsn 2}
                now-ms (System/currentTimeMillis)
                leader-db (kv/wrap-lmdb leader-kv)]
            (i/open-dbi leader-kv "a")
            (i/open-dbi follower-kv "a")
            (i/transact-kv leader-kv
                           [[:put "a" "k1" "v1"]
                            [:put "a" "k2" "v2"]])
            (vreset! follower-store-v follower-store)
            (i/close follower-store)
            (with-redefs [dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint from-lsn upto-lsn]
                            (is (= leader-endpoint endpoint))
                            (kv/open-tx-log leader-db from-lsn upto-lsn))
                          dha/report-ha-replica-floor!
                          (fn [_ _ endpoint _]
                            (is (= leader-endpoint endpoint))
                            {:ok? true})]
              (let [sync (#'dha/sync-ha-follower-batch
                          "orders" follower-runtime lease 1 now-ms)
                    state (:state sync)
                    recovered-store (:store state)
                    recovered-kv (.-lmdb recovered-store)]
                (vreset! recovered-store-v recovered-store)
                (is (seq (:records sync)))
                (is (instance? Store recovered-store))
                (is (not (i/closed? recovered-store)))
                (is (pos? (long (:ha-local-last-applied-lsn state))))
                (is (= (inc (long (:ha-local-last-applied-lsn state)))
                       (:ha-follower-next-lsn state)))
                (is (= "v1" (i/get-value recovered-kv "a" "k1")))
                (is (= "v2" (i/get-value recovered-kv "a" "k2"))))))
          (finally
            (when-not (i/closed? leader-store)
              (i/close leader-store)))))
      (finally
        (when-let [store @recovered-store-v]
          (when-not (identical? store @follower-store-v)
            (when-not (i/closed? store)
              (i/close store))))
        (when-let [store @follower-store-v]
          (when-not (i/closed? store)
            (i/close store)))
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest sync-ha-follower-batch-clears-cached-miss-before-publishing-dt-db-test
  (let [leader-endpoint "10.0.0.11:8898"
        leader-dir (u/tmp-dir (str "ha-leader-cache-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-cache-" (UUID/randomUUID)))
        opts {:db-name "ha-e2e"
              :db-identity "db-1"
              :wal? true}]
    (try
      (let [leader-conn (d/create-conn leader-dir e2e-ha-schema opts)
            follower-conn (d/create-conn follower-dir e2e-ha-schema opts)
            leader-store-v (volatile! nil)
            follower-store-v (volatile! nil)]
        (try
          (doseq [conn [leader-conn follower-conn]]
            (d/transact! conn [{:drill/key "seed"
                                :drill/value "v1"}]))
          (is (nil? (d/q e2e-ha-value-query
                         @follower-conn
                         "post-failover")))
          (d/transact! leader-conn [{:drill/key "post-failover"
                                     :drill/value "v2"}])
          (vreset! leader-store-v (st/open leader-dir nil opts))
          (vreset! follower-store-v (st/open follower-dir nil opts))
          (let [leader-store @leader-store-v
                follower-store @follower-store-v
                lease {:leader-node-id 1
                       :leader-endpoint leader-endpoint
                       :leader-last-applied-lsn 2}
                now-ms (System/currentTimeMillis)]
            (with-redefs [dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint from-lsn upto-lsn]
                            (is (= leader-endpoint endpoint))
                            (kv/open-tx-log-rows
                             (kv/wrap-lmdb (.-lmdb leader-store))
                             from-lsn
                             upto-lsn))
                          dha/report-ha-replica-floor!
                          (fn [_ _ endpoint _]
                            (is (= leader-endpoint endpoint))
                            {:ok? true})]
              (let [sync (#'dha/sync-ha-follower-batch
                          "ha-e2e"
                          {:store follower-store
                           :dt-db @follower-conn
                           :ha-node-id 2
                           :ha-local-endpoint "10.0.0.12:8898"
                           :ha-lease-renew-ms 1000}
                          lease
                          2
                          now-ms)
                    state (:state sync)]
                (is (= "v2"
                       (d/q e2e-ha-value-query
                            (:dt-db state)
                            "post-failover"))))))
          (finally
            (when-let [store @follower-store-v]
              (when-not (i/closed? store)
                (i/close store)))
            (when-let [store @leader-store-v]
              (when-not (i/closed? store)
                (i/close store)))
            (d/close leader-conn)
            (d/close follower-conn))))
      (finally
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest e2e-local-read-value-prefers-published-dt-db-test
  (let [published-dir (u/tmp-dir (str "ha-published-db-" (UUID/randomUUID)))
        stale-dir (u/tmp-dir (str "ha-stale-store-" (UUID/randomUUID)))
        opts {:db-name "ha-e2e"
              :db-identity "db-1"
              :wal? true}]
    (try
      (let [published-conn (d/create-conn published-dir e2e-ha-schema opts)
            stale-store (st/open stale-dir e2e-ha-schema opts)]
        (try
          (d/transact! published-conn [{:drill/key "post-failover"
                                        :drill/value "v2"}])
          (let [server (fake-server-with-db-state
                        "ha-e2e"
                        {:dt-db @published-conn
                         :store stale-store})
                ctx {:servers {1 server}
                     :db-name "ha-e2e"}]
            (is (= "v2"
                   (e2e-local-read-value ctx 1 "post-failover"))))
          (finally
            (d/close published-conn)
            (when-not (i/closed? stale-store)
              (i/close stale-store)))))
      (finally
        (u/delete-files published-dir)
        (u/delete-files stale-dir)))))

(deftest apply-ha-follower-txlog-record-replays-unique-identity-register-updates-test
  (let [leader-dir (u/tmp-dir (str "ha-leader-register-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-register-" (UUID/randomUUID)))
        schema {:register/key {:db/valueType :db.type/long
                               :db/unique :db.unique/identity}
                :register/value {:db/valueType :db.type/long}}
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}]
    (try
      (let [leader-conn (d/create-conn leader-dir schema opts)
            follower-store (st/open follower-dir schema opts)]
        (try
          (d/transact! leader-conn
                       (mapv (fn [k]
                               {:db/id (str "register-" k)
                                :register/key (long k)
                                :register/value 0})
                             (range 4)))
          (d/transact! leader-conn [{:register/key 0 :register/value 1}])
          (d/transact! leader-conn [{:register/key 1 :register/value 2}])
          (d/transact! leader-conn [{:register/key 0 :register/value 3}])
          (d/transact! leader-conn [[:db/cas
                                     [:register/key 1]
                                     :register/value
                                     2
                                     4]])
          (let [leader-store (:store @leader-conn)
                leader-kv (.-lmdb leader-store)
                records (vec (kv/open-tx-log-rows leader-kv 1 nil))
                follower-runtime {:store follower-store
                                  :dt-db (db/new-db follower-store)
                                  :ha-node-id 2}
                next-state (reduce #'dha/apply-ha-follower-txlog-record!
                                   follower-runtime
                                   records)
                rows (d/q '[:find ?key ?value
                            :where
                            [?e :register/key ?key]
                            [?e :register/value ?value]]
                          (db/new-db (:store next-state)))
                values (into {}
                             (map (fn [[k v]]
                                    [(long k) (long v)]))
                             rows)]
            (is (= {0 3, 1 4, 2 0, 3 0} values)))
          (finally
            (d/close leader-conn)
            (when-not (i/closed? follower-store)
              (i/close follower-store)))))
      (finally
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest apply-ha-follower-txlog-record-bypasses-local-txlog-test
  (let [leader-dir (u/tmp-dir (str "ha-leader-replay-watermark-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-replay-watermark-" (UUID/randomUUID)))
        schema {:register/key {:db/valueType :db.type/long
                               :db/unique :db.unique/identity}
                :register/value {:db/valueType :db.type/long}}
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}]
    (try
      (let [leader-conn (d/create-conn leader-dir schema opts)
            follower-store (st/open follower-dir schema opts)]
        (try
          (d/transact! leader-conn
                       [{:db/id "register-0"
                         :register/key 0
                         :register/value 0}])
          (d/transact! leader-conn [{:register/key 0 :register/value 7}])
          (let [leader-store (:store @leader-conn)
                leader-kv (.-lmdb leader-store)
                follower-kv (.-lmdb follower-store)
                records (vec (kv/open-tx-log-rows leader-kv 1 nil))
                follower-runtime {:store follower-store
                                  :dt-db (db/new-db follower-store)
                                  :ha-node-id 2}
                local-records-before (vec (kv/open-tx-log-rows follower-kv 1 nil))
                next-state (reduce #'dha/apply-ha-follower-txlog-record!
                                   follower-runtime
                                   records)
                follower-store-after (:store next-state)
                follower-kv-after (.-lmdb ^Store follower-store-after)
                local-records-after
                (vec (kv/open-tx-log-rows follower-kv-after 1 nil))
                payload-lsn
                (long (or (i/get-value follower-kv-after
                                       c/kv-info
                                       c/wal-local-payload-lsn
                                       :keyword :data)
                          0))
                last-record-lsn (long (:lsn (peek records)))
                rows (d/q '[:find ?key ?value
                            :where
                            [?e :register/key ?key]
                            [?e :register/value ?value]]
                          (db/new-db follower-store-after))]
            (is (seq records))
            (is (= {0 7}
                   (into {}
                         (map (fn [[k v]]
                                [(long k) (long v)]))
                         rows)))
            (is (= (mapv :lsn local-records-before)
                   (mapv :lsn local-records-after)))
            (is (= last-record-lsn payload-lsn)))
          (finally
            (d/close leader-conn)
            (when-not (i/closed? follower-store)
              (i/close follower-store)))))
      (finally
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

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

(deftest ha-renew-step-follower-gap-bootstraps-from-copy-payload-newer-than-snapshot-floor-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-dir (u/tmp-dir (str "ha-local-copy-payload-" (UUID/randomUUID)))
        source-dir (u/tmp-dir (str "ha-source-copy-payload-" (UUID/randomUUID)))
        db-identity (:ha-db-identity runtime)
        local-store (st/open local-dir nil {:db-name "orders"
                                            :db-identity db-identity
                                            :wal? true})
        source-store (st/open source-dir nil {:db-name "orders"
                                              :db-identity db-identity
                                              :wal? true})
        snapshot-lsn* (atom nil)
        payload-lsn* (atom nil)
        reported (atom nil)
        next-state (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [local-kv (.-lmdb local-store)
            source-kv (.-lmdb source-store)
            _ (i/open-dbi source-kv "a")
            _ (i/transact-kv source-kv [[:put "a" "k1" "v1"]])
            _ (i/create-snapshot! source-kv)
            snapshot-lsn
            (long (or (i/get-value source-kv c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (i/transact-kv source-kv [[:put "a" "k2" "v2"]])
            payload-lsn
            (long (or (:last-applied-lsn (i/txlog-watermarks source-kv)) 0))
            _ (reset! snapshot-lsn* snapshot-lsn)
            _ (reset! payload-lsn* payload-lsn)
            authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity db-identity
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn (inc payload-lsn)
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
               [{:lsn (inc payload-lsn)
                 :ops [[:put "a" "k3" "v3"]]}]
               "10.0.0.13:8898"
               [{:lsn (inc payload-lsn)
                 :ops [[:put "a" "k3" "v3"]]}]
               []))
           #'dha/fetch-ha-endpoint-watermark-lsn
           (fn [_ _ endpoint]
             (case endpoint
               "10.0.0.13:8898"
               {:reachable? true :last-applied-lsn payload-lsn}
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
                       :snapshot-last-applied-lsn snapshot-lsn
                       :payload-last-applied-lsn payload-lsn)}
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
        (is (= (inc @payload-lsn*) (:ha-local-last-applied-lsn state)))
        (is (= (inc @payload-lsn*)
               (i/get-value next-kv c/kv-info
                            c/ha-local-applied-lsn
                            :keyword :data)))
        (is (= (+ 2 @payload-lsn*) (:ha-follower-next-lsn state)))
        (is (= 1 (:ha-follower-last-batch-size state)))
        (is (= "10.0.0.11:8898" (:ha-follower-source-endpoint state)))
        (is (= "10.0.0.13:8898"
               (:ha-follower-bootstrap-source-endpoint state)))
        (is (= @payload-lsn*
               (:ha-follower-bootstrap-snapshot-last-applied-lsn state)))
        (is (> @payload-lsn* @snapshot-lsn*))
        (is (nil? (:ha-follower-degraded? state)))
        (is (= "v1" (i/get-value next-kv "a" "k1")))
        (is (= "v2" (i/get-value next-kv "a" "k2")))
        (is (= "v3" (i/get-value next-kv "a" "k3")))
        (is (= {:db-name "orders"
                :ha-node-id 2
                :leader-endpoint "10.0.0.11:8898"
                :applied-lsn (inc @payload-lsn*)}
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
