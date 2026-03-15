(ns datalevin.server-ha-test-support
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as cl]
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
   [datalevin.udf :as udf]
   [datalevin.util :as u]
   [datalevin.validate :as vld]
   [taoensso.timbre :as log])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.server Server]
   [java.net ConnectException]
   [java.nio.channels ClosedChannelException]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]
   [java.util.concurrent.atomic AtomicBoolean]))

(defn quiet-server-ha-logs-fixture
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

(defn valid-ha-opts
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
    :ha-demotion-drain-ms 1000
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

(def ha-runtime-option-keys
  [:ha-mode
   :db-identity
   :ha-node-id
   :ha-members
   :ha-lease-renew-ms
   :ha-lease-timeout-ms
   :ha-promotion-base-delay-ms
   :ha-promotion-rank-delay-ms
   :ha-max-promotion-lag-lsn
   :ha-client-credentials
   :ha-demotion-drain-ms
   :ha-fencing-hook
   :ha-clock-skew-budget-ms
   :ha-clock-skew-hook
   :ha-control-plane])

(defn resolved-ha-runtime-opts
  [root db-name ha-opts]
  (-> (#'srv/with-default-ha-control-raft-dir root db-name ha-opts)
      (select-keys ha-runtime-option-keys)))

(def e2e-ha-schema
  {:drill/key {:db/valueType :db.type/string
               :db/unique :db.unique/identity}
   :drill/value {:db/valueType :db.type/string}})

(def e2e-ha-value-query
  '[:find ?v .
    :in $ ?k
    :where
    [?e :drill/key ?k]
    [?e :drill/value ?v]])

(def e2e-ha-conn-client-opts
  {:pool-size 1
   :time-out 60000})

(defn now-ms
  []
  (System/currentTimeMillis))

(defn random-port-candidate
  []
  (+ 35000 (rand-int 20000)))

(defn e2e-admin-uri
  [endpoint]
  (str "dtlv://" c/default-username ":" c/default-password "@" endpoint))

(defn e2e-db-uri
  [endpoint db-name]
  (str (e2e-admin-uri endpoint) "/" db-name))

(defn e2e-make-nodes
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

(defn e2e-promotable-voters
  [nodes]
  (mapv (fn [{:keys [node-id peer-id]}]
          {:peer-id peer-id
           :ha-node-id node-id
           :promotable? true})
        nodes))

(defn e2e-base-ha-opts
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

(defn e2e-node-ha-opts
  [base-opts node]
  (-> base-opts
      (assoc :ha-node-id (:node-id node))
      (assoc-in [:ha-control-plane :local-peer-id] (:peer-id node))))

(declare e2e-effective-local-lsn)
(declare e2e-write-value!)
(declare e2e-verify-value-on-nodes!)

(defn start-e2e-ha-server!
  [node]
  (u/create-dirs (:root node))
  (let [server (binding [c/*db-background-sampling?* false]
                 (srv/create {:port (:port node)
                              :root (:root node)}))]
    (binding [c/*db-background-sampling?* false]
      (srv/start server))
    server))

(defn open-e2e-ha-conn!
  ([node db-name opts]
   (open-e2e-ha-conn! node db-name e2e-ha-schema opts))
  ([node db-name schema opts]
   (d/create-conn (e2e-db-uri (:endpoint node) db-name)
                  schema
                  (assoc opts :client-opts e2e-ha-conn-client-opts))))

(defn safe-close-e2e-ha-conn!
  [conn]
  (when conn
    (try
      (d/close conn)
      (catch Throwable _ nil))))

(defn safe-stop-e2e-ha-server!
  [server]
  (when server
    (try
      (srv/stop server)
      (catch Throwable _ nil))))

(defn safe-delete-dir!
  [path]
  (when path
    (try
      (u/delete-files path)
      (catch Throwable _ nil))))

(defn e2e-db-state
  [server db-name]
  (when server
    (get (.-dbs ^Server server) db-name)))

(defn e2e-node-diagnostics
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
                     :ha-follower-last-batch-size
                     :ha-follower-last-batch-records
                     :ha-follower-last-apply-readback
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
        :store-open?
        (let [store (:store db-state)]
          (cond
            (nil? store) false
            (instance? Store store) (not (i/closed? store))
            :else (not (i/closed-kv? store))))
        :store-class (some-> (:store db-state) class str)
        :dt-db? (boolean (:dt-db db-state))
        :ha-follower-next-lsn (:ha-follower-next-lsn db-state)
        :ha-follower-last-bootstrap-ms
        (:ha-follower-last-bootstrap-ms db-state)
        :ha-follower-bootstrap-source-endpoint
        (:ha-follower-bootstrap-source-endpoint db-state)
        :ha-follower-bootstrap-snapshot-last-applied-lsn
        (:ha-follower-bootstrap-snapshot-last-applied-lsn db-state)
        :store-ha-opts
        (some-> store-opts
                (select-keys [:ha-mode
                              :db-identity
                              :ha-node-id
                              :ha-members
                              :ha-control-plane]))}))
    {:status :missing-db-state}))

(defn e2e-node-diagnostics-by-node
  [servers db-name live-node-ids]
  (into {}
        (map (fn [node-id]
               [node-id
                (e2e-node-diagnostics (get servers node-id) db-name)]))
        live-node-ids))

(defn e2e-probe-write-admission
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

(defn e2e-probe-snapshot
  [servers db-name live-node-ids]
  (into {}
        (map (fn [node-id]
               [node-id
                (e2e-probe-write-admission (get servers node-id) db-name)]))
        live-node-ids))

(defn e2e-wait-for-single-leader!
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

(defn e2e-read-value
  [conn key]
  (d/q e2e-ha-value-query @conn key))

(defn e2e-remote-read-value
  [ctx node-id key]
  (when-let [conn (get-in ctx [:conns node-id])]
    (try
      (e2e-read-value conn key)
      (catch Throwable _
        nil))))

(defn e2e-store-open?
  [store]
  (cond
    (nil? store) false
    (instance? Store store) (not (i/closed? store))
    :else (not (i/closed-kv? store))))

(defn e2e-local-read-value
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

(defn e2e-local-raw-value-probe
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

(defn e2e-local-watermarks
  [ctx node-id]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    (let [store (:store db-state)
          lmdb (if (instance? Store store)
                 (.-lmdb ^Store store)
                 store)]
      (when (e2e-store-open? lmdb)
        (try
          (assoc (kv/txlog-watermarks lmdb)
                 :wal-local-payload-lsn
                 (long (or (i/get-value lmdb c/kv-info
                                        c/wal-local-payload-lsn
                                        :keyword :data)
                           0)))
          (catch Throwable _
            nil))))))

(defn e2e-local-open-tx-log-rows
  [ctx node-id from-lsn upto-lsn]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    (let [store (:store db-state)
          lmdb  (if (instance? Store store)
                  (.-lmdb ^Store store)
                  store)]
      (when (e2e-store-open? lmdb)
        (try
          (kv/open-tx-log-rows lmdb from-lsn upto-lsn)
          (catch Throwable _
            nil))))))

(defn e2e-local-eav-entity-rows
  [ctx node-id eid]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    (let [store (:store db-state)
          lmdb  (if (instance? Store store)
                  (.-lmdb ^Store store)
                  store)]
      (when (e2e-store-open? lmdb)
        (try
          (vec (i/get-list lmdb c/eav eid :id :avg))
          (catch Throwable _
            nil))))))

(defn e2e-local-dbi-range
  [ctx node-id dbi-name kt vt]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    (let [store (:store db-state)
          lmdb  (if (instance? Store store)
                  (.-lmdb ^Store store)
                  store)]
      (when (e2e-store-open? lmdb)
        (try
          (vec (take 32 (i/get-range lmdb dbi-name [:all] kt vt)))
          (catch Throwable _
            nil))))))

(defn e2e-fresh-store-read-value
  [ctx node-id key]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    (let [store (:store db-state)]
      (when (and (instance? Store store)
                 (not (e2e-store-open? store)))
        (try
          (let [store-dir (i/dir store)
                store-opts (i/opts store)
                fresh-store (st/open store-dir nil store-opts)]
            (try
              (d/q e2e-ha-value-query
                   (db/new-db fresh-store)
                   key)
              (catch Throwable _
                nil)
              (finally
                (i/close fresh-store))))
          (catch Throwable _
            nil))))))

(defn e2e-remote-open-tx-log-rows
  [ctx node-id from-lsn upto-lsn]
  (when-let [conn (get-in ctx [:conns node-id])]
    (try
      (vec (i/open-tx-log (.-store ^DB @conn) from-lsn upto-lsn))
      (catch Throwable _
        nil))))

(defn e2e-local-ha-persisted-lsn
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

(defn e2e-effective-local-lsn
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

(defn e2e-retention-state-on-node
  [ctx node-id]
  (when-let [db-state (e2e-db-state (get-in ctx [:servers node-id])
                                    (:db-name ctx))]
    (let [store (:store db-state)
          lmdb (if (instance? Store store)
                 (.-lmdb ^Store store)
                 store)]
      (i/txlog-retention-state lmdb))))

(defn e2e-create-snapshot-on-node!
  [ctx node-id]
  (let [store (-> (get-in ctx [:conns node-id]) deref .-store)
        result (i/create-snapshot! store)]
    (when-not (:ok? result)
      (throw (ex-info "Failed to create e2e HA snapshot"
                      {:node-id node-id
                       :result result})))
    result))

(defn e2e-create-snapshots-on-nodes!
  [ctx node-ids]
  (into {}
        (map (fn [node-id]
               [node-id (e2e-create-snapshot-on-node! ctx node-id)]))
        node-ids))

(defn e2e-gc-txlog-segments-on-node!
  [ctx node-id]
  (let [store (-> (get-in ctx [:conns node-id]) deref .-store)
        result (i/gc-txlog-segments! store)]
    (when-not (:ok? result)
      (throw (ex-info "Failed to GC e2e HA WAL segments"
                      {:node-id node-id
                       :result result})))
    result))

(defn e2e-write-series-with-rolls!
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

(defn e2e-clock-skew-script-path
  []
  (.getCanonicalPath (io/file "." "script" "ha" "clock-skew-file.sh")))

(defn e2e-fence-script-path
  []
  (.getCanonicalPath (io/file "." "script" "ha" "fence-log.sh")))

(defn e2e-fence-hook-config
  [log-file exit-code]
  {:cmd [(e2e-fence-script-path) log-file (str exit-code)]
   :timeout-ms 1000
   :retries 0
   :retry-delay-ms 0})

(defn e2e-parse-fence-log-line
  [line]
  (let [parts (s/split line #"," 9)
        [timestamp-ms db-name fence-op-id observed-term candidate-term new-node-id
         old-node-id old-leader-endpoint fence-shared-op-id]
        (if (= 6 (count parts))
          (let [[ts op observed candidate new-id old-id] parts]
            [ts nil op observed candidate new-id old-id nil nil])
          (into (vec parts)
                (repeat (max 0 (- 9 (count parts))) nil)))]
    {:timestamp-ms timestamp-ms
     :db-name db-name
     :fence-op-id fence-op-id
     :observed-term observed-term
     :candidate-term candidate-term
     :new-node-id new-node-id
     :old-node-id old-node-id
     :old-leader-endpoint old-leader-endpoint
     :fence-shared-op-id fence-shared-op-id}))

(defn e2e-read-fence-log
  [path]
  (let [f (io/file path)]
    (if (.exists f)
      (->> (line-seq (io/reader f))
           (remove s/blank?)
           (mapv e2e-parse-fence-log-line))
      [])))

(defn e2e-clock-skew-hook-config
  [state-dir]
  {:cmd [(e2e-clock-skew-script-path) state-dir]
   :timeout-ms 1000
   :retries 0
   :retry-delay-ms 0})

(defn e2e-clock-skew-state-file
  [state-dir node-id]
  (str state-dir u/+separator+ "clock-skew-" node-id ".txt"))

(defn e2e-write-clock-skew-ms!
  [state-dir node-id skew-ms]
  (u/create-dirs state-dir)
  (spit (e2e-clock-skew-state-file state-dir node-id) (str (long skew-ms))))

(defn e2e-clock-skew-state
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

(defn e2e-follower-state
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

(defn e2e-verify-value-on-nodes!
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
                           :fresh-store-node-values
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (e2e-fresh-store-read-value
                                          ctx
                                          node-id
                                          key)]))
                                 live-node-ids)
                           :raw-value-probes
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (e2e-local-raw-value-probe
                                          ctx
                                          node-id
                                          key)]))
                                 live-node-ids)
                           :node-watermarks
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (select-keys
                                          (or (e2e-local-watermarks ctx node-id)
                                              {})
                                          [:last-applied-lsn
                                           :last-appended-lsn
                                           :last-durable-lsn
                                           :durable-lsn
                                           :replica-floor-lsn
                                           :wal-local-payload-lsn])]))
                                 live-node-ids)
                           :node-txlog-rows
                           (into {}
                                 (map (fn [node-id]
                                        (let [watermarks
                                              (or (e2e-local-watermarks
                                                   ctx
                                                   node-id)
                                                  {})
                                              from-lsn
                                              (long (max 1
                                                         (dec
                                                          (long
                                                           (or (:last-applied-lsn
                                                                watermarks)
                                                               (:wal-local-payload-lsn
                                                                watermarks)
                                                               1)))))]
                                          [node-id
                                           (e2e-local-open-tx-log-rows
                                            ctx
                                            node-id
                                            from-lsn
                                            nil)])))
                                 live-node-ids)
                           :node-eav-entity-2
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (e2e-local-eav-entity-rows
                                          ctx
                                          node-id
                                          2)]))
                                 live-node-ids)
                           :node-eav-range
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (e2e-local-dbi-range
                                          ctx
                                          node-id
                                          c/eav
                                          :id
                                          :avg)]))
                                 live-node-ids)
                           :node-ave-range
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (e2e-local-dbi-range
                                          ctx
                                          node-id
                                          c/ave
                                          :avg
                                          :id)]))
                                 live-node-ids)
                           :node-meta-range
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (e2e-local-dbi-range
                                          ctx
                                          node-id
                                          c/meta
                                          :attr
                                          :data)]))
                                 live-node-ids)
                           :remote-txlog-rows
                           (into {}
                                 (map (fn [node-id]
                                        [node-id
                                         (e2e-remote-open-tx-log-rows
                                          ctx
                                          node-id
                                          3
                                          nil)]))
                                 live-node-ids)
                           :node-diagnostics
                           (e2e-node-diagnostics-by-node
                            (:servers ctx)
                            (:db-name ctx)
                            live-node-ids)})))))))

(defn e2e-write-value!
  [conns leader-id key value]
  (let [tx-data [{:drill/key key
                  :drill/value value}]
        deadline-ms (+ (now-ms) 15000)]
    (loop [target-node-id leader-id]
      (let [conn (get conns target-node-id)]
        (when-not conn
          (throw (ex-info "Missing E2E HA connection for write target"
                          {:leader-id leader-id
                           :target-node-id target-node-id
                           :key key
                           :value value})))
        (let [attempt (try
                        {:ok? true
                         :result (d/transact! conn tx-data)}
                        (catch Throwable e
                          {:ok? false
                           :error e}))]
          (if (:ok? attempt)
            (:result attempt)
            (let [e (:error attempt)
                  data (ex-data e)
                  err-data (:err-data data)
                  bad-rslot? (or (s/includes? (or (:server-message data) "")
                                              "MDB_BAD_RSLOT")
                                 (s/includes? (or (:cause err-data) "")
                                              "MDB_BAD_RSLOT"))
                  retryable? (and (map? err-data)
                                  (= :ha/write-rejected (:error err-data))
                                  (true? (:retryable? err-data)))
                  next-node-id (or (:ha-authoritative-leader-node-id err-data)
                                   target-node-id)]
              (if (and (or retryable? bad-rslot?)
                       (< (now-ms) deadline-ms))
                (do
                  (Thread/sleep 250)
                  (recur next-node-id))
                (throw e)))))))))

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

(defn with-e2e-ha-cluster
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

(defn e2e-stop-node
  [ctx node-id]
  (safe-close-e2e-ha-conn! (get-in ctx [:conns node-id]))
  (safe-stop-e2e-ha-server! (get-in ctx [:servers node-id]))
  (-> ctx
      (assoc-in [:conns node-id] nil)
      (assoc-in [:servers node-id] nil)
      (update :live-node-ids disj node-id)))

(defn e2e-restart-node
  [ctx node-id]
  (let [node (get-in ctx [:nodes (dec node-id)])
        server (start-e2e-ha-server! node)
        ;; Reopening an existing HA store with schema can synthesize a local
        ;; schema txlog row and interfere with follower catch-up ordering.
        conn (open-e2e-ha-conn! node
                                (:db-name ctx)
                                nil
                                (e2e-node-ha-opts (:base-opts ctx) node))]
    (-> ctx
        (assoc-in [:servers node-id] server)
        (assoc-in [:conns node-id] conn)
        (update :live-node-ids conj node-id))))

(defn e2e-wait-for-follower-rejoin!
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

(defn e2e-wait-for-live-followers-under-leader!
  [ctx-atom leader-id min-lsn visible-key visible-expected]
  (let [ctx @ctx-atom
        follower-ids (sort (disj (:live-node-ids ctx) leader-id))]
    (doseq [node-id follower-ids]
      (e2e-wait-for-follower-rejoin! ctx-atom
                                     node-id
                                     leader-id
                                     min-lsn
                                     visible-key
                                     visible-expected))
    true))

(defn e2e-wait-for-follower-bootstrap!
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

(defn e2e-wait-for-follower-degraded!
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

(defn e2e-wait-for-follower-stays-degraded!
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

(defn e2e-wait-for-replica-floor!
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

(defn e2e-wait-for-clock-skew-block!
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

(defn e2e-wait-for-clock-skew-clear!
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

(defn e2e-wait-for-fence-entry!
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

(defn e2e-verify-fencing-hook-entry
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
    (let [expected-fence-op-id (str db-name ":" observed-term ":" new-leader-id)
          expected-fence-shared-op-id (str db-name ":" observed-term)]
      (when-not (= expected-fence-op-id (:fence-op-id entry))
        (throw (ex-info "E2E fencing hook entry recorded unexpected fence op id"
                        {:expected-fence-op-id expected-fence-op-id
                         :entry entry})))
      (when-not (= expected-fence-shared-op-id
                   (:fence-shared-op-id entry))
        (throw (ex-info "E2E fencing hook entry recorded unexpected shared fence op id"
                        {:expected-fence-shared-op-id
                         expected-fence-shared-op-id
                         :entry entry})))
      (assoc entry
             :expected-fence-op-id expected-fence-op-id
             :expected-fence-shared-op-id expected-fence-shared-op-id))))

(defn e2e-drifted-ha-members
  [members target-node-id]
  (mapv (fn [member]
          (if (= target-node-id (:node-id member))
            (assoc member :endpoint
                   (str "127.0.0.1:" (+ 29000 target-node-id)))
            member))
        members))

(defn run-e2e-ha-failover!
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
        (let [baseline-lsn (e2e-effective-local-lsn ctx leader-id)]
          (swap! ctx-atom e2e-stop-node leader-id)
          (let [ctx @ctx-atom
                {:keys [leader-id snapshot]}
                (e2e-wait-for-single-leader! (:servers ctx)
                                             (:db-name ctx)
                                             (:live-node-ids ctx)
                                             leader-timeout-ms)
                stabilized-lsn (max baseline-lsn
                                    (e2e-effective-local-lsn ctx leader-id))
                new-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])]
            (e2e-wait-for-live-followers-under-leader! ctx-atom
                                                       leader-id
                                                       stabilized-lsn
                                                       "seed"
                                                       "v1")
            (e2e-write-value! (:conns ctx) leader-id "post-failover" "v2")
            (e2e-verify-value-on-nodes! ctx
                                        (:live-node-ids ctx)
                                        "post-failover"
                                        "v2")
            {:control-backend control-backend
             :initial-leader-endpoint leader-endpoint
             :new-leader-endpoint new-endpoint
             :probe-snapshot snapshot}))))))

(defn run-e2e-ha-follower-rejoin!
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
        (let [baseline-lsn (e2e-effective-local-lsn ctx leader-id)]
          (swap! ctx-atom e2e-stop-node initial-leader-id)
          (let [ctx @ctx-atom
                {:keys [leader-id]}
                (e2e-wait-for-single-leader! (:servers ctx)
                                             (:db-name ctx)
                                             (:live-node-ids ctx)
                                             leader-timeout-ms)
                stabilized-lsn (max baseline-lsn
                                    (e2e-effective-local-lsn ctx leader-id))
                failover-leader-id leader-id]
            (e2e-wait-for-live-followers-under-leader! ctx-atom
                                                       leader-id
                                                       stabilized-lsn
                                                       "seed"
                                                       "v1")
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
               :replica-floor replica-floor}))))))))

(defn run-e2e-ha-membership-hash-drift!
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
            (let [baseline-lsn (e2e-effective-local-lsn ctx leader-id)
                  drift-error
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
                                                 leader-timeout-ms)
                    stabilized-lsn (max baseline-lsn
                                        (e2e-effective-local-lsn ctx
                                                                 leader-id))]
                (e2e-wait-for-live-followers-under-leader! ctx-atom
                                                           leader-id
                                                           stabilized-lsn
                                                           "seed"
                                                           "v1")
                (let [ctx @ctx-atom]
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
                                               (:live-node-ids ctx))}))))))
      (finally
        (log/set-config! old-config)))))

(defn run-e2e-ha-fencing-hook-verify!
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
            (let [baseline-lsn (e2e-effective-local-lsn ctx leader-id)]
              (swap! ctx-atom e2e-stop-node initial-leader-id)
              (let [ctx @ctx-atom
                    {:keys [leader-id snapshot]}
                    (e2e-wait-for-single-leader! (:servers ctx)
                                                 (:db-name ctx)
                                                 (:live-node-ids ctx)
                                                 leader-timeout-ms)
                    stabilized-lsn (max baseline-lsn
                                        (e2e-effective-local-lsn ctx leader-id))
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
                (e2e-wait-for-live-followers-under-leader! ctx-atom
                                                           new-leader-id
                                                           stabilized-lsn
                                                           "seed"
                                                           "v1")
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
                                               (:live-node-ids ctx))})))))
      (finally
        (log/set-config! old-config)
        (safe-delete-dir! dir)))))

(defn run-e2e-ha-clock-skew-pause!
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

(defn e2e-wal-gap-base-opts
  [base-opts]
  (assoc base-opts
         :wal-segment-max-ms 100
         :wal-segment-prealloc? false
         :wal-segment-prealloc-mode :none
         :wal-replica-floor-ttl-ms 500
         :snapshot-scheduler? false))

(defn run-e2e-ha-rejoin-bootstrap!
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
                    stabilized-lsn (max baseline-lsn
                                        (e2e-effective-local-lsn ctx leader-id))
                    failover-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])
                    source-node-ids (->> (:live-node-ids ctx)
                                         sort
                                         vec)
                    source-endpoints (->> source-node-ids
                                          (map #(get-in ctx [:nodes (dec %) :endpoint]))
                                          set)
                    _ (e2e-wait-for-live-followers-under-leader! ctx-atom
                                                                  failover-leader-id
                                                                  stabilized-lsn
                                                                  "seed"
                                                                  "v1")
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
                    ;; The current leader is the deterministic WAL-gap anchor
                    ;; for this bootstrap scenario after failover and replica
                    ;; floor expiry.
                    leader-gc-result (get gc-results failover-leader-id)
                    leader-min-retained-lsn
                    (long (or (get-in leader-gc-result [:after :min-retained-lsn])
                              0))
                    _ (when (zero? (:deleted-count leader-gc-result))
                        (throw (ex-info "Rejoin bootstrap e2e did not delete leader WAL segments"
                                        {:leader-id failover-leader-id
                                         :follower-next-lsn follower-next-lsn
                                         :gc-results gc-results})))
                    _ (when (<= leader-min-retained-lsn (long follower-next-lsn))
                        (throw (ex-info "Rejoin bootstrap e2e did not create a real leader WAL gap"
                                        {:leader-id failover-leader-id
                                         :follower-next-lsn follower-next-lsn
                                         :gc-results gc-results})))
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

(defn run-e2e-ha-degraded-mode-no-valid-source!
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
                  gc-results
                  (into {}
                        (map (fn [node-id]
                               [node-id
                                (e2e-gc-txlog-segments-on-node!
                                 @ctx-atom node-id)]))
                        source-node-ids)
                  ;; The leader is the reliable WAL-gap anchor for degraded-mode
                  ;; bootstrap coverage in this scenario.
                  leader-gc-result (get gc-results leader-id)
                  leader-min-retained-lsn
                  (long (or (get-in leader-gc-result [:after :min-retained-lsn])
                            0))
                  _ (when (zero? (:deleted-count leader-gc-result))
                      (throw (ex-info "Degraded-mode e2e did not delete leader WAL segments"
                                      {:leader-id leader-id
                                       :follower-next-lsn follower-next-lsn
                                       :gc-results gc-results})))
                  _ (when (<= leader-min-retained-lsn
                                (long follower-next-lsn))
                      (throw (ex-info "Degraded-mode e2e did not create a real leader WAL gap"
                                      {:leader-id leader-id
                                       :follower-next-lsn follower-next-lsn
                                       :gc-results gc-results})))
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


(defn txlog-record-with-max-tx
  [store tx]
  (->> (kv/open-tx-log-rows (.-lmdb store) 1 64)
       (filter
         (fn [record]
           (some (fn [[op dbi k v]]
                   (and (= op :put)
                        (= dbi c/meta)
                        (= k :max-tx)
                        (= v tx)))
                 (:rows record))))
       last))

(defn latest-datalog-txlog-record
  [records]
  (->> records
       (filter
        (fn [record]
          (some (fn [[op dbi k]]
                  (and (= op :put)
                       (= dbi c/meta)
                       (= k :max-tx)))
                (:rows record))))
       last))

(defn fake-server-with-db-state
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

(defn acquire-leader-state
  [runtime opts]
  (let [now-ms (System/currentTimeMillis)
        acquire (ha/try-acquire-lease
                 (:ha-authority runtime)
                 {:db-identity (:db-identity opts)
                  :leader-node-id (:ha-node-id opts)
                  :leader-endpoint "10.0.0.12:8898"
                  :lease-renew-ms (:ha-lease-renew-ms opts)
                  :lease-timeout-ms (:ha-lease-timeout-ms opts)
                  :leader-last-applied-lsn 1
                  :now-ms now-ms
                  :observed-version 0
                  :observed-lease nil})]
    (is (:ok? acquire))
    (-> runtime
        (assoc :ha-role :leader
               :ha-authority-owner-node-id (:ha-node-id opts)
               :ha-lease-until-ms (+ now-ms 10000)
               :ha-leader-term (:term acquire)
               :ha-authority-term (:term acquire)))))
