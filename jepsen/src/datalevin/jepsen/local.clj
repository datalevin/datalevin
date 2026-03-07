(ns datalevin.jepsen.local
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as ddb]
   [datalevin.ha :as dha]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.server :as srv]
   [datalevin.util :as u]
   [jepsen.db :as db])
  (:import
   [datalevin.storage Store]
   [datalevin.server Server]
   [java.net ConnectException]
   [java.nio.channels ClosedChannelException]
   [java.util UUID]))

(def ^:private default-port-base 19001)
(def ^:private conn-client-opts {:pool-size 1 :time-out 60000})
(def ^:private cluster-timeout-ms 10000)
(def ^:private leader-connect-retry-sleep-ms 250)

(def default-nodes ["n1" "n2" "n3"])

(defonce ^:private clusters (atom {}))

(defn- existing-canonical-path
  [& path-parts]
  (let [^java.io.File file (apply io/file path-parts)]
    (when (.exists file)
      (.getCanonicalPath file))))

(defn admin-uri
  [endpoint]
  (str "dtlv://" c/default-username ":" c/default-password "@" endpoint))

(defn db-uri
  [endpoint db-name]
  (str (admin-uri endpoint) "/" db-name))

(defn- now-ms
  []
  (System/currentTimeMillis))

(defn- random-port-candidate
  []
  (+ 35000 (rand-int 20000)))

(defn- make-nodes
  [work-dir logical-nodes]
  (let [ports (loop [chosen #{}]
                (if (= 6 (count chosen))
                  (vec chosen)
                  (recur (conj chosen (random-port-candidate)))))]
    (mapv
     (fn [idx logical-node]
       (let [node-id   (inc idx)
             port      (nth ports idx)
             peer-port (nth ports (+ 3 idx))]
         {:logical-node logical-node
          :node-id      node-id
          :port         port
          :endpoint     (str "127.0.0.1:" port)
          :peer-port    peer-port
          :peer-id      (str "127.0.0.1:" peer-port)
          :root         (str work-dir u/+separator+ logical-node)}))
     (range)
     logical-nodes)))

(defn- promotable-voters
  [nodes]
  (mapv (fn [{:keys [node-id peer-id]}]
          {:peer-id peer-id
           :ha-node-id node-id
           :promotable? true})
        nodes))

(defn- base-ha-opts
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
    :voters (promotable-voters nodes)
    :rpc-timeout-ms 5000
    :election-timeout-ms 5000
    :operation-timeout-ms 30000}})

(defn- clock-skew-script-path
  []
  (or (existing-canonical-path "." "script" "ha" "clock-skew-file.sh")
      (existing-canonical-path ".." "script" "ha" "clock-skew-file.sh")
      (u/raise "Unable to locate Jepsen clock skew hook script"
               {:script "clock-skew-file.sh"})))

(defn- clock-skew-hook-config
  [state-dir]
  {:cmd [(clock-skew-script-path) state-dir]
   :timeout-ms 1000
   :retries 0
   :retry-delay-ms 0})

(defn- clock-skew-state-file
  [state-dir node-id]
  (str state-dir u/+separator+ "clock-skew-" node-id ".txt"))

(defn- write-clock-skew-ms!
  [state-dir node-id skew-ms]
  (u/create-dirs state-dir)
  (spit (clock-skew-state-file state-dir node-id) (str (long skew-ms))))

(defn- node-ha-opts
  [base-opts node]
  (-> base-opts
      (assoc :ha-node-id (:node-id node))
      (assoc-in [:ha-control-plane :local-peer-id] (:peer-id node))))

(defn- start-server!
  [{:keys [port root]} verbose?]
  (u/create-dirs root)
  (let [server (binding [c/*db-background-sampling?* false]
                 (srv/create (cond-> {:port port :root root}
                               verbose? (assoc :verbose true))))]
    (binding [c/*db-background-sampling?* false]
      (srv/start server))
    server))

(defn- safe-close-conn!
  [conn]
  (when conn
    (try
      (d/close conn)
      (catch Throwable _ nil))))

(defn- safe-stop-server!
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

(defn- open-ha-conn!
  [node db-name schema opts]
  (d/create-conn (db-uri (:endpoint node) db-name)
                 schema
                 (assoc opts :client-opts conn-client-opts)))

(defn- with-control-backend
  [control-backend f]
  (with-redefs [srv/*start-ha-authority-fn*
                (fn [db-name' ha-opts]
                  (dha/start-ha-authority
                   db-name'
                   (assoc-in ha-opts
                             [:ha-control-plane :backend]
                             control-backend)))
                srv/*stop-ha-authority-fn*
                dha/stop-ha-authority]
    (f)))

(defn- db-state
  [server db-name]
  (when server
    (get (.-dbs ^Server server) db-name)))

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

(defn effective-local-lsn
  [cluster-id logical-node]
  (let [{:keys [db-name servers]} (get @clusters cluster-id)]
    (if-let [state (db-state (get servers logical-node) db-name)]
      (let [txlog-lsn     (long (or (:last-applied-lsn
                                     (local-watermarks
                                       (get servers logical-node)
                                       db-name))
                                    0))
            runtime-lsn   (long (or (:ha-local-last-applied-lsn state) 0))
            persisted-lsn (local-ha-persisted-lsn state)
            comparable    (long (max runtime-lsn persisted-lsn))]
        (if (= :leader (:ha-role state))
          (long (max txlog-lsn comparable))
          comparable))
      0)))

(defn wait-for-live-nodes-at-least-lsn!
  ([cluster-id target-lsn]
   (wait-for-live-nodes-at-least-lsn! cluster-id target-lsn cluster-timeout-ms))
  ([cluster-id target-lsn timeout-ms]
   (let [deadline (+ (now-ms) (long timeout-ms))
         target   (long target-lsn)]
     (loop [last-snapshot nil]
       (let [{:keys [live-nodes]} (get @clusters cluster-id)
             snapshot            (into {}
                                       (map (fn [logical-node]
                                              [logical-node
                                               (effective-local-lsn
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
                            :previous-snapshot last-snapshot}))))))))

(defn- probe-write-admission
  [server db-name]
  (let [db-state (db-state server db-name)]
    (cond
      (nil? server)
      {:status :down}

      (nil? db-state)
      {:status :missing-db-state}

      (nil? (:ha-authority db-state))
      {:status :ha-runtime-missing}

      :else
      (try
        (if-let [err (#'srv/ha-write-admission-error
                      server
                      {:type :open-dbi
                       :args [db-name "__jepsen_probe" nil]})]
          {:status :rejected
           :reason (:reason err)
           :error (:error err)}
          {:status :leader})
        (catch Throwable e
          {:status :probe-failed
           :message (ex-message e)})))))

(defn wait-for-single-leader!
  ([cluster-id]
   (wait-for-single-leader! cluster-id cluster-timeout-ms))
  ([cluster-id timeout-ms]
   (let [timeout-ms (long timeout-ms)
         deadline   (+ (now-ms) timeout-ms)]
     (loop [last-snapshot nil]
       (let [{:keys [db-name live-nodes servers]} (get @clusters cluster-id)
             snapshot (into {}
                            (map (fn [logical-node]
                                   [logical-node
                                    (probe-write-admission
                                     (get servers logical-node)
                                     db-name)]))
                            live-nodes)
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
           (throw (ex-info "Timed out waiting for single leader"
                           {:timeout-ms timeout-ms
                            :probe-snapshot snapshot
                            :previous-snapshot last-snapshot}))))))))

(defn maybe-wait-for-single-leader
  ([cluster-id]
   (maybe-wait-for-single-leader cluster-id cluster-timeout-ms))
  ([cluster-id timeout-ms]
   (try
     (wait-for-single-leader! cluster-id timeout-ms)
     (catch clojure.lang.ExceptionInfo e
       (let [{:keys [probe-snapshot previous-snapshot]} (ex-data e)]
         (when-not (and probe-snapshot previous-snapshot)
           (throw e))
         nil)))))

(defn cluster-state
  [cluster-id]
  (get @clusters cluster-id))

(defn node-diagnostics
  [cluster-id logical-node]
  (let [{:keys [db-name servers]} (get @clusters cluster-id)]
    (when-let [state (db-state (get servers logical-node) db-name)]
      {:ha-role (:ha-role state)
       :ha-authority-owner-node-id (:ha-authority-owner-node-id state)
       :ha-authority-term (:ha-authority-term state)
       :ha-local-last-applied-lsn (:ha-local-last-applied-lsn state)
       :ha-follower-next-lsn (:ha-follower-next-lsn state)
       :ha-follower-last-batch-size (:ha-follower-last-batch-size state)
       :ha-follower-last-sync-ms (:ha-follower-last-sync-ms state)
       :ha-follower-leader-endpoint (:ha-follower-leader-endpoint state)
       :ha-follower-source-endpoint (:ha-follower-source-endpoint state)
       :ha-follower-source-order (:ha-follower-source-order state)
       :ha-follower-degraded? (:ha-follower-degraded? state)
       :ha-follower-degraded-reason (:ha-follower-degraded-reason state)
       :ha-follower-last-error (:ha-follower-last-error state)
       :ha-follower-last-error-details (:ha-follower-last-error-details state)
       :ha-follower-next-sync-not-before-ms
       (:ha-follower-next-sync-not-before-ms state)
       :ha-effective-local-lsn (effective-local-lsn cluster-id logical-node)})))

(defn local-query
  [cluster-id logical-node q & inputs]
  (let [{:keys [db-name servers]} (get @clusters cluster-id)]
    (if-let [state (db-state (get servers logical-node) db-name)]
      (let [store (:store state)]
        (if (store-open? store)
          (try
            (apply d/q q (ddb/new-db store) inputs)
            (catch Throwable _
              ::unavailable))
          ::unavailable))
      ::unavailable)))

(defn clock-skew-enabled?
  [cluster-id]
  (boolean (get-in @clusters [cluster-id :clock-skew-dir])))

(defn clock-skew-budget-ms
  [cluster-id]
  (long (or (get-in @clusters [cluster-id :base-opts :ha-clock-skew-budget-ms])
            c/*ha-clock-skew-budget-ms*)))

(defn set-node-clock-skew!
  [cluster-id logical-node skew-ms]
  (when-let [{:keys [clock-skew-dir node-by-name]} (get @clusters cluster-id)]
    (when-let [node (get node-by-name logical-node)]
      (write-clock-skew-ms! clock-skew-dir (:node-id node) skew-ms)
      true)))

(defn endpoint-for-node
  [cluster-id logical-node]
  (get-in @clusters [cluster-id :node-by-name logical-node :endpoint]))

(defn- leader-connect-transport-failure?
  [e]
  (boolean
    (some
      (fn [cause]
        (let [message (ex-message cause)]
          (or (instance? ClosedChannelException cause)
              (instance? ConnectException cause)
              (and (string? message)
                   (or (str/includes? message "Socket channel is closed.")
                       (str/includes? message "ClosedChannelException")
                       (str/includes? message "Unable to connect to server:")
                       (str/includes? message "Connection refused")
                       (str/includes? message "Connection reset by peer")
                       (str/includes? message "Broken pipe"))))))
      (take-while some? (iterate ex-cause e)))))

(defn open-leader-conn!
  [test schema]
  (let [cluster-id (:datalevin/cluster-id test)
        deadline   (+ (now-ms) cluster-timeout-ms)]
    (loop []
      (let [leader-node (:leader (wait-for-single-leader! cluster-id))
            leader-uri  (db-uri (endpoint-for-node cluster-id leader-node)
                                (:db-name test))
            outcome     (try
                          {:conn (d/create-conn leader-uri
                                                schema
                                                {:client-opts conn-client-opts})}
                          (catch Throwable e
                            {:error e}))]
        (if-let [conn (:conn outcome)]
          conn
          (let [e (:error outcome)]
            (if (and (< (now-ms) deadline)
                     (leader-connect-transport-failure? e))
              (do
                (Thread/sleep (long leader-connect-retry-sleep-ms))
                (recur))
              (throw e))))))))

(defn with-leader-conn
  [test schema f]
  (let [conn (open-leader-conn! test schema)]
    (try
      (f conn)
      (finally
        (d/close conn)))))

(defn stop-node!
  [cluster-id logical-node]
  (locking clusters
    (when-let [cluster (get @clusters cluster-id)]
      (safe-close-conn! (get-in cluster [:admin-conns logical-node]))
      (safe-stop-server! (get-in cluster [:servers logical-node]))
      (swap! clusters
             (fn [clusters*]
               (-> clusters*
                   (assoc-in [cluster-id :admin-conns logical-node] nil)
                   (assoc-in [cluster-id :servers logical-node] nil)
                   (update-in [cluster-id :live-nodes] disj logical-node)))))))

(defn restart-node!
  [cluster-id logical-node]
  (locking clusters
    (let [{:keys [db-name schema base-opts node-by-name verbose? control-backend]}
          (get @clusters cluster-id)
          node   (get node-by-name logical-node)
          server (start-server! node verbose?)
          conn   (with-control-backend
                   control-backend
                   #(open-ha-conn! node
                                   db-name
                                   schema
                                   (node-ha-opts base-opts node)))]
      (swap! clusters
             (fn [clusters*]
               (-> clusters*
                   (assoc-in [cluster-id :servers logical-node] server)
                   (assoc-in [cluster-id :admin-conns logical-node] conn)
                   (update-in [cluster-id :live-nodes] conj logical-node))))
      true)))

(defn- resolve-work-dir
  [cluster-id test]
  (if-let [base-dir (:work-dir test)]
    (let [path (str (.getCanonicalPath (io/file base-dir))
                    u/+separator+
                    cluster-id)]
      (u/create-dirs path)
      path)
    (let [dir (u/tmp-dir (str "datalevin-jepsen-" cluster-id "-" (UUID/randomUUID)))]
      (u/create-dirs dir)
      dir)))

(defn- init-cluster!
  [cluster-id test]
  (let [logical-nodes    (vec (or (seq (:nodes test)) default-nodes))
        work-dir         (resolve-work-dir cluster-id test)
        nodes            (make-nodes work-dir logical-nodes)
        db-name          (:db-name test)
        schema           (:schema test)
        control-backend  (:control-backend test)
        nemesis-faults   (set (:datalevin/nemesis-faults test))
        clock-skew?      (contains? nemesis-faults :clock-skew-pause)
        clock-skew-dir   (when clock-skew?
                           (str work-dir u/+separator+ "clock-skew"))
        group-id         (str "datalevin-jepsen-" cluster-id)
        db-identity      (str "db-" (UUID/randomUUID))
        base-opts        (cond-> (base-ha-opts
                                   nodes group-id db-identity control-backend)
                           clock-skew?
                           (assoc :ha-clock-skew-hook
                                  (clock-skew-hook-config clock-skew-dir)))
        verbose?         (boolean (:verbose test))
        servers-atom     (atom {})
        conns-atom       (atom {})]
    (try
      (when clock-skew?
        (doseq [{:keys [node-id]} nodes]
          (write-clock-skew-ms! clock-skew-dir node-id 0)))
      (with-control-backend
        control-backend
        (fn []
          (doseq [node nodes]
            (swap! servers-atom assoc
                   (:logical-node node)
                   (start-server! node verbose?)))
          ;; JRaft-backed startup needs peers to enter the open path together;
          ;; a sequential open can deadlock waiting for a quorum that has not
          ;; started its authority yet.
          (doseq [[logical-node conn]
                  (->> nodes
                       (mapv (fn [node]
                               (future
                                 [(:logical-node node)
                                  (open-ha-conn! node
                                                 db-name
                                                 schema
                                                 (node-ha-opts base-opts node))])))
                       (mapv deref))]
            (swap! conns-atom assoc logical-node conn))))
      (let [cluster {:cluster-id      cluster-id
                     :work-dir        work-dir
                     :keep-work-dir?  (boolean (:keep-work-dir test))
                     :db-name         db-name
                     :schema          schema
                     :control-backend control-backend
                     :group-id        group-id
                     :db-identity     db-identity
                     :base-opts       base-opts
                     :clock-skew-dir  clock-skew-dir
                     :verbose?        verbose?
                     :nodes           nodes
                     :node-by-name    (into {}
                                            (map (juxt :logical-node identity))
                                            nodes)
                     :servers         @servers-atom
                     :admin-conns     @conns-atom
                     :live-nodes      (set logical-nodes)
                     :teardown-nodes  #{}}]
        (swap! clusters assoc cluster-id cluster)
        (wait-for-single-leader! cluster-id)
        cluster)
      (catch Throwable e
        (doseq [conn (vals @conns-atom)]
          (safe-close-conn! conn))
        (doseq [server (vals @servers-atom)]
          (safe-stop-server! server))
        (safe-delete-dir! work-dir)
        (throw e)))))

(defn- teardown-cluster!
  [cluster-id]
  (when-let [{:keys [admin-conns servers work-dir keep-work-dir?]} (get @clusters cluster-id)]
    (doseq [conn (vals admin-conns)]
      (safe-close-conn! conn))
    (doseq [server (vals servers)]
      (safe-stop-server! server))
    (swap! clusters dissoc cluster-id)
    (when-not keep-work-dir?
      (safe-delete-dir! work-dir))))

(defrecord LocalClusterDB [cluster-id]
  db/DB
  (setup! [this test _node]
    (locking clusters
      (when-not (cluster-state cluster-id)
        (init-cluster! cluster-id test)))
    this)

  (teardown! [this test node]
    (locking clusters
      (when-let [cluster (cluster-state cluster-id)]
        (let [teardown-nodes (conj (:teardown-nodes cluster) node)]
          (swap! clusters assoc-in [cluster-id :teardown-nodes] teardown-nodes)
          (when (= teardown-nodes (set (:nodes test)))
            (teardown-cluster! cluster-id)))))
    this))

(defn db
  [cluster-id]
  (->LocalClusterDB cluster-id))
