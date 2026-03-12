(ns datalevin.jepsen.local
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as ddb]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ctrl]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.remote :as r]
   [datalevin.server :as srv]
   [datalevin.util :as u]
   [jepsen.db :as db]
   [jepsen.net.proto :as net.proto]
   [taoensso.timbre :as log])
  (:import
   [datalevin.jepsen PartitionFaults]
   [datalevin.storage Store]
   [datalevin.server Server]
   [java.net ConnectException]
   [java.net InetSocketAddress]
   [java.net ServerSocket]
   [java.nio.channels ClosedChannelException]
   [java.util UUID]))

(def ^:private default-port-base 19001)
(def ^:private default-port-limit 31999)
(def ^:private port-block-size 32)
(def ^:private cluster-timeout-ms 10000)
(def ^:private default-cluster-setup-timeout-ms 30000)
; Keep request timeout close to Jepsen's leader wait so paused nodes fail fast.
(def ^:private conn-client-opts {:pool-size 1 :time-out cluster-timeout-ms})
(def ^:private leader-connect-retry-sleep-ms 250)
(def ^:private default-slow-link-profile
  {:delay-ms 250
   :jitter-ms 250
   :drop-probability 0.0})
(def ^:private default-flaky-link-profile
  {:delay-ms 0
   :jitter-ms 0
   :drop-probability 0.3})
(def ^:private degraded-network-profile-templates
  [{:delay-ms 25
    :jitter-ms 10
    :drop-probability 0.02}
   {:delay-ms 100
    :jitter-ms 25
    :drop-probability 0.05}
   {:delay-ms 250
    :jitter-ms 100
    :drop-probability 0.1}
   {:delay-ms 500
    :jitter-ms 200
    :drop-probability 0.2}
   {:delay-ms 750
    :jitter-ms 300
    :drop-probability 0.35}])
(def ^:private graph-cut-direction-modes
  [:none :left->right :right->left :bidirectional])
(def ^:private storage-fault-modes
  #{:stall :disk-full})
(def ^:private storage-fault-default-stages
  #{:txlog-append
    :txlog-sync
    :txlog-replay
    :txlog-force-sync
    :lmdb-sync})
(def ^:private storage-stall-poll-ms 100)

(def default-nodes ["n1" "n2" "n3"])

(defonce ^:private clusters (atom {}))
(defonce ^:private next-port-block (atom -1))

(def ^:private base-fetch-ha-leader-txlog-batch dha/fetch-ha-leader-txlog-batch)
(def ^:private base-report-ha-replica-floor! dha/report-ha-replica-floor!)
(def ^:private base-fetch-ha-endpoint-snapshot-copy!
  dha/fetch-ha-endpoint-snapshot-copy!)
(def ^:private base-server-runtime-opts-fn srv/*server-runtime-opts-fn*)

(defonce ^:private server-runtime-opts-overrides (atom {}))

(defn- cluster-entry-for-db-identity
  [db-identity]
  (some (fn [[cluster-id {:keys [db-identity db-name] :as cluster}]]
          (when (= db-identity (:db-identity cluster))
            [cluster-id cluster]))
        @clusters))

(defn- logical-node-for-ha-state
  [m]
  (when-let [[cluster-id cluster]
             (cluster-entry-for-db-identity (:ha-db-identity m))]
    {:cluster-id cluster-id
     :logical-node (get-in cluster [:node-by-id (:ha-node-id m)])}))

(defn- blocked-link-exception
  [src-logical-node dest-logical-node endpoint]
  (ConnectException.
   (str "Unable to connect to server: Jepsen partition blocks "
        src-logical-node
        " -> "
        dest-logical-node
        " via "
        endpoint)))

(defn- authority-diagnostics-snapshot
  [authority]
  (when authority
    (if-let [f (try
                 (requiring-resolve 'datalevin.ha.control/authority-diagnostics)
                 (catch Throwable _
                   nil))]
      (f authority)
      {:backend :diagnostics-unavailable})))

(defn- normalize-storage-fault
  [{:keys [mode stages] :as fault}]
  (let [mode* (keyword (or mode :stall))]
    (when-not (contains? storage-fault-modes mode*)
      (u/raise "Unsupported Jepsen storage fault mode"
               {:mode mode*
                :allowed storage-fault-modes}))
    (assoc fault
           :mode mode*
           :stages (set (or stages storage-fault-default-stages)))))

(defn storage-fault
  [cluster-id logical-node]
  (get-in @clusters [cluster-id :storage-faults logical-node]))

(defn- active-storage-fault-target
  [{:keys [db-identity ha-node-id]}]
  (when (and (string? db-identity)
             (some? ha-node-id))
    (when-let [[cluster-id cluster] (cluster-entry-for-db-identity db-identity)]
      (when-let [logical-node (get-in cluster [:node-by-id ha-node-id])]
        (when-let [fault (storage-fault cluster-id logical-node)]
          {:cluster-id cluster-id
           :logical-node logical-node
           :fault fault})))))

(defn- disk-full-exception
  [cluster-id logical-node stage]
  (ex-info "No space left on device"
           {:type :jepsen/disk-full
            :cluster-id cluster-id
            :logical-node logical-node
            :stage stage}))

(defn maybe-apply-storage-fault!
  [{:keys [stage] :as context}]
  (when-let [{:keys [cluster-id logical-node fault]}
             (active-storage-fault-target context)]
    (when (contains? (:stages fault) stage)
      (case (:mode fault)
        :disk-full
        (throw (disk-full-exception cluster-id logical-node stage))

        :stall
        (loop []
          (when-let [current (storage-fault cluster-id logical-node)]
            (when (and (= :stall (:mode current))
                       (contains? (:stages current) stage))
              (Thread/sleep (long storage-stall-poll-ms))
              (recur))))

        nil))))

(defn blocked-link?
  [cluster-id src-logical-node dest-logical-node]
  (contains? (get-in @clusters [cluster-id :dropped-links])
             [src-logical-node dest-logical-node]))

(defn- normalized-link-profile
  [profile]
  (let [delay-ms (long (max 0 (or (:delay-ms profile) 0)))
        jitter-ms (long (max 0 (or (:jitter-ms profile) 0)))
        drop-probability (double (max 0.0
                                      (min 1.0
                                           (double
                                            (or (:drop-probability profile)
                                                0.0)))))]
    {:delay-ms delay-ms
     :jitter-ms jitter-ms
     :drop-probability drop-probability}))

(defn- active-link-profile
  [cluster-id src-logical-node dest-logical-node]
  (get-in @clusters [cluster-id :link-behaviors [src-logical-node dest-logical-node]]))

(defn- endpoint-link-fault
  [m endpoint]
  (when-let [{:keys [cluster-id logical-node]} (logical-node-for-ha-state m)]
    (let [dest-logical-node (get-in @clusters [cluster-id :endpoint->node endpoint])]
      (when (and logical-node dest-logical-node)
        {:cluster-id cluster-id
         :src-logical-node logical-node
         :dest-logical-node dest-logical-node
         :blocked? (blocked-link? cluster-id logical-node dest-logical-node)
         :profile (active-link-profile cluster-id
                                       logical-node
                                       dest-logical-node)}))))

(defn- degraded-link-exception
  [src-logical-node dest-logical-node endpoint]
  (ConnectException.
   (str "Unable to connect to server: Jepsen degraded network dropped "
        src-logical-node
        " -> "
        dest-logical-node
        " via "
        endpoint)))

(defn- maybe-apply-link-fault!
  [{:keys [src-logical-node dest-logical-node blocked? profile]} endpoint]
  (when blocked?
    (throw (blocked-link-exception src-logical-node
                                   dest-logical-node
                                   endpoint)))
  (when profile
    (let [{:keys [delay-ms jitter-ms drop-probability]}
          (normalized-link-profile profile)
          extra-delay-ms (if (pos? jitter-ms)
                           (rand-int (inc (int jitter-ms)))
                           0)
          total-delay-ms (+ delay-ms extra-delay-ms)]
      (when (pos? total-delay-ms)
        (Thread/sleep (long total-delay-ms)))
      (when (or (>= drop-probability 1.0)
                (and (pos? drop-probability)
                     (< (rand) drop-probability)))
        (throw (degraded-link-exception src-logical-node
                                        dest-logical-node
                                        endpoint))))))

(defn- partition-aware-fetch-ha-leader-txlog-batch
  [db-name m leader-endpoint from-lsn upto-lsn]
  (let [fault (endpoint-link-fault m leader-endpoint)]
    (maybe-apply-link-fault! fault leader-endpoint)
    (base-fetch-ha-leader-txlog-batch
     db-name m leader-endpoint from-lsn upto-lsn)))

(defn- partition-aware-report-ha-replica-floor!
  [db-name m leader-endpoint applied-lsn]
  (let [fault (endpoint-link-fault m leader-endpoint)]
    (maybe-apply-link-fault! fault leader-endpoint)
    (base-report-ha-replica-floor! db-name m leader-endpoint applied-lsn)))

(defn- partition-aware-fetch-ha-endpoint-snapshot-copy!
  [db-name m endpoint dest-dir]
  (let [fault (endpoint-link-fault m endpoint)]
    (maybe-apply-link-fault! fault endpoint)
    (base-fetch-ha-endpoint-snapshot-copy! db-name m endpoint dest-dir)))

(defonce ^:private partition-aware-ha-transports-installed?
  (do
    (alter-var-root #'dha/fetch-ha-leader-txlog-batch
                    (constantly partition-aware-fetch-ha-leader-txlog-batch))
    (alter-var-root #'dha/report-ha-replica-floor!
                    (constantly partition-aware-report-ha-replica-floor!))
    (alter-var-root #'dha/fetch-ha-endpoint-snapshot-copy!
                    (constantly partition-aware-fetch-ha-endpoint-snapshot-copy!))
    true))

(defonce ^:private storage-fault-hook-installed?
  (do
    (kv/set-storage-fault-hook! maybe-apply-storage-fault!)
    true))

(defn- normalize-server-runtime-opts-override
  [override]
  (cond
    (ifn? override) override
    (map? override) (constantly override)
    :else nil))

(defn- override-key
  [root db-name]
  [root db-name])

(defn- resolved-server-runtime-opts
  [server db-name store m]
  (let [root         (some-> ^Server server .-root)
        override-fn  (get @server-runtime-opts-overrides
                          (override-key root db-name))
        base-opts    (base-server-runtime-opts-fn server db-name store m)
        override-opts (when override-fn
                        (override-fn server db-name store m))]
    (cond
      (and (map? base-opts) (map? override-opts))
      (merge base-opts override-opts)

      (map? override-opts)
      override-opts

      :else
      base-opts)))

(defonce ^:private server-runtime-opts-hook-installed?
  (do
    (alter-var-root #'srv/*server-runtime-opts-fn*
                    (constantly resolved-server-runtime-opts))
    true))

(defn- install-server-runtime-opts-overrides!
  [db-name nodes override]
  (when-let [override-fn (normalize-server-runtime-opts-override override)]
    (swap! server-runtime-opts-overrides
           (fn [m]
             (reduce (fn [acc {:keys [root]}]
                       (assoc acc (override-key root db-name) override-fn))
                     m
                     nodes)))
    true))

(defn- clear-server-runtime-opts-overrides!
  [db-name nodes]
  (swap! server-runtime-opts-overrides
         (fn [m]
           (reduce (fn [acc {:keys [root]}]
                     (dissoc acc (override-key root db-name)))
                   m
                   nodes))))

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

(defn- port-block-count
  []
  (inc (quot (- default-port-limit default-port-base) port-block-size)))

(defn- reserve-port-socket
  [^long port]
  (doto (ServerSocket.)
    (.setReuseAddress false)
    (.bind (InetSocketAddress. "127.0.0.1" (int port)))))

(defn- port-bind-error
  [^long port ^Exception e]
  {:port    port
   :class   (.getName (class e))
   :message (.getMessage e)})

(defn- reserve-port-block
  [^long base ^long n]
  (loop [idx     0
         sockets []]
    (if (= idx n)
      {:sockets sockets}
      (let [port   (+ base idx)
            result (try
                     {:socket (reserve-port-socket port)}
                     (catch Exception e
                       {:error (port-bind-error port e)}))]
        (if-let [error (:error result)]
          (do
            (doseq [^ServerSocket socket sockets]
              (try
                (.close socket)
                (catch Exception _
                  nil)))
            {:base  base
             :error error})
          (recur (inc idx) (conj sockets (:socket result))))))))

(defn- reserve-ports
  [n]
  (when (> n port-block-size)
    (u/raise "Unable to reserve Jepsen server ports"
             {:requested-ports n
              :port-block-size port-block-size}))
  (let [block-count    (port-block-count)
        start-block    (mod (swap! next-port-block inc) block-count)
        sample-limit   8]
    (loop [attempt 0
           errors  []]
      (when (>= attempt block-count)
        (let [data {:requested-ports n
                    :attempt attempt
                    :port-base default-port-base
                    :port-limit default-port-limit
                    :port-block-size port-block-size
                    :sample-bind-errors errors}
              sample-error (some-> errors first :error)
              message      (cond-> "Unable to reserve Jepsen server ports"
                             sample-error
                             (str " (sample bind error: "
                                  (:class sample-error)
                                  " on port "
                                  (:port sample-error)
                                  ": "
                                  (:message sample-error)
                                  ")"))]
          (u/raise message
                   data)))
      (let [block-idx (mod (+ start-block attempt) block-count)
            base      (+ default-port-base (* block-idx port-block-size))
            result    (reserve-port-block base n)]
        (if-let [sockets (:sockets result)]
          (try
            (mapv (fn [^ServerSocket socket]
                    (.getLocalPort socket))
                  sockets)
            (finally
              (doseq [^ServerSocket socket sockets]
                (try
                  (.close socket)
                  (catch Exception _
                    nil)))))
          (recur (inc attempt)
                 (cond-> errors
                   (< (count errors) sample-limit)
                   (conj result))))))))

(defn- make-nodes
  [work-dir logical-nodes]
  (let [node-count (count logical-nodes)
        ports      (reserve-ports (* 2 node-count))]
    (mapv
     (fn [idx logical-node]
       (let [node-id   (inc idx)
             port      (nth ports idx)
             peer-port (nth ports (+ node-count idx))]
         {:logical-node logical-node
          :node-id      node-id
          :port         port
          :endpoint     (str "127.0.0.1:" port)
          :peer-port    peer-port
          :peer-id      (str "127.0.0.1:" peer-port)
          :root         (str work-dir u/+separator+ logical-node)}))
     (range)
     logical-nodes)))

(defn- control-voters
  [data-nodes control-nodes]
  (let [promotable-node-ids (set (map :node-id data-nodes))]
    (mapv (fn [{:keys [node-id peer-id]}]
            (if (contains? promotable-node-ids node-id)
              {:peer-id peer-id
               :ha-node-id node-id
               :promotable? true}
              {:peer-id peer-id
               :promotable? false}))
          control-nodes)))

(defn- base-ha-opts
  [data-nodes control-nodes group-id db-identity control-backend]
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
   :ha-members (mapv #(select-keys % [:node-id :endpoint]) data-nodes)
   :ha-control-plane
   {:backend (if (= :in-memory control-backend)
               :sofa-jraft
               control-backend)
    :group-id group-id
    :voters (control-voters data-nodes control-nodes)
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

(defn- merge-ha-opts
  [base-opts override-opts]
  (if (map? override-opts)
    (let [control-plane-override (:ha-control-plane override-opts)]
      (cond-> (merge base-opts (dissoc override-opts :ha-control-plane))
        (map? control-plane-override)
        (update :ha-control-plane merge control-plane-override)))
    base-opts))

(defn- node-ha-opts
  ([base-opts node]
   (node-ha-opts base-opts node nil))
  ([base-opts node override-opts]
   (-> base-opts
       (assoc :ha-node-id (:node-id node))
       (assoc-in [:ha-control-plane :local-peer-id] (:peer-id node))
       (merge-ha-opts override-opts))))

(defn- cluster-setup-timeout-ms
  [base-opts]
  (let [control-plane (:ha-control-plane base-opts)
        election-ms   (long (or (:election-timeout-ms control-plane) 0))
        operation-ms  (long (or (:operation-timeout-ms control-plane)
                                default-cluster-setup-timeout-ms))]
    (long (max default-cluster-setup-timeout-ms
               (+ election-ms operation-ms 10000)))))

(defn workload-setup-timeout-ms
  ([cluster-id]
   (workload-setup-timeout-ms cluster-id default-cluster-setup-timeout-ms))
  ([cluster-id default-timeout-ms]
   (let [default-timeout-ms (long default-timeout-ms)]
     (long
       (max default-timeout-ms
            (or (some-> (get-in @clusters [cluster-id :base-opts])
                        cluster-setup-timeout-ms)
                default-cluster-setup-timeout-ms))))))

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

(defn- safe-stop-authority!
  [authority]
  (when authority
    (try
      (ctrl/stop-authority! authority)
      (catch Throwable _ nil))))

(defn- safe-delete-dir!
  [path]
  (when path
    (try
      (u/delete-files path)
      (catch Throwable _ nil))))

(declare create-conn-with-timeout!
         transport-failure?)

(defn- open-ha-conn!
  ([node db-name schema opts]
   (open-ha-conn! node db-name schema opts cluster-timeout-ms))
  ([node db-name schema opts timeout-ms]
   (let [uri        (db-uri (:endpoint node) db-name)
         timeout-ms (long timeout-ms)
         deadline   (+ (now-ms) timeout-ms)]
     (loop []
       (let [remaining-ms      (- deadline (now-ms))
             attempt-timeout-ms (long (max 1 (min timeout-ms remaining-ms)))
             outcome           (try
                                 {:conn (create-conn-with-timeout! uri
                                                                   schema
                                                                   opts
                                                                   attempt-timeout-ms)}
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
               (throw e)))))))))

(defn- validate-cluster-topology!
  [data-logical-nodes control-logical-nodes control-backend]
  (let [data-logical-nodes    (vec data-logical-nodes)
        control-logical-nodes (vec control-logical-nodes)
        data-node-set         (set data-logical-nodes)
        control-node-set      (set control-logical-nodes)]
    (when (empty? data-logical-nodes)
      (u/raise "Jepsen cluster requires at least one data node"
               {:data-nodes data-logical-nodes
                :control-nodes control-logical-nodes}))
    (when (not= (count data-logical-nodes) (count data-node-set))
      (u/raise "Jepsen data nodes must be unique"
               {:data-nodes data-logical-nodes}))
    (when (not= (count control-logical-nodes) (count control-node-set))
      (u/raise "Jepsen control nodes must be unique"
               {:control-nodes control-logical-nodes}))
    (when (some #(not (contains? control-node-set %)) data-logical-nodes)
      (u/raise "Jepsen control nodes must include every data node"
               {:data-nodes data-logical-nodes
                :control-nodes control-logical-nodes}))
    (when (and (> (count control-logical-nodes) (count data-logical-nodes))
               (not= :sofa-jraft control-backend))
      (u/raise "Jepsen control-only witness nodes require sofa-jraft"
               {:control-backend control-backend
                :data-nodes data-logical-nodes
                :control-nodes control-logical-nodes}))))

(defn- ^:redef create-conn-with-timeout!
  ([uri schema]
   (create-conn-with-timeout! uri schema {} cluster-timeout-ms))
  ([uri schema timeout-ms]
   (create-conn-with-timeout! uri schema {} timeout-ms))
  ([uri schema opts timeout-ms]
   (let [timeout-ms (long timeout-ms)
         timed-out? (atom false)
         result-f   (future
                      (try
                        (let [conn (d/create-conn uri
                                                  schema
                                                  (assoc opts
                                                         :client-opts
                                                         conn-client-opts))]
                          (if @timed-out?
                            (do
                              (safe-close-conn! conn)
                              ::timed-out)
                            {:conn conn}))
                        (catch Throwable e
                          {:error e})))
         result     (deref result-f timeout-ms ::timeout)]
     (cond
       (= ::timeout result)
       (do
         (reset! timed-out? true)
         (future-cancel result-f)
         (u/raise "Timeout in making request"
                  {:timeout-ms timeout-ms
                   :phase :open-conn
                   :uri uri}))

       (= ::timed-out result)
       (u/raise "Timeout in making request"
                {:timeout-ms timeout-ms
                 :phase :open-conn
                 :uri uri})

       (:error result)
       (throw (:error result))

       :else
       (:conn result)))))

(defn- with-control-backend
  [control-backend f]
  (with-redefs [srv/*start-ha-authority-fn*
                (fn [db-name' ha-opts]
                  (let [ha-opts' (assoc-in ha-opts
                                           [:ha-control-plane :backend]
                                           control-backend)
                        local-peer-id (get-in ha-opts'
                                              [:ha-control-plane
                                               :local-peer-id])]
                    (PartitionFaults/setCurrentLocalPeerId local-peer-id)
                    (try
                      (dha/start-ha-authority db-name' ha-opts')
                      (finally
                        (PartitionFaults/clearCurrentLocalPeerId)))))
                srv/*stop-ha-authority-fn*
                dha/stop-ha-authority]
    (f)))

(defn- control-authority-opts
  [work-dir base-opts node]
  (let [control-plane-opts (:ha-control-plane base-opts)]
    (cond-> (assoc control-plane-opts :local-peer-id (:peer-id node))
      (= :sofa-jraft (:backend control-plane-opts))
      (assoc :raft-dir (str work-dir
                            u/+separator+
                            "control-authority-"
                            (:node-id node))))))

(defn- start-control-authority!
  [work-dir base-opts node]
  (let [opts          (control-authority-opts work-dir base-opts node)
        local-peer-id (:local-peer-id opts)
        authority     (ctrl/new-authority opts)]
    (when-let [raft-dir (:raft-dir opts)]
      (u/create-dirs raft-dir))
    (PartitionFaults/setCurrentLocalPeerId local-peer-id)
    (try
      (ctrl/start-authority! authority)
      authority
      (finally
        (PartitionFaults/clearCurrentLocalPeerId)))))

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

(defn effective-local-lsn
  [cluster-id logical-node]
  (let [{:keys [db-name servers]} (get @clusters cluster-id)]
    (if-let [state (db-state (get servers logical-node) db-name)]
      (let [txlog-lsn     (long (or (:last-applied-lsn
                                     (local-watermarks
                                       (get servers logical-node)
                                       db-name))
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
      0)))

(defn node-progress-lsn
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
        ;; Snapshot-floor metadata is a historical retention marker, not proof
        ;; that a live node has replayed newer WAL. Catch-up waits must use the
        ;; node's actual applied progress so later snapshots cannot regress.
        (long (max txlog-lsn comparable)))
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
                                               (node-progress-lsn
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

(defn wait-for-nodes-at-least-lsn!
  ([cluster-id logical-nodes target-lsn]
   (wait-for-nodes-at-least-lsn! cluster-id logical-nodes target-lsn
                                 cluster-timeout-ms))
  ([cluster-id logical-nodes target-lsn timeout-ms]
   (let [deadline      (+ (now-ms) (long timeout-ms))
         target        (long target-lsn)
         logical-nodes (vec logical-nodes)]
     (loop [last-snapshot nil]
       (let [snapshot (into {}
                            (map (fn [logical-node]
                                   [logical-node
                                    (node-progress-lsn cluster-id
                                                       logical-node)]))
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
                            :previous-snapshot last-snapshot}))))))))

(defn wait-for-at-least-nodes-at-least-lsn!
  ([cluster-id logical-nodes target-lsn required-count]
   (wait-for-at-least-nodes-at-least-lsn! cluster-id logical-nodes target-lsn
                                          required-count cluster-timeout-ms))
  ([cluster-id logical-nodes target-lsn required-count timeout-ms]
   (let [deadline      (+ (now-ms) (long timeout-ms))
         target        (long target-lsn)
         logical-nodes (vec logical-nodes)
         required-count (long required-count)]
     (loop [last-snapshot nil]
       (let [snapshot (into {}
                            (map (fn [logical-node]
                                   [logical-node
                                    (node-progress-lsn cluster-id
                                                       logical-node)]))
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
                    :previous-snapshot last-snapshot}))))))))

(defn- probe-write-admission
  [cluster-id logical-node server db-name]
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

(declare node-diagnostics)

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
                                     cluster-id
                                     logical-node
                                     (get servers logical-node)
                                     db-name)]))
                            live-nodes)
             diagnostics-snapshot
             (into {}
                   (map (fn [logical-node]
                          [logical-node
                           (select-keys
                            (node-diagnostics cluster-id logical-node)
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
           (let [data {:timeout-ms timeout-ms
                       :probe-snapshot snapshot
                       :diagnostics-snapshot diagnostics-snapshot
                       :previous-snapshot last-snapshot}]
             (log/warn "Jepsen timed out waiting for single Datalevin leader"
                       data)
             (throw (ex-info "Timed out waiting for single leader"
                             data)))))))))

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

(defn paused-node-info
  [cluster-id logical-node]
  (get-in @clusters [cluster-id :paused-node-info logical-node]))

(defn- authority-leader-logical-node
  [cluster-id peer-id]
  (get-in @clusters [cluster-id :peer-id->node peer-id]))

(defn wait-for-authority-leader!
  ([cluster-id]
   (wait-for-authority-leader! cluster-id cluster-timeout-ms))
  ([cluster-id timeout-ms]
   (let [timeout-ms (long timeout-ms)
         deadline   (+ (now-ms) timeout-ms)]
     (loop [last-snapshot nil]
       (let [{:keys [control-node-names]} (get @clusters cluster-id)
             control-node-names (or control-node-names [])
             snapshot (into {}
                            (map (fn [logical-node]
                                   (let [state (node-diagnostics cluster-id
                                                                 logical-node)
                                         leader-peer-id
                                         (or (:ha-control-leader-peer-id state)
                                             (when (:ha-control-node-leader? state)
                                               (:ha-control-local-peer-id state)))]
                                     [logical-node
                                      {:leader-peer-id leader-peer-id
                                       :leader (authority-leader-logical-node
                                                cluster-id
                                                leader-peer-id)
                                       :node-leader?
                                       (:ha-control-node-leader? state)
                                       :node-state (:ha-control-node-state state)
                                       :term (:ha-authority-term state)
                                       :role (:ha-role state)}]))
                            control-node-names))
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
                            :previous-snapshot last-snapshot}))))))))

(defn maybe-wait-for-authority-leader
  ([cluster-id]
   (maybe-wait-for-authority-leader cluster-id cluster-timeout-ms))
  ([cluster-id timeout-ms]
   (try
     (wait-for-authority-leader! cluster-id timeout-ms)
     (catch clojure.lang.ExceptionInfo e
       (let [{:keys [authority-snapshot previous-snapshot]} (ex-data e)]
         (when-not (and authority-snapshot previous-snapshot)
           (throw e))
         nil)))))

(defn node-diagnostics
  [cluster-id logical-node]
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
          :ha-effective-local-lsn (effective-local-lsn cluster-id logical-node)}
         (authority-diagnostics->control-state authority-diagnostics)))
      (when-let [authority (get control-authorities logical-node)]
        (merge
         {:ha-role :control-only
          :jepsen-control-only? true}
         (authority-diagnostics->control-state
          (authority-diagnostics-snapshot authority)))))))

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

(defn- normalized-grudge
  [grudge]
  (into (sorted-map)
        (map (fn [[dest srcs]]
               [dest (vec (sort (set srcs)))]))
        grudge))

(defn- grudge->dropped-links
  [grudge]
  (into #{}
        (mapcat (fn [[dest srcs]]
                  (map (fn [src]
                         [src dest])
                       srcs)))
        grudge))

(defn- nodes->directed-links
  [nodes]
  (for [src nodes
        dest nodes
        :when (not= src dest)]
    [src dest]))

(defn- nodes->link-behaviors
  [nodes profile]
  (let [profile' (normalized-link-profile profile)]
    (into (sorted-map)
          (map (fn [link]
                 [link profile']))
          (nodes->directed-links nodes))))

(defn- link-profile-summary
  [link-behaviors]
  (let [profiles (vec (vals link-behaviors))
        delays   (mapv :delay-ms profiles)
        jitters  (mapv :jitter-ms profiles)
        drops    (mapv :drop-probability profiles)]
    {:distinct-profile-count (count (set profiles))
     :delay-ms {:min (apply min delays)
                :max (apply max delays)}
     :jitter-ms {:min (apply min jitters)
                 :max (apply max jitters)}
     :drop-probability {:min (apply min drops)
                        :max (apply max drops)}}))

(defn- behavior->link-behaviors
  [nodes behavior]
  (cond
    (:link-profiles behavior)
    (into (sorted-map)
          (keep (fn [[[src dest] profile]]
                  (when (and (some? src)
                             (some? dest)
                             (not= src dest)
                             (some #{src} nodes)
                             (some #{dest} nodes))
                    [[src dest] (normalized-link-profile profile)])))
          (:link-profiles behavior))

    (:profile behavior)
    (nodes->link-behaviors nodes (:profile behavior))

    :else
    (nodes->link-behaviors nodes behavior)))

(defn- behavior->network-state
  [nodes behavior link-behaviors]
  (cond
    (:link-profiles behavior)
    (merge (select-keys behavior [:kind])
           {:nodes nodes
            :link-profiles link-behaviors
            :profile-summary (or (:profile-summary behavior)
                                 (link-profile-summary link-behaviors))})

    (:profile behavior)
    {:nodes nodes
     :profile (normalized-link-profile (:profile behavior))}

    :else
    {:nodes nodes
     :profile (normalized-link-profile behavior)}))

(defn network-link-behaviors
  [cluster-id]
  (get-in @clusters [cluster-id :link-behaviors]))

(defn network-behavior
  [cluster-id]
  (get-in @clusters [cluster-id :network-behavior]))

(declare heal-network!)

(defn apply-network-shape!
  [cluster-id nodes behavior]
  (let [nodes' (->> nodes
                    (filter some?)
                    distinct
                    sort
                    vec)
        link-behaviors (behavior->link-behaviors nodes' behavior)
        network-state  (behavior->network-state nodes'
                                                behavior
                                                link-behaviors)]
    (heal-network! cluster-id)
    (doseq [[[src-logical-node dest-logical-node]
             {:keys [delay-ms jitter-ms drop-probability]}]
            link-behaviors]
      (PartitionFaults/setLinkBehavior cluster-id
                                       src-logical-node
                                       dest-logical-node
                                       delay-ms
                                       jitter-ms
                                       drop-probability))
    (swap! clusters
           (fn [clusters*]
             (if (contains? clusters* cluster-id)
               (-> clusters*
                   (assoc-in [cluster-id :link-behaviors] link-behaviors)
                   (assoc-in [cluster-id :network-behavior] network-state))
               clusters*)))
    {:cluster-id cluster-id
     :nodes nodes'
     :link-behaviors link-behaviors
     :behavior network-state}))

(defn heal-network!
  [cluster-id]
  (PartitionFaults/healCluster cluster-id)
  (swap! clusters
         (fn [clusters*]
           (if (contains? clusters* cluster-id)
             (-> clusters*
                 (assoc-in [cluster-id :network-grudge] (sorted-map))
                 (assoc-in [cluster-id :dropped-links] #{})
                 (assoc-in [cluster-id :link-behaviors] (sorted-map))
                 (assoc-in [cluster-id :network-behavior] nil))
             clusters*)))
  true)

(defn apply-network-grudge!
  [cluster-id grudge]
  (let [grudge' (normalized-grudge grudge)
        dropped-links (grudge->dropped-links grudge')]
    (heal-network! cluster-id)
    (doseq [[src-logical-node dest-logical-node] dropped-links]
      (PartitionFaults/dropLink cluster-id
                                src-logical-node
                                dest-logical-node))
    (swap! clusters
           (fn [clusters*]
             (if (contains? clusters* cluster-id)
               (-> clusters*
                   (assoc-in [cluster-id :network-grudge] grudge')
                   (assoc-in [cluster-id :dropped-links] dropped-links))
               clusters*)))
    {:cluster-id cluster-id
     :grudge grudge'
     :dropped-links dropped-links}))

(defn network-grudge
  [cluster-id]
  (get-in @clusters [cluster-id :network-grudge]))

(defn leader-partition-grudge
  ([cluster-id]
   (leader-partition-grudge cluster-id
                            (:leader (wait-for-authority-leader! cluster-id))))
  ([cluster-id leader]
   (let [live-nodes (-> (cluster-state cluster-id) :live-nodes sort vec)
         followers  (vec (remove #{leader} live-nodes))]
     (when (seq followers)
       (into {leader followers}
             (map (fn [follower]
                    [follower [leader]]))
             followers)))))

(defn- random-node-groups
  [nodes]
  (let [shuffled    (vec (shuffle nodes))
        group-count (if (= 2 (count shuffled))
                      2
                      (+ 2 (rand-int (dec (count shuffled)))))
        cut-points  (->> (range 1 (count shuffled))
                         shuffle
                         (take (dec group-count))
                         sort
                         vec)
        bounds      (vec (concat [0] cut-points [(count shuffled)]))]
    (->> (partition 2 1 bounds)
         (mapv (fn [[start end]]
                 (vec (subvec shuffled start end)))))))

(defn- add-blocked-links
  [grudge srcs dests]
  (reduce (fn [grudge' dest]
            (update grudge' dest (fnil into []) srcs))
          grudge
          dests))

(defn- pair-cut->grudge
  [grudge groups {:keys [left-group right-group mode]}]
  (let [left  (nth groups left-group)
        right (nth groups right-group)]
    (case mode
      :left->right
      (add-blocked-links grudge left right)

      :right->left
      (add-blocked-links grudge right left)

      :bidirectional
      (-> grudge
          (add-blocked-links left right)
          (add-blocked-links right left))

      grudge)))

(defn- random-pair-cuts
  [group-count]
  (vec
   (for [left-group (range group-count)
         right-group (range (inc left-group) group-count)
         :let [mode (rand-nth graph-cut-direction-modes)]
         :when (not= :none mode)]
     {:left-group left-group
      :right-group right-group
      :mode mode})))

(defn- fallback-graph-cut
  [nodes]
  (let [[src & rest-nodes] nodes
        groups            [(vector src) (vec rest-nodes)]
        pair-cuts         [{:left-group 0
                            :right-group 1
                            :mode :left->right}]
        grudge            (normalized-grudge
                           (pair-cut->grudge {} groups (first pair-cuts)))]
    {:groups groups
     :pair-cuts pair-cuts
     :grudge grudge
     :dropped-links (grudge->dropped-links grudge)}))

(defn random-graph-cut
  [cluster-id]
  (let [live-nodes (-> (cluster-state cluster-id) :live-nodes sort vec)]
    (when (> (count live-nodes) 1)
      (let [total-links (count (nodes->directed-links live-nodes))]
        (loop [attempt 0]
          (if (>= attempt 32)
            (fallback-graph-cut live-nodes)
            (let [groups         (random-node-groups live-nodes)
                  pair-cuts      (random-pair-cuts (count groups))
                  grudge         (normalized-grudge
                                  (reduce (fn [grudge' pair-cut]
                                            (pair-cut->grudge grudge'
                                                              groups
                                                              pair-cut))
                                          {}
                                          pair-cuts))
                  dropped-links  (grudge->dropped-links grudge)]
              (if (and (seq dropped-links)
                       (< (count dropped-links) total-links))
                {:groups groups
                 :pair-cuts pair-cuts
                 :grudge grudge
                 :dropped-links dropped-links}
                (recur (inc attempt))))))))))

(defn- random-link-profiles
  [links]
  (loop [attempt 0]
    (let [link-profiles (into (sorted-map)
                              (map (fn [link]
                                     [link (rand-nth degraded-network-profile-templates)]))
                              links)]
      (if (or (<= (count links) 1)
              (> (count (set (vals link-profiles))) 1)
              (>= attempt 16))
        link-profiles
        (recur (inc attempt))))))

(defn random-degraded-network-shape
  [cluster-id]
  (let [nodes (-> (cluster-state cluster-id) :live-nodes sort vec)]
    (when (> (count nodes) 1)
      (let [link-profiles (random-link-profiles (nodes->directed-links nodes))]
        {:kind :heterogeneous
         :nodes nodes
         :link-profiles link-profiles
         :profile-summary (link-profile-summary link-profiles)}))))

(defrecord LocalClusterNet [cluster-id]
  net.proto/Net
  (drop! [_ _test src dest]
    (apply-network-grudge! cluster-id {dest [src]}))
  (heal! [_ _test]
    (heal-network! cluster-id))
  (slow! [_ _test]
    (apply-network-shape! cluster-id
                          (-> (cluster-state cluster-id) :live-nodes sort vec)
                          default-slow-link-profile))
  (slow! [_ _test opts]
    (let [nodes (or (:nodes opts)
                    (-> (cluster-state cluster-id) :live-nodes sort vec))
          profile (merge default-slow-link-profile
                         (select-keys opts
                                      [:delay-ms :jitter-ms :drop-probability]))]
      (apply-network-shape! cluster-id nodes profile)))
  (flaky! [_ _test]
    (apply-network-shape! cluster-id
                          (-> (cluster-state cluster-id) :live-nodes sort vec)
                          default-flaky-link-profile))
  (fast! [_ _test]
    (heal-network! cluster-id))
  (shape! [_ _test nodes behavior]
    (apply-network-shape! cluster-id nodes behavior))

  net.proto/PartitionAll
  (drop-all! [_ _test grudge]
    (apply-network-grudge! cluster-id grudge)))

(defn- leader-connect-transport-failure?
  [e]
  (boolean
    (some
      (fn [cause]
        (let [message (ex-message cause)
              data    (ex-data cause)]
          (or (instance? ClosedChannelException cause)
              (instance? ConnectException cause)
              (= :open-conn (:phase data))
              (and (string? message)
                   (or (str/includes? message "Socket channel is closed.")
                       (str/includes? message "ClosedChannelException")
                       (str/includes? message "Unable to connect to server:")
                       (str/includes? message "Connection refused")
                       (str/includes? message "Connection reset by peer")
                       (str/includes? message "Broken pipe")
                       (str/includes? message "Timeout in making request"))))))
      (take-while some? (iterate ex-cause e)))))

(defn transport-failure?
  [e]
  (leader-connect-transport-failure? e))

(def ^:private disruption-write-failure-markers
  ["HA write admission rejected"
   "Timed out waiting for single leader"
   "Timeout in making request"
   "No space left on device"])

(def ^:private write-disruption-faults
  #{:leader-failover
    :leader-partition
    :quorum-loss
    :leader-pause
    :node-pause
    :multi-node-pause
    :asymmetric-partition
    :degraded-network
    :leader-io-stall
    :leader-disk-full
    :clock-skew-pause
    :clock-skew-leader-fast
    :clock-skew-leader-slow
    :clock-skew-mixed})

(defn write-disruption-fault-active?
  [test]
  (boolean (some write-disruption-faults (:datalevin/nemesis-faults test))))

(defn- disruption-write-failure-message
  [error]
  (cond
    (string? error)
    error

    (keyword? error)
    (name error)

    (vector? error)
    (some->> error
             (keep disruption-write-failure-message)
             first)

    (map? error)
    (or (disruption-write-failure-message (:message error))
        (disruption-write-failure-message (:error error)))

    (some? error)
    (str error)

    :else
    nil))

(defn expected-disruption-write-failure?
  [test error]
  (and (write-disruption-fault-active? test)
       (when-let [message (disruption-write-failure-message error)]
         (some #(str/includes? message %)
               disruption-write-failure-markers))))

(defn open-leader-conn!
  [test schema]
  (let [cluster-id (:datalevin/cluster-id test)
        deadline   (+ (now-ms) cluster-timeout-ms)]
    (loop []
      (let [leader-node (:leader (wait-for-single-leader! cluster-id))
            leader-uri  (db-uri (endpoint-for-node cluster-id leader-node)
                                (:db-name test))
            outcome     (try
                          {:conn (create-conn-with-timeout! leader-uri
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
              (throw e))))))))

(defn with-leader-conn
  [test schema f]
  (let [conn (open-leader-conn! test schema)]
    (try
      (f conn)
      (finally
        (safe-close-conn! conn)))))

(defn open-node-conn!
  [test logical-node schema]
  (let [cluster-id (:datalevin/cluster-id test)
        deadline   (+ (now-ms) cluster-timeout-ms)]
    (loop []
      (let [node        (get-in @clusters [cluster-id :node-by-name logical-node])
            endpoint    (:endpoint node)
            outcome     (if (string? endpoint)
                          (try
                            {:conn (create-conn-with-timeout!
                                    (db-uri endpoint (:db-name test))
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
  [test logical-node schema f]
  (let [conn (open-node-conn! test logical-node schema)]
    (try
      (f conn)
      (finally
        (safe-close-conn! conn)))))

(defn stopped-node-info
  [cluster-id logical-node]
  (get-in @clusters [cluster-id :stopped-node-info logical-node]))

(defn override-node-ha-opts!
  [cluster-id logical-node override-opts]
  (locking clusters
    (when-let [{:keys [node-by-name]} (get @clusters cluster-id)]
      (when-not (contains? node-by-name logical-node)
        (u/raise "Cannot override HA opts for unknown Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node}))
      (swap! clusters assoc-in
             [cluster-id :node-ha-opt-overrides logical-node]
             override-opts)
      override-opts)))

(defn clear-node-ha-opts-override!
  [cluster-id logical-node]
  (locking clusters
    (let [override (get-in @clusters
                           [cluster-id :node-ha-opt-overrides logical-node])]
      (swap! clusters update-in [cluster-id :node-ha-opt-overrides]
             dissoc logical-node)
      override)))

(defn txlog-retention-state
  [cluster-id logical-node]
  (let [{:keys [db-name servers]} (get @clusters cluster-id)]
    (when-let [state (db-state (get servers logical-node) db-name)]
      (let [store (:store state)
            lmdb  (if (instance? Store store)
                    (.-lmdb ^Store store)
                    store)]
        (when (store-open? lmdb)
          (i/txlog-retention-state lmdb))))))

(defn copy-backup-pin-ids
  [cluster-id logical-node]
  (->> (get-in (txlog-retention-state cluster-id logical-node)
               [:floor-providers :backup :pins])
       (keep (fn [{:keys [pin-id expired?]}]
               (when (and (string? pin-id)
                          (not expired?)
                          (str/starts-with? pin-id "backup-copy/"))
                 pin-id)))
       sort
       vec))

(defn- with-node-kv-store
  [cluster-id logical-node f]
  (let [{:keys [db-name node-by-name]} (get @clusters cluster-id)
        endpoint (get-in node-by-name [logical-node :endpoint])
        kv-store (r/open-kv (db-uri endpoint db-name)
                            {:client-opts conn-client-opts})]
    (try
      (f kv-store)
      (finally
        (i/close-kv kv-store)))))

(defn assoc-opt-on-node!
  [cluster-id logical-node k v]
  (with-node-kv-store
    cluster-id
    logical-node
    (fn [kv-store]
      (i/assoc-opt kv-store k v))))

(defn assoc-opt-on-node-store!
  [cluster-id logical-node k v]
  (let [{:keys [db-name admin-conns]} (get @clusters cluster-id)
        store (some-> (get admin-conns logical-node)
                      deref
                      .-store)]
    (when-not store
      (u/raise "Cannot update remote store opt on unavailable Jepsen node"
               {:cluster-id cluster-id
                :logical-node logical-node
                :db-name db-name}))
    (i/assoc-opt store k v)))

(defn assoc-opt-on-stopped-node-store!
  [cluster-id logical-node k v]
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
    (let [store (#'srv/open-store root db-name nil true)]
      (try
        (i/assoc-opt store k v)
        (finally
          (i/close store))))))

(defn clear-copy-backup-pins-on-node!
  [cluster-id logical-node]
  (let [{:keys [db-name servers]} (get @clusters cluster-id)
        state (db-state (get servers logical-node) db-name)]
    (when-not state
      (u/raise "Cannot clear copy backup pins on unavailable Jepsen node"
               {:cluster-id cluster-id
                :logical-node logical-node}))
    (let [pin-ids  (copy-backup-pin-ids cluster-id logical-node)]
      (when (seq pin-ids)
        (with-node-kv-store
          cluster-id
          logical-node
          (fn [kv-store]
            (doseq [pin-id pin-ids]
              (i/txlog-unpin-backup-floor! kv-store pin-id)))))
      {:cleared-pin-ids pin-ids
       :remaining-pin-ids (copy-backup-pin-ids cluster-id logical-node)})))

(defn create-snapshot-on-node!
  [cluster-id logical-node]
  (let [{:keys [db-name servers]} (get @clusters cluster-id)
        state (db-state (get servers logical-node) db-name)]
    (when-not state
      (u/raise "Cannot create snapshot on unavailable Jepsen node"
               {:cluster-id cluster-id
                :logical-node logical-node}))
    (let [store  (:store state)
          lmdb   (if (instance? Store store)
                   (.-lmdb ^Store store)
                   store)
          result (i/create-snapshot! lmdb)]
      (when-not (:ok? result)
        (u/raise "Jepsen snapshot creation failed"
                 {:cluster-id cluster-id
                  :logical-node logical-node
                  :result result}))
      result)))

(defn create-snapshots-on-nodes!
  [cluster-id logical-nodes]
  (into {}
        (map (fn [logical-node]
               [logical-node
                (create-snapshot-on-node! cluster-id logical-node)]))
        logical-nodes))

(defn gc-txlog-segments-on-node!
  [cluster-id logical-node]
  (let [{:keys [db-name servers]} (get @clusters cluster-id)
        state (db-state (get servers logical-node) db-name)]
    (when-not state
      (u/raise "Cannot GC WAL segments on unavailable Jepsen node"
               {:cluster-id cluster-id
                :logical-node logical-node}))
    (let [store  (:store state)
          lmdb   (if (instance? Store store)
                   (.-lmdb ^Store store)
                   store)
          result (i/gc-txlog-segments! lmdb)]
      (when-not (:ok? result)
        (u/raise "Jepsen WAL GC failed"
                 {:cluster-id cluster-id
                  :logical-node logical-node
                  :result result}))
      result)))

(defn wait-for-follower-bootstrap!
  ([cluster-id logical-node min-snapshot-lsn]
   (wait-for-follower-bootstrap! cluster-id logical-node min-snapshot-lsn
                                 cluster-timeout-ms))
  ([cluster-id logical-node min-snapshot-lsn timeout-ms]
   (let [timeout-ms (long timeout-ms)
         deadline   (+ (now-ms) timeout-ms)
         min-snapshot-lsn (long min-snapshot-lsn)]
     (loop [last-state nil]
       (let [state (node-diagnostics cluster-id logical-node)
             applied-lsn (long (or (:ha-local-last-applied-lsn state) 0))
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
                            :last-state last-state}))))))))

(defn stop-node!
  [cluster-id logical-node]
  (locking clusters
    (when-let [cluster (get @clusters cluster-id)]
      (let [server   (get-in cluster [:servers logical-node])
            db-name  (:db-name cluster)
            db-state (db-state server db-name)
            stopped-info
            (when db-state
              {:stopped-at-ms (now-ms)
               :ha-role (:ha-role db-state)
               :effective-local-lsn (effective-local-lsn cluster-id logical-node)
               :node-diagnostics (node-diagnostics cluster-id logical-node)})]
        (safe-close-conn! (get-in cluster [:admin-conns logical-node]))
        (safe-stop-server! (get-in cluster [:servers logical-node]))
        (swap! clusters
               (fn [clusters*]
                 (-> clusters*
                     (assoc-in [cluster-id :admin-conns logical-node] nil)
                     (assoc-in [cluster-id :servers logical-node] nil)
                     (assoc-in [cluster-id :stopped-node-info logical-node]
                               stopped-info)
                     (update-in [cluster-id :paused-node-info]
                                dissoc
                                logical-node)
                     (update-in [cluster-id :paused-nodes] disj logical-node)
                     (update-in [cluster-id :live-nodes] disj logical-node))))))))

(defn restart-node!
  [cluster-id logical-node]
  (locking clusters
    (let [{:keys [db-name schema base-opts node-by-name verbose?
                  control-backend node-ha-opt-overrides]}
          (get @clusters cluster-id)]
      (if (get-in @clusters [cluster-id :servers logical-node])
        true
        (let [node     (get node-by-name logical-node)
              override (get node-ha-opt-overrides logical-node)
              server   (start-server! node verbose?)]
          (try
            (let [conn (with-control-backend
                         control-backend
                         #(open-ha-conn! node
                                         db-name
                                         nil
                                         (node-ha-opts base-opts
                                                       node
                                                       override)))]
              (swap! clusters
                     (fn [clusters*]
                       (-> clusters*
                           (assoc-in [cluster-id :servers logical-node] server)
                           (assoc-in [cluster-id :admin-conns logical-node] conn)
                           (update-in [cluster-id :stopped-node-info]
                                      dissoc logical-node)
                           (update-in [cluster-id :paused-node-info]
                                      dissoc logical-node)
                           (update-in [cluster-id :paused-nodes] disj logical-node)
                           (update-in [cluster-id :live-nodes] conj logical-node))))
              true)
            (catch Throwable e
              (safe-stop-server! server)
              (throw e))))))))

(defn- pause-server-loop!
  [^Server server]
  (let [running  (.-running server)
        selector ^java.nio.channels.Selector (.-selector server)]
    (.set ^java.util.concurrent.atomic.AtomicBoolean running false)
    (.wakeup selector)))

(defn- disconnect-server-client-channels!
  [^Server server]
  (let [client-ids (mapv key (seq (.-clients server)))
        selector   ^java.nio.channels.Selector (.-selector server)]
    (doseq [client-id client-ids]
      (try
        (#'srv/disconnect-client* server client-id)
        (catch Throwable _ nil)))
    (when (.isOpen selector)
      (doseq [^java.nio.channels.SelectionKey skey (.keys selector)
              :let [channel (.channel skey)]
              :when (instance? java.nio.channels.SocketChannel channel)]
        (try
          (.close ^java.nio.channels.SocketChannel channel)
          (catch Throwable _ nil))))))

(defn- rebuild-node-ha-runtime!
  [control-backend ^Server server db-name]
  (when-not server
    (u/raise "Cannot rebuild HA runtime for missing Jepsen server"
             {:db-name db-name}))
  (when-not (db-state server db-name)
    (u/raise "Cannot rebuild HA runtime for missing Jepsen db state"
             {:db-name db-name}))
  (with-control-backend
    control-backend
    #(do
       (#'srv/update-db
        server
        db-name
        (fn [m]
          (if-let [store (:store m)]
            (#'srv/ensure-ha-runtime (.-root server) db-name m store)
            m)))
       (#'srv/ensure-ha-renew-loop server db-name))))

(defn pause-node!
  [cluster-id logical-node]
  (locking clusters
    (when-let [cluster (get @clusters cluster-id)]
      (when-not (contains? (:live-nodes cluster) logical-node)
        (u/raise "Cannot pause unavailable Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node}))
      (when-not (contains? (:paused-nodes cluster) logical-node)
        (let [server   (get-in cluster [:servers logical-node])
              db-name  (:db-name cluster)
              db-state (db-state server db-name)
              conn     (get-in cluster [:admin-conns logical-node])
              pause-info
              (when db-state
                {:paused-at-ms (now-ms)
                 :ha-role (:ha-role db-state)
                 :effective-local-lsn (effective-local-lsn cluster-id logical-node)
                 :node-diagnostics (node-diagnostics cluster-id logical-node)})]
          ;; Close the harness admin connection before stalling the server loop.
          ;; Once the node is paused, remote close can block forever waiting on a
          ;; server that no longer services requests.
          (safe-close-conn! conn)
          (when-let [authority (:ha-authority db-state)]
            (#'srv/stop-ha-renew-loop db-state)
            (ctrl/stop-authority! authority))
          (when server
            ;; Pause emulation keeps the listener bound, so drop live channels
            ;; before halting the event loop to keep clients from hanging.
            (disconnect-server-client-channels! server)
            (pause-server-loop! server))
          (swap! clusters
                 (fn [clusters*]
                   (-> clusters*
                       (assoc-in [cluster-id :admin-conns logical-node] nil)
                       (assoc-in [cluster-id :paused-node-info logical-node]
                                 pause-info)
                       (update-in [cluster-id :paused-nodes] conj logical-node)))))))
    true))

(defn resume-node!
  [cluster-id logical-node]
  (locking clusters
    (when-let [cluster (get @clusters cluster-id)]
      (when-not (contains? (:paused-nodes cluster) logical-node)
        (u/raise "Cannot resume unpaused Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node}))
      (let [{:keys [db-name node-by-name control-backend]} cluster
            server   (get-in cluster [:servers logical-node])
            node     (get node-by-name logical-node)
            _        (when-not server
                       (u/raise "Cannot resume missing Jepsen server"
                                {:cluster-id cluster-id
                                 :logical-node logical-node}))
            _        (when-not node
                       (u/raise "Cannot resume unknown Jepsen node"
                                {:cluster-id cluster-id
                                 :logical-node logical-node}))]
        (srv/start server)
        (rebuild-node-ha-runtime! control-backend server db-name)
        ;; The harness admin connection is only used for setup/teardown.
        ;; Reopening it synchronously here can wedge the nemesis on a node
        ;; that is still coming back, so leave it nil after resume.
        (swap! clusters
               (fn [clusters*]
                 (-> clusters*
                     (assoc-in [cluster-id :admin-conns logical-node] nil)
                     (update-in [cluster-id :paused-node-info]
                                dissoc
                                logical-node)
                     (update-in [cluster-id :paused-nodes] disj logical-node))))))
    true))

(defn wedge-node-storage!
  [cluster-id logical-node fault]
  (locking clusters
    (when-let [cluster (get @clusters cluster-id)]
      (when-not (contains? (:live-nodes cluster) logical-node)
        (u/raise "Cannot wedge storage on unavailable Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node}))
      (let [fault* (assoc (normalize-storage-fault fault)
                          :faulted-at-ms (now-ms))]
        (swap! clusters assoc-in [cluster-id :storage-faults logical-node] fault*)
        fault*))))

(defn heal-node-storage!
  [cluster-id logical-node]
  (locking clusters
    (let [fault (storage-fault cluster-id logical-node)]
      (swap! clusters update-in [cluster-id :storage-faults] dissoc logical-node)
      fault)))

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
  (let [data-logical-nodes
        (vec (or (seq (:nodes test)) default-nodes))
        control-logical-nodes
        (vec (or (seq (:datalevin/control-nodes test))
                 data-logical-nodes))
        control-backend  (:control-backend test)
        _                (validate-cluster-topology!
                          data-logical-nodes
                          control-logical-nodes
                          control-backend)
        work-dir         (resolve-work-dir cluster-id test)
        all-nodes        (make-nodes work-dir control-logical-nodes)
        data-node-set    (set data-logical-nodes)
        data-nodes       (->> all-nodes
                              (filter #(contains? data-node-set
                                                  (:logical-node %)))
                              vec)
        control-only-nodes
        (->> all-nodes
             (remove #(contains? data-node-set (:logical-node %)))
             vec)
        db-name          (:db-name test)
        schema           (:schema test)
        nemesis-faults   (set (:datalevin/nemesis-faults test))
        clock-skew?      (some #{:clock-skew-pause
                                 :clock-skew-leader-fast
                                 :clock-skew-leader-slow
                                 :clock-skew-mixed}
                               nemesis-faults)
        clock-skew-dir   (when clock-skew?
                           (str work-dir u/+separator+ "clock-skew"))
        group-id         (str "datalevin-jepsen-" cluster-id)
        db-identity      (str "db-" (UUID/randomUUID))
        cluster-opts     (or (:datalevin/cluster-opts test) {})
        server-runtime-opts-override
        (:datalevin/server-runtime-opts-fn test)
        base-opts        (cond-> (merge (base-ha-opts
                                          data-nodes
                                          all-nodes
                                          group-id
                                          db-identity
                                          control-backend)
                                        cluster-opts)
                           clock-skew?
                           (assoc :ha-clock-skew-hook
                                  (clock-skew-hook-config clock-skew-dir)))
        verbose?         (boolean (:verbose test))
        setup-timeout-ms (cluster-setup-timeout-ms base-opts)
        servers-atom     (atom {})
        conns-atom       (atom {})
        authorities-atom (atom {})]
    (try
      (doseq [{:keys [logical-node peer-id]} all-nodes]
        (PartitionFaults/registerPeer cluster-id logical-node peer-id))
      (install-server-runtime-opts-overrides!
       db-name
       data-nodes
       server-runtime-opts-override)
      (when clock-skew?
        (doseq [{:keys [node-id]} all-nodes]
          (write-clock-skew-ms! clock-skew-dir node-id 0)))
      (with-control-backend
        control-backend
        (fn []
          (doseq [node control-only-nodes]
            (swap! authorities-atom assoc
                   (:logical-node node)
                   (start-control-authority! work-dir base-opts node)))
          (doseq [node data-nodes]
            (swap! servers-atom assoc
                   (:logical-node node)
                   (start-server! node verbose?)))
          ;; JRaft-backed startup needs peers to enter the open path together;
          ;; a sequential open can deadlock waiting for a quorum that has not
          ;; started its authority yet.
          (doseq [[logical-node conn]
                  (->> data-nodes
                       (mapv (fn [node]
                               (future
                                 [(:logical-node node)
                                  (open-ha-conn! node
                                                 db-name
                                                 schema
                                                 (node-ha-opts base-opts node)
                                                 setup-timeout-ms)])))
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
                     :nodes           data-nodes
                     :control-nodes   all-nodes
                     :data-node-names data-logical-nodes
                     :control-node-names control-logical-nodes
                     :control-only-node-names
                     (vec (map :logical-node control-only-nodes))
                     :node-by-id      (into {}
                                            (map (juxt :node-id :logical-node))
                                            all-nodes)
                     :node-by-name    (into {}
                                            (map (juxt :logical-node identity))
                                            all-nodes)
                     :endpoint->node  (into {}
                                            (map (juxt :endpoint :logical-node))
                                            all-nodes)
                     :peer-id->node   (into {}
                                            (map (juxt :peer-id :logical-node))
                                            all-nodes)
                     :servers         @servers-atom
                     :control-authorities @authorities-atom
                     :admin-conns     @conns-atom
                     :live-nodes      (set data-logical-nodes)
                     :network-grudge  (sorted-map)
                     :dropped-links   #{}
                     :link-behaviors  (sorted-map)
                     :network-behavior nil
                     :paused-nodes    #{}
                     :paused-node-info {}
                     :node-ha-opt-overrides {}
                     :storage-faults  {}
                     :stopped-node-info {}
                     :teardown-nodes  #{}}]
        (swap! clusters assoc cluster-id cluster)
        (wait-for-single-leader! cluster-id setup-timeout-ms)
        cluster)
      (catch Throwable e
        (doseq [conn (vals @conns-atom)]
          (safe-close-conn! conn))
        (doseq [authority (vals @authorities-atom)]
          (safe-stop-authority! authority))
        (doseq [server (vals @servers-atom)]
          (safe-stop-server! server))
        (clear-server-runtime-opts-overrides! db-name data-nodes)
        (PartitionFaults/unregisterCluster cluster-id)
        (safe-delete-dir! work-dir)
        (throw e)))))

(defn- teardown-cluster!
  [cluster-id]
  (when-let [{:keys [admin-conns servers control-authorities work-dir
                     keep-work-dir? db-name nodes]}
             (get @clusters cluster-id)]
    (heal-network! cluster-id)
    (doseq [conn (vals admin-conns)]
      (safe-close-conn! conn))
    (doseq [authority (vals control-authorities)]
      (safe-stop-authority! authority))
    (doseq [server (vals servers)]
      (safe-stop-server! server))
    (clear-server-runtime-opts-overrides! db-name nodes)
    (PartitionFaults/unregisterCluster cluster-id)
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

(defn net
  [cluster-id]
  (->LocalClusterNet cluster-id))
