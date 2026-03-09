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
   [datalevin.server :as srv]
   [datalevin.util :as u]
   [jepsen.db :as db]
   [jepsen.net.proto :as net.proto])
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
(def ^:private cluster-timeout-ms 10000)
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

(def default-nodes ["n1" "n2" "n3"])

(defonce ^:private clusters (atom {}))

(def ^:private base-fetch-ha-leader-txlog-batch dha/fetch-ha-leader-txlog-batch)
(def ^:private base-report-ha-replica-floor! dha/report-ha-replica-floor!)
(def ^:private base-fetch-ha-endpoint-snapshot-copy!
  dha/fetch-ha-endpoint-snapshot-copy!)

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

(defn- reserve-port-socket
  [chosen]
  (loop [attempt 0]
    (when (>= attempt 256)
      (u/raise "Unable to reserve Jepsen server port"
               {:chosen chosen
                :attempt attempt}))
    (let [candidate (random-port-candidate)]
      (if (contains? chosen candidate)
        (recur (inc attempt))
        (let [socket (try
                       (doto (ServerSocket.)
                         (.bind (InetSocketAddress. candidate)))
                       (catch Exception _
                         ::retry))]
          (if (identical? ::retry socket)
            (recur (inc attempt))
            socket))))))

(defn- reserve-ports
  [n]
  (let [sockets (loop [chosen  #{}
                       sockets []]
                  (if (= n (count sockets))
                    sockets
                    (let [socket (reserve-port-socket chosen)
                          port   (.getLocalPort ^ServerSocket socket)]
                      (recur (conj chosen port)
                             (conj sockets socket)))))]
    (try
      (mapv (fn [^ServerSocket socket]
              (.getLocalPort socket))
            sockets)
      (finally
        (doseq [^ServerSocket socket sockets]
          (try
            (.close socket)
            (catch Exception _
              nil)))))))

(defn- make-nodes
  [work-dir logical-nodes]
  (let [ports (reserve-ports 6)]
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

(defn- create-conn-with-timeout!
  ([uri schema]
   (create-conn-with-timeout! uri schema cluster-timeout-ms))
  ([uri schema timeout-ms]
   (let [timeout-ms (long timeout-ms)
         timed-out? (atom false)
         result-f   (future
                      (try
                        (let [conn (d/create-conn uri
                                                  schema
                                                  {:client-opts
                                                   conn-client-opts})]
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

(defn paused-node-info
  [cluster-id logical-node]
  (get-in @clusters [cluster-id :paused-node-info logical-node]))

(defn- authority-leader-logical-node
  [cluster-id peer-id]
  (get-in @clusters [cluster-id :peer-id->node peer-id]))

(declare node-diagnostics)

(defn wait-for-authority-leader!
  ([cluster-id]
   (wait-for-authority-leader! cluster-id cluster-timeout-ms))
  ([cluster-id timeout-ms]
   (let [timeout-ms (long timeout-ms)
         deadline   (+ (now-ms) timeout-ms)]
     (loop [last-snapshot nil]
       (let [{:keys [live-nodes]} (get @clusters cluster-id)
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
                            live-nodes))
             quorum-size (inc (quot (count live-nodes) 2))
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
  (let [{:keys [db-name servers]} (get @clusters cluster-id)]
    (when-let [state (db-state (get servers logical-node) db-name)]
      (let [authority-diagnostics
            (when-let [authority (:ha-authority state)]
              (ctrl/authority-diagnostics authority))]
        {:ha-role (:ha-role state)
         :ha-authority-owner-node-id (:ha-authority-owner-node-id state)
         :ha-authority-term (:ha-authority-term state)
         :ha-control-local-peer-id (:local-peer-id authority-diagnostics)
         :ha-control-leader-peer-id (:leader-peer-id authority-diagnostics)
         :ha-control-node-leader? (:node-leader? authority-diagnostics)
         :ha-control-node-state (some-> (:node-state authority-diagnostics)
                                        str)
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
         :jepsen-paused? (contains? (get-in @clusters [cluster-id :paused-nodes])
                                    logical-node)
         :ha-effective-local-lsn (effective-local-lsn cluster-id logical-node)}))))

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

(defn network-link-behaviors
  [cluster-id]
  (get-in @clusters [cluster-id :link-behaviors]))

(defn network-behavior
  [cluster-id]
  (get-in @clusters [cluster-id :network-behavior]))

(declare heal-network!)

(defn apply-network-shape!
  [cluster-id nodes profile]
  (let [nodes' (->> nodes
                    (filter some?)
                    distinct
                    sort
                    vec)
        link-behaviors (nodes->link-behaviors nodes' profile)]
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
                   (assoc-in [cluster-id :network-behavior]
                             {:nodes nodes'
                              :profile (normalized-link-profile profile)}))
               clusters*)))
    {:cluster-id cluster-id
     :nodes nodes'
     :link-behaviors link-behaviors}))

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

(defn random-graph-cut
  [cluster-id]
  (let [live-nodes (-> (cluster-state cluster-id) :live-nodes sort vec)]
    (when (> (count live-nodes) 1)
      (let [shuffled  (vec (shuffle live-nodes))
            split-idx (inc (rand-int (dec (count shuffled))))
            left      (vec (take split-idx shuffled))
            right     (vec (drop split-idx shuffled))
            direction (rand-nth [:left->right :right->left :bidirectional])
            grudge    (case direction
                        :left->right
                        (into (sorted-map)
                              (map (fn [dest] [dest left]))
                              right)
                        :right->left
                        (into (sorted-map)
                              (map (fn [dest] [dest right]))
                              left)
                        :bidirectional
                        (into (sorted-map)
                              (concat (map (fn [dest] [dest left]) right)
                                      (map (fn [dest] [dest right]) left))))]
        {:left left
         :right right
         :direction direction
         :grudge grudge}))))

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

(defn transport-failure?
  [e]
  (leader-connect-transport-failure? e))

(def ^:private disruption-write-failure-markers
  ["HA write admission rejected"
   "Timed out waiting for single leader"
   "Timeout in making request"])

(def ^:private write-disruption-faults
  #{:leader-partition
    :leader-pause
    :asymmetric-partition
    :degraded-network})

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
        (d/close conn)))))

(defn stopped-node-info
  [cluster-id logical-node]
  (get-in @clusters [cluster-id :stopped-node-info logical-node]))

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
                (>= snapshot-lsn min-snapshot-lsn)
                (not (:ha-follower-degraded? state))
                (nil? (:ha-follower-last-error state)))
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
                   (update-in [cluster-id :stopped-node-info] dissoc logical-node)
                   (update-in [cluster-id :paused-node-info] dissoc logical-node)
                   (update-in [cluster-id :paused-nodes] disj logical-node)
                   (update-in [cluster-id :live-nodes] conj logical-node))))
      true)))

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
              pause-info
              (when db-state
                {:paused-at-ms (now-ms)
                 :ha-role (:ha-role db-state)
                 :effective-local-lsn (effective-local-lsn cluster-id logical-node)
                 :node-diagnostics (node-diagnostics cluster-id logical-node)})]
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
      (let [server   (get-in cluster [:servers logical-node])
            db-name  (:db-name cluster)
            db-state (db-state server db-name)]
        (when-let [authority (:ha-authority db-state)]
          (ctrl/start-authority! authority)
          (#'srv/ensure-ha-renew-loop server db-name))
        (srv/start server)
        (swap! clusters
               (fn [clusters*]
                 (-> clusters*
                     (update-in [cluster-id :paused-node-info]
                                dissoc
                                logical-node)
                     (update-in [cluster-id :paused-nodes] disj logical-node))))))
    true))

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
        cluster-opts     (or (:datalevin/cluster-opts test) {})
        base-opts        (cond-> (merge (base-ha-opts
                                          nodes
                                          group-id
                                          db-identity
                                          control-backend)
                                        cluster-opts)
                           clock-skew?
                           (assoc :ha-clock-skew-hook
                                  (clock-skew-hook-config clock-skew-dir)))
        verbose?         (boolean (:verbose test))
        servers-atom     (atom {})
        conns-atom       (atom {})]
    (try
      (doseq [{:keys [logical-node peer-id]} nodes]
        (PartitionFaults/registerPeer cluster-id logical-node peer-id))
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
                     :node-by-id      (into {}
                                            (map (juxt :node-id :logical-node))
                                            nodes)
                     :node-by-name    (into {}
                                            (map (juxt :logical-node identity))
                                            nodes)
                     :endpoint->node  (into {}
                                            (map (juxt :endpoint :logical-node))
                                            nodes)
                     :peer-id->node   (into {}
                                            (map (juxt :peer-id :logical-node))
                                            nodes)
                     :servers         @servers-atom
                     :admin-conns     @conns-atom
                     :live-nodes      (set logical-nodes)
                     :network-grudge  (sorted-map)
                     :dropped-links   #{}
                     :link-behaviors  (sorted-map)
                     :network-behavior nil
                     :paused-nodes    #{}
                     :paused-node-info {}
                     :stopped-node-info {}
                     :teardown-nodes  #{}}]
        (swap! clusters assoc cluster-id cluster)
        (wait-for-single-leader! cluster-id)
        cluster)
      (catch Throwable e
        (doseq [conn (vals @conns-atom)]
          (safe-close-conn! conn))
        (doseq [server (vals @servers-atom)]
          (safe-stop-server! server))
        (PartitionFaults/unregisterCluster cluster-id)
        (safe-delete-dir! work-dir)
        (throw e)))))

(defn- teardown-cluster!
  [cluster-id]
  (when-let [{:keys [admin-conns servers work-dir keep-work-dir?]} (get @clusters cluster-id)]
    (heal-network! cluster-id)
    (doseq [conn (vals admin-conns)]
      (safe-close-conn! conn))
    (doseq [server (vals servers)]
      (safe-stop-server! server))
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
