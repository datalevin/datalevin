(ns datalevin.jepsen.local.cluster
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ctrl]
   [datalevin.interface :as i]
   [datalevin.jepsen.local.remote :as lremote]
   [datalevin.server :as srv]
   [datalevin.util :as u])
  (:import
   [datalevin.jepsen PartitionFaults]
   [datalevin.server Server]
   [datalevin.storage Store]
   [java.net InetSocketAddress ServerSocket]
   [java.util UUID]))

(def ^:private default-port-base 19001)
(def ^:private default-port-limit 31999)
(def ^:private port-block-size 32)
(def ^:private cluster-timeout-ms 10000)
(def ^:private default-cluster-setup-timeout-ms 30000)
(def ^:private conn-client-opts {:pool-size 1 :time-out cluster-timeout-ms})
(def ^:private leader-connect-retry-sleep-ms 250)
(def ^:private node-store-release-poll-ms 50)

(def ^:private base-server-runtime-opts-fn srv/*server-runtime-opts-fn*)
(defonce ^:private server-runtime-opts-overrides (atom {}))

(defn- existing-canonical-path
  [& path-parts]
  (let [^java.io.File file (apply io/file path-parts)]
    (when (.exists file)
      (.getCanonicalPath file))))

(defn- now-ms
  []
  (System/currentTimeMillis))

(defn admin-uri
  [endpoint]
  (str "dtlv://" c/default-username ":" c/default-password "@" endpoint))

(defn db-uri
  [endpoint db-name]
  (str (admin-uri endpoint) "/" db-name))

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
  [next-port-block n]
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
  [next-port-block work-dir logical-nodes]
  (let [node-count (count logical-nodes)
        ports      (reserve-ports next-port-block (* 2 node-count))]
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
   {:backend control-backend
    :group-id group-id
    :voters (control-voters data-nodes control-nodes)
    :rpc-timeout-ms 5000
    :election-timeout-ms 5000
    :operation-timeout-ms 30000}})

(defn- clock-skew-script-path
  []
  (or (existing-canonical-path "." "script" "ha" "clock-skew-file.sh")
      (existing-canonical-path ".." "script" "ha" "clock-skew-file.sh")
      (existing-canonical-path ".." "dtlvtest" "script" "ha"
                               "clock-skew-file.sh")
      (existing-canonical-path ".." ".." "dtlvtest" "script" "ha"
                               "clock-skew-file.sh")
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

(defn write-clock-skew-ms!
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

(defn node-ha-opts
  ([base-opts node]
   (node-ha-opts base-opts node nil))
  ([base-opts node override-opts]
   (-> base-opts
       (assoc :ha-node-id (:node-id node))
       (assoc-in [:ha-control-plane :local-peer-id] (:peer-id node))
       (merge-ha-opts override-opts))))

(defn cluster-setup-timeout-ms
  [base-opts]
  (let [control-plane (:ha-control-plane base-opts)
        election-ms   (long (or (:election-timeout-ms control-plane) 0))
        operation-ms  (long (or (:operation-timeout-ms control-plane)
                                default-cluster-setup-timeout-ms))]
    (long (max default-cluster-setup-timeout-ms
               (+ election-ms operation-ms 10000)))))

(defn workload-setup-timeout-ms
  [{:keys [clusters]} cluster-id default-timeout-ms]
  (let [default-timeout-ms (long default-timeout-ms)]
    (long
     (max default-timeout-ms
          (or (some-> (get-in @clusters [cluster-id :base-opts])
                      cluster-setup-timeout-ms)
              default-cluster-setup-timeout-ms)))))

(defn- start-server!
  [{:keys [port root]} verbose?]
  (u/create-dirs root)
  (let [server (binding [c/*db-background-sampling?* false]
                 (srv/create (cond-> {:port port :root root}
                               verbose? (assoc :verbose true))))]
    (binding [c/*db-background-sampling?* false]
      (srv/start server))
    server))

(defn safe-close-conn!
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

(defn- node-db-dir
  [root db-name]
  (str root u/+separator+ (u/hexify-string db-name)))

(defn- multiple-lmdb-open-error?
  [e]
  (str/includes? (or (ex-message e) "")
                 "Please do not open multiple LMDB connections"))

(defn- close-opened-node-store!
  [store]
  (when store
    (if (instance? Store store)
      (i/close store)
      (i/close-kv store))))

(defn- node-store-released?
  [root db-name]
  (let [dir (io/file (node-db-dir root db-name))]
    (or (not (.exists dir))
        (try
          (let [store (#'srv/open-store root db-name nil true)]
            (try
              true
              (finally
                (close-opened-node-store! store))))
          (catch Throwable e
            (if (multiple-lmdb-open-error? e)
              false
              (throw e)))))))

(defn wait-for-node-store-released!
  [{:keys [clusters]} cluster-id logical-node timeout-ms]
  (when-not (true? (get-in @clusters [cluster-id :remote?]))
    (let [{:keys [db-name node-by-name]} (get @clusters cluster-id)
          root (get-in node-by-name [logical-node :root])
          deadline (+ (System/currentTimeMillis) (long timeout-ms))]
      (when root
        (loop []
          (if (node-store-released? root db-name)
            true
            (if (< (System/currentTimeMillis) deadline)
              (do
                (Thread/sleep (long node-store-release-poll-ms))
                (recur))
              (u/raise "Timed out waiting for Jepsen node store release"
                       {:cluster-id cluster-id
                        :logical-node logical-node
                        :db-name db-name
                        :timeout-ms timeout-ms
                        :root root}))))))))

(defn- normalize-server-runtime-opts-override
  [override]
  (cond
    (ifn? override) override
    (map? override) (constantly override)
    :else nil))

(defn- override-key
  [root db-name]
  [root db-name])

(defn resolved-server-runtime-opts
  [server db-name store m]
  (let [root          (some-> ^Server server .-root)
        override-fn   (get @server-runtime-opts-overrides
                           (override-key root db-name))
        base-opts     (base-server-runtime-opts-fn server db-name store m)
        override-opts (when override-fn
                        (override-fn server db-name store m))]
    (cond
      (and (map? base-opts) (map? override-opts))
      (merge base-opts override-opts)

      (map? override-opts)
      override-opts

      :else
      base-opts)))

(defn install-server-runtime-opts-overrides!
  [db-name nodes override]
  (when-let [override-fn (normalize-server-runtime-opts-override override)]
    (swap! server-runtime-opts-overrides
           (fn [m]
             (reduce (fn [acc {:keys [root]}]
                       (assoc acc (override-key root db-name) override-fn))
                     m
                     nodes)))
    true))

(defn clear-server-runtime-opts-overrides!
  [db-name nodes]
  (swap! server-runtime-opts-overrides
         (fn [m]
           (reduce (fn [acc {:keys [root]}]
                     (dissoc acc (override-key root db-name)))
                   m
                   nodes))))

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

(defn create-conn-with-timeout!
  ([uri schema]
   (create-conn-with-timeout! uri schema {} cluster-timeout-ms))
  ([uri schema timeout-ms]
   (create-conn-with-timeout! uri schema {} timeout-ms))
  ([uri schema opts timeout-ms]
   (let [timeout-ms (long timeout-ms)
         timed-out? (atom false)
         result-f   (future
                      (try
                        (let [create-conn (var-get #'d/create-conn)
                              conn (create-conn uri
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

(defn- open-ha-conn!
  [{:keys [transport-failure?]} node db-name schema opts timeout-ms]
  (let [uri        (db-uri (:endpoint node) db-name)
        timeout-ms (long timeout-ms)
        deadline   (+ (now-ms) timeout-ms)]
    (loop []
      (let [remaining-ms       (- deadline (now-ms))
            attempt-timeout-ms (long (max 1 (min timeout-ms remaining-ms)))
            outcome            (try
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
              (throw e))))))))

(defn- with-control-backend
  [control-backend f]
  (with-redefs [srv/*start-ha-authority-fn*
                (fn [db-name' ha-opts]
                  (let [ha-opts' (assoc-in ha-opts
                                           [:ha-control-plane :backend]
                                           control-backend)
                        local-peer-id (get-in ha-opts'
                                              [:ha-control-plane
                                               :local-peer-id])
                        start-authority! ctrl/start-authority!]
                    (with-redefs [ctrl/start-authority!
                                  (fn [authority]
                                    (PartitionFaults/setCurrentLocalPeerId
                                     local-peer-id)
                                    (try
                                      (start-authority! authority)
                                      (finally
                                        (PartitionFaults/clearCurrentLocalPeerId))))]
                      (dha/start-ha-authority db-name' ha-opts'))))
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

(defn- db-state
  [server db-name]
  (when server
    (get (.-dbs ^Server server) db-name)))

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
       (#'srv/ensure-ha-renew-loop server db-name)
       (#'srv/ensure-ha-follower-sync-loop server db-name))))

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

(defn init-cluster!
  [{:keys [clusters next-port-block default-nodes remote-deps fault-deps
           wait-for-single-leader! node-diagnostics effective-local-lsn
           transport-failure?] :as deps}
   cluster-id test]
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
        all-nodes        (make-nodes next-port-block work-dir control-logical-nodes)
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
          (doseq [[logical-node conn]
                  (->> data-nodes
                       (mapv (fn [node]
                               (future
                                 [(:logical-node node)
                                  (open-ha-conn! {:transport-failure? transport-failure?}
                                                 node
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

(defn teardown-cluster!
  [{:keys [clusters fault-deps]} cluster-id]
  (when-let [{:keys [admin-conns servers control-authorities work-dir
                     keep-work-dir? db-name nodes]}
             (get @clusters cluster-id)]
    ((:heal-network! (fault-deps)) cluster-id)
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

(defn stop-node!
  [{:keys [clusters remote-deps node-diagnostics effective-local-lsn] :as deps}
   cluster-id logical-node]
  (locking clusters
    (when-let [cluster (get @clusters cluster-id)]
      (if (:remote? cluster)
        (let [{:keys [node-by-name ssh]} cluster
              node         (get node-by-name logical-node)
              repo-root    (lremote/remote-node-repo-root cluster node)
              rdeps        (remote-deps)
              stopped-info {:stopped-at-ms (now-ms)
                            :ha-role (:ha-role (node-diagnostics cluster-id logical-node))
                            :effective-local-lsn
                            (effective-local-lsn cluster-id logical-node)
                            :node-diagnostics
                            (node-diagnostics cluster-id logical-node)}]
          (lremote/close-remote-admin-client! rdeps cluster-id logical-node)
          (lremote/stop-remote-node-launcher! rdeps ssh repo-root node)
          (swap! clusters
                 (fn [clusters*]
                   (-> clusters*
                       (assoc-in [cluster-id :remote-admin-clients logical-node]
                                 nil)
                       (assoc-in [cluster-id :stopped-node-info logical-node]
                                 stopped-info)
                       (update-in [cluster-id :paused-node-info] dissoc logical-node)
                       (update-in [cluster-id :paused-nodes] disj logical-node)
                       (update-in [cluster-id :live-nodes] disj logical-node)))))
        (let [server           (get-in cluster [:servers logical-node])
              db-name          (:db-name cluster)
              setup-timeout-ms (:setup-timeout-ms cluster)
              db-state         (db-state server db-name)
              stopped-info     (when db-state
                                 {:stopped-at-ms (now-ms)
                                  :ha-role (:ha-role db-state)
                                  :effective-local-lsn (effective-local-lsn cluster-id logical-node)
                                  :node-diagnostics (node-diagnostics cluster-id logical-node)})]
          (safe-close-conn! (get-in cluster [:admin-conns logical-node]))
          (safe-stop-server! (get-in cluster [:servers logical-node]))
          (wait-for-node-store-released! deps
                                         cluster-id
                                         logical-node
                                         (or setup-timeout-ms cluster-timeout-ms))
          (swap! clusters
                 (fn [clusters*]
                   (-> clusters*
                       (assoc-in [cluster-id :admin-conns logical-node] nil)
                       (assoc-in [cluster-id :servers logical-node] nil)
                       (assoc-in [cluster-id :stopped-node-info logical-node] stopped-info)
                       (update-in [cluster-id :paused-node-info] dissoc logical-node)
                       (update-in [cluster-id :paused-nodes] disj logical-node)
                       (update-in [cluster-id :live-nodes] disj logical-node)))))))))

(defn restart-node!
  [{:keys [clusters remote-deps transport-failure?] :as deps} cluster-id logical-node]
  (locking clusters
    (let [{:keys [db-name base-opts node-by-name verbose?
                  control-backend node-ha-opt-overrides remote?
                  ssh remote-config-path setup-timeout-ms]
           :as cluster}
          (get @clusters cluster-id)]
      (cond
        (and (not remote?)
             (get-in @clusters [cluster-id :servers logical-node]))
        true

        remote?
        (let [node      (get node-by-name logical-node)
              repo-root (lremote/remote-node-repo-root cluster node)
              rdeps     (remote-deps)]
          (lremote/upload-remote-config! rdeps ssh node remote-config-path)
          (lremote/start-remote-node-launcher! rdeps ssh repo-root node verbose?)
          (lremote/wait-for-remote-node-running! {:remote-launcher-ops (:remote-launcher-ops rdeps)
                                                  :now-ms now-ms}
                                                 ssh
                                                 node
                                                 (or setup-timeout-ms cluster-timeout-ms))
          (swap! clusters
                 (fn [clusters*]
                   (-> clusters*
                       (update-in [cluster-id :stopped-node-info] dissoc logical-node)
                       (update-in [cluster-id :paused-node-info] dissoc logical-node)
                       (update-in [cluster-id :paused-nodes] disj logical-node)
                       (update-in [cluster-id :live-nodes] conj logical-node))))
          true)

        :else
        (let [node     (get node-by-name logical-node)
              override (get node-ha-opt-overrides logical-node)
              _        (wait-for-node-store-released! deps
                                                     cluster-id
                                                     logical-node
                                                     (or setup-timeout-ms cluster-timeout-ms))
              server   (start-server! node verbose?)]
          (try
            (let [conn (with-control-backend
                         control-backend
                         #(open-ha-conn! {:transport-failure? transport-failure?}
                                         node
                                         db-name
                                         nil
                                         (node-ha-opts base-opts node override)
                                         (or setup-timeout-ms cluster-timeout-ms)))]
              (swap! clusters
                     (fn [clusters*]
                       (-> clusters*
                           (assoc-in [cluster-id :servers logical-node] server)
                           (assoc-in [cluster-id :admin-conns logical-node] conn)
                           (update-in [cluster-id :stopped-node-info] dissoc logical-node)
                           (update-in [cluster-id :paused-node-info] dissoc logical-node)
                           (update-in [cluster-id :paused-nodes] disj logical-node)
                           (update-in [cluster-id :live-nodes] conj logical-node))))
              true)
            (catch Throwable e
              (safe-stop-server! server)
              (wait-for-node-store-released! deps
                                             cluster-id
                                             logical-node
                                             (or setup-timeout-ms cluster-timeout-ms))
              (throw e))))))))

(defn pause-node!
  [{:keys [clusters remote-deps node-diagnostics effective-local-lsn] :as deps}
   cluster-id logical-node]
  (locking clusters
    (when-let [cluster (get @clusters cluster-id)]
      (when-not (contains? (:live-nodes cluster) logical-node)
        (u/raise "Cannot pause unavailable Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node}))
      (when-not (contains? (:paused-nodes cluster) logical-node)
        (if (:remote? cluster)
          (let [{:keys [node-by-name ssh]} cluster
                node       (get node-by-name logical-node)
                rdeps      (remote-deps)
                pause-info {:paused-at-ms (now-ms)
                            :ha-role (:ha-role (node-diagnostics cluster-id logical-node))
                            :effective-local-lsn
                            (effective-local-lsn cluster-id logical-node)
                            :node-diagnostics
                            (node-diagnostics cluster-id logical-node)}]
            (lremote/close-remote-admin-client! rdeps cluster-id logical-node)
            (lremote/signal-remote-node! rdeps ssh node "STOP")
            (swap! clusters
                   (fn [clusters*]
                     (-> clusters*
                         (assoc-in [cluster-id :paused-node-info logical-node] pause-info)
                         (update-in [cluster-id :paused-nodes] conj logical-node)))))
          (let [server    (get-in cluster [:servers logical-node])
                db-name   (:db-name cluster)
                db-state  (db-state server db-name)
                conn      (get-in cluster [:admin-conns logical-node])
                pause-info
                (when db-state
                  {:paused-at-ms (now-ms)
                   :ha-role (:ha-role db-state)
                   :effective-local-lsn (effective-local-lsn cluster-id logical-node)
                   :node-diagnostics (node-diagnostics cluster-id logical-node)})]
            (safe-close-conn! conn)
            (when-let [authority (:ha-authority db-state)]
              (#'srv/stop-ha-renew-loop db-state)
              (ctrl/stop-authority! authority))
            (when server
              (disconnect-server-client-channels! server)
              (pause-server-loop! server))
            (swap! clusters
                   (fn [clusters*]
                     (-> clusters*
                         (assoc-in [cluster-id :admin-conns logical-node] nil)
                         (assoc-in [cluster-id :paused-node-info logical-node] pause-info)
                         (update-in [cluster-id :paused-nodes] conj logical-node))))))))
    true))

(defn resume-node!
  [{:keys [clusters remote-deps] :as deps} cluster-id logical-node]
  (locking clusters
    (when-let [cluster (get @clusters cluster-id)]
      (when-not (contains? (:paused-nodes cluster) logical-node)
        (u/raise "Cannot resume unpaused Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node}))
      (if (:remote? cluster)
        (let [{:keys [node-by-name ssh]} cluster
              node  (get node-by-name logical-node)
              rdeps (remote-deps)]
          (lremote/signal-remote-node! rdeps ssh node "CONT")
          (swap! clusters
                 (fn [clusters*]
                   (-> clusters*
                       (update-in [cluster-id :paused-node-info] dissoc logical-node)
                       (update-in [cluster-id :paused-nodes] disj logical-node)))))
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
          (swap! clusters
                 (fn [clusters*]
                   (-> clusters*
                       (assoc-in [cluster-id :admin-conns logical-node] nil)
                       (update-in [cluster-id :paused-node-info] dissoc logical-node)
                       (update-in [cluster-id :paused-nodes] disj logical-node)))))))
    true))
