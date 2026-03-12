(ns datalevin.jepsen.remote-node
  (:require
   [clojure.edn :as edn]
   [clojure.pprint :as pp]
   [clojure.string :as str]
   [clojure.tools.cli :refer [parse-opts]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.ha.control :as ctrl]
   [datalevin.jepsen.core :as core]
   [datalevin.jepsen.local :as local]
   [datalevin.server :as srv]
   [datalevin.util :as u])
  (:import
   [java.lang ProcessHandle]
   [java.net InetSocketAddress Socket]
   [java.nio.file Files Paths]
   [java.util Optional]))

(def ^:private join-timeout-ms-default 60000)
(def ^:private endpoint-check-timeout-ms 1000)
(def ^:private endpoint-check-sleep-ms 500)
(def ^:private connect-client-opts {:pool-size 1 :time-out 10000})

(def ^:private common-cli-opts
  [[nil "--config PATH" "Path to the remote Jepsen cluster EDN config"
    :validate [some? "Must be provided"]]

   [nil "--node NAME" "Logical node name from the config"
    :validate [some? "Must be provided"]]

   [nil "--join-timeout-ms MS" "How long to wait for peers and cluster join"
    :default join-timeout-ms-default
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   ["-v" "--verbose" "Enable verbose Datalevin server logging"]

   [nil "--print-edn" "Print node details as EDN"]

   ["-h" "--help" "Show this help"]])

(def ^:private command-help
  {"start"
   ["Start a real Datalevin node for Jepsen HA testing."
    ""
    "Examples:"
    "  script/jepsen/start-remote-node --config jepsen/remote-cluster.example.edn --node n1"
    "  script/jepsen/start-remote-node --config /etc/dtlv-jepsen/cluster.edn --node n2 --print-edn"
    ""
    "This starts either a data node or a control-only witness from the shared"
    "cluster config, writes pid/state files under the node root, and keeps the"
    "node running until stopped."]

   "stop"
   ["Stop a real Datalevin node started by start-remote-node."
    ""
    "Examples:"
    "  script/jepsen/stop-remote-node --config jepsen/remote-cluster.example.edn --node n1"
    ""
    "This reads the pid file under the configured node root and terminates the"
    "launcher process."]})

(defn- usage
  [command summary]
  (str/join
   \newline
   (concat (get command-help command)
           ["" "Options:" summary])))

(defn- read-config
  [path]
  (-> path slurp edn/read-string))

(defn- parse-endpoint
  [endpoint]
  (let [[host port-str] (str/split (or endpoint "") #":" 2)
        port            (parse-long port-str)]
    (when (or (str/blank? host)
              (str/blank? port-str)
              (nil? port)
              (not (pos? ^long port)))
      (u/raise "Invalid endpoint"
               {:endpoint endpoint}))
    {:host host
     :port port}))

(defn- ensure-unique!
  [label values]
  (when (not= (count values) (count (set values)))
    (u/raise "Duplicate remote Jepsen config values"
             {:field label
              :values values})))

(defn- validate-node-entry!
  [node]
  (doseq [k [:logical-node :node-id :endpoint :peer-id :root]]
    (when (nil? (get node k))
      (u/raise "Remote Jepsen node config is missing a required key"
               {:key k
                :node node})))
  (when-not (string? (:logical-node node))
    (u/raise "Remote Jepsen node names must be strings"
             {:node node}))
  (when-not (pos-int? (:node-id node))
    (u/raise "Remote Jepsen node ids must be positive integers"
             {:node node}))
  (when-not (string? (:root node))
    (u/raise "Remote Jepsen node roots must be strings"
             {:node node}))
  (parse-endpoint (:endpoint node))
  (parse-endpoint (:peer-id node))
  node)

(defn- configured-control-node-entries
  [config]
  (vec (or (:control-nodes config)
           (:nodes config))))

(defn- validate-cluster-topology!
  [data-node-names control-node-names control-backend]
  (let [data-node-names    (vec data-node-names)
        control-node-names (vec control-node-names)
        data-node-set      (set data-node-names)
        control-node-set   (set control-node-names)]
    (when (empty? data-node-names)
      (u/raise "Remote Jepsen cluster requires at least one data node"
               {:data-nodes data-node-names
                :control-nodes control-node-names}))
    (when (not= (count data-node-names) (count data-node-set))
      (u/raise "Remote Jepsen data nodes must be unique"
               {:data-nodes data-node-names}))
    (when (not= (count control-node-names) (count control-node-set))
      (u/raise "Remote Jepsen control nodes must be unique"
               {:control-nodes control-node-names}))
    (when (some #(not (contains? control-node-set %)) data-node-names)
      (u/raise "Remote Jepsen control nodes must include every data node"
               {:data-nodes data-node-names
                :control-nodes control-node-names}))
    (when (and (> (count control-node-names) (count data-node-names))
               (not= :sofa-jraft control-backend))
      (u/raise "Remote Jepsen control-only witness nodes require sofa-jraft"
               {:control-backend control-backend
                :data-nodes data-node-names
                :control-nodes control-node-names}))))

(defn- merge-node-entry!
  [merged node]
  (if-let [existing (get merged (:logical-node node))]
    (let [identity-keys [:logical-node :node-id :endpoint :peer-id :root]]
      (when-not (= (select-keys existing identity-keys)
                   (select-keys node identity-keys))
        (u/raise "Remote Jepsen node config disagrees for the same logical node"
                 {:logical-node (:logical-node node)
                  :existing existing
                  :incoming node}))
      (assoc merged (:logical-node node) (merge existing node)))
    (assoc merged (:logical-node node) node)))

(defn- validate-config!
  [config]
  (let [data-nodes          (vec (:nodes config))
        control-nodes       (configured-control-node-entries config)
        configured-node-map (reduce merge-node-entry!
                                    {}
                                    (concat data-nodes control-nodes))]
    (when-not (seq data-nodes)
      (u/raise "Remote Jepsen config requires a non-empty :nodes vector"
               {:config (select-keys config [:db-name :workload])}))
    (doseq [node (vals configured-node-map)]
      (validate-node-entry! node))
    (ensure-unique! :logical-node (map :logical-node (vals configured-node-map)))
    (ensure-unique! :node-id (map :node-id (vals configured-node-map)))
    (ensure-unique! :endpoint (map :endpoint (vals configured-node-map)))
    (ensure-unique! :peer-id (map :peer-id (vals configured-node-map)))
    (when-not (string? (:db-name config))
      (u/raise "Remote Jepsen config requires :db-name"
               {:config config}))
    (when-not (keyword? (:workload config))
      (u/raise "Remote Jepsen config requires keyword :workload"
               {:config config}))
    (when-not (contains? core/workloads (:workload config))
      (u/raise "Unsupported remote Jepsen workload"
               {:workload (:workload config)
                :supported (sort (keys core/workloads))}))
    (when-not (string? (:group-id config))
      (u/raise "Remote Jepsen config requires :group-id"
               {:config config}))
    (when-not (string? (:db-identity config))
      (u/raise "Remote Jepsen config requires :db-identity"
               {:config config}))
    (when-not (= :sofa-jraft (or (:control-backend config) :sofa-jraft))
      (u/raise "Remote Jepsen node launcher currently requires :control-backend :sofa-jraft"
               {:control-backend (:control-backend config)}))
    (assoc config :control-backend :sofa-jraft)))

(defn- config-workload
  [config]
  (let [workload-fn       (get core/workloads (:workload config))
        data-node-names   (mapv :logical-node (:nodes config))
        control-node-names (mapv :logical-node
                                 (configured-control-node-entries config))
        workload-opts     (merge (:workload-opts config)
                                 (select-keys config
                                              [:key-count
                                               :account-balance
                                               :max-transfer
                                               :max-writes-per-key
                                               :min-txn-length
                                               :max-txn-length
                                               :giant-payload-bytes]))
        workload-opts'    (assoc workload-opts
                                 :nodes data-node-names
                                 :datalevin/control-nodes control-node-names)]
    (workload-fn workload-opts')))

(defn- workload-topology
  [config workload]
  (let [data-node-names       (vec (or (seq (:nodes workload))
                                       (map :logical-node (:nodes config))))
        control-node-names    (vec (or (seq (:datalevin/control-nodes workload))
                                       (map :logical-node
                                            (configured-control-node-entries
                                             config))))
        _                     (validate-cluster-topology!
                               data-node-names
                               control-node-names
                               (:control-backend config))
        configured-node-map   (reduce merge-node-entry!
                                      {}
                                      (concat (:nodes config)
                                              (configured-control-node-entries
                                               config)))
        data-nodes            (mapv
                               (fn [node-name]
                                 (or (get configured-node-map node-name)
                                     (u/raise
                                      "Remote Jepsen data node is missing from config"
                                      {:node node-name
                                       :available (sort (keys configured-node-map))})))
                               data-node-names)
        control-nodes         (mapv
                               (fn [node-name]
                                 (or (get configured-node-map node-name)
                                     (u/raise
                                      "Remote Jepsen control node is missing from config"
                                      {:node node-name
                                       :available (sort (keys configured-node-map))})))
                               control-node-names)
        data-node-set         (set data-node-names)
        control-only-nodes    (->> control-nodes
                                   (remove #(contains? data-node-set
                                                       (:logical-node %)))
                                   vec)]
    (doseq [node control-only-nodes]
      (when (true? (:promotable? node))
        (u/raise "Remote Jepsen control-only witness nodes cannot be promotable"
                 {:node node
                  :data-nodes data-node-names
                  :control-nodes control-node-names})))
    {:data-nodes data-nodes
     :control-nodes control-nodes
     :control-only-nodes control-only-nodes
     :node-by-name (into {}
                          (map (juxt :logical-node identity))
                          control-nodes)}))

(defn- control-voters
  [data-nodes control-nodes]
  (let [promotable-node-ids (set (map :node-id data-nodes))]
    (mapv
     (fn [{:keys [node-id peer-id promotable?]}]
       (if (contains? promotable-node-ids node-id)
         {:peer-id peer-id
          :ha-node-id node-id
          :promotable? true}
         {:peer-id peer-id
          :promotable? (boolean promotable?)}))
     control-nodes)))

(defn- merge-ha-opts
  [base-opts override-opts]
  (if (map? override-opts)
    (let [control-plane-override (:ha-control-plane override-opts)]
      (cond-> (merge base-opts (dissoc override-opts :ha-control-plane))
        (map? control-plane-override)
        (update :ha-control-plane merge control-plane-override)))
    base-opts))

(defn- base-ha-opts
  [config data-nodes control-nodes]
  (let [data-nodes    (vec (sort-by :node-id data-nodes))
        control-nodes (vec (sort-by :node-id control-nodes))
        base-opts     {:wal? true
                       :db-identity (:db-identity config)
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
                       :ha-members (mapv #(select-keys % [:node-id :endpoint])
                                         data-nodes)
                       :ha-control-plane
                       {:backend :sofa-jraft
                        :group-id (:group-id config)
                        :voters (control-voters data-nodes control-nodes)
                        :rpc-timeout-ms 5000
                        :election-timeout-ms 5000
                        :operation-timeout-ms 30000}}]
    (merge-ha-opts base-opts (:cluster-opts config))))

(defn- node-ha-opts
  [config node workload data-nodes control-nodes]
  (-> (base-ha-opts config data-nodes control-nodes)
      (merge-ha-opts (:datalevin/cluster-opts workload))
      (assoc :db-name (:db-name config))
      (assoc :ha-node-id (:node-id node))
      (assoc-in [:ha-control-plane :local-peer-id] (:peer-id node))))

(defn- control-authority-opts
  [node base-opts]
  (let [control-plane-opts (:ha-control-plane base-opts)]
    (assoc control-plane-opts
           :local-peer-id (:peer-id node)
           :raft-dir (str (:root node)
                          u/+separator+
                          "ha-control-authority"
                          u/+separator+
                          (:group-id control-plane-opts)))))

(defn- server-options
  [node verbose?]
  (let [{:keys [port]} (parse-endpoint (:endpoint node))]
    (cond-> {:port port
             :root (:root node)}
      verbose?
      (assoc :verbose true))))

(defn- endpoint-reachable?
  [endpoint timeout-ms]
  (let [{:keys [host port]} (parse-endpoint endpoint)
        ^String host        host]
    (try
      (with-open [socket (Socket.)]
        (.connect socket (InetSocketAddress. host (int port)) (int timeout-ms))
        true)
      (catch Throwable _
        false))))

(defn- wait-for-endpoints!
  [endpoints timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-missing nil]
      (let [missing (->> endpoints
                         (remove #(endpoint-reachable? % endpoint-check-timeout-ms))
                         vec)]
        (cond
          (empty? missing)
          :ready

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep (long endpoint-check-sleep-ms))
            (recur missing))

          :else
          (u/raise "Timed out waiting for remote Jepsen peer endpoints"
                   {:missing missing
                    :last-missing last-missing
                    :timeout-ms timeout-ms}))))))

(defn- wait-for-open-prereqs!
  [node topology timeout-ms]
  (let [data-endpoints        (mapv :endpoint (:data-nodes topology))
        control-only-peer-ids (mapv :peer-id (:control-only-nodes topology))
        prereq-endpoints      (vec (concat data-endpoints
                                           control-only-peer-ids))]
    (when (seq prereq-endpoints)
      (wait-for-endpoints! prereq-endpoints timeout-ms)))
  node)

(defn- open-ha-conn!
  [endpoint db-name schema opts timeout-ms]
  (let [uri        (local/db-uri endpoint db-name)
        timeout-ms (long timeout-ms)
        deadline   (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [remaining-ms      (- deadline (System/currentTimeMillis))
            attempt-timeout-ms (long (max 1 (min timeout-ms remaining-ms)))
            outcome           (try
                                {:conn (d/create-conn
                                        uri
                                        schema
                                        (assoc opts
                                               :client-opts
                                               (assoc connect-client-opts
                                                      :time-out
                                                      attempt-timeout-ms)))}
                                (catch Throwable e
                                  {:error e}))]
        (if-let [conn (:conn outcome)]
          conn
          (let [e (:error outcome)]
            (if (and (< (System/currentTimeMillis) deadline)
                     (local/transport-failure? e))
              (do
                (Thread/sleep (long endpoint-check-sleep-ms))
                (recur))
              (throw (ex-info "Unable to open remote Jepsen HA node"
                              {:endpoint endpoint
                               :db-name db-name}
                              e)))))))))

(defn- normalize-runtime-opts-override
  [override]
  (cond
    (ifn? override) override
    (map? override) (constantly override)
    :else nil))

(defn- merge-runtime-opts
  [base-opts override-opts]
  (cond
    (and (map? base-opts) (map? override-opts))
    (merge base-opts override-opts)

    (map? override-opts)
    override-opts

    :else
    base-opts))

(defn- runtime-opts-wrapper
  [base-fn override]
  (if-let [override-fn (normalize-runtime-opts-override override)]
    (fn [server db-name store m]
      (merge-runtime-opts (base-fn server db-name store m)
                          (override-fn server db-name store m)))
    base-fn))

(defn- pid-file
  [node]
  (str (:root node) u/+separator+ "jepsen-remote-node.pid"))

(defn- state-file
  [node]
  (str (:root node) u/+separator+ "jepsen-remote-node.edn"))

(defn- write-pid-file!
  [node]
  (u/create-dirs (:root node))
  (spit (pid-file node) (str (.pid (ProcessHandle/current)) "\n")))

(defn- write-state-file!
  [node details]
  (u/create-dirs (:root node))
  (spit (state-file node) (pr-str details)))

(defn- delete-if-exists!
  [path]
  (when path
    (try
      (Files/deleteIfExists (Paths/get path (make-array String 0)))
      (catch Throwable _
        nil))))

(defn- process-handle
  [pid]
  (let [^Optional h (ProcessHandle/of (long pid))]
    (when (.isPresent h)
      (.get h))))

(defn- stop-process!
  [pid]
  (if-some [^ProcessHandle handle (process-handle pid)]
    (do
      (.destroy handle)
      (let [deadline (+ (System/currentTimeMillis) 5000)]
        (loop []
          (cond
            (not (.isAlive handle))
            :stopped

            (< (System/currentTimeMillis) deadline)
            (do
              (Thread/sleep 200)
              (recur))

            :else
            (do
              (.destroyForcibly handle)
              (let [forced-deadline (+ (System/currentTimeMillis) 5000)]
                (loop []
                  (cond
                    (not (.isAlive handle))
                    :forced

                    (< (System/currentTimeMillis) forced-deadline)
                    (do
                      (Thread/sleep 200)
                      (recur))

                    :else
                    :still-running)))))))
    :missing)))

(defn- safe-close-conn!
  [conn]
  (when conn
    (try
      (d/close conn)
      (catch Throwable _
        nil))))

(defn- safe-stop-server!
  [server]
  (when server
    (try
      (srv/stop server)
      (catch Throwable _
        nil))))

(defn- safe-stop-authority!
  [authority]
  (when authority
    (try
      (ctrl/stop-authority! authority)
      (catch Throwable _
        nil))))

(defn- running-summary
  [{:keys [kind config node topology workload]} conn authority]
  (let [base {:status          :running
              :kind            kind
              :node            (:logical-node node)
              :node-id         (:node-id node)
              :db-name         (:db-name config)
              :workload        (:workload config)
              :endpoint        (:endpoint node)
              :peer-id         (:peer-id node)
              :root            (:root node)
              :pid-file        (pid-file node)
              :state-file      (state-file node)
              :control-backend :sofa-jraft
              :data-nodes      (mapv :logical-node (:data-nodes topology))
              :control-nodes   (mapv :logical-node (:control-nodes topology))
              :cluster-opts    (select-keys (node-ha-opts config
                                                          node
                                                          workload
                                                          (:data-nodes topology)
                                                          (:control-nodes topology))
                                            [:db-identity
                                             :ha-mode
                                             :ha-members
                                             :ha-node-id
                                             :ha-control-plane])}]
    (case kind
      :data-node
      (assoc base
             :db-uri (local/db-uri (:endpoint node) (:db-name config))
             :peer-endpoints (mapv :endpoint (:data-nodes topology))
             :control-peer-ids (mapv :peer-id (:control-nodes topology)))

      :control-only-node
      (assoc base
             :control-peer-ids (mapv :peer-id (:control-nodes topology))
             :authority-diagnostics (ctrl/authority-diagnostics authority)))))

(defn- failed-summary
  [{:keys [kind config node topology]} error]
  {:status        :failed
   :kind          kind
   :node          (:logical-node node)
   :node-id       (:node-id node)
   :db-name       (:db-name config)
   :workload      (:workload config)
   :endpoint      (:endpoint node)
   :peer-id       (:peer-id node)
   :root          (:root node)
   :pid-file      (pid-file node)
   :state-file    (state-file node)
   :data-nodes    (mapv :logical-node (:data-nodes topology))
   :control-nodes (mapv :logical-node (:control-nodes topology))
   :error-message (ex-message error)
   :error-data    (ex-data error)})

(defn- print-summary!
  [details print-edn?]
  (if print-edn?
    (prn details)
    (do
      (println "Remote Jepsen node is running.")
      (println)
      (pp/pprint details)
      (println)
      (println "Press Ctrl-C to stop."))))

(defn- start-data-node!
  [{:keys [config node topology workload join-timeout-ms verbose? print-edn?]}]
  (let [base-runtime-fn    (var-get #'srv/*server-runtime-opts-fn*)
        runtime-opts-fn    (runtime-opts-wrapper
                            base-runtime-fn
                            (:datalevin/server-runtime-opts-fn workload))
        server             (atom nil)
        conn               (atom nil)
        stopped?           (atom false)
        keep-state-file?   (atom false)
        stop!              (fn []
                             (when (compare-and-set! stopped? false true)
                               (safe-close-conn! @conn)
                               (safe-stop-server! @server)
                               (alter-var-root #'srv/*server-runtime-opts-fn*
                                               (constantly base-runtime-fn))
                               (delete-if-exists! (pid-file node))
                               (when-not @keep-state-file?
                                 (delete-if-exists! (state-file node)))))
        shutdown-hook      (Thread. ^Runnable stop!)]
    (write-pid-file! node)
    (write-state-file! node
                       {:status :starting
                        :kind :data-node
                        :node (:logical-node node)
                        :pid-file (pid-file node)
                        :state-file (state-file node)})
    (.addShutdownHook (Runtime/getRuntime) shutdown-hook)
    (try
      (alter-var-root #'srv/*server-runtime-opts-fn*
                      (constantly runtime-opts-fn))
      (reset! server
              (binding [c/*db-background-sampling?* false]
                (srv/create (server-options node verbose?))))
      (binding [c/*db-background-sampling?* false]
        (srv/start @server))
      (wait-for-open-prereqs! node topology join-timeout-ms)
      (reset! conn
              (open-ha-conn! (:endpoint node)
                             (:db-name config)
                             (:schema workload)
                             (node-ha-opts config
                                           node
                                           workload
                                           (:data-nodes topology)
                                           (:control-nodes topology))
                             join-timeout-ms))
      (let [details (running-summary {:kind :data-node
                                      :config config
                                      :node node
                                      :topology topology
                                      :workload workload}
                                     @conn
                                     nil)]
        (write-state-file! node details)
        (print-summary! details print-edn?))
      (loop []
        (Thread/sleep 60000)
        (recur))
      (catch InterruptedException _
        nil)
      (catch Throwable e
        (reset! keep-state-file? true)
        (write-state-file! node
                           (failed-summary {:kind :data-node
                                            :config config
                                            :node node
                                            :topology topology}
                                           e))
        (throw e))
      (finally
        (stop!)
        (try
          (.removeShutdownHook (Runtime/getRuntime) shutdown-hook)
          (catch IllegalStateException _
            nil))))))

(defn- start-control-only-node!
  [{:keys [config node topology workload print-edn?]}]
  (let [authority      (atom nil)
        stopped?       (atom false)
        base-opts      (base-ha-opts config
                                     (:data-nodes topology)
                                     (:control-nodes topology))
        keep-state-file? (atom false)
        stop!          (fn []
                         (when (compare-and-set! stopped? false true)
                           (safe-stop-authority! @authority)
                           (delete-if-exists! (pid-file node))
                           (when-not @keep-state-file?
                             (delete-if-exists! (state-file node)))))
        shutdown-hook  (Thread. ^Runnable stop!)]
    (write-pid-file! node)
    (write-state-file! node
                       {:status :starting
                        :kind :control-only-node
                        :node (:logical-node node)
                        :pid-file (pid-file node)
                        :state-file (state-file node)})
    (.addShutdownHook (Runtime/getRuntime) shutdown-hook)
    (try
      (let [opts (control-authority-opts node base-opts)]
        (u/create-dirs (:raft-dir opts))
        (reset! authority (ctrl/new-authority opts))
        (ctrl/start-authority! @authority))
      (let [details (running-summary {:kind :control-only-node
                                      :config config
                                      :node node
                                      :topology topology
                                      :workload workload}
                                     nil
                                     @authority)]
        (write-state-file! node details)
        (print-summary! details print-edn?))
      (loop []
        (Thread/sleep 60000)
        (recur))
      (catch InterruptedException _
        nil)
      (catch Throwable e
        (reset! keep-state-file? true)
        (write-state-file! node
                           (failed-summary {:kind :control-only-node
                                            :config config
                                            :node node
                                            :topology topology}
                                           e))
        (throw e))
      (finally
        (stop!)
        (try
          (.removeShutdownHook (Runtime/getRuntime) shutdown-hook)
          (catch IllegalStateException _
            nil))))))

(defn- start-node!
  [config node-name join-timeout-ms verbose? print-edn?]
  (let [config        (validate-config! config)
        workload      (config-workload config)
        topology      (workload-topology config workload)
        node          (or (get (:node-by-name topology) node-name)
                          (u/raise "Unknown remote Jepsen node"
                                   {:node node-name
                                    :available (sort (keys (:node-by-name topology)))}))
        data-node?    (contains? (set (map :logical-node (:data-nodes topology)))
                                 node-name)
        launch-opts   {:config config
                       :node node
                       :topology topology
                       :workload workload
                       :join-timeout-ms join-timeout-ms
                       :verbose? verbose?
                       :print-edn? print-edn?}]
    (if data-node?
      (start-data-node! launch-opts)
      (start-control-only-node! launch-opts))))

(defn- stop-node!
  [config node-name]
  (let [config       (validate-config! config)
        workload     (config-workload config)
        topology     (workload-topology config workload)
        node         (or (get (:node-by-name topology) node-name)
                         (u/raise "Unknown remote Jepsen node"
                                  {:node node-name
                                   :available (sort (keys (:node-by-name topology)))}))
        pid-path     (pid-file node)
        pid          (when (Files/exists (Paths/get pid-path (make-array String 0))
                                         (make-array java.nio.file.LinkOption 0))
                       (some-> pid-path slurp str/trim parse-long))
        result       (if pid
                       (stop-process! pid)
                       :missing)]
    (when (#{:stopped :forced :missing} result)
      (delete-if-exists! pid-path)
      (delete-if-exists! (state-file node)))
    (println
     (pr-str {:node node-name
              :pid pid
              :result result}))))

(defn -main
  [& args]
  (let [[command & rest-args] (if (seq args) args ["start"])
        command               (if (#{"start" "stop"} command)
                                command
                                "start")
        args'                 (if (#{"start" "stop"} (first args))
                                rest-args
                                args)
        {:keys [options errors summary]} (parse-opts args' common-cli-opts)]
    (cond
      (:help options)
      (do
        (println (usage command summary))
        (System/exit 0))

      (seq errors)
      (do
        (binding [*out* *err*]
          (doseq [error errors]
            (println error))
          (println)
          (println (usage command summary)))
        (System/exit 2))

      :else
      (let [config (read-config (:config options))]
        (case command
          "start" (start-node! config
                               (:node options)
                               (:join-timeout-ms options)
                               (boolean (:verbose options))
                               (boolean (:print-edn options)))
          "stop"  (stop-node! config (:node options)))))))
