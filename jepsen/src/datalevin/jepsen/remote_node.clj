(ns datalevin.jepsen.remote-node
  (:require
   [clojure.pprint :as pp]
   [clojure.string :as str]
   [clojure.tools.cli :refer [parse-opts]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.ha.control :as ctrl]
   [datalevin.jepsen.core :as core]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.remote :as remote]
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
  (remote/read-config path))

(defn- parse-endpoint
  [endpoint]
  (remote/parse-endpoint endpoint))

(defn- validate-config!
  [config]
  (remote/validate-config! config core/workloads))

(defn- config-workload
  [config]
  (remote/config-workload config core/workloads))

(defn- workload-topology
  [config workload]
  (remote/workload-topology config workload))

(defn- base-ha-opts
  [config data-nodes control-nodes]
  (remote/base-ha-opts config data-nodes control-nodes))

(defn- node-ha-opts
  [config node workload data-nodes control-nodes]
  (remote/node-ha-opts config node workload data-nodes control-nodes))

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
  (if-let [^ProcessHandle handle (process-handle pid)]
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
                    :still-running))))))))
    :missing))

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
  [{:keys [kind config node topology workload]} authority]
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
