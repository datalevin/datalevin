(ns datalevin.jepsen.local.remote
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.client :as cl]
   [datalevin.jepsen.remote :as remote]
   [datalevin.util :as u]
   [jepsen.control :as control])
  (:import
   [java.util UUID]))

(def ^:private remote-launch-log-file "jepsen-remote-launch.log")
(def ^:private remote-config-file "jepsen-remote-cluster.edn")
(def ^:private remote-state-poll-ms 500)
(def ^:private local-launcher-command-timeout-ms 45000)
(def ^:private remote-daemon-spawn-script "remote-daemon-spawn.py")

(declare remote-config-path-for-node
         remote-launch-log-path
         remote-pid-file
         remote-state-file)

(defn- now-ms
  []
  (System/currentTimeMillis))

(defn- control-remote-backend
  []
  (case (some-> (System/getenv "DTLV_JEPSEN_CONTROL_REMOTE")
                str/trim
                not-empty
                str/lower-case)
    "clj-ssh" control/clj-ssh
    control/ssh))

(defmacro with-control-ssh
  [ssh & body]
  `(control/with-remote (control-remote-backend)
     (control/with-ssh ~ssh
       ~@body)))

(defn- safe-disconnect-client!
  [client]
  (when client
    (try
      (cl/disconnect client)
      (catch Throwable _ nil))))

(defn- safe-read-edn-file
  [path]
  (when (and (string? path) (u/file-exists path))
    (try
      (-> path slurp edn/read-string)
      (catch Throwable _
        nil))))

(defn- safe-read-long-file
  [path]
  (when (and (string? path) (u/file-exists path))
    (try
      (some-> path slurp str/trim parse-long)
      (catch Throwable _
        nil))))

(defn controller-local-node?
  [node]
  (true? (:controller-local? node)))

(defn remote-node-repo-root
  [cluster-or-config node]
  (or (:repo-root node)
      (:repo-root cluster-or-config)))

(defn- remote-script-path
  [repo-root script-name]
  (.getPath (io/file repo-root "script" "jepsen" script-name)))

(defn- copy-local-file!
  [from to]
  (u/create-dirs (.getParent (io/file to)))
  (io/copy (io/file from) (io/file to))
  to)

(defn- delete-local-path!
  [path]
  (let [file (io/file path)]
    (when (.exists file)
      (io/delete-file file true)))
  true)

(defn- run-local-command
  [repo-root cmd timeout-ms]
  (try
    (let [process-builder (ProcessBuilder. ^java.util.List (mapv str cmd))
          _               (when repo-root
                            (.directory process-builder (io/file repo-root)))
          _               (.redirectErrorStream process-builder true)
          process         (.start process-builder)
          finished?       (.waitFor process
                                    (long timeout-ms)
                                    java.util.concurrent.TimeUnit/MILLISECONDS)]
      (if finished?
        (let [exit   (.exitValue process)
              output (try
                       (slurp (.getInputStream process) :encoding "UTF-8")
                       (catch Exception _
                         ""))]
          {:ok? true
           :exit exit
           :output output})
        (do
          (.destroy process)
          (when-not (.waitFor process
                              200
                              java.util.concurrent.TimeUnit/MILLISECONDS)
            (.destroyForcibly process))
          {:ok? false
           :reason :timeout
           :timeout-ms timeout-ms})))
    (catch Exception e
      {:ok? false
       :reason :exception
       :message (ex-message e)})))

(defn- ensure-local-command-ok!
  [result message data]
  (when-not (and (:ok? result)
                 (zero? (:exit result)))
    (u/raise message
             (merge data
                    (select-keys result
                                 [:exit
                                  :output
                                  :reason
                                  :timeout-ms
                                  :message]))))
  true)

(defn- controller-local-script
  [repo-root script-name]
  (.getPath (io/file repo-root "script" "jepsen" script-name)))

(defn- start-controller-local-launcher!
  [repo-root node verbose?]
  (let [command         (cond-> [(controller-local-script repo-root
                                                          "start-remote-node")
                                 "--config"
                                 (remote-config-path-for-node node)
                                 "--node"
                                 (:logical-node node)]
                          verbose?
                          (conj "--verbose"))
        launch-log-path (remote-launch-log-path node)
        process-builder (ProcessBuilder. ^java.util.List (mapv str command))]
    (u/create-dirs (:root node))
    (.directory process-builder (io/file repo-root))
    (.redirectErrorStream process-builder true)
    (.redirectOutput process-builder (io/file launch-log-path))
    (.start process-builder)
    true))

(defn- stop-controller-local-launcher!
  [repo-root node]
  (ensure-local-command-ok!
   (run-local-command repo-root
                      [(controller-local-script repo-root
                                                "stop-remote-node")
                       "--config"
                       (remote-config-path-for-node node)
                       "--node"
                       (:logical-node node)]
                      local-launcher-command-timeout-ms)
   "Controller-local remote Jepsen node failed to stop"
   {:node (:logical-node node)
    :repo-root repo-root}))

(defn- signal-controller-local-node!
  [node signal]
  (let [pid (safe-read-long-file (remote-pid-file node))]
    (when-not pid
      (u/raise "Controller-local remote Jepsen node is missing a pid file"
               {:node (:logical-node node)
                :pid-file (remote-pid-file node)
                :signal signal}))
    (ensure-local-command-ok!
     (run-local-command nil
                        ["kill" (str "-" signal) (str pid)]
                        5000)
     "Controller-local remote Jepsen node failed to receive a signal"
     {:node (:logical-node node)
      :pid pid
      :signal signal})))

(defn remote-config-path-for-node
  [node]
  (str (:root node) u/+separator+ remote-config-file))

(defn remote-launch-log-path
  [node]
  (str (:root node) u/+separator+ remote-launch-log-file))

(defn remote-pid-file
  [node]
  (str (:root node) u/+separator+ "jepsen-remote-node.pid"))

(defn remote-state-file
  [node]
  (str (:root node) u/+separator+ "jepsen-remote-node.edn"))

(defn write-remote-content!
  [{:keys [remote-launcher-ops]} ssh node remote-path content]
  (cond
    (controller-local-node? node)
    (do
      (u/create-dirs (.getParent (io/file remote-path)))
      (spit remote-path content)
      remote-path)

    :else
    (if-let [f (:write-content remote-launcher-ops)]
      (f ssh node remote-path content)
      (let [tmp-dir  (u/tmp-dir (str "jepsen-remote-upload-" (UUID/randomUUID)))
            tmp-file (str tmp-dir u/+separator+ "payload")]
        (u/create-dirs tmp-dir)
        (spit tmp-file content)
        (try
          (with-control-ssh ssh
            (control/on (:logical-node node)
              (control/exec :mkdir :-p (.getParent (io/file remote-path)))
              (control/upload tmp-file remote-path)))
          (finally
            (u/delete-files tmp-dir)))))))

(defn delete-remote-path!
  [{:keys [remote-launcher-ops]} ssh node remote-path]
  (cond
    (controller-local-node? node)
    (delete-local-path! remote-path)

    :else
    (if-let [f (:delete-path remote-launcher-ops)]
      (f ssh node remote-path)
      (with-control-ssh ssh
        (control/on (:logical-node node)
          (control/exec :rm :-f remote-path))))))

(defn sync-remote-node-control-defaults!
  [deps ssh node]
  (cond
    (controller-local-node? node)
    (do
      (u/create-dirs (remote/control-state-dir node))
      (write-remote-content! deps
                             ssh
                             node
                             (remote/network-state-file node)
                             (pr-str {:blocked-endpoints #{}
                                      :endpoint-profiles {}}))
      (write-remote-content! deps
                             ssh
                             node
                             (remote/clock-skew-state-file node)
                             "0\n")
      (write-remote-content! deps
                             ssh
                             node
                             (remote/fencing-mode-file node)
                             "success\n")
      (delete-remote-path! deps ssh node (remote/storage-fault-state-file node))
      (delete-remote-path! deps ssh node (remote/snapshot-failpoint-file node)))

    :else
    (if-let [f (:sync-control-defaults (:remote-launcher-ops deps))]
      (f ssh node)
      (do
        (with-control-ssh ssh
          (control/on (:logical-node node)
            (control/exec :mkdir :-p (remote/control-state-dir node))))
        (write-remote-content! deps
                               ssh
                               node
                               (remote/network-state-file node)
                               (pr-str {:blocked-endpoints #{}
                                        :endpoint-profiles {}}))
        (write-remote-content! deps
                               ssh
                               node
                               (remote/clock-skew-state-file node)
                               "0\n")
        (write-remote-content! deps
                               ssh
                               node
                               (remote/fencing-mode-file node)
                               "success\n")
        (delete-remote-path! deps ssh node (remote/storage-fault-state-file node))
        (delete-remote-path! deps ssh node (remote/snapshot-failpoint-file node))))))

(defn- remote-network-node-state
  [{:keys [clusters endpoint-for-node]} cluster-id logical-node]
  (let [{:keys [node-by-name]} (get @clusters cluster-id)
        blocked-endpoints      (into #{}
                                     (keep (fn [[src dest]]
                                             (when (= src logical-node)
                                               (endpoint-for-node cluster-id
                                                                  dest))))
                                     (get-in @clusters
                                             [cluster-id :dropped-links]))
        endpoint-profiles      (into {}
                                     (keep (fn [[[src dest] profile]]
                                             (when (= src logical-node)
                                               [(endpoint-for-node cluster-id
                                                                   dest)
                                                profile])))
                                     (get-in @clusters
                                             [cluster-id :link-behaviors]))]
    (when (contains? node-by-name logical-node)
      {:blocked-endpoints blocked-endpoints
       :endpoint-profiles endpoint-profiles})))

(defn sync-remote-network-state!
  [deps cluster-id]
  (let [clusters (:clusters deps)]
    (when-let [{:keys [remote? ssh node-by-name]} (get @clusters cluster-id)]
      (when remote?
        (doseq [[logical-node node] node-by-name
                :when (get-in @clusters [cluster-id :live-nodes logical-node] true)]
          (write-remote-content! deps
                                 ssh
                                 node
                                 (remote/network-state-file node)
                                 (pr-str (remote-network-node-state
                                          deps
                                          cluster-id
                                          logical-node))))))))

(defn- remote-node-state*
  [{:keys [remote-launcher-ops]} ssh logical-node node]
  (cond
    (controller-local-node? node)
    (safe-read-edn-file (remote-state-file node))

    :else
    (if-let [f (:node-state remote-launcher-ops)]
      (f ssh logical-node node)
      (with-control-ssh ssh
        (control/on (:logical-node node)
          (try
            (some-> (control/exec :cat (remote-state-file node))
                    edn/read-string)
            (catch Throwable _
              nil)))))))

(defn wait-for-remote-node-running!
  [{:keys [now-ms] :as deps} ssh node timeout-ms]
  (let [deadline (+ (now-ms) (long timeout-ms))]
    (loop [last-state nil]
      (let [state (remote-node-state* deps ssh (:logical-node node) node)
            status (:status state)]
        (cond
          (= :running status)
          state

          (= :failed status)
          (u/raise "Remote Jepsen node failed to start"
                   {:node (:logical-node node)
                    :state state})

          (< (now-ms) deadline)
          (do
            (Thread/sleep (long remote-state-poll-ms))
            (recur (or state last-state)))

          :else
          (u/raise "Timed out waiting for remote Jepsen node to start"
                   {:node (:logical-node node)
                    :timeout-ms timeout-ms
                    :last-state last-state}))))))

(defn upload-remote-config!
  [{:keys [remote-launcher-ops]} ssh node local-config-path]
  (cond
    (controller-local-node? node)
    (copy-local-file! local-config-path (remote-config-path-for-node node))

    :else
    (if-let [f (:upload-config remote-launcher-ops)]
      (f ssh node local-config-path)
      (with-control-ssh ssh
        (control/on (:logical-node node)
          (control/exec :mkdir :-p (:root node))
          (control/upload local-config-path (remote-config-path-for-node node)))))))

(defn persist-remote-config!
  [{:keys [clusters]} cluster-id]
  (when-let [{:keys [remote? remote-config remote-config-path]} (get @clusters cluster-id)]
    (when remote?
      (spit remote-config-path (pr-str remote-config))
      remote-config-path)))

(defn start-remote-node-launcher!
  [{:keys [remote-launcher-ops]} ssh repo-root node verbose?]
  (cond
    (controller-local-node? node)
    (start-controller-local-launcher! repo-root node verbose?)

    :else
    (if-let [f (:start-launcher remote-launcher-ops)]
      (f ssh repo-root node verbose?)
      (let [launcher-args (cond-> [(remote-script-path repo-root
                                                       "start-remote-node")
                                   "--config"
                                   (remote-config-path-for-node node)
                                   "--node"
                                   (:logical-node node)]
                            verbose? (conj "--verbose"))
            command (str "mkdir -p " (control/escape (:root node))
                         " && python3 "
                         (control/escape
                          (remote-script-path repo-root
                                              remote-daemon-spawn-script))
                         " "
                         (control/escape (remote-launch-log-path node))
                         " "
                         (str/join " " (map control/escape launcher-args)))]
        (with-control-ssh ssh
          (control/on (:logical-node node)
            (control/exec :bash :-lc command)))))))

(defn stop-remote-node-launcher!
  [{:keys [remote-launcher-ops]} ssh repo-root node]
  (cond
    (controller-local-node? node)
    (stop-controller-local-launcher! repo-root node)

    :else
    (if-let [f (:stop-launcher remote-launcher-ops)]
      (f ssh repo-root node)
      (with-control-ssh ssh
        (control/on (:logical-node node)
          (control/cd repo-root
            (control/exec :bash :-lc
                          (str "script/jepsen/stop-remote-node"
                               " --config "
                               (control/escape (remote-config-path-for-node node))
                               " --node "
                               (control/escape (:logical-node node))))))))))

(defn signal-remote-node!
  [{:keys [remote-launcher-ops]} ssh node signal]
  (cond
    (controller-local-node? node)
    (signal-controller-local-node! node signal)

    :else
    (if-let [f (:signal-node remote-launcher-ops)]
      (f ssh node signal)
      (with-control-ssh ssh
        (control/on (:logical-node node)
          (control/exec :bash :-lc
                        (str "kill -" signal " $(cat "
                             (control/escape (remote-pid-file node))
                             ")")))))))

(defn remote-admin-client!
  [{:keys [clusters admin-uri conn-client-opts]} cluster-id logical-node]
  (locking clusters
    (let [{:keys [node-by-name]} (get @clusters cluster-id)
          endpoint (get-in node-by-name [logical-node :endpoint])
          existing (get-in @clusters [cluster-id :remote-admin-clients logical-node])]
      (cond
        (nil? endpoint)
        nil

        (and existing (not (cl/disconnected? existing)))
        existing

        :else
        (let [client (cl/new-client (admin-uri endpoint) conn-client-opts)]
          (swap! clusters assoc-in
                 [cluster-id :remote-admin-clients logical-node]
                 client)
          client)))))

(defn remote-ha-watermark
  [{:keys [clusters transport-failure?] :as deps} cluster-id logical-node]
  (let [{:keys [db-name]} (get @clusters cluster-id)]
    (when-let [client (remote-admin-client! deps cluster-id logical-node)]
      (try
        (cl/normal-request client :ha-watermark [db-name] false)
        (catch Throwable e
          (when (transport-failure? e)
            (safe-disconnect-client! client)
            (swap! clusters assoc-in
                   [cluster-id :remote-admin-clients logical-node]
                   nil))
          (throw e))))))

(defn close-remote-admin-client!
  [{:keys [clusters]} cluster-id logical-node]
  (when-let [client (get-in @clusters [cluster-id :remote-admin-clients logical-node])]
    (safe-disconnect-client! client)
    (swap! clusters assoc-in [cluster-id :remote-admin-clients logical-node] nil)))

(defn safe-stop-remote-launcher!
  [deps ssh repo-root node]
  (try
    (stop-remote-node-launcher! deps ssh repo-root node)
    (catch Throwable _
      nil)))

(defn build-remote-cluster-state
  [cluster-id config config-path ssh topology workload base-opts setup-timeout-ms
   verbose?]
  (let [data-nodes    (:data-nodes topology)
        control-nodes (:control-nodes topology)
        control-only  (:control-only-nodes topology)]
    {:cluster-id      cluster-id
     :remote?         true
     :db-name         (:db-name config)
     :db-identity     (:db-identity config)
     :schema          (:schema workload)
     :control-backend (:control-backend config)
     :base-opts       base-opts
     :verbose?        verbose?
     :repo-root       (:repo-root config)
     :remote-config   config
     :remote-config-path config-path
     :setup-timeout-ms setup-timeout-ms
     :nodes           data-nodes
     :control-nodes   control-nodes
     :data-node-names (vec (map :logical-node data-nodes))
     :control-node-names (vec (map :logical-node control-nodes))
     :control-only-node-names
     (vec (map :logical-node control-only))
     :node-by-id      (into {}
                            (map (juxt :node-id :logical-node))
                            control-nodes)
     :node-by-name    (into {}
                            (map (juxt :logical-node identity))
                            control-nodes)
     :endpoint->node  (into {}
                            (map (juxt :endpoint :logical-node))
                            control-nodes)
     :peer-id->node   (into {}
                            (map (juxt :peer-id :logical-node))
                            control-nodes)
     :servers         {}
     :control-authorities {}
     :admin-conns     {}
     :remote-admin-clients {}
     :ssh             ssh
     :live-nodes      (set (map :logical-node data-nodes))
     :network-grudge  (sorted-map)
     :dropped-links   #{}
     :link-behaviors  (sorted-map)
     :network-behavior nil
     :paused-nodes    #{}
     :paused-node-info {}
     :node-ha-opt-overrides {}
     :storage-faults  {}
     :stopped-node-info {}
     :teardown-nodes  #{}}))

(defn- launch-node-stages
  [topology]
  (let [control-only-nodes (vec (:control-only-nodes topology))
        data-node-names    (set (map :logical-node (:data-nodes topology)))
        data-launch-nodes  (vec (filter #(contains? data-node-names
                                                   (:logical-node %))
                                        (:control-nodes topology)))]
    (cond-> []
      (seq control-only-nodes) (conj control-only-nodes)
      (seq data-launch-nodes) (conj data-launch-nodes))))

(defn init-remote-cluster!
  [{:keys [clusters cluster-setup-timeout-ms wait-for-single-leader!
           cluster-timeout-ms] :as deps}
   cluster-id test {:keys [config config-path ssh topology workload]}]
  (let [clock-skew?      (some #{:clock-skew-pause
                                 :clock-skew-leader-fast
                                 :clock-skew-leader-slow
                                 :clock-skew-mixed}
                               (:datalevin/nemesis-faults test))
        config*          (cond-> config
                           clock-skew?
                           (assoc :jepsen-remote-clock-skew-hook? true))
        _                (spit config-path (pr-str config*))
        data-nodes       (:data-nodes topology)
        control-nodes    (:control-nodes topology)
        base-opts        (remote/base-ha-opts config* data-nodes control-nodes)
        setup-timeout-ms (cluster-setup-timeout-ms base-opts)
        verbose?         (boolean (:verbose test))
        launch-stages    (launch-node-stages topology)
        all-nodes        (vec control-nodes)]
    (try
      (doseq [node all-nodes]
        (upload-remote-config! deps ssh node config-path))
      (doseq [node all-nodes]
        (safe-stop-remote-launcher! deps ssh (remote-node-repo-root config node) node))
      (doseq [node all-nodes]
        (sync-remote-node-control-defaults! deps ssh node))
      (doseq [nodes launch-stages
              :when (seq nodes)]
        (doseq [node nodes]
          (start-remote-node-launcher! deps
                                       ssh
                                       (remote-node-repo-root config node)
                                       node
                                       verbose?))
        (doseq [node nodes]
          (wait-for-remote-node-running! (assoc deps :now-ms now-ms)
                                         ssh
                                         node
                                         setup-timeout-ms)))
      (let [cluster (build-remote-cluster-state cluster-id
                                                config*
                                                config-path
                                                ssh
                                                topology
                                                workload
                                                base-opts
                                                setup-timeout-ms
                                                verbose?)]
        (swap! clusters assoc cluster-id cluster)
        (wait-for-single-leader! cluster-id setup-timeout-ms)
        cluster)
      (catch Throwable e
        (doseq [node (reverse all-nodes)]
          (safe-stop-remote-launcher! deps
                                      ssh
                                      (remote-node-repo-root config node)
                                      node))
        (swap! clusters dissoc cluster-id)
        (throw e)))))

(defn teardown-remote-cluster!
  [{:keys [clusters] :as deps} cluster-id]
  (when-let [{:keys [node-by-name ssh] :as cluster} (get @clusters cluster-id)]
    (doseq [logical-node (keys (:remote-admin-clients (get @clusters cluster-id)))]
      (close-remote-admin-client! deps cluster-id logical-node))
    (doseq [node (reverse (vals node-by-name))]
      (safe-stop-remote-launcher! deps
                                  ssh
                                  (remote-node-repo-root cluster node)
                                  node))
    (swap! clusters dissoc cluster-id)))
