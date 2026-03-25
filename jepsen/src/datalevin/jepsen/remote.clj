 (ns datalevin.jepsen.remote
   (:require
    [clojure.edn :as edn]
    [clojure.string :as str]
    [datalevin.util :as u]))

 (def ^:private control-state-dir-name "jepsen-control")

 (defn control-state-dir
   [node]
   (str (:root node) u/+separator+ control-state-dir-name))

 (defn network-state-file
   [node]
   (str (control-state-dir node) u/+separator+ "network.edn"))

 (defn storage-fault-state-file
   [node]
   (str (control-state-dir node) u/+separator+ "storage-fault.edn"))

 (defn clock-skew-state-file
   [node]
   (str (control-state-dir node) u/+separator+ "clock-skew-ms.txt"))

 (defn fencing-mode-file
   [node]
   (str (control-state-dir node) u/+separator+ "fencing-mode.txt"))

 (defn snapshot-failpoint-file
   [node]
   (str (control-state-dir node) u/+separator+ "snapshot-failpoint.edn"))

 (defn remote-clock-skew-hook
   [node]
   {:cmd ["/bin/sh"
          "-c"
          "cat \"$1\" 2>/dev/null || echo 0"
          "clock-skew-hook"
          (clock-skew-state-file node)]
    :timeout-ms 1000
    :retries 0
    :retry-delay-ms 0})

 (defn remote-fencing-hook
   [node]
   {:cmd ["/bin/sh"
          "-c"
          (str "mode=$(cat \"$1\" 2>/dev/null || true); "
               "if [ -z \"$mode\" ]; then mode=success; fi; "
               "if [ \"$mode\" = fail ]; then exit 7; fi; "
               "exit 0")
          "fence-hook"
          (fencing-mode-file node)]})

 (defn read-config
   [path]
   (-> path slurp edn/read-string))

 (defn parse-endpoint
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
   (when-let [repo-root (:repo-root node)]
     (when-not (string? repo-root)
       (u/raise "Remote Jepsen node repo roots must be strings"
                {:node node})))
   (when-let [controller-local? (:controller-local? node)]
     (when-not (instance? Boolean controller-local?)
       (u/raise "Remote Jepsen controller-local flag must be boolean"
                {:node node})))
   (parse-endpoint (:endpoint node))
   (parse-endpoint (:peer-id node))
   node)

 (defn configured-control-node-entries
   [config]
   (vec (or (:control-nodes config)
            (:nodes config))))

 (defn validate-cluster-topology!
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
       (assoc merged
              (:logical-node node)
              (merge existing node)))
     (assoc merged (:logical-node node) node)))

 (defn validate-config!
   [config workloads]
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
     (when-not (contains? workloads (:workload config))
       (u/raise "Unsupported remote Jepsen workload"
                {:workload (:workload config)
                 :supported (sort (keys workloads))}))
     (when-not (string? (:group-id config))
       (u/raise "Remote Jepsen config requires :group-id"
                {:config config}))
     (when-not (string? (:db-identity config))
       (u/raise "Remote Jepsen config requires :db-identity"
                {:config config}))
     (when-let [ssh-opts (:ssh config)]
       (when-not (map? ssh-opts)
         (u/raise "Remote Jepsen config :ssh must be a map"
                  {:ssh ssh-opts})))
     (when-not (= :sofa-jraft (or (:control-backend config) :sofa-jraft))
       (u/raise
        "Remote Jepsen node launcher currently requires :control-backend :sofa-jraft"
        {:control-backend (:control-backend config)}))
     (assoc config :control-backend :sofa-jraft)))

 (defn config-workload
   [config workloads]
   (let [workload-fn        (get workloads (:workload config))
         data-node-names    (mapv :logical-node (:nodes config))
         control-node-names (mapv :logical-node
                                  (configured-control-node-entries config))
         workload-opts      (merge (:workload-opts config)
                                   (select-keys config
                                                [:key-count
                                                 :account-balance
                                                 :max-transfer
                                                 :max-writes-per-key
                                                 :min-txn-length
                                                 :max-txn-length
                                                 :giant-payload-bytes])
                                   {:datalevin/remote-runner? true})
         workload-opts'     (assoc workload-opts
                                   :nodes data-node-names
                                   :datalevin/control-nodes control-node-names)]
     (workload-fn workload-opts')))

 (defn workload-topology
   [config workload]
   (let [data-node-names    (vec (or (seq (:nodes workload))
                                     (map :logical-node (:nodes config))))
         control-node-names (vec (or (seq (:datalevin/control-nodes workload))
                                     (map :logical-node
                                          (configured-control-node-entries
                                           config))))
         _                  (validate-cluster-topology!
                             data-node-names
                             control-node-names
                             (:control-backend config))
         configured-node-map (reduce merge-node-entry!
                                     {}
                                     (concat (:nodes config)
                                             (configured-control-node-entries
                                              config)))
         data-nodes         (mapv
                             (fn [node-name]
                               (or (get configured-node-map node-name)
                                   (u/raise
                                    "Remote Jepsen data node is missing from config"
                                    {:node node-name
                                     :available
                                     (sort (keys configured-node-map))})))
                             data-node-names)
         control-nodes      (mapv
                             (fn [node-name]
                               (or (get configured-node-map node-name)
                                   (u/raise
                                    "Remote Jepsen control node is missing from config"
                                    {:node node-name
                                     :available
                                     (sort (keys configured-node-map))})))
                             control-node-names)
         data-node-set      (set data-node-names)
         control-only-nodes (->> control-nodes
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

 (defn merge-ha-opts
   [base-opts override-opts]
   (if (map? override-opts)
     (let [control-plane-override (:ha-control-plane override-opts)]
       (cond-> (merge base-opts (dissoc override-opts :ha-control-plane))
         (map? control-plane-override)
         (update :ha-control-plane merge control-plane-override)))
     base-opts))

 (defn base-ha-opts
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

 (defn node-ha-opts
   [config node workload data-nodes control-nodes]
   (let [node-override (get-in config
                               [:node-ha-opts-overrides (:logical-node node)])]
     (cond-> (-> (base-ha-opts config data-nodes control-nodes)
                 (merge-ha-opts (:datalevin/cluster-opts workload))
                 (merge-ha-opts node-override)
                 (assoc :db-name (:db-name config))
                 (assoc :ha-node-id (:node-id node))
                 (assoc-in [:ha-control-plane :local-peer-id] (:peer-id node)))
       (true? (:jepsen-remote-clock-skew-hook? config))
       (assoc :ha-clock-skew-hook (remote-clock-skew-hook node))

       (true? (:datalevin/remote-fencing-retry? workload))
       (assoc :ha-fencing-hook
              (merge {:timeout-ms 1000
                      :retries 0
                      :retry-delay-ms 0}
                     (remote-fencing-hook node)
                     (select-keys (or (:ha-fencing-hook
                                        (:datalevin/cluster-opts workload))
                                      {})
                                  [:timeout-ms :retries :retry-delay-ms]))))))
