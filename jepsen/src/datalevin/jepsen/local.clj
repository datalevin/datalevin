(ns datalevin.jepsen.local
  (:require
   [datalevin.constants :as c]
   [datalevin.ha :as dha]
   [datalevin.jepsen.init-cache :as init-cache]
   [datalevin.jepsen.local.cluster :as lcluster]
   [datalevin.jepsen.local.faults :as lfaults]
   [datalevin.jepsen.local.ops :as lops]
   [datalevin.jepsen.local.remote :as lremote]
   [datalevin.jepsen.remote :as remote]
   [datalevin.kv :as kv]
   [datalevin.server :as srv]
   [jepsen.db :as db]))

(def ^:private cluster-timeout-ms 10000)
(def ^:private default-cluster-setup-timeout-ms 30000)
; Keep request timeout close to Jepsen's leader wait so paused nodes fail fast.
(def ^:private conn-client-opts {:pool-size 1 :time-out cluster-timeout-ms})

(def default-nodes ["n1" "n2" "n3"])

(defonce ^:private clusters (atom {}))
(defonce ^:private next-port-block (atom -1))
(defonce ^:private remote-runtime-nodes (atom {}))

(def ^:private base-fetch-ha-leader-txlog-batch dha/fetch-ha-leader-txlog-batch)
(def ^:private base-report-ha-replica-floor! dha/report-ha-replica-floor!)
(def ^:private base-fetch-ha-endpoint-snapshot-copy!
  dha/fetch-ha-endpoint-snapshot-copy!)

(def ^:dynamic *remote-launcher-ops* nil)

(declare wait-for-single-leader!
         wait-for-authority-leader!
         cluster-state
         node-diagnostics
         effective-local-lsn
         endpoint-for-node
         transport-failure?
         storage-fault)

(defn- remote-cluster?
  [cluster-id]
  (true? (get-in @clusters [cluster-id :remote?])))

(defn- cluster-entry-for-db-identity
  [db-identity]
  (some (fn [[cluster-id {:keys [db-identity] :as cluster}]]
          (when (= db-identity (:db-identity cluster))
            [cluster-id cluster]))
        @clusters))

(defn remote-runtime-node
  [db-identity node-id]
  (get @remote-runtime-nodes [db-identity node-id]))

(defn register-remote-node-runtime!
  [config topology node]
  (let [control-nodes (:control-nodes topology)
        data-nodes    (:data-nodes topology)
        metadata      {:db-identity (:db-identity config)
                       :db-name (:db-name config)
                       :node-id (:node-id node)
                       :logical-node (:logical-node node)
                       :endpoint (:endpoint node)
                       :root (:root node)
                       :data-node? (contains? (set (map :logical-node data-nodes))
                                              (:logical-node node))
                       :endpoint->node
                       (into {}
                             (map (juxt :endpoint :logical-node))
                             control-nodes)
                       :network-state-file (remote/network-state-file node)
                       :storage-fault-state-file
                       (remote/storage-fault-state-file node)
                       :clock-skew-state-file
                       (remote/clock-skew-state-file node)
                       :fencing-mode-file (remote/fencing-mode-file node)
                       :snapshot-failpoint-file
                       (remote/snapshot-failpoint-file node)}]
    (swap! remote-runtime-nodes assoc
           [(:db-identity config) (:node-id node)]
           metadata)
    metadata))

(defn unregister-remote-node-runtime!
  [db-identity node-id]
  (swap! remote-runtime-nodes dissoc [db-identity node-id])
  true)

(defn admin-uri
  [endpoint]
  (lcluster/admin-uri endpoint))

(defn db-uri
  [endpoint db-name]
  (lcluster/db-uri endpoint db-name))

(defn endpoint-for-node
  [cluster-id logical-node]
  (get-in @clusters [cluster-id :node-by-name logical-node :endpoint]))

(defn ^:redef cluster-state
  [cluster-id]
  (get @clusters cluster-id))

(defn paused-node-info
  [cluster-id logical-node]
  (get-in @clusters [cluster-id :paused-node-info logical-node]))

(defn stopped-node-info
  [cluster-id logical-node]
  (get-in @clusters [cluster-id :stopped-node-info logical-node]))

(defn- remote-deps
  []
  {:clusters clusters
   :remote-launcher-ops *remote-launcher-ops*
   :endpoint-for-node endpoint-for-node
   :cluster-setup-timeout-ms lcluster/cluster-setup-timeout-ms
   :wait-for-single-leader! (fn [cluster-id timeout-ms]
                              (wait-for-single-leader! cluster-id timeout-ms))
   :transport-failure? transport-failure?
   :admin-uri admin-uri
   :conn-client-opts conn-client-opts})

(defn- faults-deps
  []
  {:clusters clusters
   :remote-runtime-node remote-runtime-node
   :cluster-entry-for-db-identity cluster-entry-for-db-identity
   :endpoint-for-node endpoint-for-node
   :write-remote-content! (fn [ssh node remote-path content]
                            (lremote/write-remote-content! (remote-deps)
                                                           ssh
                                                           node
                                                           remote-path
                                                           content))
   :delete-remote-path! (fn [ssh node remote-path]
                          (lremote/delete-remote-path! (remote-deps)
                                                       ssh
                                                       node
                                                       remote-path))
   :sync-remote-network-state! (fn [cluster-id]
                                 (lremote/sync-remote-network-state!
                                  (assoc (remote-deps)
                                         :endpoint-for-node endpoint-for-node)
                                  cluster-id))
   :wait-for-authority-leader! (fn [cluster-id timeout-ms]
                                 (wait-for-authority-leader! cluster-id
                                                             timeout-ms))
   :cluster-state cluster-state
   :base-fetch-ha-leader-txlog-batch base-fetch-ha-leader-txlog-batch
   :base-report-ha-replica-floor! base-report-ha-replica-floor!
   :base-fetch-ha-endpoint-snapshot-copy!
   base-fetch-ha-endpoint-snapshot-copy!})

(defn storage-fault
  [cluster-id logical-node]
  (lfaults/storage-fault (faults-deps) cluster-id logical-node))

(def transport-failure? lfaults/transport-failure?)
(def write-disruption-fault-active? lfaults/write-disruption-fault-active?)
(def expected-disruption-write-failure?
  lfaults/expected-disruption-write-failure?)

(defn- cluster-deps
  []
  {:clusters clusters
   :next-port-block next-port-block
   :default-nodes default-nodes
   :remote-deps remote-deps
   :fault-deps (fn []
                 {:heal-network! (fn [cluster-id]
                                   (lfaults/heal-network! (faults-deps)
                                                          cluster-id))})
   :wait-for-single-leader! (fn [cluster-id timeout-ms]
                              (wait-for-single-leader! cluster-id timeout-ms))
   :node-diagnostics (fn [cluster-id logical-node]
                       (node-diagnostics cluster-id logical-node))
   :effective-local-lsn (fn [cluster-id logical-node]
                          (effective-local-lsn cluster-id logical-node))
   :transport-failure? transport-failure?})

(defn- ops-deps
  []
  {:clusters clusters
   :remote-cluster? remote-cluster?
   :remote-deps remote-deps
   :cluster-deps cluster-deps
   :transport-failure? transport-failure?
   :storage-fault storage-fault})

(defonce ^:private partition-aware-ha-transports-installed?
  (do
    (alter-var-root
     #'dha/fetch-ha-leader-txlog-batch
     (constantly
      (fn [db-name m leader-endpoint from-lsn upto-lsn]
        (lfaults/partition-aware-fetch-ha-leader-txlog-batch
         (faults-deps) db-name m leader-endpoint from-lsn upto-lsn))))
    (alter-var-root
     #'dha/report-ha-replica-floor!
     (constantly
      (fn [db-name m leader-endpoint applied-lsn]
        (lfaults/partition-aware-report-ha-replica-floor!
         (faults-deps) db-name m leader-endpoint applied-lsn))))
    (alter-var-root
     #'dha/fetch-ha-endpoint-snapshot-copy!
     (constantly
      (fn [db-name m endpoint dest-dir]
        (lfaults/partition-aware-fetch-ha-endpoint-snapshot-copy!
         (faults-deps) db-name m endpoint dest-dir))))
    true))

(defonce ^:private storage-fault-hook-installed?
  (do
    (kv/set-storage-fault-hook!
     (fn [context]
       (lfaults/maybe-apply-storage-fault! (faults-deps) context)))
    true))

(defonce ^:private server-runtime-opts-hook-installed?
  (do
    (alter-var-root #'srv/*server-runtime-opts-fn*
                    (constantly lcluster/resolved-server-runtime-opts))
    true))

(defn workload-setup-timeout-ms
  ([cluster-id]
   (workload-setup-timeout-ms cluster-id default-cluster-setup-timeout-ms))
  ([cluster-id default-timeout-ms]
   (lcluster/workload-setup-timeout-ms (cluster-deps)
                                       cluster-id
                                       default-timeout-ms)))

(defn effective-local-lsn
  [cluster-id logical-node]
  (lops/effective-local-lsn (ops-deps) cluster-id logical-node))

(defn node-progress-lsn
  [cluster-id logical-node]
  (lops/node-progress-lsn (ops-deps) cluster-id logical-node))

(defn wait-for-live-nodes-at-least-lsn!
  ([cluster-id target-lsn]
   (wait-for-live-nodes-at-least-lsn! cluster-id target-lsn cluster-timeout-ms))
  ([cluster-id target-lsn timeout-ms]
   (lops/wait-for-live-nodes-at-least-lsn! (ops-deps)
                                           cluster-id
                                           target-lsn
                                           timeout-ms)))

(defn wait-for-nodes-at-least-lsn!
  ([cluster-id logical-nodes target-lsn]
   (wait-for-nodes-at-least-lsn! cluster-id logical-nodes target-lsn
                                 cluster-timeout-ms))
  ([cluster-id logical-nodes target-lsn timeout-ms]
   (lops/wait-for-nodes-at-least-lsn! (ops-deps)
                                      cluster-id
                                      logical-nodes
                                      target-lsn
                                      timeout-ms)))

(defn wait-for-at-least-nodes-at-least-lsn!
  ([cluster-id logical-nodes target-lsn required-count]
   (wait-for-at-least-nodes-at-least-lsn! cluster-id logical-nodes target-lsn
                                          required-count cluster-timeout-ms))
  ([cluster-id logical-nodes target-lsn required-count timeout-ms]
   (lops/wait-for-at-least-nodes-at-least-lsn! (ops-deps)
                                               cluster-id
                                               logical-nodes
                                               target-lsn
                                               required-count
                                               timeout-ms)))

(defn ^:redef wait-for-single-leader!
  ([cluster-id]
   (wait-for-single-leader! cluster-id cluster-timeout-ms))
  ([cluster-id timeout-ms]
   (lops/wait-for-single-leader! (ops-deps) cluster-id timeout-ms)))

(defn ^:redef maybe-wait-for-single-leader
  ([cluster-id]
   (maybe-wait-for-single-leader cluster-id cluster-timeout-ms))
  ([cluster-id timeout-ms]
   (lops/maybe-wait-for-single-leader (ops-deps) cluster-id timeout-ms)))

(defn ^:redef wait-for-authority-leader!
  ([cluster-id]
   (wait-for-authority-leader! cluster-id cluster-timeout-ms))
  ([cluster-id timeout-ms]
   (lops/wait-for-authority-leader! (ops-deps) cluster-id timeout-ms)))

(defn ^:redef maybe-wait-for-authority-leader
  ([cluster-id]
   (maybe-wait-for-authority-leader cluster-id cluster-timeout-ms))
  ([cluster-id timeout-ms]
   (lops/maybe-wait-for-authority-leader (ops-deps) cluster-id timeout-ms)))

(defn node-diagnostics
  [cluster-id logical-node]
  (lops/node-diagnostics (ops-deps) cluster-id logical-node))

(defn local-query
  [cluster-id logical-node q & inputs]
  (apply lops/local-query (ops-deps) cluster-id logical-node q inputs))

(defn ^:redef clock-skew-enabled?
  [cluster-id]
  (boolean
   (or (get-in @clusters [cluster-id :clock-skew-dir])
       (get-in @clusters
               [cluster-id :remote-config :jepsen-remote-clock-skew-hook?]))))

(defn ^:redef clock-skew-budget-ms
  [cluster-id]
  (long (or (get-in @clusters [cluster-id :base-opts :ha-clock-skew-budget-ms])
            c/*ha-clock-skew-budget-ms*)))

(defn ^:redef set-node-clock-skew!
  [cluster-id logical-node skew-ms]
  (when-let [{:keys [clock-skew-dir node-by-name remote? ssh]} (get @clusters cluster-id)]
    (when-let [node (get node-by-name logical-node)]
      (if remote?
        (lremote/write-remote-content! (remote-deps)
                                       ssh
                                       node
                                       (remote/clock-skew-state-file node)
                                       (str (long skew-ms) "\n"))
        (lcluster/write-clock-skew-ms! clock-skew-dir (:node-id node) skew-ms))
      true)))

(defn network-link-behaviors
  [cluster-id]
  (lfaults/network-link-behaviors (faults-deps) cluster-id))

(defn network-behavior
  [cluster-id]
  (lfaults/network-behavior (faults-deps) cluster-id))

(defn apply-network-shape!
  [cluster-id nodes behavior]
  (lfaults/apply-network-shape! (faults-deps) cluster-id nodes behavior))

(defn heal-network!
  [cluster-id]
  (lfaults/heal-network! (faults-deps) cluster-id))

(defn apply-network-grudge!
  [cluster-id grudge]
  (lfaults/apply-network-grudge! (faults-deps) cluster-id grudge))

(defn network-grudge
  [cluster-id]
  (lfaults/network-grudge (faults-deps) cluster-id))

(defn leader-partition-grudge
  ([cluster-id]
   (leader-partition-grudge cluster-id
                            (:leader (wait-for-authority-leader! cluster-id))))
  ([cluster-id leader]
   (lfaults/leader-partition-grudge (faults-deps) cluster-id leader)))

(defn random-graph-cut
  [cluster-id]
  (lfaults/random-graph-cut (faults-deps) cluster-id))

(defn random-degraded-network-shape
  [cluster-id]
  (lfaults/random-degraded-network-shape (faults-deps) cluster-id))

(defn open-leader-conn!
  [test schema]
  (lops/open-leader-conn! (ops-deps) test schema))

(defn with-leader-conn
  [test schema f]
  (lops/with-leader-conn (ops-deps) test schema f))

(defn open-node-conn!
  [test logical-node schema]
  (lops/open-node-conn! (ops-deps) test logical-node schema))

(defn with-node-conn
  [test logical-node schema f]
  (lops/with-node-conn (ops-deps) test logical-node schema f))

(defn with-admin-node-conn
  [test logical-node f]
  (lops/with-admin-node-conn (ops-deps) test logical-node f))

(defn with-admin-leader-conn
  [test f]
  (lops/with-admin-leader-conn (ops-deps) test f))

(defn override-node-ha-opts!
  [cluster-id logical-node override-opts]
  (lops/override-node-ha-opts! (ops-deps) cluster-id logical-node override-opts))

(defn clear-node-ha-opts-override!
  [cluster-id logical-node]
  (lops/clear-node-ha-opts-override! (ops-deps) cluster-id logical-node))

(defn txlog-retention-state
  [cluster-id logical-node]
  (lops/txlog-retention-state (ops-deps) cluster-id logical-node))

(defn copy-backup-pin-ids
  [cluster-id logical-node]
  (lops/copy-backup-pin-ids (ops-deps) cluster-id logical-node))

(defn assoc-opt-on-node!
  [cluster-id logical-node k v]
  (lops/assoc-opt-on-node! (ops-deps) cluster-id logical-node k v))

(defn assoc-opt-on-node-store!
  [cluster-id logical-node k v]
  (lops/assoc-opt-on-node-store! (ops-deps) cluster-id logical-node k v))

(defn set-live-node-ha-membership!
  [cluster-id logical-node members]
  (lops/set-live-node-ha-membership! (ops-deps)
                                     cluster-id
                                     logical-node
                                     members))

(defn assoc-opt-on-stopped-node-store!
  [cluster-id logical-node k v]
  (lops/assoc-opt-on-stopped-node-store! (ops-deps) cluster-id logical-node k v))

(defn clear-copy-backup-pins-on-node!
  [cluster-id logical-node]
  (lops/clear-copy-backup-pins-on-node! (ops-deps) cluster-id logical-node))

(defn create-snapshot-on-node!
  [cluster-id logical-node]
  (lops/create-snapshot-on-node! (ops-deps) cluster-id logical-node))

(defn create-snapshots-on-nodes!
  [cluster-id logical-nodes]
  (lops/create-snapshots-on-nodes! (ops-deps) cluster-id logical-nodes))

(defn gc-txlog-segments-on-node!
  [cluster-id logical-node]
  (lops/gc-txlog-segments-on-node! (ops-deps) cluster-id logical-node))

(defn wait-for-follower-bootstrap!
  ([cluster-id logical-node min-snapshot-lsn]
   (wait-for-follower-bootstrap! cluster-id logical-node min-snapshot-lsn
                                 cluster-timeout-ms))
  ([cluster-id logical-node min-snapshot-lsn timeout-ms]
   (lops/wait-for-follower-bootstrap! (ops-deps)
                                      cluster-id
                                      logical-node
                                      min-snapshot-lsn
                                      timeout-ms)))

(defn ^:redef stop-node!
  [cluster-id logical-node]
  (lcluster/stop-node! (cluster-deps) cluster-id logical-node))

(defn ^:redef restart-node!
  [cluster-id logical-node]
  (lcluster/restart-node! (cluster-deps) cluster-id logical-node))

(defn ^:redef pause-node!
  [cluster-id logical-node]
  (lcluster/pause-node! (cluster-deps) cluster-id logical-node))

(defn resume-node!
  [cluster-id logical-node]
  (lcluster/resume-node! (cluster-deps) cluster-id logical-node))

(defn wedge-node-storage!
  [cluster-id logical-node fault]
  (lfaults/wedge-node-storage! (faults-deps) cluster-id logical-node fault))

(defn heal-node-storage!
  [cluster-id logical-node]
  (lfaults/heal-node-storage! (faults-deps) cluster-id logical-node))

(defn set-node-snapshot-failpoint!
  [cluster-id logical-node mode]
  (lfaults/set-node-snapshot-failpoint! (faults-deps)
                                        cluster-id
                                        logical-node
                                        mode))

(defn clear-node-snapshot-failpoint!
  [cluster-id logical-node]
  (lfaults/clear-node-snapshot-failpoint! (faults-deps)
                                          cluster-id
                                          logical-node))

(defn set-fencing-hook-mode!
  [cluster-id mode]
  (lfaults/set-fencing-hook-mode! (faults-deps) cluster-id mode))

(def ^:private remote-config-path-for-node lremote/remote-config-path-for-node)
(def ^:private remote-launch-log-path lremote/remote-launch-log-path)
(def ^:private remote-pid-file lremote/remote-pid-file)
(def ^:private remote-state-file lremote/remote-state-file)

(defn- run-local-command
  [repo-root cmd timeout-ms]
  (#'lremote/run-local-command repo-root cmd timeout-ms))

(defrecord LocalClusterDB [cluster-id]
  db/DB
  (setup! [this test _node]
    (locking clusters
      (when-not (cluster-state cluster-id)
        (lcluster/init-cluster! (cluster-deps) cluster-id test)))
    this)

  (teardown! [this test node]
    (locking clusters
      (when-let [cluster (cluster-state cluster-id)]
        (let [teardown-nodes (conj (:teardown-nodes cluster) node)]
          (swap! clusters assoc-in [cluster-id :teardown-nodes] teardown-nodes)
          (when (= teardown-nodes (set (:nodes test)))
            (lcluster/teardown-cluster! (cluster-deps) cluster-id)
            (init-cache/release-cluster! cluster-id)))))
    this))

(defrecord RemoteClusterDB [cluster-id remote-spec]
  db/DB
  (setup! [this test _node]
    (locking clusters
      (when-not (cluster-state cluster-id)
        (lremote/init-remote-cluster! (assoc (remote-deps)
                                             :wait-for-single-leader!
                                             (fn [cluster-id timeout-ms]
                                               (wait-for-single-leader!
                                                cluster-id
                                                timeout-ms)))
                                      cluster-id
                                      test
                                      remote-spec)))
    this)

  (teardown! [this test node]
    (locking clusters
      (when-let [cluster (cluster-state cluster-id)]
        (let [teardown-nodes (conj (:teardown-nodes cluster) node)]
          (swap! clusters assoc-in [cluster-id :teardown-nodes] teardown-nodes)
          (when (= teardown-nodes (set (:nodes test)))
            (lremote/teardown-remote-cluster! (remote-deps) cluster-id)
            (init-cache/release-cluster! cluster-id)))))
    this))

(defn db
  [cluster-id]
  (->LocalClusterDB cluster-id))

(defn remote-db
  [cluster-id remote-spec]
  (->RemoteClusterDB cluster-id remote-spec))

(defn net
  [cluster-id]
  (lfaults/->LocalClusterNet cluster-id (faults-deps)))
