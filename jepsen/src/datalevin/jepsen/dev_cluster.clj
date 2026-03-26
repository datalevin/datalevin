(ns datalevin.jepsen.dev-cluster
  (:require
   [clojure.pprint :as pp]
   [clojure.string :as str]
   [clojure.tools.cli :refer [parse-opts]]
   [datalevin.jepsen.core :as core]
   [datalevin.jepsen.local :as local]
   [jepsen.db :as jdb]))

(defn- parse-node-list
  [s]
  (->> (str/split (or s "") #",")
       (map str/trim)
       (remove str/blank?)
       vec))

(def ^:private supported-workloads
  (->> (keys core/workloads)
       sort
       (map name)
       (str/join ", ")))

(def ^:private cli-opts
  [[nil "--control-backend BACKEND" "HA control-plane backend: sofa-jraft"
    :parse-fn keyword
    :default :sofa-jraft
    :validate [#{:sofa-jraft}
               "Must be: sofa-jraft"]]

   [nil "--db-name NAME" "Database name to create for the cluster"
    :default "jepsen-dev"]

   [nil "--work-dir DIR" "Base work directory for local cluster data"]

   [nil "--keep-work-dir" "Keep local cluster work directories after shutdown"]

   [nil "--key-count NUM" "Number of active workload keys when applicable"
    :default 8
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--account-balance NUM" "Initial per-account balance for the bank workload"
    :default 100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-transfer NUM" "Maximum transfer amount for the bank workload"
    :default 5
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Upper bound for append values per key"
    :default 32
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--min-txn-length NUM" "Minimum transaction length"
    :default 1
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-txn-length NUM" "Maximum transaction length"
    :default 1
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   ["-w" "--workload NAME" "Workload whose schema/runtime opts should be used"
    :parse-fn keyword
    :default :append
    :validate [core/workloads (str "Supported workloads: " supported-workloads)]]

   ["-v" "--verbose" "Enable verbose Datalevin server logging"]

   [nil "--nodes N1,N2,N3" "Comma-separated logical data nodes"
    :parse-fn parse-node-list]

   [nil "--control-nodes N1,N2,N3" "Comma-separated logical control nodes"
    :parse-fn parse-node-list]

   [nil "--giant-payload-bytes BYTES" "Payload size for giant-value workloads"
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--print-edn" "Print cluster details as EDN"]

   ["-h" "--help" "Show this help"]])

(defn- usage
  [summary]
  (str/join
   \newline
   ["Start a local Jepsen-backed Datalevin cluster and keep it running."
    ""
    "Examples:"
    "  script/jepsen/start-local-cluster"
    "  script/jepsen/start-local-cluster --workload register --db-name dev-register"
    "  script/jepsen/start-local-cluster --workload witness-topology --keep-work-dir"
    ""
    "Press Ctrl-C to stop the cluster and tear it down."
    ""
    "Options:"
    summary]))

(defn- cluster-test
  [opts]
  (let [opts' (cond-> (merge {:workload :append
                              :control-backend :sofa-jraft
                              :db-name "jepsen-dev"
                              :rate 1
                              :time-limit 1
                              :nemesis []}
                             (dissoc opts :help :print-edn))
                (seq (:control-nodes opts))
                (assoc :datalevin/control-nodes (:control-nodes opts)))]
    (core/datalevin-test (dissoc opts' :control-nodes))))

(defn- node-summary
  [db-name node]
  {:logical-node (:logical-node node)
   :node-id      (:node-id node)
   :endpoint     (:endpoint node)
   :peer-id      (:peer-id node)
   :db-uri       (local/db-uri (:endpoint node) db-name)
   :root         (:root node)})

(defn- cluster-summary
  [test-map]
  (let [cluster-id          (:datalevin/cluster-id test-map)
        cluster             (local/cluster-state cluster-id)
        data-nodes          (:nodes cluster)
        control-node-map    (into {}
                                  (map (juxt :logical-node identity))
                                  (:control-nodes cluster))
        control-node-names  (set (:control-node-names cluster))
        data-node-names     (set (:data-node-names cluster))
        control-only-names  (->> control-node-names
                                 (remove data-node-names)
                                 sort)
        leader              (some-> (local/maybe-wait-for-single-leader
                                      cluster-id
                                      5000)
                                    :leader)]
    {:cluster-id         cluster-id
     :db-name            (:db-name test-map)
     :workload           (:workload test-map)
     :control-backend    (:control-backend test-map)
     :work-dir           (:work-dir cluster)
     :leader             leader
     :data-nodes         (mapv (partial node-summary (:db-name test-map))
                               data-nodes)
     :control-only-nodes (mapv (comp (partial node-summary (:db-name test-map))
                                     control-node-map)
                               control-only-names)}))

(defn- start-cluster!
  [test-map]
  (let [db (:db test-map)]
    (doseq [node (:nodes test-map)]
      (jdb/setup! db test-map node))
    test-map))

(defn- stop-cluster!
  [{:keys [test-map stopped?]}]
  (when (compare-and-set! stopped? false true)
    (doseq [node (:nodes test-map)]
      (try
        (jdb/teardown! (:db test-map) test-map node)
        (catch Throwable _
          nil)))))

(defn- print-summary!
  [summary print-edn?]
  (if print-edn?
    (prn summary)
    (do
      (println "Jepsen local cluster is running.")
      (println)
      (pp/pprint summary)
      (println)
      (println "Press Ctrl-C to stop."))))

(defn -main
  [& args]
  (let [{:keys [options errors summary]} (parse-opts args cli-opts)]
    (cond
      (:help options)
      (do
        (println (usage summary))
        (System/exit 0))

      (seq errors)
      (do
        (binding [*out* *err*]
          (doseq [error errors]
            (println error))
          (println)
          (println (usage summary)))
        (System/exit 2))

      :else
      (let [test-map      (cluster-test options)
            cluster-state {:test-map test-map
                           :stopped? (atom false)}
            shutdown-hook (Thread.
                           ^Runnable
                           #(stop-cluster! cluster-state))]
        (.addShutdownHook (Runtime/getRuntime) shutdown-hook)
        (try
          (start-cluster! test-map)
          (print-summary! (cluster-summary test-map) (:print-edn options))
          (loop []
            (Thread/sleep 60000)
            (recur))
          (catch InterruptedException _
            nil)
          (finally
            (stop-cluster! cluster-state)
            (try
              (.removeShutdownHook (Runtime/getRuntime) shutdown-hook)
              (catch IllegalStateException _
                nil))))))))
