(ns ha-drill
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [clojure.tools.cli :refer [parse-opts]]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ctrl]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.server :as srv]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [com.alipay.sofa.jraft Node]
   [datalevin.db DB]
   [datalevin.server Server]
   [datalevin.storage Store]
   [java.io File]
   [java.net ServerSocket]
   [java.util UUID]))

(def ^:private default-port-base 19001)
(def ^:private probe-dbi "__ha_drill_probe")
(def ^:private conn-client-opts {:pool-size 1 :time-out 60000})
(def ^:private drill-schema
  {:drill/key   {:db/valueType :db.type/string
                 :db/unique :db.unique/identity}
   :drill/value {:db/valueType :db.type/string}})
(def ^:private value-query
  '[:find ?v .
    :in $ ?k
    :where
    [?e :drill/key ?k]
    [?e :drill/value ?v]])

(def ^:private cli-options
  [["-w" "--work-dir DIR" "Run directory. Must not already contain drill data."]
   ["-p" "--port-base PORT" "Base client port for the 3-node local cluster"
    :default default-port-base
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 65434) "Must leave room for 3 client ports and 3 peer ports"]]
   ["-b" "--control-backend BACKEND"
    "Control-plane backend: in-memory or sofa-jraft"
    :default :in-memory
    :parse-fn keyword
    :validate [#{:in-memory :sofa-jraft}
               "Must be one of: in-memory, sofa-jraft"]]
   ["-d" "--db-name NAME" "Database name to create"
    :default "ha-drill"]
   ["-k" "--keep-work-dir" "Keep the run directory after success"]
   ["-v" "--verbose" "Enable verbose logging"]
   ["-h" "--help" "Show help"]])

(defn- usage
  [summary]
  (str "Usage: clojure -M:dev script/ha/drill.clj <scenario> [options]\n\n"
       "Scenarios:\n"
       "  failover\n"
       "  follower-rejoin\n"
       "  membership-hash-drift\n"
       "  fencing-hook-verify\n"
       "  clock-skew-pause\n"
       "  degraded-mode-no-valid-source\n"
       "  wal-gap\n"
       "  fencing-failure\n"
       "  witness-topology\n"
       "  control-quorum-loss\n\n"
       "Options:\n"
       summary))

(defn- step!
  [& xs]
  (binding [*out* *err*]
    (apply println xs)
    (flush)))

(defn- now-ms [] (System/currentTimeMillis))

(defn- reserve-port
  []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn- ensure-empty-or-create-dir!
  [path]
  (let [dir (io/file path)]
    (cond
      (.exists dir)
      (when-let [entries (seq (.listFiles dir))]
        (throw
          (ex-info "Drill work dir must be empty"
                   {:work-dir path
                    :entries  (mapv #(.getName ^File %) entries)})))

      :else
      (.mkdirs dir))
    path))

(defn- resolve-work-dir
  [{:keys [work-dir]} scenario]
  (if work-dir
    (ensure-empty-or-create-dir! work-dir)
    (let [dir (u/tmp-dir (str "ha-drill-" scenario "-" (UUID/randomUUID)))]
      (.mkdirs (io/file dir))
      dir)))

(defn- repo-root
  []
  (.getCanonicalPath (io/file ".")))

(defn- fence-script-path
  []
  (.getCanonicalPath (io/file (repo-root) "script" "ha" "fence-log.sh")))

(defn- clock-skew-script-path
  []
  (.getCanonicalPath (io/file (repo-root) "script" "ha" "clock-skew-file.sh")))

(defn- admin-uri
  [endpoint]
  (str "dtlv://" c/default-username ":" c/default-password "@" endpoint))

(defn- db-uri
  [endpoint db-name]
  (str (admin-uri endpoint) "/" db-name))

(defn- hook-config
  [script-path log-path exit-code retries]
  {:cmd [script-path log-path (str exit-code)]
   :timeout-ms 1000
   :retries retries
   :retry-delay-ms 0})

(defn- clock-skew-hook-config
  [script-path state-dir]
  {:cmd [script-path state-dir]
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

(defn- parse-log-line
  [line]
  (let [parts (s/split line #"," 8)
        [timestamp-ms db-name fence-op-id observed-term candidate-term new-node-id
         old-node-id old-leader-endpoint]
        (if (= 6 (count parts))
          (let [[ts op observed candidate new-id old-id] parts]
            [ts nil op observed candidate new-id old-id nil])
          (into (vec parts)
                (repeat (max 0 (- 8 (count parts))) nil)))]
    {:timestamp-ms timestamp-ms
     :db-name db-name
     :fence-op-id fence-op-id
     :observed-term observed-term
     :candidate-term candidate-term
     :new-node-id new-node-id
     :old-node-id old-node-id
     :old-leader-endpoint old-leader-endpoint}))

(defn- read-fence-log
  [path]
  (let [f (io/file path)]
    (if (.exists f)
      (->> (line-seq (io/reader f))
           (remove s/blank?)
           (mapv parse-log-line))
      [])))

(defn- repeated-fence-op-ids
  [path]
  (->> (read-fence-log path)
       (keep :fence-op-id)
       frequencies
       (keep (fn [[op-id n]]
               (when (> n 1)
                 [op-id n])))
       (into {})))

(defn- fence-log-entries-by-node
  [ctx node-ids]
  (into {}
        (map (fn [node-id]
               [node-id
                (read-fence-log (get-in ctx [:nodes (dec node-id) :fence-log]))]))
        node-ids))

(defn- wait-for-fence-entry!
  [ctx-atom expected-new-node-id]
  (let [timeout-ms 10000
        deadline (+ (now-ms) timeout-ms)
        expected-node-id (str expected-new-node-id)]
    (loop [last-entries nil]
      (let [ctx @ctx-atom
            entries-by-node (fence-log-entries-by-node ctx
                                                       (map :node-id
                                                            (:nodes ctx)))
            matching-entries (->> entries-by-node
                                  vals
                                  (apply concat)
                                  (filter #(= expected-node-id
                                              (:new-node-id %)))
                                  vec)]
        (if (seq matching-entries)
          {:entries-by-node entries-by-node
           :matching-entries matching-entries}
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 200)
              (recur entries-by-node))
            (throw (ex-info "Timed out waiting for fencing hook verification entry"
                            {:expected-new-node-id expected-new-node-id
                             :timeout-ms timeout-ms
                             :last-entries last-entries}))))))))

(defn- verify-fencing-hook-entry
  [ctx initial-leader-id initial-endpoint new-leader-id entry]
  (let [db-name (:db-name ctx)
        observed-term (parse-long (or (:observed-term entry) ""))
        candidate-term (parse-long (or (:candidate-term entry) ""))]
    (when-not (= db-name (:db-name entry))
      (throw (ex-info "Fencing hook entry recorded unexpected DB name"
                      {:expected-db-name db-name
                       :entry entry})))
    (when-not (= (str initial-leader-id) (:old-node-id entry))
      (throw (ex-info "Fencing hook entry recorded unexpected old leader node"
                      {:expected-old-node-id initial-leader-id
                       :entry entry})))
    (when-not (= initial-endpoint (:old-leader-endpoint entry))
      (throw (ex-info "Fencing hook entry recorded unexpected old leader endpoint"
                      {:expected-old-leader-endpoint initial-endpoint
                       :entry entry})))
    (when-not (= (str new-leader-id) (:new-node-id entry))
      (throw (ex-info "Fencing hook entry recorded unexpected new leader node"
                      {:expected-new-node-id new-leader-id
                       :entry entry})))
    (when-not (and (integer? observed-term)
                   (integer? candidate-term)
                   (= (unchecked-inc (long observed-term))
                      (long candidate-term)))
      (throw (ex-info "Fencing hook entry recorded unexpected leader term transition"
                      {:entry entry})))
    (let [expected-fence-op-id (str db-name ":" observed-term ":" new-leader-id)]
      (when-not (= expected-fence-op-id (:fence-op-id entry))
        (throw (ex-info "Fencing hook entry recorded unexpected fence op id"
                        {:expected-fence-op-id expected-fence-op-id
                         :entry entry})))
      (assoc entry :expected-fence-op-id expected-fence-op-id))))

(defn- delete-file-if-exists!
  [path]
  (let [f (io/file path)]
    (when (.exists f)
      (io/delete-file f))))

(defn- await-value
  [label timeout-ms interval-ms f]
  (let [deadline (+ (now-ms) timeout-ms)]
    (loop []
      (if-let [value (f)]
        value
        (if (< (now-ms) deadline)
          (do
            (Thread/sleep interval-ms)
            (recur))
          (throw (ex-info (str "Timed out waiting for " label)
                          {:label label
                           :timeout-ms timeout-ms})))))))

(defn- safe-disconnect!
  [client]
  (when client
    (try
      (cl/disconnect client)
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
  (when (and path (.exists (io/file path)))
    (try
      (u/delete-files path)
      (catch Throwable _ nil))))

(defn- make-nodes
  [work-dir port-base]
  (mapv
    (fn [idx]
      (let [node-id   (inc idx)
            port      (+ port-base idx)
            peer-port (+ port-base 100 idx)]
        {:node-id   node-id
         :port      port
         :endpoint  (str "127.0.0.1:" port)
         :peer-port peer-port
         :peer-id   (str "127.0.0.1:" peer-port)
         :root      (str work-dir u/+separator+ "node-" node-id)
         :fence-log (str work-dir u/+separator+ "fence-node-" node-id ".log")}))
    (range 3)))

(defn- control-node
  [ctx node-id]
  (or (some #(when (= node-id (:node-id %)) %) (:nodes ctx))
      (throw (ex-info "Missing control node" {:node-id node-id}))))

(defn- promotable-voters
  [nodes]
  (mapv (fn [{:keys [node-id peer-id]}]
          {:peer-id peer-id
           :ha-node-id node-id
           :promotable? true})
        nodes))

(defn- authority-leader?
  [authority]
  (when-let [^Node node @(:node-v authority)]
    (.isLeader node)))

(defn- authority-leader-node-ids
  [authorities node-ids]
  (->> node-ids
       (keep (fn [node-id]
               (when-let [authority (get authorities node-id)]
                 (when (authority-leader? authority)
                   node-id))))
       vec))

(defn- wait-for-single-authority-leader!
  [authorities node-ids timeout-ms]
  (let [deadline (+ (now-ms) timeout-ms)]
    (loop [last-leaders nil]
      (let [leaders (authority-leader-node-ids authorities node-ids)]
        (cond
          (= 1 (count leaders))
          (first leaders)

          (> (count leaders) 1)
          (throw (ex-info "Multiple control-plane leaders detected"
                          {:leaders leaders}))

          (< (now-ms) deadline)
          (do
            (Thread/sleep 50)
            (recur leaders))

          :else
          (throw (ex-info "Timed out waiting for control-plane leader"
                          {:label "control leader"
                           :timeout-ms timeout-ms
                           :leaders leaders
                           :previous-leaders last-leaders})))))))

(defn- require-sofa-jraft!
  [ctx scenario]
  (when (not= :sofa-jraft (:control-backend ctx))
    (throw
      (ex-info "This drill requires --control-backend sofa-jraft"
               {:scenario scenario
                :control-backend (:control-backend ctx)}))))

(defn- authority-voters
  [ctx promotable-node-ids witness-node-ids]
  (->> (:nodes ctx)
       (keep (fn [{:keys [node-id peer-id]}]
               (cond
                 (contains? promotable-node-ids node-id)
                 {:peer-id peer-id
                  :ha-node-id node-id
                  :promotable? true}

                 (contains? witness-node-ids node-id)
                 {:peer-id peer-id
                  :promotable? false}

                 :else nil)))
       vec))

(defn- authority-raft-dir
  [ctx node-id]
  (str (:work-dir ctx)
       u/+separator+
       "control-authority-"
       node-id))

(defn- start-control-authority!
  [ctx node-id voters]
  (let [{:keys [peer-id]} (control-node ctx node-id)
        raft-dir (authority-raft-dir ctx node-id)]
    (u/create-dirs raft-dir)
    (doto
      (ctrl/new-sofa-jraft-authority
        {:group-id (:group-id ctx)
         :local-peer-id peer-id
         :voters voters
         :rpc-timeout-ms 600
         :election-timeout-ms 1200
         :operation-timeout-ms 12000
         :raft-dir raft-dir})
      (ctrl/start-authority!))))

(defn- setup-control-authorities!
  [ctx-atom node-ids voters]
  (doseq [node-id node-ids]
    (swap! ctx-atom assoc-in [:authorities node-id]
           (start-control-authority! @ctx-atom node-id voters)))
  @ctx-atom)

(defn- stop-control-authority!
  [ctx node-id]
  (safe-stop-authority! (get-in ctx [:authorities node-id]))
  (assoc-in ctx [:authorities node-id] nil))

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

(defn- node-ha-opts
  [base-opts node script-path exit-code retries]
  (-> base-opts
      (assoc :ha-node-id (:node-id node)
             :ha-fencing-hook
             (hook-config script-path (:fence-log node) exit-code retries))
      (assoc-in [:ha-control-plane :local-peer-id] (:peer-id node))))

(defn- start-server!
  [node]
  (.mkdirs (io/file (:root node)))
  (let [server (binding [c/*db-background-sampling?* false]
                 (srv/create {:port (:port node)
                              :root (:root node)}))]
    (binding [c/*db-background-sampling?* false]
      (srv/start server))
    server))

(defn- open-remote-conn!
  [node db-name opts]
  (d/create-conn (db-uri (:endpoint node) db-name)
                 drill-schema
                 (assoc opts :client-opts conn-client-opts)))

(defn- probe-write-admission
  [server db-name]
  (let [db-state (when server
                   (get (.-dbs ^Server server) db-name))
        ha-meta  (when db-state
                   (select-keys db-state
                                [:ha-role
                                 :ha-authority-owner-node-id
                                 :ha-authority-read-ok?
                                 :ha-clock-skew-paused?
                                 :ha-follower-degraded?
                                 :ha-promotion-last-failure
                                 :ha-promotion-failure-details
                                 :ha-follower-last-error
                                 :ha-follower-last-error-details]))]
    (cond
      (nil? server)
      {:status :down}

      (nil? db-state)
      {:status :missing-db-state}

      (nil? (:ha-authority db-state))
      (merge ha-meta {:status :ha-runtime-missing})

      :else
      (try
        (merge
          ha-meta
          (if-let [err (#'srv/ha-write-admission-error
                        server
                        {:type :open-dbi :args [db-name probe-dbi nil]})]
            {:status :rejected
             :reason (:reason err)
             :error (:error err)}
            {:status :leader}))
        (catch Throwable e
          (merge ha-meta
                 {:status :down
                  :message (ex-message e)}))))))

(defn- probe-snapshot
  [servers db-name live-node-ids]
  (into {}
        (map (fn [node-id]
               [node-id (probe-write-admission (get servers node-id) db-name)]))
        live-node-ids))

(defn- wait-for-single-leader!
  [servers db-name live-node-ids timeout-ms]
  (let [deadline (+ (now-ms) timeout-ms)]
    (loop [last-snapshot nil]
      (let [snapshot (probe-snapshot servers db-name live-node-ids)
            leaders  (->> snapshot
                          (keep (fn [[node-id {:keys [status]}]]
                                  (when (= :leader status) node-id)))
                          vec)]
        (cond
          (= 1 (count leaders))
          {:leader-id (first leaders)
           :snapshot snapshot}

          (> (count leaders) 1)
          (throw (ex-info "Multiple leaders detected"
                          {:snapshot snapshot}))

          (< (now-ms) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info "Timed out waiting for single leader"
                          {:label "single leader"
                           :timeout-ms timeout-ms
                           :probe-snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn- local-db-state
  [ctx node-id]
  (let [^Server server (get-in ctx [:servers node-id])]
    (when server
      (get (.-dbs server) (:db-name ctx)))))

(defn- replication-status-by-node
  [ctx live-node-ids]
  (into {}
        (map (fn [node-id]
               [node-id
                (try
                  (if-let [db-state (local-db-state ctx node-id)]
                    (let [store (:store db-state)
                          store-opts (when (instance? Store store)
                                       (i/opts store))
                          lmdb  (if (instance? Store store)
                                  (.-lmdb ^Store store)
                                  store)
                          watermarks (when lmdb
                                       (kv/txlog-watermarks lmdb))
                          txlog-lsn  (long (or (:last-applied-lsn watermarks)
                                               0))
                          effective-lsn
                          (long (max txlog-lsn
                                     (long (or (:ha-local-last-applied-lsn
                                                db-state)
                                               0))
                                     (long (or (dha/read-ha-local-last-applied-lsn
                                                db-state)
                                               0))))]
                      {:ok? true
                       :last-applied-lsn effective-lsn
                       :txlog-last-applied-lsn txlog-lsn
                       :ha-role (:ha-role db-state)
                       :ha-authority-term (:ha-authority-term db-state)
                       :ha-authority-owner-node-id
                       (:ha-authority-owner-node-id db-state)
                       :ha-local-last-applied-lsn
                       (:ha-local-last-applied-lsn db-state)
                       :ha-follower-next-lsn (:ha-follower-next-lsn db-state)
                       :ha-follower-degraded? (:ha-follower-degraded? db-state)
                       :ha-follower-last-error
                       (:ha-follower-last-error db-state)
                       :ha-follower-last-error-details
                       (:ha-follower-last-error-details db-state)
                       :ha-runtime-opts
                       (some-> (:ha-runtime-opts db-state)
                               (select-keys [:ha-mode
                                             :db-identity
                                             :ha-node-id
                                             :ha-members
                                             :ha-control-plane]))
                       :store-ha-opts
                       (some-> store-opts
                               (select-keys [:ha-mode
                                             :db-identity
                                             :ha-node-id
                                             :ha-members
                                             :ha-control-plane]))})
                    {:ok? false
                     :error :missing-db-state})
                  (catch Throwable e
                    {:ok? false
                     :error :probe-failed
                     :message (ex-message e)}))]))
        live-node-ids))

(defn- wait-for-replication!
  [ctx live-node-ids target-lsn]
  (let [timeout-ms 30000
        deadline   (+ (now-ms) timeout-ms)]
    (loop [last-status nil]
      (let [status (replication-status-by-node ctx live-node-ids)]
        (if (every? (fn [[_ {:keys [ok? last-applied-lsn]}]]
                      (and ok?
                           (>= (long (or last-applied-lsn 0))
                               target-lsn)))
                    status)
          true
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur status))
            (throw (ex-info (str "Timed out waiting for replication to lsn "
                                 target-lsn)
                            {:label "replication"
                             :timeout-ms timeout-ms
                             :target-lsn target-lsn
                             :replication-status status
                             :previous-status last-status}))))))))

(defn- local-watermarks
  [ctx node-id]
  (let [store (:store (local-db-state ctx node-id))
        lmdb  (if (instance? Store store)
                (.-lmdb ^Store store)
                store)]
    (kv/txlog-watermarks lmdb)))

(defn- local-ha-persisted-lsn
  [ctx node-id]
  (when-let [db-state (local-db-state ctx node-id)]
    (let [store (:store db-state)
          lmdb  (if (instance? Store store)
                  (.-lmdb ^Store store)
                  store)]
      (long (or (try
                  (i/get-value lmdb c/kv-info c/ha-local-applied-lsn
                               :keyword :data)
                  (catch Throwable _
                    nil))
                0)))))

(defn- effective-local-lsn
  [ctx node-id]
  (if-let [db-state (local-db-state ctx node-id)]
    (let [txlog-lsn      (long (or (:last-applied-lsn
                                    (local-watermarks ctx node-id))
                                   0))
          runtime-lsn    (long (or (:ha-local-last-applied-lsn db-state) 0))
          persisted-lsn  (long (or (local-ha-persisted-lsn ctx node-id) 0))
          comparable-lsn (long (max runtime-lsn persisted-lsn))]
      (if (= :leader (:ha-role db-state))
        (long (max txlog-lsn comparable-lsn))
        comparable-lsn))
    0))

(defn- drifted-ha-members
  [members target-node-id]
  (mapv (fn [member]
          (if (= target-node-id (:node-id member))
            (assoc member :endpoint
                   (str "127.0.0.1:" (+ 29000 target-node-id)))
            member))
        members))

(defn- watermarks-by-node
  [ctx live-node-ids]
  (into {}
        (map (fn [node-id]
               [node-id
                (try
                  (local-watermarks ctx node-id)
                  (catch Throwable e
                    {:error (ex-message e)}))]))
        live-node-ids))

(defn- write-value!
  [conns leader-id key value]
  (d/transact! (get conns leader-id)
               [{:drill/key key
                 :drill/value value}]))

(defn- setup-cluster!
  ([ctx-atom success-hook-exit]
   (setup-cluster! ctx-atom success-hook-exit {}))
  ([ctx-atom success-hook-exit {:keys [data-nodes base-opts]}]
   (let [{:keys [nodes db-name fence-script group-id db-identity
                 control-backend]} @ctx-atom
         data-nodes (or data-nodes nodes)
         base-opts  (or base-opts
                        (base-ha-opts data-nodes
                                      group-id
                                      db-identity
                                      control-backend))]
     (doseq [node nodes]
       (swap! ctx-atom assoc-in [:servers (:node-id node)] nil)
       (swap! ctx-atom assoc-in [:conns (:node-id node)] nil))
     (doseq [node data-nodes]
       (swap! ctx-atom assoc-in [:servers (:node-id node)] (start-server! node)))
     (let [conn-futures
           (into {}
                 (map (fn [node]
                        [(:node-id node)
                         (future
                           (open-remote-conn!
                             node
                             db-name
                             (node-ha-opts base-opts node fence-script
                                           success-hook-exit 0)))]))
                 data-nodes)]
       (doseq [node data-nodes]
         (swap! ctx-atom assoc-in [:conns (:node-id node)]
                @(get conn-futures (:node-id node)))))
     (swap! ctx-atom assoc :live-node-ids (set (map :node-id data-nodes)))
     @ctx-atom)))

(defn- stop-node!
  [ctx node-id]
  (when-let [conn (get-in ctx [:conns node-id])]
    (try
      (d/close conn)
      (catch Throwable _ nil)))
  (safe-stop-server! (get-in ctx [:servers node-id]))
  (-> ctx
      (assoc-in [:conns node-id] nil)
      (assoc-in [:servers node-id] nil)
      (update :live-node-ids disj node-id)))

(defn- restart-node!
  [ctx node-id open-opts]
  (let [node   (control-node ctx node-id)
        server (start-server! node)
        conn   (open-remote-conn! node (:db-name ctx) open-opts)]
    (-> ctx
        (assoc-in [:servers node-id] server)
        (assoc-in [:conns node-id] conn)
        (update :live-node-ids conj node-id))))

(defn- require-store!
  [ctx node-id]
  (or (some-> (get-in ctx [:conns node-id]) deref .-store)
      (throw (ex-info "Missing remote store" {:node-id node-id}))))

(defn- require-conn!
  [ctx node-id]
  (or (get-in ctx [:conns node-id])
      (throw (ex-info "Missing remote connection" {:node-id node-id}))))

(defn- read-value
  [conn key]
  (d/q value-query @conn key))

(defn- fresh-read-value
  [endpoint db-name key]
  (let [conn (d/get-conn (db-uri endpoint db-name))]
    (try
      (read-value conn key)
      (finally
        (d/close conn)))))

(defn- verify-value-on-nodes!
  [ctx live-node-ids key expected]
  (await-value
    (str "value verification for " key)
    15000
    500
    (fn []
      (when
        (every?
          (fn [node-id]
            (= expected
               (fresh-read-value
                 (get-in ctx [:nodes (dec node-id) :endpoint])
                 (:db-name ctx)
                 key)))
          live-node-ids)
        true))))

(defn- follower-node-ids
  [ctx leader-id]
  (->> (:live-node-ids ctx)
       (remove #{leader-id})
       sort
       vec))

(defn- create-snapshot-on-node!
  [ctx node-id]
  (let [result (i/create-snapshot! (require-store! ctx node-id))]
    (when-not (:ok? result)
      (throw (ex-info "Failed to create snapshot"
                      {:node-id node-id
                       :result result})))
    result))

(defn- create-snapshots-on-nodes!
  [ctx node-ids]
  (into {}
        (map (fn [node-id]
               [node-id (create-snapshot-on-node! ctx node-id)]))
        node-ids))

(defn- retention-state-on-node
  [ctx node-id]
  (i/txlog-retention-state (require-store! ctx node-id)))

(defn- gc-txlog-segments-on-node!
  [ctx node-id]
  (let [result (i/gc-txlog-segments! (require-store! ctx node-id))]
    (when-not (:ok? result)
      (throw (ex-info "Failed to GC WAL segments"
                      {:node-id node-id
                       :result result})))
    result))

(defn- write-series-with-rolls!
  [ctx leader-id prefix n sleep-ms]
  (doseq [idx (range n)]
    (write-value! (:conns ctx)
                  leader-id
                  (str prefix "-" idx)
                  (str prefix "-v-" idx))
    (Thread/sleep sleep-ms))
  (let [target-lsn (effective-local-lsn ctx leader-id)]
    (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
    target-lsn))

(defn- follower-bootstrap-state
  [ctx node-id]
  (when-let [db-state (local-db-state ctx node-id)]
    (let [watermarks (try
                       (local-watermarks ctx node-id)
                       (catch Throwable e
                         {:error (ex-message e)}))]
      {:ha-role (:ha-role db-state)
       :ha-local-last-applied-lsn (:ha-local-last-applied-lsn db-state)
       :ha-follower-next-lsn (:ha-follower-next-lsn db-state)
       :ha-follower-last-bootstrap-ms (:ha-follower-last-bootstrap-ms db-state)
       :ha-follower-bootstrap-source-endpoint
       (:ha-follower-bootstrap-source-endpoint db-state)
       :ha-follower-bootstrap-snapshot-last-applied-lsn
       (:ha-follower-bootstrap-snapshot-last-applied-lsn db-state)
       :ha-follower-source-endpoint (:ha-follower-source-endpoint db-state)
       :ha-follower-degraded? (:ha-follower-degraded? db-state)
       :ha-follower-last-error (:ha-follower-last-error db-state)
       :ha-follower-last-error-details (:ha-follower-last-error-details db-state)
       :watermarks watermarks})))

(defn- follower-degraded-state
  [ctx node-id]
  (when-let [db-state (local-db-state ctx node-id)]
    {:ha-role (:ha-role db-state)
     :ha-authority-owner-node-id (:ha-authority-owner-node-id db-state)
     :ha-authority-term (:ha-authority-term db-state)
     :ha-local-last-applied-lsn (:ha-local-last-applied-lsn db-state)
     :ha-follower-next-lsn (:ha-follower-next-lsn db-state)
     :ha-follower-last-bootstrap-ms (:ha-follower-last-bootstrap-ms db-state)
     :ha-follower-bootstrap-source-endpoint
     (:ha-follower-bootstrap-source-endpoint db-state)
     :ha-follower-bootstrap-snapshot-last-applied-lsn
     (:ha-follower-bootstrap-snapshot-last-applied-lsn db-state)
     :ha-follower-degraded? (:ha-follower-degraded? db-state)
     :ha-follower-degraded-reason (:ha-follower-degraded-reason db-state)
     :ha-follower-last-error (:ha-follower-last-error db-state)
     :ha-follower-last-error-details (:ha-follower-last-error-details db-state)
     :ha-promotion-last-failure (:ha-promotion-last-failure db-state)
     :ha-promotion-failure-details (:ha-promotion-failure-details db-state)
     :last-applied-lsn (effective-local-lsn ctx node-id)}))

(defn- clock-skew-state
  [ctx node-id]
  (when-let [db-state (local-db-state ctx node-id)]
    {:ha-role (:ha-role db-state)
     :ha-authority-owner-node-id (:ha-authority-owner-node-id db-state)
     :ha-authority-term (:ha-authority-term db-state)
     :ha-clock-skew-paused? (:ha-clock-skew-paused? db-state)
     :ha-clock-skew-last-observed-ms (:ha-clock-skew-last-observed-ms db-state)
     :ha-clock-skew-last-result (:ha-clock-skew-last-result db-state)
     :ha-promotion-last-failure (:ha-promotion-last-failure db-state)
     :ha-promotion-failure-details (:ha-promotion-failure-details db-state)}))

(defn- wait-for-follower-bootstrap!
  [ctx-atom node-id min-snapshot-lsn]
  (let [timeout-ms 30000
        deadline   (+ (now-ms) timeout-ms)]
    (loop [last-state nil]
      (let [state        (follower-bootstrap-state @ctx-atom node-id)
            applied-lsn  (long (or (:ha-local-last-applied-lsn state) 0))
            snapshot-lsn (long (or (:ha-follower-bootstrap-snapshot-last-applied-lsn
                                    state)
                                   0))]
        (if (and state
                 (integer? (:ha-follower-last-bootstrap-ms state))
                 (string? (:ha-follower-bootstrap-source-endpoint state))
                 (>= applied-lsn snapshot-lsn)
                 (>= snapshot-lsn (long min-snapshot-lsn))
                 (not (:ha-follower-degraded? state))
                 (nil? (:ha-follower-last-error state)))
          state
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or state last-state)))
            (throw (ex-info (str "Timed out waiting for snapshot bootstrap on follower "
                                 node-id)
                            {:label "snapshot bootstrap"
                             :node-id node-id
                             :timeout-ms timeout-ms
                             :last-state last-state}))))))))

(defn- wait-for-follower-degraded!
  [ctx-atom node-id]
  (let [timeout-ms 30000
        deadline   (+ (now-ms) timeout-ms)]
    (loop [last-state nil]
      (let [state (follower-degraded-state @ctx-atom node-id)]
        (if (and state
                 (= :follower (:ha-role state))
                 (true? (:ha-follower-degraded? state))
                 (= :wal-gap (:ha-follower-degraded-reason state))
                 (= :sync-failed (:ha-follower-last-error state)))
          state
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or state last-state)))
            (throw (ex-info (str "Timed out waiting for follower degraded mode on node "
                                 node-id)
                            {:label "follower degraded"
                             :node-id node-id
                             :timeout-ms timeout-ms
                             :last-state last-state}))))))))

(defn- wait-for-follower-stays-degraded!
  [ctx-atom node-id leader-id]
  (let [timeout-ms 30000
        deadline   (+ (now-ms) timeout-ms)]
    (loop [last-state nil]
      (let [state (follower-degraded-state @ctx-atom node-id)]
        (if (and state
                 (= :follower (:ha-role state))
                 (= leader-id (:ha-authority-owner-node-id state))
                 (integer? (:ha-authority-term state))
                 (pos? (long (:ha-authority-term state)))
                 (true? (:ha-follower-degraded? state))
                 (= :wal-gap (:ha-follower-degraded-reason state)))
          state
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or state last-state)))
            (throw (ex-info (str "Timed out waiting for degraded follower state to persist on node "
                                 node-id)
                            {:label "degraded follower persists"
                             :node-id node-id
                             :leader-id leader-id
                             :timeout-ms timeout-ms
                             :last-state last-state}))))))))

(defn- wait-for-clock-skew-block!
  [ctx-atom paused-node-ids]
  (let [timeout-ms 15000
        deadline   (+ (now-ms) timeout-ms)]
    (loop [last-data nil]
      (let [ctx      @ctx-atom
            snapshot (probe-snapshot (:servers ctx)
                                     (:db-name ctx)
                                     (:live-node-ids ctx))
            leaders  (->> snapshot
                          (keep (fn [[node-id {:keys [status]}]]
                                  (when (= :leader status) node-id)))
                          vec)
            states   (into {}
                           (map (fn [node-id]
                                  [node-id (clock-skew-state ctx node-id)]))
                           paused-node-ids)]
        (if (and (empty? leaders)
                 (every? (fn [[_ state]]
                           (and state
                                (= :follower (:ha-role state))
                                (true? (:ha-clock-skew-paused? state))
                                (= :clock-skew-paused
                                   (:ha-promotion-last-failure state))
                                (= :clock-skew-budget-breached
                                   (get-in state
                                           [:ha-promotion-failure-details
                                            :reason]))))
                         states))
          {:probe-snapshot snapshot
           :paused-states states}
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur {:probe-snapshot snapshot
                      :paused-states states}))
            (throw (ex-info "Timed out waiting for clock-skew pause to block failover"
                            {:label "clock-skew pause"
                             :node-ids paused-node-ids
                             :timeout-ms timeout-ms
                             :last-data last-data}))))))))

(defn- wait-for-clock-skew-clear!
  [ctx-atom node-id leader-id role]
  (let [timeout-ms 15000
        deadline   (+ (now-ms) timeout-ms)]
    (loop [last-state nil]
      (let [state (clock-skew-state @ctx-atom node-id)]
        (if (and state
                 (= role (:ha-role state))
                 (= leader-id (:ha-authority-owner-node-id state))
                 (integer? (:ha-authority-term state))
                 (pos? (long (:ha-authority-term state)))
                 (false? (:ha-clock-skew-paused? state))
                 (= :clock-skew-within-budget
                    (get-in state [:ha-clock-skew-last-result :reason])))
          state
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or state last-state)))
            (throw (ex-info "Timed out waiting for clock-skew pause to clear"
                            {:label "clock-skew clear"
                             :node-id node-id
                             :leader-id leader-id
                             :role role
                             :timeout-ms timeout-ms
                             :last-state last-state}))))))))

(defn- wait-for-follower-rejoin!
  [ctx-atom node-id leader-id min-lsn]
  (let [timeout-ms 30000
        deadline   (+ (now-ms) timeout-ms)]
    (loop [last-status nil]
      (let [status (get (replication-status-by-node @ctx-atom [node-id])
                        node-id)]
        (if (and (:ok? status)
                 (= :follower (:ha-role status))
                 (= leader-id (:ha-authority-owner-node-id status))
                 (integer? (:ha-authority-term status))
                 (pos? (long (:ha-authority-term status)))
                 (>= (long (or (:last-applied-lsn status) 0))
                     (long min-lsn))
                 (not (:ha-follower-degraded? status))
                 (nil? (:ha-follower-last-error status)))
          status
          (if (< (now-ms) deadline)
            (do
              (Thread/sleep 250)
              (recur (or status last-status)))
            (throw (ex-info (str "Timed out waiting for follower-only rejoin on node "
                                 node-id)
                            {:label "follower-only rejoin"
                             :node-id node-id
                             :leader-id leader-id
                             :min-lsn min-lsn
                             :timeout-ms timeout-ms
                             :last-status last-status}))))))))

(defn- wait-for-replica-floor!
  [ctx-atom leader-id follower-id min-lsn]
  (await-value
    (str "replica floor on leader " leader-id " for follower " follower-id)
    10000
    250
    (fn []
      (let [state    (retention-state-on-node @ctx-atom leader-id)
            replicas (get-in state [:floor-providers :replica :replicas])
            replica  (some #(when (= follower-id (:replica-id %)) %) replicas)]
        (when (and replica
                   (not (:stale? replica))
                   (>= (long (or (:floor-lsn replica) 0))
                       (long min-lsn)))
          {:replica replica
           :retention-state state})))))

(defn- run-failover!
  [ctx-atom]
  (step! "Starting local 3-node HA cluster for failover drill")
  (let [ctx      (setup-cluster! ctx-atom 0)
        {:keys [leader-id]} (wait-for-single-leader! (:servers ctx)
                                                     (:db-name ctx)
                                                     (:live-node-ids ctx)
                                                     20000)
        leader-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])]
    (step! "Initial leader:" leader-id leader-endpoint)
    (write-value! (:conns ctx) leader-id "seed" "v1")
    (let [target-lsn (effective-local-lsn ctx leader-id)]
      (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
      (verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1"))
    (step! "Seed write replicated across all three nodes")
    (swap! ctx-atom stop-node! leader-id)
    (let [ctx @ctx-atom
          {:keys [leader-id snapshot]} (wait-for-single-leader!
                                         (:servers ctx)
                                         (:db-name ctx)
                                         (:live-node-ids ctx)
                                         20000)
          new-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])]
      (step! "Post-failover leader:" leader-id new-endpoint)
      (write-value! (:conns ctx) leader-id "post-failover" "v2")
      (let [target-lsn (effective-local-lsn ctx leader-id)]
        (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
        (verify-value-on-nodes! ctx (:live-node-ids ctx)
                                "post-failover" "v2"))
      {:ctx ctx
       :result {:scenario :failover
                :initial-leader-endpoint leader-endpoint
                :new-leader-endpoint new-endpoint
                :probe-snapshot snapshot
                :watermarks (watermarks-by-node ctx
                                                (:live-node-ids ctx))}})))

(defn- run-follower-rejoin!
  [ctx-atom]
  (step! "Starting local 3-node HA cluster for follower-rejoin drill")
  (let [base-opts          (base-ha-opts (:nodes @ctx-atom)
                                         (:group-id @ctx-atom)
                                         (:db-identity @ctx-atom)
                                         (:control-backend @ctx-atom))
        ctx                (setup-cluster! ctx-atom 0 {:base-opts base-opts})
        {:keys [leader-id]} (wait-for-single-leader! (:servers ctx)
                                                     (:db-name ctx)
                                                     (:live-node-ids ctx)
                                                     20000)
        initial-leader-id  leader-id
        leader-endpoint    (get-in ctx [:nodes (dec leader-id) :endpoint])]
    (step! "Initial leader:" initial-leader-id leader-endpoint)
    (write-value! (:conns ctx) initial-leader-id "seed" "v1")
    (let [target-lsn (effective-local-lsn ctx initial-leader-id)]
      (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
      (verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1"))
    (step! "Stopping leader for follower-only rejoin:" initial-leader-id)
    (swap! ctx-atom stop-node! initial-leader-id)
    (let [ctx                  @ctx-atom
          {:keys [leader-id]}  (wait-for-single-leader! (:servers ctx)
                                                        (:db-name ctx)
                                                        (:live-node-ids ctx)
                                                        20000)
          failover-leader-id   leader-id
          failover-endpoint    (get-in ctx [:nodes (dec leader-id) :endpoint])]
      (step! "Failover leader:" failover-leader-id failover-endpoint)
      (write-value! (:conns ctx) failover-leader-id "post-failover" "v2")
      (let [target-lsn (effective-local-lsn ctx failover-leader-id)]
        (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
        (verify-value-on-nodes! ctx (:live-node-ids ctx)
                                "post-failover"
                                "v2"))
      (swap! ctx-atom restart-node!
             initial-leader-id
             (node-ha-opts base-opts
                           (control-node @ctx-atom initial-leader-id)
                           (:fence-script @ctx-atom)
                           0
                           0))
      (step! "Restarted node for follower-only rejoin:" initial-leader-id)
      (let [ctx                   @ctx-atom
            {:keys [leader-id]}   (wait-for-single-leader! (:servers ctx)
                                                           (:db-name ctx)
                                                           (:live-node-ids ctx)
                                                           20000)
            current-leader-id     leader-id
            current-leader-endpoint
            (get-in ctx [:nodes (dec leader-id) :endpoint])
            catch-up-lsn          (effective-local-lsn ctx
                                                       current-leader-id)
            rejoin-state          (wait-for-follower-rejoin! ctx-atom
                                                             initial-leader-id
                                                             current-leader-id
                                                             catch-up-lsn)]
        (step! "Rejoined node is following leader:" current-leader-id
               current-leader-endpoint
               "term:" (:ha-authority-term rejoin-state))
        (write-value! (:conns ctx) current-leader-id "post-rejoin" "v3")
        (let [target-lsn (effective-local-lsn ctx current-leader-id)
              replica-floor
              (wait-for-replica-floor! ctx-atom
                                       current-leader-id
                                       initial-leader-id
                                       target-lsn)]
          (wait-for-replication! @ctx-atom
                                 (:live-node-ids @ctx-atom)
                                 target-lsn)
          (verify-value-on-nodes! @ctx-atom
                                  (:live-node-ids @ctx-atom)
                                  "post-rejoin"
                                  "v3")
          {:ctx @ctx-atom
           :result {:scenario :follower-rejoin
                    :initial-leader-id initial-leader-id
                    :initial-leader-endpoint leader-endpoint
                    :failover-leader-id failover-leader-id
                    :failover-leader-endpoint failover-endpoint
                    :current-leader-id current-leader-id
                    :current-leader-endpoint current-leader-endpoint
                    :rejoined-node-id initial-leader-id
                    :rejoin-state rejoin-state
                    :replica-floor replica-floor
                    :watermarks (watermarks-by-node @ctx-atom
                                                    (:live-node-ids
                                                     @ctx-atom))}})))))

(defn- run-membership-hash-drift!
  [ctx-atom]
  (step! "Starting local 3-node HA cluster for membership-hash-drift drill")
  (let [ctx      (setup-cluster! ctx-atom 0)
        {:keys [leader-id]} (wait-for-single-leader! (:servers ctx)
                                                     (:db-name ctx)
                                                     (:live-node-ids ctx)
                                                     20000)
        drifted-node-id leader-id
        leader-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])
        leader-store    (require-store! ctx leader-id)
        original-members (:ha-members (local-db-state ctx leader-id))
        drifted-members  (drifted-ha-members original-members drifted-node-id)]
    (step! "Initial leader:" drifted-node-id leader-endpoint)
    (write-value! (:conns ctx) drifted-node-id "seed" "v1")
    (let [target-lsn (effective-local-lsn ctx drifted-node-id)]
      (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
      (verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1"))
    (let [drift-error
          (try
            (i/assoc-opt leader-store :ha-members drifted-members)
            nil
            (catch Exception e
              {:message (ex-message e)
               :data    (ex-data e)}))]
      (when-not drift-error
        (throw (ex-info "Membership drift update unexpectedly succeeded"
                        {:drifted-node-id drifted-node-id
                         :original-members original-members
                         :drifted-members drifted-members})))
      (step! "Rejected membership-drift update on leader:" drifted-node-id)
      (i/assoc-opt leader-store :ha-members original-members)
      (step! "Restored authoritative members on node:" drifted-node-id)
      (let [ctx @ctx-atom
            {:keys [leader-id snapshot]} (wait-for-single-leader! (:servers ctx)
                                                                  (:db-name ctx)
                                                                  (:live-node-ids ctx)
                                                                  20000)
            recovered-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])]
        (step! "Recovered leader after membership reconciliation:"
               leader-id recovered-endpoint)
        (write-value! (:conns ctx) leader-id "post-reconcile" "v2")
        (let [target-lsn (effective-local-lsn ctx leader-id)]
          (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
          (verify-value-on-nodes! ctx (:live-node-ids ctx)
                                  "post-reconcile" "v2"))
        {:ctx ctx
         :result {:scenario :membership-hash-drift
                  :drifted-node-id drifted-node-id
                  :drift-error drift-error
                  :recovered-leader-id leader-id
                  :recovered-leader-endpoint recovered-endpoint
                  :probe-snapshot snapshot
                  :watermarks (watermarks-by-node ctx
                                                  (:live-node-ids ctx))}}))))

(defn- run-fencing-hook-verify!
  [ctx-atom]
  (step! "Starting local 3-node HA cluster for fencing-hook-verify drill")
  (let [ctx                 (setup-cluster! ctx-atom 0)
        {:keys [leader-id]} (wait-for-single-leader! (:servers ctx)
                                                     (:db-name ctx)
                                                     (:live-node-ids ctx)
                                                     20000)
        initial-leader-id   leader-id
        initial-endpoint    (get-in ctx [:nodes (dec leader-id) :endpoint])]
    (step! "Initial leader:" initial-leader-id initial-endpoint)
    (write-value! (:conns ctx) leader-id "seed" "v1")
    (let [target-lsn (effective-local-lsn ctx leader-id)]
      (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
      (verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1"))
    (step! "Stopping leader to verify fencing hook contract:" initial-leader-id)
    (swap! ctx-atom stop-node! initial-leader-id)
    (let [ctx @ctx-atom
          {:keys [leader-id snapshot]}
          (wait-for-single-leader! (:servers ctx)
                                   (:db-name ctx)
                                   (:live-node-ids ctx)
                                   20000)
          new-leader-id leader-id
          new-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])
          fence-result (wait-for-fence-entry! ctx-atom new-leader-id)
          verified-entry (verify-fencing-hook-entry
                          @ctx-atom
                          initial-leader-id
                          initial-endpoint
                          new-leader-id
                          (first (:matching-entries fence-result)))]
      (step! "Verified fencing hook contract on promoted node:"
             new-leader-id
             "fence-op-id:" (:fence-op-id verified-entry))
      (write-value! (:conns ctx) new-leader-id "post-failover" "v2")
      (let [target-lsn (effective-local-lsn ctx new-leader-id)]
        (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
        (verify-value-on-nodes! ctx (:live-node-ids ctx)
                                "post-failover"
                                "v2")
        {:ctx ctx
         :result {:scenario :fencing-hook-verify
                  :initial-leader-id initial-leader-id
                  :initial-leader-endpoint initial-endpoint
                  :new-leader-id new-leader-id
                  :new-leader-endpoint new-endpoint
                  :verified-entry verified-entry
                  :matching-entries (:matching-entries fence-result)
                  :fence-logs (:entries-by-node fence-result)
                  :probe-snapshot snapshot
                  :watermarks (watermarks-by-node ctx
                                                  (:live-node-ids ctx))}}))))

(defn- run-degraded-mode-no-valid-source!
  [ctx-atom]
  (let [segment-max-ms       100
        write-sleep-ms       150
        replica-floor-ttl-ms 500
        writes-per-batch     4
        base-opts            (assoc (base-ha-opts (:nodes @ctx-atom)
                                                  (:group-id @ctx-atom)
                                                  (:db-identity @ctx-atom)
                                                  (:control-backend @ctx-atom))
                               :wal-segment-max-ms segment-max-ms
                               :wal-segment-prealloc? false
                               :wal-segment-prealloc-mode :none
                               :wal-replica-floor-ttl-ms replica-floor-ttl-ms
                               :snapshot-scheduler? false)
        old-config           log/*config*]
    (log/set-min-level! :fatal)
    (try
      (step! "Starting local 3-node HA cluster for degraded-mode-no-valid-source drill")
      (let [ctx                (setup-cluster! ctx-atom 0 {:base-opts base-opts})
            {:keys [leader-id]} (wait-for-single-leader! (:servers ctx)
                                                         (:db-name ctx)
                                                         (:live-node-ids ctx)
                                                         20000)
            initial-leader-id  leader-id
            initial-endpoint   (get-in ctx [:nodes (dec leader-id) :endpoint])
            degraded-node-id   (last (follower-node-ids ctx leader-id))
            source-node-ids    (->> (:live-node-ids ctx)
                                    (remove #{degraded-node-id})
                                    sort
                                    vec)]
        (step! "Initial leader:" initial-leader-id initial-endpoint)
        (write-value! (:conns ctx) leader-id "seed" "v1")
        (let [target-lsn (effective-local-lsn ctx leader-id)]
          (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
          (verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1"))
        (let [baseline-lsn      (write-series-with-rolls! ctx
                                                          leader-id
                                                          "baseline"
                                                          writes-per-batch
                                                          write-sleep-ms)
              _                 (create-snapshots-on-nodes! ctx source-node-ids)
              _                 (step! "Stopping follower to force degraded recovery:"
                                       degraded-node-id)
              _                 (swap! ctx-atom stop-node! degraded-node-id)
              ctx               @ctx-atom
              batch-2-lsn       (write-series-with-rolls! ctx
                                                          leader-id
                                                          "gap-batch-1"
                                                          writes-per-batch
                                                          write-sleep-ms)
              _                 (create-snapshots-on-nodes! ctx source-node-ids)
              batch-3-lsn       (write-series-with-rolls! ctx
                                                          leader-id
                                                          "gap-batch-2"
                                                          writes-per-batch
                                                          write-sleep-ms)
              _                 (create-snapshots-on-nodes! ctx source-node-ids)
              follower-next-lsn (unchecked-inc (long baseline-lsn))
              gc-results        (into {}
                                      (map (fn [node-id]
                                             [node-id
                                              (gc-txlog-segments-on-node!
                                               @ctx-atom node-id)]))
                                      source-node-ids)]
          (doseq [[node-id gc-result] gc-results
                  :let [min-retained-lsn
                        (long (or (get-in gc-result [:after :min-retained-lsn])
                                  0))]]
            (when (zero? (:deleted-count gc-result))
              (throw (ex-info "Degraded-mode drill did not delete any WAL segments on a live source"
                              {:node-id node-id
                               :gc-result gc-result})))
            (when (<= min-retained-lsn (long follower-next-lsn))
              (throw (ex-info "Degraded-mode drill did not advance retained WAL floor beyond the stopped follower"
                              {:node-id node-id
                               :follower-id degraded-node-id
                               :follower-next-lsn follower-next-lsn
                               :gc-result gc-result}))))
          (let [injected-result
                (with-redefs-fn
                  {#'dha/fetch-ha-endpoint-snapshot-copy!
                   (fn [_ _ endpoint _]
                     (throw (ex-info "forced snapshot source failure"
                                     {:error :ha/follower-snapshot-unavailable
                                      :endpoint endpoint})))}
                  (fn []
                    (swap! ctx-atom restart-node!
                           degraded-node-id
                           (node-ha-opts base-opts
                                         (control-node @ctx-atom degraded-node-id)
                                         (:fence-script @ctx-atom)
                                         0
                                         0))
                    (step! "Restarted follower with no valid snapshot source:"
                           degraded-node-id)
                    (let [degraded-state (wait-for-follower-degraded! ctx-atom
                                                                      degraded-node-id)]
                      (step! "Follower entered degraded mode:"
                             degraded-node-id
                             "reason:" (:ha-follower-degraded-reason degraded-state))
                      (let [ctx @ctx-atom
                            _ (write-value! (:conns ctx)
                                            initial-leader-id
                                            "post-gap-live"
                                            "v-live")
                            _ (verify-value-on-nodes! ctx
                                                      source-node-ids
                                                      "post-gap-live"
                                                      "v-live")
                            blocked-state (wait-for-follower-stays-degraded!
                                           ctx-atom
                                           degraded-node-id
                                           initial-leader-id)]
                        (step! "Healthy nodes kept serving writes while follower stayed degraded:"
                               initial-leader-id initial-endpoint)
                        {:post-gap-live-lsn (effective-local-lsn ctx
                                                                 initial-leader-id)
                         :degraded-state degraded-state
                         :blocked-state blocked-state}))))
                recovery-state  (wait-for-follower-bootstrap! ctx-atom
                                                              degraded-node-id
                                                              (:post-gap-live-lsn
                                                               injected-result))
                ctx             @ctx-atom
                {:keys [leader-id]} (wait-for-single-leader! (:servers ctx)
                                                             (:db-name ctx)
                                                             (:live-node-ids ctx)
                                                             20000)]
            (step! "Valid source restored; degraded follower recovered:"
                   degraded-node-id
                   "source:"
                   (:ha-follower-bootstrap-source-endpoint recovery-state))
            (write-value! (:conns ctx) leader-id "post-recovery" "v-final")
            (let [target-lsn (effective-local-lsn ctx leader-id)]
              (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
              (verify-value-on-nodes! ctx (:live-node-ids ctx)
                                      "post-recovery"
                                      "v-final")
              {:ctx ctx
               :result {:scenario :degraded-mode-no-valid-source
                        :initial-leader-id initial-leader-id
                        :initial-leader-endpoint initial-endpoint
                        :degraded-node-id degraded-node-id
                        :follower-next-lsn follower-next-lsn
                        :mid-gap-lsn batch-2-lsn
                        :pre-recovery-leader-lsn batch-3-lsn
                        :gc-results gc-results
                        :degraded-state (:degraded-state injected-result)
                        :blocked-state (:blocked-state injected-result)
                        :recovered-state recovery-state
                        :watermarks (watermarks-by-node ctx
                                                        (:live-node-ids ctx))}}))))
      (finally
        (log/set-config! old-config)))))

(defn- run-clock-skew-pause!
  [ctx-atom]
  (let [clock-skew-dir      (str (:work-dir @ctx-atom)
                                 u/+separator+
                                 "clock-skew")
        base-opts           (assoc (base-ha-opts (:nodes @ctx-atom)
                                                 (:group-id @ctx-atom)
                                                 (:db-identity @ctx-atom)
                                                 (:control-backend @ctx-atom))
                              :ha-clock-skew-budget-ms 100
                              :ha-clock-skew-hook
                              (clock-skew-hook-config
                               (clock-skew-script-path)
                               clock-skew-dir))]
    (step! "Starting local 3-node HA cluster for clock-skew-pause drill")
    (doseq [node-id (map :node-id (:nodes @ctx-atom))]
      (write-clock-skew-ms! clock-skew-dir node-id 0))
    (let [ctx                (setup-cluster! ctx-atom 0 {:base-opts base-opts})
          {:keys [leader-id]} (wait-for-single-leader! (:servers ctx)
                                                       (:db-name ctx)
                                                       (:live-node-ids ctx)
                                                       20000)
          initial-leader-id  leader-id
          initial-endpoint   (get-in ctx [:nodes (dec leader-id) :endpoint])
          follower-ids       (follower-node-ids ctx leader-id)
          resume-node-id     (first follower-ids)
          blocked-node-id    (last follower-ids)]
      (step! "Initial leader:" initial-leader-id initial-endpoint)
      (write-value! (:conns ctx) leader-id "seed" "v1")
      (let [target-lsn (effective-local-lsn ctx leader-id)]
        (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
        (verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1"))
      (doseq [node-id follower-ids]
        (write-clock-skew-ms! clock-skew-dir node-id 250))
      (step! "Configured follower clock skew breach; stopping leader"
             initial-leader-id)
      (swap! ctx-atom stop-node! initial-leader-id)
      (let [blocked-result (wait-for-clock-skew-block! ctx-atom follower-ids)]
        (step! "Clock-skew pause blocked automatic failover on followers:"
               follower-ids)
        (write-clock-skew-ms! clock-skew-dir resume-node-id 25)
        (step! "Cleared skew budget on follower:" resume-node-id)
        (let [ctx                 @ctx-atom
              {:keys [leader-id snapshot]}
              (wait-for-single-leader! (:servers ctx)
                                       (:db-name ctx)
                                       (:live-node-ids ctx)
                                       20000)
              _                   (when (not= leader-id resume-node-id)
                                    (throw
                                      (ex-info "Unexpected leader after clock-skew resume"
                                               {:expected-leader-id resume-node-id
                                                :leader-id leader-id
                                                :probe-snapshot snapshot
                                                :blocked-result blocked-result})))
              leader-state        (wait-for-clock-skew-clear! ctx-atom
                                                              leader-id
                                                              leader-id
                                                              :leader)]
          (step! "Clock-skew cleared; follower promoted:"
                 leader-id
                 (get-in ctx [:nodes (dec leader-id) :endpoint]))
          (write-value! (:conns ctx) leader-id "post-resume" "v2")
          (let [target-lsn (effective-local-lsn ctx leader-id)]
            (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
            (verify-value-on-nodes! ctx (:live-node-ids ctx)
                                    "post-resume"
                                    "v2"))
          (write-clock-skew-ms! clock-skew-dir blocked-node-id 25)
          (step! "Cleared skew budget on remaining follower:" blocked-node-id)
          (let [resumed-follower-state
                (wait-for-clock-skew-clear! ctx-atom
                                            blocked-node-id
                                            leader-id
                                            :follower)
                ctx @ctx-atom]
            (write-value! (:conns ctx) leader-id "post-clear" "v3")
            (let [target-lsn (effective-local-lsn ctx leader-id)]
              (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
              (verify-value-on-nodes! ctx (:live-node-ids ctx)
                                      "post-clear"
                                      "v3")
              {:ctx ctx
               :result {:scenario :clock-skew-pause
                        :initial-leader-id initial-leader-id
                        :initial-leader-endpoint initial-endpoint
                        :paused-node-ids follower-ids
                        :resume-node-id resume-node-id
                        :blocked-node-id blocked-node-id
                        :blocked-result blocked-result
                        :resumed-leader-id leader-id
                        :resumed-leader-endpoint
                        (get-in ctx [:nodes (dec leader-id) :endpoint])
                        :leader-state leader-state
                        :resumed-follower-state resumed-follower-state
                        :probe-snapshot snapshot
                        :watermarks (watermarks-by-node ctx
                                                        (:live-node-ids ctx))}})))))))

(defn- run-wal-gap!
  [ctx-atom]
  (let [segment-max-ms         100
        write-sleep-ms         150
        replica-floor-ttl-ms   500
        writes-per-batch       4
        base-opts              (assoc (base-ha-opts (:nodes @ctx-atom)
                                                    (:group-id @ctx-atom)
                                                    (:db-identity @ctx-atom)
                                                    (:control-backend @ctx-atom))
                                 :wal-segment-max-ms segment-max-ms
                                 :wal-segment-prealloc? false
                                 :wal-segment-prealloc-mode :none
                                 :wal-replica-floor-ttl-ms replica-floor-ttl-ms
                                 :snapshot-scheduler? false)]
    (step! "Starting local 3-node HA cluster for WAL-gap drill")
    (let [ctx                 (setup-cluster! ctx-atom 0 {:base-opts base-opts})
          {:keys [leader-id]} (wait-for-single-leader! (:servers ctx)
                                                       (:db-name ctx)
                                                       (:live-node-ids ctx)
                                                       20000)
          leader-endpoint     (get-in ctx [:nodes (dec leader-id) :endpoint])
          gapped-follower-id  (last (follower-node-ids ctx leader-id))
          source-node-ids     (->> (:live-node-ids ctx)
                                   (remove #{gapped-follower-id})
                                   sort
                                   vec)
          baseline-lsn        (write-series-with-rolls! ctx
                                                        leader-id
                                                        "baseline"
                                                        writes-per-batch
                                                        write-sleep-ms)
          snapshot-1          (create-snapshots-on-nodes! ctx source-node-ids)]
      (step! "Initial leader:" leader-id leader-endpoint)
      (step! "Created baseline snapshot at LSN:"
             (get-in snapshot-1 [leader-id :snapshot :applied-lsn]))
      (step! "Stopping follower to create WAL gap:" gapped-follower-id)
      (swap! ctx-atom stop-node! gapped-follower-id)
      (let [ctx               @ctx-atom
            batch-2-lsn       (write-series-with-rolls! ctx
                                                        leader-id
                                                        "gap-batch-1"
                                                        writes-per-batch
                                                        write-sleep-ms)
            snapshot-2        (create-snapshots-on-nodes! ctx source-node-ids)
            batch-3-lsn       (write-series-with-rolls! ctx
                                                        leader-id
                                                        "gap-batch-2"
                                                        writes-per-batch
                                                        write-sleep-ms)
            snapshot-3        (create-snapshots-on-nodes! ctx source-node-ids)
            follower-next-lsn (unchecked-inc (long baseline-lsn))
            retention-before  (await-value
                                "replica floor expiry before WAL GC"
                                10000
                                200
                                (fn []
                                  (let [state (retention-state-on-node
                                               @ctx-atom leader-id)]
                                    (when (and (= 1 (long (or (get-in state
                                                                       [:floor-providers
                                                                        :replica
                                                                        :active-count])
                                                              0)))
                                               (> (long (or
                                                         (:required-retained-floor-lsn
                                                          state)
                                                         0))
                                                  (long follower-next-lsn)))
                                      state))))
            gc-results        (into {}
                                    (map (fn [node-id]
                                           [node-id
                                            (gc-txlog-segments-on-node!
                                             @ctx-atom node-id)]))
                                    source-node-ids)]
        (doseq [[node-id gc-result] gc-results
                :let [min-retained-lsn
                      (long (or (get-in gc-result [:after :min-retained-lsn])
                                0))]]
          (when (zero? (:deleted-count gc-result))
            (throw (ex-info "WAL-gap drill did not delete any WAL segments on a live source"
                            {:node-id node-id
                             :gc-result gc-result})))
          (when (<= min-retained-lsn (long follower-next-lsn))
            (throw (ex-info "WAL-gap drill did not advance retained WAL floor beyond the stopped follower"
                            {:node-id node-id
                             :follower-id gapped-follower-id
                             :follower-next-lsn follower-next-lsn
                             :gc-result gc-result}))))
        (step! "Forced WAL GC on leader:" leader-id
               "deleted-segments:" (:deleted-count (get gc-results leader-id))
               "min-retained-lsn:"
               (get-in gc-results [leader-id :after :min-retained-lsn]))
        (swap! ctx-atom restart-node!
               gapped-follower-id
               (node-ha-opts base-opts
                             (control-node @ctx-atom gapped-follower-id)
                             (:fence-script @ctx-atom)
                             0
                             0))
        (step! "Restarted follower:" gapped-follower-id)
        (let [bootstrap-state (wait-for-follower-bootstrap!
                               ctx-atom
                               gapped-follower-id
                               (long (or (get-in snapshot-2
                                                 [leader-id :snapshot :applied-lsn])
                                         0)))
              ctx             @ctx-atom
              {:keys [leader-id]} (wait-for-single-leader! (:servers ctx)
                                                           (:db-name ctx)
                                                           (:live-node-ids ctx)
                                                           20000)]
          (step! "Follower bootstrapped from:"
                 (:ha-follower-bootstrap-source-endpoint bootstrap-state)
                 "snapshot-lsn:"
                 (:ha-follower-bootstrap-snapshot-last-applied-lsn
                  bootstrap-state))
          (wait-for-replication! ctx (:live-node-ids ctx) batch-3-lsn)
          (write-value! (:conns ctx) leader-id "post-bootstrap" "v-final")
          (let [target-lsn (effective-local-lsn ctx leader-id)]
            (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
            (verify-value-on-nodes! ctx (:live-node-ids ctx)
                                    "post-bootstrap" "v-final")
            {:ctx ctx
             :result {:scenario :wal-gap
                      :leader-endpoint leader-endpoint
                      :gapped-follower-id gapped-follower-id
                      :baseline-lsn baseline-lsn
                      :follower-next-lsn follower-next-lsn
                      :mid-gap-lsn batch-2-lsn
                      :pre-rejoin-leader-lsn batch-3-lsn
                      :retention-before retention-before
                      :source-node-ids source-node-ids
                      :gc-results gc-results
                      :bootstrap-state bootstrap-state
                      :snapshots {:baseline
                                  (into {}
                                        (map (fn [[node-id snapshot]]
                                               [node-id
                                                (select-keys (:snapshot snapshot)
                                                             [:slot :applied-lsn
                                                              :created-ms])]))
                                        snapshot-1)
                                  :gap-batch-1
                                  (into {}
                                        (map (fn [[node-id snapshot]]
                                               [node-id
                                                (select-keys (:snapshot snapshot)
                                                             [:slot :applied-lsn
                                                              :created-ms])]))
                                        snapshot-2)
                                  :gap-batch-2
                                  (into {}
                                        (map (fn [[node-id snapshot]]
                                               [node-id
                                                (select-keys (:snapshot snapshot)
                                                             [:slot :applied-lsn
                                                              :created-ms])]))
                                        snapshot-3)}
                      :watermarks (watermarks-by-node ctx
                                                      (:live-node-ids ctx))}}))))))

(defn- run-fencing-failure!
  [ctx-atom]
  (step! "Starting local 3-node HA cluster for fencing-failure drill")
  (let [ctx      (setup-cluster! ctx-atom 0)
        {:keys [leader-id]} (wait-for-single-leader! (:servers ctx)
                                                     (:db-name ctx)
                                                     (:live-node-ids ctx)
                                                     20000)
        leader-endpoint (get-in ctx [:nodes (dec leader-id) :endpoint])
        follower-ids    (follower-node-ids ctx leader-id)]
    (step! "Initial leader:" leader-id leader-endpoint)
    (write-value! (:conns ctx) leader-id "seed" "v1")
    (let [target-lsn (effective-local-lsn ctx leader-id)]
      (wait-for-replication! ctx (:live-node-ids ctx) target-lsn)
      (verify-value-on-nodes! ctx (:live-node-ids ctx) "seed" "v1"))
    (doseq [node-id follower-ids]
      (delete-file-if-exists! (get-in ctx [:nodes (dec node-id) :fence-log]))
      (i/assoc-opt (require-store! ctx node-id)
                   :ha-fencing-hook
                   (hook-config (:fence-script ctx)
                                (get-in ctx [:nodes (dec node-id) :fence-log])
                                7
                                2)))
    (step! "Updated follower fencing hooks to fail; stopping current leader")
    (swap! ctx-atom stop-node! leader-id)
    (let [ctx @ctx-atom
          result
          (await-value
            "failed promotion attempts with no surviving leader"
            20000
            250
            (fn []
              (let [snapshot   (probe-snapshot (:servers ctx)
                                               (:db-name ctx)
                                               (:live-node-ids ctx))
                    leaders    (->> snapshot
                                    (keep (fn [[node-id {:keys [status]}]]
                                            (when (= :leader status) node-id)))
                                    vec)
                    retry-info (into {}
                                     (map (fn [node-id]
                                            [node-id
                                             (repeated-fence-op-ids
                                               (get-in ctx [:nodes (dec node-id)
                                                             :fence-log]))]))
                                     follower-ids)]
                (cond
                  (seq leaders)
                  (throw (ex-info "A follower promoted despite failing fencing"
                                  {:leaders leaders
                                   :snapshot snapshot}))

                  (some seq (vals retry-info))
                  {:probe-snapshot snapshot
                   :fence-retries retry-info}

                  :else
                  nil))))]
      {:ctx ctx
       :result {:scenario :fencing-failure
                :initial-leader-endpoint leader-endpoint
                :follower-ids follower-ids
                :probe-snapshot (:probe-snapshot result)
                :fence-retries (:fence-retries result)
                :watermarks (watermarks-by-node ctx
                                                (:live-node-ids ctx))}})))

(def ^:private control-drill-db-identity
  "7a9f1f8d-cf5a-4fd6-a5a0-6db4a74a6f6f")

(defn- run-witness-topology!
  [ctx-atom]
  (require-sofa-jraft! @ctx-atom :witness-topology)
  (step! "Starting standalone JRaft authorities for witness-topology drill")
  (let [promotable-node-ids #{1 2}
        witness-node-ids    #{3}
        voters              (authority-voters @ctx-atom
                                              promotable-node-ids
                                              witness-node-ids)
        ctx                 (setup-control-authorities! ctx-atom [1 2 3] voters)
        leader-before       (wait-for-single-authority-leader!
                             (:authorities ctx)
                             [1 2 3]
                             10000)
        authority-a         (get-in ctx [:authorities 1])]
    (step! "Initial control-plane leader:" leader-before)
    (let [membership (ctrl/init-membership-hash! authority-a "abc123")
          acquire    (ctrl/try-acquire-lease
                      authority-a
                      {:db-identity control-drill-db-identity
                       :leader-node-id 1
                       :leader-endpoint "10.0.0.11:8898"
                       :lease-renew-ms 1000
                       :lease-timeout-ms 3000
                       :leader-last-applied-lsn 7
                       :now-ms 1000
                       :observed-version 0
                       :observed-lease nil})]
      (when-not (:ok? membership)
        (throw (ex-info "Failed to initialize membership hash"
                        {:membership membership})))
      (when-not (:ok? acquire)
        (throw (ex-info "Failed to acquire initial witness drill lease"
                        {:acquire acquire})))
      (swap! ctx-atom stop-control-authority! 2)
      (let [ctx          @ctx-atom
            leader-after (wait-for-single-authority-leader!
                          (:authorities ctx)
                          [1 3]
                          10000)
            renew-result (await-value
                          "lease renew with witness quorum"
                          10000
                          100
                          (fn []
                            (let [res (try
                                        (ctrl/renew-lease
                                          authority-a
                                          {:db-identity
                                           control-drill-db-identity
                                           :leader-node-id 1
                                           :leader-endpoint "10.0.0.11:8898"
                                           :term (:term (:lease acquire))
                                           :lease-renew-ms 1000
                                           :lease-timeout-ms 3000
                                           :leader-last-applied-lsn 8
                                           :now-ms 1500})
                                        (catch Exception e
                                          {:ok? false
                                           :exception (ex-message e)}))]
                              (when (:ok? res)
                                res))))]
        (step! "Stopped promotable voter:" 2)
        {:ctx ctx
         :result {:scenario :witness-topology
                  :leader-before leader-before
                  :leader-after leader-after
                  :stopped-node-id 2
                  :renew-result renew-result
                  :voters voters}}))))

(defn- run-control-quorum-loss!
  [ctx-atom]
  (require-sofa-jraft! @ctx-atom :control-quorum-loss)
  (step! "Starting standalone JRaft authorities for control-quorum-loss drill")
  (let [node-ids [1 2 3]
        voters   (authority-voters @ctx-atom #{1 2 3} #{})
        ctx      (setup-control-authorities! ctx-atom node-ids voters)
        leader-id (wait-for-single-authority-leader!
                   (:authorities ctx)
                   node-ids
                   10000)
        authority (get-in ctx [:authorities leader-id])
        stopped-node-ids (vec (remove #{leader-id} node-ids))]
    (step! "Initial control-plane leader:" leader-id)
    (let [membership (ctrl/init-membership-hash! authority "abc123")]
      (when-not (:ok? membership)
        (throw (ex-info "Failed to initialize membership hash"
                        {:membership membership}))))
    (doseq [node-id stopped-node-ids]
      (swap! ctx-atom stop-control-authority! node-id))
    (let [ctx @ctx-atom
          read-error (try
                       (ctrl/read-membership-hash authority)
                       (throw (ex-info "Control-plane quorum-loss read unexpectedly succeeded"
                                       {:leader-id leader-id}))
                       (catch clojure.lang.ExceptionInfo e
                         (let [data (ex-data e)]
                           (when-not (contains? #{:ha/control-timeout
                                                  :ha/control-read-failed
                                                  :ha/control-node-unavailable}
                                                (:error data))
                             (throw e))
                           data)))]
      {:ctx ctx
       :result {:scenario :control-quorum-loss
                :leader-before leader-id
                :stopped-node-ids stopped-node-ids
                :read-error read-error
                :voters voters}})))

(defn- safe-close-conn!
  [conn]
  (when conn
    (try
      (d/close conn)
      (catch Throwable _ nil))))

(defn- cleanup!
  [ctx delete-work-dir?]
  (doseq [conn (vals (:conns ctx))]
    (safe-close-conn! conn))
  (doseq [server (vals (:servers ctx))]
    (safe-stop-server! server))
  (doseq [authority (vals (:authorities ctx))]
    (safe-stop-authority! authority))
  (when delete-work-dir?
    (safe-delete-dir! (:work-dir ctx))))

(defmacro with-control-backend
  [control-backend & body]
  `(with-redefs [srv/*start-ha-authority-fn*
                 (fn [db-name# ha-opts#]
                   (dha/start-ha-authority
                     db-name#
                     (assoc-in ha-opts#
                               [:ha-control-plane :backend]
                               ~control-backend)))
                 srv/*stop-ha-authority-fn*
                 dha/stop-ha-authority]
     ~@body))

(defn- print-result!
  [{:keys [scenario] :as result}]
  (step! "Scenario:" (name scenario))
  (doseq [[k v] (dissoc result :scenario)]
    (step! (str (name k) ":") (pr-str v))))

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        scenario (first arguments)]
    (cond
      (:help options)
      (do
        (println (usage summary))
        (System/exit 0))

      (seq errors)
      (do
        (binding [*out* *err*]
          (doseq [e errors] (println e))
          (println)
          (println (usage summary)))
        (System/exit 1))

      (nil? scenario)
      (do
        (binding [*out* *err*]
          (println (usage summary)))
        (System/exit 1))

      :else
      (let [work-dir    (resolve-work-dir options scenario)
            ctx-atom    (atom {:scenario scenario
                               :work-dir work-dir
                               :db-name (:db-name options)
                               :control-backend (:control-backend options)
                               :group-id (str "ha-drill-" scenario "-" (UUID/randomUUID))
                               :db-identity (str (UUID/randomUUID))
                               :fence-script (fence-script-path)
                               :nodes (make-nodes work-dir (:port-base options))
                               :servers {}
                               :conns {}
                               :authorities {}
                               :probe-clients {}
                               :live-node-ids #{}})
            keep-dir?   (:keep-work-dir options)]
        (log/set-min-level! (if (:verbose options) :info :warn))
        (try
          (with-control-backend (:control-backend options)
            (let [{:keys [ctx result]}
                  (case scenario
                    "failover"        (run-failover! ctx-atom)
                    "follower-rejoin" (run-follower-rejoin! ctx-atom)
                    "membership-hash-drift" (run-membership-hash-drift! ctx-atom)
                    "fencing-hook-verify" (run-fencing-hook-verify! ctx-atom)
                    "clock-skew-pause" (run-clock-skew-pause! ctx-atom)
                    "degraded-mode-no-valid-source"
                    (run-degraded-mode-no-valid-source! ctx-atom)
                    "wal-gap"         (run-wal-gap! ctx-atom)
                    "fencing-failure" (run-fencing-failure! ctx-atom)
                    "witness-topology" (run-witness-topology! ctx-atom)
                    "control-quorum-loss" (run-control-quorum-loss! ctx-atom)
                    (throw (ex-info "Unknown scenario"
                                    {:scenario scenario})))]
              (reset! ctx-atom ctx)
              (print-result! result)
              (when keep-dir?
                (step! "Work dir kept at:" work-dir))
              (cleanup! @ctx-atom (not keep-dir?))
              (System/exit 0)))
          (catch Throwable t
            (binding [*out* *err*]
              (println "HA drill failed:" (ex-message t))
              (when-let [data (ex-data t)]
                (println "Failure data:" (pr-str data)))
              (println "Work dir kept at:" work-dir))
            (cleanup! @ctx-atom false)
            (System/exit 1)))))))

(apply -main *command-line-args*)
