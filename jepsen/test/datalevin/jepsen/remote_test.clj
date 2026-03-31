(ns datalevin.jepsen.remote-test
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [datalevin.jepsen.core :as core]
   [datalevin.jepsen.integration-harness :as harness]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.local.remote :as lremote]
   [datalevin.jepsen.remote :as remote]
   [datalevin.jepsen.remote-node :as remote-node]
   [datalevin.jepsen.workload.witness-topology :as witness-topology]
   [jepsen.control :as control]
   [jepsen.db :as jdb]
   [datalevin.util :as u])
  (:import
   [java.io StringWriter]
   [java.net ServerSocket]
   [java.util UUID]))

(defn- node
  [logical-node node-id]
  (let [host-octet (+ 10 (long node-id))]
    {:logical-node logical-node
     :node-id      node-id
     :endpoint     (str "10.0.0." host-octet ":8898")
     :peer-id      (str "10.0.0." host-octet ":15001")
     :root         (str "/var/tmp/dtlv-jepsen/" logical-node)}))

(defn- base-remote-config
  [overrides]
  (merge {:db-name     "remote-smoke"
          :workload    :append
          :group-id    "remote-smoke-group"
          :db-identity "remote-smoke-db"
          :repo-root   "/srv/datalevin"
          :nodes       [(node "n1" 1)
                        (node "n2" 2)
                        (node "n3" 3)]}
         overrides))

(defn- repo-root-path
  []
  (let [cwd         (.getCanonicalFile (io/file "."))
        cwd-script  (io/file cwd "script" "jepsen" "start-remote-node")
        parent      (.getCanonicalFile (io/file cwd ".."))
        parent-script (io/file parent "script" "jepsen" "start-remote-node")]
    (cond
      (.exists cwd-script)
      (.getPath cwd)

      (.exists parent-script)
      (.getPath parent)

      :else
      (u/raise "Unable to resolve Datalevin repo root for remote Jepsen test"
               {:cwd (.getPath cwd)}))))

(defn- reserve-ports
  [n]
  (let [sockets (repeatedly n #(ServerSocket. 0))]
    (try
      (mapv #(.getLocalPort ^ServerSocket %) sockets)
      (finally
        (doseq [^ServerSocket socket sockets]
          (.close socket))))))

(defn- controller-node
  [root-dir logical-node node-id endpoint-port peer-port]
  {:logical-node logical-node
   :node-id      node-id
   :endpoint     (str "127.0.0.1:" endpoint-port)
   :peer-id      (str "127.0.0.1:" peer-port)
   :root         (str root-dir u/+separator+ logical-node)})

(defn- controller-remote-config
  ([root-dir]
   (controller-remote-config root-dir {}))
  ([root-dir {:keys [config-overrides node-overrides]}]
  (let [[endpoint-1 peer-1
         endpoint-2 peer-2
         endpoint-3 peer-3] (reserve-ports 6)
        apply-node-override
        (fn [node]
          (merge node (get node-overrides (:logical-node node))))
        n1 (apply-node-override
            (controller-node root-dir "n1" 1 endpoint-1 peer-1))
        n2 (apply-node-override
            (controller-node root-dir "n2" 2 endpoint-2 peer-2))
        n3 (apply-node-override
            (controller-node root-dir "n3" 3 endpoint-3 peer-3))]
    (merge
     {:db-name         (str "remote-controller-" (UUID/randomUUID))
      :workload        :append
      :group-id        (str "remote-controller-group-" (UUID/randomUUID))
      :db-identity     (str "remote-controller-db-" (UUID/randomUUID))
      :control-backend :sofa-jraft
      :repo-root       (repo-root-path)
      :nodes           [n1]
      :control-nodes   [n1
                        (assoc n2 :promotable? false)
                        (assoc n3 :promotable? false)]
      :cluster-opts
      {:ha-control-plane
       {:operation-timeout-ms 45000}}}
     config-overrides))))

(defn- with-temp-remote-config
  [config f]
  (let [dir  (u/tmp-dir (str "jepsen-remote-config-" (UUID/randomUUID)))
        path (str dir u/+separator+ "cluster.edn")]
    (u/create-dirs dir)
    (spit path (pr-str config))
    (try
      (f path)
      (finally
        (u/delete-files dir)))))

(defn- delete-tree-with-retries!
  [dir]
  (loop [attempt 0]
    (let [exists? (u/file-exists dir)]
      (when exists?
        (try
          (u/delete-files dir)
          (catch Throwable _
            nil))
        (when (u/file-exists dir)
          (if (< attempt 9)
            (do
              (Thread/sleep 200)
              (recur (inc attempt)))
            (u/raise "Unable to delete remote Jepsen test directory"
                     {:dir dir
                      :attempts (inc attempt)})))))))

(defn- with-temp-controller-remote-config
  ([f]
   (with-temp-controller-remote-config {} f))
  ([overrides f]
  (let [dir    (u/tmp-dir (str "jepsen-remote-controller-" (UUID/randomUUID)))
        config (controller-remote-config dir overrides)
        path   (str dir u/+separator+ "cluster.edn")]
    (u/create-dirs dir)
    (spit path (pr-str config))
    (try
      (f config path)
      (finally
        (delete-tree-with-retries! dir))))))

(defn- capture-ex-info
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      e)))

(defn- remote-test-exception
  [config opts]
  (with-temp-remote-config
    config
    (fn [config-path]
      (capture-ex-info
       #(core/datalevin-test
         (merge {:remote-config config-path
                 :rate          10
                 :time-limit    5
                 :nemesis       []}
                opts))))))

(defn- remote-test-map
  [config opts]
  (with-temp-remote-config
    config
    (fn [config-path]
      (core/datalevin-test
       (merge {:remote-config config-path
               :rate          10
               :time-limit    5
               :nemesis       []}
              opts)))))

(defn- witness-remote-config
  []
  (base-remote-config
   {:workload      :witness-topology
    :key-count     4
    :nodes         [(node "n1" 1)
                    (node "n2" 2)]
    :control-nodes [(node "n1" 1)
                    (node "n2" 2)
                    (assoc (node "n3" 3)
                           :promotable? false)]}))

(defn- unsupported-workload-config
  [workload-name]
  (if (= :witness-topology workload-name)
    (witness-remote-config)
    (base-remote-config
     {:workload workload-name
      :key-count 4})))

(defn- copy-file!
  [from to]
  (u/create-dirs (.getParent (io/file to)))
  (io/copy (io/file from) (io/file to))
  to)

(defn- write-file!
  [path content]
  (u/create-dirs (.getParent (io/file path)))
  (spit path content)
  path)

(defn- delete-file!
  [path]
  (let [file (io/file path)]
    (when (.exists file)
      (io/delete-file file true)))
  true)

(defn- read-edn-file
  [path]
  (let [file (io/file path)]
    (when (.exists file)
      (-> path slurp edn/read-string))))

(def ^:private controller-launcher-stop-timeout-ms 30000)

(defn- cancel-launcher!
  [launchers logical-node]
  (when-let [{:keys [thread stopped]} (get @launchers logical-node)]
    (when thread
      (.interrupt ^Thread thread))
    (when (= ::timeout
             (deref stopped controller-launcher-stop-timeout-ms ::timeout))
      (u/raise "Timed out waiting for controller launcher to stop"
               {:logical-node logical-node
                :timeout-ms controller-launcher-stop-timeout-ms}))
    (swap! launchers dissoc logical-node))
  true)

(defn- with-local-controller-launchers
  [f]
  (let [launchers (atom {})
        calls     (atom [])]
    (binding [local/*remote-launcher-ops*
              {:upload-config
               (fn [_ssh node local-config-path]
                 (swap! calls conj {:op :upload-config
                                    :logical-node (:logical-node node)})
                 (copy-file! local-config-path
                             (#'local/remote-config-path-for-node node)))
               :write-content
               (fn [_ssh node remote-path content]
                 (swap! calls conj {:op :write-content
                                    :logical-node (:logical-node node)})
                 (write-file! remote-path content))
               :delete-path
               (fn [_ssh node remote-path]
                 (swap! calls conj {:op :delete-path
                                    :logical-node (:logical-node node)})
                 (delete-file! remote-path))
               :sync-control-defaults
               (fn [_ssh node]
                 (swap! calls conj {:op :sync-control-defaults
                                    :logical-node (:logical-node node)})
                 (u/create-dirs (remote/control-state-dir node))
                 (write-file! (remote/network-state-file node)
                              (pr-str {:blocked-endpoints #{}
                                       :endpoint-profiles {}}))
                 (write-file! (remote/clock-skew-state-file node)
                              "0\n")
                 (write-file! (remote/fencing-mode-file node)
                              "success\n")
                 (delete-file! (remote/storage-fault-state-file node))
                 (delete-file! (remote/snapshot-failpoint-file node)))
               :node-state
               (fn [_ssh _logical-node node]
                 (swap! calls conj {:op :node-state
                                    :logical-node (:logical-node node)})
                 (read-edn-file (#'local/remote-state-file node)))
               :start-launcher
               (fn [_ssh _repo-root node _verbose?]
                 (swap! calls conj {:op :start-launcher
                                    :logical-node (:logical-node node)})
                 (let [config-path (#'local/remote-config-path-for-node node)
                       stopped    (promise)
                       launcher   (doto
                                    (Thread.
                                     ^Runnable
                                     (fn []
                                       (binding [*out* (StringWriter.)
                                                 *err* (StringWriter.)]
                                         (try
                                           (#'remote-node/start-node!
                                            (remote/read-config config-path)
                                            (:logical-node node)
                                            45000
                                            false
                                            false)
                                           (catch Throwable _
                                             nil)
                                           (finally
                                             (deliver stopped true))))))
                                    (.setName
                                     (str "remote-controller-launcher-"
                                          (:logical-node node)))
                                    (.setDaemon true)
                                    (.start))]
                   (swap! launchers assoc
                          (:logical-node node)
                          {:thread launcher
                           :stopped stopped})
                   true))
               :stop-launcher
               (fn [_ssh _repo-root node]
                 (swap! calls conj {:op :stop-launcher
                                    :logical-node (:logical-node node)})
                 (cancel-launcher! launchers (:logical-node node)))}]
      (try
        (f {:launchers launchers
            :calls calls})
        (finally
          (doseq [logical-node (keys @launchers)]
            (cancel-launcher! launchers logical-node)))))))

(def ^:private previously-unsupported-workloads
  [:degraded-rejoin
   :membership-drift
   :membership-drift-live
   :rejoin-bootstrap
   :snapshot-checksum-rejoin
   :snapshot-copy-corruption-rejoin
   :snapshot-db-identity-rejoin
   :snapshot-manifest-corruption-rejoin
   :fencing-retry])

(def ^:private previously-unsupported-nemeses
  [:leader-io-stall
   :leader-disk-full
   :clock-skew-pause
   :clock-skew-leader-fast
   :clock-skew-leader-slow
   :clock-skew-mixed])

(deftest config-workload-applies-configured-topology-and-workload-opts-test
  (let [config   (-> (witness-remote-config)
                     (remote/validate-config! core/workloads))
        workload (remote/config-workload config core/workloads)]
    (is (= witness-topology/schema (:schema workload)))
    (is (= ["n1" "n2"] (:nodes workload)))
    (is (= ["n1" "n2" "n3"] (:datalevin/control-nodes workload)))))

(deftest workload-topology-identifies-control-only-witness-test
  (let [config   (-> (base-remote-config
                      {:workload      :register
                       :key-count     4
                       :nodes         [(node "n1" 1)
                                       (node "n2" 2)]
                       :control-nodes [(node "n1" 1)
                                       (node "n2" 2)
                                       (assoc (node "n3" 3)
                                              :promotable? false)]})
                     (remote/validate-config! core/workloads))
        workload (remote/config-workload config core/workloads)
        topology (remote/workload-topology config workload)]
    (is (= ["n1" "n2"] (mapv :logical-node (:data-nodes topology))))
    (is (= ["n1" "n2" "n3"] (mapv :logical-node (:control-nodes topology))))
    (is (= ["n3"] (mapv :logical-node (:control-only-nodes topology))))
    (is (false? (get-in topology [:node-by-name "n3" :promotable?])))))

(deftest workload-topology-rejects-promotable-control-only-witness-test
  (let [config   (-> (base-remote-config
                      {:workload      :register
                       :key-count     4
                       :nodes         [(node "n1" 1)
                                       (node "n2" 2)]
                       :control-nodes [(node "n1" 1)
                                       (node "n2" 2)
                                       (assoc (node "n3" 3)
                                              :promotable? true)]})
                     (remote/validate-config! core/workloads))
        workload (remote/config-workload config core/workloads)
        e        (capture-ex-info #(remote/workload-topology config workload))]
    (is e)
    (is (re-find #"cannot be promotable" (ex-message e)))
    (is (= "n3" (get-in (ex-data e) [:node :logical-node])))))

(deftest node-ha-opts-merges-config-and-workload-overrides-test
  (let [data-nodes    [(node "n1" 1)
                       (node "n2" 2)]
        config        (-> (base-remote-config
                           {:workload    :register
                            :key-count   4
                            :nodes       data-nodes
                            :cluster-opts {:ha-lease-timeout-ms 9000
                                           :ha-control-plane
                                           {:rpc-timeout-ms 7000}}})
                          (remote/validate-config! core/workloads))
        workload      {:datalevin/cluster-opts
                       {:ha-max-promotion-lag-lsn 7
                        :ha-control-plane
                        {:operation-timeout-ms 12345}}}
        opts          (remote/node-ha-opts config
                                           (first data-nodes)
                                           workload
                                           data-nodes
                                           data-nodes)]
    (is (= (:db-name config) (:db-name opts)))
    (is (= 1 (:ha-node-id opts)))
    (is (= (:peer-id (first data-nodes))
           (get-in opts [:ha-control-plane :local-peer-id])))
    (is (= 9000 (:ha-lease-timeout-ms opts)))
    (is (= 7 (:ha-max-promotion-lag-lsn opts)))
    (is (= 7000 (get-in opts [:ha-control-plane :rpc-timeout-ms])))
    (is (= 12345 (get-in opts [:ha-control-plane :operation-timeout-ms])))
    (is (= [1 2]
           (mapv :ha-node-id
                 (get-in opts [:ha-control-plane :voters]))))))

(deftest datalevin-test-remote-accepts-previously-unsupported-workloads-test
  (doseq [workload-name previously-unsupported-workloads]
    (testing (name workload-name)
      (let [test-map (remote-test-map
                      (unsupported-workload-config workload-name)
                      {})
            remote-spec (:remote-spec (:db test-map))]
        (is (= (str (name workload-name) " remote")
               (:name test-map)))
        (is (= workload-name
               (get-in remote-spec [:config :workload])))))))

(deftest datalevin-test-remote-accepts-previously-unsupported-nemeses-test
  (doseq [fault previously-unsupported-nemeses]
    (testing (name fault)
      (let [test-map (remote-test-map
                      (base-remote-config
                       {:workload :register
                        :key-count 4})
                      {:nemesis [fault]})]
        (is (= [fault] (:datalevin/nemesis-faults test-map)))))))

(deftest datalevin-test-remote-requires-repo-root-test
  (let [e (remote-test-exception
           (-> (base-remote-config
                {:workload :register
                 :key-count 4})
               (dissoc :repo-root))
           {})]
    (is e)
    (is (re-find #"requires :repo-root" (ex-message e)))
    (is (= :register (:workload (ex-data e))))))

(deftest datalevin-test-remote-applies-ssh-opts-from-config-test
  (let [test-map (remote-test-map
                  (base-remote-config
                   {:workload :register
                    :key-count 4
                    :ssh {:username "ubuntu"
                          :password nil}})
                  {})
        ssh     (:ssh test-map)]
    (is (= "ubuntu" (:username ssh)))
    (is (nil? (:password ssh)))
    (is (false? (:strict-host-key-checking ssh)))))

(deftest datalevin-test-remote-config-ssh-beats-cli-defaults-test
  (let [test-map (remote-test-map
                  (base-remote-config
                   {:workload :register
                    :key-count 4
                    :ssh {:username "ubuntu"
                          :password nil}})
                  {:ssh {:dummy? false
                         :username "root"
                         :password "root"
                         :private-key-path nil
                         :strict-host-key-checking false}})
        ssh     (:ssh test-map)]
    (is (= "ubuntu" (:username ssh)))
    (is (nil? (:password ssh)))
    (is (false? (:strict-host-key-checking ssh)))))

(deftest datalevin-test-remote-cli-ssh-opts-override-config-test
  (let [test-map (remote-test-map
                  (base-remote-config
                   {:workload :register
                    :key-count 4
                    :ssh {:username "ubuntu"
                          :password nil}})
                  {:ssh {:username "ec2-user"}})
        ssh     (:ssh test-map)]
    (is (= "ec2-user" (:username ssh)))
    (is (nil? (:password ssh)))))

(deftest datalevin-test-remote-respects-dynamic-control-remote-test
  (let [test-map (control/with-remote control/clj-ssh
                   (remote-test-map
                    (base-remote-config
                     {:workload :register
                      :key-count 4})
                    {}))]
    (is (= control/clj-ssh (:remote test-map)))))

(deftest datalevin-test-remote-rejects-non-boolean-controller-local-flag-test
  (let [e (remote-test-exception
           (base-remote-config
            {:workload :register
             :key-count 4
             :nodes [(assoc (node "n1" 1) :controller-local? :yes)
                     (node "n2" 2)
                     (node "n3" 3)]})
           {})]
    (is e)
    (is (re-find #"controller-local flag must be boolean" (ex-message e)))))

(deftest remote-daemon-spawn-script-launches-detached-command-test
  (let [dir    (u/tmp-dir (str "remote-daemon-spawn-" (UUID/randomUUID)))
        log    (str dir u/+separator+ "daemon.log")
        done   (str dir u/+separator+ "done.txt")
        result (#'local/run-local-command
                nil
                ["python3"
                 (str (repo-root-path)
                      u/+separator+
                      "script"
                      u/+separator+
                      "jepsen"
                      u/+separator+
                      "remote-daemon-spawn.py")
                 log
                 "python3"
                 "-c"
                 (str "from pathlib import Path; "
                      "Path("
                      (pr-str done)
                      ").write_text('ok\\n')")]
                5000)]
    (try
      (is (:ok? result))
      (is (zero? (:exit result)))
      (is (re-find #"started" (:output result)))
      (loop [attempt 0]
        (cond
          (u/file-exists done)
          (is (= "ok\n" (slurp done)))

          (< attempt 49)
          (do
            (Thread/sleep 100)
            (recur (inc attempt)))

          :else
          (is false (str "Timed out waiting for detached command output at "
                         done))))
      (finally
        (delete-tree-with-retries! dir)))))

(deftest datalevin-test-remote-accepts-control-only-witness-topology-test
  (let [test-map    (remote-test-map (witness-remote-config) {})
        remote-spec (:remote-spec (:db test-map))
        topology    (:topology remote-spec)
        workload    (:workload remote-spec)]
    (is (= "witness-topology remote" (:name test-map)))
    (is (= ["n1" "n2"] (:nodes test-map)))
    (is (= ["n1" "n2" "n3"] (:datalevin/control-nodes test-map)))
    (is (= ["n1" "n2"] (:nodes workload)))
    (is (= ["n1" "n2" "n3"] (:datalevin/control-nodes workload)))
    (is (= ["n1" "n2"] (mapv :logical-node (:data-nodes topology))))
    (is (= ["n1" "n2" "n3"] (mapv :logical-node (:control-nodes topology))))
    (is (= ["n3"] (mapv :logical-node (:control-only-nodes topology))))))

(deftest node-ha-opts-applies-remote-hooks-and-per-node-overrides-test
  (let [data-nodes [(node "n1" 1)
                    (node "n2" 2)]
        config     (-> (base-remote-config
                        {:workload :register
                         :key-count 4
                         :nodes data-nodes
                         :jepsen-remote-clock-skew-hook? true
                         :node-ha-opts-overrides
                         {"n1" {:ha-max-promotion-lag-lsn 11}}})
                       (remote/validate-config! core/workloads))
        workload   {:datalevin/cluster-opts
                    {:ha-fencing-hook {:timeout-ms 2222
                                       :retries 4
                                       :retry-delay-ms 0}}
                    :datalevin/remote-fencing-retry? true}
        opts       (remote/node-ha-opts config
                                        (first data-nodes)
                                        workload
                                        data-nodes
                                        data-nodes)]
    (is (= 11 (:ha-max-promotion-lag-lsn opts)))
    (is (= 2222 (get-in opts [:ha-fencing-hook :timeout-ms])))
    (is (= 4 (get-in opts [:ha-fencing-hook :retries])))
    (is (= (remote/clock-skew-state-file (first data-nodes))
           (last (get-in opts [:ha-clock-skew-hook :cmd]))))
    (is (= (remote/fencing-mode-file (first data-nodes))
           (last (get-in opts [:ha-fencing-hook :cmd]))))))

(deftest controller-managed-remote-cluster-lifecycle-test
  (with-temp-controller-remote-config
    (fn [config config-path]
      (with-local-controller-launchers
        (fn [_state]
          (let [test-map    (core/datalevin-test
                             {:remote-config config-path
                              :rate          1
                              :time-limit    5
                              :nemesis       []
                              :ssh           {:dummy? true}})
                db          (:db test-map)
                cluster-id  (:datalevin/cluster-id test-map)
                db-identity (:db-identity config)]
            (try
              (doseq [node-name (:nodes test-map)]
                (jdb/setup! db test-map node-name))
              (let [cluster  (local/cluster-state cluster-id)
                    n1       (get-in cluster [:node-by-name "n1"])
                    n2       (get-in cluster [:node-by-name "n2"])
                    n3       (get-in cluster [:node-by-name "n3"])
                    leader   (:leader (local/wait-for-single-leader!
                                       cluster-id
                                       45000))
                    runtime  (local/remote-runtime-node db-identity 1)
                    n1-storage-path  (remote/storage-fault-state-file n1)
                    n1-snapshot-path (remote/snapshot-failpoint-file n1)
                    n1-fencing-path  (remote/fencing-mode-file n1)]
                (is (true? (:remote? cluster)))
                (is (= ["n1"] (:data-node-names cluster)))
                (is (= ["n1" "n2" "n3"] (:control-node-names cluster)))
                (is (= ["n2" "n3"] (:control-only-node-names cluster)))
                (is (= "n1" leader))
                (is (= "n1" (:logical-node runtime)))
                (is (nil? (local/remote-runtime-node db-identity 2)))
                (is (nil? (local/remote-runtime-node db-identity 3)))

                (doseq [node [n1 n2 n3]]
                  (is (u/file-exists
                       (#'local/remote-config-path-for-node node)))
                  (is (= {:blocked-endpoints #{}
                          :endpoint-profiles {}}
                         (read-edn-file (remote/network-state-file node))))
                  (is (= "0\n" (slurp (remote/clock-skew-state-file node))))
                  (is (= "success\n" (slurp (remote/fencing-mode-file node))))
                  (is (not (u/file-exists
                            (remote/storage-fault-state-file node))))
                  (is (not (u/file-exists
                            (remote/snapshot-failpoint-file node)))))

                (harness/write-append-batch! test-map 0 [1] 0)
                (is (= {"n1" [1]}
                       (harness/wait-for-append-values-on-nodes!
                        cluster-id
                        ["n1"]
                        0
                        [1]
                        30000)))

                (local/wedge-node-storage! cluster-id "n1" {:mode :stall})
                (let [storage-state (read-edn-file n1-storage-path)]
                  (is (= :stall (:mode storage-state)))
                  (is (integer? (:faulted-at-ms storage-state))))
                (local/heal-node-storage! cluster-id "n1")
                (is (not (u/file-exists n1-storage-path)))

                (local/set-node-snapshot-failpoint! cluster-id
                                                    "n1"
                                                    :corrupt-manifest)
                (is (= {:mode :corrupt-manifest}
                       (read-edn-file n1-snapshot-path)))
                (local/clear-node-snapshot-failpoint! cluster-id "n1")
                (is (not (u/file-exists n1-snapshot-path)))

                (local/set-fencing-hook-mode! cluster-id :fail)
                (is (= "fail\n" (slurp n1-fencing-path))))
              (finally
                (doseq [node-name (:nodes test-map)]
                  (jdb/teardown! db test-map node-name))))
            (is (nil? (local/cluster-state cluster-id)))
            (is (nil? (local/remote-runtime-node db-identity 1)))))))))

(deftest init-remote-cluster-starts-control-only-witnesses-before-data-nodes-test
  (with-temp-controller-remote-config
    (fn [config config-path]
      (let [config    (remote/validate-config! config core/workloads)
            workload  (remote/config-workload config core/workloads)
            topology  (remote/workload-topology config workload)
            stages    (#'lremote/launch-node-stages topology)]
        (is (= [["n2" "n3"]
                ["n1"]]
               (mapv (fn [stage]
                       (mapv :logical-node stage))
                     stages)))))))

(deftest controller-managed-mixed-local-remote-cluster-lifecycle-test
  (with-temp-controller-remote-config
    {:config-overrides {:repo-root "/srv/unused"}
     :node-overrides {"n1" {:controller-local? true
                            :repo-root (repo-root-path)}}}
    (fn [config config-path]
      (with-local-controller-launchers
        (fn [{:keys [calls]}]
          (let [test-map    (core/datalevin-test
                             {:remote-config config-path
                              :rate          1
                              :time-limit    5
                              :nemesis       []
                              :ssh           {:dummy? true}})
                db          (:db test-map)
                cluster-id  (:datalevin/cluster-id test-map)
                db-identity (:db-identity config)]
            (try
              (doseq [node-name (:nodes test-map)]
                (jdb/setup! db test-map node-name))
              (let [cluster  (local/cluster-state cluster-id)
                    n1       (get-in cluster [:node-by-name "n1"])
                    leader   (:leader (local/wait-for-single-leader!
                                       cluster-id
                                       45000))
                    state    (read-edn-file (#'local/remote-state-file n1))]
                (is (true? (:controller-local? n1)))
                (is (= (repo-root-path) (:repo-root n1)))
                (is (= [] (:datalevin/session-nodes test-map)))
                (is (= "n1" leader))
                (is (= :running (:status state)))
                (is (= "n1" (:node state)))
                (is (nil? (local/remote-runtime-node db-identity 1)))
                (is (u/file-exists (#'local/remote-config-path-for-node n1)))
                (is (= {"n1" [1]}
                       (do
                         (harness/write-append-batch! test-map 0 [1] 0)
                         (harness/wait-for-append-values-on-nodes!
                          cluster-id
                          ["n1"]
                          0
                          [1]
                          30000))))
                (is (empty? (filter #(= "n1" (:logical-node %)) @calls))))
              (finally
                (doseq [node-name (:nodes test-map)]
                  (jdb/teardown! db test-map node-name))))
            (is (nil? (local/cluster-state cluster-id)))
            (is (nil? (local/remote-runtime-node db-identity 1)))))))))

(deftest controller-managed-mixed-local-remote-run-test
  (with-temp-controller-remote-config
    {:config-overrides {:repo-root "/srv/unused"}
     :node-overrides {"n1" {:controller-local? true
                            :repo-root (repo-root-path)}}}
    (fn [config config-path]
      (with-local-controller-launchers
        (fn [_state]
          (let [test-map    (core/datalevin-test
                             {:remote-config config-path
                              :rate          1
                              :time-limit    1
                              :nemesis       []
                              :ssh           {:dummy? true}})
                cluster-id  (:datalevin/cluster-id test-map)
                db-identity (:db-identity config)
                result      (core/run! test-map)]
            (is (= [] (:datalevin/session-nodes test-map)))
            (is (contains? #{true :unknown}
                           (get-in result [:results :valid?])))
            (is (true? (get-in result [:results :exceptions :valid?])))
            (is (nil? (local/cluster-state cluster-id)))
            (is (nil? (local/remote-runtime-node db-identity 1)))))))))
