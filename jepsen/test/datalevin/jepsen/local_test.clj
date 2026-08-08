(ns datalevin.jepsen.local-test
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.interface :as i]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.local.cluster :as lcluster]
   [datalevin.jepsen.local.ops :as lops]
   [datalevin.jepsen.test-support :as test-support]
   [datalevin.kv :as kv]
   [datalevin.remote :as r]
   [datalevin.server :as srv]
   [datalevin.util :as u])
  (:import
   [datalevin.db DB]
   [datalevin.server Server]
   [java.net ServerSocket]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures :once test-support/quiet-logs-fixture)

(defn- current-java-bin
  []
  (.getPath (io/file (System/getProperty "java.home") "bin" "java")))

(declare ha-test-server)

(defn- last-nonblank-line
  [s]
  (some->> (clojure.string/split-lines (or s ""))
           reverse
           (some (fn [line]
                   (let [trimmed (clojure.string/trim line)]
                     (when-not (clojure.string/blank? trimmed)
                       trimmed))))))

(def ^:private wal-child-ready-timeout-ms 15000)
(def ^:private wal-child-process-timeout-ms 30000)

(defn- process-output
  [^Process process]
  (try
    (slurp (.getInputStream process) :encoding "UTF-8")
    (catch Exception _
      "")))

(defn- child-process-result
  [^Process process timeout-ms]
  (let [finished? (.waitFor process
                            (long timeout-ms)
                            TimeUnit/MILLISECONDS)]
    (if finished?
      (let [exit   (.exitValue process)
            output (process-output process)
            result (try
                     (some-> output last-nonblank-line edn/read-string)
                     (catch Exception _
                       nil))]
        {:ok? (zero? exit)
         :exit exit
         :output output
         :result result})
      (do
        (.destroy process)
        (when-not (.waitFor process 200 TimeUnit/MILLISECONDS)
          (.destroyForcibly process))
        {:ok? false
         :reason :timeout
         :timeout-ms timeout-ms
         :output (process-output process)}))))

(defn- reserve-ports
  [n]
  (let [sockets (repeatedly n #(ServerSocket. 0))]
    (try
      (mapv #(.getLocalPort ^ServerSocket %) sockets)
      (finally
        (doseq [^ServerSocket socket sockets]
          (.close socket))))))

(deftest node-kv-open-opts-includes-node-ha-opts-test
  (let [cluster {:base-opts
                 {:wal? true
                  :db-identity "db-test"
                  :ha-mode :consensus-lease
                  :ha-members [{:node-id 1
                                :endpoint "127.0.0.1:19001"}]
                  :ha-control-plane
                  {:backend :sofa-jraft
                   :group-id "group"
                   :voters [{:peer-id "127.0.0.1:19004"
                             :ha-node-id 1
                             :promotable? true}]
                   :operation-timeout-ms 30000}}
                 :node-ha-opt-overrides
                 {:n1 {:wal-segment-max-ms 17
                       :ha-control-plane
                       {:operation-timeout-ms 12345}}}}
        node    {:logical-node :n1
                 :node-id 1
                 :peer-id "127.0.0.1:19004"
                 :endpoint "127.0.0.1:19001"}
        opts    (#'lops/node-kv-open-opts cluster :n1 node)]
    (is (= true (:wal? opts)))
    (is (= 1 (:ha-node-id opts)))
    (is (= "127.0.0.1:19004"
           (get-in opts [:ha-control-plane :local-peer-id])))
    (is (= 17 (:wal-segment-max-ms opts)))
    (is (= 12345
           (get-in opts [:ha-control-plane :operation-timeout-ms])))
    (is (= {:pool-size 1 :time-out 10000}
           (:client-opts opts)))))

(deftest local-node-conn-open-uses-node-ha-opts-test
  (let [cluster {:remote? false
                 :base-opts {:wal? true
                             :ha-mode :consensus-lease
                             :db-identity "db-test"
                             :ha-control-plane
                             {:backend :sofa-jraft
                              :group-id "test-group"
                              :voters []}}
                 :node-ha-opt-overrides
                 {"n1" {:ha-lease-timeout-ms 1234}}}
        node    {:logical-node "n1"
                 :node-id 1
                 :endpoint "127.0.0.1:19000"
                 :peer-id "127.0.0.1:19001"}
        opts    (#'lops/node-ha-open-opts cluster "n1" node)]
    (is (= true (:wal? opts)))
    (is (= :consensus-lease (:ha-mode opts)))
    (is (= 1 (:ha-node-id opts)))
    (is (= "127.0.0.1:19001"
           (get-in opts [:ha-control-plane :local-peer-id])))
    (is (= 1234 (:ha-lease-timeout-ms opts)))))

(deftest transient-ha-open-failure-detects-gap-errors-test
  (is (lcluster/transient-ha-open-failure?
       (ex-info "Txn-log is not enabled for this LMDB"
                {:type :txlog/not-enabled})))
  (is (lcluster/transient-ha-open-failure?
       (ex-info "snapshot bootstrap failed"
                {:error :ha/follower-snapshot-bootstrap-failed
                 :data {:gap-error
                        {:data {:type :txlog/not-enabled}}}})))
  (is (lcluster/transient-ha-open-failure?
       (ex-info "Request to Datalevin server failed: \"HA read admission rejected\""
                {:type :start-sampling
                 :err-data {:error :ha/read-rejected
                            :retryable? true}})))
  (is (not (lcluster/transient-ha-open-failure?
            (ex-info "retryable but unrelated"
                     {:err-data {:error :unrelated
                                 :retryable? true}}))))
  (is (not (lcluster/transient-ha-open-failure?
            (ex-info "not a HA gap" {:error :unrelated})))))

(deftest open-ha-conn-retries-transient-ha-gap-open-failure-test
  (let [attempts (atom 0)]
    (with-redefs [d/create-conn
                  (fn [_uri _schema _opts]
                    (if (= 1 (swap! attempts inc))
                      (throw (ex-info "Txn-log is not enabled for this LMDB"
                                      {:type :txlog/not-enabled}))
                      ::conn))]
      (is (= ::conn
             (#'lcluster/open-ha-conn!
              {:transport-failure? (constantly false)}
              {:endpoint "127.0.0.1:19000"}
              "db"
              nil
              {:wal? true}
              1000)))
      (is (= 2 @attempts)))))

(defn- restart-node-test-cluster
  [node]
  {:db-name "restart-node-test"
   :base-opts
   {:wal? true
    :ha-mode :consensus-lease
    :ha-members [{:node-id (:node-id node)
                  :endpoint (:endpoint node)}]
    :ha-control-plane
    {:backend :sofa-jraft
     :group-id "restart-node-test"
     :voters [{:peer-id (:peer-id node)
               :ha-node-id (:node-id node)
               :promotable? true}]}}
   :node-by-name {"n1" node}
   :node-ha-opt-overrides {}
   :control-backend :sofa-jraft
   :remote? false
   :verbose? false
   :setup-timeout-ms 1000
   :servers {}
   :admin-conns {}
   :live-nodes #{}
   :paused-nodes #{"n1"}
   :paused-node-info {"n1" {:paused? true}}
   :stopped-node-info {"n1" {:stopped? true}}})

(deftest restart-node-keeps-server-live-after-retryable-admin-open-failure-test
  (let [dir                 (u/tmp-dir (str "jepsen-restart-retryable-open-"
                                            (UUID/randomUUID)))
        [endpoint-port
         peer-port]         (reserve-ports 2)
        node                {:logical-node "n1"
                             :node-id 1
                             :port endpoint-port
                             :endpoint (str "127.0.0.1:" endpoint-port)
                             :peer-id (str "127.0.0.1:" peer-port)
                             :root (str dir u/+separator+ "n1")}
        clusters            (atom {::cluster (restart-node-test-cluster node)})
        attempts            (atom 0)]
    (try
      (with-redefs [d/create-conn
                    (fn [& _]
                      (swap! attempts inc)
                      (throw (ex-info "Timeout in making request"
                                      {:phase :open-conn
                                       :timeout-ms 50})))]
        (is (true?
             (lcluster/restart-node!
              {:clusters clusters
               :transport-failure? (constantly true)}
              ::cluster
              "n1")))
        (is (instance? Server (get-in @clusters [::cluster :servers "n1"])))
        (is (nil? (get-in @clusters [::cluster :admin-conns "n1"])))
        (is (contains? (get-in @clusters [::cluster :live-nodes]) "n1"))
        (is (not (contains? (get-in @clusters [::cluster :paused-nodes]) "n1")))
        (is (not (contains? (get-in @clusters [::cluster :paused-node-info]) "n1")))
        (is (not (contains? (get-in @clusters [::cluster :stopped-node-info]) "n1")))
        (is (<= 1 @attempts)))
      (finally
        (when-let [server (get-in @clusters [::cluster :servers "n1"])]
          (srv/stop server))
        (u/delete-files dir)))))

(deftest restart-node-stops-server-after-nonretryable-admin-open-failure-test
  (let [dir                 (u/tmp-dir (str "jepsen-restart-nonretryable-open-"
                                            (UUID/randomUUID)))
        [endpoint-port
         peer-port]         (reserve-ports 2)
        node                {:logical-node "n1"
                             :node-id 1
                             :port endpoint-port
                             :endpoint (str "127.0.0.1:" endpoint-port)
                             :peer-id (str "127.0.0.1:" peer-port)
                             :root (str dir u/+separator+ "n1")}
        clusters            (atom {::cluster (restart-node-test-cluster node)})]
    (try
      (with-redefs [d/create-conn
                    (fn [& _]
                      (throw (ex-info "nonretryable admin open" {:error :bad-config})))]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"nonretryable admin open"
             (lcluster/restart-node!
              {:clusters clusters
               :transport-failure? (constantly false)}
              ::cluster
              "n1")))
        (is (nil? (get-in @clusters [::cluster :servers "n1"])))
        (is (not (contains? (get-in @clusters [::cluster :live-nodes]) "n1"))))
      (finally
        (when-let [server (get-in @clusters [::cluster :servers "n1"])]
          (srv/stop server))
        (u/delete-files dir)))))

(defn- local-ops-test-deps
  [db-name server]
  {:clusters (atom {:test-cluster {:db-name db-name
                                   :servers {"n1" server}}})
   :remote-cluster? (constantly false)})

(deftest local-with-node-kv-store-uses-live-server-store-test
  (let [dir      (u/tmp-dir (str "jepsen-local-with-node-kv-store-"
                                 (UUID/randomUUID)))
        db-name  "local-with-node-kv-store"
        kv-store (d/open-kv dir {:wal? true})
        server   (ha-test-server {db-name {:store kv-store}})]
    (try
      (with-redefs [r/open-kv
                    (fn [& _]
                      (throw (ex-info "remote open-kv should not be used"
                                      {})))]
        (is (identical? kv-store
                        (lops/with-node-kv-store
                         (local-ops-test-deps db-name server)
                         :test-cluster
                         "n1"
                         identity))))
      (finally
        (d/close-kv kv-store)
        (u/delete-files dir)))))

(deftest clear-copy-backup-pins-on-live-store-test
  (let [dir      (u/tmp-dir (str "jepsen-clear-copy-backup-pins-"
                                 (UUID/randomUUID)))
        db-name  "clear-copy-backup-pins"
        kv-store (d/open-kv dir {:wal? true})
        server   (ha-test-server {db-name {:store kv-store}})
        deps     (local-ops-test-deps db-name server)]
    (try
      (i/txlog-pin-backup-floor! kv-store "backup-copy/active" 0)
      (i/txlog-pin-backup-floor! kv-store
                                 "backup-copy/expired"
                                 0
                                 (dec (System/currentTimeMillis)))
      (i/txlog-pin-backup-floor! kv-store "replication/n1" 0)
      (is (= ["backup-copy/active"]
             (lops/copy-backup-pin-ids deps :test-cluster "n1")))
      (is (= {:cleared-pin-ids ["backup-copy/active"]
              :remaining-pin-ids []}
             (lops/clear-copy-backup-pins-on-node!
              deps
              :test-cluster
              "n1")))
      (is (= []
             (lops/copy-backup-pin-ids deps :test-cluster "n1")))
      (is (= ["backup-copy/expired" "replication/n1"]
             (->> (get-in (i/txlog-retention-state kv-store)
                          [:floor-providers :backup :pins])
                  (map :pin-id)
                  sort
                  vec)))
      (finally
        (d/close-kv kv-store)
        (u/delete-files dir)))))

(deftest local-with-node-kv-store-reports-closed-store-test
  (let [error (try
                (lops/with-node-kv-store
                 (local-ops-test-deps "closed-store" nil)
                 :test-cluster
                 "n1"
                 identity)
                nil
                (catch clojure.lang.ExceptionInfo e
                  e))]
    (is (= "Cannot access KV store on unavailable Jepsen node"
           (ex-message error)))
    (is (= :lmdb/closed (:type (ex-data error))))))

(deftest gc-txlog-segments-on-node-allows-expected-skip-test
  (let [db-name "gc-skip-test"
        server  (ha-test-server {db-name {:store ::store}})
        result  {:ok? false
                 :skipped? true
                 :reason :rollback
                 :watermarks {:wal? true}}]
    (with-redefs [i/gc-txlog-segments! (fn
                                          ([_] result)
                                          ([_ _] result))]
      (is (= result
             (lops/gc-txlog-segments-on-node!
              (local-ops-test-deps db-name server)
              :test-cluster
              "n1"))))))

(deftest gc-txlog-segments-on-node-rejects-real-failure-test
  (let [db-name "gc-failure-test"
        server  (ha-test-server {db-name {:store ::store}})
        result  {:ok? false
                 :reason :delete-failed}]
    (with-redefs [i/gc-txlog-segments! (fn
                                          ([_] result)
                                          ([_ _] result))]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Jepsen WAL GC failed"
           (lops/gc-txlog-segments-on-node!
            (local-ops-test-deps db-name server)
            :test-cluster
            "n1"))))))

(defn- start-clojure-child!
  [form]
  (let [cmd [(current-java-bin)
             "-cp"
             (System/getProperty "java.class.path")
             "clojure.main"
             "-e"
             form]
        process-builder (ProcessBuilder. ^java.util.List (mapv str cmd))]
    (.directory process-builder (io/file (System/getProperty "user.dir")))
    (.redirectErrorStream process-builder true)
    (.start process-builder)))

(defn- ha-test-server
  [dbs]
  (srv/->Server (AtomicBoolean. true)
                0
                ""
                0
                nil
                nil
                (ConcurrentLinkedQueue.)
                nil
                nil
                nil
                (ConcurrentHashMap.)
                (doto (ConcurrentHashMap.)
                  (.putAll dbs))))

(defn- assert-local-query-refreshes-ha-read-view!
  [db-state]
  (let [dir        (u/tmp-dir (str "jepsen-local-query-refresh-"
                                   (UUID/randomUUID)))
        db-name    "jepsen-local-query-refresh"
        query      '[:find ?v .
                     :where
                     [?e :register/key 0]
                     [?e :register/value ?v]]
        conn       (d/create-conn dir {:register/key {:db/valueType :db.type/long
                                                      :db/unique :db.unique/identity}
                                       :register/value {:db/valueType :db.type/long}})
        _          (d/transact! conn [{:register/key 0 :register/value 0}])
        stale-db   @conn
        store      (.-store ^DB stale-db)
        server     (ha-test-server {db-name (merge {:store store
                                                    :dt-db stale-db}
                                                   db-state)})
        cluster-id (keyword (str "local-query-refresh-" (UUID/randomUUID)))
        clusters*  @#'local/clusters]
    (swap! clusters* assoc cluster-id {:db-name db-name
                                       :servers {"n1" server}})
    (try
      (is (= 0 (local/local-query cluster-id "n1" query)))
      (d/transact! conn [{:register/key 0 :register/value 1000}])
      (is (= 1000 (local/local-query cluster-id "n1" query)))
      (finally
        (swap! clusters* dissoc cluster-id)
        (d/close conn)
        (u/delete-files dir)))))

(defn- wal-child-overlap-form
  [dir opts ready-path release-path]
  (let [form
        `(do
           (require '[clojure.java.io :as io]
                    '[datalevin.core :as d]
                    '[datalevin.kv :as kv])
           (let [db# (d/open-kv ~dir ~opts)]
             (try
               (d/open-dbi db# "a")
               (spit ~ready-path "ready")
               (loop [elapsed# 0]
                 (cond
                   (.exists (io/file ~release-path))
                   nil

                   (>= elapsed# 5000)
                   (throw (ex-info "timed out waiting for release"
                                   {:elapsed-ms elapsed#}))

                   :else
                   (do
                     (Thread/sleep 25)
                     (recur (+ elapsed# 25)))))
               (d/transact-kv db# [[:put "a" :k2 :v2]])
               (println (pr-str {:status :ok
                                 :lsns (mapv :lsn (kv/open-tx-log db# 1))
                                 :applied-lsn
                                 (get-in (kv/read-commit-marker db#)
                                         [:current :applied-lsn])}))
               (finally
                 (d/close-kv db#)))))]
    (pr-str form)))

(defn- wait-for-file
  [path timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (cond
        (u/file-exists path)
        true

        (< (System/currentTimeMillis) deadline)
        (do
          (Thread/sleep 25)
          (recur))

        :else
        false))))

(deftest wal-multi-process-writable-overlap-test
  (let [dir          (u/tmp-dir (str "jepsen-wal-multi-process-"
                                     (UUID/randomUUID)))
        ready-path   (str dir u/+separator+ "child.ready")
        release-path (str dir u/+separator+ "child.release")
        opts         {:wal? true
                      :wal-commit-marker? true
                      :snapshot-bootstrap-force? false
                      :wal-durability-profile :strict}]
    (try
      (let [db1 (d/open-kv dir opts)]
        (try
          (d/open-dbi db1 "a")
          (let [child (start-clojure-child!
                       (wal-child-overlap-form dir
                                               opts
                                               ready-path
                                               release-path))]
            (try
              (is (wait-for-file ready-path wal-child-ready-timeout-ms))
              (is (= :transacted
                     (d/transact-kv db1 [[:put "a" :k1 :v1]])))
              (spit release-path "go")
              (let [{:keys [ok? result output]}
                    (child-process-result child wal-child-process-timeout-ms)]
                (is ok? output)
                (is (= {:status :ok
                        :lsns [1 2]
                        :applied-lsn 2}
                       result))
                (is (= [1 2]
                       (mapv :lsn (kv/open-tx-log db1 1))))
                (is (= :v1
                       (d/get-value db1 "a" :k1)))
                (is (= :v2
                       (d/get-value db1 "a" :k2)))
                (is (:ok? (kv/verify-commit-marker! db1))))
              (finally
                (when-not (u/file-exists release-path)
                  (spit release-path "go"))
                (child-process-result child 1000))))
          (finally
            (d/close-kv db1))))
      (let [db2 (d/open-kv dir opts)]
        (try
          (d/open-dbi db2 "a")
          (is (= :v1
                 (d/get-value db2 "a" :k1)))
          (is (= :v2
                 (d/get-value db2 "a" :k2)))
          (finally
            (d/close-kv db2))))
      (finally
        (u/delete-files dir)))))

(deftest expected-disruption-write-failure-matches-transport-errors-test
  (let [active-test {:datalevin/nemesis-faults [:clock-skew-pause
                                                :leader-failover]}
        inactive-test {:datalevin/nemesis-faults []}
        transport-error "Unable to connect to server: Connection refused"
        control-timeout "Request to Datalevin server failed: \"HA control command timed out\""
        commit-confirmation-failure
        "Request to Datalevin server failed: \"HA write commit confirmation failed\""
        missing-transaction
        (str "Request to Datalevin server failed: \"Error Handling "
             "with-transaction message:Cannot confirm a transaction "
             "that is no longer active\"")
        owner-mismatch
        (str "Request to Datalevin server failed: \"Error Handling "
             "with-transaction message:Active transaction belongs to "
             "another client\"")
        write-indeterminate
        {:message "Request to Datalevin server failed"
         :err-data {:error :ha/write-indeterminate
                    :indeterminate? true
                    :reason :transaction-owner-mismatch}}]
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   {:datalevin/nemesis-faults [:node-kill]}
                   "Request to Datalevin server failed: \"HA write admission rejected\""))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   active-test
                   transport-error))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   active-test
                   {:error transport-error}))))
    (is (false? (boolean
                  (local/expected-disruption-write-failure?
                    inactive-test
                    transport-error))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   active-test
                   control-timeout))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   active-test
                   {:error commit-confirmation-failure}))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                  active-test
                  missing-transaction))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                  active-test
                  owner-mismatch))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   active-test
                   write-indeterminate))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   active-test
                   (ex-info "indeterminate close"
                            (:err-data write-indeterminate))))))
    (is (false? (boolean
                  (local/expected-disruption-write-failure?
                    inactive-test
                    control-timeout))))
    (is (false? (boolean
                  (local/expected-disruption-write-failure?
                    inactive-test
                    write-indeterminate))))
    (is (false? (boolean
                  (local/expected-disruption-write-failure?
                   inactive-test
                   missing-transaction))))))

(deftest local-query-uses-server-ha-read-view-test
  (assert-local-query-refreshes-ha-read-view!
   {:ha-role :follower
    :ha-authority (Object.)}))

(deftest local-query-uses-server-ha-read-view-without-authority-test
  (assert-local-query-refreshes-ha-read-view!
   {:ha-role :follower}))

(deftest local-query-uses-server-ha-read-view-for-leader-test
  (assert-local-query-refreshes-ha-read-view!
   {:ha-role :leader
    :ha-authority (Object.)}))

(deftest local-query-returns-unavailable-for-stopped-node-test
  (let [cluster-id (keyword (str "local-query-stopped-" (UUID/randomUUID)))
        query      '[:find ?e .
                     :where
                     [?e :db/ident ?ident]]
        clusters*  @#'local/clusters]
    (swap! clusters* assoc cluster-id {:db-name "jepsen-local-query-stopped"
                                       :servers {"n1" nil}})
    (try
      (is (= ::local/unavailable
             (local/local-query cluster-id "n1" query)))
      (finally
        (swap! clusters* dissoc cluster-id)))))
