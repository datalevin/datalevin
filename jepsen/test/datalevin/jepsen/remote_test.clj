(ns datalevin.jepsen.remote-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.jepsen.core :as core]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.remote :as remote]
   [datalevin.jepsen.workload.witness-topology :as witness-topology]
   [datalevin.util :as u])
  (:import
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

(deftest datalevin-test-remote-rejects-all-unsupported-workloads-test
  (doseq [workload-name (sort @#'core/remote-unsupported-workloads)]
    (testing (name workload-name)
      (let [e (remote-test-exception
               (unsupported-workload-config workload-name)
               {})]
        (is e)
        (is (re-find #"not yet supported by the remote runner"
                     (ex-message e)))
        (is (= workload-name (:workload (ex-data e))))))))

(deftest datalevin-test-remote-rejects-all-unsupported-nemeses-test
  (doseq [fault (sort @#'core/remote-unsupported-nemeses)]
    (testing (name fault)
      (let [e (remote-test-exception
               (base-remote-config
                {:workload :register
                 :key-count 4})
               {:nemesis [fault]})]
        (is e)
        (is (re-find #"not yet supported by the remote runner"
                     (ex-message e)))
        (is (= fault (:fault (ex-data e))))
        (is (= [fault] (:nemesis (ex-data e))))))))

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

(deftest init-remote-cluster-records-control-only-witness-nodes-test
  (let [cluster-id    (str "remote-witness-" (UUID/randomUUID))
        config        (-> (witness-remote-config)
                          (remote/validate-config! core/workloads))
        workload      (remote/config-workload config core/workloads)
        topology      (remote/workload-topology config workload)
        base-opts     (remote/base-ha-opts config
                                           (:data-nodes topology)
                                           (:control-nodes topology))
        cluster       (#'local/build-remote-cluster-state
                       cluster-id
                       config
                       "/tmp/remote-cluster.edn"
                       {:username "root"}
                       topology
                       workload
                       base-opts
                       45000
                       false)]
    (is (= ["n1" "n2"] (:data-node-names cluster)))
    (is (= ["n1" "n2" "n3"] (:control-node-names cluster)))
    (is (= ["n3"] (:control-only-node-names cluster)))
    (is (= #{"n1" "n2"} (:live-nodes cluster)))
    (is (= "n3" (get-in cluster [:node-by-id 3])))
    (is (= "n3" (get-in cluster [:peer-id->node "10.0.0.13:15001"])))
    (is (= "n3" (get-in cluster [:endpoint->node "10.0.0.13:8898"])))
    (is (false? (get-in cluster [:node-by-name "n3" :promotable?])))))
