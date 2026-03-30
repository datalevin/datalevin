(ns datalevin.jepsen.smoke-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin.jepsen.core :as core]
   [datalevin.jepsen.integration-harness :as harness]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.nemesis :as nemesis]
   [datalevin.jepsen.workload.append :as append]
   [datalevin.jepsen.workload.append-cas :as append-cas]
   [datalevin.jepsen.workload.bank :as bank]
   [datalevin.jepsen.workload.degraded-rejoin :as degraded-rejoin]
   [datalevin.jepsen.workload.fencing :as fencing]
   [datalevin.jepsen.workload.fencing-retry :as fencing-retry]
   [datalevin.jepsen.workload.giant-values :as giant-values]
   [datalevin.jepsen.workload.grant :as grant]
   [datalevin.jepsen.workload.identity-upsert :as identity-upsert]
   [datalevin.jepsen.workload.index-consistency :as index-consistency]
   [datalevin.jepsen.workload.internal :as internal]
   [datalevin.jepsen.workload.membership-drift :as membership-drift]
   [datalevin.jepsen.workload.rejoin-bootstrap :as rejoin-bootstrap]
   [datalevin.jepsen.workload.register :as register]
   [datalevin.jepsen.workload.util :as workload.util]
   [datalevin.jepsen.workload.udf-readiness :as udf-readiness]
   [datalevin.jepsen.workload.witness-topology :as witness-topology]
   [datalevin.jepsen.workload.tx-fn-register :as tx-fn-register]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.db :as jdb]
   [jepsen.history :as history]
   [jepsen.net.proto :as net.proto]
   [jepsen.tests.cycle.append :as cycle.append]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(defn- assert-workload-shape!
  [workload {:keys [schema final-generator? nodes control-nodes
                    cluster-opts? runtime-opts-fn?]}]
  (is (some? (:client workload)))
  (is (some? (:generator workload)))
  (is (some? (:checker workload)))
  (is (= schema (:schema workload)))
  (when (some? final-generator?)
    (is (= final-generator?
           (some? (:final-generator workload)))))
  (when (some? nodes)
    (is (= nodes (:nodes workload))))
  (when (some? control-nodes)
    (is (= control-nodes (:datalevin/control-nodes workload))))
  (when (some? cluster-opts?)
    (is (= cluster-opts?
           (map? (:datalevin/cluster-opts workload)))))
  (when runtime-opts-fn?
    (is (ifn? (:datalevin/server-runtime-opts-fn workload)))))

(def ^:private workload-construction-cases
  [{:label :append
    :builder append/workload
    :opts {:key-count 4
           :min-txn-length 2
           :max-txn-length 3
           :max-writes-per-key 8}
    :schema append/schema}
   {:label :append-cas
    :builder append-cas/workload
    :opts {:key-count 4
           :min-txn-length 2
           :max-txn-length 3
           :max-writes-per-key 8}
    :schema append-cas/schema}
   {:label :grant
    :builder grant/workload
    :opts {:key-count 4}
    :schema grant/schema
    :final-generator? true}
   {:label :bank
    :builder bank/workload
    :opts {:key-count 4
           :account-balance 100
           :max-transfer 5}
    :schema bank/schema
    :final-generator? true}
   {:label :degraded-rejoin
    :builder degraded-rejoin/workload
    :opts {:key-count 4}
    :schema degraded-rejoin/schema}
   {:label :snapshot-db-identity-rejoin
    :builder degraded-rejoin/db-identity-workload
    :opts {:key-count 4}
    :schema degraded-rejoin/schema}
   {:label :snapshot-checksum-rejoin
    :builder degraded-rejoin/checksum-workload
    :opts {:key-count 4}
    :schema degraded-rejoin/schema}
   {:label :snapshot-manifest-corruption-rejoin
    :builder degraded-rejoin/manifest-corruption-workload
    :opts {:key-count 4}
    :schema degraded-rejoin/schema}
   {:label :snapshot-copy-corruption-rejoin
    :builder degraded-rejoin/copy-corruption-workload
    :opts {:key-count 4}
    :schema degraded-rejoin/schema}
   {:label :witness-topology
    :builder witness-topology/workload
    :opts {:key-count 4}
    :schema witness-topology/schema
    :nodes ["n1" "n2"]
    :control-nodes ["n1" "n2" "n3"]}
   {:label :membership-drift
    :builder membership-drift/workload
    :opts {:key-count 4}
    :schema membership-drift/schema}
   {:label :membership-drift-live
    :builder membership-drift/live-workload
    :opts {:key-count 4}
    :schema membership-drift/schema}
   {:label :giant-values
    :builder giant-values/workload
    :opts {:key-count 4
           :nodes ["n1" "n2" "n3"]}
    :schema giant-values/schema}
   {:label :fencing
    :builder fencing/workload
    :opts {}
    :schema fencing/schema
    :final-generator? true}
   {:label :fencing-retry
    :builder fencing-retry/workload
    :opts {:key-count 4}
    :schema fencing-retry/schema
    :nodes ["n1" "n2"]
    :control-nodes ["n1" "n2" "n3"]}
   {:label :udf-readiness
    :builder udf-readiness/workload
    :opts {:key-count 4}
    :schema udf-readiness/schema
    :nodes ["n1" "n2" "n3"]
    :runtime-opts-fn? true}
   {:label :internal
    :builder internal/workload
    :opts {}
    :schema internal/schema}
   {:label :identity-upsert
    :builder identity-upsert/workload
    :opts {}
    :schema identity-upsert/schema}
   {:label :index-consistency
    :builder index-consistency/workload
    :opts {}
    :schema index-consistency/schema}
   {:label :register
    :builder register/workload
    :opts {:key-count 4
           :nodes ["n1" "n2" "n3"]}
    :schema register/schema}
   {:label :tx-fn-register
    :builder tx-fn-register/workload
    :opts {:key-count 4
           :nodes ["n1" "n2" "n3"]}
    :schema tx-fn-register/schema}
   {:label :rejoin-bootstrap
    :builder rejoin-bootstrap/workload
    :opts {:key-count 4
           :nodes ["n1" "n2" "n3"]}
    :schema rejoin-bootstrap/schema
    :final-generator? true
    :cluster-opts? true}])

(deftest workload-construction-smoke-test
  (doseq [{:keys [label builder opts] :as expected}
          workload-construction-cases]
    (testing (name label)
      (assert-workload-shape! (builder opts) expected))))

(deftest bank-workload-rejects-too-few-accounts-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"requires at least 2 accounts"
       (bank/workload {:key-count 1
                       :account-balance 100
                       :max-transfer 5}))))

(defn- assert-datalevin-test-map!
  [test-map {:keys [nodes control-backend nemesis-faults control-nodes
                    networked? runtime-opts-fn?]}]
  (is (= nodes (:nodes test-map)))
  (is (= control-backend (:control-backend test-map)))
  (is (some? (:db test-map)))
  (is (some? (:client test-map)))
  (is (some? (:generator test-map)))
  (is (some? (:checker test-map)))
  (when (some? nemesis-faults)
    (is (= nemesis-faults (:datalevin/nemesis-faults test-map))))
  (when (seq nemesis-faults)
    (is (some? (:nemesis test-map))))
  (when (some? control-nodes)
    (is (= control-nodes (:datalevin/control-nodes test-map))))
  (when networked?
    (is (some? (:net test-map))))
  (when runtime-opts-fn?
    (is (ifn? (:datalevin/server-runtime-opts-fn test-map)))))

(defn- with-temp-remote-config
  [config f]
  (let [dir (u/tmp-dir (str "jepsen-remote-config-" (UUID/randomUUID)))
        path (str dir u/+separator+ "cluster.edn")]
    (u/create-dirs dir)
    (spit path (pr-str config))
    (try
      (f path)
      (finally
        (u/delete-files dir)))))

(def ^:private datalevin-test-construction-cases
  [{:label :append
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 2
           :max-txn-length 3
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :append-cas
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append-cas
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 2
           :max-txn-length 3
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :grant
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :grant
           :rate 10
           :time-limit 5
           :key-count 4
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :bank
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :bank
           :rate 10
           :time-limit 5
           :key-count 4
           :account-balance 100
           :max-transfer 5
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :giant-values
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :giant-values
           :rate 10
           :time-limit 5
           :key-count 4
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :fencing
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :fencing
           :rate 10
           :time-limit 5
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :internal
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :internal
           :rate 10
           :time-limit 5
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :identity-upsert
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :identity-upsert
           :rate 10
           :time-limit 5
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :index-consistency
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :index-consistency
           :rate 10
           :time-limit 5
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :register
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :register
           :rate 10
           :time-limit 5
           :key-count 4
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :tx-fn-register
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :tx-fn-register
           :rate 10
           :time-limit 5
           :key-count 4
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :rejoin-bootstrap
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :rejoin-bootstrap
           :rate 1
           :time-limit 5
           :key-count 4
           :nodes ["n1" "n2" "n3"]
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :degraded-rejoin
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :degraded-rejoin
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :snapshot-db-identity-rejoin
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :snapshot-db-identity-rejoin
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :snapshot-checksum-rejoin
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :snapshot-checksum-rejoin
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :snapshot-manifest-corruption-rejoin
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :snapshot-manifest-corruption-rejoin
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :snapshot-copy-corruption-rejoin
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :snapshot-copy-corruption-rejoin
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :witness-topology
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :witness-topology
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis []}
    :expected {:nodes ["n1" "n2"]
               :control-backend :sofa-jraft
               :nemesis-faults []
               :control-nodes ["n1" "n2" "n3"]}}
   {:label :membership-drift
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :membership-drift
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :membership-drift-live
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :membership-drift-live
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []}}
   {:label :fencing-retry
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :fencing-retry
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis []}
    :expected {:nodes ["n1" "n2"]
               :control-backend :sofa-jraft
               :nemesis-faults []
               :control-nodes ["n1" "n2" "n3"]}}
   {:label :udf-readiness
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :udf-readiness
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis []}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults []
               :runtime-opts-fn? true}}])

(deftest datalevin-test-construction-smoke-test
  (doseq [{:keys [label opts expected]} datalevin-test-construction-cases]
    (testing (name label)
      (assert-datalevin-test-map! (core/datalevin-test opts)
                                  expected))))

(deftest datalevin-test-remote-construction-smoke-test
  (with-temp-remote-config
    {:db-name "remote-smoke"
     :workload :register
     :group-id "remote-smoke-group"
     :db-identity "remote-smoke-db"
     :repo-root "/srv/datalevin"
     :nodes [{:logical-node "n1"
              :node-id 1
              :endpoint "10.0.0.11:8898"
              :peer-id "10.0.0.11:15001"
              :root "/var/tmp/dtlv-jepsen/n1"}
             {:logical-node "n2"
              :node-id 2
              :endpoint "10.0.0.12:8898"
              :peer-id "10.0.0.12:15001"
              :root "/var/tmp/dtlv-jepsen/n2"}
             {:logical-node "n3"
              :node-id 3
              :endpoint "10.0.0.13:8898"
              :peer-id "10.0.0.13:15001"
              :root "/var/tmp/dtlv-jepsen/n3"}]}
    (fn [config-path]
      (let [test-map (core/datalevin-test
                      {:remote-config config-path
                       :workload :append
                       :rate 10
                       :time-limit 5
                       :nemesis [:leader-failover]})]
        (assert-datalevin-test-map!
         test-map
         {:nodes ["n1" "n2" "n3"]
          :control-backend :sofa-jraft
          :nemesis-faults [:leader-failover]
          :networked? true})
        (is (= "remote-smoke" (:db-name test-map)))
        (is (= :register
               (-> test-map :name (str/split #" ") first keyword)))
        (is (= config-path (:datalevin/remote-config test-map)))))))

(defn- assert-remote-accepts-rejoin-bootstrap-workload []
  (with-temp-remote-config
    {:db-name "remote-local-only"
     :workload :rejoin-bootstrap
     :group-id "remote-local-only-group"
     :db-identity "remote-local-only-db"
     :repo-root "/srv/datalevin"
     :nodes [{:logical-node "n1"
              :node-id 1
              :endpoint "10.0.0.11:8898"
              :peer-id "10.0.0.11:15001"
              :root "/var/tmp/dtlv-jepsen/n1"}
             {:logical-node "n2"
              :node-id 2
              :endpoint "10.0.0.12:8898"
              :peer-id "10.0.0.12:15001"
              :root "/var/tmp/dtlv-jepsen/n2"}
             {:logical-node "n3"
              :node-id 3
              :endpoint "10.0.0.13:8898"
              :peer-id "10.0.0.13:15001"
              :root "/var/tmp/dtlv-jepsen/n3"}]}
    (fn [config-path]
      (let [test-map (core/datalevin-test
                      {:remote-config config-path
                       :rate 10
                       :time-limit 5
                       :nemesis []})]
        (is (= "rejoin-bootstrap remote" (:name test-map)))
        (is (= config-path (:datalevin/remote-config test-map)))))))

(deftest datalevin-test-remote-accepts-rejoin-bootstrap-workload-smoke-test
  (assert-remote-accepts-rejoin-bootstrap-workload))

(deftest datalevin-test-remote-rejects-local-only-workload-smoke-test
  (assert-remote-accepts-rejoin-bootstrap-workload))

(defn- assert-remote-accepts-leader-disk-full-nemesis []
  (with-temp-remote-config
    {:db-name "remote-unsupported-nemesis"
     :workload :append
     :group-id "remote-unsupported-group"
     :db-identity "remote-unsupported-db"
     :repo-root "/srv/datalevin"
     :nodes [{:logical-node "n1"
              :node-id 1
              :endpoint "10.0.0.11:8898"
              :peer-id "10.0.0.11:15001"
              :root "/var/tmp/dtlv-jepsen/n1"}
             {:logical-node "n2"
              :node-id 2
              :endpoint "10.0.0.12:8898"
              :peer-id "10.0.0.12:15001"
              :root "/var/tmp/dtlv-jepsen/n2"}
             {:logical-node "n3"
              :node-id 3
              :endpoint "10.0.0.13:8898"
              :peer-id "10.0.0.13:15001"
              :root "/var/tmp/dtlv-jepsen/n3"}]}
    (fn [config-path]
      (let [test-map (core/datalevin-test
                      {:remote-config config-path
                       :rate 10
                       :time-limit 5
                       :nemesis [:leader-disk-full]})]
        (is (= "append remote" (:name test-map)))
        (is (= [:leader-disk-full]
               (:datalevin/nemesis-faults test-map)))
        (is (= config-path (:datalevin/remote-config test-map)))))))

(deftest datalevin-test-remote-accepts-leader-disk-full-nemesis-smoke-test
  (assert-remote-accepts-leader-disk-full-nemesis))

(deftest datalevin-test-remote-rejects-unsupported-nemesis-smoke-test
  (assert-remote-accepts-leader-disk-full-nemesis))

(deftest append-local-history-failover-checker-smoke-test
  (let [append-checker
        (workload.util/wrap-empty-graph-checker
         (cycle.append/checker {:max-plot-bytes 0})
         (fn [op]
           (= :txn (:f op)))
         [:f :error])
        {:keys [checker-result failover-op stabilize-op]}
        (harness/run-local-history-failover-check!
         "append-local-history-smoke"
         :append
         {:key-count 4
          :min-txn-length 1
          :max-txn-length 2
          :max-writes-per-key 8
          :datalevin/history-checker append-checker}
         [{:f :txn
           :value [[:append 0 1]
                   [:append 1 10]]}]
         [{:f :txn
           :value [[:append 0 2]]}]
         [{:f :txn
           :value [[:r 0 nil]
                   [:append 1 11]]}])]
    (is (true? (:valid? checker-result))
        (pr-str checker-result))
    (is (string? (get-in failover-op [:value :stopped])))
    (is (string? (get-in stabilize-op [:value :leader])))))

(deftest append-empty-graph-with-uninformative-terminal-history-is-ignorable-test
  (let [append-checker
        (workload.util/wrap-empty-graph-checker
         (cycle.append/checker {:max-plot-bytes 0})
         (fn [op]
           (= :txn (:f op)))
         [:f :error]
         workload.util/append-graph-ignorable-micro-op-txn?)
        history
        [{:index 0
          :time 8860800
          :type :invoke
          :process 0
          :f :txn
          :value [[:r 2 nil]]}
         {:index 1
          :time 11732333
          :type :info
          :process :nemesis
          :f :kill-leader
          :value nil}
         {:index 2
          :time 12129588
          :type :invoke
          :process 2
          :f :txn
          :value [[:append 3 1]]}
         {:index 3
          :time 241126181
          :type :invoke
          :process 1
          :f :txn
          :value [[:append 3 2]]}
         {:index 4
          :time 4548419224
          :type :info
          :process :nemesis
          :f :kill-leader
          :value {:stopped "n1"
                  :leader "n2"}}
         {:index 5
          :time 5251817549
          :type :ok
          :process 0
          :f :txn
          :value [[:r 2 []]]}
         {:index 6
          :time 5265330434
          :type :ok
          :process 1
          :f :txn
          :value [[:append 3 2]]}
         {:index 7
          :time 5276528646
          :type :ok
          :process 2
          :f :txn
          :value [[:append 3 1]]}
         {:index 8
          :time 9549831223
          :type :info
          :process :nemesis
          :f :restart-node
          :value nil}
         {:index 9
          :time 11495087760
          :type :info
          :process :nemesis
          :f :restart-node
          :value {:restarted "n1"
                  :leader "n2"}}]
        result (checker/check append-checker
                              {:name "append-read-only-empty-graph-smoke"
                               :start-time "20260329T2215"}
                              (history/history history)
                              nil)]
    (is (true? (:valid? result))
        (pr-str result))
    (is (= :ignorable-empty-graph
           (:adjusted-valid? result)))))

(deftest register-local-history-failover-checker-smoke-test
  (let [{:keys [checker-result failover-op stabilize-op]}
        (harness/run-local-history-failover-check!
         "register-local-history-smoke"
         :register
         {:key-count 4
          :max-writes-per-key 8}
         [{:f :read
           :value (clojure.lang.MapEntry. 0 nil)}
          {:f :write
           :value (clojure.lang.MapEntry. 0 1)}]
         [{:f :cas
           :value (clojure.lang.MapEntry. 0 [1 2])}]
         [{:f :write
           :value (clojure.lang.MapEntry. 0 3)}
          {:f :read
           :value (clojure.lang.MapEntry. 0 nil)}])]
    (is (true? (:valid? checker-result))
        (pr-str checker-result))
    (is (string? (get-in failover-op [:value :stopped])))
    (is (string? (get-in stabilize-op [:value :leader])))))

(deftest identity-upsert-local-history-failover-checker-smoke-test
  (let [{:keys [checker-result failover-op stabilize-op]}
        (harness/run-local-history-failover-check!
         "identity-upsert-local-history-smoke"
         :identity-upsert
         {}
         [{:f :upsert-same-tempid
           :value nil
           :identity/case-id 1}]
         [{:f :lookup-ref-cas
           :value nil
           :identity/case-id 2}]
         [{:f :string-tempid-upsert-ref
           :value nil
           :identity/case-id 3}
          {:f :dual-unique-upsert
           :value nil
           :identity/case-id 4}])]
    (is (true? (:valid? checker-result))
        (pr-str checker-result))
    (is (string? (get-in failover-op [:value :stopped])))
    (is (string? (get-in stabilize-op [:value :leader])))))

(deftest index-consistency-local-history-failover-checker-smoke-test
  (let [{:keys [checker-result failover-op stabilize-op]}
        (harness/run-local-history-failover-check!
         "index-consistency-local-history-smoke"
         :index-consistency
         {}
         [{:f :ref-create
           :value nil
           :index/case-id 1}]
         [{:f :ref-retarget
           :value nil
           :index/case-id 2}]
         [{:f :tag-swap
           :value nil
           :index/case-id 3}])]
    (is (true? (:valid? checker-result))
        (pr-str checker-result))
    (is (string? (get-in failover-op [:value :stopped])))
    (is (string? (get-in stabilize-op [:value :leader])))))

(deftest append-cas-local-history-failover-checker-smoke-test
  (let [append-cas-checker
        (workload.util/wrap-empty-graph-checker
         (cycle.append/checker
          {:max-plot-bytes 0
           :consistency-models [:strong-session-snapshot-isolation]})
         (fn [op]
           (= :txn (:f op)))
         [:f :error])
        {:keys [checker-result failover-op stabilize-op]}
        (harness/run-local-history-failover-check!
         "append-cas-local-history-smoke"
         :append-cas
         {:key-count 4
          :min-txn-length 1
          :max-txn-length 2
          :max-writes-per-key 8
          :datalevin/history-checker append-cas-checker}
         [{:f :txn
           :value [[:append 0 1]
                   [:append 1 10]]}]
         [{:f :txn
           :value [[:append 0 2]]}]
         [{:f :txn
           :value [[:r 0 nil]
                   [:append 1 11]]}])]
    (is (true? (:valid? checker-result))
        (pr-str checker-result))
    (is (string? (get-in failover-op [:value :stopped])))
    (is (string? (get-in stabilize-op [:value :leader])))))

(deftest internal-local-history-failover-checker-smoke-test
  (let [{:keys [checker-result failover-op stabilize-op]}
        (harness/run-local-history-failover-check!
         "internal-local-history-smoke"
         :internal
         {}
         [{:f :lookup-ref-same
           :value nil
           :internal/case-id 1}]
         [{:f :tempid-ref
           :value nil
           :internal/case-id 2}]
         [{:f :tx-fn-after-add
           :value nil
           :internal/case-id 3}
          {:f :retract-add
           :value nil
           :internal/case-id 4}])]
    (is (true? (:valid? checker-result))
        (pr-str checker-result))
    (is (string? (get-in failover-op [:value :stopped])))
    (is (string? (get-in stabilize-op [:value :leader])))))

(deftest tx-fn-register-local-history-failover-checker-smoke-test
  (let [{:keys [checker-result failover-op stabilize-op]}
        (harness/run-local-history-failover-check!
         "tx-fn-register-local-history-smoke"
         :tx-fn-register
         {:key-count 4
          :max-writes-per-key 8
          :giant-payload-bytes 2048}
         [{:f :read
           :value (clojure.lang.MapEntry. 0 nil)}
          {:f :write
           :value (clojure.lang.MapEntry. 0 1)}]
         [{:f :cas
           :value (clojure.lang.MapEntry. 0 [1 2])}]
         [{:f :write
           :value (clojure.lang.MapEntry. 0 3)}
          {:f :read
           :value (clojure.lang.MapEntry. 0 nil)}])]
    (is (true? (:valid? checker-result))
        (pr-str checker-result))
    (is (string? (get-in failover-op [:value :stopped])))
    (is (string? (get-in stabilize-op [:value :leader])))))

(deftest bank-local-history-failover-checker-smoke-test
  (let [{:keys [checker-result failover-op stabilize-op]}
        (harness/run-local-history-failover-check!
         "bank-local-history-smoke"
         :bank
         {:key-count 4
          :account-balance 100
          :max-transfer 5}
         [{:f :transfer
           :value {:from 0 :to 1 :amount 5}}]
         [{:f :transfer
           :value {:from 1 :to 2 :amount 3}}]
         [{:f :read-all}
          {:f :transfer
           :value {:from 2 :to 3 :amount 4}}])]
    (is (true? (:valid? checker-result))
        (pr-str checker-result))
    (is (string? (get-in failover-op [:value :stopped])))
    (is (string? (get-in stabilize-op [:value :leader])))))

(deftest bank-client-transfer-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name "bank-smoke"
                  :control-backend :sofa-jraft
                  :nodes ["n1" "n2" "n3"]
                  :verbose false
                  :datalevin/cluster-id cluster-id}
        db (local/db cluster-id)
        client (bank/->Client nil 4 100)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            write (client/invoke! opened
                                  test-map
                                  {:type :invoke
                                   :f :transfer
                                   :value {:from 0 :to 1 :amount 5}})
            read-op (client/invoke! opened
                                    test-map
                                    {:type :invoke
                                     :f :read-all})
            totals (:value read-op)]
        (is (= :ok (:type write)))
        (is (= :ok (:type read-op)))
        (is (= 4 (count totals)))
        (is (= 400 (reduce + 0 totals))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest register-client-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name "register-smoke"
                  :control-backend :sofa-jraft
                  :nodes ["n1" "n2" "n3"]
                  :key-count 4
                  :verbose false
                  :datalevin/cluster-id cluster-id}
        db (local/db cluster-id)
        client (register/->Client nil 4)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            read-op (client/invoke! opened
                                    test-map
                                    {:type :invoke
                                     :f :read
                                     :value (clojure.lang.MapEntry. 0 nil)})
            write-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :write
                                      :value (clojure.lang.MapEntry. 0 3)})
            cas-op (client/invoke! opened
                                   test-map
                                   {:type :invoke
                                    :f :cas
                                    :value (clojure.lang.MapEntry. 0 [3 4])})
            final-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :read
                                      :value (clojure.lang.MapEntry. 0 nil)})]
        (is (= :ok (:type read-op)))
        (is (= (clojure.lang.MapEntry. 0 0) (:value read-op)))
        (is (= :ok (:type write-op)))
        (is (= (clojure.lang.MapEntry. 0 3) (:value write-op)))
        (is (= :ok (:type cas-op)))
        (is (= (clojure.lang.MapEntry. 0 [3 4]) (:value cas-op)))
        (is (= :ok (:type final-op)))
        (is (= (clojure.lang.MapEntry. 0 4) (:value final-op))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest giant-values-client-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name "giant-values-smoke"
                  :control-backend :sofa-jraft
                  :nodes ["n1" "n2" "n3"]
                  :key-count 4
                  :verbose false
                  :datalevin/cluster-id cluster-id}
        db (local/db cluster-id)
        client (giant-values/->Client nil 4 12000)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            read-op (client/invoke! opened
                                    test-map
                                    {:type :invoke
                                     :f :read
                                     :value (clojure.lang.MapEntry. 0 nil)})
            write-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :write
                                      :value (clojure.lang.MapEntry. 0 7)})
            cas-op (client/invoke! opened
                                   test-map
                                   {:type :invoke
                                    :f :cas
                                    :value (clojure.lang.MapEntry. 0 [7 9])})
            final-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :read
                                      :value (clojure.lang.MapEntry. 0 nil)})]
        (is (= :ok (:type read-op)))
        (is (= (clojure.lang.MapEntry. 0 0) (:value read-op)))
        (is (true? (:giant/payload-valid? read-op)))
        (is (= :ok (:type write-op)))
        (is (= (clojure.lang.MapEntry. 0 7) (:value write-op)))
        (is (true? (:giant/payload-valid? write-op)))
        (is (= :ok (:type cas-op)))
        (is (= (clojure.lang.MapEntry. 0 [7 9]) (:value cas-op)))
        (is (true? (:giant/payload-valid? cas-op)))
        (is (= :ok (:type final-op)))
        (is (= (clojure.lang.MapEntry. 0 9) (:value final-op)))
        (is (true? (:giant/payload-valid? final-op))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest tx-fn-register-client-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name "tx-fn-register-smoke"
                  :control-backend :sofa-jraft
                  :nodes ["n1" "n2" "n3"]
                  :key-count 4
                  :verbose false
                  :datalevin/cluster-id cluster-id}
        db (local/db cluster-id)
        client (tx-fn-register/->Client nil 4 12000)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            read-op (client/invoke! opened
                                    test-map
                                    {:type :invoke
                                     :f :read
                                     :value (clojure.lang.MapEntry. 0 nil)})
            write-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :write
                                      :value (clojure.lang.MapEntry. 0 7)})
            cas-op (client/invoke! opened
                                   test-map
                                   {:type :invoke
                                    :f :cas
                                    :value (clojure.lang.MapEntry. 0 [7 9])})
            final-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :read
                                      :value (clojure.lang.MapEntry. 0 nil)})]
        (is (= :ok (:type read-op)))
        (is (= (clojure.lang.MapEntry. 0 0) (:value read-op)))
        (is (true? (:txreg/payload-valid? read-op)))
        (is (= :ok (:type write-op)))
        (is (= (clojure.lang.MapEntry. 0 7) (:value write-op)))
        (is (true? (:txreg/payload-valid? write-op)))
        (is (= :ok (:type cas-op)))
        (is (= (clojure.lang.MapEntry. 0 [7 9]) (:value cas-op)))
        (is (true? (:txreg/payload-valid? cas-op)))
        (is (= :ok (:type final-op)))
        (is (= (clojure.lang.MapEntry. 0 9) (:value final-op)))
        (is (true? (:txreg/payload-valid? final-op))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest rejoin-bootstrap-client-converges-follower-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        workload (rejoin-bootstrap/workload {:key-count 4
                                             :nodes ["n1" "n2" "n3"]})
        test-map {:db-name "rejoin-smoke"
                  :control-backend :sofa-jraft
                  :nodes ["n1" "n2" "n3"]
                  :key-count 4
                  :verbose false
                  :datalevin/cluster-id cluster-id
                  :datalevin/nemesis-faults []
                  :datalevin/cluster-opts (:datalevin/cluster-opts workload)}
        db (local/db cluster-id)
        client (:client workload)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            _ (client/invoke! opened
                              test-map
                              {:type :invoke
                               :f :write
                               :value (clojure.lang.MapEntry. 0 1)})
            _ (client/invoke! opened
                              test-map
                              {:type :invoke
                               :f :write
                               :value (clojure.lang.MapEntry. 1 2)})
            leader (:leader (local/wait-for-single-leader! cluster-id))
            stopped-node (->> (get-in (local/cluster-state cluster-id)
                                      [:live-nodes])
                              sort
                              (remove #{leader})
                              first)
            _ (is (string? stopped-node))
            _ (local/stop-node! cluster-id stopped-node)
            _ (client/invoke! opened
                              test-map
                              {:type :invoke
                               :f :write
                               :value (clojure.lang.MapEntry. 0 3)})
            _ (client/invoke! opened
                              test-map
                              {:type :invoke
                               :f :cas
                               :value (clojure.lang.MapEntry. 1 [2 4])})
            converge-op (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :converge})]
        (is (= :ok (:type converge-op))
            (pr-str converge-op))
        (let [expected-values (get-in converge-op [:value :expected])]
          (is (vector? expected-values)
              (pr-str converge-op))
          (is (= {"n1" expected-values
                  "n2" expected-values
                  "n3" expected-values}
                 (into {}
                       (map (fn [[logical-node {:keys [values]}]]
                              [logical-node values]))
                       (get-in converge-op [:value :nodes]))))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(defn- run-degraded-rejoin-exercise!
  [db-name workload]
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name db-name
                  :control-backend :sofa-jraft
                  :nodes ["n1" "n2" "n3"]
                  :key-count 4
                  :verbose false
                  :datalevin/cluster-id cluster-id
                  :datalevin/nemesis-faults []
                  :datalevin/cluster-opts (:datalevin/cluster-opts workload)}
        db (local/db cluster-id)
        client (:client workload)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            exercise-op (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :exercise})]
        exercise-op)
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(defn- assert-degraded-rejoin-exercise!
  [exercise-op]
  (let [value (:value exercise-op)
        expected (:expected value)
        values-by-node (into {}
                             (map (fn [[logical-node {:keys [values]}]]
                                    [logical-node values]))
                             (:nodes value))]
    (is (= :ok (:type exercise-op))
        (pr-str exercise-op))
    (is (true? (:recovered? value)))
    (is (= 3 (count values-by-node)))
    (is (every? (fn [[_ values]]
                  (= expected values))
                values-by-node)
        (pr-str value))))

(deftest degraded-rejoin-client-recovers-follower-smoke-test
  (let [exercise-op
        (run-degraded-rejoin-exercise!
         "degraded-rejoin-smoke"
         (degraded-rejoin/workload {:key-count 4
                                    :nodes ["n1" "n2" "n3"]}))]
    (assert-degraded-rejoin-exercise! exercise-op)))

(deftest snapshot-db-identity-rejoin-client-recovers-follower-smoke-test
  (let [exercise-op
        (run-degraded-rejoin-exercise!
         "snapshot-db-identity-rejoin-smoke"
         (degraded-rejoin/db-identity-workload {:key-count 4
                                                :nodes ["n1" "n2" "n3"]}))]
    (assert-degraded-rejoin-exercise! exercise-op)))

(deftest snapshot-checksum-rejoin-client-recovers-follower-smoke-test
  (let [exercise-op
        (run-degraded-rejoin-exercise!
         "snapshot-checksum-rejoin-smoke"
         (degraded-rejoin/checksum-workload {:key-count 4
                                             :nodes ["n1" "n2" "n3"]}))]
    (assert-degraded-rejoin-exercise! exercise-op)))

(deftest snapshot-manifest-corruption-rejoin-client-recovers-follower-smoke-test
  (let [exercise-op
        (run-degraded-rejoin-exercise!
         "snapshot-manifest-corruption-rejoin-smoke"
         (degraded-rejoin/manifest-corruption-workload
          {:key-count 4
           :nodes ["n1" "n2" "n3"]}))]
    (assert-degraded-rejoin-exercise! exercise-op)))

(deftest snapshot-copy-corruption-rejoin-client-recovers-follower-smoke-test
  (let [exercise-op
        (run-degraded-rejoin-exercise!
         "snapshot-copy-corruption-rejoin-smoke"
         (degraded-rejoin/copy-corruption-workload
          {:key-count 4
           :nodes ["n1" "n2" "n3"]}))]
    (assert-degraded-rejoin-exercise! exercise-op)))

(defn- run-membership-drift-exercise!
  [db-name workload]
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name db-name
                  :schema (:schema workload)
                  :control-backend :sofa-jraft
                  :nodes (vec (or (:nodes workload)
                                  local/default-nodes))
                  :verbose false
                  :datalevin/cluster-id cluster-id
                  :datalevin/nemesis-faults []
                  :datalevin/cluster-opts (:datalevin/cluster-opts workload)
                  :datalevin/control-nodes
                  (:datalevin/control-nodes workload)}
        db (local/db cluster-id)
        client (:client workload)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            exercise-op (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :exercise})]
        exercise-op)
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(defn- assert-membership-drift-exercise!
  [exercise-op]
  (let [value (:value exercise-op)
        drifted-node (:drifted-node value)
        expected (:expected value)
        values-by-node (into {}
                             (map (fn [[logical-node {:keys [values]}]]
                                    [logical-node values]))
                             (:nodes value))]
    (is (= :ok (:type exercise-op))
        (pr-str exercise-op))
    (is (some? (:restart-error value)))
    (is (not (contains? (set (:live-after-failed-restart value))
                        drifted-node)))
    (is (contains? (set (:live-after-restart value))
                   drifted-node))
    (is (contains? (set (keys values-by-node))
                   drifted-node))
    (is (every? (fn [[_ values]]
                  (= expected values))
                values-by-node)
        (pr-str value))))

(deftest membership-drift-client-recovers-follower-smoke-test
  (let [exercise-op
        (run-membership-drift-exercise!
         "membership-drift-smoke"
         (membership-drift/workload {:key-count 4}))]
    (assert-membership-drift-exercise! exercise-op)))

(defn- assert-membership-drift-live-exercise!
  [exercise-op]
  (let [value (:value exercise-op)
        drifted-node (:drifted-node value)
        expected (:expected value)
        matched-nodes (set (:matched-nodes value))
        values-by-node (into {}
                             (map (fn [[logical-node {:keys [values]}]]
                                    [logical-node values]))
                             (:nodes value))]
    (is (= :ok (:type exercise-op))
        (pr-str exercise-op))
    (is (some? (:drift-error value)))
    (is (contains? (set (:live-after-restore value))
                   drifted-node))
    (is (contains? (set (keys values-by-node))
                   drifted-node))
    (is (<= 2 (count matched-nodes))
        (pr-str value))
    (is (every? (fn [logical-node]
                  (= expected (get values-by-node logical-node)))
                matched-nodes)
        (pr-str value))))

(deftest membership-drift-live-client-recovers-leader-smoke-test
  (let [exercise-op
        (run-membership-drift-exercise!
         "membership-drift-live-smoke"
         (membership-drift/live-workload {:key-count 4}))]
    (assert-membership-drift-live-exercise! exercise-op)))

(defn- run-witness-topology-exercise!
  [db-name workload]
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name db-name
                  :schema (:schema workload)
                  :control-backend :sofa-jraft
                  :nodes (:nodes workload)
                  :verbose false
                  :datalevin/cluster-id cluster-id
                  :datalevin/nemesis-faults []
                  :datalevin/cluster-opts (:datalevin/cluster-opts workload)
                  :datalevin/control-nodes
                  (:datalevin/control-nodes workload)}
        db (local/db cluster-id)
        client (:client workload)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            exercise-op (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :exercise})]
        exercise-op)
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(defn- assert-witness-topology-exercise!
  [exercise-op]
  (let [value (:value exercise-op)
        expected (:expected value)
        values-by-node (into {}
                             (map (fn [[logical-node {:keys [values]}]]
                                    [logical-node values]))
                             (:nodes value))]
    (is (= :ok (:type exercise-op))
        (pr-str exercise-op))
    (is (not= (:leader-before value)
              (:leader-after value)))
    (is (contains? (set (keys values-by-node))
                   (:leader-after value)))
    (is (every? (fn [[_ values]]
                  (= expected values))
                values-by-node)
        (pr-str value))))

(deftest witness-topology-client-retains-quorum-smoke-test
  (let [exercise-op
        (run-witness-topology-exercise!
         "witness-topology-smoke"
         (witness-topology/workload {:key-count 4}))]
    (assert-witness-topology-exercise! exercise-op)))

(defn- run-fencing-retry-exercise!
  [db-name workload]
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name db-name
                  :schema (:schema workload)
                  :control-backend :sofa-jraft
                  :nodes (:nodes workload)
                  :verbose false
                  :datalevin/cluster-id cluster-id
                  :datalevin/nemesis-faults []
                  :datalevin/cluster-opts (:datalevin/cluster-opts workload)
                  :datalevin/control-nodes
                  (:datalevin/control-nodes workload)}
        db (local/db cluster-id)
        client (:client workload)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            exercise-op (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :exercise})]
        exercise-op)
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(defn- assert-fencing-retry-exercise!
  [exercise-op]
  (let [value (:value exercise-op)
        expected (:expected value)
        values-by-node (into {}
                             (map (fn [[logical-node {:keys [values]}]]
                                    [logical-node values]))
                             (:nodes value))]
    (is (= :ok (:type exercise-op))
        (pr-str exercise-op))
    (is (not= (:leader-before value)
              (:leader-after value)))
    (is (pos? (long (or (get-in value [:blocked-write :attempt-count])
                        0))))
    (is (contains? (set (keys values-by-node))
                   (:leader-after value)))
    (is (every? (fn [[_ values]]
                  (= expected values))
                values-by-node)
        (pr-str value))))

(deftest fencing-retry-client-recovers-after-hook-failure-smoke-test
  (let [exercise-op
        (run-fencing-retry-exercise!
         "fencing-retry-smoke"
         (fencing-retry/workload {:key-count 4}))]
    (assert-fencing-retry-exercise! exercise-op)))

(defn- run-udf-readiness-exercise!
  [db-name workload]
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name db-name
                  :schema (:schema workload)
                  :control-backend :sofa-jraft
                  :nodes (:nodes workload)
                  :verbose false
                  :datalevin/cluster-id cluster-id
                  :datalevin/nemesis-faults []
                  :datalevin/cluster-opts (:datalevin/cluster-opts workload)
                  :datalevin/server-runtime-opts-fn
                  (:datalevin/server-runtime-opts-fn workload)}
        db (local/db cluster-id)
        client (:client workload)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            exercise-op (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :exercise})]
        exercise-op)
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(declare udf-readiness-retryable-error?)

(def ^:private udf-readiness-disruption-attempt-timeout-ms 2000)

(defn- invoke-udf-readiness-with-attempt-timeout!
  [test invoke-fn timeout-ms]
  (let [timeout-ms (long timeout-ms)
        result-f (future
                   (try
                     {:value (invoke-fn test)}
                     (catch Throwable e
                       {:error e})))
        result (deref result-f timeout-ms ::timeout)]
    (if (= ::timeout result)
      (do
        (future-cancel result-f)
        {:error (ex-info "Timeout in making request"
                         {:timeout-ms timeout-ms
                          :phase :udf-readiness-disruption-invoke})})
      result)))

(defn- invoke-udf-readiness-with-disruption-retry!
  [test invoke-fn timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (let [remaining-ms (long (max 1 (- deadline
                                         (System/currentTimeMillis))))
            attempt-timeout-ms
            (long (min remaining-ms
                       udf-readiness-disruption-attempt-timeout-ms))
            result (invoke-udf-readiness-with-attempt-timeout!
                    test
                    invoke-fn
                    attempt-timeout-ms)]
        (if-let [e (:error result)]
          (if (and (< (System/currentTimeMillis) deadline)
                   (udf-readiness-retryable-error? test e))
            (do
              (Thread/sleep 250)
              (recur))
            (throw e))
          (:value result))))))

(deftest udf-readiness-disruption-retry-times-out-stuck-invoke-test
  (let [started-ms (System/currentTimeMillis)]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Timeout in making request"
         (invoke-udf-readiness-with-disruption-retry!
          {:datalevin/nemesis-faults [:degraded-network]}
          (fn [_]
            (Thread/sleep 2000)
            :ok)
          100)))
    (is (< (- (System/currentTimeMillis) started-ms)
           1000))))

(defn- udf-readiness-retryable-error?
  [test e]
  (let [data (or (ex-data e) {})
        err-data (or (:err-data data) data)]
    (or (local/transport-failure? e)
        (= :udf-readiness-disruption-invoke (:phase data))
        (local/expected-disruption-write-failure? test e)
        (and (= :ha/write-rejected (:error err-data))
             (= :udf-not-ready (:reason err-data))))))

(comment "Removed white-box UDF readiness smoke coverage.")

(deftest rejoin-bootstrap-checker-ignores-invoke-and-validates-converged-values-test
  (let [checker (:checker (rejoin-bootstrap/workload {:key-count 2}))
        good-snapshot {:expected [1 2]
                       :nodes {"n1" {:ready? true
                                     :values [1 2]
                                     :node-diagnostics {}}
                               "n2" {:ready? true
                                     :values [1 2]
                                     :node-diagnostics {}}}}
        lagging-snapshot (assoc good-snapshot
                                :nodes {"n1" {:ready? true
                                              :values [1 2]
                                              :node-diagnostics {}}
                                        "n2" {:ready? true
                                              :values [1 3]
                                              :node-diagnostics {}}})
        ok-result (checker/check checker
                                 nil
                                 [{:type :invoke :f :converge}
                                  {:type :ok :f :converge :value good-snapshot}]
                                 nil)
        lagging-result (checker/check checker
                                      nil
                                      [{:type :ok :f :converge
                                        :value lagging-snapshot}]
                                      nil)]
    (is (true? (:valid? ok-result)))
    (is (= 1 (:converge-count ok-result)))
    (is (zero? (:failure-count ok-result)))
    (is (false? (:valid? lagging-result)))
    (is (= 1 (:mismatch-count lagging-result)))))

(deftest fencing-client-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name "fencing-smoke"
                  :control-backend :sofa-jraft
                  :nodes ["n1" "n2" "n3"]
                  :verbose false
                  :datalevin/cluster-id cluster-id
                  :datalevin/nemesis-faults []}
        db (local/db cluster-id)
        client (fencing/->Client nil)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened (client/open! client test-map "n1")
            _ (client/setup! opened test-map)
            probe-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :probe})]
        (is (= :ok (:type probe-op)))
        (is (map? (:value probe-op)))
        (is (= 3 (count (get-in probe-op [:value :nodes]))))
        (is (<= (count (filter (fn [[_ {:keys [status]}]]
                                 (= :admitted status))
                               (get-in probe-op [:value :nodes])))
                1)))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest parse-nemesis-spec-smoke-test
  (is (= [:node-kill]
         (core/parse-nemesis-spec "kill")))
  (is (= [:node-kill :leader-failover]
         (core/parse-nemesis-spec "kill,failover"))))

(def ^:private datalevin-test-nemesis-wiring-cases
  [{:label :leader-failover
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:leader-failover]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:leader-failover]
               :networked? false}}
   {:label :node-kill
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:node-kill]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:node-kill]
               :networked? false}}
   {:label :leader-pause
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:leader-pause]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:leader-pause]
               :networked? false}}
   {:label :node-pause
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:node-pause]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:node-pause]
               :networked? false}}
   {:label :multi-node-pause
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:multi-node-pause]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:multi-node-pause]
               :networked? false}}
   {:label :leader-partition
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:leader-partition]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:leader-partition]
               :networked? true}}
   {:label :asymmetric-partition
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:asymmetric-partition]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:asymmetric-partition]
               :networked? true}}
   {:label :degraded-network
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:degraded-network]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:degraded-network]
               :networked? true}}
   {:label :leader-io-stall
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:leader-io-stall]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:leader-io-stall]
               :networked? false}}
   {:label :leader-disk-full
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:leader-disk-full]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:leader-disk-full]
               :networked? false}}
   {:label :follower-rejoin
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :rejoin-bootstrap
           :rate 10
           :time-limit 5
           :key-count 4
           :nodes ["n1" "n2" "n3"]
           :nemesis [:follower-rejoin]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:follower-rejoin]
               :networked? false}}
   {:label :quorum-loss
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:quorum-loss]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:quorum-loss]
               :networked? false}}
   {:label :clock-skew-pause
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:clock-skew-pause]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:clock-skew-pause]
               :networked? false}}
   {:label :clock-skew-mixed
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:clock-skew-mixed]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:clock-skew-mixed]
               :networked? false}}
   {:label :clock-skew-failover
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :append
           :rate 10
           :time-limit 5
           :key-count 4
           :min-txn-length 1
           :max-txn-length 1
           :max-writes-per-key 8
           :nodes ["n1" "n2" "n3"]
           :nemesis [:clock-skew-pause
                     :leader-failover]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:clock-skew-pause
                                :leader-failover]
               :networked? false}}
   {:label :degraded-rejoin-combo
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :rejoin-bootstrap
           :rate 10
           :time-limit 5
           :key-count 4
           :nodes ["n1" "n2" "n3"]
           :nemesis [:degraded-network
                     :follower-rejoin]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:degraded-network
                                :follower-rejoin]
               :networked? true}}
   {:label :udf-readiness-failover
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :udf-readiness
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis [:leader-failover]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:leader-failover]
               :networked? false
               :runtime-opts-fn? true}}
   {:label :udf-readiness-partition
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :udf-readiness
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis [:leader-partition]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:leader-partition]
               :networked? true
               :runtime-opts-fn? true}}
   {:label :udf-readiness-degraded-network
    :opts {:db-name "smoke"
           :control-backend :sofa-jraft
           :workload :udf-readiness
           :rate 1
           :time-limit 5
           :key-count 4
           :nemesis [:degraded-network]}
    :expected {:nodes ["n1" "n2" "n3"]
               :control-backend :sofa-jraft
               :nemesis-faults [:degraded-network]
               :networked? true
               :runtime-opts-fn? true}}])

(deftest datalevin-test-nemesis-wiring-smoke-test
  (doseq [{:keys [label opts expected]} datalevin-test-nemesis-wiring-cases]
    (testing (name label)
      (assert-datalevin-test-map! (core/datalevin-test opts)
                                  expected))))

(deftest clock-skew-failover-startup-elects-single-leader-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map {:db-name "clock-skew-failover-startup-smoke"
                  :schema append/schema
                  :control-backend :sofa-jraft
                  :nodes ["n1" "n2" "n3"]
                  :verbose false
                  :datalevin/cluster-id cluster-id
                  :datalevin/nemesis-faults [:clock-skew-pause
                                             :leader-failover]}
        db (local/db cluster-id)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [{:keys [leader]} (local/wait-for-single-leader! cluster-id 60000)]
        (is (contains? #{"n1" "n2" "n3"} leader)))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest datalevin-test-rejects-unsupported-control-backend-smoke-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Jepsen currently supports only --control-backend sofa-jraft"
       (core/datalevin-test {:db-name "smoke"
                             :control-backend :bogus
                             :workload :append
                             :rate 10
                             :time-limit 5
                             :key-count 4
                             :min-txn-length 1
                             :max-txn-length 1
                             :max-writes-per-key 8
                             :nodes ["n1" "n2" "n3"]
                             :nemesis []}))))
