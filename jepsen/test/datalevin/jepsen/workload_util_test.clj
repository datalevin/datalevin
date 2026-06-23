(ns datalevin.jepsen.workload-util-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.conn :as conn]
   [datalevin.jepsen.workload.identity-upsert :as identity-upsert]
   [datalevin.jepsen.workload.index-consistency :as index-consistency]
   [datalevin.jepsen.workload.internal :as internal]
   [datalevin.jepsen.workload.rejoin-bootstrap :as rejoin-bootstrap]
   [datalevin.jepsen.workload.register :as register]
   [datalevin.jepsen.workload.tx-fn-register :as tx-fn-register]
   [datalevin.jepsen.workload.util :as workload.util]
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]
   [jepsen.history :as history]))

(deftest assoc-exception-op-classifies-indeterminate-errors-test
  (testing "transport and timeout failures are reported as info"
    (let [op     {:type :invoke :f :write}
          error  (ex-info "Timeout in making request" {:phase :open-conn})
          result (workload.util/assoc-exception-op op error :timeout)]
      (is (= :info (:type result)))
      (is (= :timeout (:error result)))))

  (testing "definite application errors remain fail"
    (let [op     {:type :invoke :f :write}
          error  (ex-info "boom" {:error :unexpected})
          result (workload.util/assoc-exception-op op error :unexpected)]
      (is (= :fail (:type result)))
      (is (= :unexpected (:error result)))))

  (testing "explicit indeterminate HA confirmation errors are reported as info"
    (let [op     {:type :invoke :f :write}
          error  (ex-info "HA write commit confirmation failed"
                          {:error :ha/write-indeterminate
                           :indeterminate? true})
          result (workload.util/assoc-exception-op
                  op
                  error
                  :ha/write-indeterminate)]
      (is (= :info (:type result)))
      (is (= :ha/write-indeterminate (:error result))))))

(deftest exception-detail-sanitizes-history-unsafe-values-test
  (let [opaque (Object.)
        error  (ex-info "boom"
                        {:opaque opaque
                         :nested {:values [1 opaque]}})
        detail (workload.util/exception-detail error)]
    (is (= "boom" (:message detail)))
    (is (= (str opaque) (:opaque detail)))
    (is (= [1 (str opaque)] (get-in detail [:nested :values])))))

(deftest retryable-leader-conn-error-classifies-ha-disruption-test
  (with-redefs [local/transport-failure? (constantly false)]
    (is (true?
          (workload.util/retryable-leader-conn-error?
            (ex-info "Request to Datalevin server failed: \"HA write admission rejected\""
                     {:error :ha/write-rejected
                      :retryable? true}))))
    (is (true?
          (workload.util/retryable-leader-conn-error?
            (ex-info "Request to Datalevin server failed: \"Timed out waiting for durable LSN\""
                     {:err-data {:type :txlog/commit-timeout
                                 :lsn 22
                                 :timeout-ms 5000}}))))
    (is (false?
          (workload.util/retryable-leader-conn-error?
            (ex-info "definite setup failure" {:error :bad-setup}))))))

(deftest with-retrying-leader-conn-retries-transient-ha-error-test
  (let [attempts (atom 0)]
    (with-redefs [local/transport-failure? (constantly false)]
      (binding [workload.util/*with-leader-conn*
                (fn [_test _schema f]
                  (if (= 1 (swap! attempts inc))
                    (throw (ex-info
                             "Request to Datalevin server failed: \"Timed out waiting for durable LSN\""
                             {:err-data {:type :txlog/commit-timeout}}))
                    (f ::conn)))]
        (is (= [:ok ::conn]
               (workload.util/with-retrying-leader-conn
                 {:db-name "retry-test"}
                 {}
                 1000
                 0
                 (fn [conn]
                   [:ok conn]))))
        (is (= 2 @attempts))))))

(deftest rejoin-bootstrap-retries-transient-gap-during-bootstrap-wait-test
  (let [attempts (atom 0)]
    (with-redefs [rejoin-bootstrap/wal-gap-retry-sleep-ms 0]
      (let [result (#'rejoin-bootstrap/with-retrying-bootstrap-gap!
                    1000
                    (fn [timeout-ms]
                      (if (= 1 (swap! attempts inc))
                        (throw (ex-info
                                "Txn-log is not enabled for this LMDB"
                                {:type :txlog/not-enabled}))
                        {:timeout-ms timeout-ms})))]
        (is (<= 1 (long (:timeout-ms result)) 1000)))
      (is (= 2 @attempts)))))

(deftest rejoin-bootstrap-retries-transient-gap-under-remote-error-data-test
  (let [attempts (atom 0)]
    (with-redefs [rejoin-bootstrap/wal-gap-retry-sleep-ms 0]
      (let [result (#'rejoin-bootstrap/with-retrying-bootstrap-gap!
                    1000
                    (fn [timeout-ms]
                      (if (= 1 (swap! attempts inc))
                        (throw (ex-info
                                "Request to Datalevin server failed"
                                {:err-data {:type :txlog/not-enabled}}))
                        {:timeout-ms timeout-ms})))]
        (is (<= 1 (long (:timeout-ms result)) 1000)))
      (is (= 2 @attempts)))))

(deftest rejoin-bootstrap-retries-transient-gap-during-forced-bootstrap-test
  (let [attempts (atom 0)
        test     {:datalevin/cluster-id ::force-bootstrap-retry
                  :db-name "force-bootstrap-retry"}
        result   {:restarted-nodes ["n2"]}]
    (with-redefs [rejoin-bootstrap/wal-gap-retry-sleep-ms 0
                  rejoin-bootstrap/converge-timeout-ms 1000]
      (is (= result
             (#'rejoin-bootstrap/retrying-force-snapshot-bootstrap!
              test
              2
              (fn [actual-test actual-key-count timeout-ms]
                (is (= test actual-test))
                (is (= 2 actual-key-count))
                (is (<= 1 (long timeout-ms) 1000))
                (if (= 1 (swap! attempts inc))
                  (throw (ex-info
                          "Txn-log is not enabled for this LMDB"
                          {:type :txlog/not-enabled}))
                  result)))))
      (is (= 2 @attempts)))))

(deftest rejoin-bootstrap-retries-transient-gap-during-post-bootstrap-converge-test
  (let [attempts         (atom 0)
        test             {:datalevin/cluster-id ::post-bootstrap-converge-retry
                          :db-name "post-bootstrap-converge-retry"}
        bootstrap-result {:restarted-nodes ["n2"]}
        result           {:leader "n1"
                          :caught-up? true
                          :restarted-nodes ["n2"]}]
    (with-redefs [rejoin-bootstrap/wal-gap-retry-sleep-ms 0
                  rejoin-bootstrap/converge-timeout-ms 1000]
      (is (= result
             (#'rejoin-bootstrap/retrying-post-bootstrap-convergence-result
              test
              2
              bootstrap-result
              (fn [actual-test actual-key-count actual-bootstrap timeout-ms]
                (is (= test actual-test))
                (is (= 2 actual-key-count))
                (is (= bootstrap-result actual-bootstrap))
                (is (<= 1 (long timeout-ms) 1000))
                (if (= 1 (swap! attempts inc))
                  (throw (ex-info
                          "Txn-log is not enabled for this LMDB"
                          {:type :txlog/not-enabled}))
                  result)))))
      (is (= 2 @attempts)))))

(deftest register-initialization-retries-transient-ha-error-test
  (let [attempts             (atom 0)
        initialized-clusters @#'register/initialized-clusters]
    (reset! initialized-clusters #{})
    (try
      (with-redefs [local/transport-failure? (constantly false)
                    local/workload-setup-timeout-ms (fn
                                                      ([_cluster-id]
                                                       1000)
                                                      ([_cluster-id _default-timeout-ms]
                                                       1000))
                    local/cluster-state (fn [_cluster-id]
                                          {:live-nodes ["n1"]})
                    local/local-query (fn [_cluster-id _logical-node _query]
                                        [[0 0]])]
        (binding [workload.util/*with-leader-conn*
                  (fn [_test _schema _f]
                    (if (= 1 (swap! attempts inc))
                      (throw (ex-info
                               "Request to Datalevin server failed: \"HA write admission rejected\""
                               {:error :ha/write-rejected
                                :retryable? true}))
                      :seeded))]
          (#'register/ensure-registers-initialized!
           {:datalevin/cluster-id ::register-retry
            :db-name "register-retry"}
           1)))
      (is (= 2 @attempts))
      (is (contains? @initialized-clusters ::register-retry))
      (finally
        (reset! initialized-clusters #{})))))

(deftest history-safe-bounds-unbounded-collections-test
  (let [result (workload.util/history-safe {:values (range)})
        values (:values result)]
    (is (= (vec (range 64)) (subvec values 0 64)))
    (is (true? (:datalevin.jepsen/truncated? (peek values))))
    (is (= :collection-limit (:datalevin.jepsen/reason (peek values))))))

(deftest history-safe-does-not-stringify-depth-limited-collections-test
  (let [result  (workload.util/history-safe {:outer [{:inner (range)}]} 1)
        summary (get-in result [:outer 0])]
    (is (true? (:datalevin.jepsen/truncated? summary)))
    (is (= :max-depth (:datalevin.jepsen/reason summary)))))

(deftest append-graph-ignores-terminal-micro-op-transactions-test
  (testing "read-only terminal transactions are uninformative for append graphs"
    (is (true? (workload.util/append-graph-ignorable-micro-op-txn?
                {:type :info
                 :f :txn
                 :error "Timeout in making request"
                 :value [[:r 3 nil]]}))))

  (testing "append-only terminal transactions are uninformative without reads"
    (is (true? (workload.util/append-graph-ignorable-micro-op-txn?
                {:type :fail
                 :f :txn
                 :error :cas-failed
                 :value [[:append 2 1]]}))))

  (testing "mixed transactions still carry graph information"
    (is (false? (workload.util/append-graph-ignorable-micro-op-txn?
                 {:type :info
                  :f :txn
                  :error "Timeout in making request"
                  :value [[:r 3 nil]
                          [:append 2 1]]})))))

(defn- exact-state-checker-result
  [checker-f expected-value-f op]
  (checker/check
    (checker-f)
    {}
    (history/history
      [(assoc op
              :process 0
              :type :ok
              :value (expected-value-f op))
       (assoc op
              :process 1
              :type :info
              :error "Timeout in making request")])
    nil))

(deftest internal-checker-ignores-indeterminate-ops-test
  (let [op     {:f :lookup-ref-same
                :internal/case-id 1}
        result (exact-state-checker-result
                 #'internal/internal-checker
                 (fn [op]
                   (:value (#'internal/expected-outcome op)))
                 op)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:indeterminate-count result)))))

(deftest identity-upsert-checker-ignores-indeterminate-ops-test
  (let [op     {:f :upsert-same-tempid
                :identity/case-id 1}
        result (exact-state-checker-result
                 #'identity-upsert/identity-upsert-checker
                 #'identity-upsert/expected-states
                 op)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:indeterminate-count result)))))

(deftest identity-upsert-checker-ignores-node-kill-admission-rejections-test
  (let [op     {:f :lookup-ref-intermediate
                :identity/case-id 2}
        result (checker/check
                 (#'identity-upsert/identity-upsert-checker)
                 {:datalevin/nemesis-faults [:node-kill]}
                 (history/history
                  [(assoc op
                          :process 0
                          :type :ok
                          :value (#'identity-upsert/expected-states op))
                   (assoc op
                          :process 1
                          :type :fail
                          :error "Request to Datalevin server failed: \"HA write admission rejected\"")])
                 nil)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:disruption-failure-count result)))))

(deftest identity-upsert-checker-ignores-closed-client-during-failover-test
  (let [op     {:f :string-tempid-upsert-ref
                :identity/case-id 8}
        result (checker/check
                (#'identity-upsert/identity-upsert-checker)
                {:datalevin/nemesis-faults [:clock-skew :leader-failover]}
                (history/history
                 [(assoc op
                         :process 0
                         :type :ok
                         :value (#'identity-upsert/expected-states op))
                  (assoc op
                         :process 1
                         :type :fail
                         :error "This client is closed")])
                nil)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:disruption-failure-count result)))))

(deftest identity-upsert-checker-allows-transient-read-back-mismatch-test
  (let [op      {:f :string-tempid-upsert-ref
                 :identity/case-id 8}
        result  (checker/check
                 (#'identity-upsert/identity-upsert-checker)
                 {}
                 (history/history
                  [(assoc op
                          :process 0
                          :type :ok
                          :value [])
                   {:process 0
                    :type :ok
                    :f :probe
                    :value {8 (#'identity-upsert/expected-final-state op)}}])
                 nil)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 1 (:transient-mismatch-count result)))
    (is (= 0 (:probe-mismatch-count result)))))

(deftest index-consistency-checker-ignores-indeterminate-ops-test
  (let [op     {:f :ref-create
                :index/case-id 1}
        result (exact-state-checker-result
                 #'index-consistency/index-consistency-checker
                 #'index-consistency/expected-states
                 op)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:indeterminate-count result)))))

(deftest index-consistency-checker-allows-transient-read-back-mismatch-test
  (let [op     {:f :ref-create
                :index/case-id 8}
        result (checker/check
                (#'index-consistency/index-consistency-checker)
                {}
                (history/history
                 [(assoc op
                         :process 0
                         :type :ok
                         :value [])
                  {:process 0
                   :type :ok
                   :f :probe
                   :value {8 (#'index-consistency/expected-final-state op)}}])
                nil)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 1 (:transient-mismatch-count result)))
    (is (= 0 (:probe-mismatch-count result)))))

(deftest tx-fn-register-write-reports-requested-value-test
  (let [txs (atom [])]
    (with-redefs [conn/transact! (fn
                                    ([_conn tx]
                                     (swap! txs conj tx)
                                     {:tx-data []})
                                    ([_conn tx _tx-meta]
                                     (swap! txs conj tx)
                                     {:tx-data []}))]
      (let [result (#'tx-fn-register/write-via-tx-fn!
                    (atom ::stale-db)
                    128
                    1
                    29)]
        (is (= 29 (:version result)))
        (is (true? (:payload-valid? result)))
        (is (= 128 (:payload-bytes result)))
        (is (= 1 (count @txs)))))))

(deftest tx-fn-register-cas-reports-requested-new-value-test
  (let [txs (atom [])]
    (with-redefs [conn/transact! (fn
                                    ([_conn tx]
                                     (swap! txs conj tx)
                                     {:tx-data []})
                                    ([_conn tx _tx-meta]
                                     (swap! txs conj tx)
                                     {:tx-data []}))]
      (let [result (#'tx-fn-register/cas-via-tx-fn!
                    (atom ::stale-db)
                    128
                    1
                    [13 29])]
        (is (= 29 (:version result)))
        (is (true? (:payload-valid? result)))
        (is (= 128 (:payload-bytes result)))
        (is (= 1 (count @txs)))))))

(deftest tx-fn-register-leader-snapshot-checks-only-leader-test
  (let [checked-nodes (atom [])]
    (is (= {:n1 {:ready? true}}
           (#'tx-fn-register/leader-txreg-snapshot
            (fn [_cluster-id logical-node _key-count _payload-bytes]
              (swap! checked-nodes conj logical-node)
              {:ready? true})
            :cluster
            :n1
            4
            128)))
    (is (= [:n1] @checked-nodes))
    (is (true? (#'tx-fn-register/ready-txreg-snapshot?
                {:n1 {:ready? true}})))
    (is (false? (#'tx-fn-register/ready-txreg-snapshot?
                 {:n1 {:ready? false}})))))
