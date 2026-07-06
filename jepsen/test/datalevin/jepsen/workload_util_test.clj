(ns datalevin.jepsen.workload-util-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.conn :as conn]
   [datalevin.core :as d]
   [datalevin.jepsen.workload.identity-upsert :as identity-upsert]
   [datalevin.jepsen.workload.index-consistency :as index-consistency]
   [datalevin.jepsen.workload.internal :as internal]
   [datalevin.jepsen.workload.membership-drift :as membership-drift]
   [datalevin.jepsen.workload.rejoin-bootstrap :as rejoin-bootstrap]
   [datalevin.jepsen.workload.register :as register]
   [datalevin.jepsen.test-support :as test-support]
   [datalevin.jepsen.workload.tx-fn-register :as tx-fn-register]
   [datalevin.jepsen.workload.util :as workload.util]
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.history :as history]))

(use-fixtures :once test-support/quiet-logs-fixture)

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
            (ex-info "Request to Datalevin server failed: \"HA read admission rejected\""
                     {}))))
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
                 :setup
                 {:db-name "retry-test"}
                 {}
                 1000
                 0
                 (fn [conn]
                   [:ok conn]))))
        (is (= 2 @attempts))))))

(deftest with-retrying-leader-conn-rejects-op-path-purpose-test
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"idempotent setup/bootstrap paths"
        (workload.util/with-retrying-leader-conn
          :operation
          {:db-name "op-path-retry"}
          {}
          1000
          (fn [_conn]
            :should-not-run)))))

(deftest with-cached-leader-conn-reuses-open-connection-test
  (let [opens  (atom 0)
        client (workload.util/attach-cached-leader-conn {})]
    (binding [workload.util/*open-leader-conn*
              (fn [_test _schema]
                [:conn (swap! opens inc)])]
      (is (= [:ok [:conn 1]]
             (workload.util/with-cached-leader-conn
               client
               {:db-name "cached-leader"}
               {}
               (fn [conn]
                 [:ok conn]))))
      (is (= [:ok [:conn 1]]
             (workload.util/with-cached-leader-conn
               client
               {:db-name "cached-leader"}
               {}
               (fn [conn]
                 [:ok conn]))))
      (is (= 1 @opens)))))

(deftest with-cached-leader-conn-clears-stale-conn-without-retry-test
  (let [opens  (atom 0)
        bodies (atom 0)
        client (workload.util/attach-cached-leader-conn {})]
    (with-redefs [local/transport-failure? (constantly false)]
      (binding [workload.util/*open-leader-conn*
                (fn [_test _schema]
                  [:conn (swap! opens inc)])]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"HA write admission rejected"
             (workload.util/with-cached-leader-conn
               client
               {:db-name "cached-leader"}
               {}
               (fn [_conn]
                 (swap! bodies inc)
                 (throw (ex-info
                         "Request to Datalevin server failed: \"HA write admission rejected\""
                         {:err-data {:error :ha/write-rejected
                                     :retryable? true}}))))))
        (is (= 1 @opens))
        (is (= 1 @bodies))
        (is (= [:ok [:conn 2]]
               (workload.util/with-cached-leader-conn
                 client
                 {:db-name "cached-leader"}
                 {}
                 (fn [conn]
                   [:ok conn]))))
        (is (= 2 @opens))
        (is (= 1 @bodies))))))

(deftest with-cached-leader-conn-clears-ha-read-rejection-test
  (let [opens  (atom 0)
        client (workload.util/attach-cached-leader-conn {})]
    (with-redefs [local/transport-failure? (constantly false)]
      (binding [workload.util/*open-leader-conn*
                (fn [_test _schema]
                  [:conn (swap! opens inc)])]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"HA read admission rejected"
             (workload.util/with-cached-leader-conn
               client
               {:db-name "cached-leader"}
               {}
               (fn [_conn]
                 (throw (ex-info
                         "Request to Datalevin server failed: \"HA read admission rejected\""
                         {:err-data {:error :ha/read-rejected
                                     :retryable? true}}))))))
        (is (= [:ok [:conn 2]]
               (workload.util/with-cached-leader-conn
                 client
                 {:db-name "cached-leader"}
                 {}
                 (fn [conn]
                   [:ok conn]))))
        (is (= 2 @opens))))))

(deftest with-cached-leader-conn-clears-closed-client-conn-test
  (let [opens  (atom 0)
        client (workload.util/attach-cached-leader-conn {})]
    (with-redefs [local/transport-failure? (constantly false)]
      (binding [workload.util/*open-leader-conn*
                (fn [_test _schema]
                  [:conn (swap! opens inc)])]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"This client is closed"
             (workload.util/with-cached-leader-conn
               client
               {:db-name "cached-leader"}
               {}
               (fn [_conn]
                 (throw (ex-info "This client is closed" {}))))))
        (is (= [:ok [:conn 2]]
               (workload.util/with-cached-leader-conn
                 client
                 {:db-name "cached-leader"}
                 {}
                 (fn [conn]
                   [:ok conn]))))
        (is (= 2 @opens))))))

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

(deftest rejoin-bootstrap-retries-ha-read-rejection-during-post-bootstrap-converge-test
  (let [attempts         (atom 0)
        test             {:datalevin/cluster-id ::post-bootstrap-read-reject-retry
                          :db-name "post-bootstrap-read-reject-retry"}
        bootstrap-result {:restarted-nodes ["n2"]}
        result           {:leader "n3"
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
                          "Request to Datalevin server failed: \"HA read admission rejected\""
                          {:type :start-sampling
                           :writing? false
                           :err-data {:error :ha/read-rejected
                                      :reason :not-leader
                                      :retryable? true}}))
                  result)))))
      (is (= 2 @attempts)))))

(deftest rejoin-bootstrap-retries-closed-lmdb-during-post-bootstrap-converge-test
  (let [attempts         (atom 0)
        test             {:datalevin/cluster-id ::post-bootstrap-lmdb-closed-retry
                          :db-name "post-bootstrap-lmdb-closed-retry"}
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
                (case (swap! attempts inc)
                  1 (throw (ex-info "LMDB env is closed."
                                    {:type :lmdb/closed}))
                  2 (throw (ex-info "Request to Datalevin server failed"
                                    {:err-data
                                     {:message "LMDB env is closed."}}))
                  result)))))
      (is (= 3 @attempts)))))

(deftest rejoin-bootstrap-checker-classifies-disrupted-converge-read-rejection-test
  (let [result (checker/check
                (#'rejoin-bootstrap/rejoin-checker)
                {:datalevin/nemesis-faults [:leader-failover
                                             :follower-rejoin]}
                (history/history
                 [{:process 0
                   :type :fail
                   :f :converge
                   :error "Request to Datalevin server failed: \"HA read admission rejected\""
                   :value {:message "Request to Datalevin server failed: \"HA read admission rejected\""
                           :server-message "HA read admission rejected"
                           :err-data {:error :ha/read-rejected
                                      :reason :not-leader
                                      :retryable? true}}}])
                nil)]
    (is (= :unknown (:valid? result)) (pr-str result))
    (is (= :disruption-only-converge (:adjusted-valid? result)))
    (is (= 0 (:converge-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:disruption-failure-count result)))
    (is (= 0 (:mismatch-count result)))))

(deftest rejoin-bootstrap-local-leader-values-avoid-remote-read-test
  (let [cluster-id ::local-leader-values
        test       {:datalevin/cluster-id cluster-id
                    :db-name "local-leader-values"}]
    (with-redefs [local/cluster-state
                  (fn [actual-cluster-id]
                    (is (= cluster-id actual-cluster-id))
                    {:remote? false})
                  local/wait-for-single-leader!
                  (fn [actual-cluster-id timeout-ms]
                    (is (= cluster-id actual-cluster-id))
                    (is (= 1000 timeout-ms))
                    {:leader "n1"})
                  local/local-query
                  (fn [actual-cluster-id logical-node query]
                    (is (= cluster-id actual-cluster-id))
                    (is (= "n1" logical-node))
                    (is (= @#'rejoin-bootstrap/register-rows-query query))
                    [[0 7] [1 8]])]
      (binding [workload.util/*with-leader-conn*
                (fn [& _]
                  (throw (ex-info "remote leader read should not be used" {})))]
        (is (= [7 8]
               (#'rejoin-bootstrap/leader-register-values
                test
                2
                1000)))))))

(deftest rejoin-bootstrap-local-leader-values-retry-unavailable-read-test
  (let [cluster-id ::local-leader-values-retry
        attempts   (atom 0)
        test       {:datalevin/cluster-id cluster-id
                    :db-name "local-leader-values-retry"}]
    (with-redefs [rejoin-bootstrap/wal-gap-retry-sleep-ms 0
                  local/cluster-state (fn [_cluster-id]
                                        {:remote? false})
                  local/wait-for-single-leader!
                  (fn [actual-cluster-id _timeout-ms]
                    (is (= cluster-id actual-cluster-id))
                    {:leader "n1"})
                  local/node-diagnostics
                  (fn [actual-cluster-id logical-node]
                    (is (= cluster-id actual-cluster-id))
                    (is (= "n1" logical-node))
                    {:ha-role :leader})
                  local/local-query
                  (fn [actual-cluster-id logical-node query]
                    (is (= cluster-id actual-cluster-id))
                    (is (= "n1" logical-node))
                    (is (= @#'rejoin-bootstrap/register-rows-query query))
                    (if (= 1 (swap! attempts inc))
                      ::local/unavailable
                      [[0 11] [1 12]]))]
      (is (= [11 12]
             (#'rejoin-bootstrap/leader-register-values
              test
              2
              1000)))
      (is (= 2 @attempts)))))

(deftest rejoin-bootstrap-register-state-reader-selection-test
  (let [cluster-id  ::register-state-reader-selection
        live-reader @#'rejoin-bootstrap/in-process-node-register-state
        remote-reader @#'rejoin-bootstrap/remote-node-register-state]
    (with-redefs [local/cluster-state
                  (fn [actual-cluster-id]
                    (is (= cluster-id actual-cluster-id))
                    {:remote? false})]
      (is (identical? live-reader
                      (#'rejoin-bootstrap/register-state-reader
                       cluster-id #{"n2"} "n1")))
      (is (identical? live-reader
                      (#'rejoin-bootstrap/register-state-reader
                       cluster-id #{"n2"} "n2"))))
    (with-redefs [local/cluster-state
                  (fn [actual-cluster-id]
                    (is (= cluster-id actual-cluster-id))
                    {:remote? true})]
      (is (identical? live-reader
                      (#'rejoin-bootstrap/register-state-reader
                       cluster-id #{"n2"} "n1")))
      (is (identical? remote-reader
                      (#'rejoin-bootstrap/register-state-reader
                       cluster-id #{"n2"} "n2"))))))

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

(deftest membership-drift-leader-register-values-retries-transient-ha-read-test
  (let [attempts (atom 0)
        test     {:datalevin/cluster-id ::membership-drift-read-retry
                  :db-name "membership-drift-read-retry"}
        db       (d/db-with
                  (d/empty-db nil membership-drift/schema)
                  [{:register/key 0 :register/value 7}
                   {:register/key 1 :register/value 8}])]
    (with-redefs [local/transport-failure? (constantly false)]
      (binding [workload.util/*with-leader-conn*
                (fn [_test _schema f]
                  (if (= 1 (swap! attempts inc))
                    (throw (ex-info
                            "Request to Datalevin server failed: \"HA read admission rejected\""
                            {:type :start-sampling
                             :writing? false
                             :err-data {:error :ha/read-rejected
                                        :reason :not-leader
                                        :retryable? true}}))
                    (f (delay db))))]
        (is (= [7 8]
               (#'membership-drift/leader-register-values test 2)))
        (is (= 2 @attempts))))))

(deftest membership-drift-restart-retries-transient-open-timeout-test
  (let [attempts (atom 0)
        test     {:datalevin/cluster-id ::membership-drift-restart-retry
                  :db-name "membership-drift-restart-retry"}]
    (with-redefs [local/transport-failure? (constantly false)
                  local/restart-node!
                  (fn [_cluster-id _logical-node]
                    (if (= 1 (swap! attempts inc))
                      (throw (ex-info "Timeout in making request"
                                      {:phase :open-conn
                                       :timeout-ms 250}))
                      true))]
      (is (true?
           (#'membership-drift/restart-node-with-retry!
            test
            "n1"
            1000)))
      (is (= 2 @attempts)))))

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

(defn- empty-transaction-graph-checker
  []
  (reify checker/Checker
    (check [_ _test _history _opts]
      {:valid? :unknown
       :anomalies {:empty-transaction-graph true}})))

(deftest wrap-empty-graph-checker-preserves-unknown-for-ignorable-history-test
  (let [wrapped (workload.util/wrap-empty-graph-checker
                 (empty-transaction-graph-checker)
                 (fn [op]
                   (= :txn (:f op)))
                 [:f :error]
                 workload.util/append-graph-ignorable-micro-op-txn?)
        result  (checker/check
                 wrapped
                 {}
                 (history/history
                  [{:type :ok
                    :f :txn
                    :value [[:r 1 []]]}])
                 nil)]
    (is (= :unknown (:valid? result)) (pr-str result))
    (is (= :unknown (:base-valid? result)))
    (is (= :ignorable-empty-graph (:adjusted-valid? result)))))

(deftest wrap-empty-graph-checker-preserves-unknown-for-disruption-only-history-test
  (let [wrapped (workload.util/wrap-empty-graph-checker
                 (empty-transaction-graph-checker)
                 (fn [op]
                   (= :txn (:f op)))
                 [:f :error])
        result  (checker/check
                 wrapped
                 {:datalevin/nemesis-faults [:node-kill]}
                 (history/history
                  [{:type :fail
                    :f :txn
                    :error "Timeout in making request"
                    :value [[:append 1 1]
                            [:r 1 []]]}])
                 nil)]
    (is (= :unknown (:valid? result)) (pr-str result))
    (is (= :unknown (:base-valid? result)))
    (is (= :disruption-only-empty-graph (:adjusted-valid? result)))
    (is (= 1 (:disruption-failure-count result)))
    (is (= [{:f :txn
             :error "Timeout in making request"}]
           (:disruption-failure-samples result)))))

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

(deftest internal-checker-ignores-ha-read-rejection-during-disruption-test
  (let [op     {:f :tx-fn-after-add
                :internal/case-id 15}
        result (checker/check
                (#'internal/internal-checker)
                {:datalevin/nemesis-faults [:clock-skew-pause
                                             :leader-failover]}
                (history/history
                 [(assoc op
                         :process 0
                         :type :fail
                         :error "Request to Datalevin server failed: \"HA read admission rejected\"")
                  {:process 1
                   :type :ok
                   :f :probe
                   :value {}}])
                nil)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:disruption-failure-count result)))
    (is (= 0 (:probe-mismatch-count result)))))

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

(deftest index-consistency-checker-ignores-ha-read-rejection-during-disruption-test
  (let [op     {:f :tag-swap
                :index/case-id 13}
        result (checker/check
                (#'index-consistency/index-consistency-checker)
                {:datalevin/nemesis-faults [:clock-skew-pause
                                             :leader-failover]}
                (history/history
                 [(assoc op
                         :process 0
                         :type :fail
                         :error (str "Request to Datalevin server failed: "
                                     "\"HA read admission rejected\""))
                  {:process 1
                   :type :ok
                   :f :probe
                   :value {13 (first
                               (#'index-consistency/expected-states op))}}])
                nil)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:disruption-failure-count result)))
    (is (= 0 (:probe-mismatch-count result)))))

(deftest index-consistency-checker-ignores-final-probe-timeout-test
  (let [op     {:f :ref-create
                :index/case-id 8}
        result (checker/check
                (#'index-consistency/index-consistency-checker)
                {}
                (history/history
                 [(assoc op
                         :process 0
                         :type :ok
                         :value (#'index-consistency/expected-states op))
                  {:process 0
                   :type :info
                   :f :probe
                   :error :probe-timeout
                   :value {:timeout-ms 50}}])
                nil)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 1 (:probe-timeout-count result)))
    (is (= 0 (:probe-failure-count result)))
    (is (= 0 (:probe-mismatch-count result)))))

(deftest index-consistency-final-probe-is-bounded-test
  (let [started? (promise)
        blocking-conn
        (reify clojure.lang.IDeref
          (deref [_]
            (deliver started? true)
            (Thread/sleep 10000)
            nil))
        client   (workload.util/attach-cached-leader-conn
                  (index-consistency/->Client "n1"))
        result   (with-redefs-fn
                   {#'datalevin.jepsen.workload.index-consistency/final-probe-timeout-ms
                    50}
                   (fn []
                     (binding [workload.util/*open-leader-conn*
                               (fn [_test _schema]
                                 blocking-conn)]
                       (let [started-ms (System/currentTimeMillis)
                             op         (client/invoke!
                                         client
                                         {}
                                         {:type :invoke
                                          :f :probe})
                             elapsed-ms (- (System/currentTimeMillis)
                                           started-ms)]
                         {:op op
                          :elapsed-ms elapsed-ms}))))]
    (is (deref started? 500 false))
    (is (= {:type :info
            :f :probe
            :error :probe-timeout
            :value {:timeout-ms 50}}
           (:op result)))
    (is (< (:elapsed-ms result) 1000))))

(deftest tx-fn-register-read-is-bounded-test
  (let [started? (promise)
        cluster-id ::tx-fn-register-read-is-bounded
        read-op   {:type :invoke
                   :f :read
                   :value (first {0 nil})}
        blocking-conn
        (reify clojure.lang.IDeref
          (deref [_]
            (deliver started? true)
            (Thread/sleep 10000)
            nil))
        initialized-clusters
        (var-get #'tx-fn-register/initialized-clusters)
        original-initialized-clusters @initialized-clusters
        client   (workload.util/attach-cached-leader-conn
                  (tx-fn-register/->Client "n1" 1 128))
        result   (try
                   (swap! initialized-clusters conj cluster-id)
                   (with-redefs-fn
                    {#'workload.util/*open-leader-conn*
                     (fn [_test _schema]
                       blocking-conn)
                     #'d/close
                     (fn [_conn]
                       nil)}
                    (fn []
                      (binding [tx-fn-register/*read-timeout-ms* 50]
                        (let [started-ms (System/currentTimeMillis)
                              op         (client/invoke!
                                          client
                                          {:datalevin/cluster-id cluster-id}
                                          read-op)
                              elapsed-ms (- (System/currentTimeMillis)
                                            started-ms)]
                          {:op op
                           :elapsed-ms elapsed-ms}))))
                   (finally
                     (reset! initialized-clusters
                             original-initialized-clusters)))]
    (is (deref started? 500 false))
    (is (= (assoc read-op
                  :type :info
                  :error :read-timeout
                  :txreg/timeout-ms 50)
           (:op result)))
    (is (< (:elapsed-ms result) 1000))))

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
