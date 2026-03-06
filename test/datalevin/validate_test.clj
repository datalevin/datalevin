(ns datalevin.validate-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.validate :as vld]))

(defn- valid-ha-opts
  []
  {:ha-mode :consensus-lease
   :db-identity "7a9f1f8d-cf5a-4fd6-a5a0-6db4a74a6f6f"
   :ha-node-id 2
   :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                {:node-id 2 :endpoint "10.0.0.12:8898"}
                {:node-id 3 :endpoint "10.0.0.13:8898"}]
   :ha-lease-renew-ms 5000
   :ha-lease-timeout-ms 15000
   :ha-promotion-base-delay-ms 300
   :ha-promotion-rank-delay-ms 700
   :ha-max-promotion-lag-lsn 0
   :ha-clock-skew-budget-ms 100
   :ha-fencing-hook {:cmd ["/usr/local/bin/dtlv-fence"]
                     :timeout-ms 3000
                     :retries 2
                     :retry-delay-ms 1000}
   :ha-control-plane {:backend :sofa-jraft
                      :group-id "ha-prod"
                      :local-peer-id "10.0.0.12:7801"
                      :voters [{:peer-id "10.0.0.11:7801" :ha-node-id 1 :promotable? true}
                               {:peer-id "10.0.0.12:7801" :ha-node-id 2 :promotable? true}
                               {:peer-id "10.0.0.13:7801" :ha-node-id 3 :promotable? true}]
                      :rpc-timeout-ms 2000
                      :election-timeout-ms 3000
                      :operation-timeout-ms 5000}})

(deftest derive-ha-membership-hash-deterministic-test
  (let [opts-a (valid-ha-opts)
        opts-b (-> (valid-ha-opts)
                   (assoc :ha-members [{:node-id 3 :endpoint "10.0.0.13:8898"}
                                       {:node-id 1 :endpoint "10.0.0.11:8898"}
                                       {:node-id 2 :endpoint "10.0.0.12:8898"}])
                   (assoc-in [:ha-control-plane :voters]
                             [{:peer-id "10.0.0.13:7801" :ha-node-id 3 :promotable? true}
                              {:peer-id "10.0.0.11:7801" :ha-node-id 1 :promotable? true}
                              {:peer-id "10.0.0.12:7801" :ha-node-id 2 :promotable? true}]))]
    (is (= (vld/derive-ha-membership-hash opts-a)
           (vld/derive-ha-membership-hash opts-b)))))

(deftest validate-ha-options-test
  (testing "accepts valid consensus HA options"
    (let [opts (valid-ha-opts)]
      (is (= opts (vld/validate-ha-options opts)))))

  (testing "accepts witness voter topology"
    (let [opts (-> (valid-ha-opts)
                   (assoc :ha-node-id 1)
                   (assoc :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                                       {:node-id 2 :endpoint "10.0.0.12:8898"}])
                   (assoc-in [:ha-control-plane :local-peer-id]
                             "10.0.0.11:7801")
                   (assoc-in [:ha-control-plane :voters]
                             [{:peer-id "10.0.0.11:7801"
                               :ha-node-id 1
                               :promotable? true}
                              {:peer-id "10.0.0.12:7801"
                               :ha-node-id 2
                               :promotable? true}
                              {:peer-id "10.0.0.13:7801"
                               :promotable? false}]))]
      (is (= opts (vld/validate-ha-options opts)))))

  (testing "rejects unsorted :ha-members"
    (let [opts (assoc (valid-ha-opts)
                      :ha-members [{:node-id 2 :endpoint "10.0.0.12:8898"}
                                   {:node-id 1 :endpoint "10.0.0.11:8898"}
                                   {:node-id 3 :endpoint "10.0.0.13:8898"}])]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"ordered by ascending :node-id"
           (vld/validate-ha-options opts)))))

  (testing "rejects membership-hash mismatch when provided"
    (let [opts (assoc (valid-ha-opts) :ha-membership-hash "DEADBEEF")]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"membership hash does not match authoritative hash"
           (vld/validate-ha-options opts)))))

  (testing "rejects missing db-identity in consensus mode"
    (let [opts (dissoc (valid-ha-opts) :db-identity)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"db-identity must be a non-blank string"
           (vld/validate-ha-options opts)))))

  (testing "rejects non-parseable port in control-plane peer-id"
    (let [opts (assoc-in (valid-ha-opts)
                         [:ha-control-plane :local-peer-id]
                         "10.0.0.12:99999999999999999999")]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"port must be a valid integer"
           (vld/validate-ha-options opts)))))

  (testing "rejects non-positive clock skew budget"
    (let [opts (assoc (valid-ha-opts) :ha-clock-skew-budget-ms 0)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"ha-clock-skew-budget-ms must be a positive integer"
           (vld/validate-ha-options opts)))))

  (testing "rejects malformed clock skew hook"
    (let [opts (assoc (valid-ha-opts)
                      :ha-clock-skew-hook {:cmd []
                                           :timeout-ms 1000
                                           :retries 0
                                           :retry-delay-ms 0})]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"clock skew hook :cmd must be a non-empty vector"
           (vld/validate-ha-options opts))))))
