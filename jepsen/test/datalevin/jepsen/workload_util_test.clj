(ns datalevin.jepsen.workload-util-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.jepsen.workload.identity-upsert :as identity-upsert]
   [datalevin.jepsen.workload.index-consistency :as index-consistency]
   [datalevin.jepsen.workload.internal :as internal]
   [datalevin.jepsen.workload.util :as workload.util]
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
      (is (= :unexpected (:error result))))))

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
