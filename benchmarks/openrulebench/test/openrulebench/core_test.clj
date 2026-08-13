(ns openrulebench.core-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [openrulebench.core :as core]))

(deftest transitive-closure-reference-test
  (testing "paths and cycles"
    (is (= 3 (core/tc-reference-count [[0 1] [1 2]])))
    (is (= 4 (core/tc-reference-count [[0 1] [1 0]])))
    (is (= 0 (core/tc-reference-count [])))))

(deftest same-generation-reference-test
  (is (= 2
         (core/sg-reference-count
           {:par [[0 1] [2 3]]
            :sib [[1 3]]})))
  (is (= 1 (core/sg-reference-count {:par [] :sib [[2 3]]}))))

(deftest repeated-measurement-test
  (let [calls  (atom 0)
        result (core/run-repeated
                 "test" "join1:small"
                 (fn [_]
                   (let [n (swap! calls inc)]
                     {:status :ok :result-count 7 :time-ms (double n)}))
                 {:warmup 1 :iterations 3 :verify? true})]
    (is (= 4 @calls))
    (is (= :ok (:status result)))
    (is (= [2.0 3.0 4.0] (:samples-ms result)))
    (is (= 3.0 (:time-ms result)))
    (is (= :consistent (get-in result [:correctness :status])))))

(deftest correctness-and-failure-gates-test
  (let [incorrect (core/run-repeated
                    "test" "tc:tiny"
                    (constantly {:status :ok :result-count 9 :time-ms 1.0})
                    {:warmup 0 :iterations 2 :verify? true})
        calls     (atom 0)
        failed    (core/run-repeated
                    "test" "join1:small"
                    (fn [_]
                      (swap! calls inc)
                      {:status :error})
                    {:warmup 0 :iterations 5 :verify? true})]
    (is (= :incorrect (:status incorrect)))
    (is (= 10000 (:expected-count incorrect)))
    (is (= :error (:status failed)))
    (is (= 1 @calls))))

(deftest runner-argument-test
  (is (= {:warmup 0
          :iterations 3
          :verify? false
          :quiet? false
          :output nil
          :benchmarks ["tc:tiny"]}
         (core/parse-run-args
           ["--warmup" "0" "--iterations" "3" "--no-verify" "tc:tiny"]
           ["tc:small"]))))
