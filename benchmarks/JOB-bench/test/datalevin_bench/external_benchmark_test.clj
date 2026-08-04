(ns datalevin-bench.external-benchmark-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin-bench.external-benchmark :as external]))

(deftest deterministic-query-samples
  (testing "the external runner uses the optimizer runner's frozen seed rule"
    (is (= 213459894921913
           (external/query-sample-seed 20261021 "22d")))
    (is (= (external/query-sample-seed 20261301 "1a")
           (external/query-sample-seed 20261301 "1a")))
    (is (not= (external/query-sample-seed 20261301 "1a")
              (external/query-sample-seed 20261302 "1a")))))

(deftest deterministic-query-schedules
  (let [first-order  (external/query-order 20261301)
        second-order (external/query-order 20261301)
        other-order  (external/query-order 20261302)]
    (is (= 113 (count first-order)))
    (is (= 113 (count (distinct first-order))))
    (is (= first-order second-order))
    (is (not= first-order other-order))))
