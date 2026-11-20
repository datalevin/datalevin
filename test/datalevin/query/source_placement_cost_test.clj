(ns datalevin.query.source-placement-cost-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source.site-join :as site-join]))

(deftest transfer-cost-prices-round-trips-and-bytes
  (let [estimate {:rows 800000 :row-bytes 32 :request-ns 300000 :fetch-rows 65536}
        independent #(site-join/transfer-cost estimate 800000 1 0 %)
        bound #(site-join/transfer-cost estimate 10000 147 1200000 %)]
    (is (< (bound {}) (independent {})))
    (is (< (independent {:link-rtt-ns 10000000}) (bound {:link-rtt-ns 10000000})))
    (is (= 10000 (bound {:link-rtt-ns 10000000 :cost-model :cardinality})))
    (is (< (bound {:transfer-ns-per-byte 1}) (bound {:transfer-ns-per-byte 10})))))

(deftest conditional-fanout-includes-missing-keys-and-skew
  ;; Four sampled keys have degrees 0, 0, 0 and 8.
  (let [sparse (site-join/fanout-range 40 4 8.0 64.0)
        uniform (site-join/fanout-range 40 4 8.0 16.0)]
    (is (= 80.0 (:rows sparse) (:rows uniform)))
    (is (= 0.0 (:low sparse)))
    (is (> (:high sparse) (:high uniform)))
    (is (= {:rows 8.0 :low 8.0 :high 8.0}
           (site-join/fanout-range 4 4 8.0 64.0)))))

(deftest receiver-costs-retain-the-route-when-units-disagree
  (is (= :bound (site-join/receiver-route 10 100 5 10 nil)))
  (is (= :independent (site-join/receiver-route 100 10 10 5 nil)))
  (is (nil? (site-join/receiver-route 10 100 50 10 nil)))
  (is (= :bound (site-join/receiver-route 10 100 50 10 1)))
  (is (= :independent (site-join/receiver-route 10 100 50 10 10))))
