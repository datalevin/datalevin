(ns datalevin.testability-test
  "Covers the dependency-injection seams and reset helpers added for tests."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as cl]
   [datalevin.ha.promotion :as promo]
   [datalevin.server :as srv]
   [datalevin.util :as u]
   [datalevin.vector :as vec]))

(deftest env-overrides-test
  (testing "unknown variables fall through to the process environment"
    (is (nil? (u/env "DATALEVIN_TEST_MISSING_ENV")))
    (is (false? (u/env-enabled? "DATALEVIN_TEST_MISSING_ENV"))))
  (testing "*env-overrides* shadows the process environment, including nil"
    (binding [u/*env-overrides* {"DATALEVIN_TEST_ENV" "1"
                                 "DATALEVIN_TEST_BLANK" nil}]
      (is (= "1" (u/env "DATALEVIN_TEST_ENV")))
      (is (true? (u/env-enabled? "DATALEVIN_TEST_ENV")))
      (is (nil? (u/env "DATALEVIN_TEST_BLANK")))
      (is (false? (u/env-enabled? "DATALEVIN_TEST_BLANK"))))))

(deftest trace-remote-tx-reset-test
  (srv/reset-trace-remote-tx!)
  (try
    (is (false? (#'srv/trace-remote-tx?)))
    (binding [u/*env-overrides* {"DTLV_TRACE_REMOTE_TX" "1"}]
      (srv/reset-trace-remote-tx!)
      (is (true? (#'srv/trace-remote-tx?))))
    (finally
      (srv/reset-trace-remote-tx!))))

(deftest client-state-reset-test
  (let [^java.util.concurrent.ConcurrentHashMap m @#'cl/connection-wire-opts]
    (.put m :testability-key {:timeout 1})
    (is (pos? (.size m)))
    (is (nil? (cl/reset-client-state!)))
    (is (zero? (.size m)))))

(deftest vector-save-cache-reset-test
  (let [k (vec/vec-save-key "cache-test")]
    (is (= k (vec/vec-save-key "cache-test")))
    (is (nil? (vec/reset-vec-save-cache!)))
    (is (= k (vec/vec-save-key "cache-test")))))

(deftest injected-promotion-clock-test
  (let [res (promo/maybe-wait-unreachable-leader-before-pre-cas!
             {:ha-now-ms-fn (fn [] 1000)}
             {:ha-lease-renew-ms 10}
             {:lease-until-ms 2000})]
    (is (= 1010 (:wait-ms res)))
    (is (= 2010 (:wait-until-ms res)))))
