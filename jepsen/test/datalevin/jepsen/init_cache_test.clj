(ns datalevin.jepsen.init-cache-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.jepsen.init-cache :as init-cache]
   [datalevin.jepsen.test-support :as test-support]))

(use-fixtures :once test-support/quiet-logs-fixture)

(deftest release-cluster-removes-cluster-id-from-all-registered-caches-test
  (let [cluster-id "cluster-a"
        other-id   "cluster-b"
        cache-a    (init-cache/register-cache! (atom #{cluster-id other-id}))
        cache-b    (init-cache/cluster-cache)]
    (swap! cache-b conj cluster-id)
    (init-cache/release-cluster! cluster-id)
    (is (= #{other-id} @cache-a))
    (is (= #{} @cache-b))))
