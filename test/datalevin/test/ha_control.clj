(ns datalevin.test.ha-control
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.lease :as lease]))

(deftest renew-transition-clamps-leader-last-applied-lsn-monotonically-test
  (let [db-identity "ha-control-renew-clamp"
        lease-entry {:lease (lease/new-lease-record
                             {:db-identity db-identity
                              :leader-node-id 1
                              :leader-endpoint "127.0.0.1:19033"
                              :term 7
                              :lease-renew-ms 1000
                              :lease-timeout-ms 5000
                              :now-ms 1000
                              :leader-last-applied-lsn 11})
                     :version 3}
        state {:leases {db-identity lease-entry}}
        request {:db-identity db-identity
                 :leader-node-id 1
                 :leader-endpoint "127.0.0.1:19033"
                 :term 7
                 :lease-renew-ms 1000
                 :lease-timeout-ms 5000
                 :leader-last-applied-lsn 10
                 :now-ms 2000}
        {:keys [result state]}
        (#'ctrl/apply-renew-transition state request 2000 1000)]
    (is (:ok? result))
    (is (= 11
           (get-in result [:lease :leader-last-applied-lsn])))
    (is (= 11
           (get-in state
                   [:leases db-identity :lease :leader-last-applied-lsn])))))
