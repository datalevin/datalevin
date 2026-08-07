(ns datalevin.test.remote
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as client]
   [datalevin.remote :as remote]))

(deftest explicit-transaction-routing-stays-pinned-through-cleanup-test
  (let [routing-client ::routing-client
        active-client  ::active-client
        disabled       (atom [])
        enabled        (atom [])
        cleared        (atom [])
        requests       (atom [])
        fail-close?    (atom false)]
    (with-redefs [client/active-ha-request-client
                  (fn [client]
                    (if (= routing-client client)
                      active-client
                      client))
                  client/disable-ha-write-retry!
                  (fn [client]
                    (swap! disabled conj client)
                    client)
                  client/enable-ha-write-retry!
                  (fn [client]
                    (swap! enabled conj client)
                    client)
                  client/clear-preferred-ha-endpoint!
                  (fn [client]
                    (swap! cleared conj client)
                    client)
                  client/normal-request
                  (fn [client request-type args writing?]
                    (swap! requests conj
                           [client request-type args writing?])
                    (when (and @fail-close?
                               (contains? #{:close-transact
                                            :close-transact-kv}
                                          request-type))
                      (throw (ex-info "close rejected" {})))
                    :ok)]
      (testing "the selected endpoint is pinned to its owning session"
        (is (= active-client
               (#'remote/disable-ha-transaction-retry! routing-client)))
        (is (= [active-client] @cleared))
        (is (= [routing-client active-client] @disabled)))

      (testing "a successful close restores normal HA routing"
        (is (= :ok
               (#'remote/close-ha-transaction!
                active-client :close-transact "db")))
        (is (= [active-client] @enabled)))

      (reset! enabled [])
      (reset! fail-close? true)
      (testing "a rejected close remains pinned for the subsequent abort"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"close rejected"
             (#'remote/close-ha-transaction!
              active-client :close-transact "db")))
        (is (empty? @enabled)))

      (reset! fail-close? false)
      (testing "abort restores routing even when used as failure cleanup"
        (is (= :ok
               (#'remote/abort-ha-transaction!
                active-client :abort-transact "db")))
        (is (= [active-client] @enabled))
        (is (= [[active-client :close-transact ["db"] true]
                [active-client :close-transact ["db"] true]
                [active-client :abort-transact ["db"] true]]
               @requests))))))
