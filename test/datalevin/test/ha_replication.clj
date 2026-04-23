(ns datalevin.test.ha-replication
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha.replication :as drep]))

(deftest next-ha-follower-sync-lsn-clamps-to-local-floor-test
  (is (= 15 (#'drep/next-ha-follower-sync-lsn 15 23)))
  (is (= 10 (#'drep/next-ha-follower-sync-lsn 15 10)))
  (is (= 15 (#'drep/next-ha-follower-sync-lsn 15 nil))))

(deftest empty-follower-batch-advances-from-local-floor-test
  (let [reported-floor (atom nil)
        fetched-range  (atom nil)
        m              {:ha-node-id 2
                        :ha-local-last-applied-lsn 14
                        :ha-follower-next-lsn 23
                        :ha-follower-max-batch-records 8}
        lease          {:leader-endpoint "127.0.0.1:19001"
                        :leader-last-applied-lsn 22}]
    (with-redefs-fn
      {#'drep/reopen-ha-local-store-if-needed identity
       #'drep/fetch-ha-follower-records-with-gap-fallback
       (fn [_db-name _m _lease next-lsn upto-lsn]
         (reset! fetched-range [next-lsn upto-lsn])
         {:records []
          :source-endpoint "127.0.0.1:19001"
          :source-order ["127.0.0.1:19001"]
          :source-order-dynamic? false
          :source-last-applied-lsn-known? true
          :source-last-applied-lsn 22})
       #'drep/report-ha-replica-floor!
       (fn [_db-name _m leader-endpoint applied-lsn]
         (reset! reported-floor [leader-endpoint applied-lsn]))
       #'drep/refresh-ha-local-dt-db identity}
      (fn []
        (let [{:keys [applied-lsn state]}
              (#'drep/sync-ha-follower-batch "db" m lease 23 1000)]
          (is (= [23 30] @fetched-range))
          (is (= ["127.0.0.1:19001" 14] @reported-floor))
          (is (= 14 applied-lsn))
          (is (= 14 (:ha-local-last-applied-lsn state)))
          (is (= 15 (:ha-follower-next-lsn state)))
          (is (true? (:ha-follower-source-last-applied-lsn-known? state)))
          (is (= 22 (:ha-follower-source-last-applied-lsn state))))))))
