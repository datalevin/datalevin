(ns datalevin-tpcc.schema-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin-tpcc.datalevin :as bench]
   [datalevin-tpcc.generate :as g]
   [datalevin-tpcc.txns :as t]))

(deftest bulk-load-and-transactions-maintain-business-keys
  (let [dir (u/tmp-dir (str "tpcc-key-test-" (random-uuid)))
        rows {"warehouse" (take 1 (g/warehouse-rows 42 1))
              "district" (take 1 (g/district-rows 42 1))
              "customer" (take 1 (g/customer-rows 42 1))
              "item" (take 1 (g/item-rows 42))
              "stock" (take 1 (g/stock-rows 42 1))}
        eid (atom 0)]
    (try
      (let [db (d/empty-db dir bench/schema {:closed-schema? true})]
        (try
          (with-out-str
            (doseq [[table rows] rows]
              (#'bench/load-table! db table rows eid)))
          (finally (d/close-db db))))
      ;; Reopen through the benchmark's schema check, using the bulk-loaded keys.
      (let [conn (#'bench/open-bench-conn dir)]
        (try
          (doseq [[key value] [[:warehouse/id 1] [:item/id 1] [:district/key [1 1]]
                               [:customer/key [1 1 1]] [:stock/key [1 1]]]]
            (is (pos-int? (:db/id (d/pull (d/db conn) [:db/id] [key value])))))
          (is (= 1 (count (d/datoms (d/db conn) :ave :customer/name-key [1 1 "BARBARBAR"]))))
          ;; A complete AVE scan must contain only application lookup keys.
          ;; Component values and mutable counters remain available through EAV.
          (is (= #{:warehouse/id :item/id :district/key :customer/key
                   :customer/name-key :stock/key}
                 (set (map :a (d/datoms (d/db conn) :ave)))))
          (let [result (t/new-order! conn {:w 1 :d 1 :c 1
                                           :ol [{:i-id 1 :supply-w 1 :qty 2}]})
                order (:o-id result)]
            (is (= :ok (:status result)))
            (doseq [[key value] [[:orders/key [1 1 order]]
                                 [:new-order/key [1 1 order]]
                                 [:order-line/key [1 1 order 1]]]]
              (is (some? (d/pull (d/db conn) [:db/id] [key value]))))
            (is (= order (get-in (t/order-status! conn {:w 1 :d 1 :c 1}) [:order :id])))
            (is (= 1 (:delivered (t/delivery! conn {:w 1 :carrier 2}))))
            (is (nil? (d/pull (d/db conn) [:db/id] [:new-order/key [1 1 order]]))))
          (is (thrown? Exception
                       (d/transact! conn [{:warehouse/id 1 :warehouse/tax 0.2}])))
          (is (thrown? Exception
                       (d/transact! conn [{:customer/w-id 1 :customer/d-id 1 :customer/id 1}])))
          (finally (d/close conn))))
      (finally (u/delete-files dir)))))

(deftest old-benchmark-databases-require-a-reload
  (let [dir (u/tmp-dir (str "tpcc-old-schema-" (random-uuid)))
        conn (d/get-conn dir {:warehouse/id {:db/valueType :db.type/long}})]
    (d/close conn)
    (try
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"reload with :datalevin-db"
                            (#'bench/open-bench-conn dir)))
      (finally (u/delete-files dir)))))

(deftest excess-value-indexes-require-a-reload
  (let [dir (u/tmp-dir (str "tpcc-old-indexes-" (random-uuid)))
        old-schema (update-in bench/schema [:stock/quantity] dissoc :db/noindex)
        conn (d/create-conn dir old-schema
                            {:wal? true :wal-durability-profile :strict
                             :wal-sync-mode :fsync})]
    (d/close conn)
    (try
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"reload with :datalevin-db"
                            (#'bench/open-bench-conn dir)))
      (finally (u/delete-files dir)))))
