(ns datalevin-tpcc.schema-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.util :as u]
            [datalevin-tpcc.datalevin :as bench]
            [datalevin-tpcc.generate :as g]
            [datalevin-tpcc.schema :as schema]
            [datalevin-tpcc.test-data :as td]
            [datalevin-tpcc.txns :as t]))

(deftest bulk-load-and-transactions-maintain-references-and-keys
  (let [dir (u/tmp-dir (str "tpcc-ref-test-" (random-uuid)))
        rows {"warehouse" (take 1 (g/warehouse-rows 42 1))
              "district" (take 1 (g/district-rows 42 1))
              "customer" (take 1 (g/customer-rows 42 1))
              "item" (take 1 (g/item-rows 42))
              "stock" (take 1 (g/stock-rows 42 1))}
        layout (schema/entity-layout 1)]
    (try
      (let [db (d/empty-db dir bench/schema {:closed-schema? true :wal? true
                                             :wal-durability-profile :strict :wal-sync-mode :fsync})]
        (try
          (with-out-str
            ;; Target IDs do not depend on table loading order.
            (doseq [table ["customer" "stock" "district" "item" "warehouse"]]
              (bench/load-table! db table (rows table) layout)))
          (finally (d/close-db db))))
      (with-open [conn (#'bench/open-bench-conn dir)]
        (let [db @conn
              warehouse (td/eid db "warehouse" [1])
              district (td/eid db "district" [1 1])
              customer (td/eid db "customer" [1 1 1])
              item (td/eid db "item" [1])
              stock (td/eid db "stock" [1 1])]
          (is (= (schema/entity-id layout "warehouse" [1]) warehouse))
          (is (= (schema/entity-id layout "customer" [1 1 1]) customer))
          (is (= #{[warehouse]} (d/q '[:find ?w :in $ ?d :where [?d :district/warehouse ?w]] db district)))
          (is (= #{[district]} (d/q '[:find ?d :in $ ?c :where [?c :customer/district ?d]] db customer)))
          (is (= #{[warehouse item]} (d/q '[:find ?w ?i :in $ ?s
                                            :where [?s :stock/warehouse ?w] [?s :stock/item ?i]] db stock)))
          (is (not-any? schema/foreign-key-columns (keys (d/schema conn))))
          (is (not-any? schema/foreign-key-columns (map :a (d/datoms db :eav))))
          (is (= 1 (count (d/datoms db :ave :customer/name-key [district "BARBARBAR"]))))
          (is (= #{:warehouse/id :item/id :district/key :customer/key :customer/name-key
                   :stock/key :district/warehouse :customer/district :stock/warehouse :stock/item}
                 (set (map :a (d/datoms db :ave)))))
          (let [result (t/new-order! conn {:w 1 :d 1 :c 1 :ol [{:i-id 1 :supply-w 1 :qty 2}]})
                o-id (:o-id result) order (td/eid @conn "orders" [1 1 o-id])
                line (td/eid @conn "order_line" [1 1 o-id 1])]
            (is (= :ok (:status result)))
            (is (= #{[district customer]} (d/q '[:find ?d ?c :in $ ?o
                                                  :where [?o :orders/district ?d] [?o :orders/customer ?c]] @conn order)))
            (is (= #{[order stock]} (d/q '[:find ?o ?s :in $ ?l
                                           :where [?l :order-line/order ?o] [?l :order-line/stock ?s]] @conn line)))
            (is (some? (td/eid @conn "new_order" [1 1 o-id])))
            (is (= o-id (get-in (t/order-status! conn {:w 1 :d 1 :c 1}) [:order :id])))
            (is (= 1 (:delivered (t/delivery! conn {:w 1 :carrier 2}))))
            (is (nil? (td/eid @conn "new_order" [1 1 o-id])))
            (is (some? (d/pull @conn [:db/id] order)) "deleting the pending row preserves the order")
            (is (some? (d/pull @conn [:db/id] line))))
          (is (thrown? Exception (d/transact! conn [{:warehouse/id 1 :warehouse/tax 0.2}])))
          (is (thrown? Exception (d/transact! conn [{:customer/district district :customer/id 1}])))))
      (finally (u/delete-files dir)))))

(deftest seed-ranges-are-disjoint-and-references-use-full-composite-keys
  (let [layout (schema/entity-layout 2)
        ranges (sort-by :base (vals layout))]
    (doseq [[a b] (partition 2 1 ranges)] (is (= (+ (:base a) (:size a)) (:base b))))
    (is (apply distinct? (for [w [1 2] district [1 2] customer [1 2]]
                           (schema/entity-id layout "customer" [w district customer]))))
    (let [row [3000 10 2 15 999 1 nil 5 10.0 "dist"]
          entity (schema/entity-map layout "order_line" row 1)]
      (is (= (schema/entity-id layout "orders" [2 10 3000]) (:order-line/order entity)))
      (is (= (schema/entity-id layout "stock" [1 999]) (:order-line/stock entity)))
      (is (= entity (schema/entity-map layout "order_line" row 77)))
      (is (not (contains? entity :order-line/delivery-d))))
    (doseq [[table key] [["warehouse" [0]] ["district" [1 11]] ["customer" [2 1 3001]]
                         ["orders" [1 1 3001]] ["order_line" [1 1 1 16]]]]
      (is (thrown? clojure.lang.ExceptionInfo (schema/entity-id layout table key))))))

(deftest outdated-databases-require-a-reload
  (doseq [old-schema [{:warehouse/id {:db/valueType :db.type/long}}
                      (update-in bench/schema [:stock/quantity] dissoc :db/noindex)
                      (assoc bench/schema :customer/w-id {:db/valueType :db.type/long})]]
    (let [dir (u/tmp-dir (str "tpcc-old-schema-" (random-uuid)))
          conn (d/create-conn dir old-schema {:wal? true :wal-durability-profile :strict :wal-sync-mode :fsync})]
      (d/close conn)
      (try
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"reload with :datalevin-db"
                              (#'bench/open-bench-conn dir)))
        (finally (u/delete-files dir))))))
