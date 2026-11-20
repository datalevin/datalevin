(ns datalevin-tpcc.verify-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.util :as u]
            [datalevin-tpcc.common :as c]
            [datalevin-tpcc.generate :as g]
            [datalevin-tpcc.schema :as schema]
            [datalevin-tpcc.test-data :as td]
            [datalevin-tpcc.verify :as v]))

(deftest reconstruct-all-columns-through-references
  (let [dir (u/tmp-dir (str "tpcc-verify-" (random-uuid)))
        rows {"warehouse" (g/warehouse-rows 42 2)
              "district" (g/district-rows 42 2)
              "customer" [(first (g/customer-rows 42 1))]
              "item" [(first (g/item-rows 42))]
              "stock" [(assoc (first (g/stock-rows 42 1)) 1 2)]
              "orders" [[1 1 1 1 "entry" nil 1 0]
                        [2 1 1 1 "entry" 3 1 0]]
              "new_order" [[1 1 1]]
              "order_line" [[1 1 1 1 1 2 nil 5 10.0 "dist"]
                            [2 1 1 1 1 2 "delivered" 5 0.0 "dist"]]
              ;; Identical history rows are distinct events. Payment's district
              ;; and warehouse differ from the customer's location.
              "history" (repeat 2 [1 1 1 3 2 "date" 10.0 "history"])}]
    (try
      (with-open [conn (d/create-conn dir schema/schema)]
        (td/transact-rows! conn rows)
        (is (= schema/foreign-key-columns (set (keys v/column-paths))))
        (is (empty? (v/reference-errors @conn)))
        (doseq [table c/table-order]
          (let [expected (mapv #(mapv c/parse-value (:types (c/table-specs table)) %) (rows table))
                actual (v/table-rows @conn table)]
            (is (= (frequencies expected) (frequencies actual)) table)
            (is (= (v/fingerprint expected) (v/fingerprint actual)) table)))
        (is (not= (v/fingerprint (rows "history"))
                  (v/fingerprint (take 1 (rows "history"))))))
      (finally (u/delete-files dir)))))

(deftest invalid-reference-graph-is-reported
  (let [dir (u/tmp-dir (str "tpcc-invalid-ref-" (random-uuid)))]
    (try
      (with-open [conn (d/create-conn dir schema/schema)]
        (d/transact! conn [{:db/id 1 :warehouse/id 1}
                          {:db/id 2 :district/warehouse 1 :district/id 1}
                          {:db/id 3 :district/warehouse 1 :district/id 2}
                          {:db/id 4 :customer/district 2 :customer/id 1}
                          {:db/id 5 :orders/district 3 :orders/customer 4 :orders/id 1}
                          {:db/id 6 :new-order/order 5}
                          {:db/id 7 :history/customer 999 :history/district 2}])
        (is (= #{{:order 5 :error :customer-district}
                 {:pending 6 :error :missing-queue-key}
                 {:attribute :history/customer :missing-targets 1}}
               (set (v/reference-errors @conn))))
        (d/transact! conn [{:db/id 6 :new-order/key [2 1]}])
        (is (some #{{:pending 6 :error :queue-key}} (v/reference-errors @conn))))
      (finally (u/delete-files dir)))))
