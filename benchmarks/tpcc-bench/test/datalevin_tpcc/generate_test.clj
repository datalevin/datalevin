(ns datalevin-tpcc.generate-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin-tpcc.common :as c]
   [datalevin-tpcc.generate :as g]
   [datalevin-tpcc.txns :as t])
  (:import
   [java.util Random]))

(deftest last-names
  (is (= 1000 (count g/last-names)))
  (is (= "BARBARBAR" (g/last-name-from-index 0)))
  (is (= (count (distinct g/last-names)) 1000)))

(deftest customer-last-names-cover-every-district
  (let [rows      (vec (get (g/population-rows 42 1) "customer"))
        name-pool (set g/last-names)]
    (is (= (* c/districts-per-warehouse c/customers-per-district)
           (count rows)))
    (doseq [d (range 1 (inc c/districts-per-warehouse))]
      (let [district-rows (filter #(= d (nth % 1)) rows)
            first-1000    (filter #(<= (nth % 0) 1000) district-rows)
            later         (filter #(> (nth % 0) 1000) district-rows)
            first-lasts   (map #(nth % 5) first-1000)
            later-lasts   (map #(nth % 5) later)]
        (testing (str "district " d)
          (is (= 1000 (count first-1000)))
          (is (= name-pool (set first-lasts))
              "the first 1000 customers must cover all 1000 standard names")
          (is (= 1000 (count (distinct first-lasts)))
              "the first 1000 last names must be unique")
          (is (every? name-pool later-lasts)
              "later customers must draw from the standard name pool"))))))

(deftest nurand-range
  (let [r (Random. 1)]
    (dotimes [_ 1000]
      (let [x (t/nurand r 8191 1 100000 0)]
        (is (<= 1 x 100000))))))

(deftest population-cardinality
  (let [rows (g/population-rows 42 1)]
    (testing "fixed table sizes"
      (is (= c/item-count (count (get rows "item"))))
      (is (= c/item-count (count (get rows "stock"))))
      (is (= 1 (count (get rows "warehouse"))))
      (is (= c/districts-per-warehouse (count (get rows "district"))))
      (is (= (* c/districts-per-warehouse c/customers-per-district)
             (count (get rows "customer"))))
      (is (= (* c/districts-per-warehouse c/customers-per-district)
             (count (get rows "history"))))
      (is (= (* c/districts-per-warehouse c/initial-orders-per-district)
             (count (get rows "orders"))))
      (is (= (* c/districts-per-warehouse c/new-orders-per-district)
             (count (get rows "new_order")))))
    (testing "order lines are 5-15 per order"
      (let [n (count (get rows "order_line"))]
        (is (<= (* 5 (count (get rows "orders"))) n
                (* 15 (count (get rows "orders")))))))))

(deftest deterministic
  (let [a (g/order-data 42 1)
        b (g/order-data 42 1)]
    (is (= (take 50 (:orders a)) (take 50 (:orders b))))))

(deftest row-shapes
  (testing "each generated row matches its table spec width"
    (let [rows (g/population-rows 42 1)]
      (doseq [table c/table-order]
        (let [n (count (:columns (c/table-specs table)))
              row (first (get rows table))]
          (is (= n (count row)) (str table " width")))))))
