(ns datalevin-tpch.queries-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin-tpch.common :as c]
   [datalevin-tpch.datalevin :as dt]
   [datalevin-tpch.queries :as q]
   [datalevin-tpch.verify :as v])
  (:import
   [java.sql DriverManager]))

(defn- q14-datoms [lines]
  (into [[1 :part/partkey 1] [1 :part/type "PROMO SMALL"]
         [2 :part/partkey 2] [2 :part/type "STANDARD"]]
        (mapcat (fn [eid [part date price discount]]
                  [[eid :lineitem/partkey part]
                   [eid :lineitem/shipdate date]
                   [eid :lineitem/extendedprice price]
                   [eid :lineitem/discount discount]])
                (iterate inc 3) lines)))

(defn- q14-sqlite-rows [lines]
  (with-open [conn (DriverManager/getConnection "jdbc:sqlite::memory:")
              stmt (.createStatement conn)]
    (.execute stmt "CREATE TABLE part (p_partkey INTEGER, p_type TEXT)")
    (.execute stmt "CREATE TABLE lineitem (l_partkey INTEGER, l_shipdate TEXT, l_extendedprice REAL, l_discount REAL)")
    (.execute stmt "INSERT INTO part VALUES (1, 'PROMO SMALL'), (2, 'STANDARD')")
    (with-open [ps (.prepareStatement conn "INSERT INTO lineitem VALUES (?, ?, ?, ?)")]
      (doseq [[part date price discount] lines]
        (.setLong ps 1 part)
        (.setString ps 2 date)
        (.setDouble ps 3 price)
        (.setDouble ps 4 discount)
        (.executeUpdate ps)))
    (with-open [rs (.executeQuery stmt (slurp (c/query-file :sqlite 14)))]
      (loop [rows []]
        (if (.next rs)
          (recur (conj rows [(.getObject rs 1)]))
          rows)))))

(deftest q14-empty-range-returns-one-null-row
  (doseq [[label lines]
          [["empty lineitem table" []]
           ["no dates in September"
            [[1 "1995-08-31" 100.0 0.0] [2 "1995-10-01" 200.0 0.0]]]
           ["no matching part" [[99 "1995-09-15" 100.0 0.0]]]]]
    (testing label
      (let [datoms (q14-datoms lines)
            expected (q14-sqlite-rows lines)
            actual (#'v/datalevin-rows datoms 14)]
        (is (= [[nil]] expected))
        (is (= expected actual))
        (is (nil? (d/q q/q-14 datoms)) "the scalar query represents SQL NULL")
        (doseq [explain? [false true]]
          (is (= 1 (:rows (#'dt/run-one datoms 14 explain?)))
              "benchmark row counts include the NULL aggregate row"))))))

(deftest q14-nonempty-range-retains-promotion-ratio
  (doseq [[label lines ratio]
          [["no promotional parts" [[2 "1995-09-15" 100.0 0.1]] 0.0]
           ["only promotional parts" [[1 "1995-09-15" 100.0 0.1]] 100.0]
           ["duplicate revenues and lower date boundary"
            [[1 "1995-09-01" 100.0 0.2]
             [1 "1995-09-01" 100.0 0.2]
             [2 "1995-09-30" 200.0 0.2]] 50.0]]]
    (testing label
      (let [datoms (q14-datoms lines)
            expected (q14-sqlite-rows lines)
            actual (#'v/datalevin-rows datoms 14)]
        (is (= [[ratio]] expected))
        (is (v/results-match? 14 expected actual))
        (is (= ratio (d/q q/q-14 datoms)))))))

(defn- q18-datoms [orders]
  (into [[1 :customer/custkey 1] [1 :customer/name "Customer"]]
        (mapcat
          (fn [eid [key date total quantities]]
            (concat [[eid :orders/orderkey key] [eid :orders/custkey 1]
                     [eid :orders/orderdate date] [eid :orders/totalprice total]]
                    (mapcat (fn [line quantity]
                              [[line :lineitem/orderkey key] [line :lineitem/quantity quantity]])
                            (range (* 100 eid) (+ (* 100 eid) (count quantities))) quantities)))
          (iterate inc 2) orders)))

(defn- q18-sqlite-rows [orders]
  (with-open [conn (DriverManager/getConnection "jdbc:sqlite::memory:")
              stmt (.createStatement conn)]
    (.execute stmt "CREATE TABLE customer (c_custkey INTEGER, c_name TEXT)")
    (.execute stmt "CREATE TABLE orders (o_orderkey INTEGER, o_custkey INTEGER, o_orderdate TEXT, o_totalprice REAL)")
    (.execute stmt "CREATE TABLE lineitem (l_orderkey INTEGER, l_quantity REAL)")
    (.execute stmt "INSERT INTO customer VALUES (1, 'Customer')")
    (doseq [[key date total quantities] orders]
      (with-open [ps (.prepareStatement conn "INSERT INTO orders VALUES (?, 1, ?, ?)")]
        (.setLong ps 1 key) (.setString ps 2 date) (.setDouble ps 3 total) (.executeUpdate ps))
      (with-open [ps (.prepareStatement conn "INSERT INTO lineitem VALUES (?, ?)")]
        (doseq [quantity quantities]
          (.setLong ps 1 key) (.setDouble ps 2 quantity) (.executeUpdate ps))))
    (with-open [rs (.executeQuery stmt (#'v/reference-sql 18))]
      (loop [rows []]
        (if (.next rs)
          (recur (conj rows (mapv #(.getObject rs (int %)) (range 1 7)))) rows)))))

(deftest q18-quantity-witnesses-having-and-ordering
  (doseq [orders [[]
                  [[1 "1995-01-01" 100.0 [50.0 50.0 50.0 50.0 50.0 50.0]]]
                  [[1 "1995-01-02" 100.0 [50.0 50.0 50.0 50.0 50.0 50.0 1.0]]
                   [2 "1995-01-01" 100.0 [50.0 50.0 50.0 50.0 50.0 50.0 2.0]]
                   [3 "1995-01-01" 200.0 [50.0 50.0 50.0 50.0 50.0 50.0 3.0]]]
                  (vec (for [key (range 1 105)]
                         [key "1995-01-01" (double key) [50.0 50.0 50.0 50.0 50.0 50.0 5.0]]))]]
    (let [expected (q18-sqlite-rows orders)
          actual (#'v/datalevin-rows (q18-datoms orders) 18)]
      (is (v/results-match? 18 expected actual)))))
