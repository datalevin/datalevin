(ns datalevin-tpch.hotspots-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datalevin-tpch.common :as c]
   [datalevin-tpch.fixtures :as f]
            [datalevin-tpch.sqlite :as sq]
            [datalevin-tpch.verify :as v])
  (:import [java.sql DriverManager]
           [java.util Random]))

(defn- compare-fixture! [n tables]
  (with-open [conn (DriverManager/getConnection "jdbc:sqlite::memory:")
              stmt (.createStatement conn)]
    (#'sq/exec-sql-file! conn (io/file (c/data-dir) "schema-sqlite.sql"))
    (.execute stmt "PRAGMA case_sensitive_like=ON")
    (let [entity (volatile! 0)
          facts (into []
                  (mapcat
                    (fn [table]
                      (let [attributes (c/attrs table)
                            types (:types (c/table-specs table))]
                        (with-open [insert (.prepareStatement conn
                                             (str "INSERT INTO " table " VALUES ("
                                                  (str/join "," (repeat (count attributes) "?")) ")"))]
                          (into []
                            (mapcat
                              (fn [[i row]]
                                (let [e (vswap! entity inc)
                                      row (merge {:lineitem/linenumber (inc i)} row)
                                      values (mapv (fn [a t] (get row a (case t :long 0 :double 0.0 :string "")))
                                                   attributes types)]
                                  (doseq [[j value] (map-indexed vector values)] (.setObject insert (inc j) value))
                                  (.executeUpdate insert)
                                  (mapv #(vector e %1 %2) attributes values))))
                            (map-indexed vector (tables table)))))))
                  c/table-order)
          expected (with-open [rs (.executeQuery stmt (#'v/reference-sql n))]
                     (let [width (.getColumnCount (.getMetaData rs))]
                       (loop [rows []]
                         (if (.next rs)
                           (recur (conj rows (mapv #(.getObject rs (int %)) (range 1 (inc width))))) rows))))
          actual (#'v/datalevin-rows (f/reference-facts facts) n)]
      (is (v/results-match? n expected actual)
          (pr-str {:query n :expected expected :actual actual}))
      {:expected expected :actual actual})))

(deftest q13-grouped-orders-preserve-left-join-and-comment-semantics
  (doseq [tables [{}
                  {"customer" [{:customer/custkey 1} {:customer/custkey 2}]}
                  {"customer" [{:customer/custkey 1} {:customer/custkey 2} {:customer/custkey 3}]
                   "orders" [{:orders/orderkey 10 :orders/custkey 1 :orders/comment "ordinary"}
                             {:orders/orderkey 11 :orders/custkey 1 :orders/comment "ordinary"}
                             {:orders/orderkey 12 :orders/custkey 1 :orders/comment "special requests"}
                             {:orders/orderkey 13 :orders/custkey 2 :orders/comment "special\nrequests"}
                             {:orders/orderkey 14 :orders/custkey 999 :orders/comment "orphan"}]}
                  {"customer" (mapv #(hash-map :customer/custkey %) (range 1 9))
                   "orders" (mapv (fn [i comment] {:orders/orderkey (+ 10 i) :orders/custkey (inc i)
                                                  :orders/comment comment})
                                  (range) ["specialrequests" "special special requests requests"
                                           "Special requests" "special REQUESTS" "requests special"
                                           "special\nrequests" "unrelated" ""])}]]
    (compare-fixture! 13 tables)))

(deftest q13-distribution-retains-zero-customers-duplicates-and-ordering
  (let [comments [[1 "ordinary"] [1 "ordinary"]
                  [2 "ordinary"] [2 "special requests"]
                  [3 "specialrequests"] [3 "special special requests requests"]
                  [5 "Special requests"] [6 "requests special"]
                  [7 "special\nrequests"] [8 ""] [999 "orphan"]]
        result (compare-fixture! 13
                 {"customer" (mapv #(hash-map :customer/custkey (+ 100 %)) (range 1 10))
                  "orders" (mapv (fn [key [customer comment]]
                                    {:orders/orderkey key :orders/custkey (+ 100 customer)
                                     :orders/comment comment})
                                  (range 10 21) comments)})]
    (is (= [[1 4] [0 4] [2 1]] (:expected result) (:actual result)))))

(deftest q13-generated-customer-order-distributions-match-sql
  (let [rng (Random. 132026)
        comments ["ordinary" "" "specialrequests" "special requests"
                  "Special requests" "requests special" "special\nrequests"]]
    (dotimes [_ 24]
      (let [customers (.nextInt rng 10)
            orders (.nextInt rng 40)]
        (compare-fixture! 13
          {"customer" (mapv #(hash-map :customer/custkey %) (range 1 (inc customers)))
           "orders" (mapv (fn [key]
                             {:orders/orderkey key
                              :orders/custkey (inc (.nextInt rng (+ customers 2)))
                              :orders/comment (comments (.nextInt rng (count comments)))})
                           (range 1 (inc orders)))})))))

(deftest q19-brand-branches-retain-bounds-and-lineitem-multiplicity
  (compare-fixture! 19 {})
  (doseq [[brand container min-q max-q max-size]
          [["Brand#12" "SM BOX" 1 11 5] ["Brand#23" "MED BOX" 10 20 10]
           ["Brand#34" "LG BOX" 20 30 15]]]
    (testing brand
      (let [parts (mapv (fn [key size cont] {:part/partkey key :part/brand brand :part/container cont :part/size size})
                        [1 2 3 4 5] [1 max-size 0 (inc max-size) 1]
                        [container container container container "INVALID"])
            line (fn [part quantity mode instruction]
                   {:lineitem/partkey part :lineitem/quantity (double quantity)
                    :lineitem/shipmode mode :lineitem/shipinstruct instruction
                    :lineitem/extendedprice 100.0 :lineitem/discount 0.1})]
        (compare-fixture! 19
          {"part" parts
           "lineitem" [(line 1 min-q "AIR" "DELIVER IN PERSON")
                       (line 1 min-q "AIR" "DELIVER IN PERSON")
                       (line 2 max-q "AIR REG" "DELIVER IN PERSON")
                       (line 1 (dec min-q) "AIR" "DELIVER IN PERSON")
                       (line 1 (inc max-q) "AIR" "DELIVER IN PERSON")
                       (line 3 min-q "AIR" "DELIVER IN PERSON")
                       (line 4 min-q "AIR" "DELIVER IN PERSON")
                       (line 5 min-q "AIR" "DELIVER IN PERSON")
                       (line 1 min-q "TRUCK" "DELIVER IN PERSON")
                       (line 1 min-q "AIR" "TAKE BACK RETURN")
                       (line 999 min-q "AIR" "DELIVER IN PERSON")]})))))

(deftest q1-repeated-lines-groups-and-date-cutoff
  (compare-fixture! 1 {})
  (let [line {:lineitem/returnflag "N" :lineitem/linestatus "O"
              :lineitem/quantity 10.0 :lineitem/extendedprice 100.0
              :lineitem/discount 0.05 :lineitem/tax 0.07 :lineitem/shipdate "1998-09-02"}]
    (compare-fixture! 1 {"lineitem" [line line (assoc line :lineitem/shipdate "1998-09-03")
                                               (assoc line :lineitem/returnflag "R" :lineitem/quantity 20.0)
                                               (assoc line :lineitem/linestatus "F" :lineitem/extendedprice 111.11)
                                               (assoc line :lineitem/shipdate "1998-09-01" :lineitem/discount 0.0)]})))

(deftest q2-minimum-offers-retain-part-region-scope-and-ties
  (compare-fixture! 2 {})
  (compare-fixture! 2
    {"region" [{:region/regionkey 1 :region/name "EUROPE"}
               {:region/regionkey 2 :region/name "ASIA"}]
     "nation" [{:nation/nationkey 1 :nation/regionkey 1 :nation/name "FRANCE"}
               {:nation/nationkey 2 :nation/regionkey 2 :nation/name "CHINA"}]
     "supplier" (mapv (fn [key nation]
                         {:supplier/suppkey key :supplier/nationkey nation
                          :supplier/name (str "supplier-" key) :supplier/acctbal 10.0})
                       [1 2 3 4] [1 1 2 1])
     "part" [{:part/partkey 1 :part/size 15 :part/type "SMALL BRASS"}
             {:part/partkey 2 :part/size 16 :part/type "SMALL BRASS"}
             {:part/partkey 3 :part/size 15 :part/type "SMALL STEEL"}
             {:part/partkey 4 :part/size 15 :part/type "SMALL BRASS"}
             {:part/partkey 5 :part/size 15 :part/type "SMALL BRASS"}]
     "partsupp" (mapv (fn [part supplier cost]
                         {:partsupp/partkey part :partsupp/suppkey supplier
                          :partsupp/supplycost cost})
                       ;; The cheaper European offer for part 5 must not
                       ;; reject either tied minimum-cost supplier for part 1.
                       [1 1 1 1 2 3 4 5] [1 2 3 4 1 1 3 4]
                       [10.0 10.0 1.0 12.0 1.0 1.0 1.0 1.0])}))

(deftest q17-grouped-averages-retain-duplicate-quantities-and-part-scope
  (compare-fixture! 17 {})
  (compare-fixture! 17
    {"part" [{:part/partkey 1 :part/brand "Brand#23" :part/container "MED BOX"}
             {:part/partkey 2 :part/brand "Brand#23" :part/container "OTHER"}
             {:part/partkey 3 :part/brand "Brand#12" :part/container "MED BOX"}
             {:part/partkey 4 :part/brand "Brand#23" :part/container "MED BOX"}
             {:part/partkey 5 :part/brand "Brand#23" :part/container "MED BOX"}]
     ;; The two quantity-1 lines must both contribute to the average and sum.
     ;; Deduplicating quantities raises the threshold enough to admit quantity 2.
     "lineitem" (mapv (fn [part quantity price]
                         {:lineitem/partkey part :lineitem/quantity quantity
                          :lineitem/extendedprice price})
                       [1 1 1 1 2 3 5]
                       [1.0 1.0 2.0 30.0 1000.0 0.0 10.0]
                       [70.0 70.0 140.0 700.0 7000.0 7000.0 7000.0])}))

(deftest q20-grouped-sums-retain-empty-zero-and-boundary-semantics
  (compare-fixture! 20 {})
  (let [line (fn [supplier part quantity date]
               {:lineitem/suppkey supplier :lineitem/partkey part
                :lineitem/quantity quantity :lineitem/shipdate date})]
    (compare-fixture! 20
      {"nation" [{:nation/nationkey 1 :nation/name "CANADA"}
                 {:nation/nationkey 2 :nation/name "UNITED STATES"}]
       "supplier" (mapv (fn [key nation]
                           {:supplier/suppkey key :supplier/nationkey nation
                            :supplier/name (str "supplier-" key)
                            :supplier/address (str "address-" key)})
                         [1 2 3 4 5 6 7] [1 2 1 1 1 1 1])
       "part" [{:part/partkey 1 :part/name "forest green"}
               {:part/partkey 2 :part/name "forest blue"}
               {:part/partkey 3 :part/name "Forest uppercase"}]
       "partsupp" (mapv (fn [supplier part available]
                           {:partsupp/suppkey supplier :partsupp/partkey part
                            :partsupp/availqty available})
                         [1 2 3 4 5 6 7] [1 1 2 1 3 1 1] [11 100 9 100 100 1 10])
       "lineitem" [(line 1 1 10.0 "1994-01-01")
                   (line 1 1 10.0 "1994-12-31")
                   (line 1 1 1000.0 "1993-12-31")
                   (line 1 1 1000.0 "1995-01-01")
                   (line 2 1 10.0 "1994-01-01")
                   (line 3 2 10.0 "1994-01-01")
                   (line 3 2 10.0 "1994-01-01")
                   (line 4 1 10.0 "1995-01-01")
                   (line 5 3 10.0 "1994-01-01")
                   (line 6 1 0.0 "1994-01-01")
                   (line 7 1 10.0 "1994-01-01")
                   (line 7 1 10.0 "1994-01-01")]})))
