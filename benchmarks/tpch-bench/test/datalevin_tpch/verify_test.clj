(ns datalevin-tpch.verify-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin-tpch.verify :as v]))

(deftest identifiers-and-counts-are-exact
  (doseq [[n row columns]
          [[1 ["N" "O" 1000000.0 10.0 9.0 9.5 1.0 10.0 0.1 1000000]
            {2 "quantity sum" 9 "line count"}]
           [2 [10.0 "Supplier" "Nation" 1000000 "Mfgr" "Address" "Phone" "Comment"]
            {3 "part key"}]
           [3 [1000000 10.0 "1995-03-01" 0] {0 "order key"}]
           [4 ["1-URGENT" 1000000] {1 "order count"}]
           [10 [1000000 "Customer" 10.0 20.0 "Nation" "Address" "Phone" "Comment"]
            {0 "customer key"}]
           [11 [1000000 10.0] {0 "part key"}]
           [12 ["MAIL" 1000000 1000000] {1 "high-priority count" 2 "low-priority count"}]
           [13 [1000000 1000000] {0 "orders per customer" 1 "customer count"}]
           [15 [1000000 "Supplier" "Address" "Phone" 10.0] {0 "supplier key"}]
           [16 ["Brand#12" "SMALL BRASS" 15 1000000] {3 "distinct supplier count"}]
           [18 ["Customer" 1000000 1000000 "1995-03-01" 10.0 1000000.0]
            {1 "customer key" 2 "order key" 5 "quantity sum"}]
           [21 ["Supplier" 1000000] {1 "waiting line count"}]
           [22 ["13" 1000000 10.0] {1 "customer count"}]]]
    (doseq [[column label] columns]
      (testing (str "Q" n " " label)
        (is (v/results-match? n [row] [row]))
        (is (not (v/results-match? n [row] [(update row column inc)])))
        (is (not (v/results-match? n [row] [(update row column dec)])))))))

(deftest projected-values-are-exact
  (doseq [[n row column delta]
          [[2 [10000.0 "Supplier" "Nation" 1 "Mfgr" "Address" "Phone" "Comment"] 0 0.001]
           [3 [1 10.0 "1995-03-01" 0] 3 0.0000001]
           [7 ["FRANCE" "GERMANY" 1995 10.0] 2 0.0001]
           [8 [1995 0.5] 0 0.0001]
           [9 ["FRANCE" 1995 10.0] 1 0.0001]
           [10 [1 "Customer" 10.0 10000.0 "Nation" "Address" "Phone" "Comment"] 3 0.001]
           [16 ["Brand#12" "SMALL BRASS" 15 10] 2 0.000001]
           [18 ["Customer" 1 2 "1995-03-01" 1000000.0 300.0] 4 0.01]]]
    (testing (str "Q" n " projected column " column)
      (is (not (v/results-match? n [row] [(update row column + delta)]))))))

(deftest aggregate-rounding-remains-tolerated
  (doseq [[n row columns]
          [[1 ["N" "O" 100.0 100.0 100.0 100.0 100.0 100.0 100.0 100]
            [3 4 5 6 7 8]]
           [3 [1 100.0 "1995-03-01" 0] [1]]
           [5 ["FRANCE" 100.0] [1]]
           [6 [100.0] [0]]
           [7 ["FRANCE" "GERMANY" 1995 100.0] [3]]
           [8 [1995 100.0] [1]]
           [9 ["FRANCE" 1995 100.0] [2]]
           [10 [1 "Customer" 100.0 10.0 "Nation" "Address" "Phone" "Comment"] [2]]
           [11 [1 100.0] [1]]
           [14 [100.0] [0]]
           [15 [1 "Supplier" "Address" "Phone" 100.0] [4]]
           [17 [100.0] [0]]
           [19 [100.0] [0]]
           [22 ["13" 100 100.0] [2]]]]
    (doseq [column columns]
      (testing (str "Q" n " aggregate column " column)
        (is (v/results-match? n [row] [(update row column + 0.000001)]))
        (is (not (v/results-match? n [row] [(update row column + 1.0)])))))))

(deftest numeric-representations-retain-exact-values
  (let [row [1000000 100.0 "1995-03-01" 0]]
    (doseq [key [(int 1000000) 1000000N 1000000.0 1000000M]]
      (is (v/results-match? 3 [row] [(assoc row 0 key)]))))
  (let [row [9007199254740992 100.0 "1995-03-01" 0]]
    (is (not (v/results-match? 3 [row] [(update row 0 inc)]))
        "adjacent integers above double precision remain distinct")
    (is (not (v/results-match? 3 [(update row 0 inc)] [(update row 0 double)]))
        "a rounded floating-point key cannot match the exact integer"))
  (let [rows [[1 100.0 "1995-03-01" 0] [10 100.0 "1995-03-01" 0]]]
    (is (v/results-match? 3 rows
                         (mapv #(update % 0 bigdec) (reverse rows)))
        "mixed numeric representations preserve row matching across ORDER BY ties")))

(deftest row-shape-and-multiplicity
  (is (not (v/results-match? 3 [[1 100.0]] [[1 100.0]]))
      "matching truncated rows are still invalid query results")
  (is (not (v/results-match? 6 [[1.0 2.0]] [[1.0 2.0]]))
      "matching extra columns are not silently ignored")
  (is (not (v/results-match? 6 [[nil]] [[0.0]])))
  (is (v/results-match? 6 [[nil]] [[nil]]))
  (is (v/results-match? 20 [["Supplier" "Address"]] [["Supplier" "Address"]]))
  (is (not (v/results-match? 20 [["Supplier" "Address"]]
                           [["Supplier" "Other address"]])))
  (is (not (v/results-match? 20 [["Supplier" "Address"]]
                           [["Supplier" "Address"] ["Supplier" "Address"]]))))

(deftest limit-boundary-ties-are-interchangeable
  ;; Q3 sorts by revenue desc, date asc and keeps ten rows. More than ten
  ;; orders share the final ordering key, so each backend may legitimately
  ;; return a different ten.
  (let [fixed (mapv (fn [i] [i (double (- 20 i)) "1995-03-01" 0])
                    (range 1 9))
        tie   (fn [orderkey] [orderkey 5.0 "1995-03-02" 0])
        a     (into fixed (map tie [100 101]))
        b     (into fixed (map tie [109 110]))]
    (is (= 10 (count a) (count b)))
    (is (v/results-match? 3 a b)
        "different members of the boundary tie group are both valid")
    (is (v/results-match? 3 b a) "the check is symmetric")
    (is (not (v/results-match? 3 a (assoc-in b [9 1] 4.0)))
        "an untied revenue at the boundary is a real mismatch")
    (is (not (v/results-match? 3 a (assoc-in b [0 3] 1)))
        "a mismatch above the boundary is still caught")
    (is (not (v/results-match? 3 (subvec a 0 9) (subvec b 0 9)))
        "a result shorter than the limit was not truncated")))
