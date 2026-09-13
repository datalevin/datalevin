(ns datalevin-tpch.common-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [datalevin-tpch.common :as c]
   [datalevin-tpch.queries :as q]))

(deftest table-metadata
  (testing "column and attribute counts agree for every table"
    (doseq [table c/table-order]
      (let [{:keys [columns types]} (c/table-specs table)]
        (is (= (count columns) (count types))
            (str table " column/type count"))
        (is (= (count columns) (count (c/attrs table)))
            (str table " attribute count")))))
  (testing "attribute names drop the TPC-H column prefix"
    (is (= [:lineitem/extendedprice :lineitem/shipdate]
           [(nth (c/attrs "lineitem") 5) (nth (c/attrs "lineitem") 10)]))
    (is (= :partsupp/supplycost (nth (c/attrs "partsupp") 3)))))

(deftest row-parsing
  (is (= ["1" "370" "O"] (c/trim-row ["1" "370" "O" ""]))
      "dbgen's trailing pipe is dropped")
  (is (= ["a" "" "b"] (c/trim-row ["a" "" "b"]))
      "interior empties are preserved")
  (is (= 42 (c/parse-value :long "42")))
  (is (= 1.5 (c/parse-value :double "1.5")))
  (is (= "x" (c/parse-value :string "x")))
  (is (nil? (c/parse-value :long ""))))

(deftest numeric-comparison
  (is (c/close-enough? 100.0 100.0000001))
  (is (not (c/close-enough? 100.0 101.0)))
  (is (c/close-enough? "a" "a"))
  (is (not (c/close-enough? "a" "b"))))

(deftest query-set
  (testing "all 22 queries have a Datalog translation"
    (is (= (vec (range 1 23)) (q/query-ids))))
  (testing "every query has both an SQLite and PostgreSQL file"
    (doseq [n (range 1 23)]
      (is (.exists (c/query-file :sqlite n)) (str "sqlite " n))
      (is (.exists (c/query-file :postgres n)) (str "postgres " n)))))

(def required-limits
  "TPC-H 2.1.2.9 requires a result row limit on these queries. qgen reports it
  as the ROWS_FETCH directive, which must be restored as a real LIMIT clause."
  {2 100, 3 10, 10 20, 18 100, 21 100})

(deftest required-row-limits
  (testing "Datalog translations carry the specification limit"
    (doseq [[n expected] required-limits]
      (let [form (q/datalog n)
            i    (first (keep-indexed (fn [i x] (when (= :limit x) i)) form))]
        (is (some? i) (str "q-" n " has no :limit"))
        (is (= expected (nth form (inc i))) (str "q-" n " limit")))))
  (testing "SQL files carry the specification limit"
    (doseq [[n expected] required-limits
            dialect     [:standard :postgres :sqlite]]
      (let [sql (slurp (c/query-file dialect n))]
        (is (re-find (re-pattern (str "(?i)\\blimit\\s+" expected "\\b")) sql)
            (str (name dialect) " q" n " limit " expected)))))
  (testing "queries without a specification limit do not add one"
    (doseq [n       (remove required-limits (range 1 23))
            dialect [:standard :postgres :sqlite]]
      (let [sql (slurp (c/query-file dialect n))]
        (is (not (re-find #"(?i)\blimit\b" sql))
            (str (name dialect) " q" n " should not have a limit"))))))
