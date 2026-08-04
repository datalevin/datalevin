(ns datalevin-bench.cardinality-sql-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datalevin-bench.cardinality-sql :as sql]))

(deftest split-job-where-conditions
  (is (= ["t.year BETWEEN 2000 AND 2010"
          "n.name = 'A AND B'"
          "(x = 1 AND y = 2)"
          "z = 3"]
         (sql/split-top-level-and
           (str "t.year BETWEEN 2000 AND 2010 AND "
                "n.name = 'A AND B' AND (x = 1 AND y = 2) AND z = 3;")))))

(deftest subset-sql-adds-datalog-implied-joins
  (let [spec {:from-items [{:alias "a" :sql "ta AS a"}
                           {:alias "b" :sql "tb AS b"}
                           {:alias "c" :sql "tc AS c"}]
              :conditions [{:sql "a.kind = 'x'" :aliases #{"a"}}]}
        query (sql/subset-sql
                spec #{'?a '?b '?c}
                {"a" #{"x"} "b" #{"x"} "c" #{"x"}}
                {}
                {'?shared [["a" "x"] ["b" "x"] ["c" "x"]]})]
    (is (str/includes? query "a.kind = 'x'"))
    (is (str/includes? query "a.x = b.x"))
    (is (str/includes? query "a.x = c.x"))
    (is (str/includes? query "a.x IS NOT NULL"))))

(deftest factorized-sql-eliminates-leaf-variables
  (let [spec {:from-items [{:alias "a" :sql "ta AS a"}
                           {:alias "b" :sql "tb AS b"}
                           {:alias "c" :sql "tc AS c"}]
              :conditions []}
        query (sql/factorized-subset-sql
                spec #{'?a '?b '?c} {} {}
                {'?x [["a" "x"] ["b" "x"]]
                 '?y [["b" "y"] ["c" "y"]]})]
    (testing "the x leaf is summed out before the y join"
      (is (str/includes? query "e0 AS MATERIALIZED"))
      (is (str/includes? query "SUM(f0.w * f1.w)"))
      (is (str/includes? query "f0.v0 = f1.v0")))
    (testing "all remaining factors reduce to one scalar count"
      (is (str/includes? query "e1 AS MATERIALIZED"))
      (is (str/ends-with? query
                          "SELECT COALESCE(e1.w, 0)::bigint FROM e1")))))
