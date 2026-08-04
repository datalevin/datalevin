(ns datalevin-bench.cardinality-sql-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datalevin-bench.cardinality-sql :as sql]
   [datalevin.db :as db]))

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

(deftest align-sql-aliases-with-translated-entities
  (let [analysis
        {:entities #{'?it1 '?it2 '?mi-idx}
         :by-entity
         {'?it1 [['?it1 :info-type/info "rating"]]
          '?it2 [['?it2 :info-type/info "release dates"]]
          '?mi-idx [['?mi-idx :movie-info-idx/info '?value]]}}
        aligned
        (sql/align-sql-spec
          {:from-items [{:table "info_type" :alias "it"
                         :sql "info_type AS it"}
                        {:table "info_type" :alias "it2"
                         :sql "info_type AS it2"}
                        {:table "movie_info_idx" :alias "miidx"
                         :sql "movie_info_idx AS miidx"}
                        {:table "company_type" :alias "ct"
                         :sql "company_type AS ct"}]
           :conditions [{:sql "it.id = miidx.info_type_id"
                         :aliases #{"it" "miidx"}}
                        {:sql "ct.id = 2" :aliases #{"ct"}}]}
          analysis)]
    (is (= #{"it1" "it2" "mi_idx"}
           (into #{} (map :alias) (:from-items aligned))))
    (is (= "it1.id = mi_idx.info_type_id"
           (get-in aligned [:conditions 0 :sql])))
    (is (= #{"it1" "mi_idx"}
           (get-in aligned [:conditions 0 :aliases])))
    (testing "a SQL-only existence relation is not part of Datalog subsets"
      (is (not-any? #(= "ct" (:alias %)) (:from-items aligned))))))

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

(deftest factorized-sql-can-leave-one-factor-unprepared
  (let [spec {:from-items [{:alias "a" :sql "ta AS a"}
                           {:alias "b" :sql "tb AS b"}]
              :conditions []
              :prepared-factors
              {:var-name {'?x "v0"}
               :unprepared-aliases #{"b"}
               :factors
               {"a" {:vars #{'?x}
                     :projections {#{'?x} "prepared_a"}}
                "b" {:vars #{'?x}
                     :projections {#{'?x} "prepared_b"}}}}}
        query (sql/factorized-subset-sql
                spec #{'?a '?b} {} {}
                {'?x [["a" "x"] ["b" "x"]]})]
    (is (str/includes? query "FROM prepared_a"))
    (is (str/includes? query "FROM tb AS b"))
    (is (not (str/includes? query "prepared_b")))))

(deftest material-sql-omits-target-local-filters
  (let [spec {:from-items [{:alias "t" :sql "title AS t"}
                           {:alias "mi" :sql "movie_info AS mi"}]
              :conditions [{:sql "t.kind_id = 1" :aliases #{"t"}}
                           {:sql "mi.info = 'rating'" :aliases #{"mi"}}
                           {:sql "mi.movie_id = t.id"
                            :aliases #{"mi" "t"}}]}
        request {:kind :link-input
                 :entities '#{?t}
                 :link-e '?t
                 :target '?mi
                 :type :_ref
                 :attr :movie-info/movie
                 :var nil
                 :attrs nil}
        required {"t" #{"kind_id"}
                  "mi" #{"movie_id" "info"}}
        joins {'?t [["t" "id"] ["mi" "movie_id"]]}
        factors {:var-name {'?t "v0"}
                 :factors
                 {"t" {:vars #{'?t}
                       :projections {#{'?t} "prepared_t"}}
                  "mi" {:vars #{'?t}
                        :projections {#{'?t} "prepared_mi"}}}}
        query (with-redefs [db/-schema
                            (constantly
                              {:movie-info/movie
                               {:db/valueType :db.type/ref}})]
                (sql/material-factorized-sql
                  nil spec request required {} joins factors))]
    (is (str/includes? query "FROM prepared_t"))
    (is (str/includes? query "FROM movie_info AS mi"))
    (is (str/includes? query "mi.movie_id IS NOT NULL"))
    (is (not (str/includes? query "mi.info = 'rating'")))
    (is (not (str/includes? query "prepared_mi")))))
