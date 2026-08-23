(ns openrulebench.backend-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [openrulebench.core :as core]
   [openrulebench.postgresql :as postgresql]
   [openrulebench.souffle :as souffle]
   [openrulebench.sqlite :as sqlite]
   [openrulebench.xsb :as xsb]))

(deftest sql-backends-share-binding-aware-translations-test
  (doseq [[family builder]
          [[:tc sqlite/tc-query-for]
           [:sg sqlite/sg-query-for]]
          binding [:ff :bf :fb]]
    (testing (str (name family) " " (name binding))
      (let [sqlite-sql (builder binding 1)
            postgres-sql ((case family
                            :tc postgresql/tc-query-for
                            :sg postgresql/sg-query-for)
                          binding 1)]
        (is (= sqlite-sql postgres-sql))
        (is (str/includes? sqlite-sql "WITH RECURSIVE")))))
  (is (str/includes? (sqlite/tc-query-for :bf 1) "WHERE a = 1"))
  (is (str/includes? (sqlite/tc-query-for :fb 1) "WHERE b = 1"))
  (is (str/includes? (sqlite/sg-query-for :bf 1) "magic(x)"))
  (is (str/includes? (sqlite/sg-query-for :fb 1) "magic(y)")))

(deftest external-program-bindings-test
  (let [task (core/require-benchmark-task "join1:50k-b1-fb")]
    (is (= "findall(X, b1(X,1), L), length(L,N)"
           (#'xsb/answer-count-goal task)))
    (is (str/includes? (souffle/program-for-task task)
                       "result(x) :- b1(x, 1)."))))
