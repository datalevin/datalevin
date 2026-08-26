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
    (is (= "findall(X, b1(X,1), L)"
           (#'xsb/answer-materialization-goal task)))
    (is (str/includes? (souffle/program-for-task task)
                       "result(x) :- b1(x, 1)."))
    (is (= ["souffle" "-m" "result" "-g" "task.cpp" "task.dl"]
           (#'souffle/souffle-generation-args
             task "task.cpp" "task.dl"))))
  (let [task (core/require-benchmark-task "tc:50k-cyclic-ff")]
    (is (= ["souffle" "-g" "task.cpp" "task.dl"]
           (#'souffle/souffle-generation-args
             task "task.cpp" "task.dl")))))

(deftest external-engine-timing-boundary-test
  (let [source @#'souffle/embedded-harness-source]
    (is (< (str/index-of source "program->loadAll")
           (str/index-of source "const auto start")))
    (is (< (str/index-of source "const auto start")
           (str/index-of source "program->runAll")))
    (is (< (str/index-of source "program->getRelationSize")
           (str/index-of source "const auto finish"))))
  (is (= ["/sdk/lib" "/sdk/lib" "/other/lib"]
         (#'souffle/mach-o-rpaths
           (str "         path /sdk/lib (offset 12)\n"
                "         path /sdk/lib (offset 12)\n"
                "         path /other/lib (offset 12)\n")))))
