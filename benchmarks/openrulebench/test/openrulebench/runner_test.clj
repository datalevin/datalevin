(ns openrulebench.runner-test
  (:require
   [clojure.test :refer [deftest is]]
   [openrulebench.runner :as runner]))

(deftest orchestrator-argument-test
  (let [opts (runner/parse-args
               ["--systems" "datalevin,sqlite"
                "--warmup" "0"
                "--iterations" "2"
                "tc:tiny" "sg:tiny"])]
    (is (= [:datalevin :sqlite] (:systems opts)))
    (is (= 0 (:warmup opts)))
    (is (= 2 (:iterations opts)))
    (is (= ["tc:tiny" "sg:tiny"] (:benchmarks opts)))))

(deftest help-does-not-resolve-to-benchmarks-test
  (is (true? (:help? (runner/parse-args ["--help"])))))

(deftest benchmark-groups-cover-published-matrix-test
  (is (= 42 (count runner/all-benchmarks)))
  (is (= 21 (count (:benchmarks (runner/parse-args ["differential"])))))
  (is (= runner/default-benchmarks
         (:benchmarks (runner/parse-args []))))
  (is (thrown? clojure.lang.ExceptionInfo
               (runner/parse-args ["dblp:small"]))))

(deftest system-capabilities-are-task-specific-test
  (is (#'runner/supported? :datalevin "join1:50k-a-bf"))
  (is (#'runner/supported? :sqlite "tc:50k-acyclic-fb"))
  (is (#'runner/supported? :clara "sg:6k-cyclic-ff"))
  (is (not (#'runner/supported? :clara "sg:6k-cyclic-bf")))
  (is (not (#'runner/supported? :odoyle "join1:50k-a-ff"))))

(deftest cross-system-input-digest-test
  (is (empty? (#'runner/input-mismatches
                [{:benchmark "tc:tiny" :status :ok :input-digest "same"}
                 {:benchmark "tc:tiny" :status :ok :input-digest "same"}])))
  (is (= [{:benchmark "tc:tiny" :digests #{"a" "b"}}]
         (#'runner/input-mismatches
          [{:benchmark "tc:tiny" :status :ok :input-digest "a"}
           {:benchmark "tc:tiny" :status :ok :input-digest "b"}]))))
