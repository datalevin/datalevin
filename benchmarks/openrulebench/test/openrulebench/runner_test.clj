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
