(ns datalevin.tx-engine-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.db.tx.engine :as engine]))

(deftest explicit-test-engine-contract
  (binding [engine/*engine* :current]
    (is (false? (engine/interpreter?)))
    (binding [engine/*engine* :interpreter]
      (is (true? (engine/interpreter?))))
    (is (= :current engine/*engine*)))
  (binding [engine/*engine* :unavailable]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown Datalog transaction engine"
                          (engine/interpreter?)))))

(deftest compiler-is-an-explicit-opt-in
  (binding [engine/*engine* :current] (is (false? (engine/compiler?))))
  (binding [engine/*engine* :compiler]
    (is (true? (engine/compiler?)))
    (is (false? (engine/interpreter?)))))
