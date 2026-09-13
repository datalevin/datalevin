(ns ycsb-bench.test-runner
  (:require [clojure.test :as test]
            [ycsb-bench.core-test]
            [ycsb-bench.lifecycle-test]
            [ycsb-bench.sql-test]))

(defn -main [& _]
  (let [{:keys [fail error]} (try (test/run-tests 'ycsb-bench.core-test
                                                'ycsb-bench.lifecycle-test
                                                'ycsb-bench.sql-test)
                                (finally (shutdown-agents)))]
    (when (pos? (+ fail error)) (System/exit 1))))
