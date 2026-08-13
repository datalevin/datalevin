(ns idoc-bench.test-runner
  (:require
   [clojure.test :as test]
   [idoc-bench.core-test]))

(defn -main
  [& _]
  (let [{:keys [fail error]}
        (test/run-tests 'idoc-bench.core-test)]
    (shutdown-agents)
    (when (pos? (+ fail error))
      (System/exit 1))))
