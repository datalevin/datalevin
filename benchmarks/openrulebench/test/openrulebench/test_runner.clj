(ns openrulebench.test-runner
  (:require
   [clojure.test :as test]
   [openrulebench.backend-test]
   [openrulebench.core-test]
   [openrulebench.runner-test]))

(defn -main
  [& _]
  (let [{:keys [fail error]}
        (test/run-tests 'openrulebench.backend-test
                        'openrulebench.core-test
                        'openrulebench.runner-test)]
    (shutdown-agents)
    (when (pos? (+ fail error))
      (System/exit 1))))
