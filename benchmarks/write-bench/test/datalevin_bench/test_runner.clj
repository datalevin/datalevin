(ns datalevin-bench.test-runner
  (:require
   [clojure.test :as test]
   [datalevin-bench.core-test]
   [datalevin-bench.harness-test]))

(defn -main
  [& _]
  (let [{:keys [fail error]}
        (test/run-tests 'datalevin-bench.harness-test
                        'datalevin-bench.core-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
