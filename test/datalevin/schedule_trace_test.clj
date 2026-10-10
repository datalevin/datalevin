(ns datalevin.schedule-trace-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.phase :as phase]))

(deftest schedule-trace-follows-final-preparation
  (let [selections (atom [])
        collector (batch/create
                    (fn [b]
                      (is (empty? @selections))
                      (is (= :inline (batch/batch-schedule b)))
                      (batch/freeze-schedule! b 1 true true)
                      (batch/begin-dispatch! b)
                      (is (= [{:schedule :parallel :weight 1}] @selections))
                      (object-array [:done])))
        stop (phase/observe!
               (fn [event context]
                 (when (= event :schedule-selected)
                   (swap! selections conj context))))]
    (try
      (is (= :done (batch/submit! collector {:data :request})))
      (is (= [{:schedule :parallel :weight 1}] @selections))
      (finally (stop) (batch/close! collector)))))
