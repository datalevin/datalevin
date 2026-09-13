(ns datalevin-tpcc.sql-timing-test
  "Guards that SQL latency includes the wait for contention and retries, so
  the percentiles stay comparable with the Datalevin backend."
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin-tpcc.sqlite :as sqlite]
   [datalevin-tpcc.postgres :as postgres])
  (:import
   [java.sql SQLException]
   [java.util.concurrent.locks ReentrantLock]))

(deftest sqlite-writer-lock-wait-is-measured
  (let [lock    (ReentrantLock.)
        started (promise)
        result  (promise)]
    (.lock lock)
    (try
      (future
        (deliver started true)
        (deliver result (#'sqlite/with-write-lock lock (fn [] :done))))
      (is (deref started 1000 false) "worker started")
      (Thread/sleep 50)
      (finally (.unlock lock)))
    (let [[res ms] (deref result 5000 [:timeout nil])]
      (is (= :done res))
      (is (number? ms))
      (is (>= (double ms) 45.0)
          "waiting for the SQLite writer lock must be part of the latency"))))

(deftest postgres-retry-latency-includes-all-attempts
  (let [calls    (atom 0)
        [res ms] (#'postgres/run-with-retry
                   :new-order
                   (fn []
                     (if (= 1 (swap! calls inc))
                       (do (Thread/sleep 40)
                           (throw (SQLException. "serialization failure" "40001")))
                       {:status :ok})))]
    (is (= 2 @calls))
    (is (= {:status :ok} res))
    (is (>= (double ms) 35.0)
        "retry backoff and the failed attempt must be part of the latency")))

(deftest postgres-non-retryable-failure-is-not-swallowed
  (let [calls (atom 0)]
    (is (thrown-with-msg?
          SQLException #"constraint"
          (#'postgres/run-with-retry
            :new-order
            (fn []
              (swap! calls inc)
              (throw (SQLException. "constraint violation" "23505"))))))
    (is (= 1 @calls) "a non-retryable failure is not retried")))
