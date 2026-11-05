(ns datalevin-tpcc.sql-timing-test
  "Guards that SQL latency includes the wait for contention and retries, so
  the percentiles stay comparable with the Datalevin backend."
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.util :as u]
   [datalevin-tpcc.generate :as g]
   [datalevin-tpcc.postgres :as postgres]
   [datalevin-tpcc.sqlite :as sqlite])
  (:import
   [java.sql DriverManager SQLException]))

(deftest sqlite-native-writer-wait-isolates-terminal-rollback
  (let [dir (u/tmp-dir (str "tpcc-native-wait-" (random-uuid)))
        path (str dir "/sqlite.db")]
    (try
      (.mkdirs (u/file dir))
      (with-open [conn (DriverManager/getConnection (str "jdbc:sqlite:" path))]
        (#'sqlite/exec-schema! conn)
        (with-out-str
          (doseq [[table rows] {"warehouse" (g/warehouse-rows 42 1)
                                "district" (take 1 (g/district-rows 42 1))
                                "customer" (take 1 (g/customer-rows 42 1))}]
            (#'sqlite/load-table! conn table rows))))
      (#'sqlite/enable-wal! path)
      (with-open [owner (#'sqlite/open-conn path)
                  peer (#'sqlite/open-conn path)]
        (.setAutoCommit owner false)
        (#'sqlite/exec! owner "UPDATE warehouse SET w_ytd=w_ytd+7 WHERE w_id=1")
        (let [attempted (promise)
              worker (future
                       (deliver attempted true)
                       (sqlite/payment! peer {:w 1 :d 1 :c 1 :amount 5.0 :by-name? false}))]
          (try
            @attempted
            (is (= :waiting (deref worker 100 :waiting))
                "the native busy handler waits for the independent writer")
            (finally
              (.rollback owner)
              (.setAutoCommit owner true)))
          (is (= :ok (:status @worker)))
          (is (= 300005.0 (double (ffirst (#'sqlite/qall peer "SELECT w_ytd FROM warehouse")))))
          (is (= 1 (long (ffirst (#'sqlite/qall peer "SELECT COUNT(*) FROM history")))))))
      (finally (u/delete-files dir)))))

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
