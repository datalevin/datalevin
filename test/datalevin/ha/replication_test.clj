(ns datalevin.ha.replication-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.kv :as kv]
   [datalevin.ha.replication :as repl])
  (:import
   [datalevin.lmdb KVTxData]
   [java.util.concurrent Executors]))

(deftest ha-parallel-mapv-clamps-overflowing-probe-deadlines
  (let [executor (Executors/newFixedThreadPool 2)
        timeout-ms (inc (quot Long/MAX_VALUE 1000000))]
    (try
      (with-redefs [repl/ha-now-nanos (constantly 100)
                    repl/ha-probe-round-timeout-ms (constantly timeout-ms)]
        (is (= [2 3]
               (#'datalevin.ha.replication/ha-parallel-mapv
                {:ha-probe-executor executor}
                (fn [x]
                  (Thread/sleep 20)
                  (inc x))
                [1 2]
                (constantly ::timeout)))))
      (finally
        (.shutdownNow executor)))))

(deftest follower-replay-appends-watermarks-in-the-same-kv-batch
  (let [rows (#'kv/append-payload-lsn-row
              [[:put :user/id 1 2]]
              42)
        payload-row ^KVTxData (nth rows 1)]
    (is (= 2 (count rows)))
    (is (= [:put :user/id 1 2]
           (nth rows 0)))
    (is (= :put (.-op payload-row)))
    (is (= c/kv-info (.-dbi-name payload-row)))
    (is (= c/wal-local-payload-lsn (.-k payload-row)))
    (is (= 42 (.-v payload-row)))
    (is (= :keyword (.-kt payload-row)))
    (is (= :data (.-vt payload-row)))))

(deftest advance-store-max-tx-to-target-progresses
  (let [max-tx* (atom 2)]
    (#'datalevin.ha.replication/advance-store-max-tx-to-target!
     (fn []
       @max-tx*)
     (fn []
       (swap! max-tx* inc))
     5)
    (is (= 5 @max-tx*))))

(deftest advance-store-max-tx-to-target-fails-fast-when-stalled
  (let [max-tx* (atom 2)]
    (try
      (#'datalevin.ha.replication/advance-store-max-tx-to-target!
       (fn []
         @max-tx*)
       (fn []
         nil)
       5)
      (is false)
      (catch clojure.lang.ExceptionInfo e
        (is (= :ha/follower-max-tx-stalled
               (:error (ex-data e))))
        (is (= 2
               (:current-max-tx (ex-data e))))
        (is (= 5
               (:target-max-tx (ex-data e))))))))
