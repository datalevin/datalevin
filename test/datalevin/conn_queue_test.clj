(ns datalevin.conn-queue-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.conn :as conn]
   [datalevin.core :as d]
   [datalevin.interface :as i]
   [datalevin.util :as u])
  (:import
   [java.util UUID]
   [datalevin.db DB]
   [datalevin.storage Store]))

(defn- with-temp-dl-conn
  [kv-opts f]
  (let [dir (u/tmp-dir (str "conn-queue-" (UUID/randomUUID)))
        c   (d/get-conn dir
                        {:k {:db/valueType :db.type/long}}
                        {:kv-opts kv-opts})]
    (try
      (f c)
      (finally
        (d/close c)
        (u/delete-files dir)))))

(defn- env-opts
  [conn]
  (let [^DB db   @conn
        ^Store s (.-store db)]
    (i/env-opts (.-lmdb s))))

(deftest strict-profile-uses-sync-queue-test
  (with-temp-dl-conn
    {:wal? true
     :wal-durability-profile :strict}
    (fn [c]
      (is (true? (#'conn/strict-txlog-sync-queue? c)))
      (d/with-transaction [cn c]
        (testing "inside an existing write transaction, queueing is disabled"
          (is (false? (#'conn/strict-txlog-sync-queue? cn))))))))

(deftest relaxed-profile-skips-sync-queue-test
  (with-temp-dl-conn
    {:wal? true
     :wal-durability-profile :relaxed}
    (fn [c]
      (is (false? (#'conn/strict-txlog-sync-queue? c))))))

(deftest relaxed-profile-uses-sync-queue-test
  (with-temp-dl-conn
    {:wal? true
     :wal-durability-profile :relaxed}
    (fn [c]
      (let [queued? (atom false)]
        (with-redefs [conn/queued-transact!
                      (fn [conn' tx-data tx-meta]
                        (reset! queued? true)
                        (#'conn/run-transact-now! conn' tx-data tx-meta))]
          (d/transact! c [{:k 2}]))
        (is (true? @queued?))
        (is (= 1 (d/q '[:find (count ?e) .
                        :where [?e :k]]
                      (d/db c))))))))

(deftest strict-profile-report-db-after-is-usable-test
  (with-temp-dl-conn
    {:wal? true
     :wal-durability-profile :strict}
    (fn [c]
      (let [rp (d/transact! c [{:k 1}])]
        (is (= 1 (d/q '[:find (count ?e) .
                        :where [?e :k]]
                      (:db-after rp))))
        (is (= 1 (d/q '[:find (count ?e) .
                        :where [?e :k]]
                      (d/db c))))))))

(deftest strict-profile-concurrent-writers-smoke-test
  (with-temp-dl-conn
    {:wal? true
     :wal-durability-profile :strict}
    (fn [c]
      (let [threads    4
            per-thread 20
            futs       (mapv (fn [t]
                               (future
                                 (dotimes [i per-thread]
                                   (d/transact! c [{:k (+ i (* t per-thread))}]))))
                             (range threads))]
        (doseq [f futs] @f)
        (is (= (* threads per-thread)
               (d/q '[:find (count ?e) .
                      :where [?e :k]]
                    (d/db c))))))))

(deftest wal-strict-profile-transact-async-test
  (with-temp-dl-conn
    {:wal? true
     :wal-durability-profile :strict}
    (fn [c]
      (is (true? (:wal? (env-opts c))))
      (is (= :strict (:wal-durability-profile (env-opts c))))
      (let [n    40
            futs (mapv (fn [i]
                         (d/transact-async c [{:k i}]))
                       (range n))]
        (doseq [f futs] @f)
        (is (= n
               (d/q '[:find (count ?e) .
                      :where [?e :k]]
                    (d/db c))))))))

(deftest wal-relaxed-profile-transact-async-test
  (with-temp-dl-conn
    {:wal? true
     :wal-durability-profile :relaxed}
    (fn [c]
      (is (true? (:wal? (env-opts c))))
      (is (= :relaxed (:wal-durability-profile (env-opts c))))
      (let [n    40
            futs (mapv (fn [i]
                         (d/transact-async c [{:k i}]))
                       (range n))]
        (doseq [f futs] @f)
        (is (= n
               (d/q '[:find (count ?e) .
                      :where [?e :k]]
                    (d/db c))))))))
