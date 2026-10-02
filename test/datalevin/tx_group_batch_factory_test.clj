(ns datalevin.tx-group-batch-factory-test
  "Assembly tests for the new-protocol executor factory.

  Drives a real private-WAL runtime through the factory-built collector, with a
  fake native application writer so the WAL branch, policy completion, native
  gate and worker are exercised without LMDB."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.factory :as factory]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [java.io Closeable]))

(defn- with-runtime [opts f]
  (let [dir (u/tmp-dir (str "wal-factory-" (random-uuid)))
        {:keys [state]}
        (wal/init-runtime-state
         (merge {:dir dir :wal? true :wal-shared? false
                 :wal-durability-profile :strict :wal-sync-mode :fsync
                 :wal-segment-prealloc? false :wal-commit-wait-ms 10000}
                opts)
         nil)]
    (try (f state)
         (finally
           (doseq [key [:segment-channel :sync-lock-channel]]
             (when-let [^Closeable channel (some-> (get state key) deref)]
               (.close channel)))
           (u/delete-files dir)))))

(defn- control []
  {:check-admission! (fn [])
   :throw-if-fatal! (fn [_state])
   :before-append! (fn [_state])
   :mark-fatal! (fn [_state _error])})

(deftest factory-wires-a-real-wal-through-a-fake-native-branch
  (with-runtime
    {}
    (fn [state]
      (let [events (atom [])
            apply-fn (fn [_raw body before-commit]
                       (body :fake-wdb)
                       (before-commit :fake-wdb {:operation :close-transact-kv})
                       (swap! events conj :native-commit))
            runtime (factory/collector
                     state :raw
                     {:runtime-control (control)
                      :native-opts
                      {:apply-fn apply-fn
                       :transact! (fn [_wdb rows]
                                    (swap! events conj [:rows (vec rows)]))}})
            c (:collector runtime)
            rows [[:put "data" 1 "v" :long :string]]]
        (try
          (is (= :ok (batch/submit!
                      c {:allowance 1024
                         :data {:rows rows
                                :wal-body (wal/prepare-append-body rows {})
                                :result :ok}})))
          (testing "the WAL record is durable before the native commit"
            (is (= 1 (:last-durable-lsn
                      (wal/sync-manager-state (:sync-manager state)))))
            (is (= [:rows rows] (first @events)) "native rows reached the writer")
            (is (= :native-commit (second @events))))
          (testing "one physical WAL record holds the write"
            (is (= 1 (:last-appended-lsn
                      (wal/sync-manager-state (:sync-manager state))))))
          (finally
            ((:close! runtime))))))))

(deftest factory-worker-services-relaxed-maintenance-after-inline-writes
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 1 :wal-group-commit-ms 0
     :wal-full-prefix? true}
    (fn [state]
      (let [runtime (factory/collector
                     state :raw
                     {:runtime-control (control)
                      :native-opts
                      {:apply-fn (fn [_raw body before-commit]
                                   (body :fake-wdb)
                                   (before-commit :fake-wdb
                                                  {:operation :close-transact-kv}))
                       :transact! (fn [_wdb _rows])}})
            c (:collector runtime)
            rows [[:put "data" 1 "v" :long :string]]]
        (try
          (is (= :ok (batch/submit!
                      c {:allowance 1024
                         :data {:rows rows
                                :wal-body (wal/prepare-append-body rows {})
                                :result :ok}})))
          (is (loop [n 0]
                (cond
                  (= 1 (long (:last-durable-lsn
                              (wal/sync-manager-state (:sync-manager state))))) true
                  (< n 400) (do (Thread/sleep 5) (recur (inc n)))
                  :else false))
              "the factory worker forced the armed relaxed prefix")
          (is (zero? (:unsynced-count
                      (wal/sync-manager-state (:sync-manager state)))))
          (finally
            ((:close! runtime))))))))
