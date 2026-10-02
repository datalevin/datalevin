(ns datalevin.tx-group-batch-native-test
  "Contract tests for the native branch adapter.

  A fake application writer stands in for `cpp/apply-native-range!` so the
  adapter's row gathering, WAL-policy gating and result mapping are checked
  without a real LMDB transaction."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.batch.native :as native])
  (:import [java.util.concurrent ConcurrentLinkedQueue]))

(def ^:private limits
  {:wal-pending-max-requests 64
   :wal-pending-max-bytes 1048576
   :write-batch-size 8
   :write-batch-max-bytes 12288
   :wal-rmw-max-bytes 4096})

(defn- index-of [^ConcurrentLinkedQueue events wanted]
  (first (keep-indexed (fn [i e] (when (= e wanted) i)) (vec events))))

(defn- fake-wal [events]
  (reify executor/IWalBranch
    (append-group! [_ _batch lsn]
      (.add events [:append lsn])
      :token)
    (complete-policy! [_ _token _deadline-ns]
      (.add events :policy)
      true)))

(deftest native-adapter-applies-rows-and-gates-metadata-on-wal-policy
  (let [rows [[:put "dbi" 1 "v"]]
        events (ConcurrentLinkedQueue.)
        apply-fn (fn [_raw body before-commit]
                   (.add events :apply)
                   (body :fake-wdb)
                   (before-commit :fake-wdb {:operation :close-transact-kv})
                   (.add events :commit))
        nat (native/branch :raw
                           {:apply-fn apply-fn
                            :transact! (fn [wdb applied]
                                         (.add events [:rows wdb (vec applied)]))
                            :write-metadata! (fn [_wdb token]
                                               (.add events [:metadata token]))})
        lsn (atom 0)
        c (batch/create (executor/create (fake-wal events) nat #(swap! lsn inc))
                        {:limits (charge/resolve-limits limits)})
        value (batch/submit! c {:allowance 1024
                                :data {:rows rows
                                       :wal-body :body
                                       :result :ok}})]
    (is (= :ok value))
    (is (= 1 @lsn))
    (testing "the frozen rows reach the native writer unchanged"
      (is (some #{[:rows :fake-wdb rows]} (vec events))))
    (testing "WAL policy completes before metadata and commit"
      (let [a (index-of events [:append 1])
            p (index-of events :policy)
            m (index-of events [:metadata :token])
            k (index-of events :commit)]
        (is (every? some? [a p m k]))
        (is (< a p m k))))))

(deftest a-wal-policy-failure-aborts-native-commit
  (let [events (ConcurrentLinkedQueue.)
        apply-fn (fn [_raw body before-commit]
                   (.add events :apply)
                   (body :fake-wdb)
                   (before-commit :fake-wdb {:operation :close-transact-kv})
                   (.add events :commit))
        nat (native/branch :raw
                           {:apply-fn apply-fn
                            :transact! (fn [_wdb _rows] (.add events :rows))})
        failing-wal (reify executor/IWalBranch
                      (append-group! [_ _ _batch] :token)
                      (complete-policy! [_ _ _deadline-ns]
                        (throw (ex-info "wal failed"
                                        {:error :txlog/write-indeterminate
                                         :outcome :indeterminate}))))
        lsn (atom 0)
        c (batch/create (executor/create failing-wal nat #(swap! lsn inc)
                                         {:schedule-fn (constantly :parallel)})
                        {:limits (charge/resolve-limits limits)})
        thrown (try (batch/submit! c {:allowance 1024
                                      :data {:rows [[:put "dbi" 1 "v"]]
                                             :wal-body :body
                                             :result :ok}})
                    nil
                    (catch Throwable t t))]
    (is (instance? Throwable thrown))
    (is (= :txlog/write-indeterminate (:error (ex-data thrown))))
    (testing "rows applied but the native transaction never committed"
      (is (some #{:rows} (vec events)))
      (is (not (some #{:commit} (vec events)))))
    (is (not (batch/serving? c)))))