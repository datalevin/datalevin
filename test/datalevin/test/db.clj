(ns ^:no-doc datalevin.test.db
  (:require
   [clojure.data]
   [clojure.test :as t :refer [is are deftest testing]]
   [datalevin.binding.cpp]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as l]
   [datalevin.query.plan :as qplan]
   [datalevin.util :as u :refer [defrecord-updatable]])
  (:import
   [java.util UUID]
   [java.util.concurrent Callable ExecutorService Future TimeUnit]))

;;
;; verify that defrecord-updatable works with compiler/core macro configuration
;; define dummy class which redefines hash, could produce either
;; compiler or runtime error
;;
(defrecord-updatable HashBeef [x]
  clojure.lang.IHashEq (hasheq [hb] 0xBEEF))

(deftest test-defrecord-updatable
  (is (= 0xBEEF (-> (map->HashBeef {:x :ignored}) hash))))

(deftest deterministic-sample-cache-key-includes-seed
  (let [cache-key
        (fn [seed]
          (binding [u/*reservoir-sampling-seed* seed]
            (#'db/sample-init-cache-key
             :movie/title 1000 [[[:closed "A"] [:closed "Z"]]] nil true)))]
    (is (= (cache-key 42) (cache-key 42)))
    (is (not= (cache-key 42) (cache-key 43)))
    (is (not= (cache-key nil) (cache-key 42)))))

(deftest deterministic-entity-sample-cache-key-includes-seed-and-budget
  (let [cache-key
        (fn [seed budget]
          (binding [u/*reservoir-sampling-seed* seed
                    c/init-exec-size-threshold budget]
            (#'db/e-sample-cache-key :movie/title 10000)))]
    (is (= (cache-key 42 1000) (cache-key 42 1000)))
    (is (not= (cache-key 42 1000) (cache-key 43 1000)))
    (is (not= (cache-key 42 1000) (cache-key 42 4000)))))

(deftest query-plan-pool-shuts-down-with-last-lmdb-executors
  (let [^ExecutorService pool (#'qplan/get-pipe-thread-pool)]
    (is (= :started
           (.get ^Future (.submit pool ^Callable (fn [] :started)))))
    (is (not (.isShutdown pool)))
    (l/shutdown-last-lmdb-executors!)
    (is (.isShutdown pool))
    (is (.awaitTermination pool 1 TimeUnit/SECONDS))
    (let [^ExecutorService new-pool (#'qplan/get-pipe-thread-pool)]
      (try
        (is (not (identical? pool new-pool)))
        (is (not (.isShutdown new-pool)))
        (is (= :restarted
               (.get ^Future
                     (.submit new-pool ^Callable (fn [] :restarted)))))
        (finally
          (qplan/shutdown-pipe-thread-pool!))))))

(deftest read-transaction-acquisition-preserves-cause
  (let [dir      "/tmp/datalevin-reader-error"
        cause    (IllegalArgumentException. "native reader failure")
        error-fn (ns-resolve 'datalevin.binding.cpp
                             'read-transaction-acquisition-error)
        error    (error-fn dir cause)]
    (is (= "Failed to acquire LMDB read transaction: native reader failure"
           (ex-message error)))
    (is (= {:type      :lmdb/read-transaction
            :operation :acquire
            :dir       dir
            :cause     "native reader failure"}
           (ex-data error)))
    (is (identical? cause (ex-cause error)))))

(deftest stale-ha-replay-does-not-regress-materialized-payload
  (let [dir (u/tmp-dir (str "stale-ha-replay-" (UUID/randomUUID)))]
    (try
      (let [kv-store (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi kv-store "values")
          (is (= :transacted
                 (d/transact-kv kv-store
                                [[:put "values" :key :older]])))
          (let [older-record (last (d/open-tx-log kv-store 1))]
            (is (= :transacted
                   (d/transact-kv kv-store
                                  [[:put "values" :key :newer]])))
            (let [newer-record (last (d/open-tx-log kv-store 1))
                  result (kv/mirror-replayed-txlog-record!
                          kv-store
                          older-record
                          nil
                          {:replay-skipped? true})]
              (is (< (long (:lsn older-record))
                     (long (:lsn newer-record))))
              (is (:skipped? result))
              (is (not (:replayed? result)))
              (is (= :newer (d/get-value kv-store "values" :key)))))
          (finally
            (d/close-kv kv-store))))
      (finally
        (u/delete-files dir)))))

(deftest ha-replay-repairs-wal-ahead-of-materialized-payload
  (let [dir (u/tmp-dir (str "ha-replay-unapplied-tail-"
                            (UUID/randomUUID)))]
    (try
      (let [kv-store (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi kv-store "values")
          (is (= :transacted
                 (d/transact-kv kv-store
                                [[:put "values" :key :expected]])))
          (let [record (last (d/open-tx-log kv-store 1))
                lsn (long (:lsn record))]
            ;; Model a crash after WAL append but before the corresponding
            ;; payload and materialization floor were committed to LMDB.
            (is (= :transacted
                   (kv/transact-kv-without-txlog!
                    kv-store
                    [[:put "values" :key :stale]
                     [:put c/kv-info c/wal-local-payload-lsn
                      (dec lsn) :keyword :data]])))
            (let [result (kv/mirror-replayed-txlog-record!
                          kv-store
                          record
                          nil
                          {:replay-skipped? true})]
              (is (:skipped? result))
              (is (:replayed? result))
              (is (= :expected (d/get-value kv-store "values" :key)))
              (is (= lsn
                     (long (i/get-value kv-store
                                        c/kv-info
                                        c/wal-local-payload-lsn
                                        :keyword
                                        :data))))))
          (finally
            (d/close-kv kv-store))))
      (finally
        (u/delete-files dir)))))

(defn- now [] (System/currentTimeMillis))

(deftest test-uuid
  (let [now-ms (loop []
                 (let [ts (now)]
                   (if (> ^long (mod ts 1000) 900) ;; sleeping over end of a second
                     (recur)
                     ts)))
        now    (int (/ ^long now-ms 1000))]
    (is (= (* 1000 now) (d/squuid-time-millis (d/squuid))))
    (is (not= (d/squuid) (d/squuid)))
    (is (= (subs (str (d/squuid)) 0 8)
           (subs (str (d/squuid)) 0 8)))))
