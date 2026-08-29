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
   [java.lang Thread$State]
   [java.util UUID]
   [java.util.concurrent Callable CountDownLatch ExecutorService Future
    TimeUnit]))

;;
;; verify that defrecord-updatable works with compiler/core macro configuration
;; define dummy class which redefines hash, could produce either
;; compiler or runtime error
;;
(defrecord-updatable HashBeef [x]
  clojure.lang.IHashEq (hasheq [hb] 0xBEEF))

(def ^:private stale-query-cache-coordination (atom nil))

(defn stale-query-cache-pass?
  [value]
  (when-let [{:keys [query-value release]} @stale-query-cache-coordination]
    (deliver query-value value)
    (deref release 5000 ::timeout))
  true)

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

(deftest empty-query-cache-skips-datom-touch-summary
  (let [dir  (u/tmp-dir (str "empty-query-cache-" (UUID/randomUUID)))
        conn (d/create-conn dir {:item/value {}})]
    (try
      (let [[cache generation] (db/cache-token (:store @conn))
            summary-var        (ns-resolve 'datalevin.db 'tx-touch-summary)]
        (.clear ^datalevin.utl.LRUCache cache)
        (with-redefs-fn
          {summary-var
           (fn [_]
             (throw (ex-info "empty cache should not summarize datoms" {})))}
          #(d/transact! conn [{:db/id 1 :item/value 100}]))
        (is (> (.generation ^datalevin.utl.LRUCache cache)
               (long generation))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest stale-query-result-cannot-repopulate-cache-after-commit
  (let [dir          (u/tmp-dir
                      (str "stale-query-cache-" (UUID/randomUUID)))
        conn         (d/create-conn dir {:item/value {}})
        query        '[:find ?value .
                       :where
                       [1 :item/value ?value]
                       [(datalevin.test.db/stale-query-cache-pass? ?value)
                        ?passed]
                       [(= true ?passed)]]
        query-result (promise)
        release      (promise)
        stale-query  (atom nil)]
    (try
      (d/transact! conn [{:db/id 1 :item/value 100}])
      (db/refresh-cache (:store @conn))
      (let [db-before @conn]
        (reset! stale-query-cache-coordination
                {:query-value query-result
                 :release release})
        (reset! stale-query
                (future (d/q query db-before)))
        (is (= 100 (deref query-result 5000 ::timeout)))
        (d/transact! conn [{:db/id 1 :item/value 101}])
        (deliver release true)
        (is (= 100 (deref @stale-query 5000 ::timeout)))
        ;; The old reader may return its own snapshot, but must not publish that
        ;; result into the cache after the committing transaction invalidated it.
        (is (= 101 (d/q query @conn))))
      (finally
        (reset! stale-query-cache-coordination nil)
        (deliver release true)
        (when-let [query-future @stale-query]
          (future-cancel query-future))
        (d/close conn)
        (u/delete-files dir)))))

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

(defn- wait-until-blocked
  [^Thread thread]
  (loop [attempt 0]
    (cond
      (= Thread$State/BLOCKED (.getState thread)) true
      (not (.isAlive thread))                     false
      (< attempt 1000) (do
                         (Thread/sleep 1)
                         (recur (unchecked-inc-int attempt)))
      :else false)))

(deftest close-kv-locks-before-native-teardown
  (let [dir           (u/tmp-dir
                       (str "close-kv-lock-test-" (UUID/randomUUID)))
        lmdb          (d/open-kv dir)
        write-lock    (l/write-txn lmdb)
        hooks         (var-get (ns-resolve 'datalevin.binding.cpp
                                           'shutdown-hooks))
        close-started (CountDownLatch. 1)
        close-error   (atom nil)
        closer        (Thread.
                       (fn []
                         (.countDown close-started)
                         (try
                           (d/close-kv lmdb)
                           (catch Throwable e
                             (reset! close-error e)))))]
    (try
      (is (contains? @hooks dir))
      (locking write-lock
        (.start closer)
        (is (.await close-started 1 TimeUnit/SECONDS))
        (is (wait-until-blocked closer))
        ;; Closing used to unregister the hook and release native resources
        ;; before acquiring this lock, allowing a shutdown-hook double-close.
        (is (contains? @hooks dir)))
      (.join closer 5000)
      (is (not (.isAlive closer)))
      (is (nil? @close-error))
      (is (d/closed-kv? lmdb))
      (is (not (contains? @hooks dir)))
      (finally
        (when (.isAlive closer)
          (.interrupt closer)
          (.join closer 5000))
        (when-not (d/closed-kv? lmdb)
          (d/close-kv lmdb))
        (u/delete-files dir)))))

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
