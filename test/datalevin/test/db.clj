(ns ^:no-doc datalevin.test.db
  (:require
   [clojure.data]
   [clojure.test :as t :refer [is are deftest testing]]
   [datalevin.binding.cpp]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
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
