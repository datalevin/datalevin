(ns ycsb-bench.lifecycle-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.server :as server]
            [ycsb-bench.core-test :as shared]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.store :as store])
  (:import [java.util.concurrent CountDownLatch ExecutionException TimeUnit]
           [java.util.concurrent.atomic AtomicInteger]))

(defn- check-cancellation! [reason]
  (let [entered (CountDownLatch. 1)
        cancelled (CountDownLatch. 1)
        release (CountDownLatch. 1)
        completed (CountDownLatch. 1)
        calls (AtomicInteger.)
        active (AtomicInteger.)
        closed (atom nil)
        used-after-close? (atom false)
        result (promise)
        db (reify store/Records
             (put-records! [_ _] nil)
             (record-count [_] 1)
             (read-record [_ _]
               (.incrementAndGet active)
               (try
                 (let [call (.incrementAndGet calls)]
                   (cond
                     (= call 1)
                     (do
                       (.countDown entered)
                       ;; Model a native operation that consumes interruptions
                       ;; but continues using its store until explicitly released.
                       (loop []
                         (when-not (try (.await release) true
                                        (catch InterruptedException _
                                          (.countDown cancelled)
                                          false))
                           (recur))))
                     (and (= reason :worker-failure) (= call 2))
                     (do (.await entered)
                         (throw (ex-info "Injected worker failure" {})))))
                 (when @closed (reset! used-after-close? true))
                 ["a"]
                 (finally (.decrementAndGet active))))
             (close-store! [_] (reset! closed {:active (.get active)})))
        coordinator
        (Thread.
          ^Runnable
          (fn []
            (try
              (runner/run-case!
                {:api :kv :mode :embedded :workload :c :distribution :uniform
                 :records 1 :ops 3 :warmup 0 :field-count 1 :field-length 1
                 :threads (if (= reason :worker-failure) 2 1)
                 :timeout-ms 30 :phase-timeout-ms (if (= reason :timeout) 500 10000)})
              (deliver result {:unexpected-success? true})
              (catch Throwable t
                (deliver result {:error t :interrupted? (.isInterrupted (Thread/currentThread))}))
              (finally (.countDown completed)))))]
    (with-redefs-fn {(ns-resolve 'ycsb-bench.store 'open-store!) (fn [& _] db)}
      (fn []
        (.start coordinator)
        (try
          (is (.await entered 5 TimeUnit/SECONDS))
          (when (= reason :interruption) (.interrupt coordinator))
          (is (.await cancelled 5 TimeUnit/SECONDS))
          ;; Exceed the old 30 ms shutdown limit while the operation is active.
          (is (not (.await completed 150 TimeUnit/MILLISECONDS)))
          (is (nil? @closed))
          ;; Interrupting the join itself must not permit premature cleanup.
          (.interrupt coordinator)
          (is (not (.await completed 150 TimeUnit/MILLISECONDS)))
          (is (nil? @closed))
          (finally
            (.countDown release)
            (.join coordinator 5000)))))
    (is (not (.isAlive coordinator)))
    (is (= {:active 0} @closed))
    (is (false? @used-after-close?))
    (is (= (if (= reason :worker-failure) 2 1) (.get calls))
        "Clearing an interrupt must not allow another operation after cancellation")
    (let [{:keys [error interrupted?]} (deref result 1000 {})]
      (is (true? interrupted?))
      (case reason
        :timeout (is (= "Benchmark phase timed out" (some-> error ex-message)))
        :worker-failure
        (do (is (instance? ExecutionException error))
            (is (= "Injected worker failure" (some-> error ex-cause ex-message))))
        :interruption (is (instance? InterruptedException error))))))

(deftest cancellation-waits-before-store-cleanup-test
  (doseq [reason [:timeout :worker-failure :interruption]]
    (testing (name reason) (check-cancellation! reason))))

(deftest remote-password-uri-roundtrip-test
  ;; These characters exercise spaces, literal '+', URI delimiters, percent
  ;; escaping and Unicode through actual authentication for both remote APIs.
  (with-redefs [server/get-default-password (constantly "test pass +:/?@#% 雪")]
    (doseq [api [:kv :datalog]]
      (testing (name api)
        (store/with-store (assoc shared/small-options :api api :mode :remote)
                         shared/check-adapter!)))))
