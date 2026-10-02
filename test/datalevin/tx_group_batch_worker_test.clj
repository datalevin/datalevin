(ns datalevin.tx-group-batch-worker-test
  "Contract tests for the single-thread WAL maintenance worker.

  Deterministic service/deadline fakes let the task loop, deadline-driven
  maintenance and close behavior be checked without a WAL runtime."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.batch.worker :as worker])
  (:import [java.util.concurrent CountDownLatch Executor
            RejectedExecutionException TimeUnit]))

(defn- submit! [w task]
  (.execute ^Executor (:executor w) task))

(defn- await! [p timeout-ms]
  (deref p timeout-ms ::timeout))

(defn- await-parked! [thread]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (cond
        (and @thread (= Thread$State/TIMED_WAITING
                        (.getState ^Thread @thread))) true
        (>= (System/nanoTime) deadline) false
        :else (do (Thread/sleep 1) (recur))))))

(deftest worker-executes-dispatched-wal-tasks
  (let [ran (promise)
        w (worker/create :name "test-wal-tasks")]
    (try
      (submit! w (fn [] (deliver ran true)))
      (is (true? (await! ran 5000)))
      (finally ((:close! w))))))

(deftest maintenance-wakes-coalesce-while-the-worker-is-blocked
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        checks (atom 0)
        observed (promise)
        w (worker/create :name "test-wal-coalesced-wake"
                         :deadline! #(do (swap! checks inc) 0))]
    (try
      (submit! w (fn [] (.countDown entered)
                       (.await release 5 TimeUnit/SECONDS)))
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [before @checks]
        (dotimes [_ 10000] ((:wake! w)))
        ;; FIFO puts this behind every queued notification. It should cross
        ;; one wake token, rather than doing work for 10,000 expired batches.
        (submit! w (fn [] (deliver observed (- @checks before))))
        (.countDown release)
        (let [n (deref observed 5000 ::timeout)]
          (is (number? n))
          (is (and (number? n) (<= n 8))))
        (is (true? ((:close! w))))
        (dotimes [_ 100] ((:wake! w)))
        (is (true? ((:close! w)))))
      (finally (.countDown release) ((:close! w))))))

(deftest worker-services-maintenance-when-the-deadline-is-due
  (let [deadline (atom (System/nanoTime))
        serviced (atom 0)
        entered (promise)
        w (worker/create
           :name "test-wal-service"
           :deadline! (fn [] @deadline)
           :service! (fn []
                       (swap! serviced inc)
                       (reset! deadline 0)
                       (deliver entered true)))]
    (try
      (is (true? (await! entered 5000)))
      (testing "after servicing, the settled deadline does not spin"
        (Thread/sleep 60)
        (is (= 1 @serviced)))
      (finally ((:close! w))))))

(deftest idle-worker-waits-for-the-deadline-and-can-be-woken
  (let [deadline (atom (+ (System/nanoTime) 1000000000))
        serviced (atom 0)
        entered (promise)
        w (worker/create
           :name "test-wal-idle"
           :deadline! (fn [] @deadline)
           :service! (fn []
                       (swap! serviced inc)
                       (reset! deadline 0)
                       (deliver entered true)))]
    (try
      (testing "a future deadline is not serviced early"
        (Thread/sleep 100)
        (is (zero? @serviced)))
      (testing "bringing the deadline forward and waking services it"
        (reset! deadline (System/nanoTime))
        ((:wake! w))
        (is (true? (await! entered 5000))))
      (finally ((:close! w))))))

(deftest a-closed-worker-rejects-new-tasks
  (let [w (worker/create :name "test-wal-closed")]
    ((:close! w))
    (is (thrown? RejectedExecutionException
                 (submit! w (fn [] nil))))))

(deftest for-wal-delegates-maintenance-to-the-branch
  (let [deadline (atom (System/nanoTime))
        serviced (promise)
        wal (reify executor/IWalMaintenance
              (maintenance-deadline-ns [_] @deadline)
              (service-maintenance! [_ _deadline-ns]
                (reset! deadline 0)
                (deliver serviced true)))
        w (worker/for-wal wal :name "test-wal-for-wal")]
    (try
      (is (true? (await! serviced 5000)))
      (finally ((:close! w))))))

(deftest worker-drains-accepted-tasks-before-reporting-stopped
  ;; Regression: close stopped the loop without draining, so an accepted task
  ;; never ran and a batch waiting on it could stay stuck permanently.
  (let [ran (atom [])
        block (CountDownLatch. 1)
        w (worker/create :name "test-wal-drain")]
    (submit! w (fn [] (swap! ran conj :a) (.await block 5 TimeUnit/SECONDS)))
    (Thread/sleep 50)
    (submit! w (fn [] (swap! ran conj :b)))
    (let [closed (future ((:close! w)))]
      (Thread/sleep 100)
      (testing "close waits for the queued task instead of discarding it"
        (is (false? (realized? closed))))
      (.countDown block)
      (is (true? (deref closed 5000 ::timeout))))
    (is (= [:a :b] @ran)
        "both the running and the queued accepted task ran")))

(deftest overdue-maintenance-precedes-queued-tasks
  ;; Regression: the loop polled queued tasks before servicing due maintenance,
  ;; so a later queued append delayed an already-overdue force.
  (let [events (atom [])
        deadline (atom 0)
        block (CountDownLatch. 1)
        w (worker/create
           :name "test-wal-priority"
           :deadline! (fn [] @deadline)
           :service! (fn []
                       (swap! events conj :service)
                       (reset! deadline 0)
                       true))]
    (try
      (submit! w (fn []
                   (swap! events conj :first)
                   (.await block 5 TimeUnit/SECONDS)))
      (Thread/sleep 50)
      ;; The force becomes overdue first, then a later append is queued.
      (reset! deadline (System/nanoTime))
      (submit! w (fn [] (swap! events conj :second)))
      (.countDown block)
      (Thread/sleep 200)
      (is (= [:first :service :second] @events)
          "an overdue force precedes the queued append task")
      (finally ((:close! w))))))

(deftest a-task-waking-a-parked-worker-rechecks-maintenance-first
  (doseq [scheduled? [false true]
          fail-service? [false true]]
    (testing (str {:scheduled? scheduled? :fail-service? fail-service?})
      (let [deadline (atom (if scheduled? (+ (System/nanoTime) 5000000000) 0))
            thread (atom nil)
            events (atom [])
            done (promise)
            failure (ex-info "maintenance failed" {})
            w (worker/create
               :name "test-wal-parked-priority"
               :deadline! (fn []
                            (reset! thread (Thread/currentThread))
                            @deadline)
               :service! (fn []
                           (swap! events conj :force)
                           (reset! deadline 0)
                           (when fail-service? (throw failure))
                           true))]
        (try
          (is (await-parked! thread))
          ;; Arm maintenance while the worker is inside the timed poll. The
          ;; queued task wakes it; running that task directly bypassed priority.
          (reset! deadline (System/nanoTime))
          (submit! w (fn []
                       (swap! events conj :append)
                       (deliver done true)))
          (is (true? (await! done 5000))
              "an accepted task still completes after a maintenance failure")
          (is (= [:force :append] @events))
          (is (identical? (when fail-service? failure) ((:failure w))))
          (finally ((:close! w))))))))

(deftest failed-uncleared-maintenance-does-not-starve-tasks-or-close
  ;; Regression: a due deadline whose service failed without clearing it was
  ;; retried forever, so accepted tasks never ran and close timed out.
  (doseq [advancing? [false true]]
    (testing (str {:advancing? advancing?})
      (let [events (atom [])
            failure (ex-info "maintenance failed" {})
            deadline (atom (System/nanoTime))
            w (worker/create
               :name "test-wal-settled-failure"
               :close-timeout-ms 2000
               :deadline! (fn [] (if advancing? (System/nanoTime) @deadline))
               :service! (fn []
                           (swap! events conj :service)
                           (throw failure)))
            ran (promise)]
        (try
          (submit! w (fn [] (deliver ran true)))
          (is (true? (await! ran 5000))
              "an accepted task runs after an uncleared failed service")
          (is (identical? failure ((:failure w))))
          (is (true? ((:close! w)))
              "close drains instead of timing out")
          (is (<= (count @events) 64)
              "the failed service is not retried in a tight loop")
          (finally ((:close! w))))))))
