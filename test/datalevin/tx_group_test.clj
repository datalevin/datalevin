(ns datalevin.tx-group-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-group :as group])
  (:import [datalevin.tx_group Group]
           [java.util.concurrent ConcurrentLinkedQueue CountDownLatch TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean AtomicInteger]
           [java.util.concurrent.locks ReentrantLock]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *submitted-value* nil)

(defn- await! [pred]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (cond
        (pred) true
        (> (System/nanoTime) deadline) false
        :else (do (Thread/sleep 1) (recur))))))

(defn- queued [^Group g]
  (.size ^ConcurrentLinkedQueue (.-queue g)))

(deftest completed-followers-return-before-admission-is-released
  (let [acquisitions (AtomicInteger.)
        first-entered (promise) release-first (promise)
        commit-entered (promise) release-commit (promise)
        unlock-entered (promise) release-unlock (promise)
        lock (proxy [ReentrantLock] [true]
               (lock []
                 (let [^ReentrantLock this this] (proxy-super lock))
                 (.incrementAndGet acquisitions))
               (unlock []
                 (when (= 2 (.get acquisitions))
                   (deliver unlock-entered true)
                   (assert (deref release-unlock 10000 false)))
                 (let [^ReentrantLock this this] (proxy-super unlock))))
        g (Group. lock (ConcurrentLinkedQueue.) 16 (AtomicBoolean. false))
        batches (atom [])
        returned (CountDownLatch. 8)
        run-group (fn [^FastList requests]
                    (swap! batches conj (.size requests))
                    (let [results (group/execute requests nil)]
                      (when (= 8 (.size requests))
                        (deliver commit-entered true)
                        (assert (deref release-commit 10000 false)))
                      results))
        first-job (future
                    (group/submit! g run-group
                                   (fn [_]
                                     (deliver first-entered true)
                                     (assert (deref release-first 10000 false))
                                     :first)))
        jobs (atom [])]
    (try
      (is (deref first-entered 10000 false))
      (reset! jobs
              (mapv (fn [n]
                      (future
                        (binding [*submitted-value* n]
                          (let [result (group/submit!
                                         g run-group
                                         (fn [_] [n *submitted-value*]))]
                            (.countDown returned)
                            result))))
                    (range 8)))
      (is (await! #(= 8 (queued g))))
      (deliver release-first true)
      (is (deref commit-entered 10000 false))
      (is (= 8 (.getCount returned)) "Execution alone must not acknowledge writes")
      (deliver release-commit true)
      (is (deref unlock-entered 10000 false))
      (is (await! #(= 1 (.getCount returned)))
          "All seven completed followers return while their leader holds admission")
      (is (= 2 (.get acquisitions)))
      (deliver release-unlock true)
      (is (= :first (deref first-job 10000 ::timeout)))
      (is (= (mapv #(vector % %) (range 8))
             (mapv #(deref % 10000 ::timeout) @jobs)))
      (is (= [1 8] @batches))
      (is (= 2 (.get acquisitions)) "Followers never acquire admission")
      (finally
        (doseq [gate [release-first release-commit release-unlock]]
          (deliver gate true))
        (doseq [job (cons first-job @jobs)] (deref job 10000 nil))))))

(deftest bounded-groups-hand-off-without-losing-requests
  (let [g (group/create 3)
        executing (AtomicInteger.)
        overlapping? (atom false)
        batches (atom [])
        calls (atom {})
        run-group (fn [^FastList requests]
                    (when-not (= 1 (.incrementAndGet executing))
                      (reset! overlapping? true))
                    (try
                      (swap! batches conj (.size requests))
                      (Thread/yield)
                      (group/execute requests nil)
                      (finally (.decrementAndGet executing))))]
    (dotimes [round 50]
      (let [start (promise)
            jobs (mapv (fn [n]
                         (future
                           @start
                           (group/submit! g run-group
                                          (fn [_]
                                            (swap! calls update [round n] (fnil inc 0))
                                            (case (long (mod n 3)) 0 nil 1 false n)))))
                       (range 12))]
        (deliver start true)
        (is (= (mapv #(case (long (mod % 3)) 0 nil 1 false %) (range 12))
               (mapv #(deref % 10000 ::timeout) jobs)))))
    (is (= 600 (count @calls)))
    (is (every? #(= 1 %) (vals @calls)))
    (is (every? #(<= 1 % 3) @batches))
    (is (false? @overlapping?))
    (is (zero? (queued g)))))

(deftest interrupted-followers-still-wait-for-their-result
  (let [g (group/create 8)
        lock ^ReentrantLock (.-lock ^Group g)
        result (promise)
        done (CountDownLatch. 1)
        run-group #(group/execute % nil)
        first-job (atom nil)
        follower (Thread.
                   (fn []
                     (try
                       (.interrupt (Thread/currentThread))
                       (let [value (group/submit! g run-group (constantly :done))]
                         (deliver result [value (.isInterrupted (Thread/currentThread))]))
                       (catch Throwable t (deliver result t))
                       (finally (.countDown done)))))]
    (.lock lock)
    (try
      (reset! first-job (future (group/submit! g run-group (constantly :first))))
      (is (await! #(.hasQueuedThreads lock)))
      (.start follower)
      (is (await! #(= 2 (queued g))))
      (is (not (realized? result)))
      (finally (.unlock lock)))
    (is (.await done 10 TimeUnit/SECONDS))
    (is (= [:done true] (deref result 10000 ::timeout)))
    (is (= :first (deref @first-job 10000 ::timeout)))))

(deftest commit-failure-signals-all-followers-without-retrying
  (let [g (group/create 8)
        lock ^ReentrantLock (.-lock ^Group g)
        failure (ex-info "commit failed" {})
        commits (atom 0)
        calls (atom 0)
        run-group (fn [requests]
                    (group/execute requests nil)
                    (swap! commits inc)
                    (throw failure))
        jobs (atom [])]
    (.lock lock)
    (try
      (reset! jobs
              (mapv (fn [_]
                      (future
                        (try
                          (group/submit! g run-group (fn [_] (swap! calls inc)))
                          (catch Throwable t t))))
                    (range 8)))
      (is (await! #(= 8 (queued g))))
      (finally (.unlock lock)))
    (is (every? #(identical? failure %) (mapv #(deref % 10000 ::timeout) @jobs)))
    (is (= 8 @calls))
    (is (= 1 @commits))
    (is (= :recovered (group/submit! g #(group/execute % nil)
                                   (constantly :recovered))))))
