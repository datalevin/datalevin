(ns datalevin.tx-group-window-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-group :as group])
  (:import [datalevin.tx_group Group]
           [java.util.concurrent ConcurrentLinkedQueue Semaphore]
           [java.util.concurrent.locks ReentrantLock]))

(def ^:dynamic *request-binding* nil)

(deftest specialization-requires-matching-submitter-bindings
  (doseq [different? [false true]]
    (let [g (group/create 2)
          ^ReentrantLock lock (.-lock ^Group g)
          ^ConcurrentLinkedQueue queue (.-queue ^Group g)
          specialized? (atom nil)
          jobs (atom [])
          runner (fn [execute]
                   (reset! specialized? (some? (group/batch-data execute)))
                   (execute nil))]
      (.lock lock)
      (try
        (doseq [idx (range 2)]
          (swap! jobs conj
                 (future
                   (binding [*request-binding* (if different? idx :shared)]
                     (group/submit! g runner
                                    (with-meta (fn [_] *request-binding*)
                                      {::group/data idx})))))
          (is (loop [attempt 0]
                (cond (= (inc idx) (.size queue)) true
                      (= attempt 1000) false
                      :else (do (Thread/sleep 1) (recur (inc attempt)))))))
        (finally (.unlock lock)))
      (is (= (if different? [0 1] [:shared :shared])
             (mapv #(deref % 5000 ::timeout) @jobs)))
      (is (= (not different?) @specialized?)))))

(deftest idle-collection-expires-without-another-writer
  (let [g (group/create 8)
        commits (atom 0)
        job (future (group/submit! g
                                  (fn [execute]
                                    (let [result (execute nil)]
                                      (swap! commits inc)
                                      result))
                                  (constantly :done)
                                  1000000))]
    (is (= :done (deref job 5000 ::timeout)))
    (is (= 1 @commits))
    (is (.isEmpty ^ConcurrentLinkedQueue (.-queue ^Group g)))))

(deftest collection-retains-interruption-and-waits-for-commit-confirmation
  (let [g (group/create 8)
        entered (promise) release (Semaphore. 0)
        job (future
              (.interrupt (Thread/currentThread))
              (try
                (let [result (group/submit!
                              g
                              (fn [execute]
                                (let [result (execute nil)]
                                  (group/confirm-after-commit!
                                   #(do (deliver entered true)
                                        (.acquireUninterruptibly release)))
                                  result))
                              (constantly :done)
                              1000000)]
                  [result (.isInterrupted (Thread/currentThread))])
                (finally (Thread/interrupted))))]
    (try
      (is (deref entered 5000 false))
      (is (not (realized? job)))
      (.release release)
      (is (= [:done true] (deref job 5000 ::timeout)))
      (finally (.release release)))))
