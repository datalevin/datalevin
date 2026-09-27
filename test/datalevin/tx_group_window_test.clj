(ns datalevin.tx-group-window-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-group :as group])
  (:import [datalevin.tx_group Group]
           [java.util.concurrent ConcurrentLinkedQueue Semaphore]))

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
