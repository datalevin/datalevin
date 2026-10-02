(ns datalevin.tx-group-batch-notification-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.phase :as phase])
  (:import [java.util.concurrent CountDownLatch TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean AtomicLong]
           [java.util.concurrent.locks LockSupport]))

(defn descriptor [thread]
  (batch/->Descriptor nil (volatile! :value) nil 1024 0
                      (AtomicLong. 1024) (AtomicBoolean. false)
                      (volatile! nil) thread
                      (AtomicBoolean. false) (AtomicBoolean. false)))

(deftest retained-predicates-survive-a-consumed-thread-permit
  (doseq [outcome [:result :leadership]]
    (testing (name outcome)
      (let [done (CountDownLatch. 1)
            c (batch/create (constantly nil))
            thread (Thread.
                    (fn []
                      (let [d (descriptor (Thread/currentThread))]
                        (if (= outcome :result)
                          (vreset! (.result d) [true :done])
                          (.add (.ready c) d))
                        ;; Model another synchronizer consuming the wake. The
                        ;; state predicate must prevent the subsequent park.
                        (LockSupport/unpark (Thread/currentThread))
                        (LockSupport/park)
                        (#'batch/park-for-progress! c d 0)
                        (.countDown done))))]
        (.start thread)
        (try
          (is (.await done 2 TimeUnit/SECONDS))
          (finally (.interrupt thread) (.join thread 2000)))))))

(deftest publication-wakes-an-already-parked-submitter
  (let [c (batch/create (constantly nil))
        slot (promise)
        done (CountDownLatch. 1)
        thread (Thread.
                (fn []
                  (let [d (descriptor (Thread/currentThread))]
                    (deliver slot d)
                    (#'batch/park-for-progress! c d 0)
                    (.countDown done))))]
    (.set (.active c) true)
    (.start thread)
    (try
      (let [d (deref slot 2000 nil)
            deadline (+ (System/nanoTime) 2000000000)]
        (is (some? d))
        (loop []
          (when (and (not (identical? d (LockSupport/getBlocker thread)))
                     (< (System/nanoTime) deadline))
            (Thread/sleep 1)
            (recur)))
        (is (identical? d (LockSupport/getBlocker thread)))
        (#'batch/deliver! d [true :done])
        (is (.await done 2 TimeUnit/SECONDS))
        (is (= [true :done] @(.result d))))
      (finally (.interrupt thread) (.join thread 2000)))))

(deftest spurious-wakes-do-not-complete-a-request-or-lose-its-result
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        queued (CountDownLatch. 1)
        c (batch/create
           (fn [b]
             (when (= :leader (batch/data (batch/batch-at b 0)))
               (.countDown entered)
               (.await release 5 TimeUnit/SECONDS))
             (object-array (map #(batch/data (batch/batch-at b %))
                                (range (batch/batch-count b))))))
        uninstall (phase/observe!
                   (fn [event d]
                     (when (and (= event :ready-published)
                                (= :follower (batch/data d)))
                       (.countDown queued))))
        leader (future (batch/submit! c {:allowance 1024 :data :leader}))
        result (promise)
        follower (Thread. #(deliver result
                                    (try (batch/submit! c {:allowance 1024 :data :follower})
                                         (catch Throwable t t))))]
    (try
      (is (.await entered 2 TimeUnit/SECONDS))
      (.start follower)
      (is (.await queued 2 TimeUnit/SECONDS))
      (dotimes [_ 32] (LockSupport/unpark follower))
      (is (not (realized? result)))
      (.countDown release)
      (is (= :leader (deref leader 2000 ::timeout)))
      (is (= :follower (deref result 2000 ::timeout)))
      (is (batch/await-quiescence! c 1000))
      (finally
        (.countDown release)
        (.interrupt follower)
        (.join follower 2000)
        (uninstall)))))
