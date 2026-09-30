(ns datalevin.txlog.application-signals-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-state :as state]
            [datalevin.tx-state.lifetime :refer [with-lock]]
            [datalevin.txlog :as wal]
            [datalevin.txlog.application-test :as application])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(defn- join [job] (deref job 5000 ::timeout))
(defn- caught [f] (try (f) (catch Throwable e e)))

(defn- reclaimed-usage [engine]
  ;; Publication intentionally returns before background reclamation. Wait for
  ;; an active pass, then prune the completed prefix before checking its credits.
  (with-lock (:prune-lock engine)
    (state/prune! engine)
    (state/usage engine)))

(deftest sync-followers-stay-parked-while-native-application-is-busy
  (let [applying (promise) syncing (promise) owner-waiting (promise)
        release-native (CountDownLatch. 1) release-sync (CountDownLatch. 1)
        waiting (CountDownLatch. 7) waits (atom []) signals (atom []) applied (atom [])]
    (#'application/with-runtime
     {:application-max-records 1
      :apply-range! (fn [entries _]
                      (let [lsn @(:lsn (first entries))]
                        (swap! applied conj lsn)
                        (when (= 1 lsn)
                          (deliver applying true)
                          (.await release-native))))
      :hooks {:before-sync! (fn [_ round]
                              (when (= 2 (:target-lsn round))
                                (deliver syncing true)
                                (.await release-sync)))}}
     (fn [engine wal-state]
       (let [a (first (#'application/append-group! engine [0]))
             jobs (atom [(future (caught #(state/await! engine a)))])]
         (try
           (is (deref applying 5000 false))
           (let [entries (#'application/append-group! engine (range 1 9))]
             (with-redefs [state/phase!
                           (fn [event context]
                             (case event
                               :completion-waiting
                               (do (swap! waits conj @(:lsn context)) (.countDown waiting))
                               :completion-notified (swap! signals conj @(:lsn context))
                               :application-waiting (deliver owner-waiting true)
                               nil))]
               (swap! jobs conj (future (caught #(state/await! engine (first entries)))))
               (is (deref syncing 5000 false))
               (doseq [entry (rest entries)]
                 (swap! jobs conj (future (caught #(state/await! engine entry)))))
               (is (.await waiting 5 TimeUnit/SECONDS))
               (.countDown release-sync)
               (is (deref owner-waiting 5000 false))
               (is (= 2 @(:last-durable-lsn (:sync-manager wal-state))))
               (is (= (vec (repeat 7 2)) @waits)
                   "Followers do not wake and repark at the sync boundary")
               (is (empty? @signals))
               (is (not-any? realized? @jobs))
               (.countDown release-native)
               (is (= (vec (range 9)) (mapv join @jobs)))))
           (is (= [1 2] @applied))
           (is (empty? (:completion-waiters engine)))
           (is (empty? (:application-waiters engine)))
           (is (= {:bytes 0 :requests 0} (reclaimed-usage engine)))
           (finally
             (.countDown release-sync) (.countDown release-native)
             (doseq [job @jobs] (join job)))))))))

(deftest parked-sync-followers-help-stopped-owners-without-new-arrivals
  (doseq [stop-phase [:after-sync :application-claimed]]
    (let [stop-count (if (= stop-phase :after-sync) 1 2)
          syncing (promise) release-sync (CountDownLatch. 1)
          release-owners (CountDownLatch. 1)
          stopped (CountDownLatch. stop-count) waiting (CountDownLatch. 5)
          finished (CountDownLatch. (- 6 stop-count)) claims (atom 0) applications (atom 0)]
      (#'application/with-runtime
       {:apply-range! (fn [_ _] (swap! applications inc))
        :hooks {:before-sync! (fn [_ _]
                                (deliver syncing true)
                                (.await release-sync))}}
       (fn [engine _]
         (let [entries (#'application/append-group! engine (range 1 7))
               jobs (atom [])
               start! (fn [entry]
                        (swap! jobs conj
                               (future
                                 (try (caught #(state/await! engine entry))
                                      (finally (.countDown finished))))))]
           (with-redefs [state/phase!
                         (fn [event _]
                           (cond
                             (= event :completion-waiting) (.countDown waiting)
                             (and (= event stop-phase)
                                  (<= (swap! claims inc) stop-count))
                             (do
                               (.countDown stopped)
                               (.await release-owners))))]
             (try
               (start! (first entries))
               (is (deref syncing 5000 false))
               (doseq [entry (rest entries)] (start! entry))
               (is (.await waiting 5 TimeUnit/SECONDS))
               (.countDown release-sync)
               (is (.await stopped 5 TimeUnit/SECONDS))
               (is (.await finished 5 TimeUnit/SECONDS)
                   "Already parked callers finish while the owners remain stopped")
               (is (= 1 @(:published engine)))
               (is (= 1 @applications))
               (.countDown release-owners)
               (is (= (vec (range 1 7)) (mapv join @jobs)))
               (is (= 1 @applications) "A revoked claim cannot apply again")
               (is (empty? (:completion-waiters engine)))
               (finally
                 (.countDown release-sync) (.countDown release-owners)
                 (doseq [job @jobs] (join job)))))))))))

(deftest parked-tail-replaces-a-stopped-sync-contender
  (let [syncing (promise) stopped (promise) first? (atom true)
        release-sync (CountDownLatch. 1) release-contender (CountDownLatch. 1)
        waiting (CountDownLatch. 4) finished (CountDownLatch. 4) applied (atom [])]
    (#'application/with-runtime
     {:apply-range! (fn [entries _] (swap! applied into (map #(deref (:lsn %)) entries)))
      :hooks {:before-sync! (fn [_ round]
                              (when (= 1 (:target-lsn round))
                                (deliver syncing true)
                                (.await release-sync)))}}
     (fn [engine wal-state]
       (let [a (first (#'application/append-group! engine [0]))
             jobs (atom [])
             start! (fn [entry]
                      (swap! jobs conj
                             (future
                               (try (caught #(state/await! engine entry))
                                    (finally (.countDown finished))))))]
         (with-redefs [state/phase!
                       (fn [event context]
                         (cond
                           (= event :completion-waiting) (.countDown waiting)
                           (and (= event :before-sync-target-claim)
                                (= 2 @(:lsn context))
                                (= 1 @(:last-durable-lsn (:sync-manager wal-state)))
                                (compare-and-set! first? true false))
                           (do (deliver stopped true) (.await release-contender))))]
           (try
             (start! a)
             (is (deref syncing 5000 false))
             (doseq [entry (concat (#'application/append-group! engine [1])
                                  (#'application/append-group! engine [2 3 4]))]
               (start! entry))
             (is (.await waiting 5 TimeUnit/SECONDS))
             (.countDown release-sync)
             (is (deref stopped 5000 false))
             (is (.await finished 5 TimeUnit/SECONDS))
             (is (= 3 @(:last-durable-lsn (:sync-manager wal-state))))
             (is (= 3 @(:published engine)))
             (.countDown release-contender)
             (is (= (vec (range 5)) (mapv join @jobs)))
             (is (= [1 2 3] @applied))
             (is (empty? (:completion-waiters engine)))
             (finally
               (.countDown release-sync) (.countDown release-contender)
               (doseq [job @jobs] (join job))))))))))

(deftest parked-sync-followers-observe-application-timeout-and-fencing
  (let [syncing (promise) applying (promise)
        release-sync (CountDownLatch. 1) release-native (CountDownLatch. 1)
        waiting (CountDownLatch. 5) finished (CountDownLatch. 5)
        applications (atom 0)]
    (#'application/with-runtime
     {:wal-apply-timeout-ms 100
      :apply-range! (fn [_ _]
                      (swap! applications inc)
                      (deliver applying true)
                      (.await release-native))
      :hooks {:before-sync! (fn [_ _]
                              (deliver syncing true)
                              (.await release-sync))}}
     (fn [engine _]
       (let [entries (#'application/append-group! engine (range 1 7))
             jobs (atom [])
             start! (fn [entry]
                      (swap! jobs conj
                             (future
                               (try (caught #(state/await! engine entry))
                                    (finally (.countDown finished))))))]
         (with-redefs [state/phase!
                       (fn [event _]
                         (when (= event :completion-waiting) (.countDown waiting)))]
           (try
             (start! (first entries))
             (is (deref syncing 5000 false))
             (doseq [entry (rest entries)] (start! entry))
             (is (.await waiting 5 TimeUnit/SECONDS))
             (.countDown release-sync)
             (is (deref applying 5000 false))
             (is (.await finished 1 TimeUnit/SECONDS)
                 "A helper watches the application deadline and wakes all parked callers")
             (is (= :application-timeout (:reason @(:failure engine))))
             (is (= 1 @applications) "A live native owner is never replaced")
             (.countDown release-native)
             (let [outcomes (mapv #(ex-data (join %)) @jobs)]
               (is (every? #(= :txlog/write-committed (:error %)) outcomes))
               (is (every? #(false? (:retryable? %)) outcomes))
               (is (= [1 1 1 1 1 1] (mapv :txlog-lsn outcomes))))
             (is (zero? @(:published engine)) "Late native completion stays fenced")
             (is (empty? (:completion-waiters engine)))
             (finally
               (.countDown release-sync) (.countDown release-native)
               (doseq [job @jobs] (join job))))))))))

(deftest interrupted-sync-follower-fences-and-wakes-parked-neighbors
  (let [syncing (promise) interruptible (promise)
        release-sync (CountDownLatch. 1) waiting (CountDownLatch. 2)
        applications (atom 0)]
    (#'application/with-runtime
     {:apply-range! (fn [_ _] (swap! applications inc))
      :hooks {:before-sync! (fn [_ _]
                              (deliver syncing true)
                              (.await release-sync))}}
     (fn [engine _]
       (let [entries (#'application/append-group! engine [1 2 3])
             jobs (atom [])
             start! (fn [entry]
                      (swap! jobs conj
                             (future
                               (try
                                 (let [result (caught #(state/await! engine entry))]
                                   [result (.isInterrupted (Thread/currentThread))])
                                 (finally (Thread/interrupted))))))]
         (with-redefs [state/phase!
                       (fn [event entry]
                         (when (= event :completion-waiting)
                           (when (= 2 (:result entry))
                             (deliver interruptible (Thread/currentThread)))
                           (.countDown waiting)))]
           (try
             (start! (first entries))
             (is (deref syncing 5000 false))
             (doseq [entry (rest entries)] (start! entry))
             (is (.await waiting 5 TimeUnit/SECONDS))
             (let [^Thread thread (deref interruptible 5000 nil)]
               (is (some? thread))
               (when thread (.interrupt thread)))
             (let [[interrupted-result interrupted?] (join (second @jobs))
                   [neighbor-result] (join (last @jobs))]
               (is interrupted?)
               (doseq [result [interrupted-result neighbor-result]]
                 (is (= :txlog/write-indeterminate (:error (ex-data result))))
                 (is (= 1 (:txlog-lsn (ex-data result))))
                 (is (false? (:retryable? (ex-data result))))))
             (is (not (realized? (first @jobs)))
                 "Fencing wakes followers while the sync owner remains stopped")
             (is (empty? (:completion-waiters engine)))
             (.countDown release-sync)
             (join (first @jobs))
             (is (zero? @applications))
             (is (zero? @(:published engine)))
             (finally
               (.countDown release-sync)
               (doseq [job @jobs] (join job))))))))))

(deftest application-ranges-wake-only-for-their-completion-or-turn
  (let [entered-a (promise) entered-b (promise)
        release-a (CountDownLatch. 1) release-b (CountDownLatch. 1)
        waiting (CountDownLatch. 5)
        waits (atom []) signals (atom []) completions (atom []) applied (atom [])]
    (#'application/with-runtime
     {:application-max-records 1
      :apply-range! (fn [entries _]
                      (let [lsn @(:lsn (first entries))]
                        (swap! applied conj lsn)
                        (case (long lsn)
                          1 (do (deliver entered-a true) (.await release-a))
                          2 (do (deliver entered-b true) (.await release-b))
                          nil)))}
     (fn [engine wal-state]
       (let [a (#'application/append-group! engine [1 2])
             b (#'application/append-group! engine [3 4])
             c (#'application/append-group! engine [5 6])
             spare (state/reserve! engine 200 5000)
             jobs (atom [])]
         (wal/force-sync! wal-state {})
         (with-redefs [state/phase!
                       (fn [event range]
                         (case event
                           :application-waiting
                           (do (swap! waits conj (:lo range)) (.countDown waiting))
                           :application-notified (swap! signals conj (:lo range))
                           :application-completed (swap! completions conj (:lo range))
                           nil))]
           (try
             (swap! jobs conj (future (caught #(state/await! engine (first a)))))
             (is (deref entered-a 5000 false))
             (doseq [entry (concat (rest a) b c)]
               (swap! jobs conj (future (caught #(state/await! engine entry)))))
             (is (.await waiting 5 TimeUnit/SECONDS))
             (is (= {1 1, 2 2, 3 2} (frequencies @waits)))
             (let [tail (#'application/append! engine 7)]
               ;; Both are real transitions, but neither enables an applicant
               ;; while A owns native application or completes its range.
               (state/release! spare)
               (wal/force-sync! wal-state {})
               (is (empty? @signals))
               (is (empty? @completions))
               (is (not-any? realized? @jobs))
               (.countDown release-a)
               (is (deref entered-b 5000 false))
               (is (= [1 2] (mapv join (take 2 @jobs))))
               (is (= [1] @completions))
               (is (= [2] @signals) "One waiter, from the oldest waiting range, takes B")
               (is (not-any? realized? (drop 2 @jobs)))
               (.countDown release-b)
               (is (= [1 2 3 4 5 6] (mapv join @jobs)))
               (is (= [1 2 3] @completions) "Each range completes once for all its callers")
               (is (= 7 (state/await! engine tail)))
               (is (= [1 2 3 4] @applied))
               (is (empty? (:application-waiters engine)))
               (is (= {:bytes 0 :requests 0} (reclaimed-usage engine))))
             (finally
               (.countDown release-a) (.countDown release-b)
               (state/release! spare)
               (doseq [job @jobs] (join job))))))))))

(deftest interrupted-claim-fences-before-native-entry
  (let [claimed (promise) release (CountDownLatch. 1)
        applications (atom 0)]
    (#'application/with-runtime
     {:apply-range! (fn [_ _] (swap! applications inc))}
     (fn [engine _]
       (let [entries (#'application/append-group! engine [1 2])]
         (with-redefs [state/phase!
                       (fn [event _]
                         (when (= event :application-claimed)
                           (deliver claimed (Thread/currentThread))
                           (.await release)))]
           (let [owner (future
                         (let [result (caught #(state/await! engine (first entries)))]
                           {:data (ex-data result)
                            :interrupted? (.isInterrupted (Thread/currentThread))}))]
             (try
               (let [^Thread thread (deref claimed 5000 nil)]
                 (is (some? thread))
                 (when thread (.interrupt thread)))
               (let [{:keys [data interrupted?]} (join owner)]
                 ;; Record-identity verification can itself be interrupted;
                 ;; either failure outcome must forbid automatic resubmission.
                 (is (#{:txlog/write-committed :txlog/write-indeterminate} (:error data)))
                 (is (= 1 (:txlog-lsn data)))
                 (is (false? (:retryable? data)))
                 (is interrupted?))
               (is (= :txlog/write-committed
                      (:error (ex-data (caught #(state/await! engine (second entries)))))))
               (is (zero? @applications))
               (is (nil? @(:owner engine)))
               (is (some? @(:failure engine)))
               (finally (.countDown release) (join owner))))))))))
