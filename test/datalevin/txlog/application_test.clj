(ns datalevin.txlog.application-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-state :as state]
            [datalevin.tx-state.lifetime :as lifetime]
            [datalevin.txlog :as wal]
            [datalevin.txlog.append :as append]
            [datalevin.txlog.segment :as segment]
            [datalevin.util :as u])
  (:import [java.io Closeable]
           [java.util.concurrent ConcurrentSkipListMap CountDownLatch]))

(defn- caught [f] (try (f) (catch Throwable e e)))
(defn- join [job] (deref job 5000 ::timeout))
(defn- rows [n] [[:put "data" n (str n) :long :string]])

(defn- with-runtime [opts f]
  (let [dir (u/tmp-dir (str "wal-application-" (random-uuid)))
        wal-state (:state (wal/init-runtime-state
                           (merge {:dir dir :wal? true :wal-shared? false
                                   :wal-durability-profile :strict
                                   :wal-sync-mode :fsync :wal-segment-prealloc? false
                                   :wal-commit-wait-ms 10000} opts) nil))
        engine (state/create wal-state opts)]
    (try (f engine wal-state)
         (finally
           (state/close! engine 1000 (fn []))
           (doseq [key [:segment-channel :sync-lock-channel]]
             (when-let [^Closeable channel (some-> (get wal-state key) deref)]
               (.close channel)))
           (u/delete-files dir)))))

(defn- append! [engine n]
  (let [reservation (state/reserve! engine 200 5000)]
    (state/append! engine reservation (rows n) n)))

(defn- append-group! [engine values]
  (let [record (state/prepare-record)
        entries (mapv #(state/prepare-entry (state/reserve! engine 200 5000)
                                           (rows %) % record) values)
        token (state/acquire-preparation! engine (:reservation (first entries)) 5000)]
    (try
      (let [root @(:root engine)]
        (state/append-batch! engine entries
                             {:expected-root root
                              :root (update root :lsn inc)}))
      (finally (state/release-preparation! token)))))

(defn- lsns [entries] (mapv #(deref (:lsn %)) entries))
(defn- records [wal-state]
  (mapv #(-> % :body wal/decode-commit-row-payload)
        (:records (segment/scan-segment (wal/segment-path (:dir wal-state) 1)))))

(deftest available-budget-admission-does-not-acquire-the-application-gate
  (with-runtime
    {}
    (fn [engine _]
      (let [^java.util.concurrent.locks.ReentrantLock gate (:gate engine)
            reservation (atom nil)]
        (.lock gate)
        (try
          (reset! reservation (join (future (state/reserve! engine 200 5000))))
          (is (not= ::timeout @reservation))
          (is (= {:bytes 200 :requests 1} (state/usage engine)))
          (finally (.unlock gate)))
        (when (map? @reservation) (state/release! @reservation))
        (is (= {:bytes 0 :requests 0} (state/usage engine)))))))

(deftest concurrent-admission-keeps-both-budget-dimensions-atomic
  (let [budget (datalevin.utl.PendingBudget. 18 5)
        go (CountDownLatch. 1)
        jobs (mapv (fn [_]
                     (future
                       (.await go)
                       (dotimes [_ 3000]
                         (loop [tries 0]
                           (if (.tryReserve budget 3)
                             (try
                               (let [usage (.snapshot budget)]
                                 (assert (<= 0 (.-bytes usage) 18))
                                 (assert (<= 0 (.-requests usage) 5))
                                 (assert (= (.-bytes usage) (* 3 (.-requests usage)))))
                               (finally (.release budget 3)))
                             (do
                               (assert (< tries 1000000))
                               (Thread/yield)
                               (recur (inc tries))))))
                       :done)) (range 8))]
    (.countDown go)
    (is (= (repeat 8 :done) (mapv join jobs)))
    (is (zero? (.-bytes (.snapshot budget))))
    (is (zero? (.-requests (.snapshot budget))))))

(deftest admission-accounting-retains-full-long-limits
  (let [budget (datalevin.utl.PendingBudget. Long/MAX_VALUE Long/MAX_VALUE)]
    (is (.tryReserve budget (dec Long/MAX_VALUE)))
    (is (.tryReserve budget 1))
    (is (false? (.tryReserve budget 1)))
    (is (= Long/MAX_VALUE (.-bytes (.snapshot budget))))
    (is (= 2 (.-requests (.snapshot budget))))
    (.release budget (dec Long/MAX_VALUE))
    (.release budget 1)
    (is (zero? (.-bytes (.snapshot budget))))))

(deftest one-flush-applies-a-fixed-prefix-once
  (let [applied (atom [])]
    (with-runtime
      {:apply-range! (fn [entries _] (swap! applied conj (lsns entries)))}
      (fn [engine wal-state]
        (let [a (append! engine 1) b (append! engine 2)]
          (is (= 2 (state/await! engine b)))
          (is (= 1 (state/await! engine a)))
          (is (= [[1 2]] @applied))
          (is (= [1 2] (mapv :lsn (records wal-state))))
          (is (= {:bytes 0 :requests 0} (state/usage engine))))))))

(deftest sync-captures-later-ready-appends-without-waiting-for-preparation
  (let [capturing (CountDownLatch. 1)
        resume (CountDownLatch. 1)
        joined (CountDownLatch. 1)
        owners (atom 0)
        rounds (atom [])
        applied (atom [])]
    (with-runtime
      {:apply-range! (fn [entries _] (swap! applied conj (lsns entries)))
       :hooks {:before-sync! (fn [_ round]
                               (swap! rounds conj (:target-lsn round)))}}
      (fn [engine wal-state]
        (let [a (append! engine 1)]
          (with-redefs [state/phase!
                        (fn [event _]
                          (case event
                            :before-sync-prefix-capture
                            (do (.countDown capturing) (.await resume))
                            :sync-target-owner (swap! owners inc)
                            :sync-target-joined (.countDown joined)
                            nil))]
            (let [job-a (future (caught #(state/await! engine a)))]
              (try
                (is (.await capturing 5 java.util.concurrent.TimeUnit/SECONDS))
                (let [b (append! engine 2)
                      job-b (future (caught #(state/await! engine b)))]
                  (is (= 2 @(:lsn b)))
                  (is (.await joined 5 java.util.concurrent.TimeUnit/SECONDS))
                  (is (= 1 @owners)
                      "The late append joins the round before its target is frozen")
                  (.countDown resume)
                  (is (= [1 2] (mapv join [job-a job-b])))
                  (is (= [2] @rounds))
                  (is (= 1 @owners))
                  (is (= [[1 2]] @applied))
                  (is (= 2 @(:last-durable-lsn (:sync-manager wal-state)))))
                (finally (.countDown resume) (join job-a))))))))))

(deftest later-appends-share-the-next-round-without-early-acknowledgement
  (let [first-force (promise) second-force (promise)
        release-first (CountDownLatch. 1) release-second (CountDownLatch. 1)
        joined (CountDownLatch. 7)
        owners (atom [])]
    (with-runtime
      {:apply-range! (fn [_ _])
       :hooks {:before-sync!
               (fn [_ round]
                 (if (= 1 (:target-lsn round))
                   (do (deliver first-force true) (.await release-first))
                   (do (deliver second-force (:target-lsn round))
                       (.await release-second))))}}
      (fn [engine wal-state]
        (let [a (append! engine 1)
              jobs (atom [])]
          (with-redefs [state/phase!
                        (fn [event context]
                          (case event
                            :sync-target-owner (swap! owners conj (:target context))
                            :sync-target-joined (.countDown joined)
                            nil))]
            (try
              (swap! jobs conj (future (caught #(state/await! engine a))))
              (is (deref first-force 5000 false))
              (doseq [n (range 2 9)]
                (let [entry (append! engine n)]
                  (swap! jobs conj (future (caught #(state/await! engine entry))))))
              (is (.await joined 5 java.util.concurrent.TimeUnit/SECONDS))
              (is (= [1] @owners)
                  "No later caller preclaims a target or enters WAL completion")
              (is (= 8 @(:last-appended-lsn (:sync-manager wal-state))))
              (is (not-any? realized? @jobs))
              (.countDown release-first)
              (is (= 8 (deref second-force 5000 ::timeout)))
              (is (= 1 (join (first @jobs))))
              (is (= 1 @(:last-durable-lsn (:sync-manager wal-state))))
              (is (not-any? realized? (rest @jobs))
                  "Settling the first round cannot acknowledge its uncovered tail")
              (is (= [1 8] @owners))
              (.countDown release-second)
              (is (= (vec (range 1 9)) (mapv join @jobs)))
              (is (= 8 @(:last-durable-lsn (:sync-manager wal-state))))
              (is (nil? @(:sync-completion engine)))
              (finally
                (.countDown release-first)
                (.countDown release-second)
                (doseq [job @jobs] (join job))))))))))

(deftest native-application-combines-adjacent-durable-sync-ranges
  (let [applied (atom [])]
    (with-runtime
      {:apply-range! (fn [entries _] (swap! applied conj (lsns entries)))}
      (fn [engine wal-state]
        (let [a (append! engine 1)]
          (is (:synced? (wal/force-sync! wal-state {})))
          (let [b (append! engine 2)]
            (is (:synced? (wal/force-sync! wal-state {})))
            (is (= 2 (reduce + (vals (:sync-count-by-reason
                                     (wal/sync-manager-state
                                      (:sync-manager wal-state))))))
                "The sync manager retains its independent rounds")
            (is (= 2 (state/await! engine b)))
            (is (= 1 (state/await! engine a)))
            (is (= [[1 2]] @applied))))))))

(deftest native-application-prefix-stops-at-its-byte-budget
  (let [applied (atom [])]
    (with-runtime
      {:application-max-bytes 400
       :apply-range! (fn [entries _] (swap! applied conj (lsns entries)))}
      (fn [engine wal-state]
        (let [entries (mapv (fn [n]
                              (let [entry (append! engine n)]
                                (wal/force-sync! wal-state {})
                                entry))
                            (range 1 4))]
          (is (= 3 (state/await! engine (last entries))))
          (is (= [1 2 3] (mapv #(state/await! engine %) entries)))
          (is (= [[1 2] [3]] @applied)))))))

(deftest delta-pruning-leaves-the-application-gate-free
  (let [entered (promise) release (CountDownLatch. 1)]
    (with-runtime
      {:apply-range! (fn [_ _])}
      (fn [engine _]
        (let [entry (append! engine 1)
              ^java.util.concurrent.locks.ReentrantLock gate (:gate engine)
              ^java.util.concurrent.locks.ReentrantLock prep (:preparation engine)]
          (with-redefs [state/phase!
                        (fn [event _]
                          (when (= :before-delta-prune event)
                            (deliver entered true)
                            (.await release)))]
            (let [job (future (caught #(state/await! engine entry)))]
              (try
                (is (deref entered 5000 false))
                (is (.tryLock gate 1 java.util.concurrent.TimeUnit/SECONDS))
                (when (.isHeldByCurrentThread gate) (.unlock gate))
                ;; Pruning must not hold ordered preparation either.
                (is (.tryLock prep 1 java.util.concurrent.TimeUnit/SECONDS))
                (when (.isHeldByCurrentThread prep) (.unlock prep))
                (.countDown release)
                (is (= 1 (join job)))
                (finally (.countDown release) (join job))))))))))

(deftest pruning-catches-publication-that-advances-during-rebuild
  (let [entered (promise) release (CountDownLatch. 1)
        first? (atom true)]
    (with-runtime
      {:application-max-records 1 :apply-range! (fn [_ _])}
      (fn [engine _]
        (let [a (append! engine 1) b (append! engine 2)]
          (with-redefs [state/phase!
                        (fn [event _]
                          (when (and (= :before-delta-prune event)
                                     (compare-and-set! first? true false))
                            (deliver entered true)
                            (.await release)))]
            (let [job-a (future (caught #(state/await! engine a)))]
              (try
                (is (deref entered 5000 false))
                (is (= 2 (join (future (caught #(state/await! engine b))))))
                (.countDown release)
                (is (= 1 (join job-a)))
                (is (= 2 @(:pruned engine)))
                (is (= {:bytes 0 :requests 0} (state/usage engine)))
                (finally (.countDown release) (join job-a))))))))))

(deftest gated-append-publication-survives-a-stale-prune-install
  (let [entered (promise) release (CountDownLatch. 1)
        first? (atom true)]
    (with-runtime
      {:application-max-records 1 :apply-range! (fn [_ _])}
      (fn [engine wal-state]
        (let [a (first (append-group! engine [1]))
              next0 (long @(:next-lsn wal-state))]
          ;; Pause prune inside its gate between the base check and the install.
          (with-redefs [state/phase!
                        (fn [event _]
                          (when (and (= :before-prune-install event)
                                     (compare-and-set! first? true false))
                            (deliver entered true)
                            (.await release)))]
            (let [job-a (future (caught #(state/await! engine a)))]
              (try
                (is (deref entered 5000 false))
                (let [job-b (future (caught #(first (append-group! engine [2]))))
                      ^ConcurrentSkipListMap entries (:entries engine)
                      deadline (+ (System/nanoTime) 5000000000)]
                  ;; Wait for b's WAL record, then confirm its publication is
                  ;; held by the gate prune owns.
                  (is (loop []
                        (cond (= (inc next0) (long @(:next-lsn wal-state))) true
                              (>= (System/nanoTime) deadline) false
                              :else (do (Thread/sleep 1) (recur)))))
                  (Thread/sleep 50)
                  (is (= 1 (long (:lsn @(:root engine))))
                      "No root may publish while prune installs under the gate")
                  (is (nil? (.get entries 2))
                      "No retained entry may publish while prune installs under the gate")
                  (.countDown release)
                  (is (= 2 (long @(:lsn (join job-b)))))
                  (is (= 2 (long (:lsn @(:root engine))))
                      "The concurrent append's newer root must win")
                  (is (= 2 (long @(:lsn (.get entries 2)))))
                  (is (= 1 (join job-a))))
                (finally (.countDown release) (join job-a))))))))))

(deftest receipts-sharing-a-sync-target-complete-wal-once
  (let [entered (promise) release (CountDownLatch. 1)
        joined (CountDownLatch. 7)
        completions (atom 0)]
    (with-runtime
      {:apply-range! (fn [_ _])
       :hooks {:before-sync! (fn [_ _]
                               (deliver entered true)
                               (.await release))}}
      (fn [engine wal-state]
        (let [entries (append-group! engine (range 1 9))
              jobs (atom [])]
          (is (every? #(identical? @(:append-batch (first entries))
                                   @(:append-batch %)) entries))
          (is (= (vec (repeat 8 1)) (lsns entries)))
          (is (= {:bytes 1600 :requests 8} (state/usage engine)))
          (is (= [(vec (mapcat rows (range 1 9)))] (mapv :ops (records wal-state))))
          (try
            (with-redefs [state/phase!
                          (fn [event _]
                            (case event
                              :sync-target-joined (.countDown joined)
                              :sync-target-owner (swap! completions inc)
                              nil))]
              (swap! jobs conj (future (caught #(state/await! engine (first entries)))))
              (is (deref entered 5000 false))
              (doseq [entry (rest entries)]
                (swap! jobs conj (future (caught #(state/await! engine entry)))))
              (is (.await joined 5 java.util.concurrent.TimeUnit/SECONDS))
              (is (= 1 @completions))
              (.countDown release)
              (is (= (vec (range 1 9)) (mapv join @jobs)))
              (is (= 1 @completions))
              (is (= 1 @(:last-durable-lsn (:sync-manager wal-state))))
              (is (= {:bytes 0 :requests 0} (state/usage engine)))
              (is (nil? @(:sync-completion engine))))
            (finally (.countDown release)
                     (doseq [job @jobs] (join job)))))))))

(deftest sync-claim-rechecks-durability-after-target-is-pruned
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)]
    (with-runtime
      {:apply-range! (fn [_ _])}
      (fn [engine _]
        (let [a (append! engine 1)
              b (append! engine 2)]
          (with-redefs [state/phase!
                        (fn [event context]
                          (when (and (= event :before-sync-target-claim)
                                     (identical? context a))
                            (.countDown entered)
                            (.await release)))]
            (let [first-job (future (caught #(state/await! engine a)))]
              (try
                (is (.await entered 5 java.util.concurrent.TimeUnit/SECONDS))
                (is (= 2 (state/await! engine b)))
                ;; Publication returns before background reclamation. Establish
                ;; the pruned-target race explicitly before resuming the claim.
                (lifetime/with-lock (:prune-lock engine) (state/prune! engine))
                (is (.isEmpty ^ConcurrentSkipListMap (:entries engine)))
                (.countDown release)
                (is (= 1 (join first-job)))
                (finally (.countDown release) (join first-job))))))))))

(deftest failed-shared-sync-wakes-each-receipt-with-the-group-lsn
  (let [entered (promise) release (CountDownLatch. 1)
        joined (CountDownLatch. 3)]
    (with-runtime
      {:apply-range! (fn [_ _] (throw (AssertionError. "Unflushed WAL was applied")))
       :hooks {:before-sync! (fn [_ _]
                               (deliver entered true)
                               (.await release)
                               (throw (ex-info "sync failed" {:test/failure true})))}}
      (fn [engine _]
        (let [entries (append-group! engine (range 1 5))
              jobs (atom [])]
          (try
            (with-redefs [state/phase!
                          (fn [event _]
                            (when (= :sync-target-joined event)
                              (.countDown joined)))]
              (swap! jobs conj (future (caught #(state/await! engine (first entries)))))
              (is (deref entered 5000 false))
              (doseq [entry (rest entries)]
                (swap! jobs conj (future (caught #(state/await! engine entry)))))
              (is (.await joined 5 java.util.concurrent.TimeUnit/SECONDS))
              (.countDown release)
              (let [outcomes (mapv #(ex-data (join %)) @jobs)]
                (is (= [1 1 1 1] (mapv :txlog-lsn outcomes)))
                (is (= [1 1 1 1] (mapv #(get-in % [:txlog-record :lsn]) outcomes)))
                (is (= 1 (count (distinct (map :txlog-record outcomes)))))
                (is (every? #(= :txlog/write-indeterminate (:error %)) outcomes))
                (is (every? #(= :wal-wait-failed (:reason %)) outcomes))))
            (finally (.countDown release)
                     (doseq [job @jobs] (join job)))))))))

(deftest eligibility-scans-only-newly-durable-ranges
  (with-runtime
    {:application-max-records 2}
    (fn [engine wal-state]
      (dotimes [n 4] (append! engine (inc n)))
      (state/seal-through! engine 4)
      (let [original ^ConcurrentSkipListMap (:entries engine)
            lookups (atom [])
            observed (proxy [ConcurrentSkipListMap] []
                       (get [key]
                         (swap! lookups conj key)
                         (.get original key))
                       (values []
                         (throw (AssertionError. "eligibility scanned retained values"))))
            observed-engine (assoc engine :entries observed)
            durable (:last-durable-lsn (:sync-manager wal-state))]
        (vreset! durable 2)
        (#'state/mark-eligible! observed-engine)
        (is (= [1 3] @lookups))
        (is (= 3 @(:next-unmarked engine)))
        (is (every? #(some? @(-> (.get original %) :range deref :deadline)) [1 2]))
        (is (nil? @(-> (.get original 3) :range deref :deadline)))
        (reset! lookups [])
        (#'state/mark-eligible! observed-engine)
        (is (= [3] @lookups) "An unchanged durable floor checks only the cursor")
        (vreset! durable 4)
        (reset! lookups [])
        (#'state/mark-eligible! observed-engine)
        (is (= [3 5] @lookups))
        (is (= 5 @(:next-unmarked engine)))
        ;; claim! may set deadlines and publish before after-sync! advances its
        ;; cursor; a later prune can remove those earlier entries entirely.
        (vreset! (:next-unmarked engine) 3)
        (vreset! (:published engine) 4)
        (dotimes [n 4] (.remove original (inc n)))
        (reset! lookups [])
        (#'state/mark-eligible! observed-engine)
        (is (= [5] @lookups))
        (is (= 5 @(:next-unmarked engine)))))))

(deftest committed-outcomes-verify-the-original-record-identity
  (with-runtime
    {:apply-range! (fn [_ _] (throw (ex-info "abort" {:applied? false})))}
    (fn [engine _]
      (let [entry (append! engine 1)
            data (ex-data (caught #(state/await! engine entry)))]
        (is (= :committed (:outcome data)))
        (is (= (wal/append-identity @(:append-batch entry) @(:lsn entry))
               (:txlog-record data)))
        (vswap! (:append-batch entry)
                (fn [batch]
                  (append/create
                   (append/first-lsn batch) (append/segment-id batch)
                   (append/channel batch) (append/sync-manager batch)
                   (append/started-ms batch) (append/deadline-ns batch)
                   (append/timeout-ms batch) (append/near-roll? batch)
                   [(update (append/record-info batch @(:lsn entry)) :checksum inc)])))
        (is (= :indeterminate
               (:outcome (ex-data (state/outcome engine entry :mismatch nil)))))
        ;; A tentative LSN with no successful append receipt is never proof,
        ;; even if durable progress from another record covers that number.
        (vreset! (:append-batch entry) nil)
        (is (= :indeterminate
               (:outcome (ex-data (state/outcome engine entry :missing nil)))))))))

(deftest stopped-sync-does-not-stop-append-or-change-range-membership
  (let [entered (promise) release (CountDownLatch. 1)
        applied (atom [])]
    (with-runtime
      {:apply-range! (fn [entries _] (swap! applied conj (lsns entries)))
       :hooks {:before-sync! (fn [_ round]
                               (when (= 1 (:target-lsn round))
                                 (deliver entered true)
                                 (.await release)))}}
      (fn [engine wal-state]
        (let [a (append! engine 1)
              job-a (future (caught #(state/await! engine a)))
              jobs (atom [job-a])]
          (try
            (is (deref entered 5000 false))
            (let [first-range @(:range a)
                  b (join (future (append! engine 2)))]
              (is (= 2 @(:lsn b)))
              (is (= [1] (lsns (:entries first-range))))
              (is (identical? first-range @(:range a)))
              (swap! jobs conj (future (caught #(state/await! engine b))))
              (is (empty? @applied))
              (is (zero? @(:last-durable-lsn (:sync-manager wal-state))))
              (.countDown release)
              (is (= [1 2] (mapv join @jobs)))
              ;; Native application may combine adjacent durable ranges; their
              ;; sealed membership remains fixed across the stopped force.
              (is (= [1 2] (vec (mapcat identity @applied))))
              (is (identical? first-range @(:range a)))
              (is (= [[1] [2]]
                     (mapv #(lsns (:entries @(:range %))) [a b]))))
            (finally (.countDown release) (doseq [job @jobs] (join job)))))))))

(deftest application-failure-completes-two-durable-batches-without-reappend
  (let [entered (promise) release (CountDownLatch. 1)
        second-durable (promise) calls (atom [])]
    (with-runtime
      {:application-max-records 1
       :apply-range! (fn [entries _]
                       (swap! calls conj (lsns entries))
                       (deliver entered true)
                       (.await release)
                       (throw (ex-info "native transaction aborted" {:applied? false})))}
      (fn [engine wal-state]
        (let [a (append-group! engine [1 2]) b (append-group! engine [3 4])
              job-a (future (caught #(state/await! engine (first a))))
              jobs (atom [job-a])]
          (try
            (is (deref entered 5000 false))
            (is (= 2 @(:last-durable-lsn (:sync-manager wal-state))))
            (doseq [entry (concat (rest a) b)]
              (swap! jobs conj (future (deliver second-durable true)
                                       (caught #(state/await! engine entry)))))
            (is (deref second-durable 5000 false))
            (.countDown release)
            (let [errors (mapv #(ex-data (join %)) @jobs)]
              (is (= [1 1 2 2] (mapv :txlog-lsn errors)))
              (is (every? #(= :txlog/write-committed (:error %)) errors))
              (is (every? #(false? (:applied? %)) errors))
              (is (every? #(false? (:retryable? %)) errors)))
            (is (= [[1]] @calls) "No successor enters native application")
            (is (= :not-committed
                   (:outcome (ex-data (caught #(append! engine 3))))))
            ;; The recovery input is the same two original records, regardless
            ;; of which request's caller observed the terminal failure first.
            (is (= [(vec (mapcat rows [1 2])) (vec (mapcat rows [3 4]))]
                   (mapv :ops (records wal-state))))
            (is (= [1 2] (mapv :lsn (records wal-state))))
            (finally (.countDown release) (doseq [job @jobs] (join job)))))))))

(deftest native-timeout-fences-successors-and-prevents-teardown
  (let [entered (promise) release (CountDownLatch. 1)
        closed (atom false)]
    (with-runtime
      {:application-max-records 1 :wal-apply-timeout-ms 100
       :apply-range! (fn [_ _]
                       (deliver entered true)
                       (loop []
                         (when-not (try (.await release) true
                                        (catch InterruptedException _ false))
                           (recur))))}
      (fn [engine _]
        (let [a (append! engine 1) b (append! engine 2)
              job-a (future (caught #(state/await! engine a)))
              jobs (atom [job-a])]
          (try
            (is (deref entered 5000 false))
            (let [job-b (future (caught #(state/await! engine b)))]
              (swap! jobs conj job-b)
              (is (= :txlog/write-committed
                     (:error (ex-data (deref job-b 1000 nil))))))
            (is (= :txlog/native-not-quiescent
                   (:error (ex-data
                            (caught #(state/close! engine 10
                                                   (fn [] (reset! closed true))))))))
            (is (false? @closed))
            (is (= {:phase :fenced :native-users 1}
                   (lifetime/state (:lifetime engine))))
            (.countDown release)
            (is (= :txlog/write-committed (:error (ex-data (join job-a)))))
            (is (true? (:applied? (ex-data (join job-a)))))
            (is (zero? @(:published engine)) "Late native completion cannot publish")
            (finally (.countDown release) (doseq [job @jobs] (join job)))))))))

(deftest successful-application-publishes-after-its-deadline-if-unfenced
  (let [now (atom (System/nanoTime))]
    (with-redefs [lifetime/nano-time (fn ^long [] (long @now))]
      (with-runtime
        {:wal-apply-timeout-ms 100
         :apply-range! (fn [_ _] (swap! now + 200000000))}
        (fn [engine _]
          (let [entry (append! engine 1)]
            (is (= 1 (state/await! engine entry)))
            (is (true? @(:applied? entry)))
            (is (true? @(:complete? entry)))
            (is (= 1 @(:published engine)))
            (is (nil? @(:failure engine)))
            (is (= 2 (state/await! engine (append! engine 2))))))))))

(deftest aggregate-capacity-backpressures-stalled-application
  (let [entered (promise) release (CountDownLatch. 1)
        applied (atom []) waiters (CountDownLatch. 2)]
    (with-runtime
      {:wal-pending-max-bytes 400 :wal-pending-max-requests 2
       :apply-range! (fn [entries _]
                       (deliver entered true)
                       (.await release)
                       (swap! applied into (lsns entries)))}
      (fn [engine wal-state]
        (let [a (append! engine 1)
              first-job (future (caught #(state/await! engine a)))
              jobs (atom [first-job])]
          (try
            (is (deref entered 5000 false))
            (let [b (append! engine 2)]
              (swap! jobs conj (future (caught #(state/await! engine b)))))
            (with-redefs [state/phase! (fn [event _]
                                         (when (= event :capacity-wait)
                                           (.countDown waiters)))]
              (doseq [n [3 4]]
                (swap! jobs conj (future (caught #(state/await! engine (append! engine n))))))
              (is (.await waiters 1 java.util.concurrent.TimeUnit/SECONDS)))
            (is (= 2 (state/capacity-waiters engine)))
            (doseq [_ (range 14)]
              (let [error (ex-data (caught #(state/reserve! engine 200 5000)))]
                (is (= :server/busy (:error error)))
                (is (:retryable? error))))
            (is (= {:bytes 400 :requests 2} (state/usage engine)))
            (is (= :txlog/pending-capacity
                   (:error (ex-data (caught #(state/reserve! engine 1 0))))))
            (.countDown release)
            (is (= [1 2 3 4] (mapv join @jobs)))
            (is (= [1 2 3 4] @applied))
            (is (= [1 2 3 4] (mapv :lsn (records wal-state))))
            (is (= {:bytes 0 :requests 0} (state/usage engine)))
            (finally (.countDown release) (doseq [job @jobs] (join job)))))))))

(deftest relaxed-application-can-precede-durability
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 10 :wal-group-commit-ms 0
     :apply-range! (fn [_ _])}
    (fn [engine wal-state]
      (is (= 1 (state/await! engine (append! engine 1))))
      (is (= 1 @(:published engine)))
      (is (zero? @(:last-durable-lsn (:sync-manager wal-state)))))))

(deftest caller-can-take-a-descheduled-resource-free-application-turn
  (let [claimed (promise) release (CountDownLatch. 1) first? (atom true)
        applied (atom [])]
    (with-runtime
      {:application-max-records 1
       :apply-range! (fn [entries _] (swap! applied conj (lsns entries)))}
      (fn [engine _]
        (let [a (append! engine 1) b (append! engine 2)]
          (with-redefs [state/phase!
                        (fn [event _]
                          (when (and (= event :application-claimed)
                                     (compare-and-set! first? true false))
                            (deliver claimed true)
                            (.await release)))]
            (let [job-a (future (caught #(state/await! engine a)))]
              (try
                (is (deref claimed 5000 false))
                (is (= 2 (deref (future (caught #(state/await! engine b))) 1000 ::timeout)))
                (is (= [[1] [2]] @applied))
                (.countDown release)
                (is (= 1 (join job-a)))
                (is (= [[1] [2]] @applied) "Revoked applicant cannot enter native work")
                (finally (.countDown release) (join job-a))))))))))

(deftest resource-free-takeovers-reuse-the-coalesced-prefix
  (let [claimed-a (promise) claimed-b (promise) release (CountDownLatch. 1)
        claims (atom 0) computes (atom 0) applied (atom [])]
    (with-runtime
      {:application-max-records 1
       :apply-range! (fn [entries _] (swap! applied conj (lsns entries)))}
      (fn [engine _]
        (let [a (append! engine 1)
              b (append! engine 2)]
          (with-redefs [state/phase!
                        (fn [event _]
                          (cond
                            (= event :application-prefix-computed)
                            (swap! computes inc)

                            (= event :application-claimed)
                            (case (int (swap! claims inc))
                              1 (do (deliver claimed-a true) (.await release))
                              2 (do (deliver claimed-b true) (.await release))
                              nil)))]
            (let [job-a (future (caught #(state/await! engine a)))
                  job-b (future (caught #(state/await! engine b)))]
              (try
                (is (deref claimed-a 5000 false))
                (is (deref claimed-b 5000 false))
                ;; Both resource-free claims are for the same publication
                ;; epoch; the second takeover reuses the first prefix.
                (is (= 1 @computes))
                (finally
                  (.countDown release)
                  (join job-a)
                  (join job-b)))
              (is (= [[1] [2]] @applied)))))))))

(deftest external-force-retains-resources-until-io-quiesces
  (let [entered (promise) release (CountDownLatch. 1) closed? (atom false)]
    (with-runtime
      {:apply-range! (fn [_ _])}
      (fn [engine wal-state]
        (append! engine 1)
        (let [job (future (caught #(wal/force-sync!
                                    wal-state
                                    {:before-sync! (fn [_ _]
                                                     (deliver entered true)
                                                     (.await release))})))]
          (try
            (is (deref entered 5000 false))
            (is (= :txlog/native-not-quiescent
                   (:error (ex-data (caught #(state/close! engine 10
                                                           (fn [] (reset! closed? true))))))))
            (is (false? @closed?))
            (is (.isOpen ^java.nio.channels.FileChannel @(:segment-channel wal-state)))
            (.countDown release)
            (is (:synced? (join job)))
            (state/close! engine 1000 #(reset! closed? true))
            (is @closed?)
            (finally (.countDown release) (join job))))))))

(deftest external-sync-failure-closes-the-same-admission-authority
  (with-runtime
    {:apply-range! (fn [_ _])}
    (fn [engine wal-state]
      (let [entry (append! engine 1)
            fault (ex-info "Injected external sync failure" {})]
        (is (identical? fault
                        (caught #(wal/force-sync! wal-state
                                                  {:before-sync! (fn [_ _] (throw fault))}))))
        (is (= :wal-failed (:reason @(:failure engine))))
        (is (= :txlog/admission-closed
               (:error (ex-data (caught #(state/reserve! engine 100 10))))))
        (is (= :txlog/write-indeterminate
               (:error (ex-data (caught #(state/await! engine entry))))))
        (is (zero? @(:published engine)))))))
