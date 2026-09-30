(ns datalevin.tx-group-window-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-group :as group]
            [datalevin.tx-group.compat :as compat])
  (:import [datalevin.tx_group Group]
           [java.util.concurrent ConcurrentLinkedQueue CountDownLatch Semaphore]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent.locks ReentrantLock]))

(def ^:dynamic *compat-probe* nil)

(deftest compat-direct-write-does-not-capture-bindings
  (let [g (compat/create 8)]
    (binding [*compat-probe* :caller]
      (compat/with-state false 17
        (fn []
          (with-redefs [clojure.core/get-thread-bindings
                        (fn [] (throw (ex-info "direct write captured bindings" {})))]
            (is (= [:caller 17 false]
                   (compat/submit! g #(% nil)
                                   (fn [_] [*compat-probe*
                                            (compat/request-count)
                                            (compat/batched?)]))))))))))

(deftest compat-queued-leader-does-not-capture-bindings
  ;; Holding the group lock routes the write through queued admission, but no
  ;; active owner exists, so this caller still wins leadership and executes its
  ;; own request. Cross-thread bindings must stay uncaptured.
  (let [g (compat/create 8)
        ^ReentrantLock lock (.-lock ^Group g)]
    (.lock lock)
    (try
      (binding [*compat-probe* :caller]
        (with-redefs [clojure.core/get-thread-bindings
                      (fn [] (throw (ex-info "queued leader captured bindings" {})))]
          (is (= [:caller :leader]
                 (compat/submit! g #(% nil)
                                 (fn [_] [*compat-probe* :leader]))))))
      (finally (.unlock lock)))))

(deftest compat-queued-write-retains-submitter-bindings-on-foreign-leader
  ;; A follower's request can run on a foreign leader. Arbitrary caller bindings
  ;; are captured at submission and restored on that foreign thread, while
  ;; adapter state travels thread-locally.
  (let [g (compat/create 8)
        entered (promise)
        release (promise)
        leader (future
                 (compat/submit! g #(% nil)
                                 (fn [_]
                                   (deliver entered true)
                                   @release
                                   :leader)))]
    (try
      (is (deref entered 5000 false))
      (let [follower (future
                       (binding [*compat-probe* :follower]
                         (compat/with-state false 23
                           (fn []
                             (let [submitter (Thread/currentThread)]
                               (compat/submit! g #(% nil)
                                               (fn [_]
                                                 [*compat-probe*
                                                  (compat/request-count)
                                                  (identical? submitter
                                                              (Thread/currentThread))])))))))]
        (is (loop [attempt 0]
              (cond
                (pos? (.size ^ConcurrentLinkedQueue (.-queue ^Group g))) true
                (= attempt 1000) false
                :else (do (Thread/sleep 1) (recur (inc attempt))))))
        (deliver release true)
        (is (= [:follower 23 false] (deref follower 5000 ::timeout))))
      (finally
        (deliver release true)
        (is (= :leader (deref leader 5000 ::timeout)))))))

(deftest handoff-rechecks-a-queue-drained-by-an-intervening-owner
  (let [stale? (atom true)
        ;; Model a nonempty isEmpty observation followed by another owner
        ;; draining the queue and resetting active before the handoff CAS.
        queue (proxy [ConcurrentLinkedQueue] []
                (isEmpty [] (if (compare-and-set! stale? true false) false true)))
        active (AtomicBoolean. true)
        g (Group. (ReentrantLock.) queue 8 active)]
    (#'group/handoff! g)
    (is (false? (.get active)))
    (is (= :done (group/submit! g #(% nil) (constantly :done))))))

(deftest uncontended-caller-skips-enqueue-and-ready-semaphore
  (let [g (group/create 8)]
    (is (= :done
           (group/submit!
            g (fn [execute]
                (is (.isEmpty ^ConcurrentLinkedQueue (.-queue ^Group g)))
                (is (nil? (.-ready ^datalevin.tx_group.Request
                           (.get ^org.eclipse.collections.impl.list.mutable.FastList
                            (group/requests execute) 0))))
                (execute nil))
            (constantly :done))))))

(deftest idle-single-request-does-not-materialize-a-batch
  (let [g (group/create 8)]
    (is (= :done
           (group/submit!
            g (fn [execute]
                (is (= 1 (group/request-count execute)))
                (is (nil? (group/current-requests execute)))
                (is (nil? (group/initial-request execute)))
                (let [result (execute nil)]
                  (is (nil? (group/current-requests execute)))
                  result))
            (constantly :done))))))

(deftest repeated-handoffs-do-not-strand-followers
  (doseq [limit [1 16]]
    (let [g (group/create limit)
          ready (CountDownLatch. 8) go (CountDownLatch. 1)
          runner (fn [execute]
                   (group/prepare-requests
                    execute (fn [op _]
                              (group/receipt execute (op nil) (fn [])))))
          jobs (mapv (fn [id]
                       (future
                         (.countDown ready) (.await go)
                         (dotimes [_ 5000]
                           (assert (= id (group/submit! g runner (constantly id)))))
                         :done)) (range 8))]
      (.await ready)
      (.countDown go)
      (is (= (repeat 8 :done) (mapv #(deref % 30000 ::timeout) jobs)))
      (is (.isEmpty ^ConcurrentLinkedQueue (.-queue ^Group g)))
      (is (false? (.get ^AtomicBoolean (.-active ^Group g)))))))

(deftest helping-collection-preserves-handoff-and-each-receipt
  (let [g (group/create 8)
        ready (CountDownLatch. 8) go (CountDownLatch. 1)]
    (letfn [(runner [execute]
              (group/prepare-requests
               execute (fn [op _]
                         (group/receipt
                          execute (op nil)
                          #(group/drain! g runner (+ (System/nanoTime) 1000000000))))))]
      (let [jobs (mapv (fn [id]
                         (future
                           (.countDown ready) (.await go)
                           (dotimes [_ 5000]
                             (assert (= id (group/submit! g runner (constantly id)))))
                           :done)) (range 8))]
        (.await ready)
        (.countDown go)
        (is (= (repeat 8 :done) (mapv #(deref % 30000 ::timeout) jobs)))
        (is (.isEmpty ^ConcurrentLinkedQueue (.-queue ^Group g)))
        (is (false? (.get ^AtomicBoolean (.-active ^Group g))))))))

(deftest specialization-requires-matching-request-contexts
  (doseq [different? [false true]]
    (let [g (group/create 2)
          ^ReentrantLock lock (.-lock ^Group g)
          ^ConcurrentLinkedQueue queue (.-queue ^Group g)
          specialized? (atom nil)
          jobs (atom [])
          runner (with-meta
                   (fn [execute]
                     (reset! specialized? (some? (group/batch-data execute)))
                     (execute nil))
                   {::group/context :shared})]
      (.lock lock)
      (try
        (doseq [idx (range 2)]
          (swap! jobs conj
                 (future
                   (let [context (if different? idx :shared)]
                     (group/submit! g runner
                                    (with-meta (constantly context)
                                      {::group/data idx ::group/context context})))))
          (is (loop [attempt 0]
                (cond (= (inc (long idx)) (.size queue)) true
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
                                  (group/committed
                                   result #(do (deliver entered true)
                                               (.acquireUninterruptibly release)))))
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

(defn- await-condition [pred]
  (loop [attempt 0]
    (cond (pred) true
          (= attempt 5000) false
          :else (do (Thread/sleep 1) (recur (inc attempt))))))

(deftest preparation-window-leaves-the-ordered-suffix-for-the-next-leader
  (doseq [queued? [false true]]
    (let [g (group/create 8)
          ^ReentrantLock lock (.-lock ^Group g)
          ^ConcurrentLinkedQueue queue (.-queue ^Group g)
          clock (atom 0) calls (atom []) batches (atom [])
          entered (promise) release (promise) jobs (atom [])
          runner (fn [execute]
                   (let [values (group/prepare-requests
                                 execute (fn [op _] (op nil)) 10)]
                     (swap! batches conj [(group/request-count execute) (vec values)])
                     values))]
      (with-redefs [group/preparation-nano-time (fn ^long [] (long @clock))]
        (when queued? (.lock lock))
        (try
          (swap! jobs conj
                 (future (group/submit! g runner
                                       (fn [_]
                                         (swap! calls conj 0)
                                         (deliver entered true)
                                         (assert (deref release 10000 false))
                                         (swap! clock + 5)
                                         0))))
          (when queued?
            (try (is (await-condition #(= 1 (.size queue))))
                 (finally (.unlock lock))))
          (is (deref entered 5000 false))
          (doseq [n (range 1 4)]
            (swap! jobs conj
                   (future (group/submit! g runner
                                         (fn [_]
                                           (swap! calls conj n)
                                           (swap! clock + 5)
                                           n))))
            (is (await-condition #(= n (.size queue)))))
          (deliver release true)
          (is (= [0 1 2 3] (mapv #(deref % 10000 ::timeout) @jobs)))
          (is (= [0 1 2 3] @calls) "No reordered or repeated bodies")
          (is (= [[2 [0 1]] [2 [2 3]]] @batches))
          (is (.isEmpty queue))
          (is (false? (.get ^AtomicBoolean (.-active ^Group g))))
          (finally
            (deliver release true)
            (doseq [job @jobs] (deref job 10000 nil))))))))

(deftest an-elected-leader-behind-the-prefix-releases-ready-callers-between-groups
  (let [g (group/create 8)
        clock (atom 0) ready (Semaphore. 0) result (volatile! nil)
        entered (promise) release (promise)
        runner (fn [execute]
                 (group/prepare-requests execute (fn [op _] (op nil)) 10))]
    ;; Model a later enqueuer winning the active CAS after an earlier enqueue.
    (.set ^AtomicBoolean (.-active ^Group g) true)
    (.add ^ConcurrentLinkedQueue (.-queue ^Group g)
          (datalevin.tx_group.Request.
           (fn [_] (reset! clock 10) :earlier) result ready nil nil (volatile! false)))
    (with-redefs [group/preparation-nano-time (fn ^long [] (long @clock))]
      (let [leader (future
                     (#'group/submit-queued!
                      g runner
                      (fn [_]
                        (deliver entered true)
                        (assert (deref release 10000 false))
                        :leader)
                      true 0))
            earlier (future (.acquireUninterruptibly ready) (#'group/result! result))]
        (try
          (is (deref entered 5000 false))
          (is (= :earlier (deref earlier 1000 ::blocked)))
          (is (not (realized? leader)))
          (deliver release true)
          (is (= :leader (deref leader 5000 ::timeout)))
          (finally
            (deliver release true)
            (deref leader 10000 nil)
            (deref earlier 10000 nil)))))))

(deftest request-readiness-follows-collector-release
  (doseq [queued? [false true]
          mode [:native :receipt :deferred :failure]]
    (let [body-entered (promise) release-body (promise)
          unlock-entered (promise) release-unlock (promise)
          notified-under-lock (promise)
          armed (AtomicBoolean. false)
          lock (proxy [ReentrantLock] [true]
                 (unlock []
                   (when (.compareAndSet armed true false)
                     (deliver unlock-entered true)
                     (assert (deref release-unlock 10000 false)))
                   (proxy-super unlock)))
          g (Group. lock (ConcurrentLinkedQueue.) 8 (AtomicBoolean. false))
          ready (proxy [Semaphore] [0]
                  (release []
                    (deliver notified-under-lock (.isHeldByCurrentThread lock))
                    (proxy-super release)))
          result (volatile! nil)
          error (ex-info "Commit failed" {})
          runner (fn [execute]
                   (let [values (execute nil)]
                     (case mode
                       :native values
                       :receipt (object-array
                                 (map #(group/receipt execute % (fn [])) values))
                       :deferred (group/defer-completion execute (constantly values))
                       :failure (throw error))))
          _ (when queued? (.lock lock))
          leader (future
                   (try
                     (group/submit! g runner
                                    (fn [_]
                                      (deliver body-entered true)
                                      (assert (deref release-body 10000 false))
                                      :leader))
                     (catch Throwable e e)))
          follower (future
                     (.acquireUninterruptibly ready)
                     (try (#'group/result! result) (catch Throwable e e)))]
      (try
        (when queued?
          (try
            (is (await-condition #(= 1 (.size ^ConcurrentLinkedQueue (.-queue g)))))
            (finally (.unlock lock))))
        (is (deref body-entered 5000 false))
        (.set armed true)
        (.add ^ConcurrentLinkedQueue (.-queue g)
              (datalevin.tx_group.Request. (constantly :follower) result ready
                                            nil nil (volatile! false)))
        (deliver release-body true)
        (is (deref unlock-entered 5000 false))
        (is (some? @result) "Result storage may be populated under the lock")
        (is (not (realized? notified-under-lock))
            "Readiness must not be signalled while the producing leader owns the lock")
        (is (not (realized? follower)))
        (deliver release-unlock true)
        (is (false? (deref notified-under-lock 5000 ::timeout)))
        (is (= (if (= mode :failure) error :leader) (deref leader 5000 ::timeout)))
        (is (= (if (= mode :failure) error :follower) (deref follower 5000 ::timeout)))
        (finally
          (deliver release-body true)
          (deliver release-unlock true)
          (deref leader 5000 nil)
          (.release ready)
          (deref follower 5000 nil))))))

(deftest writer-acquisition-collects-a-bounded-batch-before-commit
  (let [g (group/create 3)
        ^ConcurrentLinkedQueue queue (.-queue ^Group g)
        writer (ReentrantLock.)
        commit-entered (promise) release-commit (promise)
        sizes (atom [])
        jobs (atom [])
        runner (fn [execute]
                 (.lock writer)
                 (try
                   (let [result (execute nil)
                         n (alength ^objects result)]
                     (is (= n (group/request-count execute)) "Commit sees the collected count")
                     (when (= n 3)
                       (deliver commit-entered true)
                       (assert (deref release-commit 10000 false)))
                     (swap! sizes conj n)
                     result)
                   (finally (.unlock writer))))]
    (.lock writer)
    (try
      (swap! jobs conj (future (group/submit! g runner (constantly 0))))
      (is (await-condition #(.hasQueuedThreads writer)))
      (doseq [n (range 1 5)]
        (swap! jobs conj (future (group/submit! g runner (constantly n)))))
      (is (await-condition #(= 4 (.size queue))))
      (finally (.unlock writer)))
    (try
      (is (deref commit-entered 10000 false))
      (is (not-any? realized? @jobs))
      (deliver release-commit true)
      (is (= (vec (range 5)) (mapv #(deref % 10000 ::timeout) @jobs)))
      (is (= [3 2] @sizes))
      (finally (deliver release-commit true)))))

(deftest transaction-retries-retain-the-collected-requests
  (let [g (group/create 8)
        ^ConcurrentLinkedQueue queue (.-queue ^Group g)
        first-attempt (promise) retry (promise)
        calls (atom {})
        batches (atom [])
        runner (fn [execute]
                 (let [first-result (vec (execute nil))]
                   (when (= [:first] first-result)
                     (deliver first-attempt true)
                     (assert (deref retry 10000 false)))
                   (let [result (execute nil)]
                     (is (= first-result (vec result)))
                     (swap! batches conj (vec result))
                     result)))
        op (fn [v] (fn [_] (swap! calls update v (fnil inc 0)) v))
        first-job (future (group/submit! g runner (op :first)))]
    (try
      (is (deref first-attempt 10000 false))
      (let [next-job (future (group/submit! g runner (op :next)))]
        (is (await-condition #(= 1 (.size queue))))
        (deliver retry true)
        (is (= [:first :next] (mapv #(deref % 10000 ::timeout)
                                    [first-job next-job]))))
      (is (= [[:first] [:next]] @batches))
      (is (= {:first 2 :next 2} @calls))
      (finally (deliver retry true)))))

(deftest deferred-completion-shares-results-and-explicit-confirmation
  (let [g (group/create 3)
        ^ConcurrentLinkedQueue queue (.-queue ^Group g)
        ^ReentrantLock lock (.-lock ^Group g)
        completion-entered (promise) release-completion (Semaphore. 0)
        finishes (atom 0) confirmations (atom 0)
        jobs (atom [])
        runner (fn [execute]
                 (let [values (execute nil)]
                   (group/defer-completion execute
                                           (fn []
                                             (is (not (.isLocked lock)))
                                             (swap! finishes inc)
                                             (deliver completion-entered true)
                                             (.acquireUninterruptibly release-completion)
                                             (group/committed values #(swap! confirmations inc))))))]
    (.lock lock)
    (try
      (doseq [n (range 3)]
        (swap! jobs conj
               (future (group/submit! g runner (constantly n))))
        (is (await-condition #(= (inc (long n)) (.size queue)))))
      (finally (.unlock lock)))
    (try
      (is (deref completion-entered 10000 false))
      (is (not-any? realized? @jobs))
      (.release release-completion)
      (is (= [0 1 2] (mapv #(deref % 10000 ::timeout) @jobs)))
      (is (= 1 @finishes @confirmations))
      (finally (.release release-completion)))))

(deftest deferred-failure-never-replays-request-bodies
  (let [g (group/create 3)
        ^ConcurrentLinkedQueue queue (.-queue ^Group g)
        ^ReentrantLock lock (.-lock ^Group g)
        calls (atom 0) finishes (atom 0)
        ;; Even this tag cannot make a failure after append safe to retry.
        error (ex-info "late commit failure" {::group/body-failure true})
        jobs (atom [])
        runner (fn [execute]
                 (execute nil)
                 (group/defer-completion execute
                                         #(do (swap! finishes inc) (throw error))))]
    (.lock lock)
    (try
      (doseq [_ (range 3)]
        (swap! jobs conj
               (future (try (group/submit! g runner (fn [_] (swap! calls inc)))
                            (catch Throwable e e)))))
      (is (await-condition #(= 3 (.size queue))))
      (finally (.unlock lock)))
    (is (every? #(identical? error (deref % 10000 ::timeout)) @jobs))
    (is (= 3 @calls))
    (is (= 1 @finishes))
    (is (= :next (group/submit! g #(% nil) (constantly :next))))))

(deftest deferred-completion-retains-interrupted-caller-status
  (let [g (group/create 2)
        result (future
                 (.interrupt (Thread/currentThread))
                 (try
                   (let [value (group/submit!
                                g
                                (fn [execute]
                                  (let [values (execute nil)]
                                    (group/defer-completion execute (constantly values))))
                                (constantly :done))]
                     [value (.isInterrupted (Thread/currentThread))])
                   (finally (Thread/interrupted))))]
    (is (= [:done true] (deref result 10000 ::timeout)))))

(deftest appended-request-completion-is-independent-of-its-collector-group
  (let [g (group/create 3)
        ^ConcurrentLinkedQueue queue (.-queue ^Group g)
        ^ReentrantLock lock (.-lock ^Group g)
        entered (promise) release (Semaphore. 0)
        events (atom []) jobs (atom [])
        runner (fn [execute]
                 (group/prepare-requests
                  execute
                  (fn [op _]
                    (let [value (op nil)]
                      (group/committed
                       (group/receipt
                        execute value
                        (fn []
                          (is (not (.isHeldByCurrentThread lock)))
                          (when (zero? (long value))
                            (deliver entered true)
                            (.acquireUninterruptibly release))
                          (swap! events conj [:completed value])))
                       #(swap! events conj [:confirmed value]))))))]
    (.lock lock)
    (try
      (doseq [n (range 3)]
        (swap! jobs conj
               (future (group/submit! g runner (constantly n))))
        (is (await-condition #(= (inc (long n)) (.size queue)))))
      (finally (.unlock lock)))
    (try
      (is (deref entered 5000 false))
      (is (= [1 2] (mapv #(deref % 5000 ::timeout) (subvec @jobs 1))))
      (is (not (realized? (first @jobs))))
      (is (not-any? #(= [:confirmed 0] %) @events))
      (.release release)
      (is (= 0 (deref (first @jobs) 5000 ::timeout)))
      (doseq [n (range 3)]
        (is (< (.indexOf ^java.util.List @events [:completed n])
               (.indexOf ^java.util.List @events [:confirmed n]))))
      (finally (.release release)))))

(deftest independent-receipt-failures-do-not-resubmit-or-fail-neighbors
  (let [g (group/create 3)
        ^ConcurrentLinkedQueue queue (.-queue ^Group g)
        ^ReentrantLock lock (.-lock ^Group g)
        calls (atom []) jobs (atom [])
        error (ex-info "durable application failed" {::group/body-failure true})
        runner (fn [execute]
                 (group/prepare-requests
                  execute
                  (fn [op _]
                    (let [value (op nil)]
                      (group/receipt execute value
                                     #(when (= value 1) (throw error)))))))]
    (.lock lock)
    (try
      (doseq [n (range 3)]
        (swap! jobs conj
               (future (try
                         (group/submit! g runner
                                        (fn [_] (swap! calls conj n) n))
                         (catch Throwable e e))))
        (is (await-condition #(= (inc (long n)) (.size queue)))))
      (finally (.unlock lock)))
    (is (= [0 error 2] (mapv #(deref % 5000 ::timeout) @jobs)))
    (is (= [0 1 2] @calls))
    (is (= :next (group/submit! g runner (constantly :next))))))

(deftest released-leadership-drains-arrivals-into-one-record
  ;; The WAL runner releases the collector lock while it waits for the ordered
  ;; preparation turn. Requests arriving in that window must join the same
  ;; sealed group instead of becoming singleton records.
  (let [g (group/create 8)
        ^ConcurrentLinkedQueue queue (.-queue ^Group g)
        entered (promise) release (promise)
        records (atom []) jobs (atom [])
        runner (fn [execute]
                 (let [extract (fn [op _] (op nil))
                       initial (group/collect-submissions execute extract)]
                   (group/release-collection! execute)
                   (deliver entered true)
                   (assert (deref release 10000 false))
                   (group/acquire-collection! execute)
                   (let [late (group/collect-submissions execute extract (count initial))
                         batch (into initial late)]
                     (group/seal! execute)
                     (group/release-collection! execute)
                     (let [result (object-array batch)]
                       (group/acquire-collection! execute)
                       (swap! records conj (vec batch))
                       result))))]
    (swap! jobs conj (future (group/submit! g runner (constantly 0))))
    (is (deref entered 5000 false))
    (doseq [n (range 1 4)]
      (swap! jobs conj (future (group/submit! g runner (constantly n)))))
    (is (await-condition #(= 3 (.size queue))))
    (deliver release true)
    (is (= [0 1 2 3] (mapv #(deref % 10000 ::timeout) @jobs)))
    (is (= 1 (count @records)) "arrivals while released share one sealed record")
    (is (= #{0 1 2 3} (set (first @records))))
    (is (.isEmpty queue))))

(deftest eight-writer-released-groups-keep-writes-per-record-above-one
  ;; Regression gate: eight contending writers must amortize into records, the
  ;; behavior the descriptor-sealing refactor destroyed.
  (let [g (group/create 64)
        submissions (atom 0)
        records (atom 0)
        runner (fn [execute]
                 (let [extract (fn [op _] (op nil))
                       initial (group/collect-submissions execute extract)]
                   (group/release-collection! execute)
                   ;; Hold the released window long enough for concurrent
                   ;; writers to queue, then drain them into this record.
                   (Thread/sleep 1)
                   (group/acquire-collection! execute)
                   (let [late (group/collect-submissions execute extract (count initial))
                         batch (into initial late)]
                     (group/seal! execute)
                     (group/release-collection! execute)
                     (let [result (object-array batch)]
                       (group/acquire-collection! execute)
                       (swap! submissions + (count batch))
                       (swap! records inc)
                       result))))
        jobs (mapv (fn [id]
                     (future
                       (dotimes [_ 400]
                         (group/submit! g runner (constantly id)))))
                   (range 8))]
    (is (every? #(not= ::timeout (deref % 60000 ::timeout)) jobs))
    (is (pos? @submissions))
    (is (> (/ (double @submissions) (double @records)) 1.0)
        (str "writes/record was " (/ (double @submissions) (double @records))))))
