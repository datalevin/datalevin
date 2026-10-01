(ns datalevin.tx-group-batch-test
  "Invariant tests for the additive new-protocol collector.

  These cover the collector's own responsibilities: admission, bounded FIFO
  sealing, one executing batch, collection through join, dispatch gating,
  exactly-once result delivery, the failure fence and accounting. WAL/native
  branch behaviour belongs to the environment executor."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.phase :as phase])
  (:import [java.util.concurrent ConcurrentLinkedQueue CountDownLatch TimeUnit]
           [java.util.concurrent.atomic AtomicInteger]))

(def ^:private default-overrides
  {:wal-pending-max-requests 64
   :wal-pending-max-bytes 1048576
   :write-batch-size 8
   :write-batch-max-bytes 12288
   :wal-rmw-max-bytes 4096})

(defn- collector-with
  ([executor] (collector-with executor nil))
  ([executor overrides]
   (batch/create executor
                 {:limits (charge/resolve-limits
                           (merge default-overrides overrides))})))

(defn- echo-values
  "Default executor: each request returns its own prepared data, in order."
  [batch]
  (let [descriptors (.descriptors batch)
        n (.size descriptors)
        values (object-array n)]
    (dotimes [i n]
      (aset values i (batch/data (batch/batch-at batch i))))
    values))

(defn- await! [p]
  (let [v (deref p 5000 ::timeout)]
    (is (not= ::timeout v) "timed out waiting for a promise")
    v))

(defn- timeout [] ::timeout)

;; ---------------------------------------------------------------------------
;; Lifecycle and limits

(deftest resolved-limits-are-frozen-on-the-collector
  (let [c (collector-with echo-values)]
    (is (= 8 (:batch-limit (batch/limits c))))
    (is (= 12288 (:batch-max-bytes (batch/limits c))))
    (is (= 64 (:waiter-limit (batch/limits c))))
    (testing "the shared workspace reservation is reported, not request-charged"
      (is (= (:shared-reserved (batch/limits c))
             (:shared-reserved (batch/usage c)))))))

(deftest a-single-request-returns-its-own-value
  (let [c (collector-with echo-values)]
    (is (= :a (batch/submit! c {:allowance 1024 :data :a})))
    (is (= :b (batch/submit! c {:allowance 1024 :data :b})))
    (testing "each request holds exactly one reserve/release pair"
      (is (zero? (:requests (batch/usage c))))
      (is (zero? (:bytes (batch/usage c)))))))

(deftest joined-prefix-advances-only-on-join
  (testing "each joined group advances the prefix to its assigned LSN"
    (let [lsn (AtomicInteger. 0)
          c (collector-with (fn [batch]
                              (batch/set-lsn! batch (.incrementAndGet lsn))
                              (echo-values batch)))]
      (batch/submit! c {:allowance 1024 :data :a})
      (is (= 1 (batch/published-lsn c)))
      (batch/submit! c {:allowance 1024 :data :b})
      (is (= 2 (batch/published-lsn c)))))
  (testing "a group without an assigned LSN advances nothing"
    (let [c (collector-with echo-values)]
      (batch/submit! c {:allowance 1024 :data :a})
      (batch/submit! c {:allowance 1024 :data :b})
      (is (zero? (batch/published-lsn c))))))

;; ---------------------------------------------------------------------------
;; Fixed order, membership and one executing batch

(deftest seal-preserves-fifo-order
  (let [sealed (ConcurrentLinkedQueue.)
        published (ConcurrentLinkedQueue.)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        ;; Publication order is recorded by the collector itself, so this test
        ;; asserts FIFO against the real enqueue order rather than against the
        ;; order in which concurrent submitters happened to start.
        remove-observer (phase/observe!
                         (fn [event context]
                           (when (= :ready-published event)
                             (.add published (batch/data context)))))
        executor (fn [batch]
                   (dotimes [i (batch/batch-count batch)]
                     (.add sealed (batch/data (batch/batch-at batch i))))
                   ;; Only the first batch blocks, so the queued followers are
                   ;; then sealed as one prefix.
                   (when (zero? (.getAndIncrement first-batch))
                     (.countDown entered)
                     (.await release 5 TimeUnit/SECONDS))
                   (echo-values batch))
        c (collector-with executor)
        leader (future (batch/submit! c {:allowance 1024 :data :first}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [followers (mapv (fn [i] (future (batch/submit! c {:allowance 1024 :data i})))
                            [:a :b :c])]
        (.countDown release)
        (is (= :first (await! leader)))
        (doseq [f followers] (is (not= (timeout) (deref f 5000 ::timeout))))
        (testing "sealed membership follows publication order exactly"
          (is (= (vec published) (vec sealed)))
          (is (= :first (first (vec sealed)))))
        (testing "each request keeps its own return value"
          (is (= [:a :b :c] (mapv deref followers)))))
      (finally
        (.countDown release)
        (remove-observer)))))

(deftest only-one-batch-executes-at-a-time
  (let [concurrent (AtomicInteger. 0)
        peak (AtomicInteger. 0)
        executor (fn [batch]
                   (let [now (.incrementAndGet concurrent)]
                     (.updateAndGet peak (fn [p] (max (long p) (long now))))
                     (Thread/sleep 2)
                     (.decrementAndGet concurrent))
                   (echo-values batch))
        c (collector-with executor)
        writers (mapv (fn [i] (future (batch/submit! c {:allowance 1024 :data i})))
                      (range 16))]
    (doseq [w writers] (is (not= (timeout) (deref w 10000 ::timeout))))
    (is (= 1 (.get peak)) "a second batch ran before the first joined")))

(deftest later-arrivals-accumulate-unsealed-through-join
  (let [sizes (ConcurrentLinkedQueue.)
        schedules (ConcurrentLinkedQueue.)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        executor (fn [batch]
                   (.add sizes (.size (.descriptors batch)))
                   (.add schedules (.schedule batch))
                   (.countDown entered)
                   (.await release 5 TimeUnit/SECONDS)
                   (echo-values batch))
        c (collector-with executor)
        leader (future (batch/submit! c {:allowance 1024 :data :first}))]
    (is (.await entered 5 TimeUnit/SECONDS))
    (let [followers (mapv (fn [i] (future (batch/submit! c {:allowance 1024 :data i})))
                          [:a :b :c])]
      (Thread/sleep 100)
      (is (= 1 (.size sizes)) "a second batch was sealed before the first joined")
      (.countDown release)
      (is (= :first (await! leader)))
      (doseq [f followers] (is (not= (timeout) (deref f 5000 ::timeout))))
      (testing "the queued prefix is sealed only at joint completion"
        (is (= [1 3] (vec sizes))))
      (testing "the schedule is chosen from the frozen weight"
        (is (= [:inline :parallel] (vec schedules)))))))

(deftest batch-selection-honours-the-request-count-cap
  (let [sizes (ConcurrentLinkedQueue.)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        executor (fn [batch]
                   (.add sizes (batch/batch-count batch))
                   (when (zero? (.getAndIncrement first-batch))
                     (.countDown entered)
                     (.await release 5 TimeUnit/SECONDS))
                   (echo-values batch))
        c (collector-with executor {:wal-pending-max-bytes 8388608
                                    :write-batch-size 4
                                    :write-batch-max-bytes 1048576
                                    :wal-rmw-max-bytes 4096})
        leader (future (batch/submit! c {:allowance 1024 :data :first}))]
    (is (.await entered 5 TimeUnit/SECONDS))
    (let [followers (mapv (fn [i] (future (batch/submit! c {:allowance 1024 :data i})))
                          (range 7))]
      (.countDown release)
      (is (= :first (await! leader)))
      (doseq [f followers] (is (not= (timeout) (deref f 5000 ::timeout))))
      (let [observed (vec sizes)]
        (testing "the first batch is the blocked singleton"
          (is (= 1 (first observed))))
        (testing "no sealed batch exceeds the request-count cap"
          (is (every? #(<= (long %) 4) observed)))
        (testing "every admitted request is served exactly once"
          (is (= 8 (reduce + observed)))))
      )))

(deftest batch-selection-honours-the-byte-cap-without-skipping-a-head
  ;; Allowances of 4096 against a 9000-byte cap fit exactly two.
  (let [sizes (ConcurrentLinkedQueue.)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        executor (fn [batch]
                   (.add sizes (.size (.descriptors batch)))
                   (when (zero? (.getAndIncrement first-batch))
                     (.countDown entered)
                     (.await release 5 TimeUnit/SECONDS))
                   (echo-values batch))
        c (collector-with executor {:write-batch-max-bytes 9000})
        leader (future (batch/submit! c {:allowance 1024 :data :first}))]
    (is (.await entered 5 TimeUnit/SECONDS))
    (let [followers (mapv (fn [i] (future (batch/submit! c {:allowance 4096 :data i})))
                          [:big :big :small])]
      (.countDown release)
      (is (= :first (await! leader)))
      (doseq [f followers] (is (not= (timeout) (deref f 5000 ::timeout))))
      (testing "the oversized head keeps its place instead of being bypassed"
        (is (= [1 2 1] (vec sizes)))))))

(deftest an-allowance-above-the-batch-cap-is-rejected-before-admission
  (let [c (collector-with echo-values {:write-batch-max-bytes 2048
                                      :wal-rmw-max-bytes 1024})
        thrown (try (batch/submit! c {:allowance 4096 :data :too-big})
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
    (is (some? thrown))
    (is (= :txlog/pending-budget-exceeded (:error (ex-data thrown))))
    (is (= :not-committed (:outcome (ex-data thrown))))
    (is (false? (:retryable? (ex-data thrown))))
    (testing "a rejected allowance never reaches the request budget"
      (is (zero? (:requests (batch/usage c))))
      (is (zero? (:bytes (batch/usage c)))))))

;; ---------------------------------------------------------------------------
;; Per-request rejection and failure

(deftest a-request-local-rejection-does-not-fail-its-batch
  (let [executor (fn [batch]
                   (let [descriptors (.descriptors batch)
                         n (.size descriptors)
                         values (object-array n)]
                     (dotimes [i n]
                       (let [d (batch/batch-at batch i)]
                         (aset values i
                               (if (= :bad (batch/data d))
                                 (batch/rejected (ex-info "row too wide" {:row 1}))
                                 (batch/data d)))))
                     values))
        c (collector-with executor)
        ok (batch/submit! c {:allowance 1024 :data :ok})
        rejected (try (batch/submit! c {:allowance 1024 :data :bad})
                      nil
                      (catch clojure.lang.ExceptionInfo e e))]
    (is (= :ok ok))
    (is (= "row too wide" (ex-message rejected)))
    (testing "the runtime stays serving and keeps admitting"
      (is (batch/serving? c))
      (is (= :after (batch/submit! c {:allowance 1024 :data :after}))))
    (is (zero? (:requests (batch/usage c))))))

(deftest caller-preparation-failure-rejects-only-that-request
  (let [c (collector-with echo-values)
        thrown (try (batch/submit! c {:allowance 1024
                                      :prepare (fn []
                                                 (throw (ex-info "encode failed"
                                                                 {:row 2})))})
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
    (is (= "encode failed" (ex-message thrown)))
    (testing "no body ran and the reservation was released"
      (is (batch/serving? c))
      (is (zero? (:requests (batch/usage c))))
      (is (= :ok (batch/submit! c {:allowance 1024 :data :ok}))))))

(deftest executor-failure-fences-and-preserves-the-established-outcome
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        boom (ex-info "wal append failed" {:error :txlog/write-indeterminate
                                           :outcome :indeterminate})
        first-batch (AtomicInteger. 0)
        executor (fn [batch]
                   (when (zero? (.getAndIncrement first-batch))
                     (.countDown entered)
                     (.await release 5 TimeUnit/SECONDS)
                     (throw boom))
                   (echo-values batch))
        c (collector-with executor)
        leader (future (try (batch/submit! c {:allowance 1024 :data :first})
                            (catch Throwable t t)))]
    (is (.await entered 5 TimeUnit/SECONDS))
    (let [followers (mapv (fn [i] (future (try (batch/submit! c {:allowance 1024 :data i})
                                             (catch Throwable t t))))
                          [:a :b :c])]
      (.countDown release)
      (is (identical? boom (deref leader 5000 ::timeout)))
      (doseq [f followers]
        (let [t (deref f 5000 ::timeout)]
          (is (some? t))
          (is (= :txlog/write-indeterminate (:error (ex-data t))))))
      (testing "the terminal fence closes admission permanently"
        (is (not (batch/serving? c)))
        (let [late (try (batch/submit! c {:allowance 1024 :data :late})
                        nil
                        (catch Throwable t t))]
          (is (some? late))
          (is (= :txlog/write-indeterminate (:error (ex-data late))))))
      (testing "no reservation is refunded while a request is still owned"
        (is (zero? (:requests (batch/usage c))))))))

(deftest fence-rejects-queued-requests-and-wakes-waiters
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        c (collector-with (fn [batch]
                            (.countDown entered)
                            (.await release 5 TimeUnit/SECONDS)
                            (echo-values batch)))
        ;; The leader future must be the only request until it is executing,
        ;; otherwise a follower can win the election and change the outcome.
        leader (future (try (batch/submit! c {:allowance 1024 :data :first})
                            (catch Throwable t t)))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [followers (mapv (fn [i] (future (try (batch/submit! c {:allowance 1024 :data i})
                                                 (catch Throwable t t))))
                            [:a :b])]
        (Thread/sleep 50)
        (batch/fence! c (ex-info "terminal" {:error :txlog/runtime-fenced
                                             :outcome :not-committed}))
        (.countDown release)
        (doseq [f followers]
          (let [t (deref f 5000 ::timeout)]
            (is (some? t))
            (is (= :txlog/runtime-fenced (:error (ex-data t))))))
        (is (not (batch/serving? c)))
        ;; The rejected followers are done; wait for the executing leader to
        ;; release its reservation before asserting drained usage.
        @leader
        (is (zero? (:requests (batch/usage c)))))
      (finally
        (.countDown release)
        @leader))))

(deftest close-stops-admission-and-rejects-queued-work
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        c (collector-with (fn [batch]
                            (.countDown entered)
                            (.await release 5 TimeUnit/SECONDS)
                            (echo-values batch)))
        leader (future (batch/submit! c {:allowance 1024 :data :first}))]
    (is (.await entered 5 TimeUnit/SECONDS))
    (let [follower (future (try (batch/submit! c {:allowance 1024 :data :a})
                               (catch Throwable t t)))]
      (Thread/sleep 50)
      (batch/close! c)
      (let [t (deref follower 5000 ::timeout)]
        (is (= :txlog/runtime-closed (:error (ex-data t)))))
      (testing "the executing batch keeps its own outcome through shutdown"
        (.countDown release)
        (is (= :first (deref leader 5000 ::timeout)))))))

;; ---------------------------------------------------------------------------
;; Accounting

(deftest admission-accounts-full-allowances-and-requests
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        executor (fn [batch]
                   (when (zero? (.getAndIncrement first-batch))
                     (.countDown entered)
                     (.await release 5 TimeUnit/SECONDS))
                   (echo-values batch))
        c (collector-with executor)
        leader (future (batch/submit! c {:allowance 4096 :data :first}))]
    (is (.await entered 5 TimeUnit/SECONDS))
    (let [followers (mapv (fn [_] (future (batch/submit! c {:allowance 4096 :data :f})))
                          (range 3))
          usage (do (Thread/sleep 50) (batch/usage c))]
      (is (= 4 (:requests usage)))
      (is (= (* 4 4096) (:bytes usage)))
      (.countDown release)
      @leader
      (doseq [f followers] @f)
      (testing "reservations release once results are consumed"
        (is (zero? (:requests (batch/usage c))))
        (is (zero? (:bytes (batch/usage c))))))))

(deftest admission-backpressures-in-the-bounded-waiter-set
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        waits (AtomicInteger. 0)
        remove-observer (phase/observe!
                         (fn [event _]
                           (when (= :admission-wait event) (.incrementAndGet waits))))
        c (collector-with (fn [batch]
                            (.countDown entered)
                            (.await release 5 TimeUnit/SECONDS)
                            (echo-values batch))
                          {:wal-pending-max-requests 1
                           :write-batch-size 1
                           :write-batch-max-bytes 1024
                           :wal-rmw-max-bytes 1024})
        leader (future (batch/submit! c {:allowance 1024 :data :first}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [followers (mapv (fn [_] (future (try (batch/submit! c {:allowance 1024 :data :f})
                                                (catch Throwable t t))))
                            (range 4))]
        (Thread/sleep 150)
        (is (pos? (.get waits)) "excess requests did not wait for admission")
        (.countDown release)
        (is (= :first (deref leader 5000 ::timeout)))
        (doseq [f followers]
          (let [v (deref f 5000 ::timeout)]
            (is (or (= :f v) (instance? Throwable v)))))
        (is (zero? (:requests (batch/usage c)))))
      (finally
        (.countDown release)
        (remove-observer)))))

(deftest prepaid-charges-are-not-rechecked-per-allocation
  (let [c (collector-with echo-values)
        checked (AtomicInteger. 0)]
    (with-redefs [batch/charge! (fn [_descriptor extra]
                                  (.incrementAndGet checked extra))]
      (batch/submit! c {:allowance 1024 :data :a})
      (batch/submit! c {:allowance 2048 :data :b}))
    (testing "a prepaid blind request performs no incremental charge checks"
      (is (zero? (.get checked))))))

;; ---------------------------------------------------------------------------
;; Phase seam

(deftest the-phase-seam-records-the-execution-cycle
  (let [events (ConcurrentLinkedQueue.)
        remove-observer (phase/observe! (fn [event _] (.add events event)))
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        c (collector-with (fn [batch]
                            (.countDown entered)
                            (.await release 5 TimeUnit/SECONDS)
                            (echo-values batch)))
        leader (future (batch/submit! c {:allowance 1024
                                         :prepare (fn [] nil)
                                         :data :first}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [follower (future (batch/submit! c {:allowance 1024 :data :a}))]
        (.countDown release)
        @leader
        @follower)
      (let [seen (set (vec events))]
        (testing "preparation, publication, sealing and join are traceable"
          (is (contains? seen :admitted))
          (is (contains? seen :caller-preparation))
          (is (contains? seen :caller-prepared))
          (is (contains? seen :ready-published))
          (is (contains? seen :batch-sealed))
          (is (contains? seen :ordered-work))
          (is (contains? seen :schedule-selected))
          (is (contains? seen :joint-publication))
          (is (contains? seen :next-activation))))
      (finally
        (.countDown release)
        (remove-observer)))
    (testing "uninstalling the observer restores a disabled seam"
      (is (not (phase/observed?))))))

;; ---------------------------------------------------------------------------
;; Execution threads and request context

(def ^:dynamic *probe* :root)

(defn- submit-on-raw-thread!
  "Submit from a plain thread that inherits no caller binding frame, so the
  collector cannot be credited with avoiding one."
  [collector request]
  (let [result (promise)]
    (doto (Thread. #(deliver result (try (batch/submit! collector request)
                                        (catch Throwable t t))))
      (.start))
    (deref result 5000 ::timeout)))

(deftest bodies-run-on-execution-threads-without-binding-affinity
  (binding [*probe* :caller]
    (with-redefs [clojure.core/get-thread-bindings
                  (fn [] (throw (ex-info "collector captured bindings" {})))]
      (let [executor (fn [batch]
                       (let [n (batch/batch-count batch)
                             values (object-array n)]
                         (dotimes [i n]
                           (aset values i
                                  [(batch/data (batch/batch-at batch i)) *probe*]))
                         values))
            c (collector-with executor)
            leader-value (submit-on-raw-thread! c {:allowance 1024 :data :first})
            follower-value (submit-on-raw-thread! c {:allowance 1024 :data :second})]
        (testing "each request returns its own prepared data"
          (is (= [:first :root] leader-value))
          (is (= [:second :root] follower-value)))
        (testing "a submitting caller's binding is never conveyed to execution"
          (is (= [:root :root] (mapv second [leader-value follower-value]))))
        (testing "ambient state may be visible but carries no per-request meaning"
          (let [same-thread (batch/submit! c {:allowance 1024 :data :third})]
            (is (= [:third :caller] same-thread))))))))

;; ---------------------------------------------------------------------------
;; Admission waiting, deadlines and charging

(deftest capacity-waiters-are-admitted-after-a-release
  (let [entered (CountDownLatch. 1)
        waiting (CountDownLatch. 1)
        release (CountDownLatch. 1)
        remove-observer (phase/observe!
                         (fn [event _]
                           (when (= :admission-wait event)
                             (.countDown waiting))))
        first-batch (AtomicInteger. 0)
        c (collector-with
           (fn [batch]
             (when (zero? (.getAndIncrement first-batch))
               (.countDown entered)
               (.await release 5 TimeUnit/SECONDS))
             (echo-values batch))
           {:wal-pending-max-requests 1
            :write-batch-size 1
            :write-batch-max-bytes 1024
            :wal-rmw-max-bytes 1024})
        leader (future (batch/submit! c {:allowance 1024 :data :leader}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [follower (future (batch/submit! c {:allowance 1024 :data :follower}))]
        (testing "the follower parks as a capacity waiter"
          (is (.await waiting 5 TimeUnit/SECONDS)))
        (.countDown release)
        (is (= :leader (deref leader 5000 ::timeout)))
        (testing "a capacity waiter is admitted after the holder releases"
          (is (= :follower (deref follower 5000 ::timeout)))))
      (finally
        (.countDown release)
        (remove-observer)))
    (is (zero? (:requests (batch/usage c))))
    (is (zero? (:bytes (batch/usage c))))))

(deftest a-request-with-a-timeout-returns-its-value
  (let [c (collector-with echo-values)]
    (is (= :a (batch/submit! c {:allowance 1024 :data :a :timeout-ms 5000})))))

(deftest an-expired-queued-request-is-rejected-without-fencing
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        c (collector-with
           (fn [batch]
             (when (zero? (.getAndIncrement first-batch))
               (.countDown entered)
               (.await release 5 TimeUnit/SECONDS))
             (echo-values batch)))
        leader (future (batch/submit! c {:allowance 1024 :data :leader}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [expiring (future (try (batch/submit! c {:allowance 1024 :data :late
                                                    :timeout-ms 1})
                                  (catch Throwable t t)))]
        (Thread/sleep 100)
        (.countDown release)
        (is (= :leader (deref leader 5000 ::timeout)))
        (let [v (deref expiring 5000 ::timeout)]
          (is (instance? Throwable v))
          (is (= :txlog/write-deadline-exceeded (:error (ex-data v)))))
        (testing "request-local expiry does not fence the runtime"
          (is (batch/serving? c))
          (is (= :after (batch/submit! c {:allowance 1024 :data :after})))))
      (finally (.countDown release)))))

(deftest an-expired-selected-batch-cancels-without-fencing
  (let [at-ordered (CountDownLatch. 1)
        proceed (CountDownLatch. 1)
        blocked (AtomicInteger. 0)
        remove-observer (phase/observe!
                         (fn [event _]
                           (when (and (= :ordered-work event)
                                      (.compareAndSet blocked 0 1))
                             (.countDown at-ordered)
                             (.await proceed 5 TimeUnit/SECONDS))))
        c (collector-with echo-values)]
    (try
      (let [f (future (try (batch/submit! c {:allowance 1024 :data :a
                                             :timeout-ms 50})
                           (catch Throwable t t)))]
        (is (.await at-ordered 5 TimeUnit/SECONDS))
        (Thread/sleep 100)
        (.countDown proceed)
        (let [v (deref f 5000 ::timeout)]
          (is (instance? Throwable v))
          (is (= :txlog/write-deadline-exceeded (:error (ex-data v)))))
        (testing "a clean pre-dispatch expiry does not fence the runtime"
          (is (batch/serving? c))
          (is (= :after (batch/submit! c {:allowance 1024 :data :after})))))
      (finally
        (.countDown proceed)
        (remove-observer)))))

(deftest a-non-head-queued-expiry-does-not-cancel-earlier-work
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        c (collector-with
           (fn [batch]
             (when (zero? (.getAndIncrement first-batch))
               (.countDown entered)
               (.await release 5 TimeUnit/SECONDS))
             (echo-values batch)))
        leader (future (batch/submit! c {:allowance 1024 :data :leader}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      ;; A long-deadline request queues first; a short-deadline request queues
      ;; behind it. Only the second expires.
      (let [survivor (future (try (batch/submit! c {:allowance 1024
                                                    :data :survivor
                                                    :timeout-ms 10000})
                                  (catch Throwable t t)))
            _ (Thread/sleep 20)
            expiring (future (try (batch/submit! c {:allowance 1024
                                                    :data :expiring
                                                    :timeout-ms 1})
                                  (catch Throwable t t)))]
        (Thread/sleep 100)
        (.countDown release)
        (is (= :leader (deref leader 5000 ::timeout)))
        (is (= :survivor (deref survivor 5000 ::timeout)))
        (let [v (deref expiring 5000 ::timeout)]
          (is (instance? Throwable v))
          (is (= :txlog/write-deadline-exceeded (:error (ex-data v))))))
      (finally (.countDown release)))))

(deftest charge-accumulates-and-rejects-over-allowance
  (let [observed (promise)
        executor (fn [batch]
                   (let [d (batch/batch-at batch 0)]
                     (deliver observed
                              (try
                                (let [a (batch/charge! d 100)
                                      b (batch/charge! d 200)
                                      rejected
                                      (try
                                        (batch/charge! d 100000)
                                        :no-throw
                                        (catch clojure.lang.ExceptionInfo e
                                          (:error (ex-data e))))]
                                  [:charged a b rejected (batch/charged d)])
                                (catch Throwable t [:outer t]))))
                   (echo-values batch))
        c (collector-with executor)]
    (is (= :a (batch/submit! c {:allowance 1024 :data :a})))
    (is (= [:charged 100 300 :txlog/pending-budget-exceeded 300]
           (deref observed 5000 ::timeout)))))

(deftest a-request-allowance-below-the-minimum-is-rejected
  (let [c (collector-with echo-values)
        thrown (try (batch/submit! c {:allowance 512 :data :small})
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
    (is (some? thrown))
    (is (= :txlog/pending-budget-exceeded (:error (ex-data thrown))))
    (is (false? (:retryable? (ex-data thrown))))
    (is (zero? (:requests (batch/usage c))))
    (is (zero? (:bytes (batch/usage c))))))

;; ---------------------------------------------------------------------------
;; Preparation-deadline observer

(deftest a-stalled-preparation-owner-is-fenced-by-a-waiter
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        c (collector-with
           (fn [batch]
             (when (zero? (.getAndIncrement first-batch))
               (.countDown entered)
               (.await release 5 TimeUnit/SECONDS))
             (echo-values batch)))
        ;; The owner's short deadline becomes the batch cutoff; its body then
        ;; stalls past it.
        owner (future (try (batch/submit! c {:allowance 1024 :data :owner
                                             :timeout-ms 60})
                           (catch Throwable t t)))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [waiter (future (try (batch/submit! c {:allowance 1024 :data :waiter
                                                  :timeout-ms 10000})
                                (catch Throwable t t)))
            v (deref waiter 5000 ::timeout)]
        (is (instance? Throwable v))
        (is (= :txlog/write-deadline-exceeded (:error (ex-data v)))))
      (testing "the observer fences serving but retains the live owner's resources"
        (is (not (batch/serving? c)))
        (is (pos? (:requests (batch/usage c)))))
      (finally
        (.countDown release)
        (let [v (deref owner 5000 ::timeout)]
          (is (instance? Throwable v))
          (testing "the late owner return cannot publish success"
            (is (= :txlog/write-deadline-exceeded (:error (ex-data v))))))))))

(deftest observe-cutoff-does-not-fence-after-a-batch-retires
  (let [c (collector-with echo-values)]
    (is (= :a (batch/submit! c {:allowance 1024 :data :a :timeout-ms 20})))
    ;; The batch has retired with an elapsed cutoff; the slot must be clear so a
    ;; successor is never fenced by the predecessor's deadline.
    (Thread/sleep 50)
    (is (false? (batch/observe-cutoff! c)))
    (is (batch/serving? c))
    (is (= :b (batch/submit! c {:allowance 1024 :data :b :timeout-ms 5000})))
    (is (= :c (batch/submit! c {:allowance 1024 :data :c})))))

(deftest a-live-batch-with-an-unexpired-cutoff-is-not-fenced
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        c (collector-with
           (fn [batch]
             (when (zero? (.getAndIncrement first-batch))
               (.countDown entered)
               (.await release 5 TimeUnit/SECONDS))
             (echo-values batch)))
        owner (future (batch/submit! c {:allowance 1024 :data :owner
                                        :timeout-ms 10000}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (is (false? (batch/observe-cutoff! c)))
      (is (batch/serving? c))
      (finally
        (.countDown release)
        (is (= :owner (deref owner 5000 ::timeout)))))))

(deftest an-unbounded-capacity-waiter-does-not-spuriously-expire
  (let [entered (CountDownLatch. 1)
        waiting (CountDownLatch. 1)
        release (CountDownLatch. 1)
        remove-observer (phase/observe!
                         (fn [event _]
                           (when (= :admission-wait event)
                             (.countDown waiting))))
        first-batch (AtomicInteger. 0)
        c (collector-with
           (fn [batch]
             (when (zero? (.getAndIncrement first-batch))
               (.countDown entered)
               (.await release 5 TimeUnit/SECONDS))
             (echo-values batch))
           {:wal-pending-max-requests 1
            :write-batch-size 1
            :write-batch-max-bytes 1024
            :wal-rmw-max-bytes 1024})
        leader (future (batch/submit! c {:allowance 1024 :data :leader}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [follower (future (try (batch/submit! c {:allowance 1024 :data :follower})
                                  (catch Throwable t t)))]
        (is (.await waiting 5 TimeUnit/SECONDS))
        ;; Wait past one bounded slice: a slice ending must not be an expiry.
        (Thread/sleep 200)
        (.countDown release)
        (is (= :leader (deref leader 5000 ::timeout)))
        (is (= :follower (deref follower 5000 ::timeout))))
      (finally
        (.countDown release)
        (remove-observer)))))
