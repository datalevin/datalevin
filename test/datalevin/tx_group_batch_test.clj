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
           [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong
            AtomicLongArray]
           [java.util.concurrent.locks Condition LockSupport ReentrantLock]))

(def ^:private default-overrides
  {:wal-pending-max-requests 64
   :wal-pending-max-bytes 1048576
   :write-batch-size 8
   :write-batch-max-bytes 12288
   :wal-rmw-max-bytes 4096})

(defn- collector-with
  ([executor] (collector-with executor nil))
  ([executor {:keys [preparation-timeout-ms] :as overrides}]
   (batch/create
    executor
    (cond-> {:limits (charge/resolve-limits
                      (merge default-overrides
                             (dissoc overrides :preparation-timeout-ms)))}
      preparation-timeout-ms
      (assoc :preparation-timeout-ms preparation-timeout-ms)))))

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

(defn- waiter-counts [^datalevin.tx_group.batch.Collector c]
  (let [^AtomicLongArray waiters (.waiters c)]
    [(.get waiters 0) (.get waiters 1)]))

(defn- progress-probe
  "Observe actual condition waits/signals without changing production code."
  [^datalevin.tx_group.batch.Collector c on-await]
  (let [^ReentrantLock lock (.lock c)
        ^Condition delegate (.newCondition lock)
        signals (atom [])
        progress (reify Condition
                   (await [_] (on-await c) (.await delegate))
                   (await [_ time unit] (on-await c) (.await delegate time unit))
                   (awaitNanos [_ ns] (on-await c) (.awaitNanos delegate ns))
                   (awaitUntil [_ date] (on-await c) (.awaitUntil delegate date))
                   (awaitUninterruptibly [_]
                     (on-await c) (.awaitUninterruptibly delegate))
                   (signal [_] (swap! signals conj :one) (.signal delegate))
                   (signalAll [_] (swap! signals conj :all) (.signalAll delegate)))]
    {:signals signals
     :collector (batch/->Collector
                 lock progress (.ready c) (.queued c) (.active c) (.serving c)
                 (.active-batch c) (.failure c) (.waiters c) (.budget c)
                 (.max-requests c) (.batch-limit c) (.batch-max-bytes c)
                 (.shared-reserved c) (.preparation-timeout-ms c)
                 (.collection-delay-nanos c) (.rmw-allowance c)
                 (.published c) (.next-id c) (.executor c)
                 (.check-prepared! c) (.on-failure! c))}))

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

(deftest a-batch-retains-charges-until-caller-delivery
  ;; The reservation covers storage the caller still retains until delivery.
  ;; :next-activation fires after the sealed batch's cleanup but before lead!
  ;; returns to the caller, so the charge must still be held there. Releasing at
  ;; cleanup alone would let another write proceed while an undelivered result
  ;; still held storage.
  (let [c (collector-with echo-values)
        at-next-activation (promise)
        remove-observer
        (phase/observe!
         (fn [event _]
           (when (= :next-activation event)
             (deliver at-next-activation (:requests (batch/usage c))))))]
    (try
      (is (= :a (batch/submit! c {:allowance 1024 :data :a})))
      (is (= 1 (deref at-next-activation 5000 ::timeout))
          "the batch retains the charge until the caller finishes delivery")
      (is (zero? (:requests (batch/usage c))))
      (is (zero? (:bytes (batch/usage c))))
      (finally
        (remove-observer)))))

(deftest a-selected-descriptor-cannot-be-released-by-its-caller
  ;; Regression: the caller fallback used to release a sealed member's
  ;; reservation. A timed-out follower could then drop accounted bytes while its
  ;; active batch still retained the storage. Once selected, only batch cleanup
  ;; may release the reservation.
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        captured (promise)
        executor (fn [batch]
                   (deliver captured (batch/batch-at batch 0))
                   (.countDown entered)
                   (.await release 5 TimeUnit/SECONDS)
                   (echo-values batch))
        c (collector-with executor)
        f (future (try (batch/submit! c {:allowance 2048 :data :a})
                       (catch Throwable t t)))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (is (= 2048 (:bytes (batch/usage c))))
      (let [descriptor (deref captured 1000 nil)]
        (is (some? descriptor))
        (testing "the caller fallback cannot release a sealed member"
          (#'batch/release-descriptor! c descriptor)
          (is (= 2048 (:bytes (batch/usage c)))
              "the batch still owns the retained storage")
          (is (= 1 (:requests (batch/usage c))))))
      (finally
        (.countDown release)))
    (is (= :a (deref f 5000 ::timeout)))
    (testing "batch cleanup releases it once both branches stop"
      (is (zero? (:bytes (batch/usage c))))
      (is (zero? (:requests (batch/usage c)))))))

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

(defn- admitted-descriptor
  "A prepared request for deterministically scheduling election/handoff races."
  ([c value] (admitted-descriptor c value 0))
  ([c value deadline]
   (admitted-descriptor c value deadline (Thread/currentThread)))
  ([c value deadline ready]
   (#'batch/admit! c 1024 deadline)
   (batch/->Descriptor nil (volatile! value) nil 1024 deadline
                       (AtomicLong. 1024) (AtomicBoolean. false)
                       (volatile! nil) ready
                       (AtomicBoolean. false) (AtomicBoolean. false)
                       (AtomicBoolean. false) (AtomicBoolean. false))))

(defn- parked-descriptor
  "Start a real waiter before publishing its descriptor; observe result or handoff."
  ([c value] (parked-descriptor c value (fn [d] (some? @(.result d)))))
  ([c value progressed?]
   (let [slot (volatile! nil)
         done (promise)
         thread (Thread.
                 (fn []
                   (let [d @slot]
                     (loop []
                       (cond
                         (progressed? d) (deliver done true)
                         (Thread/interrupted) (deliver done false)
                         :else (do (LockSupport/park d) (recur)))))))
         d (admitted-descriptor c value 0 thread)
         deadline (+ (System/nanoTime) 2000000000)]
     (vreset! slot d)
     (.start thread)
     (loop []
       (when (and (not (identical? d (LockSupport/getBlocker thread)))
                  (< (System/nanoTime) deadline))
         (Thread/sleep 1)
         (recur)))
     (is (identical? d (LockSupport/getBlocker thread)))
     {:descriptor d :thread thread :done done})))

(defn- stop-waiter! [{:keys [^Thread thread]}]
  (.interrupt thread)
  (.join thread 2000))

(defn- release-callers!
  "Simulate each synthetic caller's delivery finally, dropping the caller side of
  its reservation; the batch side drops when its sealed batch retires."
  [c & descriptors]
  (doseq [descriptor descriptors]
    (#'batch/release-descriptor! c descriptor)))

(deftest a-retired-batch-retains-the-charge-until-caller-delivery
  ;; Regression: batch cleanup refunded the whole reservation, so a caller paused
  ;; before consuming its delivered result held storage for free and another
  ;; write could pass the bound covering undelivered results.
  (let [c (collector-with echo-values)
        leader (admitted-descriptor c :leader)]
    (is (true? (#'batch/publish-and-elect! c leader)))
    (#'batch/lead! c leader)
    (is (= [true :leader] @(.result leader)))
    (testing "the batch has retired, but the caller has not delivered yet"
      (is (pos? (:requests (batch/usage c))))
      (is (pos? (:bytes (batch/usage c)))))
    (release-callers! c leader)
    (is (zero? (:requests (batch/usage c))))
    (is (zero? (:bytes (batch/usage c))))))

(deftest joined-callers-are-notified-after-publication
  (let [c (collector-with echo-values)
        leader (admitted-descriptor c :leader)
        probe (parked-descriptor c :follower)
        follower (:descriptor probe)]
    (try
      (is (true? (#'batch/publish-and-elect! c leader)))
      (is (false? (#'batch/publish-and-elect! c follower)))
      (#'batch/lead! c leader)
      (is (true? (deref (:done probe) 2000 ::timeout)))
      (is (= [true :leader] @(.result leader)))
      (is (= [true :follower] @(.result follower)))
      (release-callers! c leader follower)
      (is (zero? (:requests (batch/usage c))))
      (finally (stop-waiter! probe)))))

(deftest sealed-initialization-failure-rejects-every-member-and-retires
  (let [c (collector-with echo-values)
        leader (admitted-descriptor c :leader)
        probe (parked-descriptor c :follower)
        follower (:descriptor probe)
        failure (ex-info "seal observer failed" {})
        uninstall (phase/observe!
                   (fn [event _]
                     (when (= :batch-sealed event) (throw failure))))]
    (try
      (is (true? (#'batch/publish-and-elect! c leader)))
      (is (false? (#'batch/publish-and-elect! c follower)))
      (#'batch/lead! c leader)
      (is (= [false failure] @(.result leader)))
      (is (= [false failure] @(.result follower)))
      (is (true? (deref (:done probe) 2000 ::timeout)))
      (is (false? (batch/serving? c)))
      (is (batch/await-quiescence! c 100))
      (release-callers! c leader follower)
      (is (zero? (:requests (batch/usage c))))
      (finally (stop-waiter! probe) (uninstall)))))

(deftest partial-publication-does-not-strand-a-completed-follower
  ;; Inject an undersized value array so publication fails after two slots.
  (let [c (collector-with
           (fn [b] (#'batch/join-or-reject! b (object-array [:leader :completed]))))
        leader (admitted-descriptor c :leader)
        completed-probe (parked-descriptor c :completed)
        failed-probe (parked-descriptor c :failed)
        completed (:descriptor completed-probe)
        failed (:descriptor failed-probe)]
    (try
      (is (true? (#'batch/publish-and-elect! c leader)))
      (is (false? (#'batch/publish-and-elect! c completed)))
      (is (false? (#'batch/publish-and-elect! c failed)))
      (#'batch/lead! c leader)
      (is (= [true :completed] @(.result completed)))
      (is (true? (deref (:done completed-probe) 2000 ::timeout)))
      (is (false? (first @(.result failed))))
      (is (instance? ArrayIndexOutOfBoundsException (second @(.result failed))))
      (is (true? (deref (:done failed-probe) 2000 ::timeout)))
      (is (false? (batch/serving? c)))
      (is (batch/await-quiescence! c 100))
      (release-callers! c leader completed failed)
      (is (zero? (:requests (batch/usage c))))
      (finally (stop-waiter! completed-probe) (stop-waiter! failed-probe)))))

(deftest returning-and-queued-callers-cannot-take-the-waking-heads-turn
  ;; Freeze scheduling just after the previous leader releases the slot, before
  ;; the notified head runs. Later callers must queue, making one wider batch.
  (let [executed (atom [])
        c (collector-with (fn [b]
                            (swap! executed conj (vec (echo-values b)))
                            (echo-values b)))
        first (admitted-descriptor c :first)
        head (admitted-descriptor c :head)
        queued (admitted-descriptor c :queued)
        returning (admitted-descriptor c :returning)]
    (is (true? (#'batch/publish-and-elect! c first)))
    (let [selected (#'batch/claim-next-batch! c first)]
      (is (false? (#'batch/publish-and-elect! c head)))
      (is (false? (#'batch/publish-and-elect! c queued)))
      (#'batch/run-sealed-batch! c selected))
    (is (nil? (#'batch/try-lead! c queued))
        "an already queued follower cannot take the head's turn")
    (is (false? (#'batch/publish-and-elect! c returning))
        "a newly published request cannot take the head's turn")
    (let [elected? (#'batch/try-lead! c head)]
      (is (true? elected?))
      (when elected? (#'batch/lead! c head)))
    (is (= [[:first] [:head :queued :returning]] @executed))
    (is (= [[true :first] [true :head] [true :queued] [true :returning]]
           (mapv #(deref (.result ^datalevin.tx_group.batch.Descriptor %))
                 [first head queued returning])))
    (release-callers! c first head queued returning)
    (is (zero? (:requests (batch/usage c))))
    (is (batch/await-quiescence! c 100))))

(deftest an-expiring-head-relays-the-free-slot-to-the-next-caller
  (let [executed (atom [])
        c (collector-with (fn [b]
                            (swap! executed conj (vec (echo-values b)))
                            (echo-values b)))
        first (admitted-descriptor c :first)
        head (admitted-descriptor c :expired 1)]
    (is (true? (#'batch/publish-and-elect! c first)))
    (let [selected (#'batch/claim-next-batch! c first)
          probe (parked-descriptor
                 c :tail #(and (not (.get (.active c)))
                               (identical? % (.peek (.ready c)))))
          tail (:descriptor probe)]
      (try
        (is (false? (#'batch/publish-and-elect! c head)))
        (is (false? (#'batch/publish-and-elect! c tail)))
        (#'batch/run-sealed-batch! c selected)
        ;; Expiry can race handoff before the head claims leadership.
        (#'batch/await-progress! c head (.result head) (volatile! false) 1)
        (#'batch/release-descriptor! c head)
        (is (= :txlog/write-deadline-exceeded
               (:error (ex-data (second @(.result head))))))
        (is (true? (deref (:done probe) 2000 ::timeout))
            "removing the head wakes its successor without another submission")
        (let [elected? (#'batch/try-lead! c tail)]
          (is (true? elected?))
          (when elected? (#'batch/lead! c tail)))
        (is (= [[:first] [:tail]] @executed))
        (is (= [true :tail] @(.result tail)))
        (release-callers! c first tail)
        (is (zero? (:bytes (batch/usage c))))
        (is (batch/await-quiescence! c 100))
        (finally (stop-waiter! probe))))))

(deftest publication-and-retirement-share-one-coordination-pass
  (let [record? (volatile! false)
        unlocks (atom 0)
        lock (proxy [ReentrantLock] [true]
               (unlock []
                 (when @record? (swap! unlocks inc))
                 (proxy-super unlock)))
        original (collector-with echo-values)
        c (batch/->Collector
            lock (.newCondition ^ReentrantLock lock)
            (.ready original) (.queued original) (.active original) (.serving original)
            (.active-batch original) (.failure original) (.waiters original) (.budget original)
            (.max-requests original) (.batch-limit original) (.batch-max-bytes original)
            (.shared-reserved original) (.preparation-timeout-ms original)
            (.collection-delay-nanos original) (.rmw-allowance original)
            (.published original) (.next-id original) (.executor original)
            (.check-prepared! original) (.on-failure! original))
        retired (atom 0)
        off (phase/observe!
              (fn [event _]
                (case event
                  :joint-publication (vreset! record? true)
                  :batch-retired (do (is (zero? @unlocks)
                                        "publication must retain coordination until retirement")
                                     (swap! retired inc))
                  nil)))]
    (try
      (is (= :completed (batch/submit! c {:allowance 1024 :data :completed})))
      (is (= 1 @retired))
      (is (= 1 @unlocks))
      (is (zero? (:requests (batch/usage c))))
      (finally (off)))))

(deftest retirement-failure-fences-without-revoking-published-results
  (let [boom (ex-info "retirement failed" {})
        c (collector-with (fn [b]
                            (batch/set-lsn! b 1)
                            (batch/record-wal-policy! b true)
                            (echo-values b)))
        uninstall (phase/observe!
                   (fn [event _]
                     (when (= :batch-retired event) (throw boom))))]
    (try
      (is (= :committed (batch/submit! c {:allowance 1024 :data :committed})))
      (is (= 1 (batch/published-lsn c)))
      (is (false? (batch/serving? c)))
      (is (batch/await-quiescence! c 100))
      (is (zero? (:requests (batch/usage c))))
      (let [error (try (batch/submit! c {:allowance 1024 :data :later})
                       (catch Throwable t t))]
        (is (= :not-committed (:outcome (ex-data error))))
        (is (= :txlog/runtime-fenced (:error (ex-data error))))
        (is (nil? (:txlog-lsn (ex-data error))))
        (is (identical? boom (ex-cause (ex-cause error)))))
      (finally (uninstall)))))

(deftest queued-and-future-requests-do-not-inherit-a-committed-batch-outcome
  (let [entered (CountDownLatch. 1)
        queued (CountDownLatch. 1)
        release (CountDownLatch. 1)
        boom (ex-info "native completion failed" {})
        c (collector-with
           (fn [b]
             (batch/set-lsn! b 77)
             (batch/record-wal-policy! b true)
             (.countDown entered)
             (.await release 5 TimeUnit/SECONDS)
             (throw boom)))
        uninstall (phase/observe!
                   (fn [event d]
                     (when (and (= :ready-published event)
                                (= :queued (batch/data d)))
                       (.countDown queued))))
        submit (fn [value]
                 (try (batch/submit! c {:allowance 1024 :data value})
                      (catch Throwable t t)))
        active (future (submit :active))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [unstarted (future (submit :queued))]
        (is (.await queued 5 TimeUnit/SECONDS))
        (.countDown release)
        (let [error (deref active 5000 ::timeout)]
          (is (= :committed (:outcome (ex-data error))))
          (is (= 77 (:txlog-lsn (ex-data error))))
          (is (identical? boom (ex-cause error))))
        (doseq [error [(deref unstarted 5000 ::timeout) (submit :future)]]
          (is (= :not-committed (:outcome (ex-data error))))
          (is (= :txlog/runtime-fenced (:error (ex-data error))))
          (is (nil? (:txlog-lsn (ex-data error))))
          (is (identical? boom (ex-cause (ex-cause error)))))
        (is (batch/await-quiescence! c 100))
        (is (zero? (:requests (batch/usage c)))))
      (finally (.countDown release) (uninstall)))))

(deftest selection-failure-releases-the-elected-slot
  (let [boom (ex-info "selection observer failed" {})
        c (collector-with echo-values)
        uninstall (phase/observe!
                   (fn [event _]
                     (when (= :selection-start event) (throw boom))))]
    (try
      (is (identical? boom (try (batch/submit! c {:allowance 1024 :data :first})
                               (catch Throwable t t))))
      (is (false? (batch/serving? c)))
      (is (batch/await-quiescence! c 100))
      (is (zero? (:requests (batch/usage c))))
      (let [later (future (try (batch/submit! c {:allowance 1024 :data :later})
                               (catch Throwable t t)))
            error (deref later 1000 ::timeout)]
        (is (= :txlog/runtime-fenced (:error (ex-data error))))
        (is (= :not-committed (:outcome (ex-data error)))))
      (finally (uninstall)))))

(deftest admitted-observer-failure-refunds-fast-and-waiting-reservations
  (doseq [waiting? [false true]]
    (let [c (collector-with echo-values {:wal-pending-max-requests 1
                                         :write-batch-size 1})
          parked (CountDownLatch. 1)
          boom (ex-info "admitted observer failed" {})]
      (when waiting? (#'batch/admit! c 1024 0))
      (let [uninstall (phase/observe!
                       (fn [event _]
                         (case event
                           :admitted (throw boom)
                           :admission-wait (.countDown parked)
                           nil)))
            submit (future (try (batch/submit! c {:allowance 1024 :data :value})
                                (catch Throwable t t)))]
        (try
          (when waiting?
            (is (.await parked 5 TimeUnit/SECONDS))
            (#'batch/release-allowance! c 1024))
          (is (identical? boom (deref submit 5000 ::timeout)))
          (is (zero? (:requests (batch/usage c))))
          (is (zero? (:bytes (batch/usage c))))
          (is (= [0 0] (waiter-counts c)))
          (is (= :next (do (uninstall)
                          (batch/submit! c {:allowance 1024 :data :next}))))
          (finally (uninstall)))))))

(deftest a-completed-leader-returns-while-the-successor-batch-is-blocked
  (doseq [timeout-ms [nil 10000]]
    (testing (str "successor timeout: " timeout-ms)
      (let [first-entered (CountDownLatch. 1)
            first-release (CountDownLatch. 1)
            queued (CountDownLatch. 1)
            second-entered (CountDownLatch. 1)
            second-release (CountDownLatch. 1)
            owners (ConcurrentLinkedQueue.)
            remove-observer
            (phase/observe!
             (fn [event descriptor]
               (when (and (= :ready-published event)
                          (= :second (batch/data descriptor)))
                 (.countDown queued))))
            c (collector-with
               (fn [b]
                 (.add owners (Thread/currentThread))
                 (if (= :first (batch/data (batch/batch-at b 0)))
                   (do (.countDown first-entered)
                       (.await first-release 5 TimeUnit/SECONDS))
                   (do (.countDown second-entered)
                       (.await second-release 5 TimeUnit/SECONDS)))
                 (echo-values b)))
            leader (future (batch/submit! c {:allowance 1024 :data :first}))]
        (try
          (is (.await first-entered 5 TimeUnit/SECONDS))
          (let [follower (future (batch/submit!
                                 c {:allowance 1024 :data :second
                                    :timeout-ms timeout-ms}))]
            (is (.await queued 5 TimeUnit/SECONDS))
            (.countDown first-release)
            (is (.await second-entered 5 TimeUnit/SECONDS)
                "queued work starts without another submission")
            (is (= :first (deref leader 1000 ::timeout))
                "later I/O cannot delay the completed caller")
            (is (not (realized? follower)))
            (is (= 2 (count (set owners)))
                "a successor owns the second batch")
            (.countDown second-release)
            (is (= :second (deref follower 5000 ::timeout)))
            (is (batch/await-quiescence! c 1000)))
          (finally
            (.countDown first-release)
            (.countDown second-release)
            (remove-observer)))))))

(deftest an-arrival-racing-idle-handoff-is-not-stranded
  (let [idle (CountDownLatch. 1)
        release-idle (CountDownLatch. 1)
        prepared (CountDownLatch. 1)
        second-entered (CountDownLatch. 1)
        second-release (CountDownLatch. 1)
        activations (AtomicInteger. 0)
        remove-observer
        (phase/observe!
         (fn [event _]
           (when (and (= :next-activation event)
                      (zero? (.getAndIncrement activations)))
             (.countDown idle)
             (.await release-idle 5 TimeUnit/SECONDS))))
        c (collector-with
           (fn [b]
             (when (= :second (batch/data (batch/batch-at b 0)))
               (.countDown second-entered)
               (.await second-release 5 TimeUnit/SECONDS))
             (echo-values b)))
        leader (future (batch/submit! c {:allowance 1024 :data :first}))]
    (try
      (is (.await idle 5 TimeUnit/SECONDS))
      (let [follower (future (batch/submit!
                             c {:allowance 1024 :data :second
                                :prepare (fn [_] (.countDown prepared) :second)}))]
        (is (.await prepared 5 TimeUnit/SECONDS))
        (.countDown release-idle)
        (is (.await second-entered 5 TimeUnit/SECONDS))
        (is (= :first (deref leader 1000 ::timeout)))
        (.countDown second-release)
        (is (= :second (deref follower 5000 ::timeout)))
        (is (batch/await-quiescence! c 1000)))
      (finally
        (.countDown release-idle)
        (.countDown second-release)
        (remove-observer)))))

(deftest an-interrupted-waiter-can-take-over-without-cancelling-its-batch
  (doseq [timeout-ms [nil 10000]]
    (let [entered (CountDownLatch. 1)
          release (CountDownLatch. 1)
          queued (CountDownLatch. 1)
          answer (promise)
          body-interrupted? (promise)
          remove-observer
          (phase/observe!
           (fn [event descriptor]
             (when (and (= :ready-published event)
                        (= :second (batch/data descriptor)))
               ;; Interrupt as it publishes, before it waits or can take over.
               (.interrupt (Thread/currentThread))
               (.countDown queued))))
          c (collector-with
             (fn [b]
               (if (= :first (batch/data (batch/batch-at b 0)))
                 (do (.countDown entered)
                     (.await release 5 TimeUnit/SECONDS))
                 (deliver body-interrupted?
                          (.isInterrupted (Thread/currentThread))))
               (echo-values b)))
          leader (future (batch/submit! c {:allowance 1024 :data :first}))]
      (try
        (is (.await entered 5 TimeUnit/SECONDS))
        (let [follower (Thread.
                        #(deliver answer
                                  (try
                                    (let [value (batch/submit!
                                                 c {:allowance 1024 :data :second
                                                    :timeout-ms timeout-ms})]
                                      [value (.isInterrupted (Thread/currentThread))])
                                    (catch Throwable t t))))]
          (.start follower)
          (is (.await queued 5 TimeUnit/SECONDS))
          (.countDown release)
          (is (= :first (deref leader 5000 ::timeout)))
          (is (= [:second true] (deref answer 5000 ::timeout)))
          (is (false? (deref body-interrupted? 1000 ::timeout))
              "waiting interruption is deferred until caller return")
          (.join follower 1000)
          (is (not (.isAlive follower))))
        (finally
          (.countDown release)
          (remove-observer))))))

(deftest later-arrivals-accumulate-unsealed-through-join
  (let [sizes (ConcurrentLinkedQueue.)
        schedules (ConcurrentLinkedQueue.)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        executor (fn [batch]
                   (.add sizes (.size (.descriptors batch)))
                   (.add schedules (batch/batch-schedule batch))
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
      (let [observed (vec sizes)]
        (testing "the blocked leader is sealed on its own"
          (is (= 1 (first observed))))
        (testing "no later batch exceeds the two requests the byte cap allows"
          (is (every? #(<= (long %) 2) (rest observed))))
        (testing "every follower is served exactly once"
          (is (= 3 (reduce + (rest observed)))))
        (testing "so the cap, not the head, decided the split"
          ;; Whether the three followers seal as 2+1 or 1+2 depends on when each
          ;; reaches the queue head. Either way the two oversized followers stay
          ;; in publication order: no batch skips an earlier oversized request for
          ;; a later smaller one, which would show up as a leading 1+1.
          (is (not= [1 1 1] (vec (rest observed)))))))))

(deftest batch-selection-never-exceeds-the-byte-cap-with-heterogeneous-allowances
  ;; A small head followed by a large follower exposed the old peek-loop, which
  ;; accumulated the head's allowance repeatedly and sealed past the byte cap.
  (let [batches (ConcurrentLinkedQueue.)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        executor (fn [batch]
                   (let [n (batch/batch-count batch)
                         total (reduce + (map #(batch/allowance
                                                (batch/batch-at batch %))
                                              (range n)))]
                     (.add batches [n total]))
                   (when (zero? (.getAndIncrement first-batch))
                     (.countDown entered)
                     (.await release 5 TimeUnit/SECONDS))
                   (echo-values batch))
        c (collector-with executor {:write-batch-max-bytes 10000})
        leader (future (batch/submit! c {:allowance 1024 :data :first}))]
    (is (.await entered 5 TimeUnit/SECONDS))
    (let [followers (mapv (fn [[i a]]
                            (future (batch/submit! c {:allowance a :data i})))
                          (map vector (range 5) [1024 9000 1024 1024 1024]))]
      (.countDown release)
      (is (= :first (await! leader)))
      (doseq [f followers] (is (not= (timeout) (deref f 5000 ::timeout))))
      (let [observed (vec batches)]
        (testing "no sealed batch's full allowances exceed the byte cap"
          (is (every? (fn [[_ total]] (<= (long total) 10000)) observed)))
        (testing "the large follower is not absorbed behind a small head"
          (is (some (fn [[_ total]] (> (long total) 1024)) observed)))
        (testing "every admitted request is sealed exactly once"
          (is (= 6 (reduce + (map first observed)))))))))

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
;; Unbatched preparation failure

(deftest caller-preparation-failure-rejects-only-that-request
  (let [c (collector-with echo-values)
        thrown (try (batch/submit! c {:allowance 1024
                                      :prepare (fn [_]
                                                 (throw (ex-info "encode failed"
                                                                 {:row 2})))})
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
    (is (= "encode failed" (ex-message thrown)))
    (testing "no body ran and the reservation was released"
      (is (batch/serving? c))
      (is (zero? (:requests (batch/usage c))))
      (is (= :ok (batch/submit! c {:allowance 1024 :data :ok}))))))

(deftest an-interrupt-after-preparation-rejects-only-its-request
  ;; Regression: an interrupted preparer published and led, so the WAL channel's
  ;; interruptible I/O threw ClosedByInterruptException and fenced the runtime
  ;; instead of rejecting one request.
  (let [executed (atom 0)
        c (collector-with (fn [b] (swap! executed inc) (echo-values b)))
        thrown (try
                 (batch/submit! c {:allowance 1024 :data :a
                                   :prepare (fn [_]
                                              (.interrupt (Thread/currentThread))
                                              :a)})
                 nil
                 (catch Throwable t t))]
    (try
      (is (instance? Throwable thrown))
      (is (= :txlog/write-interrupted (:error (ex-data thrown))))
      (is (= :not-committed (:outcome (ex-data thrown))))
      (is (zero? @executed) "an interrupted preparer never dispatches a batch")
      (is (batch/serving? c) "the interrupted request does not fence the runtime")
      (is (zero? (:requests (batch/usage c))))
      (is (zero? (:bytes (batch/usage c))))
      (finally
        ;; Clear the test thread's interrupt status for later tests.
        (Thread/interrupted)))))

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
          (is (= :txlog/runtime-fenced (:error (ex-data t))))
          (is (= :not-committed (:outcome (ex-data t))))
          (is (identical? boom (ex-cause t)))))
      (testing "the terminal fence closes admission permanently"
        (is (not (batch/serving? c)))
        (let [late (try (batch/submit! c {:allowance 1024 :data :late})
                        nil
                        (catch Throwable t t))]
          (is (some? late))
          (is (= :txlog/runtime-fenced (:error (ex-data late))))
          (is (= :not-committed (:outcome (ex-data late))))
          (is (identical? boom (ex-cause late)))))
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
                                         :prepare (fn [_] :first)
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

(deftest an-idle-refund-does-not-wait-for-collector-coordination
  (let [c (collector-with echo-values)
        ^ReentrantLock lock (.lock c)]
    (#'batch/admit! c 1024 0)
    (.lock lock)
    (try
      ;; A publisher/selector may hold coordination while another caller drops
      ;; its final ownership. With no capacity waiter, that refund can complete
      ;; immediately rather than join the publication lock queue.
      (let [refund (future (#'batch/release-allowance! c 1024) :released)]
        (is (= :released (deref refund 5000 ::timeout)))
        (is (zero? (:requests (batch/usage c))))
        (is (zero? (:bytes (batch/usage c)))))
      (finally (.unlock lock)))))

(deftest batches-with-no-progress-waiters-do-not-signal
  (let [{c :collector signals :signals}
        (progress-probe (collector-with echo-values) (constantly nil))]
    (is (= :plain (batch/submit! c {:allowance 1024 :data :plain})))
    (is (= :bounded (batch/submit! c {:allowance 1024 :data :bounded
                                     :timeout-ms 5000})))
    (is (batch/await-quiescence! c 100))
    (batch/close! c)
    (batch/fence! c (ex-info "closed" {}))
    (is (empty? @signals)
        "publication, retirement and shutdown skip an empty condition")))

(deftest retirement-notifies-capacity-and-quiescence-waiters
  (doseq [[capacity quiescence :as expected] [[1 0] [0 1] [1 1]]]
    (testing (str "registered waiters " expected)
      (let [entered (CountDownLatch. 1)
            waiting (CountDownLatch. 1)
            release (CountDownLatch. 1)
            first-batch (AtomicBoolean. true)
            {c :collector signals :signals}
            (progress-probe
             (collector-with
              (fn [b]
                (when (.compareAndSet first-batch true false)
                  (.countDown entered)
                  (.await release 5 TimeUnit/SECONDS))
                (echo-values b))
              {:wal-pending-max-requests 1 :write-batch-size 1
               :write-batch-max-bytes 1024 :wal-rmw-max-bytes 1024})
             (fn [c]
               (when (= expected (waiter-counts c))
                 (.countDown waiting))))
            leader (future (batch/submit! c {:allowance 1024 :data :leader}))]
        (try
          (is (.await entered 5 TimeUnit/SECONDS))
          (let [closer (when (pos? quiescence)
                         (future (batch/await-quiescence! c 5000)))
                follower (when (pos? capacity)
                           (future (batch/submit! c {:allowance 1024 :data :follower
                                                    :timeout-ms 5000})))]
            (is (.await waiting 5 TimeUnit/SECONDS))
            (.countDown release)
            (is (= :leader (deref leader 5000 ::timeout)))
            (when follower (is (= :follower (deref follower 5000 ::timeout))))
            (when closer (is (true? (deref closer 5000 false))))
            (is (= (if (= expected [1 1]) :all :one) (first @signals)))
            (is (= [0 0] (waiter-counts c))))
          (finally (.countDown release)))))))

(deftest progress-waiter-registration-is-unwound-on-failure
  (let [c (collector-with echo-values {:wal-pending-max-requests 1
                                      :write-batch-size 1})
        boom (ex-info "admission observer failed" {})]
    (#'batch/admit! c 1024 0)
    (let [uninstall (phase/observe!
                     (fn [event _] (when (= :admission-wait event) (throw boom))))]
      (try
        (is (identical? boom (try (#'batch/admit! c 1024 0)
                                 (catch Throwable t t))))
        (is (= [0 0] (waiter-counts c)))
        (finally (uninstall) (#'batch/release-allowance! c 1024))))
    (.set ^AtomicBoolean (.active c) true)
    (try
      (is (false? (batch/await-quiescence! c 0)))
      (is (= [0 0] (waiter-counts c)))
      (let [interrupted (future
                          (.interrupt (Thread/currentThread))
                          (try (batch/await-quiescence! c 5000)
                               (catch InterruptedException _ :interrupted)))]
        (is (= :interrupted (deref interrupted 5000 ::timeout)))
        (is (= [0 0] (waiter-counts c))))
      (finally (.set ^AtomicBoolean (.active c) false)))))

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

(deftest preparation-timeout-default-is-wired
  (is (= 30000 (.preparation-timeout-ms (collector-with echo-values)))))

(deftest preparation-timeout-bounds-submission-and-an-explicit-timeout-tightens-it
  (let [c (collector-with echo-values {:preparation-timeout-ms 1234})
        captured (promise)
        before (System/nanoTime)]
    (is (= :a (batch/submit! c {:allowance 1024 :data :a
                                :prepare (fn [d] (deliver captured d) :a)})))
    (let [descriptor ^datalevin.tx_group.batch.Descriptor (deref captured 1000 nil)]
      (is (some? descriptor))
      (is (<= 1234.0 (/ (- (long (.deadline-nanos descriptor)) before) 1e6) 2000.0)
          "a request with no :timeout-ms is bounded by the preparation timeout"))
    (let [captured2 (promise)
          before2 (System/nanoTime)]
      (is (= :b (batch/submit! c {:allowance 1024 :data :b :timeout-ms 5
                                  :prepare (fn [d] (deliver captured2 d) :b)})))
      (let [descriptor ^datalevin.tx_group.batch.Descriptor (deref captured2 1000 nil)]
        (is (some? descriptor))
        (is (<= 5.0 (/ (- (long (.deadline-nanos descriptor)) before2) 1e6) 1000.0)
            "a shorter explicit request timeout is honoured")))))

(deftest a-queued-request-without-a-timeout-is-bounded-by-preparation
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        c (collector-with
           (fn [batch]
             ;; The real environment executor marks dispatch before its branches;
             ;; once dispatched, the preparation cutoff no longer fences.
             (batch/mark-dispatched! batch)
             (when (zero? (.getAndIncrement first-batch))
               (.countDown entered)
               (.await release 5 TimeUnit/SECONDS))
             (echo-values batch))
           {:preparation-timeout-ms 50})
        leader (future (batch/submit! c {:allowance 1024 :data :leader}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [late (future (try (batch/submit! c {:allowance 1024 :data :late})
                              (catch Throwable t t)))]
        (Thread/sleep 250)
        (.countDown release)
        (is (= :leader (deref leader 5000 ::timeout)))
        (let [v (deref late 5000 ::timeout)]
          (is (instance? Throwable v))
          (is (= :txlog/write-deadline-exceeded (:error (ex-data v)))
              "the preparation timeout bounds a request with no :timeout-ms"))
        (testing "the bounded request does not fence the runtime"
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
    (is (= :a (batch/submit! c {:allowance 2048 :op identity :data :a})))
    (is (= [:charged 1124 1324 :txlog/pending-budget-exceeded 1324]
           (deref observed 5000 ::timeout)))))

(deftest blind-preparation-is-admitted-precharged-and-published-once
  (let [c (collector-with echo-values)
        calls (atom 0)
        allowance (charge/blind-allowance {:declared-bytes 16})]
    (is (= :prepared
           (batch/submit!
            c {:allowance allowance :data :borrowed
               :prepare (fn [descriptor]
                          (swap! calls inc)
                          (is (= allowance (batch/charged descriptor)))
                          (is (= allowance (:bytes (batch/usage c))))
                          (is (not (batch/selected? descriptor)))
                          :prepared)})))
    (is (= 1 @calls))
    (is (zero? (:bytes (batch/usage c))))))

(deftest blind-growth-rejects-before-allocation-and-releases-admission
  (let [executed (atom 0)
        allocated (atom false)
        c (collector-with (fn [b] (swap! executed inc) (echo-values b)))
        allowance (charge/blind-allowance {:declared-bytes 16})
        error (try
                (batch/submit!
                 c {:allowance allowance
                    :prepare (fn [d]
                               (batch/charge! d (charge/array-bytes 1 32))
                               (reset! allocated true)
                               (byte-array 32))})
                nil
                (catch clojure.lang.ExceptionInfo e (ex-data e)))]
    (is (= :txlog/pending-budget-exceeded (:error error)))
    (is (= :not-committed (:outcome error)))
    (is (= allowance (:charged error)))
    (is (false? @allocated))
    (is (zero? @executed))
    (is (zero? (:bytes (batch/usage c))))
    (is (zero? (:requests (batch/usage c))))
    (is (= :after (batch/submit! c {:allowance 1024 :data :after})))))

(deftest disabled-traces-do-not-evaluate-payloads
  (is (not (phase/observed?)))
  (phase/phase! :unused (throw (ex-info "disabled payload evaluated" {}))))

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
        (is (= :txlog/runtime-fenced (:error (ex-data v))))
        (is (= :not-committed (:outcome (ex-data v))))
        (is (= :txlog/write-deadline-exceeded (:error (ex-data (ex-cause v))))))
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

(deftest a-withdrawn-preparation-cutoff-does-not-expire-a-capacity-waiter
  (let [entered (CountDownLatch. 1)
        parked (CountDownLatch. 1)
        dispatch (CountDownLatch. 1)
        dispatched (CountDownLatch. 1)
        finish (CountDownLatch. 1)
        c0 (collector-with
            (fn [b]
              (when (= :owner (batch/data (batch/batch-at b 0)))
                (.countDown entered)
                (.await dispatch 5 TimeUnit/SECONDS)
                (batch/mark-dispatched! b)
                (.countDown dispatched)
                (.await finish 5 TimeUnit/SECONDS))
              (echo-values b))
            {:wal-pending-max-requests 1 :write-batch-size 1
             :write-batch-max-bytes 1024 :wal-rmw-max-bytes 1024})
        {:keys [collector]} (progress-probe c0 (fn [_] (.countDown parked)))
        owner (future (batch/submit! collector
                                    {:allowance 1024 :data :owner :timeout-ms 500}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [follower (future
                       (try (batch/submit! collector {:allowance 1024 :data :follower})
                            (catch Throwable t t)))]
        (is (.await parked 5 TimeUnit/SECONDS))
        ;; The condition callback runs under the lock. Cross coordination here
        ;; so dispatch cannot race ahead of the wait's sampled live cutoff.
        (let [^ReentrantLock lock (.lock collector)]
          (.lock lock)
          (try (.countDown dispatch) (finally (.unlock lock))))
        (is (.await dispatched 5 TimeUnit/SECONDS))
        (is (= ::waiting (deref follower 650 ::waiting)))
        (is (batch/serving? collector))
        (.countDown finish)
        (is (= :owner (deref owner 5000 ::timeout)))
        (is (= :follower (deref follower 5000 ::timeout)))
        (is (= [0 0] (waiter-counts collector))))
      (finally (.countDown dispatch) (.countDown finish)))))

(deftest a-dispatched-batch-is-not-fenced-by-its-preparation-cutoff
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        first-batch (AtomicInteger. 0)
        c (collector-with
           (fn [batch]
             ;; The executor finishes ordered preparation and forks here.
             (batch/mark-dispatched! batch)
             (when (zero? (.getAndIncrement first-batch))
               (.countDown entered)
               (.await release 5 TimeUnit/SECONDS))
             (echo-values batch)))
        owner (future (batch/submit! c {:allowance 1024 :data :owner
                                        :timeout-ms 60}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (Thread/sleep 100)
      (testing "branch execution is owned by WAL/native deadlines, not the cutoff"
        (is (false? (batch/observe-cutoff! c)))
        (is (batch/serving? c)))
      (finally
        (.countDown release)
        (is (= :owner (deref owner 5000 ::timeout)))))))

(deftest collection-during-application-preserves-caps-fifo-and-deadlines
  (doseq [[limits expected]
          [[{:write-batch-size 2 :write-batch-max-bytes 4096 :wal-rmw-max-bytes 1024}
            [[:first :big] [:small]]]
           [{:write-batch-size 8 :write-batch-max-bytes 2500 :wal-rmw-max-bytes 1024}
            [[:first] [:big] [:small]]]]]
    (let [entered (CountDownLatch. 1) release (CountDownLatch. 1)
          published [(CountDownLatch. 1) (CountDownLatch. 1)]
          first? (AtomicBoolean. true) batches (atom [])
          executor (fn [b]
                     (let [initial-cutoff (batch/batch-cutoff b)]
                       (when (.compareAndSet first? true false)
                         (.countDown entered)
                         (.await release 5 TimeUnit/SECONDS))
                       (let [added (batch/collect-ready! b)]
                         (when (pos? added)
                           (is (< (batch/batch-cutoff b) initial-cutoff))))
                       (swap! batches conj
                              (mapv #(batch/data (batch/batch-at b %))
                                    (range (batch/batch-count b))))
                       (batch/begin-dispatch! b)
                       (is (thrown? IllegalStateException (batch/collect-ready! b)))
                       (echo-values b)))
          c (collector-with executor limits)
          uninstall (phase/observe!
                     (fn [event d]
                       (when (= event :ready-published)
                         (when-let [idx (:idx (batch/context d))]
                           (.countDown ^CountDownLatch (published idx))))))]
      (try
        (let [leader (future (batch/submit! c {:allowance 1024 :data :first}))]
          (is (.await entered 5 TimeUnit/SECONDS))
          (let [followers
                (mapv (fn [idx value allowance]
                        (let [f (future (batch/submit! c {:allowance allowance :data value
                                                         :timeout-ms 10000 :context {:idx idx}}))]
                          (is (.await ^CountDownLatch (published idx) 5 TimeUnit/SECONDS))
                          f))
                      [0 1] [:big :small] [2048 1024])]
            (.countDown release)
            (is (= :first (await! leader)))
            (is (= [:big :small] (mapv await! followers)))
            (is (= expected @batches))
            (is (batch/serving? c))
            (is (zero? (:requests (batch/usage c))))))
        (finally (.countDown release) (uninstall))))))

(deftest repeated-collection-keeps-the-byte-cap-and-expires-beyond-it
  (let [c (collector-with echo-values {:write-batch-max-bytes 3072
                                       :wal-rmw-max-bytes 1024})
        owner (admitted-descriptor c :owner)
        a (admitted-descriptor c :a)
        b (admitted-descriptor c :b)
        blocked (admitted-descriptor c :blocked)
        expired (admitted-descriptor c :expired 1)]
    (is (true? (#'batch/publish-and-elect! c owner)))
    (let [selected (#'batch/claim-next-batch! c owner)]
      (#'batch/publish-and-elect! c a)
      (is (= 1 (batch/collect-ready! selected)))
      (#'batch/publish-and-elect! c b)
      (#'batch/publish-and-elect! c blocked)
      (#'batch/publish-and-elect! c expired)
      (is (= 1 (batch/collect-ready! selected)))
      (is (= 0 (batch/collect-ready! selected)))
      (is (= [:owner :a :b] (vec (echo-values selected))))
      (is (false? (batch/selected? blocked)))
      (is (= :txlog/write-deadline-exceeded
             (:error (ex-data (second @(.result expired))))))
      (#'batch/run-sealed-batch! c selected))
    (is (true? (#'batch/try-lead! c blocked)))
    (#'batch/lead! c blocked)
    (is (= [true :blocked] @(.result blocked)))
    (release-callers! c owner a b blocked expired)
    (is (zero? (:requests (batch/usage c))))
    (is (zero? (:bytes (batch/usage c))))))

(deftest wal-carrier-is-built-after-collection-and-compacts-no-ops-in-order
  (let [c (collector-with echo-values)
        owner (admitted-descriptor c {:wal-body :first})
        noop (admitted-descriptor c {:wal-body nil})
        tail (admitted-descriptor c {:wal-body :last})]
    (#'batch/publish-and-elect! c owner)
    (let [^datalevin.tx_group.batch.Batch selected (#'batch/claim-next-batch! c owner)]
      (is (nil? (.walBodies selected)))
      (#'batch/publish-and-elect! c noop)
      (#'batch/publish-and-elect! c tail)
      (is (= 2 (batch/collect-ready! selected)))
      (is (nil? (.walBodies selected)) "collection allocates no provisional body array")
      (batch/set-accepted-count! selected 2)
      (batch/refresh-wal-bodies! selected)
      (let [bodies (batch/wal-bodies selected)]
        (is (= [:first :last] (vec bodies)))
        (batch/refresh-wal-bodies! selected)
        (is (identical? bodies (batch/wal-bodies selected))))
      (#'batch/run-sealed-batch! c selected))
    (release-callers! c owner noop tail)
    (is (zero? (:bytes (batch/usage c))))))

(deftest collection-failure-releases-members-transferred-before-expiry-failed
  (let [c (collector-with (fn [b] (batch/collect-ready! b) (echo-values b)))
        owner (admitted-descriptor c :owner)
        added (admitted-descriptor c :added)
        expired (admitted-descriptor c :expired 1)
        tail (admitted-descriptor c :tail)
        failure (ex-info "expiry observer failed" {})]
    (#'batch/publish-and-elect! c owner)
    (let [selected (#'batch/claim-next-batch! c owner)
          uninstall (phase/observe!
                     (fn [event _]
                       (when (= :queue-expired event) (throw failure))))]
      (try
        (doseq [d [added expired tail]] (#'batch/publish-and-elect! c d))
        (#'batch/run-sealed-batch! c selected)
        (is (= [false failure] @(.result owner)))
        (is (= [false failure] @(.result added)))
        (is (batch/selected? added))
        (is (false? (batch/selected? tail)))
        (is (every? #(some? @(.result ^datalevin.tx_group.batch.Descriptor %))
                    [owner added expired tail]))
        (release-callers! c owner added expired tail)
        (is (zero? (:requests (batch/usage c))))
        (is (zero? (:bytes (batch/usage c))))
        (is (batch/await-quiescence! c 100))
        (finally (uninstall))))))
