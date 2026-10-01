;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch
  "Additive collector for the new write protocol.

  This namespace implements Invariants 1-6 of the design contract. It is not a
  replacement for `datalevin.tx-group` and never calls it: compatibility keeps
  its own collector, and one environment selects exactly one of them (see
  `datalevin.tx-group.batch.env`).

  The model is deliberately different from the compatibility collector:

  - callers do their own state-independent preparation, then publish one ready
    descriptor; nothing is collected during body execution;
  - the elected leader seals a bounded FIFO prefix, and only one batch executes
    per environment until it joins;
  - later ready arrivals accumulate unsealed through joint completion;
  - the leader selects the next prefix itself after a join instead of electing a
    successor, so activation is immediate;
  - request bodies and confirmations run on execution threads, never with
    submitting-caller binding affinity."
  (:require [datalevin.tx-group.phase :as phase]
            [datalevin.tx-group.batch.charge :as charge])
  (:import [datalevin.utl PendingBudget PendingBudget$Usage]
           [java.util.concurrent ConcurrentLinkedQueue Semaphore TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong]
           [java.util.concurrent.locks Condition ReentrantLock]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftype Rejected [error])

(defn rejected
  "Per-request failure that must not fail, retry or reopen its batch."
  [error]
  (Rejected. error))

(deftype Descriptor
  ;; One admitted request. Its result slot is completed exactly once, by
  ;; whichever thread gets there first.
  [op data context prepare
   ^long allowance
   ^long deadline-nanos
   ^AtomicLong charged
   ^AtomicBoolean selected?
   result
   ^Semaphore ready
   ^AtomicBoolean delivered?])

(deftype Batch [^long id ^FastList descriptors ^long weight
                ^long cutoff-nanos schedule ^AtomicLong lsn collector])

(deftype Collector [^ReentrantLock lock
                    ^Condition progress
                    ^ConcurrentLinkedQueue ready
                    ^AtomicInteger queued
                    ^AtomicBoolean active
                    ^AtomicBoolean serving
                    ^AtomicBoolean closing
                    failure
                    waiters
                    ^PendingBudget budget
                    ^long max-requests
                    ^long batch-limit
                    ^long batch-max-bytes
                    ^long shared-reserved
                    ^AtomicLong published
                    ^AtomicLong next-id
                    executor])

;; ---------------------------------------------------------------------------
;; Errors

(defn- not-committed
  ^Throwable [message data]
  (ex-info message (assoc data :outcome :not-committed)))

(defn- fenced-error
  ^Throwable [^Collector collector]
  (let [failure (first @(.failure ^clojure.lang.IRef collector))]
    (if failure
      failure
      (not-committed "Environment write runtime is fenced"
                     {:error :txlog/runtime-fenced :retryable? false}))))

(defn- closed-error
  ^Throwable []
  (not-committed "Environment write runtime is closed"
                 {:error :txlog/runtime-closed :retryable? false}))

(defn- expired-error
  ^Throwable [what ^long deadline-nanos]
  (not-committed (str "Write deadline expired during " what)
                 {:error :txlog/write-deadline-exceeded :retryable? false
                  :deadline-nanos deadline-nanos}))

(defn- interrupted-error
  ^Throwable []
  (not-committed "Write was interrupted before it completed"
                 {:error :txlog/write-interrupted :retryable? false}))

;; ---------------------------------------------------------------------------
;; Result delivery

(defn- deliver!
  "Publish one result slot and wake its waiter. Exactly-once across every racing
  completion, cancellation and fence path."
  [^Descriptor descriptor value]
  (when (.compareAndSet (.delivered? descriptor) false true)
    (vreset! (.result descriptor) value)
    ;; Readiness is published only after the producing owner released its
    ;; coordination, so a woken caller never observes a half-published slot.
    (.release (.ready descriptor))
    true))

(defn- rejected-value
  [value]
  (cond
    (instance? Rejected value) [false (.-error ^Rejected value)]
    :else [true value]))

;; ---------------------------------------------------------------------------
;; Fence and shutdown

(defn- reject-queued!
  [^Collector collector value]
  (let [^ConcurrentLinkedQueue queue (.ready collector)]
    (loop []
      (when-let [^Descriptor descriptor (.poll queue)]
        (.decrementAndGet (.queued collector))
        (deliver! descriptor value)
        (recur)))))

(defn fence!
  "Publish the terminal failure record.

  Closes write and read admission and batch activation, rejects every queued
  request and wakes capacity/result waiters, without waiting for a blocked WAL
  or native operation. An executing batch keeps its own resources and may still
  establish its established WAL outcome. The terminal record never becomes
  healthy again; recovery constructs a replacement runtime."
  [^Collector collector error]
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (when (compare-and-set! ^clojure.lang.IRef (.failure collector) nil error)
        (.set (.serving ^AtomicBoolean collector) false)
        (reject-queued! collector [false error])
        (.signalAll (.progress ^Condition collector))
        (phase/phase! :fenced error))
      nil
      (finally (.unlock lock)))))

(defn close!
  "Stop admission and reject queued work without recording a failure. The active
  batch keeps its established outcome; a replacement runtime may only open
  after confirmed quiescence."
  [^Collector collector]
  (.set (.closing ^AtomicBoolean collector) true)
  (.set (.serving ^AtomicBoolean collector) false)
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (reject-queued! collector [false (closed-error)])
      (.signalAll (.progress ^Condition collector))
      (finally (.unlock lock))))
  nil)

(defn serving?
  "Whether this runtime still accepts writes and activates new batches."
  [^Collector collector]
  (.get (.serving ^AtomicBoolean collector)))

(defn published-lsn
  "Joined, locally published LSN prefix: diagnostic progress and the snapshot
  replay floor source, never a per-reader visibility filter."
  [^Collector collector]
  (.get (.published ^AtomicLong collector)))

(defn usage
  "Charged usage of admitted-but-unreleased requests and their allowances."
  [^Collector collector]
  (let [^PendingBudget$Usage u (.snapshot (.budget collector))]
    {:requests (.requests u) :bytes (.bytes u)
     :shared-reserved (.shared-reserved collector)}))

(defn limits
  "Resolved, frozen limits for this runtime."
  [^Collector collector]
  {:max-requests (.max-requests collector)
   :batch-limit (.batch-limit collector)
   :batch-max-bytes (.batch-max-bytes collector)
   :shared-reserved (.shared-reserved collector)
   :waiter-limit (.max-requests collector)})

(defn set-lsn!
  "Record the group LSN assigned before dispatch. Publication advances the
  joined prefix to this value; an unassigned group advances nothing."
  [^Batch batch ^long lsn]
  (.set (.lsn ^AtomicLong batch) lsn))

;; ---------------------------------------------------------------------------
;; Charging

(defn charge!
  "Advance a request's monotonic charge counter before unplanned owned
  allocation. Prepaid blind requests never re-enter this path; dynamic RMW
  storage and growth do. Never extends `R_i`."
  [^Descriptor descriptor ^long extra]
  (.addAndGet ^AtomicLong (.charged descriptor) extra))

;; ---------------------------------------------------------------------------
;; Collector lifecycle

(defn create
  "Create one additive collector for a new-protocol environment.

  `executor` receives the sealed batch and returns one value per request in
  batch order, where a `rejected` element is that request's own failure. The
  executor owns ordered state-dependent work, branch dispatch, join and local
  publication; this collector owns admission, sealing, activation, result
  delivery and cleanup."
  ([executor] (create executor nil))
  ([executor opts]
   (let [{:keys [limits]} (or opts {:limits (charge/resolve-limits nil)})
         lock (ReentrantLock.)
         progress (.newCondition lock)]
     (->Collector lock progress (ConcurrentLinkedQueue.) (AtomicInteger. 0)
                  (AtomicBoolean. false) (AtomicBoolean. true) (AtomicBoolean. false)
                  (volatile! nil) (long-array 1)
                  (PendingBudget. (long (:request-budget limits))
                                  (long (:max-requests limits)))
                  (long (:max-requests limits)) (long (:batch-limit limits))
                  (long (:batch-max-bytes limits)) (long (:shared-reserved limits))
                  (AtomicLong. 0) (AtomicLong. 0) executor))))

;; ---------------------------------------------------------------------------
;; Admission

(declare await-capacity!)

(defn- admit!
  "Reserve one request allowance before the caller allocates anything.

  Waiters are counted separately from admitted requests, bounded by Q, and
  retain no encoded payload. The wait ends at the caller's own deadline; it
  never cancels or steals anything from the active owner."
  [^Collector collector ^long allowance ^long deadline-nanos]
  (when-not (.get (.serving ^AtomicBoolean collector))
    (throw (fenced-error collector)))
  (loop []
    (cond
      (not (.get (.serving ^AtomicBoolean collector)))
      (throw (fenced-error collector))

      (.tryReserve (.budget collector) allowance)
      (do (phase/phase! :admitted allowance) :admitted)

      :else
      (do (await-capacity! collector deadline-nanos)
          (recur)))))

(defn- await-capacity!
  "Wait as a bounded capacity waiter for admission to become available.

  Waiters hold no encoded payload, so this runs before any caller allocation.
  The wait ends at the caller's own deadline, at shutdown, or when admission
  is fenced; it never cancels or steals anything from the active owner."
  [^Collector collector deadline-nanos]
  (let [^ReentrantLock lock (.lock collector)
        ^longs waiters (.waiters collector)]
    (.lock lock)
    (try
      (when (>= (aget waiters 0) (.max-requests collector))
        (throw (not-committed "Too many capacity waiters"
                              {:error :txlog/pending-budget-exceeded
                               :retryable? false})))
      (aset waiters 0 (inc (aget waiters 0)))
      (phase/phase! :admission-wait nil)
      (try
        (let [remaining (if (zero? deadline-nanos)
                          Long/MAX_VALUE
                          (- deadline-nanos (System/nanoTime)))]
          (when-not (pos? remaining)
            (throw (expired-error "admission" deadline-nanos)))
          (when-not (neg? (.await ^Condition (.progress collector)
                                  remaining TimeUnit/NANOSECONDS))
            (throw (expired-error "admission" deadline-nanos))))
        (finally (aset waiters 0 (dec (aget waiters 0)))))
      (finally (.unlock lock)))))

;; ---------------------------------------------------------------------------
;; Sealing

(defn- drop-expired!
  "Remove queued requests whose own deadline passed. A queued expiry never
  affects the active owner."
  [^Collector collector]
  (let [now (System/nanoTime)
        ^ConcurrentLinkedQueue queue (.ready collector)]
    (loop []
      (when-let [^Descriptor descriptor (.peek queue)]
        (let [d (.deadline-nanos descriptor)]
          (when (and (pos? d) (<= d now))
            (.poll queue)
            (.decrementAndGet (.queued collector))
            (deliver! descriptor
                      [false (expired-error "collection" d)])
            (phase/phase! :queue-expired descriptor)
            (recur)))))))

(defn- take-prefix!
  "Seal the longest ready FIFO prefix that fits both batch caps.

  Counts every selected descriptor's full allowance and stops before the first
  descriptor that would exceed either cap: it never backfills after bodies run
  and never skips a queue head to fit smaller followers. The remainder stays
  queued and unsealed until this batch joins."
  ^FastList [^Collector collector]
  (let [^ConcurrentLinkedQueue queue (.ready collector)
        limit (.batch-limit collector)
        cap (.batch-max-bytes collector)
        available (.get (.queued collector))
        bound (min limit available)]
    (loop [taken 0 bytes 0]
      (if (< taken bound)
        (let [^Descriptor head (.peek queue)]
          (if (nil? head)
            (recur taken bytes)
            (let [next (+ bytes (.allowance head))]
              (if (> next cap)
                ;; Stop before the first descriptor that would exceed either
                ;; cap. Later, smaller followers are not backfilled in.
                (recur taken bytes)
                (recur (inc taken) next)))))
        (when (pos? taken)
          (let [batch (FastList. taken)]
            (dotimes [_ taken]
              (let [^Descriptor descriptor (.poll queue)]
                (.set (.selected? descriptor) true)
                (.add batch descriptor)
                (.decrementAndGet (.queued collector))))
            batch))))))

(defn- seal-batch!
  "Under coordination: drop expired queued requests and seal the next prefix.
  Returns nil when nothing is ready or serving has closed."
  ^FastList [^Collector collector]
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (when (.get (.serving ^AtomicBoolean collector))
        (drop-expired! collector)
        (take-prefix! collector))
      (finally (.unlock lock)))))

(defn- preparation-cutoff!
  "The batch's preparation cutoff is the earliest selected member's applicable
  deadline. Rejecting a member never extends it."
  ^long [^FastList descriptors]
  (reduce (fn [acc ^Descriptor descriptor]
            (let [d (.deadline-nanos descriptor)]
              (cond
                (zero? d) acc
                (zero? acc) d
                :else (min acc d))))
          0 descriptors))

(defn- expired-member?
  "Final admission/deadline recheck before dispatch. Losing this race prevents
  both branches from starting, so the batch is definitively not committed."
  [^FastList descriptors]
  (let [now (System/nanoTime)]
    (reduce (fn [found ^Descriptor descriptor]
              (or found
                  (let [d (.deadline-nanos descriptor)]
                    (and (pos? d) (<= d now)))))
            false descriptors)))

(defn- schedule-for
  "Fixed initial selector, chosen once after freezing the accepted writes and
  before any WAL/native execution: inline for one logical write, parallel for
  two or more. Rejected or read-only members do not raise the weight."
  [^long weight]
  (if (<= weight 1) :inline :parallel))

;; ---------------------------------------------------------------------------
;; Execution

(defn- reject-pending!
  "Reject every request without a delivered value. Used by cancellation and
  pre-dispatch failure; earlier accepted rows and results are provisional."
  [^Batch batch value]
  (let [descriptors (.descriptors batch)]
    (dotimes [idx (.size descriptors)]
      (deliver! ^Descriptor (.get descriptors idx) value)))
  nil)

(defn- join-batch!
  "Coherent local publication, result recording and joined-prefix advance."
  [^Batch batch values]
  (let [^Collector collector (.collector batch)
        descriptors (.descriptors batch)]
    (.addAndGet (.published ^AtomicLong collector) (.get (.lsn ^AtomicLong batch)))
    (phase/phase! :joint-publication batch)
    (dotimes [idx (.size descriptors)]
      (deliver! ^Descriptor (.get descriptors idx)
                (rejected-value (aget ^objects values idx))))
    nil))

(defn- dispatch-batch!
  "Hand the sealed batch to the environment executor and join its results.

  Ordered state-dependent work, branch dispatch, WAL/native execution and local
  publication all belong to the executor; sealing, dispatch ordering and result
  recording belong to this collector."
  [^Batch batch]
  (let [^Collector collector (.collector batch)
        descriptors (.descriptors batch)
        weight (long (.size descriptors))]
    (phase/phase! :ordered-work batch)
    (when (expired-member? descriptors)
      (throw (expired-error "ordered preparation" (.cutoff-nanos batch))))
    (when-not (.get (.serving ^AtomicBoolean collector))
      (throw (fenced-error collector)))
    (phase/phase! :schedule-selected {:schedule (.schedule batch) :weight weight})
    (let [values ((.executor collector) batch)]
      (when-not (= weight (alength ^objects values))
        (throw (ex-info "Batch executor returned the wrong number of values"
                        {:error :txlog/batch-executor-mismatch
                         :expected weight :actual (alength ^objects values)})))
      (join-batch! batch values))))

(defn- execute-batch!
  "Run one sealed batch to joint completion, applying the phase table's
  cancellation and pre-dispatch failure rows."
  [^Batch batch]
  (let [^Collector collector (.collector batch)]
    (try
      (dispatch-batch! batch)
      (catch InterruptedException _
        ;; An observed preparation-owner interruption cancels the undispatched
        ;; batch. The owner never evaluates the suffix or leads another batch.
        (let [interrupted? (Thread/interrupted)]
          (reject-pending! batch [false (interrupted-error)])
          (when interrupted? (.interrupt (Thread/currentThread)))
          :cancelled))
      (catch Throwable t
        (reject-pending! batch [false t])
        (fence! collector t)
        :failed))))

(defn- lead!
  "Own the single active slot, sealing and joining one batch at a time until the
  ready queue is empty. The leader never hands off: after a join it selects the
  next prefix itself, so activation is immediate."
  [^Collector collector]
  (loop []
    (if-let [^FastList descriptors (seal-batch! collector)]
      (let [selected (FastList. (.size descriptors))]
        ;; Bounded by N, so this copy cannot exceed the membership carrier
        ;; already charged in the shared workspace.
        (dotimes [idx (.size descriptors)]
          (.add selected (.get descriptors idx)))
        (let [weight (long (.size selected))
              batch (->Batch (.getAndIncrement (.next-id collector)) selected weight
                             (preparation-cutoff! selected) (schedule-for weight)
                             (AtomicLong. 0) collector)]
          (phase/phase! :batch-sealed batch)
          (try
            (execute-batch! batch)
            (catch Throwable t
              (reject-pending! batch [false t])
              (fence! collector t))))
        (recur))
      (do
        (.set (.active ^AtomicBoolean collector) false)
        :idle))))

;; ---------------------------------------------------------------------------
;; Submission

(defn- result!
  [result]
  (let [slot @result]
    (when (nil? slot)
      (throw (ex-info "Write result was never completed"
                      {:error :txlog/result-incomplete})))
    (if (nth slot 0)
      (nth slot 1)
      (throw ^Throwable (nth slot 1)))))

(defn- unlink!
  "Remove one descriptor from the ready queue, so an expired caller does not wait
  for a batch that may never select it."
  [^Collector collector descriptor]
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (when (.remove (.ready collector) descriptor)
        (.decrementAndGet (.queued collector))
        true)
      (finally (.unlock lock)))))

(defn- await-result!
  "Wait for this request's own result slot.

  Interruption of a waiting submitter never cancels its request or interrupts
  the leader: the signal is remembered, the deadline-bounded wait continues, and
  the status is restored on return. If the deadline passes while the request is
  still merely queued, it is definitively rejected; a selected request keeps its
  execution deadline with its elected owner."
  [^Collector collector ^Descriptor descriptor ^long deadline-nanos]
  (let [ready (.ready descriptor)
        result (.result descriptor)
        interrupted? (volatile! false)]
    (try
      (if (zero? deadline-nanos)
        (.acquireUninterruptibly ready)
        (loop []
          (if (some? @result)
            (result! result)
            (let [remaining (- deadline-nanos (System/nanoTime))]
              (cond
                (pos? remaining)
                ;; A short bounded poll keeps an interruption and a
                ;; post-deadline selection visible without busy waiting.
                (try
                  (.tryAcquire ready (min remaining 100000) TimeUnit/NANOSECONDS)
                  (catch InterruptedException _
                    (vreset! interrupted? true)))

                (.get (.selected? descriptor))
                ;; Selected: its execution deadline belongs to the elected
                ;; owner, which completes the request or fences the runtime.
                (.acquireUninterruptibly ready)

                :else
                (do
                  (unlink! collector descriptor)
                  (deliver! descriptor
                            [false (expired-error "execution" deadline-nanos)])
                  (.acquireUninterruptibly ready)))))))
      (result! result)
      (finally
        ;; A waiting submitter never cancels its request or interrupts the
        ;; leader, so its interrupt status is restored on return.
        (when @interrupted? (.interrupt (Thread/currentThread)))))))

(defn submit!
  "Admit, prepare, publish and complete one write request.

  `request` is a map of `:allowance` (required bytes, declared before the caller
  allocates anything), optional `:prepare` (state-independent caller work),
  optional `:op`/`:data`/`:context` (ordered body inputs, run on the execution
  thread) and optional `:timeout-ms`. Returns this request's own value, or
  throws its own failure. Preparation alone grants no batch membership, LSN,
  state visibility or permission to start I/O."
  [^Collector collector {:keys [allowance prepare op data context timeout-ms]}]
  (let [allowance (long allowance)
        deadline (if timeout-ms
                         (+ (System/nanoTime) (long (* 1000000 (long timeout-ms))))
                         0)
        batch-max-bytes (.batch-max-bytes collector)
        descriptor (doto (Descriptor. op data context prepare
                                      allowance deadline
                                      (AtomicLong. allowance) (AtomicBoolean. false)
                                      (volatile! nil) (Semaphore. 0)
                                      (AtomicBoolean. false)))]
    ;; An allowance larger than the batch byte cap could never be selected, so
    ;; it is rejected before admission, encoding or body evaluation.
    (when (> allowance batch-max-bytes)
      (throw (not-committed "Request allowance exceeds the batch byte cap"
                            {:error :txlog/pending-budget-exceeded
                             :retryable? false
                             :allowance allowance
                             :write-batch-max-bytes batch-max-bytes})))
    (admit! collector allowance deadline)
    (try
      (try
        (when prepare
          (phase/phase! :caller-preparation descriptor)
          (prepare)
          (phase/phase! :caller-prepared descriptor))
        (when-not (.get (.serving ^AtomicBoolean collector))
          (throw (fenced-error collector)))
        (let [^ReentrantLock lock (.lock collector)]
          (.lock lock)
          (try
            (.add (.ready collector) descriptor)
            (.incrementAndGet (.queued collector))
            (finally (.unlock lock))))
        (phase/phase! :ready-published descriptor)
        ;; The first ready publication in an idle environment elects an owner.
        ;; Every other caller waits for its own result slot.
        (when (.compareAndSet (.active ^AtomicBoolean collector) false true)
          (lead! collector))
        (await-result! collector descriptor deadline)
        (finally
          ;; Exactly one reserve/release pair per request: the submitting thread
          ;; owns its own release, so the reservation is held until this caller
          ;; has consumed the result and dropped its references.
          (.release (.budget collector) allowance))))))

(defn admitted-usage
  "Diagnostic snapshot used by the accounting oracle."
  [^Collector collector]
  (usage collector))
