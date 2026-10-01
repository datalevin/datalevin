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
           [java.util.function LongBinaryOperator]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ^LongBinaryOperator max-long
  "Joined-prefix advance is monotonic and allocates nothing per batch."
  (reify LongBinaryOperator
    (applyAsLong [_ a b] (if (> a b) a b))))

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
                    active-batch
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
  (let [failure @(.failure ^clojure.lang.IDeref collector)]
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

(defn- cancel-error
  "Tag a clean pre-dispatch cancellation so its batch is rejected and the slot
  released without fencing the runtime. An unexpected executor or coordinator
  failure is not tagged and still fences."
  ^Throwable [cause]
  (ex-info "Batch cancelled before dispatch"
           (assoc (ex-data cause) ::pre-dispatch-cancel true)
           cause))

(defn- pre-dispatch-cancel?
  [t]
  (boolean (::pre-dispatch-cancel (ex-data t))))

;; ---------------------------------------------------------------------------
;; Result delivery

;; Descriptor accessors used by the environment executor. A request's own
;; identity, options and prepared data travel on its descriptor; nothing is read
;; from the submitting thread's ambient bindings.

(defn op
  "Ordered body for this request, if any. Runs on the elected leader."
  ^clojure.lang.IFn [^Descriptor descriptor]
  (.op descriptor))

(defn data
  "Caller-prepared payload owned by this descriptor."
  ^Object [^Descriptor descriptor]
  (.data descriptor))

(defn context
  "Explicit per-request context for identity, options and confirmations."
  ^Object [^Descriptor descriptor]
  (.context descriptor))

(defn allowance
  "Full charged allowance reserved for this request."
  ^long [^Descriptor descriptor]
  (.allowance descriptor))

(defn deadline-nanos
  "This request's absolute deadline, or zero when it set none."
  ^long [^Descriptor descriptor]
  (.deadline-nanos descriptor))

(defn selected?
  "Whether this request has been sealed into a batch."
  [^Descriptor descriptor]
  (.get (.selected? descriptor)))

(defn batch-count
  "Sealed member count."
  ^long [^Batch batch]
  (.size (.descriptors batch)))

(defn batch-at
  "The sealed member at `idx`, in publication order."
  ^Descriptor [^Batch batch idx]
  (.get (.descriptors batch) (int idx)))

(defn batch-cutoff
  "The batch's preparation cutoff: the earliest selected member's deadline."
  ^long [^Batch batch]
  (.cutoff-nanos batch))

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

(defn- fence-under-lock!
  "Publish the terminal failure record while the collector lock is held."
  [^Collector collector error]
  ;; Written only under the lock, so the first writer publishes the one
  ;; terminal record and later callers observe it unchanged.
  (when (nil? @(.failure ^clojure.lang.IDeref collector))
    (vreset! ^clojure.lang.IDeref (.failure collector) error)
    (.set (.serving ^AtomicBoolean collector) false)
    (reject-queued! collector [false error])
    (.signalAll (.progress ^Condition collector))
    (phase/phase! :fenced error)))

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
      (fence-under-lock! collector error)
      (finally (.unlock lock)))))

(defn close!
  "Stop admission and reject queued work without recording a failure. The active
  batch keeps its established outcome; a replacement runtime may only open
  after confirmed quiescence."
  [^Collector collector]
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

(defn await-quiescence!
  "Wait until no batch is executing, up to `timeout-ms`. Returns true when the
  active slot is clear. A stuck executor keeps the slot owned; callers must not
  steal it or release its resources. Each wait rechecks, so a missed
  notification cannot hang close."
  [^Collector collector ^long timeout-ms]
  (let [deadline (+ (System/nanoTime) (* 1000000 timeout-ms))
        ^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (loop []
        (if-not (.get (.active ^AtomicBoolean collector))
          true
          (let [remaining (- deadline (System/nanoTime))]
            (if (pos? remaining)
              (do (.await ^Condition (.progress collector)
                          (min remaining 50000000) TimeUnit/NANOSECONDS)
                  (recur))
              false))))
      (finally (.unlock lock)))))

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
  allocation. Returns the new charged total. Throws `:not-committed` when the
  charge would exceed the request's allowance, leaving the counter unchanged.
  Prepaid blind requests never re-enter this path; dynamic RMW storage and
  unplanned growth do."
  [^Descriptor descriptor ^long extra]
  (let [allowance (long (.allowance descriptor))
        ^AtomicLong charged (.charged descriptor)]
    (loop []
      (let [current (.get charged)]
        (when (or (neg? extra) (> extra (- allowance current)))
          (throw (not-committed "Request charge exceeds its allowance"
                                {:error :txlog/pending-budget-exceeded
                                 :outcome :not-committed
                                 :retryable? false
                                 :allowance allowance
                                 :charged current
                                 :requested extra})))
        (let [next (+ current extra)]
          (if (.compareAndSet charged current next)
            next
            (recur)))))))

(defn charged
  "Dynamic charges accumulated against this request's allowance."
  ^long [^Descriptor descriptor]
  (.get ^AtomicLong (.charged descriptor)))

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
                  (AtomicBoolean. false) (AtomicBoolean. true)
                  (volatile! nil) (volatile! nil) (long-array 1)
                  (PendingBudget. (long (:request-budget limits))
                                  (long (:max-requests limits)))
                  (long (:max-requests limits)) (long (:batch-limit limits))
                  (long (:batch-max-bytes limits)) (long (:shared-reserved limits))
                  (AtomicLong. 0) (AtomicLong. 0) executor))))

;; ---------------------------------------------------------------------------
;; Admission

(declare await-capacity! release-allowance! observe-cutoff!)

(defn- release-allowance!
  "Release one request reservation and wake capacity waiters.

  The reservation belongs to the submitting thread, so it is held until that
  caller has consumed the result and dropped its references. Releasing under
  coordination is what lets a bounded capacity waiter make progress; a waiter
  must never poll for capacity."
  [^Collector collector ^long allowance]
  (.release (.budget collector) allowance)
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (.signalAll (.progress ^Condition collector))
      (finally (.unlock lock)))))

(defn- bound-deadline
  "The earlier of a caller's own deadline and the live active batch's
  preparation cutoff. Zero means unbounded; a cutoff is considered only while a
  batch is actually executing."
  ^long [^Collector collector ^long own-deadline]
  (let [^Batch batch @(.active-batch ^clojure.lang.IDeref collector)
        cutoff (if (and batch (.get (.active ^AtomicBoolean collector)))
                 (.cutoff-nanos batch)
                 0)]
    (cond
      (zero? own-deadline) cutoff
      (zero? cutoff) own-deadline
      :else (min own-deadline cutoff))))

(defn- admit!
  "Reserve one request allowance before the caller allocates anything.

  Waiters are counted separately from admitted requests, bounded by Q, and
  retain no encoded payload. The fast path reserves before entering
  coordination; on failure `await-capacity!` claims a racing release or parks
  under the condition lock. The wait ends at the caller's own deadline; it
  never cancels or steals anything from the active owner."
  [^Collector collector ^long allowance ^long deadline-nanos]
  (when-not (.get (.serving ^AtomicBoolean collector))
    (throw (fenced-error collector)))
  ;; Admission is one of the phase checks that observes a stalled active batch.
  (when (observe-cutoff! collector)
    (throw (fenced-error collector)))
  (if (.tryReserve (.budget collector) allowance)
    (do (phase/phase! :admitted allowance) :admitted)
    (await-capacity! collector allowance deadline-nanos)))

(defn- await-capacity!
  "Claim one released allowance or wait as a bounded capacity waiter.

  Waiters hold no encoded payload, so this runs before any caller allocation.
  `PendingBudget.tryReserve` is rechecked under the condition lock before
  parking, so a release that raced the failed fast path is claimed rather than
  lost. The wait ends at the earlier of the caller's own deadline and the live
  active batch's preparation cutoff; `observe-cutoff!` fences an expired live
  cutoff so the waiter can fail now. It never cancels or steals anything from
  the active owner."
  [^Collector collector ^long allowance ^long deadline-nanos]
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
        (loop []
          (cond
            (not (.get (.serving ^AtomicBoolean collector)))
            (throw (fenced-error collector))

            ;; Claim a release that arrived before this waiter parked.
            (.tryReserve (.budget collector) allowance)
            (do (phase/phase! :admitted allowance) :admitted)

            :else
            (let [bound (bound-deadline collector deadline-nanos)
                  remaining (if (zero? bound)
                              Long/MAX_VALUE
                              (- bound (System/nanoTime)))]
              (when-not (pos? remaining)
                (throw (if (observe-cutoff! collector)
                         (fenced-error collector)
                         (expired-error "admission" deadline-nanos))))
              (if (zero? bound)
                ;; Unbounded: slice only so a newly published cutoff is
                ;; rechecked; a slice ending is not an expiry.
                (.await ^Condition (.progress collector)
                        50000000 TimeUnit/NANOSECONDS)
                ;; Bounded: await returns false exactly when the bound elapses.
                (when-not (.await ^Condition (.progress collector)
                                  remaining TimeUnit/NANOSECONDS)
                  (throw (if (observe-cutoff! collector)
                           (fenced-error collector)
                           (expired-error "admission" deadline-nanos)))))
              (recur))))
        (finally (aset waiters 0 (dec (aget waiters 0)))))
      (finally (.unlock lock)))))

;; ---------------------------------------------------------------------------
;; Sealing

(defn- drop-expired!
  "Remove every queued request whose own deadline passed, not only a leading
  run, so an expired request behind a live head cannot be sealed. Called under
  collector coordination, so the queue is stable; survivors are restored in
  their original order. A queued expiry never affects the active owner."
  [^Collector collector]
  (let [now (System/nanoTime)
        ^ConcurrentLinkedQueue queue (.ready collector)
        survivors (FastList.)]
    (loop []
      (when-let [^Descriptor descriptor (.poll queue)]
        (let [d (.deadline-nanos descriptor)]
          (if (and (pos? d) (<= d now))
            (do (.decrementAndGet (.queued collector))
                (deliver! descriptor
                          [false (expired-error "collection" d)])
                (phase/phase! :queue-expired descriptor))
            (.add survivors descriptor)))
        (recur)))
    (dotimes [idx (.size survivors)]
      (.add queue (.get survivors idx)))))

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
      (let [head (when (< taken bound) (.peek queue))
            next-bytes (when head (+ bytes (.allowance ^Descriptor head)))]
        ;; Stop before the first descriptor that would exceed either cap.
        ;; Later, smaller followers are not backfilled in, and a head that does
        ;; not fit is never skipped.
        (if (and head (<= next-bytes cap))
          (recur (inc taken) next-bytes)
          (when (pos? taken)
          (let [batch (FastList. taken)]
              (dotimes [_ taken]
                (let [^Descriptor descriptor (.poll queue)]
                  (.set (.selected? descriptor) true)
                  (.add batch descriptor)
                  (.decrementAndGet (.queued collector))))
              batch)))))))

(defn- publish-active-batch!
  "Record the batch whose preparation cutoff observers should watch."
  [^Collector collector ^Batch batch]
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (vreset! ^clojure.lang.IDeref (.active-batch collector) batch)
      ;; A newly earlier cutoff wakes waiters so they recompute their bound.
      (.signalAll (.progress ^Condition collector))
      (finally (.unlock lock)))))

(defn- clear-active-batch!
  "Clear the active batch slot when `batch` retires, if it is still current."
  [^Collector collector ^Batch batch]
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (when (identical? batch @(.active-batch ^clojure.lang.IDeref collector))
        (vreset! ^clojure.lang.IDeref (.active-batch collector) nil)
        (.signalAll (.progress ^Condition collector)))
      (finally (.unlock lock)))))

(defn- reset-active-batch!
  "Unconditionally clear the active batch slot. Callers hold the collector lock."
  [^Collector collector]
  (vreset! ^clojure.lang.IDeref (.active-batch collector) nil))

(defn- claim-next-batch!
  "Under coordination: drop expired queued requests and seal the next bounded
  prefix, or release the single active slot when nothing is ready.

  Deciding idle and releasing the slot happen under one lock, so a request
  published in the gap between an empty-queue check and slot release cannot
  strand itself behind a leader that already stopped."
  ^FastList [^Collector collector]
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (if (.get (.serving ^AtomicBoolean collector))
        (let [batch (do (drop-expired! collector)
                        (take-prefix! collector))]
          (when (nil? batch)
            ;; Activation is available again: the next ready publication elects
            ;; its own owner without waiting for a handoff signal. Wake close
            ;; waiters once the executing batch has retired.
            (.set (.active ^AtomicBoolean collector) false)
            (reset-active-batch! collector)
            (.signalAll (.progress ^Condition collector))
            (phase/phase! :next-activation nil))
          batch)
        (do (.set (.active ^AtomicBoolean collector) false)
            (reset-active-batch! collector)
            (.signalAll (.progress ^Condition collector))
            (phase/phase! :next-activation nil)
            nil))
      (finally (.unlock lock)))))

(defn- release-slot!
  "Release the single active slot under coordination and wake capacity waiters.
  Only the leader that owns the slot may do this."
  [^Collector collector]
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (.set (.active ^AtomicBoolean collector) false)
      (reset-active-batch! collector)
      (.signalAll (.progress ^Condition collector))
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

(defn observe-cutoff!
  "Observe the live active batch's preparation cutoff.

  If the same batch is still executing and its cutoff has expired, reject its
  members and fence the runtime with the established not-committed outcome.
  This never runs a body, closes another thread's reader or reclaims the active
  slot: a stuck owner keeps its resources until it actually exits, and its late
  return cannot publish success. Returns true when it fenced."
  [^Collector collector]
  (let [^Batch batch @(.active-batch ^clojure.lang.IDeref collector)
        now (System/nanoTime)]
    (if (and batch
             (let [c (.cutoff-nanos batch)] (and (pos? c) (<= c now))))
      (let [^ReentrantLock lock (.lock collector)]
        (.lock lock)
        (try
          (let [current @(.active-batch ^clojure.lang.IDeref collector)
                c (when (identical? current batch) (.cutoff-nanos batch))
                now (System/nanoTime)]
            (if (and (identical? current batch)
                     (.get (.active ^AtomicBoolean collector))
                     (pos? (long (or c 0)))
                     (<= (long (or c 0)) now))
              (let [error (expired-error "preparation" (long c))]
                ;; Reject the live batch so its waiters can fail now; a late
                ;; owner join cannot deliver success again.
                (reject-pending! batch [false error])
                (fence-under-lock! collector error)
                true)
              false))
          (finally (.unlock lock))))
      false)))

(defn- join-batch!
  "Coherent local publication, result recording and joined-prefix advance."
  [^Batch batch values]
  (let [^Collector collector (.collector batch)
        descriptors (.descriptors batch)]
    ;; P is an absolute joined prefix, not a running sum of group lengths.
    (.accumulateAndGet (.published ^AtomicLong collector)
                       (.get (.lsn ^AtomicLong batch))
                       max-long)
    (phase/phase! :joint-publication batch)
    (dotimes [idx (.size descriptors)]
      (deliver! ^Descriptor (.get descriptors idx)
                (rejected-value (aget ^objects values idx))))
    nil))

(defn- join-or-reject!
  "Publish joined results, unless a terminal fence was recorded while the batch
  executed; a late return must never publish normal success."
  [^Batch batch values]
  (let [^Collector collector (.collector batch)]
    (if-let [failure @(.failure ^clojure.lang.IDeref collector)]
      (reject-pending! batch [false failure])
      (join-batch! batch values))))

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
      (throw (cancel-error
              (expired-error "ordered preparation" (.cutoff-nanos batch)))))
    (when-not (.get (.serving ^AtomicBoolean collector))
      (throw (cancel-error (fenced-error collector))))
    (phase/phase! :schedule-selected {:schedule (.schedule batch) :weight weight})
    (let [values ((.executor collector) batch)]
      (when-not (= weight (alength ^objects values))
        (throw (ex-info "Batch executor returned the wrong number of values"
                        {:error :txlog/batch-executor-mismatch
                         :expected weight :actual (alength ^objects values)})))
      (join-or-reject! batch values))))

(defn- execute-batch!
  "Run one sealed batch to joint completion, applying the phase table's
  cancellation and pre-dispatch failure rows.

  Returns `:joined`, `:cancelled` (a clean pre-dispatch cancellation), or
  `:interrupted` (the preparation owner must stop leading). An unexpected
  failure rejects the batch and fences, returning `:fenced`."
  [^Batch batch]
  (let [^Collector collector (.collector batch)]
    (try
      (dispatch-batch! batch)
      :joined
      (catch InterruptedException _
        ;; An observed preparation-owner interruption cancels the undispatched
        ;; batch. The owner never evaluates the suffix or leads another batch.
        (let [interrupted? (Thread/interrupted)]
          (reject-pending! batch [false (interrupted-error)])
          (when interrupted? (.interrupt (Thread/currentThread)))
          :interrupted))
      (catch Throwable t
        (if (pre-dispatch-cancel? t)
          ;; A clean pre-dispatch cancellation rejects the undispatched batch,
          ;; preserves its not-committed outcome and lets the runtime continue.
          (do (reject-pending! batch [false (ex-cause t)])
              :cancelled)
          (do (reject-pending! batch [false t])
              (fence! collector t)
              :fenced))))))

(defn- run-sealed-batch!
  "Execute one sealed batch on this leader. Returns true when the preparation
  owner was interrupted and must therefore stop leading."
  [^Collector collector ^FastList descriptors]
  (let [selected (FastList. (.size descriptors))]
    ;; Bounded by N, so this copy cannot exceed the membership carrier already
    ;; charged in the shared workspace.
    (dotimes [idx (.size descriptors)]
      (.add selected (.get descriptors idx)))
    (let [weight (long (.size selected))
          batch (->Batch (.getAndIncrement (.next-id collector)) selected weight
                         (preparation-cutoff! selected) (schedule-for weight)
                         (AtomicLong. 0) collector)]
      (phase/phase! :batch-sealed batch)
      (publish-active-batch! collector batch)
      (let [outcome (execute-batch! batch)]
        (clear-active-batch! collector batch)
        (if (= :interrupted outcome)
          (do (release-slot! collector) true)
          false)))))

(defn- lead!
  "Own the single active slot, sealing and joining one batch at a time until the
  ready queue is empty.

  The leader never hands off: after a join it selects the next prefix itself, so
  activation is immediate and no elected successor needs waking."
  [^Collector collector]
  (loop []
    (let [descriptors (claim-next-batch! collector)]
      (cond
        (nil? descriptors) :idle
        (run-sealed-batch! collector descriptors) :interrupted
        :else (recur)))))

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

(defn- wait-until-deadline!
  "Wait for a result under the earlier of this request's own deadline and the
  live active batch's preparation cutoff.

  A short bounded poll keeps an interruption and a post-deadline selection
  visible without busy waiting. When the bound elapses, the waiter observes the
  active batch's cutoff: an expired live batch is fenced so the waiter (and a
  queued request) can fail now. Otherwise a selected request keeps waiting for
  its elected owner, while an unselected one is definitively rejected and
  unlinked from the ready queue."
  [^Collector collector ^Descriptor descriptor ^Semaphore ready
   ^clojure.lang.IDeref result ^clojure.lang.IDeref interrupted?
   deadline-nanos]
  (loop []
    (when (nil? @result)
      (let [selected? (.get (.selected? descriptor))
            bound (bound-deadline collector deadline-nanos)
            remaining (- bound (System/nanoTime))]
        (cond
          (pos? remaining)
          (try
            (.tryAcquire ready (min remaining 100000) TimeUnit/NANOSECONDS)
            (catch InterruptedException _
              (vreset! interrupted? true)))

          selected?
          (do (observe-cutoff! collector)
              ;; The observer may have rejected this member; otherwise the
              ;; elected owner still owes the result.
              (.acquireUninterruptibly ready))

          :else
          (do (observe-cutoff! collector)
              (when (nil? @result)
                (unlink! collector descriptor)
                (deliver! descriptor [false (expired-error "execution" deadline-nanos)]))
              (.acquireUninterruptibly ready))))
      (recur))))

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
        (wait-until-deadline! collector descriptor ready result interrupted?
                              deadline-nanos))
      (result! result)
      (finally
        ;; A waiting submitter never cancels its request or interrupts the
        ;; leader, so its interrupt status is restored on return.
        (when @interrupted? (.interrupt (Thread/currentThread)))))))

(defn- publish-and-elect!
  "Publish one ready descriptor and elect this caller when the environment is idle.

  Publication and election are a single coordination step, so a terminal fence
  can neither strand an already published request nor let it activate a batch
  in a runtime that is no longer serving. Returns true when this caller owns the
  single active batch slot."
  [^Collector collector descriptor]
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (when-not (.get (.serving ^AtomicBoolean collector))
        (throw (fenced-error collector)))
      (.add (.ready collector) descriptor)
      (.incrementAndGet (.queued collector))
      ;; Emit at the enqueue point, under the same coordination that fixes FIFO,
      ;; so a trace observer records the real publication order.
      (phase/phase! :ready-published descriptor)
      ;; The first ready publication in an idle environment elects an owner;
      ;; every other caller waits for its own result slot.
      (.compareAndSet (.active ^AtomicBoolean collector) false true)
      (finally (.unlock lock)))))

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
                                      (AtomicLong. 0) (AtomicBoolean. false)
                                      (volatile! nil) (Semaphore. 0)
                                      (AtomicBoolean. false)))]
    ;; Every admitted request must cover at least its own control bundle; an
    ;; allowance above the batch byte cap could never be selected. Both are
    ;; rejected before admission, encoding or body evaluation.
    (when (< allowance (long charge/request-control-bundle))
      (throw (not-committed "Request allowance is below the minimum control bundle"
                            {:error :txlog/pending-budget-exceeded
                             :retryable? false
                             :allowance allowance
                             :min-allowance charge/request-control-bundle})))
    (when (> allowance batch-max-bytes)
      (throw (not-committed "Request allowance exceeds the batch byte cap"
                            {:error :txlog/pending-budget-exceeded
                             :retryable? false
                             :allowance allowance
                             :write-batch-max-bytes batch-max-bytes})))
    (admit! collector allowance deadline)
    (try
      (when prepare
        (phase/phase! :caller-preparation descriptor)
        (prepare)
        (phase/phase! :caller-prepared descriptor))
      (when (publish-and-elect! collector descriptor)
        (lead! collector))
      (await-result! collector descriptor deadline)
      (finally
        ;; Exactly one reserve/release pair per request.
        (release-allowance! collector allowance)))))

(defn admitted-usage
  "Diagnostic snapshot used by the accounting oracle."
  [^Collector collector]
  (usage collector))
