;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch
  "Additive collector for the new write protocol.

  This namespace implements Invariants 1-6 of the design contract. It is not a
  replacement for `datalevin.tx-group` and never calls it: compatibility keeps
  its own collector, and one environment selects exactly one of them (see
  `datalevin.tx-group.batch.env`).

  - callers do their own state-independent preparation, then publish one ready
    descriptor;
  - the elected leader selects a bounded FIFO prefix, and only one batch executes
    per environment until it joins;
  - native application can collect more ready members within the same caps;
    membership freezes before WAL dispatch, and later arrivals queue;
  - after its own result joins, the leader wakes a queued successor and returns;
  - request bodies and confirmations run on execution threads, never with
    submitting-caller binding affinity."
  (:require [datalevin.tx-group.phase :as phase]
            [datalevin.tx-group.batch.charge :as charge])
  (:import [datalevin.utl PendingBudget PendingBudget$Usage]
           [java.util.concurrent ConcurrentLinkedQueue TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong
            AtomicLongArray]
           [java.util.concurrent.locks Condition LockSupport ReentrantLock]
           [java.util.function LongBinaryOperator]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ^LongBinaryOperator max-long
  "Joined-prefix advance is monotonic and allocates nothing per batch."
  (reify LongBinaryOperator
    (applyAsLong [_ a b] (if (> a b) a b))))

(deftype Descriptor
  ;; One admitted request. Its result slot is completed exactly once, by
  ;; whichever thread gets there first. Its reservation has two owners once the
  ;; batch seals it: the batch, until both branches stop, and the caller, until
  ;; the result is delivered. The last owner to drop the reservation physically
  ;; releases it; an unsealed request has only the caller owner.
  [op data context
   ^long allowance
   ^long deadline-nanos
   ^AtomicLong charged
   ^AtomicBoolean selected?
   result
   ^Thread waiter
   ^AtomicBoolean delivered?
   ^AtomicBoolean released?
   ^AtomicBoolean caller-done?
   ^AtomicBoolean batch-done?])

(definterface ^:private IBatchPreparation
  (^"[Ljava.lang.Object;" walBodies [])
  (^void setWalBodies [^"[Ljava.lang.Object;" bodies])
  (executionSchedule [])
  (^void setExecutionSchedule [value])
  (^long cutoffNanos [])
  (^void setCutoffNanos [^long deadline])
  (^long acceptedCount [])
  (^void setAcceptedCount [^long accepted]))

(deftype Batch [^long id ^FastList descriptors
                ;; Mutable before dispatch, on the elected leader only: ordered
                ;; preparation compacts the body carrier for conditional no-ops
                ;; and re-selects the schedule from the final accepted
                ;; write weight. Dispatch publishes both to the other branch.
                ^objects ^:unsynchronized-mutable wal-bodies
                ^long weight
                ^long ^:volatile-mutable cutoff-nanos
                ^:unsynchronized-mutable schedule
                ^AtomicLong lsn
                ^AtomicBoolean dispatched?
                wal-status collector
                ^long ^:unsynchronized-mutable accepted-count]
  ;; Mutable deftype fields are private to its methods. Keep their access here
  ;; so preparation uses direct calls rather than reflective field access.
  IBatchPreparation
  (walBodies [_] wal-bodies)
  (setWalBodies [_ bodies] (set! wal-bodies bodies))
  (executionSchedule [_] schedule)
  (setExecutionSchedule [_ value] (set! schedule value))
  (cutoffNanos [_] cutoff-nanos)
  (setCutoffNanos [_ deadline] (set! cutoff-nanos deadline))
  (acceptedCount [_] accepted-count)
  (setAcceptedCount [_ accepted] (set! accepted-count accepted)))

(deftype Collector [^ReentrantLock lock
                    ^Condition progress
                    ^ConcurrentLinkedQueue ready
                    ^AtomicInteger queued
                    ^AtomicBoolean active
                    ^AtomicBoolean serving
                    active-batch
                    failure
                    ;; Registration is lock-protected; atomic visibility lets
                    ;; refunds skip coordination when no capacity waiter exists.
                    ^AtomicLongArray waiters
                    ^PendingBudget budget
                    ^long max-requests
                    ^long batch-limit
                    ^long batch-max-bytes
                    ^long shared-reserved
                    ^long preparation-timeout-ms
                    ;; Configured `:wal-rmw-max-bytes`, the reserved allowance
                    ;; `R_i` of every admitted body-based request.
                    ^long rmw-allowance
                    ^AtomicLong published
                    ^AtomicLong next-id
                    executor
                    check-prepared!
                    on-failure!])

(def default-preparation-timeout-ms
  "Default bound on admission and caller preparation, measured from submission.
  An explicit request `:timeout-ms` only tightens it."
  30000)

;; ---------------------------------------------------------------------------
;; Errors

(defn- not-committed
  ^Throwable [message data]
  (ex-info message (assoc data :outcome :not-committed)))

(defn- fenced-error
  ^Throwable [^Collector collector]
  (let [failure @(.failure collector)]
    (ex-info "Environment write runtime is fenced"
             {:error :txlog/runtime-fenced :outcome :not-committed
              :retryable? false}
             failure)))

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
  (let [cause (if (= :not-committed (:outcome (ex-data cause)))
                cause
                (ex-info "Batch aborted before WAL append"
                         (assoc (ex-data cause) :outcome :not-committed
                                :retryable? false)
                         cause))]
    (ex-info "Batch cancelled before dispatch"
             (assoc (ex-data cause) ::pre-dispatch-cancel true)
             cause)))

(defn- pre-dispatch-cancel?
  [t]
  (boolean (::pre-dispatch-cancel (ex-data t))))

(defn pre-dispatch-cancellation?
  "Whether this failure is a clean pre-dispatch cancellation of its whole batch.

  Ordered preparation uses it to keep expiry, fencing and interruption
  batch-level instead of turning an engine failure into one request's rejection."
  [^Throwable t]
  (pre-dispatch-cancel? t))

;; ---------------------------------------------------------------------------
;; Result delivery

(declare schedule-for)

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
  @(.data descriptor))

(defn set-data!
  "Install this descriptor's prepared payload.

  Native body execution installs `{:rows rows :result result :wal-body body}`.
  Only the elected leader touches descriptors before dispatch."
  [^Descriptor descriptor value]
  (vreset! (.data descriptor) value))

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
  "Whether this request has been selected into a batch."
  [^Descriptor descriptor]
  (.get ^AtomicBoolean (.selected? descriptor)))

(defn batch-count
  "Sealed member count."
  ^long [^Batch batch]
  (.size ^FastList (.descriptors batch)))

(defn batch-at
  "The selected member at `idx`, in publication order."
  ^Descriptor [^Batch batch idx]
  (.get ^FastList (.descriptors batch) (int idx)))

(defn wal-bodies
  "Sealed WAL body references in request order; immutable after dispatch.

  Before dispatch this is the preallocated array shared by the scheduled tasks,
  so the task that refreshes it stays a stable identity."
  ^objects [^Batch batch]
  (.walBodies batch))

(defn accepted-count
  "Sealed member count; mutable before dispatch.

  Native body execution lowers it to the number of write-bearing members;
  conditional no-ops need no WAL body. The collector and both branches read it only after dispatch."
  ^long [^Batch batch]
  (.acceptedCount batch))

(defn freeze-schedule!
  "Fix the WAL schedule after collection. Overlap only when native work remains."
  ([batch accepted-weight] (freeze-schedule! batch accepted-weight true))
  ([^Batch batch ^long accepted-weight native-work?]
   (.setExecutionSchedule batch (if native-work? (schedule-for accepted-weight) :inline))
   (.executionSchedule batch)))

(defn- compact-bodies!
  "Copy the accepted bodies into one compacted carrier, in request order.

  A conditional write producing no rows needs no WAL body. All members still
  share the same batch outcome."
  ^objects [^FastList descriptors ^long accepted]
  (let [^objects compacted (object-array (int accepted))
        total (.size descriptors)]
    (loop [idx 0 out 0]
      (if (< idx total)
        (let [body (:wal-body (data (.get descriptors idx)))]
          (if (nil? body)
            (recur (inc idx) out)
            (do (aset compacted out body)
                (recur (inc idx) (inc out)))))
        (do (when-not (== out accepted)
              (throw (IllegalStateException.
                       "Accepted WAL body count does not match the accepted members")))
            ;; Published once, so the scheduled refresh task and the dispatching
            ;; leader cannot observe different carriers.
            compacted)))))

(defn refresh-wal-bodies!
  "Fill the batch's WAL body array before dispatch.

  Sealing captures caller-prepared blind bodies. An ordered preparation hook
  refreshes the same array after finalizing state-dependent bodies, compacting
  it when conditional bodies produce no rows. When every member writes,
  this fills the preallocated array in place, keeping the identity
  that scheduled tasks and the WAL append already hold."
  [^Batch batch]
  (let [^FastList descriptors (.descriptors batch)
        accepted (.acceptedCount batch)
        ^objects bodies (.walBodies batch)]
    (if (and (== accepted (.size descriptors)) (== accepted (alength bodies)))
      (dotimes [idx (.size descriptors)]
        (aset bodies idx (:wal-body (data (.get descriptors idx)))))
      (.setWalBodies batch (compact-bodies! descriptors accepted)))
    batch))

(defn batch-cutoff
  "The batch's preparation cutoff: the earliest selected member's deadline."
  ^long [^Batch batch]
  (.cutoffNanos batch))

(defn expired?
  "Whether the earliest selected member's deadline has passed.
  Collection maintains this cutoff; dispatch and commit need no member scan."
  [^Batch batch]
  (let [cutoff (.cutoffNanos batch)]
    (and (pos? cutoff) (<= cutoff (System/nanoTime)))))

(defn batch-schedule
  "The schedule fixed for this sealed batch: `:inline` or `:parallel`."
  [^Batch batch]
  (.executionSchedule batch))

(defn batch-collector
  "The collector that owns this batch."
  [^Batch batch]
  (.collector batch))

(defn check-serving!
  "Throw the collector's terminal failure when it is no longer serving.

  Branch owners call this immediately before applying or committing native work
  so a fence recorded during WAL/policy work aborts the commit instead of
  committing after runtime failure. Returns nil while healthy."
  [^Collector collector]
  (when-not (.get ^AtomicBoolean (.serving collector))
    (throw (fenced-error collector))))

(defn- publish-result!
  "Publish one result slot. Exactly-once across every racing
  completion, cancellation and fence path."
  [^Descriptor descriptor value]
  (when (.compareAndSet ^AtomicBoolean (.delivered? descriptor) false true)
    (vreset! (.result descriptor) value)
    true))

(defn- notify-descriptor!
  "Nudge the one submitting thread; result/election predicates carry progress."
  [^Descriptor descriptor]
  (LockSupport/unpark (.waiter descriptor)))

(defn- deliver!
  "Publish and notify immediately on cancellation and other unbatched paths."
  [^Descriptor descriptor value]
  (when (publish-result! descriptor value)
    (notify-descriptor! descriptor)
    true))

;; ---------------------------------------------------------------------------
;; Fence and shutdown

(defn- signal-progress-under-lock!
  "Notify registered progress waiters while holding collector coordination."
  [^Collector collector]
  (let [^AtomicLongArray waiters (.waiters collector)
        n (+ (.get waiters 0) (.get waiters 1))]
    (when (pos? n)
      (if (= n 1)
        (.signal ^Condition (.progress collector))
        ;; Different allowances and predicates share this condition. Waking
        ;; only one of several waiters could leave an eligible caller asleep.
        (.signalAll ^Condition (.progress collector))))))

(defn- reject-queued!
  [^Collector collector value]
  (let [^ConcurrentLinkedQueue queue (.ready collector)]
    (loop []
      (when-let [^Descriptor descriptor (.poll queue)]
        (.decrementAndGet ^AtomicInteger (.queued collector))
        (deliver! descriptor value)
        (recur)))))

(defn- fence-under-lock!
  "Publish the terminal failure record while the collector lock is held."
  [^Collector collector error]
  ;; Written only under the lock, so the first writer publishes the one
  ;; terminal record and later callers observe it unchanged.
  (when (nil? @(.failure collector))
    (vreset! ^clojure.lang.IDeref (.failure collector) error)
    (.set ^AtomicBoolean (.serving collector) false)
    ;; This failure belongs to the active batch. Unstarted requests have no
    ;; WAL outcome or LSN and must not inherit that batch's committed result.
    (reject-queued! collector [false (fenced-error collector)])
    (signal-progress-under-lock! collector)
    ;; Keep native admission in step with the terminal collector fence. The
    ;; callback only closes a lifetime gate; it never waits for native owners.
    (when-let [on-failure! (.-on-failure! collector)]
      (on-failure! error))
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
  (.set ^AtomicBoolean (.serving collector) false)
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (reject-queued! collector [false (closed-error)])
      (signal-progress-under-lock! collector)
      (finally (.unlock lock))))
  nil)

(defn serving?
  "Whether this runtime still accepts writes and activates new batches."
  [^Collector collector]
  (.get ^AtomicBoolean (.serving collector)))

(defn await-quiescence!
  "Wait until no batch is executing, up to `timeout-ms`. Returns true when the
  active slot is clear. A stuck executor keeps the slot owned; callers must not
  steal it or release its resources. Each wait rechecks, so a missed
  notification cannot hang close."
  [^Collector collector ^long timeout-ms]
  (let [deadline (+ (System/nanoTime) (* 1000000 timeout-ms))
        ^ReentrantLock lock (.lock collector)
        ^AtomicLongArray waiters (.waiters collector)]
    (.lock lock)
    (try
      (.incrementAndGet waiters 1)
      (try
        (loop []
          (if-not (.get ^AtomicBoolean (.active collector))
            true
            (let [remaining (- deadline (System/nanoTime))]
              (if (pos? remaining)
                (do (.await ^Condition (.progress collector)
                            (min remaining 50000000) TimeUnit/NANOSECONDS)
                    (recur))
                false))))
        (finally (.decrementAndGet waiters 1)))
      (finally (.unlock lock)))))

(defn published-lsn
  "Joined, locally published LSN prefix: diagnostic progress and the snapshot
  replay floor source, never a per-reader visibility filter."
  [^Collector collector]
  (.get ^AtomicLong (.published collector)))

(defn initialize-prefix!
  "Install the verified recovered prefix before publishing a fresh collector.
  Runtime writes advance it only at join."
  [^Collector collector ^long lsn]
  (when (or (.get ^AtomicBoolean (.active collector))
            (pos? (.get ^AtomicInteger (.queued collector))))
    (throw (IllegalStateException. "Cannot initialize a running collector")))
  (.set ^AtomicLong (.published collector) lsn))

(defn cancel-before-dispatch!
  "Reject a sealed but undispatched batch without fencing, preserving the
  original cause/outcome. Capacity pressure uses this before either I/O branch."
  [cause]
  (throw (cancel-error cause)))

(defn usage
  "Charged usage of admitted-but-unreleased requests and their allowances."
  [^Collector collector]
  (let [^PendingBudget$Usage u (.snapshot ^PendingBudget (.budget collector))]
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
  (.set ^AtomicLong (.lsn batch) lsn))

(defn batch-lsn
  "The assigned WAL group LSN, or zero before assignment."
  ^long [^Batch batch]
  (.get ^AtomicLong (.lsn batch)))

(defn record-wal-policy!
  "Retain the established WAL policy result through join and caller delivery.

  The WAL owner records this before publishing branch completion. Nil means no
  policy result was established; :appended and :durable preserve the distinction
  between relaxed acknowledgment and confirmed durability."
  [^Batch batch durable?]
  (vreset! (.wal-status batch) (if durable? :durable :appended)))

(defn- wal-outcome-error
  "Preserve this batch's established WAL outcome when local completion fails."
  [^Batch batch cause]
  (if-let [status @(.wal-status batch)]
    (let [durable? (= :durable status)]
      (ex-info (if durable?
                 "WAL durability confirmed but local completion failed"
                 "WAL append policy-complete but local completion failed; durability unconfirmed")
               {:error (if durable? :txlog/write-committed
                           :txlog/write-indeterminate)
                :outcome (if durable? :committed :indeterminate)
                :wal-status status
                :txlog-lsn (.get ^AtomicLong (.lsn batch))
                :retryable? false}
               cause))
    cause))

(defn mark-dispatched!
  "Transition the batch from ordered preparation to branch execution.

  After this the collector stops fencing on the preparation cutoff: the
  WAL/native branch deadlines own the batch. The environment executor calls this
  between finishing ordered preparation and starting its branches. A batch that
  never reaches this point is still preparation and remains subject to the
  cutoff observer."
  [^Batch batch]
  (.set ^AtomicBoolean (.dispatched? batch) true))

(defn begin-dispatch!
  "Finalize ordered preparation and dispatch, atomically under coordination.

  Rechecks serving status and every selected member's deadline, then marks the
  batch dispatched. Raises a clean pre-dispatch cancellation when the batch must
  not start, so dispatch and cancellation are mutually exclusive and ordered
  preparation that overran the batch deadline cannot commit. If this returns,
  both branches may run. The environment executor calls this in place of
  `mark-dispatched!`."
  [^Batch batch]
  (let [^Collector collector (.collector batch)
        ^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (when-not (.get ^AtomicBoolean (.serving collector))
        (throw (cancel-error (fenced-error collector))))
      (when (expired? batch)
        (throw (cancel-error
                (expired-error "ordered preparation" (.cutoffNanos batch)))))
      (.set ^AtomicBoolean (.dispatched? batch) true)
      (finally (.unlock lock)))))

(defn dispatched?
  "Whether this batch has left ordered preparation for branch execution."
  [^Batch batch]
  (.get ^AtomicBoolean (.dispatched? batch)))

(defn set-accepted-count!
  "Record how many sealed members carry accepted writes.

  Ordered preparation publishes the final count after classifying every member,
  before dispatch. Conditional no-ops reduce it below the sealed count; it
  bounds the compacted WAL body array."
  [^Batch batch ^long accepted]
  (.setAcceptedCount batch accepted))

(defn check-preparation!
  "Ordered preparation's between-member check.

  Deliberately cheap and lock-free: it reads serving status, the batch cutoff
  and the preparation owner's interrupt flag. Each of those cancels the whole
  undispatched batch instead of rejecting one request, because none of them can
  be attributed to a single request's own work. Returns nil while the batch may
  still prepare."
  [^Batch batch]
  (let [^Collector collector (.collector batch)
        cutoff (long (.cutoffNanos batch))]
    (when-not (.get ^AtomicBoolean (.serving collector))
      (throw (cancel-error (fenced-error collector))))
    (when (and (pos? cutoff) (> (System/nanoTime) cutoff))
      (throw (cancel-error (expired-error "ordered preparation" cutoff))))
    (when (.isInterrupted (Thread/currentThread))
      (throw (cancel-error (interrupted-error))))
    nil))

;; ---------------------------------------------------------------------------
;; Charging

(defn charge!
  "Advance a request's monotonic charge counter before unplanned owned
  allocation. Returns the new charged total. Throws `:not-committed` when the
  charge would exceed the request's allowance, leaving the counter unchanged.
  Planned prepaid allocations never re-enter this path; dynamic RMW storage
  and unplanned growth do. A fully prepaid blind request rejects any growth."
  ^long [^Descriptor descriptor ^long extra]
  (let [allowance (long (.allowance descriptor))
        ^AtomicLong charged ^AtomicLong (.charged descriptor)]
    (loop []
      (let [current (.get charged)]
        (when (or (neg? extra) (> extra (- allowance current)))
          (throw (not-committed "Request charge exceeds its allowance"
                                {:error :txlog/pending-budget-exceeded
                                 :outcome :not-committed
                                 :retryable? false
                                 :reason (if (.op descriptor) :rmw-allowance :blind-growth)
                                 :phase :preparation
                                 :limit-bytes allowance
                                 :charged-bytes current
                                 :next-charge-bytes extra
                                 :allowance allowance
                                 :charged current
                                 :requested extra})))
        (let [next (+ current extra)]
          (if (.compareAndSet charged current next)
            next
            (recur)))))))

(defn charged
  "Cumulative charge, including the initial precharge."
  ^long [^Descriptor descriptor]
  (.get ^AtomicLong (.charged descriptor)))

;; ---------------------------------------------------------------------------
;; Collector lifecycle

(defn create
  "Create one additive collector for a new-protocol environment.

  `executor` receives the selected batch and returns one value per final member in
  batch order. Any preparation failure aborts the whole batch. The
  executor owns ordered state-dependent work, branch dispatch, join and local
  publication; this collector owns admission, sealing, activation, result
  delivery and cleanup."
  ([executor] (create executor nil))
  ([executor opts]
   (let [{:keys [limits preparation-timeout-ms check-prepared! on-failure!]} opts
         limits (or limits (charge/resolve-limits nil))
         ;; A selecting leader must not barge ahead of publishers already
         ;; waiting to enqueue their prepared requests.
         lock (ReentrantLock. true)
         progress (.newCondition lock)]
     (->Collector lock progress (ConcurrentLinkedQueue.) (AtomicInteger. 0)
                  (AtomicBoolean. false) (AtomicBoolean. true)
                  (volatile! nil) (volatile! nil) (AtomicLongArray. 2)
                  (PendingBudget. (long (:request-budget limits))
                                  (long (:max-requests limits)))
                  (long (:max-requests limits)) (long (:batch-limit limits))
(long (:batch-max-bytes limits)) (long (:shared-reserved limits))
                   (long (or preparation-timeout-ms default-preparation-timeout-ms))
                   (long (:rmw-allowance-bytes limits))
                   (AtomicLong. 0) (AtomicLong. 0) executor check-prepared!
                   on-failure!))))

;; ---------------------------------------------------------------------------
;; Admission

(declare await-capacity! release-allowance! observe-cutoff!)

(defn- signal-capacity!
  "Wake capacity waiters once after one or more reservations are released.

  Refunds publish budget capacity before reading the registered waiter count.
  Registration happens before the waiter's locked capacity recheck: either the
  refund sees a waiter and signals under coordination, or a later registration
  observes the refund in that recheck. Idle refunds need no collector lock."
  [^Collector collector]
  (when (pos? (.get ^AtomicLongArray (.waiters collector) 0))
    (let [^ReentrantLock lock (.lock collector)]
      (.lock lock)
      (try
        (when (pos? (.get ^AtomicLongArray (.waiters collector) 0))
          (signal-progress-under-lock! collector))
        (finally (.unlock lock))))))

(defn- release-allowance!
  "Release one request reservation and wake capacity waiters."
  [^Collector collector ^long allowance]
  (.release ^PendingBudget (.budget collector) allowance)
  (signal-capacity! collector))

(defn- release-reservation!
  "Physically release one request reservation exactly once.

  Both the caller and the batch call the two-sided release paths, so the CAS on
  `released?` is the single gate that charges the budget exactly once."
  [^Collector collector ^Descriptor descriptor]
  (when (.compareAndSet ^AtomicBoolean (.released? descriptor) false true)
    (release-allowance! collector (.allowance descriptor))))

(defn- admitted!
  "Observe a new reservation before transferring it to a descriptor."
  [^Collector collector ^long allowance]
  (try
    (phase/phase! :admitted allowance)
    :admitted
    (catch Throwable t
      (release-allowance! collector allowance)
      (throw t))))

(defn- release-descriptor!
  "Drop this caller's ownership of its request reservation exactly once.

  A reservation is released only after both owners drop it: the batch, once a
  sealed request's branches stop, and the caller, once the result is delivered.
  This caller fallback is idempotent through the `caller-done?` CAS. A request
  the batch never seals has only the caller owner, so the caller releases it
  directly; the recheck under collector coordination closes the window where the
  batch is about to seal it."
  [^Collector collector ^Descriptor descriptor]
  (when (.compareAndSet ^AtomicBoolean (.caller-done? descriptor) false true)
    (if (.get ^AtomicBoolean (.selected? descriptor))
      ;; The batch co-owns a sealed request; it releases if it already finished.
      (when (.get ^AtomicBoolean (.batch-done? descriptor))
        (release-reservation! collector descriptor))
      (let [^ReentrantLock lock (.lock collector)]
        (.lock lock)
        (try
          (if (.get ^AtomicBoolean (.selected? descriptor))
            ;; Selection won the race; the batch now co-owns and finishes later.
            (when (.get ^AtomicBoolean (.batch-done? descriptor))
              (release-reservation! collector descriptor))
            (do
              (when (.remove ^ConcurrentLinkedQueue (.ready collector) descriptor)
                (.decrementAndGet ^AtomicInteger (.queued collector)))
              (release-reservation! collector descriptor)))
          (finally (.unlock lock)))))))

(defn- bound-deadline
  "The earlier of a caller's own deadline and the live active batch's
  preparation cutoff. Zero means unbounded; a cutoff is considered only while a
  batch is executing and has not yet been dispatched to its branches."
  ^long [^Collector collector ^long own-deadline]
  (let [^Batch batch @(.active-batch collector)
        cutoff (if (and batch
                        (.get ^AtomicBoolean (.active collector))
                        (not (.get ^AtomicBoolean (.dispatched? batch))))
                 (.cutoffNanos batch)
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
  (when-not (.get ^AtomicBoolean (.serving collector))
    (throw (fenced-error collector)))
  ;; Admission is one of the phase checks that observes a stalled active batch.
  (when (observe-cutoff! collector)
    (throw (fenced-error collector)))
  (if (.tryReserve ^PendingBudget (.budget collector) allowance)
    (admitted! collector allowance)
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
        ^AtomicLongArray waiters (.waiters collector)]
    (.lock lock)
    (try
      (when (>= (.get waiters 0) (.max-requests collector))
        (throw (not-committed "Too many capacity waiters"
                              {:error :txlog/pending-budget-exceeded
                               :retryable? false})))
      (.incrementAndGet waiters 0)
      (try
        (phase/phase! :admission-wait nil)
        (loop []
          (cond
            (not (.get ^AtomicBoolean (.serving collector)))
            (throw (fenced-error collector))

            (observe-cutoff! collector)
            (throw (fenced-error collector))

            (and (pos? deadline-nanos) (<= deadline-nanos (System/nanoTime)))
            (throw (expired-error "admission" deadline-nanos))

            ;; Claim a release that arrived before this waiter parked.
            (.tryReserve ^PendingBudget (.budget collector) allowance)
            (admitted! collector allowance)

            :else
            (let [bound (bound-deadline collector deadline-nanos)
                  remaining (if (zero? bound)
                              Long/MAX_VALUE
                              (- bound (System/nanoTime)))]
              ;; A preparation cutoff can be withdrawn by dispatch while this
              ;; wait is parked. A timed wake only rechecks the live predicates;
              ;; it cannot turn the old cutoff into this caller's deadline.
              (when (pos? remaining)
                (.await ^Condition (.progress collector)
                        (if (zero? bound) 50000000 remaining)
                        TimeUnit/NANOSECONDS))
              (recur))))
        (finally (.decrementAndGet waiters 0)))
      (finally (.unlock lock)))))

;; ---------------------------------------------------------------------------
;; Sealing

(defn- drop-expired!
  "Remove every queued request whose own deadline passed, not only a leading
  run, so an expired request behind a live head cannot be sealed. Called under
  collector coordination, so the queue is stable; live entries stay in their
  original order. A queued expiry never affects the active owner."
  [^Collector collector]
  (let [now (System/nanoTime)
        ^java.util.Iterator it (.iterator ^ConcurrentLinkedQueue (.ready collector))]
    (while (.hasNext it)
      (let [^Descriptor descriptor (.next it)
            d (.deadline-nanos descriptor)]
        (when (and (pos? d) (<= d now))
          (.remove it)
          (.decrementAndGet ^AtomicInteger (.queued collector))
          (deliver! descriptor [false (expired-error "collection" d)])
          (phase/phase! :queue-expired descriptor))))))

(declare preparation-cutoff! schedule-for release-batch-charges!)

(defn- take-prefix!
  "Seal the longest ready FIFO prefix that fits both batch caps.

  Walks the queue once in publication order, counting every candidate's full
  allowance until the first descriptor that would exceed either the request-count
  or byte cap. That descriptor and its followers stay queued: the selector never
  skips a queue head to fit smaller followers. Native application may collect
  more ready requests within the same caps before WAL dispatch.
  Called under collector coordination, so the ready queue is stable."
  ^Batch [^Collector collector]
  (let [^ConcurrentLinkedQueue queue (.ready collector)
        limit (long (.batch-limit collector))
        cap (long (.batch-max-bytes collector))
        taken (loop [^java.util.Iterator it (.iterator queue)
                     taken 0
                     bytes 0]
                (if (and (.hasNext it) (< (long taken) limit))
                  (let [^Descriptor descriptor (.next it)
                        next-bytes (+ (long bytes) (long (.allowance descriptor)))]
                    (if (<= next-bytes cap)
                      (recur it (inc (long taken)) next-bytes)
                      taken))
                  taken))]
    (when (pos? (long taken))
      (let [descriptors (FastList. (int limit))
            it (.iterator queue)]
        ;; Allocate all sealed storage while the requests are still queued and
        ;; caller-owned. A constructor failure cannot strand selected charges.
        (dotimes [_ (long taken)] (.add descriptors (.next it)))
        (let [batch (->Batch (.getAndIncrement ^AtomicLong (.next-id collector))
                             descriptors (object-array taken) taken
                             (preparation-cutoff! descriptors) (schedule-for taken)
                             (AtomicLong. 0) (AtomicBoolean. false)
                             (volatile! nil) collector taken)]
          (try
            (dotimes [_ (long taken)]
              (let [^Descriptor descriptor (.peek queue)]
                (.set ^AtomicBoolean (.selected? descriptor) true)
                (.poll queue)
                (.decrementAndGet ^AtomicInteger (.queued collector))))
            batch
            (catch Throwable t
              ;; Also cover an exceptional partial ownership transfer.
              (try
                (fence-under-lock! collector t)
                (finally
                  (let [error (fenced-error collector)]
                    (dotimes [idx (.size descriptors)]
                      (deliver! (.get descriptors idx) [false error])))
                  (release-batch-charges! collector batch)))
              (throw t))))))))

(defn collect-ready!
  "Add the next ready FIFO prefix while this owner applies native writes.
  Collection ends before WAL dispatch. Count/byte caps and the earliest deadline
  cover the whole growing batch, including every newly selected member."
  [^Batch batch]
  (let [^Collector collector (.collector batch)
        ^ReentrantLock lock (.lock collector)
        ^FastList descriptors (.descriptors batch)
        ^ConcurrentLinkedQueue queue (.ready collector)]
    (.lock lock)
    (try
      (when (dispatched? batch)
        (throw (IllegalStateException. "Cannot collect after WAL dispatch")))
      (check-preparation! batch)
      (drop-expired! collector)
      (let [before (.size descriptors)]
        (when-not (.isEmpty queue)
          (let [limit (.batch-limit collector)
                cap (.batch-max-bytes collector)
                initial-bytes (reduce (fn [^long total ^Descriptor d]
                                        (+ total (.allowance d)))
                                      0 descriptors)]
            (loop [bytes (long initial-bytes)]
              (when (< (.size descriptors) limit)
                (when-let [^Descriptor d (.peek queue)]
                  (let [next-bytes (+ bytes (.allowance d))]
                    (when (<= next-bytes cap)
                      ;; The carrier was allocated to the cap before selection.
                      (.add descriptors d)
                      (.set ^AtomicBoolean (.selected? d) true)
                      (.poll queue)
                      (.decrementAndGet ^AtomicInteger (.queued collector))
                      (let [deadline (.deadline-nanos d) current (.cutoffNanos batch)]
                        (when (and (pos? deadline) (or (zero? current) (< deadline current)))
                          (.setCutoffNanos batch deadline)
                          (when (pos? (.get ^AtomicLongArray (.waiters collector) 0))
                            (signal-progress-under-lock! collector))))
                      (recur next-bytes))))))))
        (- (.size descriptors) before))
      (finally (.unlock lock)))))

(defn- publish-active-batch!
  "Record the batch whose preparation cutoff observers should watch."
  [^Collector collector ^Batch batch]
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (vreset! ^clojure.lang.IDeref (.active-batch collector) batch)
      ;; Only capacity waiters need to recompute a newly published cutoff.
      (when (and (pos? (.cutoffNanos batch))
                 (pos? (.get ^AtomicLongArray (.waiters collector) 0)))
        (signal-progress-under-lock! collector))
      (finally (.unlock lock)))))

(defn- reset-active-batch!
  "Unconditionally clear the active batch slot. Callers hold the collector lock."
  [^Collector collector]
  (vreset! ^clojure.lang.IDeref (.active-batch collector) nil))

(defn- wake-successor!
  "Wake the queue head to claim a free slot. Caller holds coordination."
  [^Collector collector]
  (when-let [^Descriptor descriptor (.peek ^ConcurrentLinkedQueue (.ready collector))]
    (notify-descriptor! descriptor)))

(defn- release-slot-under-lock!
  "Retire execution ownership and notify a successor, or publish idle."
  [^Collector collector]
  (.set ^AtomicBoolean (.active collector) false)
  (reset-active-batch! collector)
  (signal-progress-under-lock! collector)
  (if (.isEmpty ^ConcurrentLinkedQueue (.ready collector))
    (phase/phase! :next-activation nil)
    (wake-successor! collector)))

(defn- claim-next-batch!
  "Under coordination: drop expired queued requests and seal the next bounded
  prefix, or release the single active slot when nothing is ready.

  Deciding idle and releasing the slot happen under one lock, so a request
  published in the gap between an empty-queue check and slot release cannot
  strand itself behind a leader that already stopped."
  ^Batch [^Collector collector ^Descriptor own]
  (let [^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (if (.get ^AtomicBoolean (.serving collector))
        (let [_ (when (pos? (long (.get ^AtomicInteger (.queued collector))))
                  (phase/phase! :selection-start nil))
              batch (do (drop-expired! collector)
                        ;; Expiring this caller must not make it execute a
                        ;; later request before returning its own rejection.
                        (when (nil? @(.result own))
                          (take-prefix! collector)))]
          (when (nil? batch)
            (release-slot-under-lock! collector))
          batch)
        (do (release-slot-under-lock! collector)
            nil))
      (catch Throwable t
        ;; Election already owns the slot, including before any batch exists.
        ;; Coordination prevents a successor from claiming it during cleanup.
        (try (fence-under-lock! collector t)
             (finally (release-slot-under-lock! collector)))
        (throw t))
      (finally (.unlock lock)))))

(defn- try-lead!
  "Claim the free active slot only for the head of the ready queue.

  Called by a waiting submitter after a leader handed the slot off, so the
  remaining queued work is led by a successor rather than stranded or delayed
  behind a caller that already has its own result. Returns true when this caller
  became the leader; it must then run `lead!`. Later publishers cannot take the
  slot while the head is waking, so their ready work can join its batch."
  [^Collector collector ^Descriptor descriptor]
  ;; An active owner or an earlier queue member will deliver our result or wake
  ;; us as successor. The concurrent queue permits this fast check; election
  ;; still rechecks under coordination. The park predicate is checked again after
  ;; any lock wait, which could consume a thread-level unpark permit.
  (when (and (not (.get ^AtomicBoolean (.active collector)))
             (identical? descriptor (.peek ^ConcurrentLinkedQueue (.ready collector))))
    (let [^ReentrantLock lock (.lock collector)]
      (.lock lock)
      (try
        (when (and (nil? @(.result descriptor))
                   (.get ^AtomicBoolean (.serving collector))
                   (identical? descriptor
                               (.peek ^ConcurrentLinkedQueue (.ready collector)))
                   (.compareAndSet ^AtomicBoolean (.active collector) false true))
          true)
        (finally (.unlock lock))))))

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

(defn- schedule-for
  "Schedule after freezing accepted writes: inline for one logical write,
  parallel for two or more when native work remains. Bodies producing no
  physical writes do not raise the weight."
  [^long weight]
  (if (<= weight 1) :inline :parallel))

;; ---------------------------------------------------------------------------
;; Execution

(defn- reject-pending!
  "Reject every request without a delivered value. Used by cancellation and
  pre-dispatch failure; earlier accepted rows and results are provisional."
  [^Batch batch value]
  (let [descriptors ^FastList (.descriptors batch)]
    (dotimes [idx (.size descriptors)]
      (deliver! ^Descriptor (.get descriptors idx) value)))
  nil)

(defn observe-cutoff!
  "Observe the live active batch's preparation cutoff.

  Applies only while the same batch is still in ordered preparation and has not
  been dispatched (see `mark-dispatched!`); once branches run, their own
  deadlines own the batch. If the cutoff has expired, reject the batch's members
  and fence the runtime with the established not-committed outcome. This never
  runs a body, closes another thread's reader or reclaims the active slot: a
  stuck owner keeps its resources until it actually exits, and its late return
  cannot publish success. Returns true when it fenced."
  [^Collector collector]
  (let [^Batch batch @(.active-batch collector)
        now (System/nanoTime)]
    (if (and batch
             (not (.get ^AtomicBoolean (.dispatched? batch)))
             (let [c (.cutoffNanos batch)] (and (pos? c) (<= c now))))
      (let [^ReentrantLock lock (.lock collector)]
        (.lock lock)
        (try
          (let [current @(.active-batch collector)
                c (when (identical? current batch) (.cutoffNanos batch))
                now (System/nanoTime)]
            (if (and (identical? current batch)
                     (.get ^AtomicBoolean (.active collector))
                     (not (.get ^AtomicBoolean (.dispatched? batch)))
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
  "Publish the prefix and results while holding collector coordination."
  [^Batch batch values]
  (let [^Collector collector (.collector batch)
        descriptors ^FastList (.descriptors batch)]
    ;; P is an absolute joined prefix, not a running sum of group lengths.
    (.accumulateAndGet ^AtomicLong (.published collector)
                       (.get ^AtomicLong (.lsn batch))
                       max-long)
    (dotimes [idx (.size descriptors)]
      (let [^Descriptor descriptor (.get descriptors idx)
            value [true (aget ^objects values idx)]]
        (if (and (first value) (:confirm! (.context descriptor)))
          ;; Both branches have finished using the prepared data. Reuse this
          ;; already charged slot for a confirmation's result; leave the caller
          ;; parked until confirmation finishes after execution handoff.
          (vreset! (.data descriptor) value)
          (publish-result! descriptor value))))
    nil))

(defn- finish-confirmations!
  "Run accepted requests' confirmations once on the completing leader, after
  handoff. The caller retains its reservation until this final result arrives.
  Callback failure preserves the established WAL outcome and never replays or
  undoes the locally joined write."
  [^Batch batch]
  (dotimes [idx (batch-count batch)]
    (let [^Descriptor descriptor (batch-at batch idx)]
      (when (nil? @(.result descriptor))
        (when-let [confirm! (:confirm! (.context descriptor))]
          (let [value @(.data descriptor)
                outcome (try
                          (confirm! (.context descriptor) (second value))
                          value
                          (catch Throwable t
                            (when (instance? InterruptedException t)
                              (.interrupt (Thread/currentThread)))
                            [false (wal-outcome-error batch t)]))]
            (deliver! descriptor outcome)))))))

(defn- join-or-reject!
  "Publish joined results, unless a terminal fence was recorded while the batch
  executed. The final decision and publication share coordination with fencing."
  [^Batch batch values]
  ;; The seam marks entry into publication, before its atomic decision. A fault
  ;; here must preserve the WAL result just like an execution failure.
  (phase/phase! :joint-publication batch)
  (let [^Collector collector (.collector batch)
        ^ReentrantLock lock (.lock collector)]
    (.lock lock)
    (try
      (if-let [failure @(.failure collector)]
        (reject-pending! batch [false (wal-outcome-error batch failure)])
        (join-batch! batch values))
      (finally
        (.unlock lock)
        ;; Returning writers can prepare and publish the next ready batch while
        ;; this owner retires. Notify outside publication coordination.
        ;; Include any published prefix when publication threw partway through.
        ;; The first member is the owner and never parks for its own result.
        (dotimes [idx (dec (batch-count batch))]
          (let [descriptor (batch-at batch (inc idx))]
            (when (some? @(.result descriptor))
              (notify-descriptor! descriptor))))))))

(defn- dispatch-batch!
  "Hand the sealed batch to the environment executor and join its results.

  Ordered state-dependent work, branch dispatch, WAL/native execution and local
  publication all belong to the executor; sealing, dispatch ordering and result
  recording belong to this collector."
  [^Batch batch]
  (let [^Collector collector (.collector batch)
        descriptors ^FastList (.descriptors batch)
        weight (long (.size descriptors))]
    (phase/phase! :ordered-work batch)
    (when (expired? batch)
      (throw (cancel-error
              (expired-error "ordered preparation" (.cutoffNanos batch)))))
    (when-not (.get ^AtomicBoolean (.serving collector))
      (throw (cancel-error (fenced-error collector))))
    (phase/phase! :schedule-selected {:schedule (.executionSchedule batch) :weight weight})
    (let [values ((.executor collector) batch)]
      (when-not (= (batch-count batch) (alength ^objects values))
        (throw (ex-info "Batch executor returned the wrong number of values"
                        {:error :txlog/batch-executor-mismatch
                         :expected (batch-count batch) :actual (alength ^objects values)})))
      (join-or-reject! batch values))))

(defn- fail-batch!
  "Reject the active batch using its own WAL result and fence further work."
  [^Batch batch cause]
  (let [error (wal-outcome-error batch cause)]
    (reject-pending! batch [false error])
    (fence! (.collector batch) error)
    :fenced))

(defn- execute-batch!
  "Run one sealed batch to joint completion, applying the phase table's
  cancellation and pre-dispatch failure rows.

  Returns `:joined`, `:cancelled` (a clean pre-dispatch cancellation), or
  `:interrupted` (the preparation owner must stop leading). An unexpected
  failure rejects the batch and fences, returning `:fenced`."
  [^Batch batch]
  (try
    (dispatch-batch! batch)
    :joined
    (catch InterruptedException t
      ;; An interrupted join cannot turn a durable write into a pre-dispatch
      ;; cancellation. The executor drains both branches before returning.
      (try
        (if (dispatched? batch)
          (fail-batch! batch t)
          (do (reject-pending! batch [false (interrupted-error)])
              :interrupted))
        (finally (.interrupt (Thread/currentThread)))))
    (catch Throwable t
      (if (and (not (dispatched? batch)) (pre-dispatch-cancel? t))
        ;; A clean pre-dispatch cancellation rejects the undispatched batch,
        ;; preserves its not-committed outcome and lets the runtime continue.
        (do (reject-pending! batch [false (ex-cause t)])
            :cancelled)
        (fail-batch! batch t)))))

(defn- release-batch-charges!
  "Drop the batch's ownership of every sealed member's retained reservation.

  The batch owns a sealed request's reservation until both branches stop, but the
  reservation stays charged until the caller has also delivered the result, so a
  paused caller cannot refund storage it still retains. Each descriptor's
  `batch-done?` CAS keeps this path idempotent for the partial-selection failure
  path, and `released?` gates the physical release."
  [^Collector collector ^Batch batch]
  (let [descriptors ^FastList (.descriptors batch)]
    (dotimes [idx (.size descriptors)]
      (let [^Descriptor descriptor (.get descriptors idx)]
        ;; The batch owns every descriptor selected before WAL dispatch.
        (when (.get ^AtomicBoolean (.selected? descriptor))
          (when (.compareAndSet ^AtomicBoolean (.batch-done? descriptor) false true)
            (when (.get ^AtomicBoolean (.caller-done? descriptor))
              (release-reservation! collector descriptor)))))))
  nil)

(defn- run-sealed-batch!
  "Execute one sealed batch, then retire its storage and ownership together."
  [^Collector collector ^Batch batch]
  (try
    (refresh-wal-bodies! batch)
    (phase/phase! :batch-sealed batch)
    (publish-active-batch! collector batch)
    (execute-batch! batch)
    (catch Throwable t
      ;; Sealed storage initialization/observation may fail before execution's
      ;; own handler is entered. Complete all selected requests and fence.
      (fail-batch! batch t))
    (finally
      ;; Both branches have stopped. One acquisition covers charge release,
      ;; retirement and handoff, rather than rejoining the fair lock queue
      ;; separately for each cleanup step.
      (let [^ReentrantLock lock (.lock collector)]
        (.lock lock)
        (try
          (try
            (release-batch-charges! collector batch)
            (phase/phase! :batch-retired batch)
            (catch Throwable t
              ;; Results already published by join cannot be revoked. Fence
              ;; further work on a cleanup failure, then relinquish ownership
              ;; now that both branches have stopped.
              (fence-under-lock! collector (wal-outcome-error batch t)))
            (finally (release-slot-under-lock! collector)))
          (finally (.unlock lock))))
      (finish-confirmations! batch))))

(defn- lead!
  "Seal and execute this caller's one batch, then return.

  Only the queue head can be elected, so every nonempty selected prefix includes
  `own`. Selection releases the slot if this caller expires or is rejected;
  otherwise batch retirement releases it. A leader never runs a later batch
  after its own result completes."
  [^Collector collector ^Descriptor own]
  (when-let [batch (claim-next-batch! collector own)]
    (run-sealed-batch! collector batch)))

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
      (when (.remove ^ConcurrentLinkedQueue (.ready collector) descriptor)
        (.decrementAndGet ^AtomicInteger (.queued collector))
        ;; An expiring head may have consumed the handoff notification. Relay
        ;; it if there is no execution owner to notify the next caller.
        (when-not (.get ^AtomicBoolean (.active collector))
          (wake-successor! collector))
        true)
      (finally (.unlock lock)))))

(defn- park-for-progress!
  "Recheck progress after every possible lock wait, immediately before parking.

  A thread-level permit can be consumed by another synchronizer. Result and
  queue-head predicates therefore retain progress even if an earlier unpark was
  consumed. No blocking operation may be inserted between these checks and park.
  Spurious or duplicate notifications simply return to result/election checks."
  [^Collector collector ^Descriptor descriptor ^long nanos]
  (when (and (nil? @(.result descriptor))
             (or (.get ^AtomicBoolean (.active collector))
                 (not (identical? descriptor
                                  (.peek ^ConcurrentLinkedQueue (.ready collector))))))
    (if (zero? nanos)
      (LockSupport/park descriptor)
      (LockSupport/parkNanos descriptor nanos))))

(defn- await-progress!
  "Wait once for a result or handoff, observing applicable deadlines."
  [^Collector collector ^Descriptor descriptor
   ^clojure.lang.IDeref result ^clojure.lang.IDeref interrupted?
   deadline-nanos]
  (try
    (if (zero? deadline-nanos)
      (park-for-progress! collector descriptor 0)
      (let [bound (bound-deadline collector deadline-nanos)
            remaining (- bound (System/nanoTime))]
        (if (pos? remaining)
          ;; Completion and handoff unpark this caller. Poll only to discover a
          ;; preparation cutoff published after it parked; waking every 100 us
          ;; burned CPU throughout WAL I/O.
          (park-for-progress! collector descriptor (min remaining 1000000))
          (do (observe-cutoff! collector)
              ;; Removal and selection share coordination. If selection won,
              ;; only the batch may decide this request's outcome.
              (when (and (nil? @result) (unlink! collector descriptor))
                (deliver! descriptor
                          [false (expired-error "execution" deadline-nanos)]))
              (park-for-progress! collector descriptor 0)))))
    (finally
      ;; park returns on interruption without clearing it. Clear and remember
      ;; it before this waiter can take leadership; restore it on submit return.
      (when (Thread/interrupted)
        (vreset! interrupted? true)))))

(defn- await-result!
  "Wait for this request's own result slot.

  Interruption of a waiting submitter never cancels its request or interrupts
  the leader: the signal is remembered, the deadline-bounded wait continues, and
  the status is restored on return. If the deadline passes while the request is
  still merely queued, it is definitively rejected; a selected request keeps its
  execution deadline with its elected owner."
  [^Collector collector ^Descriptor descriptor ^long deadline-nanos]
  (let [result (.result descriptor)
        interrupted? (volatile! false)]
    (try
      (loop []
        (when (nil? @result)
          ;; A handoff may arrive before the caller's first wait. Remember an
          ;; already pending waiter interruption before it becomes the owner.
          (when (Thread/interrupted)
            (vreset! interrupted? true))
          ;; If a previous leader handed the slot off, take over and lead the
          ;; remaining queued work instead of only waiting for its result.
          (if (try-lead! collector descriptor)
            (lead! collector descriptor)
            (await-progress! collector descriptor result interrupted?
                             deadline-nanos))
          (recur)))
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
    ;; Ready publishers may take a free lock immediately. Selection uses fair
    ;; acquisition, so it cannot cut ahead of publishers already waiting. This
    ;; lets ready work accumulate without making every publisher pay a handoff.
    (when-not (.tryLock lock) (.lock lock))
    (try
      (when-not (.get ^AtomicBoolean (.serving collector))
        (throw (fenced-error collector)))
      (.add ^ConcurrentLinkedQueue (.ready collector) descriptor)
      (.incrementAndGet ^AtomicInteger (.queued collector))
      ;; Emit at the enqueue point, under the same coordination that fixes FIFO,
      ;; so a trace observer records the real publication order.
      (phase/phase! :ready-published descriptor)
      ;; The queue head owns the next election. A returning writer may publish
      ;; before a woken head runs, but cannot seal a smaller batch ahead of it.
      (and (identical? descriptor
                       (.peek ^ConcurrentLinkedQueue (.ready collector)))
           (.compareAndSet ^AtomicBoolean (.active collector) false true))
      (finally (.unlock lock)))))

(defn submit!
  "Admit, prepare, publish and complete one write request.

  `request` is a map of `:allowance` (required bytes, declared before the caller
  allocates anything), optional `:prepare` (fn [descriptor] returning owned
  prepared data on the caller after admission),
  optional `:op`/`:data`/`:context` (ordered body inputs, run on the execution
  thread) and optional `:timeout-ms`. Returns this request's own value, or
  throws its own failure. Preparation alone grants no batch membership, LSN,
  state visibility or permission to start I/O. Blind requests precharge their
  full allowance; body-based requests reserve the configured
  `:wal-rmw-max-bytes` allowance for captured writes when they declare no
  allowance of their own, and start with the control-bundle charge."
  [^Collector collector {:keys [allowance prepare op data context timeout-ms]}]
  (let [;; A body-based request produces writes whose encoding this engine
        ;; cannot know before the body runs, so it reserves the configured
        ;; allowance up front. An explicit allowance stays authoritative.
        allowance (long (or allowance
                           (when op (.rmw-allowance collector))
                           charge/request-control-bundle))
        prep-timeout-ms (long (.preparation-timeout-ms collector))
        ;; Admission and caller preparation are bounded from submission even
        ;; when the caller supplies no `:timeout-ms`; an explicit request
        ;; timeout only tightens that bound.
        effective-timeout-ms (if timeout-ms
                               (min (long timeout-ms) prep-timeout-ms)
                               prep-timeout-ms)
        deadline (+ (System/nanoTime) (long (* 1000000 effective-timeout-ms)))
        batch-max-bytes (.batch-max-bytes collector)]
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
    (let [^Descriptor descriptor
          (try
            (Descriptor. op (volatile! data) context allowance deadline
                         (AtomicLong. (if op charge/request-control-bundle allowance))
                         (AtomicBoolean. false) (volatile! nil) (Thread/currentThread)
                         (AtomicBoolean. false) (AtomicBoolean. false)
                         (AtomicBoolean. false) (AtomicBoolean. false))
            (catch Throwable t
              (release-allowance! collector allowance)
              (throw t)))]
      (try
        (when prepare
          (phase/phase! :caller-preparation descriptor)
          (vreset! (.data descriptor) (prepare descriptor))
          (phase/phase! :caller-prepared descriptor))
        (when-let [check! (.check-prepared! collector)]
          (check! descriptor))
        ;; An interrupt observed after preparation rejects only this request. If
        ;; this thread published or led while interrupted, the WAL channel's
        ;; interruptible I/O would close the channel and fence the whole
        ;; environment instead.
        (when (.isInterrupted (Thread/currentThread))
          (throw (interrupted-error)))
        (when (publish-and-elect! collector descriptor)
          (lead! collector descriptor))
        (await-result! collector descriptor deadline)
        (finally
          ;; Sealed storage belongs to batch cleanup; unselected storage to caller.
          (release-descriptor! collector descriptor))))))

(defn admitted-usage
  "Diagnostic snapshot used by the accounting oracle."
  [^Collector collector]
  (usage collector))
