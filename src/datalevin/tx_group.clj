;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group
  "Collect synchronous writes with backend-owned execution and completion.
  Native runners collect under their writer. WAL runners can hand off collection
  after append and complete durability and application outside admission."
  (:refer-clojure :exclude [run!])
  (:import [java.util.concurrent ConcurrentLinkedQueue Semaphore TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent.locks LockSupport ReentrantLock]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftype Request [op result ^Semaphore ready data context leadership])
(deftype Group [^ReentrantLock lock ^ConcurrentLinkedQueue queue ^long limit
                ^AtomicBoolean active])
(deftype Committed [value confirmation])
(deftype Deferred [completion])
(deftype Receipt [value complete])
(deftype Rejected [error])

(defprotocol IExecution
  (collect! [this] [this seal?])
  (collect-next! [this])
  (request-count [this])
  (observe-collection! [this observer])
  (requests [this])
  (current-requests [this])
  (initial-request [this])
  (specialization-context [this])
  (collection-group [this]))

(declare execute)

(deftype Execution [^FastList batch context observer collect sealed? ^Group group]
  IExecution
  (collect! [_] (collect false))
  (collect! [_ seal?] (collect seal?))
  (collect-next! [_]
    (when (and group (not @sealed?) (< (.size batch) (.-limit group)))
      (when-let [request (.poll ^ConcurrentLinkedQueue (.-queue group))]
        (.add batch request)))
    (when-let [f @observer] (f (.size batch))))
  (request-count [_] (.size batch))
  (observe-collection! [_ f] (vreset! observer f))
  (requests [_] batch)
  (current-requests [_] batch)
  (initial-request [_] nil)
  (specialization-context [_] context)
  (collection-group [_] group)
  clojure.lang.IFn
  (invoke [_ ctx]
    (collect false)
    (try (execute batch ctx collect)
         (finally (vreset! sealed? true)))))

(deftype SingleExecution [op data request-context context ^Group group
                          ^:volatile-mutable ^FastList batch
                          ^:volatile-mutable ^Request initial
                          ^:volatile-mutable observer
                          ^:volatile-mutable sealed?]
  IExecution
  (collect! [this] (collect! this false))
  (collect! [this seal?]
    (when-not sealed?
      (if seal?
        (set! sealed? true)
        (let [^ConcurrentLinkedQueue queue (.-queue group)]
          (loop []
            (when (< (long (request-count this)) (.-limit group))
              (when-let [request (.poll queue)]
                (.add ^FastList (requests this) request)
                (recur))))))
    (when observer (observer (request-count this)))))
  (request-count [_] (if batch (.size batch) 1))
  (collect-next! [this]
    (when (and (not sealed?) (< (long (request-count this)) (.-limit group)))
      (when-let [request (.poll ^ConcurrentLinkedQueue (.-queue group))]
        (.add ^FastList (requests this) request)))
    (when observer (observer (request-count this))))
  (observe-collection! [_ f] (set! observer f))
  (requests [_]
    (or batch
        (let [request (Request. op (volatile! nil) nil data request-context nil)
              requests (doto (FastList. 1) (.add request))]
          (set! initial request)
          (set! batch requests)
          requests)))
  (current-requests [_] batch)
  (initial-request [_] initial)
  (specialization-context [_] context)
  (collection-group [_] group)
  clojure.lang.IFn
  (invoke [this ctx]
    (collect! this false)
    (try
      (if batch
        (execute batch ctx #(collect! this %))
        (let [value (try (op ctx)
                         (catch Throwable t
                           (throw (ex-info "Grouped transaction body failed"
                                           (assoc (ex-data t) ::body-failure true)
                                           t))))]
          ;; Preserve collect-during-execution: a follower may join while the
          ;; first body runs. Only then materialize the normal batch objects.
          (collect! this false)
          (if batch
            (let [results (FastList. (.size batch))]
              (.add results value)
              (loop [idx 1]
                (if (< idx (.size batch))
                  (let [^Request request (.get batch idx)]
                    (.add results
                          (try ((.-op request) ctx)
                               (catch Throwable t
                                 (throw (ex-info "Grouped transaction body failed"
                                                 (assoc (ex-data t) ::body-failure true)
                                                 t)))))
                    (when (= (inc idx) (.size batch)) (collect! this false))
                    (recur (inc idx)))
                  (.toArray results))))
            (let [values (object-array 1)]
              (aset values 0 value)
              values))))
      (finally (set! sealed? true)))))

(defn committed
  "Attach an explicit, once-only confirmation after physical commit. Native
  groups may share this result; WAL callers attach confirmations per receipt."
  [value confirm]
  (Committed. value (delay (confirm))))

(defn await-commit
  [value]
  (cond
    (instance? Committed value)
    ;; A WAL request confirms only after its own durability/application wait.
    (let [^Committed committed value
          value (await-commit (.-value committed))]
      @(.-confirmation committed)
      value)
    (instance? Receipt value)
    (let [^Receipt receipt value]
      (await-commit ((.-complete receipt)))
      (.-value receipt))
    (instance? Rejected value) (throw ^Throwable (.-error ^Rejected value))
    :else value))

(defn receipt
  "Deliver one appended request's value after complete has established its
  durability, application and confirmation. Each submitting caller invokes its
  own completion outside collector leadership; there is no shared batch delay.
  Readiness is signalled after collector release, so no release barrier is needed.
  All completion context must be passed explicitly or closed over by complete."
  [_execute value complete]
  (Receipt. value complete))

(defn rejected
  "Return a request-specific failure without failing or retrying its group."
  [error]
  (Rejected. error))

(defn defer-completion
  "Return a runner result completed after releasing collector leadership.
  f returns the ordered result array for the already fixed batch. It runs once,
  on a waiting caller, using explicit context, and owns durability/application.
  Failures here are final: request bodies must never be replayed after append."
  [_execute f]
  (Deferred. (delay (await-commit (f)))))

(defn create
  "Create one bounded-batch admission queue for a store and execution path."
  [limit]
  (Group. (ReentrantLock. true) (ConcurrentLinkedQueue.) (max 1 (long limit))
          (AtomicBoolean. false)))

(defn execute
  "Evaluate a group's requests against a private writing context. Tag only
  body failures as retryable: commit/flush failures must never retry the group."
  ([requests context] (execute requests context identity))
  ([^FastList requests context collect]
   (let [results (FastList. (.size requests))]
     (loop [idx 0]
       (if (< idx (.size requests))
         (let [^Request request (.get requests idx)]
           (.add results
                 (try ((.-op request) context)
                      (catch Throwable t
                        (throw (ex-info "Grouped transaction body failed"
                                        (assoc (ex-data t) ::body-failure true) t)))))
           ;; Requests arriving while this writer was working can share its
           ;; commit. An empty queue or the batch limit ends collection.
           (when (= (inc idx) (.size requests)) (collect false))
           (recur (inc idx)))
         (.toArray results))))))

(defn batch-data
  "Return operation metadata (::data) for a specialized batch runner, only when
  every request has the runner's specialization context. Otherwise execute
  its operation normally. A specialized batch is sealed before execution."
  [execute]
  (collect! execute)
  ;; A lone request uses the general writer so arrivals during its execution
  ;; can join it. Do not materialize its request list just to reject the
  ;; specialized batch path.
  (when (> (long (request-count execute)) 1)
    (let [^FastList requests (requests execute)
          context (specialization-context execute)
          data (FastList. (.size requests))]
      (loop [idx 0]
        (if (= idx (.size requests))
          (do (collect! execute true) data)
          (let [^Request request (.get requests idx)]
            (when (and (some? (.-data request))
                       (= context (.-context request)))
              (.add data (.-data request))
              (recur (inc idx)))))))))

(defn ^:redef preparation-nano-time
  "Monotonic clock seam for the bounded WAL preparation window."
  ^long []
  (System/nanoTime))

(defn prepare-requests
  "Prepare bounded WAL requests independently, collecting arrivals until the
  current preparation ends. Seal before returning to the insertion runner.
  f receives an operation and its optional specialization data, and returns
  a per-request receipt (or rejection). Bodies after append are never retried.
  A positive max-nanos bounds collection between bodies; it never preempts a
  body or waits to fill a batch. Poll one request at a time so an expired window
  leaves the suffix in the existing queue for the next leader. Native runners
  retain execute's collect-during-execution behavior. Zero uses the count limit."
  ([execute f] (prepare-requests execute f 0))
  ([execute f max-nanos]
   (let [max-nanos (long max-nanos)
         bounded? (pos? max-nanos)
         started (if bounded? (preparation-nano-time) 0)]
     (when-not bounded? (collect! execute))
     (let [^FastList requests (requests execute)
           results (FastList. (.size requests))]
       (try
         (loop [idx 0]
           (when (< idx (.size requests))
             (let [^Request request (.get requests idx)]
               (.add results
                     (try (f (.-op request) (.-data request))
                          (catch Throwable t (rejected t)))))
             (when (= (inc idx) (.size requests))
               (if bounded?
                 (when (< (- (preparation-nano-time) started) max-nanos)
                   (collect-next! execute))
                 (collect! execute)))
             (recur (inc idx))))
         (.toArray results)
         (finally (collect! execute true)))))))

(defn collect-submissions
  "Collect queued requests under the caller's collector leadership and return a
  vector of (f op data) descriptors for requests added at or after from-index.
  Membership is not sealed; the caller seals once it stops accepting arrivals."
  ([execute f] (collect-submissions execute f 0))
  ([execute f from]
   (collect! execute)
   (let [^FastList requests (requests execute)
         start (long from)
         results (java.util.ArrayList.)]
     (loop [idx start]
       (when (< idx (.size requests))
         (let [^Request request (.get requests idx)]
           (.add results
                 (try (f (.-op request) (.-data request))
                      (catch Throwable t (rejected t)))))
         (recur (inc idx))))
     (vec results))))

(defn seal!
  "Fix the current group's membership. No later collect can extend it."
  [execute]
  (collect! execute true))

(defn- complete!
  [^FastList requests results error]
  (let [confirmation (when (instance? Committed results)
                       (.-confirmation ^Committed results))
        results (if confirmation (.-value ^Committed results) results)
        deferred? (instance? Deferred results)
        completion (if deferred?
                     (let [completion (.-completion ^Deferred results)]
                       (if confirmation
                         (delay (let [values @completion]
                                  @confirmation
                                  values))
                         completion))
                     confirmation)]
    (dotimes [idx (.size requests)]
      (let [^Request request (.get requests idx)]
        (vreset! (.-result request)
                 (cond error [false error]
                       deferred? [true idx completion true]
                       :else [true (aget ^objects results idx) completion]))))))

(defn- notify-ready!
  "Publish readiness only after the producing collector has released its lock."
  [^FastList requests]
  (when requests
    (dotimes [idx (.size requests)]
      (let [^Request request (.get requests idx)]
        (when-let [ready (.-ready request)] (.release ^Semaphore ready))))))

(defn- run!
  [run-transaction ^FastList requests group]
  (try
    (let [sealed? (volatile! false)
          observer (volatile! nil)
          collect (fn [seal?]
                    (when-not @sealed?
                      (when seal? (vreset! sealed? true))
                      ;; Sealing fixes the requests already inspected by a
                      ;; specialized runner; it must not append more work.
                      (when (and (not seal?) group)
                        (let [^Group group group
                              ^ConcurrentLinkedQueue queue (.-queue group)]
                          (loop []
                            (when (< (.size requests) (.-limit group))
                              (when-let [request (.poll queue)]
                                (.add requests request)
                                (recur)))))))
                    (when-let [f @observer] (f (.size requests))))
          execute (Execution. requests (::context (meta run-transaction))
                              observer collect sealed? group)]
      (complete! requests (run-transaction execute) nil))
    (catch Throwable t
      (if (::body-failure (ex-data t))
        (if (= 1 (.size requests))
          (complete! requests nil (ex-cause t))
          ;; The outer transaction has aborted. Isolate an invalid request so
          ;; it cannot fail unrelated callers. User update functions already
          ;; have the same side-effect-free requirement as map-resize retries.
          (dotimes [idx (.size requests)]
            (run! run-transaction (doto (FastList. 1) (.add (.get requests idx)))
                  nil)))
        (complete! requests nil t)))))

(defn- handoff!
  [^Group group]
  ;; The outgoing leader still owns group.lock. Keep selecting a successor
  ;; atomic with helpers that may drain the queue. The caller signals the
  ;; selected successor only after unlocking; handoff itself never wakes it.
  (let [^ConcurrentLinkedQueue queue (.-queue group)
        ^AtomicBoolean active (.-active group)]
    (loop []
      (if-let [^Request request (.peek queue)]
        (do (vreset! (.-leadership request) true)
            request)
        (do
          (.set active false)
          (when (and (not (.isEmpty queue))
                     (.compareAndSet active false true))
            (recur)))))))

(defn- release-leadership!
  [^Group group]
  (let [successor (try (handoff! group)
                       (finally (.unlock ^ReentrantLock (.-lock group))))]
    (when successor (.release ^Semaphore (.-ready ^Request successor)))))

(defn release-collection!
  "Unlock a collector the caller leads without handing off, so requests arriving
  while the caller prepares outside the lock queue for this same group. Must be
  paired with acquire-collection! on the same thread before the runner returns.
  The group's active flag stays set, so no successor forms in the meantime."
  [execute]
  (when-let [^Group group (collection-group execute)]
    (.unlock ^ReentrantLock (.-lock group))))

(defn acquire-collection!
  "Re-acquire collector leadership released by release-collection! on the same
  thread."
  [execute]
  (when-let [^Group group (collection-group execute)]
    (.lock ^ReentrantLock (.-lock group))))

(defn drain!
  "Help one bounded queued collector group before a caller's operation.
  Await an active collector only within the supplied deadline."
  [^Group group run-transaction deadline-ns]
  (let [^ConcurrentLinkedQueue queue (.-queue group)
        ^ReentrantLock lock (.-lock group)]
    (when (and (or (.isLocked lock) (not (.isEmpty queue)))
               (.tryLock lock (max 0 (- (long deadline-ns) (System/nanoTime)))
                         TimeUnit/NANOSECONDS))
      (let [requests (FastList.)]
        (try
          (when-let [request (.poll queue)]
            (.add requests request)
            (run! run-transaction requests group))
          (finally
            (.unlock lock)
            (notify-ready! requests)))))))

(defn- lead!
  [^Group group run-transaction result initial]
  (let [^ConcurrentLinkedQueue queue (.-queue group)
        ^ReentrantLock lock (.-lock group)
        completed (FastList.)]
    (.lock lock)
    (try
      ;; A helper may already have completed this reserved leader's request.
      ;; It still owes handoff, but must never execute that request again.
      (when-not @result
        (when initial
          (let [requests (doto (FastList. 1) (.add initial))]
            (.add completed requests)
            (run! run-transaction requests group)))
        (loop []
          (when-not @result
            (let [requests (doto (FastList.) (.add (.poll queue)))]
              (.add completed requests)
              (run! run-transaction requests group)
              ;; An enqueue/CAS race can elect a leader behind an older
              ;; request. Publish each finished prefix before this leader
              ;; prepares another group to reach its own request. Retain
              ;; leadership across the unlock, but never readiness waiters.
              (when-not @result
                (.unlock lock)
                (try
                  (notify-ready! requests)
                  (finally
                    (.clear completed)
                    (.lock lock))))
              (recur)))))
      (finally
        (try (release-leadership! group)
             (finally
               (dotimes [idx (.size completed)]
                 (notify-ready! (.get completed idx)))))))))

(defn- collect-idle-batch!
  [^Group group ^long delay-nanos]
  (let [started (System/nanoTime)
        interrupted? (volatile! (Thread/interrupted))]
    (try
      (loop []
        (let [remaining (- delay-nanos (- (System/nanoTime) started))]
          (when (and (pos? remaining)
                     (< (.size ^ConcurrentLinkedQueue (.-queue group))
                        (.-limit group)))
            ;; Keep the native writer free while collecting an idle burst.
            ;; Poll briefly so a full batch need not wait for the deadline.
            (LockSupport/parkNanos (min remaining 50000))
            (when (Thread/interrupted) (vreset! interrupted? true))
            (recur))))
      (finally
        (when @interrupted? (.interrupt (Thread/currentThread)))))))

(defn- result!
  [result]
  (let [[ok? value completion deferred?] @result]
    (if ok?
      (let [completed (when completion @completion)]
        (await-commit
         (if deferred? (aget ^objects completed (int value)) value)))
      (throw ^Throwable value))))

(defn- single-result!
  [result]
  (let [confirmation (when (instance? Committed result)
                       (.-confirmation ^Committed result))
        result (if confirmation (.-value ^Committed result) result)
        values (if (instance? Deferred result)
                 @(.-completion ^Deferred result)
                 result)]
    (when confirmation @confirmation)
    (await-commit (aget ^objects values 0))))

(defn- lead-single!
  [^Group group run-transaction op before-execute!]
  ;; Run caller-side preparation before taking the group lock. A two-phase
  ;; caller has already published its token; this produces its payload.
  (when before-execute! (before-execute!))
  (let [execution (SingleExecution. op (::data (meta op))
                                    (::context (meta op))
                                    (::context (meta run-transaction))
                                    group nil nil nil false)
        ^ReentrantLock lock (.-lock group)]
    (.lock lock)
    (let [result
          (try
            (try
              (let [result (run-transaction execution)]
                (when-let [batch (current-requests execution)]
                  (complete! batch result nil))
                result)
              (catch Throwable t
                (if-let [^FastList batch (current-requests execution)]
                  (do
                    (if (::body-failure (ex-data t))
                      (if (= 1 (.size batch))
                        (complete! batch nil (ex-cause t))
                        (dotimes [idx (.size batch)]
                          (run! run-transaction
                                (doto (FastList. 1) (.add (.get batch idx)))
                                nil)))
                      (complete! batch nil t))
                    nil)
                  (throw (if (::body-failure (ex-data t)) (ex-cause t) t)))))
            (finally
              (try (release-leadership! group)
                   (finally
                     (notify-ready! (current-requests execution))))))]
      (if-let [initial (initial-request execution)]
        (result! (.-result ^Request initial))
        (single-result! result)))))

(defn- submit-queued!
  ([^Group group run-transaction op leader? delay-nanos]
   (submit-queued! group run-transaction op leader? delay-nanos nil))
  ([^Group group run-transaction op leader? delay-nanos before-execute!]
   (let [result (volatile! nil)
         ready (Semaphore. 0)
         data (::data (meta op))
         request (Request. op result ready data (::context (meta op)) (volatile! false))]
     (.add ^ConcurrentLinkedQueue (.-queue group) request)
     ;; The token is visible before caller-side preparation runs, so a foreign
     ;; leader can collect it and wait for the payload while this caller encodes.
     (when before-execute! (before-execute!))
     (if (or leader? (.compareAndSet ^AtomicBoolean (.-active group) false true))
       (do
         (when (pos? (long delay-nanos))
           (collect-idle-batch! group (long delay-nanos)))
         (lead! group run-transaction result nil))
       (do
         ;; Like ReentrantLock.lock, waiting does not cancel an enqueued write
         ;; on interruption, and preserves the caller's interrupted status.
         (.acquireUninterruptibly ready)
         ;; A force owner may complete the reserved leader's request while
         ;; helping collection. That caller still owes leadership handoff.
         (when @(.-leadership request)
           (lead! group run-transaction result nil))))
     (result! result))))

(defn submit!
  "Run op in a transaction under its durability policy, batching waiting callers.
  run-transaction receives a function of the private writing context, returns
  its result, and owns commit and state publication. It may return
  defer-completion to finish a fixed batch outside collector leadership.
  Every caller enters once; an idle caller skips the queue and ready semaphore.
  The backend chooses its collection point. Contending callers
  wait for completion or leadership handoff. The optional delay-nanos adds a
  bounded idle collection window before backend execution."
  ([^Group group run-transaction op]
   (submit! group run-transaction op 0 nil))
  ([^Group group run-transaction op delay-nanos]
   (submit! group run-transaction op delay-nanos nil))
  ([^Group group run-transaction op delay-nanos before-execute!]
   (if (and (pos? (long delay-nanos)) (> (.-limit group) 1))
     (let [leader? (.compareAndSet ^AtomicBoolean (.-active group) false true)]
       (submit-queued! group run-transaction op leader?
                       (if leader? delay-nanos 0) before-execute!))
     (if (and (not (.isLocked ^ReentrantLock (.-lock group)))
              (.isEmpty ^ConcurrentLinkedQueue (.-queue group))
              (.compareAndSet ^AtomicBoolean (.-active group) false true))
       (lead-single! group run-transaction op before-execute!)
       (submit-queued! group run-transaction op false 0 before-execute!)))))

(defn submit-adaptive!
  "Use the supplied operation and runner when this caller can execute its own
  request. Build the queued pair only after admission chooses the queue and this
  caller loses the leadership race, so adapters defer expensive cross-thread
  context capture until execution is actually foreign. queued-pair returns
  [run-transaction op] on the submitting thread."
  ([^Group group run-transaction op queued-pair delay-nanos]
   (submit-adaptive! group run-transaction op queued-pair delay-nanos nil))
  ([^Group group run-transaction op queued-pair delay-nanos before-execute!]
   (if (and (pos? (long delay-nanos)) (> (.-limit group) 1))
     ;; This caller intends to collect an idle window, so it leads directly
     ;; unless another leader already owns the group.
     (if (.compareAndSet ^AtomicBoolean (.-active group) false true)
       (submit-queued! group run-transaction op true delay-nanos before-execute!)
       (let [[queued-runner queued-op] (queued-pair)]
         (submit-queued! group queued-runner queued-op false 0 before-execute!)))
     (if (and (not (.isLocked ^ReentrantLock (.-lock group)))
              (.isEmpty ^ConcurrentLinkedQueue (.-queue group))
              (.compareAndSet ^AtomicBoolean (.-active group) false true))
       (lead-single! group run-transaction op before-execute!)
       ;; The idle fast path lost. If no leader is active after all (a drained
       ;; queue with no handoff, or a just-released leader), lead here without
       ;; capturing; otherwise the request may run on a foreign leader.
       (if (.compareAndSet ^AtomicBoolean (.-active group) false true)
         (submit-queued! group run-transaction op true 0 before-execute!)
         (let [[queued-runner queued-op] (queued-pair)]
           (submit-queued! group queued-runner queued-op false 0 before-execute!)))))))
