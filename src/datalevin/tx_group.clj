;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group
  "Collect synchronous writes before acquiring the native writer. A group is
  executed in one native transaction and acknowledged after native commit and
  the configured WAL durability policy and commit confirmations."
  (:refer-clojure :exclude [run!])
  (:import [java.util.concurrent ConcurrentLinkedQueue Semaphore]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent.locks LockSupport ReentrantLock]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *enabled?* true)
(def ^:dynamic *batched?*
  "Whether the transaction runner is executing queued requests."
  false)

(def ^:dynamic *request-count*
  "Logical requests in the current physical transaction, for WAL sync counts.
  Bound around run-transaction so it covers commit after the request bodies
  restore their submitting threads' bindings."
  1)

(deftype Request [op result ^Semaphore ready])
(deftype Group [^ReentrantLock lock ^ConcurrentLinkedQueue queue ^long limit
                ^AtomicBoolean active])
(deftype Committed [value confirmation])

(def ^:dynamic ^:private *confirmations* nil)

(defn confirm-after-commit!
  "Register confirmation of an already committed write. Transaction runners
  collect these callbacks and wait after releasing execution resources. Outside
  a runner, retain synchronous confirmation. Failures must never retry a write."
  [f]
  (if *confirmations*
    (vswap! *confirmations* conj f)
    (f)))

(defn- capture-commit
  [f]
  (let [confirmations (volatile! nil)
        value (binding [*confirmations* confirmations] (f))]
    (if-let [callbacks @confirmations]
      ;; Delay shares both success and failure among all callers in a physical
      ;; group. No executor or background task can outlive the waiting callers.
      (Committed. value (delay (doseq [confirm (reverse callbacks)] (confirm))))
      value)))

(defn- await-commit
  [value]
  (if (instance? Committed value)
    (let [^Committed committed value]
      @(.-confirmation committed)
      (.-value committed))
    value))

(defn with-confirmation
  "Run f, then confirm its commits after f has released its resources. Nested
  runners leave confirmation to the outer owner. f must not acknowledge writes."
  [f]
  (if *confirmations*
    (f)
    (await-commit (capture-commit f))))

(defn create
  "Create one bounded-batch admission queue for a store and execution path."
  [limit]
  (Group. (ReentrantLock. true) (ConcurrentLinkedQueue.) (max 1 (long limit))
          (AtomicBoolean. false)))

(defn execute
  "Evaluate a group's requests against a private writing context. Tag only
  body failures as retryable: commit/flush failures must never retry the group."
  [^FastList requests context]
  (let [n (.size requests)
        results (object-array n)]
    (dotimes [idx n]
      (let [^Request request (.get requests idx)]
        (try
          (aset results idx ((.-op request) context))
          (catch Throwable t
            (throw (ex-info "Grouped transaction body failed"
                            (assoc (ex-data t) ::body-failure true) t))))))
    results))

(defn- complete!
  [^FastList requests results error]
  (let [confirmation (when (instance? Committed results)
                       (.-confirmation ^Committed results))
        results (if confirmation (.-value ^Committed results) results)]
    (dotimes [idx (.size requests)]
      (let [^Request request (.get requests idx)]
        (vreset! (.-result request)
                 (if error [false error]
                     [true (aget ^objects results idx) confirmation]))
        (.release ^Semaphore (.-ready request))))))

(defn- run!
  [run-transaction ^FastList requests]
  (try
    (complete! requests
               (binding [*batched?* true
                         *request-count* (.size requests)]
                 (capture-commit #(run-transaction (fn [ctx] (execute requests ctx)))))
               nil)
    (catch Throwable t
      (if (::body-failure (ex-data t))
        (if (= 1 (.size requests))
          (complete! requests nil (ex-cause t))
          ;; The outer transaction has aborted. Isolate an invalid request so
          ;; it cannot fail unrelated callers. User update functions already
          ;; have the same side-effect-free requirement as map-resize retries.
          (dotimes [idx (.size requests)]
            (run! run-transaction (doto (FastList. 1) (.add (.get requests idx))))))
        (complete! requests nil t)))))

(defn- handoff!
  [^Group group]
  (let [^ConcurrentLinkedQueue queue (.-queue group)
        ^AtomicBoolean active (.-active group)]
    (if-let [^Request request (.peek queue)]
      ;; Keep leadership reserved while waking exactly one queued caller.
      (.release ^Semaphore (.-ready request))
      (do
        (.set active false)
        ;; An enqueue may have raced with relinquishing leadership. Either
        ;; its submitter claims the idle group, or we wake its next leader.
        (when (and (not (.isEmpty queue))
                   (.compareAndSet active false true))
          (.release ^Semaphore (.-ready ^Request (.peek queue))))))))

(defn- lead!
  [^Group group run-transaction result]
  (let [^ConcurrentLinkedQueue queue (.-queue group)
        ^ReentrantLock lock (.-lock group)]
    (try
      ;; A delayed submitter can claim an idle group after another leader has
      ;; already completed its request. It only needs to pass leadership on.
      (when-not @result
        (.lock lock)
        (try
          (loop []
            (when-not @result
              (let [requests (FastList.)]
                (loop [n 0]
                  (when (< n (.-limit group))
                    (when-let [request (.poll queue)]
                      (.add requests request)
                      (recur (inc n)))))
                (run! run-transaction requests)
                (recur))))
          (finally (.unlock lock))))
      (finally (handoff! group)))))

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

(defn- submit-queued!
  [^Group group run-transaction op leader? delay-nanos]
  (let [result (volatile! nil)
        ready (Semaphore. 0)
        request (Request. (bound-fn [context] (op context)) result ready)]
    (.add ^ConcurrentLinkedQueue (.-queue group) request)
    (if (or leader? (.compareAndSet ^AtomicBoolean (.-active group) false true))
      (do
        (when (pos? (long delay-nanos))
          (collect-idle-batch! group (long delay-nanos)))
        (lead! group run-transaction result))
      (do
        ;; Like ReentrantLock.lock, waiting does not cancel an enqueued write
        ;; on interruption, and preserves the caller's interrupted status.
        (.acquireUninterruptibly ready)
        (when-not @result
          (lead! group run-transaction result))))
    (let [[ok? value confirmation] @result]
      (if ok?
        (do (when confirmation @confirmation) value)
        (throw ^Throwable value)))))

(defn submit!
  "Run op in a transaction under its durability policy, batching contended callers.
  run-transaction receives a function of the private writing context, returns
  its result, and owns commit and state publication. Without a collection window,
  an idle caller passes op directly, without queue or batch allocations. The
  optional delay-nanos bounds collection before the writer is acquired. Queued
  operations capture the submitting thread's bindings and wait for completion
  or a leadership handoff."
  ([^Group group run-transaction op]
   (if (.compareAndSet ^AtomicBoolean (.-active group) false true)
     (let [^ReentrantLock lock (.-lock group)]
       ;; Claim leadership before checking the queue, so an already enqueued
       ;; request cannot be overtaken by a new direct operation.
       (if (and (.isEmpty ^ConcurrentLinkedQueue (.-queue group))
                (.tryLock lock))
         (await-commit
           (try
             (try
               (capture-commit
                 #(if (and (= 1 *request-count*) (not *batched?*))
                    (run-transaction op)
                    (binding [*request-count* 1
                              *batched?* false]
                      (run-transaction op))))
               (finally (.unlock lock)))
             (finally (handoff! group))))
         (submit-queued! group run-transaction op true 0)))
     (submit-queued! group run-transaction op false 0)))
  ([^Group group run-transaction op delay-nanos]
   (if (and (pos? (long delay-nanos)) (> (.-limit group) 1))
     (let [leader? (.compareAndSet ^AtomicBoolean (.-active group) false true)]
       (submit-queued! group run-transaction op leader?
                       (if leader? delay-nanos 0)))
     (submit! group run-transaction op))))
