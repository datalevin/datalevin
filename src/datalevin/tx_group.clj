;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group
  "Collect synchronous writes before acquiring the native writer. A group is
  executed in one native transaction and acknowledged after native commit and
  the configured WAL durability policy."
  (:refer-clojure :exclude [run!])
  (:import [java.util.concurrent ConcurrentLinkedQueue Semaphore]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent.locks ReentrantLock]
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
  (dotimes [idx (.size requests)]
    (let [^Request request (.get requests idx)]
      (vreset! (.-result request)
               (if error [false error] [true (aget ^objects results idx)]))
      (.release ^Semaphore (.-ready request)))))

(defn- run!
  [run-transaction ^FastList requests]
  (try
    (complete! requests
               (binding [*batched?* true
                         *request-count* (.size requests)]
                 (run-transaction #(execute requests %)))
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

(defn- submit-queued!
  [^Group group run-transaction op leader?]
  (let [result (volatile! nil)
        ready (Semaphore. 0)
        request (Request. (bound-fn [context] (op context)) result ready)]
    (.add ^ConcurrentLinkedQueue (.-queue group) request)
    (if (or leader? (.compareAndSet ^AtomicBoolean (.-active group) false true))
      (lead! group run-transaction result)
      (do
        ;; Like ReentrantLock.lock, waiting does not cancel an enqueued write
        ;; on interruption, and preserves the caller's interrupted status.
        (.acquireUninterruptibly ready)
        (when-not @result
          (lead! group run-transaction result))))
    (let [[ok? value] @result]
      (if ok? value (throw ^Throwable value)))))

(defn submit!
  "Run op in a transaction under its durability policy, batching contended callers.
  run-transaction receives a function of the private writing context, returns
  its result, and owns commit and state publication. An idle caller passes op
  directly, without queue or batch allocations. Queued operations capture the
  submitting thread's bindings and wait for completion or a leadership handoff."
  [^Group group run-transaction op]
  (if (.compareAndSet ^AtomicBoolean (.-active group) false true)
    (let [^ReentrantLock lock (.-lock group)]
      ;; Claim leadership before checking the queue, so an already enqueued
      ;; request cannot be overtaken by a new direct operation.
      (if (and (.isEmpty ^ConcurrentLinkedQueue (.-queue group))
               (.tryLock lock))
        (try
          (try
            (if (and (= 1 *request-count*) (not *batched?*))
              (run-transaction op)
              (binding [*request-count* 1
                        *batched?* false]
                (run-transaction op)))
            (finally (.unlock lock)))
          (finally (handoff! group)))
        (submit-queued! group run-transaction op true)))
    (submit-queued! group run-transaction op false)))
