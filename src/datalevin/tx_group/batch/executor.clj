;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.executor
  "Two-branch batch executor for the new write protocol.

  Owns the hand-off from ordered preparation, LSN assignment, branch dispatch
  and the join for one sealed batch. The WAL branch writes and completes exactly
  one record; the native branch applies the same frozen rows and must call
  `before-commit` before committing, which blocks on the WAL policy outcome.
  This is the M0 skeleton: the real `txlog` and `cpp` adapters implement the
  protocols, while tests drive deterministic fakes.

  The native branch runs on the calling leader thread so native transactions stay
  on their owning thread; the WAL branch runs on the supplied executor (one task
  per environment in production). Neither branch waits for the other's I/O
  except at the gated native commit, and a late branch return cannot publish
  success after the collector fences."
  (:require [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.phase :as phase])
  (:import [java.util.concurrent Executor]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent.locks LockSupport]))

(defprotocol IWalBranch
  (append-group!
    [wal batch lsn]
    "Write one complete group record and return an owned policy token.")
  (complete-policy!
    [wal token deadline-ns]
    "Block until the record's configured policy completes, or throw on failure.
     Returns true when the record is durable at the configured strength and
     false when the policy is complete but durability is not yet confirmed
     (relaxed append with the force still deferred). `deadline-ns` is the batch's
     absolute preparation cutoff, or zero."))

(defprotocol INativeBranch
  (apply-rows!
    [native batch before-commit]
    "Apply the frozen rows and return one value per request. Call before-commit
     before committing; it throws when the WAL policy failed."))

(defprotocol IWalMaintenance
  (maintenance-deadline-ns
    [wal]
    "Absolute monotonic deadline for the next due WAL maintenance, or 0 when
     none is pending.")
  (service-maintenance!
    [wal deadline-ns]
    "Drive and force any due relaxed WAL prefix from the maintenance owner.
     Returns a force result map or nil when nothing was due."))

(defn- check-serving!
  "Abort this batch's native commit when its runtime was fenced after dispatch."
  [batch]
  (batch/check-serving! (batch/batch-collector batch)))

(defn- check-commit-gate!
  "Final pre-commit checks for the native branch.

  The runtime must still be serving and the batch deadline must not have passed.
  Called immediately before the commit attempt, including again after the WAL
  wait, so a fence or expiry recorded during that wait aborts the commit instead
  of letting an already-fenced batch commit."
  [batch]
  (check-serving! batch)
  (when (batch/expired? batch)
    (throw (ex-info "Write deadline expired before native commit"
                    {:error :txlog/write-deadline-exceeded
                     :outcome :not-committed
                     :retryable? false}))))

(defprotocol ^:private IParallelHandoff
  (start-handoff! [handoff batch lsn])
  (drain-handoff! [handoff])
  (handoff-error [handoff])
  (clear-handoff! [handoff]))

(deftype ParallelHandoff
  [wal ^AtomicBoolean done
   ^:unsynchronized-mutable current-batch
   ^:unsynchronized-mutable ^long lsn
   ^:unsynchronized-mutable token
   ^:unsynchronized-mutable error
   ^:unsynchronized-mutable waiter
   ^:unsynchronized-mutable drained?]
  IParallelHandoff
  (start-handoff! [_ batch next-lsn]
    ;; Only one batch uses this environment executor at a time. The completion
    ;; flag is reset before dispatch and is consumed by the leader before the
    ;; slot is cleared and reused.
    (set! current-batch batch)
    (set! lsn (long next-lsn))
    (set! waiter (Thread/currentThread))
    (set! drained? false)
    (.set done false))
  (drain-handoff! [this]
    (when-not drained?
      ;; Uninterruptible wait for the WAL branch, preserving the caller's
      ;; interrupt status. The flag is rechecked immediately before parking, so
      ;; an unpark that raced the check is not lost. Exactly one wait per
      ;; dispatch, even when the commit gate and final drain both run.
      (let [interrupted? (volatile! false)]
        (loop []
          (when-not (.get done)
            (when (Thread/interrupted) (vreset! interrupted? true))
            (LockSupport/park this)
            (recur)))
        (set! drained? true)
        (when @interrupted? (.interrupt (Thread/currentThread))))))
  (handoff-error [_] error)
  (clear-handoff! [_]
    (set! current-batch nil)
    (set! token nil)
    (set! error nil)
    (set! waiter nil))
  Runnable
  (run [_]
    (try
      (phase/phase! :wal-start current-batch)
      (let [value (append-group! wal current-batch lsn)
            durable? (boolean (complete-policy!
                               wal value (batch/batch-cutoff current-batch)))]
        (batch/record-wal-policy! current-batch durable?)
        (phase/phase! :wal-complete current-batch)
        (set! token value))
      (catch Throwable t (set! error t))
      ;; Publish completion before waking the leader. The leader may clear or
      ;; reuse the slot only after observing the flag; no late completion
      ;; touches it.
      (finally
        (.set done true)
        (LockSupport/unpark waiter))))
  clojure.lang.IFn
  (invoke [this]
    (check-commit-gate! current-batch)
    (drain-handoff! this)
    (when error (throw ^Throwable error))
    (check-commit-gate! current-batch)
    token))

(defn- run-inline!
  "Weight-one leader path: no worker task and no branch-completion wait.

  The leader runs the WAL append/policy routines, then applies and commits the
  frozen rows on the same thread. A WAL failure skips native application, and a
  serving check before application and again before commit aborts after a fence.
  A native failure after policy completion preserves the established WAL outcome
  rather than reporting the native error alone. `wake!` nudges the maintenance
  worker because inline work arms relaxed thresholds on the leader rather than
  on the worker's own loop."
  [wal native batch lsn wake!]
  (phase/phase! :inline-execution batch)
  (let [cutoff (batch/batch-cutoff batch)
        token (append-group! wal batch lsn)
        durable? (boolean (complete-policy! wal token cutoff))]
    (batch/record-wal-policy! batch durable?)
    (phase/phase! :wal-complete batch)
    (when-not durable? (wake!))
    (check-serving! batch)
    (apply-rows! native batch
                 (fn []
                   (check-commit-gate! batch)
                   token))))

(defn- run-parallel!
  "Parallel path: WAL runs on `wal-executor` while the leader applies rows.

  The native `before-commit` gate blocks until WAL policy completes and returns
  the append token, so the native branch can write append-dependent metadata
  before committing. A known WAL outcome is preferred over a native failure.

  The WAL branch is always drained before this function returns, including when
  the native branch fails before the gate. Otherwise the collector would retire
  the batch, report quiescence and let close release WAL ownership while WAL I/O
  still runs. Draining means a genuinely stuck WAL branch keeps the active slot
  owned, which is the established stuck-owner behavior."
  [native batch lsn ^Executor wal-executor ^ParallelHandoff handoff]
  (start-handoff! handoff batch lsn)
  (try
    (phase/phase! :worker-dispatch batch)
    (.execute wal-executor handoff)
    ;; Rejection above means no task was accepted, so there is no permit to
    ;; await. Once accepted, every native exit drains the task before cleanup.
    (try
      (apply-rows! native batch handoff)
      (finally
        (drain-handoff! handoff)
        ;; A WAL failure wins over a native failure or a skipped commit gate.
        ;; Otherwise the native value/exception passes through unchanged.
        (when-let [error (handoff-error handoff)]
          (throw ^Throwable error))))
    (finally (clear-handoff! handoff))))

(defn create
  "Build a collector executor over WAL and native branches.

  `opts`:
  - `:wal-executor` runs the parallel WAL branch (one task per environment in
    production, the shared agent pool when absent). Inline batches never use it.
  - `:prepare-batch!` runs on the leader after LSN assignment and before the
    dispatch transition (`begin-dispatch!`); it finalizes ordered state-dependent
    preparation. Blind batches omit it. The transition rechecks serving status
    and the batch deadline, so preparation that overruns the deadline cancels
    instead of committing.
  - `:schedule-fn` selects the schedule from the sealed batch; defaults to the
    collector's fixed `batch-schedule`. Tests and diagnostics inject it to drive
    both paths deterministically.
  - `:wake-maintenance!` nudges the maintenance worker after inline work arms a
    relaxed threshold; defaults to a no-op.

  The fixed schedule is read from the sealed batch: weight-one batches run inline
  on the leader, larger batches run the two branches in parallel. Both schedules
  share the same WAL policy and native-commit gate."
  ([wal native next-lsn!]
   (create wal native next-lsn! nil))
  ([wal native next-lsn! {:keys [wal-executor prepare-batch! schedule-fn
                                 wake-maintenance!]
                          :or {schedule-fn batch/batch-schedule
                               wake-maintenance! (constantly nil)}}]
   (let [^Executor wal-executor (or wal-executor clojure.lang.Agent/soloExecutor)
         ;; One reusable task, completion flag, result slot and commit gate per
         ;; environment executor. The collector serializes its batches.
         handoff (ParallelHandoff. wal (AtomicBoolean. false)
                                   nil 0 nil nil nil false)]
     (fn [^datalevin.tx_group.batch.Batch batch]
       (let [lsn (long (next-lsn!))]
         (batch/set-lsn! batch lsn)
         (when prepare-batch!
           (prepare-batch! batch)
           (batch/refresh-wal-bodies! batch))
         ;; Ordered preparation is complete. Recheck serving/deadlines and mark
         ;; the batch dispatched atomically: an overrun preparation phase must
         ;; cancel here rather than commit.
         (batch/begin-dispatch! batch)
         (let [values (if (= :inline (schedule-fn batch))
                        (run-inline! wal native batch lsn wake-maintenance!)
                        (run-parallel! native batch lsn wal-executor handoff))]
           ;; Both branches have stopped and the WAL outcome is settled; the
           ;; collector's join/publication now follows.
           (phase/phase! :execution-complete batch)
           values))))))
