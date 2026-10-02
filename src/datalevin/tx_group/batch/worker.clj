;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.worker
  "Single reusable WAL worker for the new write protocol.

  Owns one thread that runs WAL-branch tasks and, between tasks, drives relaxed
  WAL maintenance: it services an armed force, then parks until the next
  maintenance deadline. It implements `java.util.concurrent.Executor` so it can
  be supplied as the executor's `:wal-executor`.

  One reusable slot replaces the general queue: the collector serializes
  batches, so at most one WAL task is outstanding. A submit publishes the task
  into the slot and unparks the worker; a second submit waits for pickup rather
  than dropping or reordering it. Maintenance nudges are plain unparks, so any
  number of them coalesce to one re-read of `:deadline!`. The worker never runs
  user callbacks or native application (`execute` only carries WAL work), and a
  service failure is recorded rather than thrown, so a task can still be
  delivered and observed. Close is cooperative: it stops admission, unparks the
  thread and joins it with the caller's timeout after draining any accepted task."
  (:require [datalevin.tx-group.batch.executor :as executor])
  (:import [java.util.concurrent Executor RejectedExecutionException]
           [java.util.concurrent.atomic AtomicBoolean AtomicReference]
           [java.util.concurrent.locks LockSupport]))

(def ^:private idle-poll-ns 100000000)      ; 100 ms fallback re-evaluation
(def ^:private settled-poll-ns 1000000)     ; 1 ms guard against a stale deadline
(def ^:private slot-wait-ns 1000000)        ; 1 ms retry when the task slot is full

(defn create
  "Create one WAL worker.

  `opts`:
  - `:service!` zero-arg fn driving maintenance; returns truthy when it acted.
  - `:deadline!` zero-arg fn returning the next absolute monotonic deadline, 0
    when none is pending.
  - `:name` worker thread name; `:daemon?` whether the thread is a daemon.
  - `:close-timeout-ms` join bound used by `:close!`.

  Returns `{:executor :wake! :failure :close!}`. `:wake!` nudges the idle worker
  to re-read `:deadline!`; it is the seam inline leaders call after arming a
  relaxed threshold."
  [& {:keys [service! deadline! name daemon? close-timeout-ms]
      :or {service! (constantly nil)
           deadline! (constantly 0)
           name "dtlv-wal-worker"
           daemon? true
           close-timeout-ms 5000}}]
  (let [;; One reusable task slot, nil when free. Admission and close share this
        ;; lock so an accepted task is always visible to the final drain: either
        ;; the submitter wins the lock and publishes before close flips running?,
        ;; or close wins and the submitter observes the closed flag and rejects.
        slot (AtomicReference.)
        admission-lock (Object.)
        running? (AtomicBoolean. true)
        failure (AtomicReference.)
        record-failure! (fn [t] (when (some? t) (.compareAndSet failure nil t)))
        maintenance-deadline
        (fn []
          (long (or (try (deadline!)
                         (catch Throwable t
                           (record-failure! t)
                           0))
                    0)))
        due? (fn [deadline]
               (and (pos? (long deadline))
                    (<= (long deadline) (System/nanoTime))))
        service-now!
        (fn []
          (try (service!)
               (catch Throwable t
                 (record-failure! t)
                 nil)))
        run-task
        (fn [task]
          ;; A deadline may have changed after the previous park. Every task
          ;; crosses this fresh check; a failed/stale maintenance attempt still
          ;; lets the accepted task run and observe its own outcome.
          (when (due? (maintenance-deadline))
            (service-now!))
          (try (.run ^Runnable task)
               (catch Throwable t (record-failure! t))))
        ;; Timed park, re-evaluated at the next maintenance deadline with a
        ;; bounded fallback so a deadline armed without a wake is still seen.
        park-until
        (fn [deadline]
          (let [remaining (if (zero? (long deadline))
                            (long idle-poll-ns)
                            (max 0 (- (long deadline) (System/nanoTime))))]
            (LockSupport/parkNanos (Thread/currentThread)
                                   (min remaining (long idle-poll-ns)))))
        service-due!
        (fn [deadline]
          (if (due? deadline)
            (do
              (let [acted? (service-now!)]
                ;; A deadline the service did not clear must not spin.
                (when-not acted?
                  (LockSupport/parkNanos (Thread/currentThread)
                                         (long settled-poll-ns))))
              true)
            false))
        step
        (fn []
          (try
            (let [deadline (maintenance-deadline)]
              ;; Already-due maintenance has priority over an accepted batch
              ;; task, so a later append cannot delay an overdue force.
              (if (service-due! deadline)
                true
                (if-let [task (.getAndSet slot nil)]
                  (do (run-task task) true)
                  (if (.get running?)
                    (do (park-until deadline) true)
                    ;; Closed: admission is fenced, so any task accepted before
                    ;; the flip is already published. Drain it before exiting.
                    (if-let [task (.getAndSet slot nil)]
                      (do (run-task task) true)
                      false)))))
            (catch Throwable t (record-failure! t) true)))
        loop-fn (fn [] (while (step)))
        thread (doto (Thread. ^Runnable loop-fn ^String name)
                 (.setDaemon (boolean daemon?))
                 (.start))]
    {:executor
     (reify Executor
       (execute [_ task]
         (loop []
           (let [placed? (locking admission-lock
                           (if (.get running?)
                             (.compareAndSet slot nil task)
                             (throw (RejectedExecutionException.
                                     "WAL worker is closed"))))]
             (if placed?
               (LockSupport/unpark thread)
               ;; The slot holds a task awaiting pickup. Production serializes
               ;; batches so this waits only for pickup; park briefly and retry
               ;; rather than dropping or reordering the later task.
               (do (LockSupport/parkNanos (Thread/currentThread)
                                          (long slot-wait-ns))
                   (recur)))))))
     :wake! (fn [] (LockSupport/unpark thread))
     :failure (fn [] (.get failure))
     ;; Graceful drain, not an interrupt: every accepted task and the current
     ;; force must finish so an interrupted FileChannel cannot close under the
     ;; WAL. Returns true when the thread has actually exited within the timeout.
     :close! (fn []
               (locking admission-lock
                 (.set running? false))
               (LockSupport/unpark thread)
               (.join thread (long close-timeout-ms))
               (not (.isAlive thread)))}))

(defn for-wal
  "Create a worker over a WAL branch implementing `IWalMaintenance`.

  Runs the branch's WAL tasks and services its relaxed maintenance. `opts` are
  forwarded to `create`, so `:name`, `:daemon?` and `:close-timeout-ms` apply."
  [wal & opts]
  (apply create
         (concat opts
                 [:deadline! #(executor/maintenance-deadline-ns wal)
                  :service! #(executor/service-maintenance! wal 0)])))
