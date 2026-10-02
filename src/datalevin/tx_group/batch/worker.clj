;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.worker
  "Single reusable WAL worker for the new write protocol.

  Owns one thread that runs WAL-branch tasks in FIFO order and, between tasks,
  drives relaxed WAL maintenance: it services an armed force, then parks until
  the next maintenance deadline. It implements `java.util.concurrent.Executor`
  so it can be supplied as the executor's `:wal-executor`.

  The worker never runs user callbacks or native application (`execute` only
  carries WAL work), and a service failure is recorded rather than thrown, so a
  task can still be delivered and observed. Close is cooperative: it stops
  admission, wakes the thread and joins it with the caller's timeout."
  (:require [datalevin.tx-group.batch.executor :as executor])
  (:import [java.util.concurrent Executor LinkedBlockingQueue
            RejectedExecutionException TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

(def ^:private idle-poll-ns 100000000)      ; 100 ms fallback re-evaluation
(def ^:private settled-poll-ns 1000000)     ; 1 ms guard against a stale deadline

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
  (let [queue (LinkedBlockingQueue.)
        ;; Admission and close coordinate here: a task is accepted (enqueued)
        ;; only while running? is true, and close flips running? under this lock.
        ;; Every accepted task therefore happens-before the close and is
        ;; guaranteed to be in the queue for the final drain.
        admission-lock (Object.)
        running? (AtomicBoolean. true)
        wake-pending? (AtomicBoolean. false)
        wake-token (Object.)
        ;; Inline batches may retire while maintenance remains blocked. Their
        ;; notifications share one pending token, independent of request count.
        wake! (fn []
                (locking admission-lock
                  (when (and (.get running?)
                             (.compareAndSet wake-pending? false true))
                    (.offer queue wake-token))))
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
          ;; Clear before rereading the deadline, so an arriving trigger either
          ;; appears in this check or queues the next single notification.
          (when (identical? task wake-token) (.set wake-pending? false))
          ;; A deadline may have changed while a timed poll was parked. Every
          ;; task, including one returned by that poll, crosses this fresh check.
          ;; A failed/stale maintenance attempt still lets the accepted task run
          ;; and observe its own outcome; it must not be discarded on close.
          (when (due? (maintenance-deadline))
            (service-now!))
          (when-not (identical? task wake-token)
            (try (.run ^Runnable task)
                 (catch Throwable t (record-failure! t)))))
        poll! (fn [timeout-ns]
                (let [task (.poll queue (max 0 (long timeout-ns))
                                  TimeUnit/NANOSECONDS)]
                  (when task (run-task task))
                  (some? task)))
        service-due!
        (fn [deadline]
          (if (due? deadline)
            (do
              (let [acted? (service-now!)]
                ;; A deadline the service did not clear must not spin.
                (when-not acted?
                  (poll! settled-poll-ns)))
              true)
            false))
        step
        (fn []
          (try
            (let [deadline (maintenance-deadline)]
              ;; Already-due maintenance has priority over any queued batch
              ;; task, so a later append cannot delay an overdue force past its
              ;; deadline.
              (if (service-due! deadline)
                true
                (if-let [task (.poll queue)]
                  (do (run-task task) true)
                  (if (.get running?)
                    (do
                      (if (zero? deadline)
                        (poll! idle-poll-ns)
                        (let [remaining (- (long deadline) (System/nanoTime))]
                          (if (pos? remaining)
                            (poll! remaining)
                            ;; Became due while waiting; service it now.
                            (service-due! deadline))))
                      true)
                    ;; Closed: admission is fenced, so every accepted task was
                    ;; queued before this. Drain it instead of exiting with work
                    ;; a batch is still waiting on.
                    (if-let [task (.poll queue)]
                      (do (run-task task) true)
                      false)))))
            (catch InterruptedException _ true)
            (catch Throwable t (record-failure! t) true)))
        loop-fn (fn [] (while (step)))
        thread (doto (Thread. ^Runnable loop-fn ^String name)
                 (.setDaemon (boolean daemon?))
                 (.start))
        executor (reify Executor
                   (execute [_ task]
                     (locking admission-lock
                       (if (.get running?)
                         (.put queue task)
                         (throw (RejectedExecutionException.
                                 "WAL worker is closed"))))))]
    {:executor executor
     :wake! wake!
     :failure (fn [] (.get failure))
     ;; Graceful drain, not an interrupt: every accepted task and the current
     ;; force must finish so an interrupted FileChannel cannot close under the
     ;; WAL. Returns true when the thread has actually exited within the timeout.
     :close! (fn []
               (locking admission-lock
                 (.set running? false)
                 (when (.compareAndSet wake-pending? false true)
                   (.offer queue wake-token)))
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
