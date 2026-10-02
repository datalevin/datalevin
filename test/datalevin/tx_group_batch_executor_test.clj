(ns datalevin.tx-group-batch-executor-test
  "Contract tests for the two-branch executor skeleton.

  Deterministic fakes stand in for the real WAL and native branches so the
  parallel-application, gated-commit, late-failure and LSN hand-off contracts can
  be checked without disk or LMDB."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.phase :as phase])
  (:import [java.util.concurrent ConcurrentLinkedQueue Executor Executors
            RejectedExecutionException]))

(def ^:private default-overrides
  {:wal-pending-max-requests 64
   :wal-pending-max-bytes 1048576
   :write-batch-size 8
   :write-batch-max-bytes 12288
   :wal-rmw-max-bytes 4096})

(defn- emit! [^ConcurrentLinkedQueue events event]
  (.add events event)
  event)

(defn- event? [^ConcurrentLinkedQueue events wanted]
  (boolean (some #{wanted} (vec events))))

(defn- await-event!
  [^ConcurrentLinkedQueue events wanted timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (cond
        (event? events wanted) true
        (> (System/currentTimeMillis) deadline) false
        :else (do (Thread/sleep 5) (recur))))))

(defn- wal-branch
  [{:keys [append-gate policy-gate fail-policy? events durable?]
    :or {durable? true}}]
  (let [state (atom {:appended 0 :completed 0})]
    [(reify executor/IWalBranch
       (append-group! [_ _batch lsn]
         (swap! state update :appended inc)
         (emit! events :append)
         (when append-gate (deref append-gate))
         [:token lsn])
       (complete-policy! [_ _token _deadline-ns]
         (emit! events :policy)
         (when policy-gate (deref policy-gate))
         (when fail-policy?
           (throw (ex-info "wal policy failed"
                           {:error :txlog/write-indeterminate
                            :outcome :indeterminate})))
         (swap! state update :completed inc)
         durable?))
     state]))

(defn- native-branch
  [{:keys [fail-apply? events]}]
  (let [state (atom {:applied 0 :committed 0})]
    [(reify executor/INativeBranch
       (apply-rows! [_ batch before-commit]
         (swap! state update :applied inc)
         (emit! events :apply)
         (when fail-apply?
           (throw (ex-info "native apply failed"
                           {:error :txlog/native-apply-failed})))
         ;; Abort without committing when the WAL policy failed.
         (before-commit)
         (swap! state update :committed inc)
         (emit! events :commit)
         (let [n (batch/batch-count batch)
               values (object-array n)]
           (dotimes [i n]
             (aset values i (batch/data (batch/batch-at batch i))))
           values)))
     state]))

(defn- collector-with
  ([wal native lsn] (collector-with wal native lsn nil))
  ([wal native lsn executor-opts]
   (batch/create (executor/create wal native #(swap! lsn inc) executor-opts)
                 {:limits (charge/resolve-limits default-overrides)})))

(def ^:private force-parallel {:schedule-fn (constantly :parallel)})

(deftest parallel-batches-reuse-one-task-and-gate-with-fresh-results
  (let [pool (Executors/newSingleThreadExecutor)
        tasks (atom [])
        gates (atom [])
        wal (reify executor/IWalBranch
              (append-group! [_ _ lsn] lsn)
              (complete-policy! [_ _ _] true))
        native (reify executor/INativeBranch
                 (apply-rows! [_ b gate]
                   (swap! gates conj gate)
                   (let [lsn (.get ^java.util.concurrent.atomic.AtomicLong (.lsn b))]
                     (is (= lsn (gate)))
                     (is (= lsn (gate)) "a repeated gate consumes no extra permit"))
                   (object-array [(batch/data (batch/batch-at b 0))])))
        dispatch (reify Executor
                   (execute [_ task]
                     (swap! tasks conj task)
                     (.execute pool task)))
        c (collector-with wal native (atom 0)
                          (assoc force-parallel :wal-executor dispatch))]
    (try
      (dotimes [n 64]
        (is (= n (batch/submit! c {:allowance 1024 :data n}))))
      (is (= 64 (count @tasks) (count @gates)))
      (is (every? #(identical? (first @tasks) %) @tasks))
      (is (every? #(identical? (first @tasks) %) @gates)
          "the preallocated task is also the reusable native commit gate")
      (is (batch/await-quiescence! c 100))
      (is (zero? (:requests (batch/usage c))))
      (finally (.shutdownNow pool)))))

(deftest a-rejected-handoff-does-not-wait-for-an-unsubmitted-task
  (let [events (ConcurrentLinkedQueue.)
        [wal] (wal-branch {:events events})
        [native] (native-branch {:events events})
        failure (RejectedExecutionException. "closed executor")
        dispatch (reify Executor (execute [_ _] (throw failure)))
        c (collector-with wal native (atom 0)
                          (assoc force-parallel :wal-executor dispatch))
        result (future (try (batch/submit! c {:allowance 1024 :data :a})
                            (catch Throwable t t)))]
    (is (identical? failure (deref result 5000 ::timeout)))
    (is (empty? events))
    (is (false? (batch/serving? c)))
    (is (batch/await-quiescence! c 100))
    (is (zero? (:requests (batch/usage c))))))

(deftest inline-completion-wakes-maintenance-only-when-not-durable
  (doseq [durable? [true false]]
    (let [events (ConcurrentLinkedQueue.)
          [wal] (wal-branch {:events events :durable? durable?})
          [native] (native-branch {:events events})
          wakeups (atom 0)
          c (collector-with
             wal native (atom 0)
             {:wake-maintenance! #(swap! wakeups inc)
              :wal-executor (reify Executor
                              (execute [_ _]
                                (throw (ex-info "singleton dispatched" {}))))})]
      (is (= :ok (batch/submit! c {:allowance 1024 :data :ok})))
      (is (= (if durable? 0 1) @wakeups)))))

(deftest native-application-overlaps-wal-and-commits-after-policy
  (let [events (ConcurrentLinkedQueue.)
        append-gate (promise)
        policy-gate (promise)
        [wal wal-state] (wal-branch {:append-gate append-gate
                                     :policy-gate policy-gate
                                     :events events})
        [native native-state] (native-branch {:events events})
        lsn (atom 0)
        c (collector-with wal native lsn force-parallel)
        f (future (try (batch/submit! c {:allowance 1024 :data :a})
                       (catch Throwable t t)))]
    (try
      (testing "native row application runs while WAL append is blocked"
        (is (await-event! events :apply 5000)))
      (is (not (event? events :commit)) "native committed before WAL policy")
      (deliver append-gate true)
      (testing "native commit is gated on WAL policy completion"
        (is (await-event! events :policy 5000))
        (is (not (event? events :commit))))
      (deliver policy-gate true)
      (is (= :a (deref f 5000 ::timeout)))
      (is (= 1 (:appended @wal-state)))
      (is (= 1 (:completed @wal-state)))
      (is (= 1 (:committed @native-state)))
      (testing "the group LSN is assigned before dispatch and published at join"
        (is (= 1 @lsn))
        (is (= 1 (batch/published-lsn c))))
      (finally
        (deliver append-gate true)
        (deliver policy-gate true)
        (deref f 5000 nil)))))

(deftest a-wal-policy-failure-aborts-native-commit-and-fences
  (let [events (ConcurrentLinkedQueue.)
        [wal wal-state] (wal-branch {:events events :fail-policy? true})
        [native native-state] (native-branch {:events events})
        lsn (atom 0)
        c (collector-with wal native lsn force-parallel)
        thrown (try (batch/submit! c {:allowance 1024 :data :a})
                    nil
                    (catch Throwable t t))]
    (is (instance? Throwable thrown))
    (is (= :txlog/write-indeterminate (:error (ex-data thrown))))
    (testing "native applied rows but never committed"
      (is (event? events :apply))
      (is (not (event? events :commit)))
      (is (= 1 (:applied @native-state)))
      (is (zero? (:committed @native-state))))
    (is (= 1 (:appended @wal-state)))
    (is (zero? (:completed @wal-state)))
    (testing "the failed runtime is fenced"
      (is (not (batch/serving? c))))))

(deftest a-native-failure-after-wal-policy-reports-the-committed-outcome
  ;; The WAL branch must finish before the native failure is reported, so
  ;; submit on a future and release the blocked append from the test thread.
  ;; Policy completion returned durable, so the established outcome is committed.
  (let [events (ConcurrentLinkedQueue.)
        append-gate (promise)
        [wal _] (wal-branch {:append-gate append-gate :events events})
        [native _] (native-branch {:events events :fail-apply? true})
        lsn (atom 0)
        c (collector-with wal native lsn force-parallel)
        f (future (try (batch/submit! c {:allowance 1024 :data :a})
                       (catch Throwable t t)))]
    (try
      (is (await-event! events :apply 5000))
      (deliver append-gate true)
      (let [thrown (deref f 5000 ::timeout)]
        (is (instance? Throwable thrown))
        (is (= :txlog/write-committed (:error (ex-data thrown))))
        (is (= :committed (:outcome (ex-data thrown))))
        (is (= :durable (:wal-status (ex-data thrown))))
        (is (= :txlog/native-apply-failed
               (:error (ex-data (ex-cause thrown))))))
      (is (not (event? events :commit)))
      (is (not (batch/serving? c)))
      (finally
        (deliver append-gate true)
        (deref f 5000 nil)))))

(deftest a-native-failure-does-not-retire-the-batch-until-the-wal-drains
  ;; Regression: the failing native branch used to throw out of the executor
  ;; while the WAL branch was still blocked, so the collector retired the batch,
  ;; await-quiescence! returned true and close could release WAL ownership early.
  (let [events (ConcurrentLinkedQueue.)
        append-gate (promise)
        [wal _] (wal-branch {:append-gate append-gate :events events})
        [native _] (native-branch {:events events :fail-apply? true})
        lsn (atom 0)
        c (collector-with wal native lsn force-parallel)
        f (future (try (batch/submit! c {:allowance 1024 :data :a})
                       (catch Throwable t t)))]
    (try
      (is (await-event! events :apply 5000))
      (Thread/sleep 100)
      (testing "the native failure does not retire the batch while WAL is blocked"
        (is (not (realized? f)))
        (is (false? (batch/await-quiescence! c 50)))
        (is (pos? (:requests (batch/usage c)))))
      (deliver append-gate true)
      (let [thrown (deref f 5000 ::timeout)]
        (is (instance? Throwable thrown))
        (is (= :txlog/write-committed (:error (ex-data thrown))))
        (is (= :committed (:outcome (ex-data thrown))))
        (is (= :txlog/native-apply-failed
               (:error (ex-data (ex-cause thrown))))))
      (testing "once the WAL drains, quiescence and accounting settle"
        (is (true? (batch/await-quiescence! c 5000)))
        (is (zero? (:requests (batch/usage c)))))
      (finally
        (deliver append-gate true)
        (deref f 5000 nil)))))

(deftest an-interrupt-during-the-wal-drain-does-not-retire-the-batch
  ;; Regression: the drain wait was interruptible, so an interrupt during drain
  ;; threw InterruptedException and execute-batch! treated the live batch as an
  ;; undispatched cancellation: not-committed outcome, released accounting and
  ;; quiescence while the WAL branch still ran.
  (let [events (ConcurrentLinkedQueue.)
        append-gate (promise)
        [wal _] (wal-branch {:append-gate append-gate :events events})
        [native _] (native-branch {:events events :fail-apply? true})
        lsn (atom 0)
        c (collector-with wal native lsn force-parallel)
        thrown (atom nil)
        worker (Thread. (fn []
                          (try
                            (batch/submit! c {:allowance 1024 :data :a})
                            (catch Throwable t (reset! thrown t)))))]
    (try
      (.start worker)
      (is (await-event! events :apply 5000))
      (Thread/sleep 100)
      (.interrupt worker)
      (Thread/sleep 100)
      (testing "the interrupt cannot cancel the batch while the WAL is live"
        (is (false? (batch/await-quiescence! c 50)))
        (is (pos? (:requests (batch/usage c))))
        (is (.isAlive worker)))
      (deliver append-gate true)
      (.join worker 5000)
      (is (not (.isAlive worker)))
      (is (instance? Throwable @thrown))
      (is (= :txlog/write-committed (:error (ex-data @thrown))))
      (is (= :committed (:outcome (ex-data @thrown))))
      (is (= :txlog/native-apply-failed
             (:error (ex-data (ex-cause @thrown)))))
      (testing "once the WAL drains, quiescence and accounting settle"
        (is (true? (batch/await-quiescence! c 5000)))
        (is (zero? (:requests (batch/usage c)))))
      (finally
        (deliver append-gate true)
        (.join worker 5000)))))

(deftest prepare-batch-runs-after-lsn-and-before-dispatch
  (let [before (promise)
        after (promise)
        [wal _] (wal-branch {:events (ConcurrentLinkedQueue.)})
        native (reify executor/INativeBranch
                 (apply-rows! [_ batch before-commit]
                   (deliver after (batch/dispatched? batch))
                   (before-commit)
                   (let [n (batch/batch-count batch)
                         values (object-array n)]
                     (dotimes [i n]
                       (aset values i (batch/data (batch/batch-at batch i))))
                     values)))
        lsn (atom 0)
        executor (executor/create
                  wal native #(swap! lsn inc)
                  {:prepare-batch! (fn [batch]
                                     (deliver before (batch/dispatched? batch)))})
        c (batch/create executor {:limits (charge/resolve-limits default-overrides)})]
    (is (= :a (batch/submit! c {:allowance 1024 :data :a})))
    (testing "ordered preparation runs before dispatch; branches see dispatched"
      (is (false? (deref before 5000 ::timeout)))
      (is (true? (deref after 5000 ::timeout))))))

(deftest preparation-overrunning-the-deadline-cancels-before-dispatch
  ;; Regression: with no final preparation check, 80 ms of preparation under a
  ;; 20 ms deadline still dispatched and committed.
  (let [events (ConcurrentLinkedQueue.)
        [wal wal-state] (wal-branch {:events events})
        [native native-state] (native-branch {:events events})
        lsn (atom 0)
        exec (executor/create
              wal native #(swap! lsn inc)
              {:schedule-fn (constantly :inline)
               :prepare-batch! (fn [_batch] (Thread/sleep 80))})
        c (batch/create exec {:limits (charge/resolve-limits default-overrides)})
        thrown (try (batch/submit! c {:allowance 1024 :data :a :timeout-ms 20})
                    nil
                    (catch Throwable t t))]
    (is (instance? Throwable thrown))
    (is (= :txlog/write-deadline-exceeded (:error (ex-data thrown))))
    (is (= :not-committed (:outcome (ex-data thrown))))
    (testing "no branch ran and the batch cancelled cleanly"
      (is (zero? (:appended @wal-state)))
      (is (zero? (:applied @native-state)))
      (is (not (event? events :commit)))
      (is (batch/serving? c)))))

(deftest an-inline-batch-runs-wal-then-native-on-the-leader
  (let [events (ConcurrentLinkedQueue.)
        submitted (atom 0)
        wal-executor (reify Executor
                       (execute [_ _] (swap! submitted inc)))
        [wal _] (wal-branch {:events events})
        [native native-state] (native-branch {:events events})
        lsn (atom 0)
        c (collector-with wal native lsn {:wal-executor wal-executor})]
    (is (= :a (batch/submit! c {:allowance 1024 :data :a})))
    (testing "weight-one runs inline: no worker task is submitted"
      (is (zero? @submitted)))
    (testing "WAL append/policy complete before native apply/commit"
      (is (= [:append :policy :apply :commit] (vec events))))
    (is (= 1 (:committed @native-state)))
    (is (= 1 (batch/published-lsn c)))))

(deftest an-inline-wal-policy-failure-skips-native-application
  (let [events (ConcurrentLinkedQueue.)
        [wal _] (wal-branch {:events events :fail-policy? true})
        [native native-state] (native-branch {:events events})
        lsn (atom 0)
        c (collector-with wal native lsn)
        thrown (try (batch/submit! c {:allowance 1024 :data :a})
                    nil
                    (catch Throwable t t))]
    (is (instance? Throwable thrown))
    (is (= :txlog/write-indeterminate (:error (ex-data thrown))))
    (testing "inline WAL failure skips native application entirely"
      (is (not (event? events :apply)))
      (is (zero? (:applied @native-state)))
      (is (zero? (:committed @native-state))))
    (is (not (batch/serving? c)))))

(deftest an-inline-native-failure-after-policy-reports-the-committed-outcome
  (let [events (ConcurrentLinkedQueue.)
        [wal wal-state] (wal-branch {:events events})
        [native _] (native-branch {:events events :fail-apply? true})
        lsn (atom 0)
        c (collector-with wal native lsn)
        thrown (try (batch/submit! c {:allowance 1024 :data :a})
                    nil
                    (catch Throwable t t))]
    (is (instance? Throwable thrown))
    (is (= :txlog/write-committed (:error (ex-data thrown))))
    (is (= :committed (:outcome (ex-data thrown))))
    (is (= :durable (:wal-status (ex-data thrown))))
    (is (= :txlog/native-apply-failed
           (:error (ex-data (ex-cause thrown)))))
    (testing "the WAL policy outcome was already established"
      (is (event? events :policy))
      (is (= 1 (:completed @wal-state))))
    (is (not (event? events :commit)))
    (is (not (batch/serving? c)))))

(deftest failures-through-publication-preserve-the-established-wal-outcome
  (doseq [schedule [:inline :parallel]
          durable? [true false]
          fault-phase [:execution-complete :joint-publication]
          fault [:fence :throw :interrupt]]
    (testing (str [schedule durable? fault-phase fault])
      (let [events (ConcurrentLinkedQueue.)
            [wal _] (wal-branch {:events events :durable? durable?})
            [native native-state] (native-branch {:events events})
            c (collector-with wal native (atom 0)
                              {:schedule-fn (constantly schedule)})
            failure (if (= :interrupt fault)
                      (InterruptedException. "publication interrupted")
                      (ex-info "publication failed"
                               {:error :txlog/runtime-fenced
                                :outcome :not-committed}))
            uninstall (phase/observe!
                       (fn [event _]
                         (when (= fault-phase event)
                           (if (= :fence fault)
                             (batch/fence! c failure)
                             (throw failure)))))]
        (try
          (let [thrown (try (batch/submit! c {:allowance 1024 :data :a})
                           nil
                           (catch Throwable t t))
                interrupted? (.isInterrupted (Thread/currentThread))]
            (is (= {:error (if durable? :txlog/write-committed
                              :txlog/write-indeterminate)
                    :outcome (if durable? :committed :indeterminate)
                    :wal-status (if durable? :durable :appended)
                    :txlog-lsn 1
                    :retryable? false}
                   (ex-data thrown)))
            (is (identical? failure (ex-cause thrown)))
            (is (= 1 (:committed @native-state)))
            (is (not (batch/serving? c)))
            (is (zero? (:requests (batch/usage c))))
            (is (batch/await-quiescence! c 1000))
            (when (= :interrupt fault)
              (is interrupted?)))
          (finally
            (uninstall)
            (Thread/interrupted)))))))

(deftest a-fence-after-inline-policy-aborts-native-commit
  (let [events (ConcurrentLinkedQueue.)
        [wal _] (wal-branch {:events events})
        collector (atom nil)
        native (reify executor/INativeBranch
                 (apply-rows! [_ _batch before-commit]
                   (emit! events :apply)
                   ;; A concurrent terminal failure arrives after policy
                   ;; completion but before the commit gate.
                   (batch/fence! @collector
                                 (ex-info "fenced during inline"
                                          {:error :txlog/runtime-fenced
                                           :outcome :not-committed
                                           :retryable? false}))
                   (before-commit)
                   (emit! events :commit)))
        lsn (atom 0)
        c (collector-with wal native lsn)]
    (reset! collector c)
    (let [thrown (try (batch/submit! c {:allowance 1024 :data :a})
                      nil
                      (catch Throwable t t))]
      (is (instance? Throwable thrown))
      (is (= :txlog/write-committed (:error (ex-data thrown))))
      (is (= :committed (:outcome (ex-data thrown))))
      (is (= :txlog/runtime-fenced
             (:error (ex-data (ex-cause thrown)))))
      (is (not (event? events :commit))))))

(deftest a-fence-during-the-wal-wait-aborts-the-late-native-commit
  ;; Regression: the commit gate checked serving before the WAL wait but not
  ;; after, so a fence recorded during the wait still allowed the native commit.
  (let [events (ConcurrentLinkedQueue.)
        append-gate (promise)
        [wal _] (wal-branch {:append-gate append-gate :events events})
        [native native-state] (native-branch {:events events})
        lsn (atom 0)
        c (collector-with wal native lsn force-parallel)
        f (future (try (batch/submit! c {:allowance 1024 :data :a})
                       (catch Throwable t t)))]
    (try
      (is (await-event! events :apply 5000))
      ;; The native branch is now blocked in the commit gate's WAL wait.
      (Thread/sleep 100)
      (batch/fence! c (ex-info "fenced during the wait"
                               {:error :txlog/runtime-fenced
                                :outcome :not-committed
                                :retryable? false}))
      (deliver append-gate true)
      (let [thrown (deref f 5000 ::timeout)]
        (is (instance? Throwable thrown))
        (is (= :txlog/write-committed (:error (ex-data thrown))))
        (is (= :committed (:outcome (ex-data thrown))))
        (is (= :txlog/runtime-fenced
               (:error (ex-data (ex-cause thrown))))))
      (testing "the native commit never ran"
        (is (zero? (:committed @native-state)))
        (is (not (event? events :commit))))
      (finally
        (deliver append-gate true)
        (deref f 5000 nil)))))
