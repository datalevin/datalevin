;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-state
  "Bounded, caller-driven WAL application. The retained LSN index is not a
  second request queue: appended entries reference shared batch metadata."
  (:require [datalevin.txlog :as wal]
            [datalevin.txlog.append :as append]
            [datalevin.tx-state.view :as view]
            [datalevin.tx-state.lifetime :as lifetime :refer [with-lock]])
  (:import [datalevin.utl PendingBudget PendingBudget$Usage]
           [java.lang AutoCloseable]
           [java.util TreeMap]
           [java.util.concurrent ConcurrentSkipListMap CountDownLatch ExecutorService
            Executors ThreadFactory TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent.locks Condition LockSupport ReentrantLock]))

(defn ^:redef phase!
  "Deterministic fault/concurrency seam; no production observer allocation."
  [_event _context]
  nil)

(declare seal-through! mark-eligible! capture-ready-sync-prefix!
         check-admission! prune! schedule-reclaim! fail! next-range eligible?
         acquire-preparation! release-preparation!)

(defrecord Reservation [bytes released? appended? runtime deadline])
(defrecord GroupReservation [bytes runtime members])
(defrecord Entry [lsn append-batch error rows result reservation range applied? complete?
                  wait-state])
(defrecord CompletionWaiters [changed waiters])
(defrecord ApplicationCompletion [changed waiters complete?])
(defrecord ApplicationRange [lo hi entries deadline completion])
(defrecord ApplicationOwner [range token phase generation thread])
(defrecord SyncCompletion [target batch done error])

(defn create
  "Create an unopened engine runtime around a private WAL. apply-range! must
  apply the supplied immutable records directly, never append or re-collect.
  The open adapter owns recovery and the lifetime protocol lease."
  [wal-state {:keys [apply-range! db-identity applied-lsn
                     wal-pending-max-bytes wal-pending-max-requests
                     wal-apply-timeout-ms application-max-records
                     application-max-bytes prune-step-records hooks]
              :or {applied-lsn 0 wal-pending-max-bytes 67108864
                   wal-pending-max-requests 4096 wal-apply-timeout-ms 30000
                   application-max-records 4096 application-max-bytes 4194304
                   prune-step-records 2048}}]
  (when (:wal-shared? wal-state)
    (throw (ex-info "Independent application requires a private WAL"
                    {:error :txlog/unsupported-write-protocol})))
  (doseq [n [wal-pending-max-bytes wal-pending-max-requests wal-apply-timeout-ms
             application-max-records application-max-bytes]]
    (when-not (and (integer? n) (pos? (long n)))
      (throw (ex-info "WAL pipeline limits must be positive integers" {:value n}))))
  (let [gate (ReentrantLock.)
        state {:wal wal-state :db-identity db-identity :generation (Object.)
               :failure (volatile! nil) :gate gate
               :capacity-changed (.newCondition gate)
               :publication-changed (.newCondition gate)
               :application-waiters (TreeMap.)
               :completion-waiters (TreeMap.)
               :preparation (ReentrantLock. true) :lifetime (lifetime/create)
               :io-lifetime (lifetime/create)
               :prune-lock (ReentrantLock.)
               :prune-pending? (AtomicBoolean. false)
               :prune-step (long prune-step-records)
               :entries (ConcurrentSkipListMap.)
               :sync-completion (volatile! nil)
               :ready-prefix (volatile! nil)
               :budget (PendingBudget. wal-pending-max-bytes wal-pending-max-requests)
               :capacity-waiters (long-array 1)
               :root (volatile! (view/empty-root applied-lsn))
               :pruned (volatile! (long applied-lsn))
               :pins (volatile! {})
               :max-bytes (long wal-pending-max-bytes)
               :max-requests (long wal-pending-max-requests)
               :apply-timeout-ms (long wal-apply-timeout-ms)
               :range-max-records (long application-max-records)
               :range-max-bytes (long application-max-bytes)
               :sealed (volatile! (long applied-lsn))
               :next-unmarked (volatile! (inc (long applied-lsn)))
               :applied (volatile! (long applied-lsn))
               :published (volatile! (long applied-lsn))
               :owner (volatile! nil) :apply-range! apply-range! :hooks hooks}]
    (locking (:append-lock wal-state)
      (let [slot (:application-hooks wal-state)]
        (when @slot
          (throw (ex-info "WAL already has an application runtime"
                          {:error :txlog/write-protocol-in-use})))
        (vreset! slot
                 {:io-lifetime (:io-lifetime state)
                  :check-admission! #(check-admission! state)
                  :on-failure! #(fail! state :wal-failed %)
                  :capture-sync-prefix!
                  (fn [_ round ch]
                    (capture-ready-sync-prefix! state round ch))
                  :before-sync! (fn [_ round]
                                  (seal-through! state (:target-lsn round))
                                  (phase! :sync-target-sealed round)
                                  (phase! :before-sync round))
                  :after-sync! (fn [_ round]
                                 (mark-eligible! state)
                                 (phase! :after-sync round))})))
    state))

(defn check-admission!
  "One volatile failure read on the healthy insertion path."
  [state]
  (when-let [failure @(:failure state)]
    (throw (ex-info "WAL engine is fenced; recover the original environment"
                    {:error :txlog/admission-closed :outcome :not-committed
                     :reason (:reason failure) :retryable? false}
                    (:cause failure)))))

(defn fail!
  "Fence new admission and wake application/capacity waiters. Do not wait for
  insertion: its current owner may be stuck in I/O. Its lease prevents teardown,
  and its eventual return cannot publish success after the fence."
  [state reason cause]
  (with-lock (:gate state)
    (when-not @(:failure state)
      (vreset! (:failure state) {:reason reason :cause cause}))
    (.signalAll ^Condition (:capacity-changed state))
    (.signalAll ^Condition (:publication-changed state))
    (doseq [range (.values ^TreeMap (:application-waiters state))]
      (.signalAll ^Condition (:changed (:completion range))))
    (doseq [entry (.values ^TreeMap (:completion-waiters state))]
      (.signalAll ^Condition (:changed @(:wait-state entry)))))
  (lifetime/fence! (:lifetime state))
  nil)

(defn usage [state]
  (let [^PendingBudget$Usage used (.snapshot ^PendingBudget (:budget state))]
    {:bytes (.-bytes used) :requests (.-requests used)}))

(defn capacity-waiters ^long [state]
  (with-lock (:gate state) (aget ^longs (:capacity-waiters state) 0)))

(defn reserve!
  "Reserve owned bytes and one request before collection/preparation. The
  available-budget path uses atomic accounting, not the application mutex.
  Capacity waits recheck under the condition lock so releases cannot be lost."
  [state bytes timeout-ms]
  (let [bytes (long bytes) limit (lifetime/deadline timeout-ms)
        ^PendingBudget budget (:budget state)
        waiting? (volatile! false)]
    (when (or (neg? bytes) (> bytes (long (:max-bytes state))))
      (throw (ex-info "Write exceeds the pending WAL budget"
                      {:error :txlog/pending-capacity :outcome :not-committed
                       :bytes bytes :retryable? false})))
    (check-admission! state)
    (if (.tryReserve budget bytes)
      (->Reservation bytes (AtomicBoolean. false) (AtomicBoolean. false) state limit)
      (with-lock (:gate state)
        (try
          (loop []
            (check-admission! state)
            (let [remaining (- limit (lifetime/nano-time))]
              (cond
                (.tryReserve budget bytes)
                (->Reservation bytes (AtomicBoolean. false) (AtomicBoolean. false) state limit)
                (not (pos? remaining))
                (throw (ex-info "Timed out waiting for pending WAL capacity"
                                {:error :txlog/pending-capacity :outcome :not-committed
                                 :retryable? true}))
                :else
                (do
                  ;; Admission pressure: reclaim completed credits promptly.
                  (schedule-reclaim! state)
                  (when-not @waiting?
                    (let [^longs waiters (:capacity-waiters state)]
                      (when (>= (aget waiters 0) (.-requests (.snapshot budget)))
                        (throw (ex-info "Pending WAL admission is busy"
                                        {:error :server/busy :outcome :not-committed
                                         :retryable? true})))
                      (aset waiters 0 (inc (aget waiters 0)))
                      (vreset! waiting? true)
                      (phase! :capacity-wait state)))
                  (.awaitNanos ^Condition (:capacity-changed state) remaining)
                  (recur)))))
          (finally
            (when @waiting?
              (let [^longs waiters (:capacity-waiters state)]
                (aset waiters 0 (dec (aget waiters 0)))))))))))

(defn- release-under-gate! [state reservation]
  (if (instance? GroupReservation reservation)
    (reduce (fn [released? member]
              (or (release-under-gate! state member) released?))
            false (:members reservation))
    (when (.compareAndSet ^AtomicBoolean (:released? reservation) false true)
      (.release ^PendingBudget (:budget state) (long (:bytes reservation)))
      true)))

(defn release!
  "Release a reservation once its owned state is no longer retained."
  [reservation]
  (let [state (:runtime reservation)]
    (with-lock (:gate state)
      (when (and (release-under-gate! state reservation)
                 (pos? (aget ^longs (:capacity-waiters state) 0)))
        ;; Reservations have different sizes; waking only a large request can
        ;; strand smaller requests that fit. Only capacity waiters wake here.
        (.signalAll ^Condition (:capacity-changed state))))))

(defn preparation-remaining-ms
  "Remaining admission/preparation budget, never restarted by queue handoff."
  ^long [reservation]
  (let [remaining (- (long (:deadline reservation)) (lifetime/nano-time))]
    (when-not (pos? remaining)
      (throw (ex-info "WAL request expired before append"
                      {:error :txlog/preparation-timeout :outcome :not-committed
                       :retryable? true})))
    (quot (+ remaining 999999) 1000000)))

(defn outcome
  "Classify this request using established WAL/native state, never group LSNs."
  [state entry reason cause]
  (let [lsn (long @(:lsn entry))
        batch @(:append-batch entry)
        identity (wal/append-identity batch lsn)
        committed? (and (pos? lsn) (= lsn (:lsn identity))
                        (wal/durable-append? (:wal state) batch lsn))]
    (ex-info (if committed? "WAL write committed; application requires reconciliation"
                 "WAL write outcome is indeterminate; reconcile its original LSN")
             (cond-> {:error (if committed? :txlog/write-committed
                                 :txlog/write-indeterminate)
                      :outcome (if committed? :committed :indeterminate)
                      :wal-status (if committed? :durable
                                      (if batch :appended :unknown))
                      :durability-profile (:durability-profile (:wal state))
                      :db-identity (:db-identity state) :reason reason
                      :applied? @(:applied? entry) :retryable? false}
               (pos? lsn) (assoc :txlog-lsn lsn)
               identity (assoc :txlog-record identity)
               (not committed?) (assoc :indeterminate? true))
             cause)))

(defn prepare-record
  "Shared WAL/application outcome for one collected group. No caller results
  or reservations are retained here until the sealed record is registered."
  []
  (->Entry (volatile! 0) (volatile! nil) (volatile! nil)
           (volatile! nil) nil nil (volatile! nil)
           (volatile! false) (volatile! false) (volatile! nil)))

(defn prepare-entry
  "Retain a request's result and admission credits. Collected requests share
  record's LSN and completion cells; their rows remain private until sealing."
  ([reservation rows result]
   (->Entry (volatile! 0) (volatile! nil) (volatile! nil)
            (volatile! rows) result reservation (volatile! nil)
            (volatile! false) (volatile! false) (volatile! nil)))
  ([reservation rows result record]
   (->Entry (:lsn record) (:append-batch record) (:error record)
            (volatile! rows) result reservation (:range record)
            (:applied? record) (:complete? record) (:wait-state record))))

(defn append-batch!
  "Insert a sealed collection as one atomic WAL record. Members must share
  completion cells, but retain their own results. Optional
  bodies align with entries: owned WAL bytes for preencoded requests, nil for
  RMW requests whose rows become known only under preparation. Neither entries
  nor shared append metadata retain the bodies after insertion. The retained
  record owns combined rows/reservations, never other callers' return values."
  [state entries {:keys [expected-root root bodies]}]
  (when (seq entries)
    (let [attempted? (volatile! false)
          lo (inc (long (:lsn expected-root)))
          hooks (:hooks state)
          own-turn? (not (.isHeldByCurrentThread ^ReentrantLock (:preparation state)))
          turn (when own-turn?
                 (acquire-preparation! state (:reservation (first entries))
                                       (preparation-remaining-ms
                                        (:reservation (first entries)))))]
      (try
        ;; Callers may supply a staged base from before acquisition. Reject a
        ;; head that advanced before installation, before consuming reservations
        ;; or entering WAL. Body replay is an explicit adapter contract.
        ;; Compare LSN, not identity: prune! may replace the root in place with
        ;; an equivalent pruned tree at the same LSN.
        (when-not (= (long (:lsn expected-root)) (long (:lsn @(:root state))))
          (throw (ex-info "Stale optimistic WAL preparation"
                          {:error :txlog/stale-preparation :outcome :not-committed
                           :datalevin.tx-group/body-failure true})))
        (when (and bodies (not= (count entries) (count bodies)))
          (throw (IllegalArgumentException. "WAL bodies must match prepared entries")))
        (doseq [entry entries]
          (when-not (every? #(identical? (get (first entries) %) (get entry %))
                            [:lsn :append-batch :error :range :applied? :complete?
                             :wait-state])
            (throw (IllegalArgumentException. "Collected requests must share record state")))
          (let [reservation (:reservation entry)]
            (when-not (and (identical? state (:runtime reservation))
                           (not (.get ^AtomicBoolean (:released? reservation)))
                           (.compareAndSet ^AtomicBoolean (:appended? reservation) false true))
              (throw (IllegalArgumentException. "Invalid batch reservation")))))
        (let [record (if (= 1 (count entries))
                       (first entries)
                       (prepare-entry
                        (->GroupReservation
                         (reduce (fn [^long n entry]
                                   (+ n (long (:bytes (:reservation entry))))) 0 entries)
                         state (mapv :reservation entries))
                        (into [] (mapcat #(deref (:rows %))) entries)
                        nil (first entries)))]
          (wal/append-prepared-batch-pending!
           (:wal state) lo
           (mapv (fn [entry body]
                   (or body (wal/prepare-append-body @(:rows entry) hooks)))
                 entries (or bodies (repeat nil)))
           (assoc hooks
                  :throw-if-fatal!
                  (fn [wal-state]
                    (check-admission! state)
                    (phase! :admission-checked state)
                    (doseq [entry entries]
                      (preparation-remaining-ms (:reservation entry)))
                    (when-not (and (= (long (:lsn expected-root))
                                      (long (:lsn @(:root state))))
                                   (= (long (:lsn root)) lo)
                                   (.isHeldByCurrentThread ^ReentrantLock (:preparation state)))
                      (throw (ex-info "Stale WAL preparation"
                                      {:error :txlog/stale-preparation :outcome :not-committed})))
                    (when-let [check (:throw-if-fatal! hooks)] (check wal-state)))
                  :before-append!
                  (fn [wal-state]
                    (when-let [before (:before-append! hooks)] (before wal-state))
                    (phase! :before-append state)
                    (vreset! (:lsn record) lo)
                    (vreset! attempted? true))
                  :register-appends!
                  (fn [_ batch]
                    ;; Publish the retained entry and its root atomically under
                    ;; the gate. prune! installs a rebuilt root only when the
                    ;; exact base is still current; an unguarded root write
                    ;; could otherwise clobber that rebuild with an older LSN.
                    (with-lock (:gate state)
                      (vreset! (:append-batch record) batch)
                      (.put ^ConcurrentSkipListMap (:entries state) lo record)
                      (vreset! (:root state) root)))))
          (when (> (count entries) 1)
            (doseq [entry entries] (vreset! (:rows entry) nil))))
        (doseq [entry entries] (phase! :receipt-published entry))
        (catch Throwable e
          (cond
            (= :txlog/stale-preparation (:error (ex-data e)))
            (throw e)
            @attempted?
            (do
              (fail! state :append-failed e)
              (doseq [entry entries]
                (vreset! (:error entry) (outcome state entry :append-failed e))))
            :else
            (doseq [entry entries]
              (release! (:reservation entry))
              (vreset! (:error entry) e))))
        (finally
          (when turn (release-preparation! turn))))))
  entries)

(defn append!
  "Insert one prepared request. The caller owns preparation, never a native
  writer. A failure after insertion starts is terminal and cannot replay a body."
  ([state reservation rows result]
   (append! state reservation rows result nil))
  ([state reservation rows result prepared]
   (when-not (identical? state (:runtime reservation))
     (throw (IllegalArgumentException. "Reservation belongs to another runtime")))
   (when (or (.get ^AtomicBoolean (:released? reservation))
             (not (.compareAndSet ^AtomicBoolean (:appended? reservation) false true)))
     (throw (IllegalArgumentException. "Reservation has already been consumed")))
   (let [entry (prepare-entry reservation rows result)
         attempted? (volatile! false)
         hooks (:hooks state)]
     (try
       (let [batch
             (wal/append-pending!
              (:wal state) rows
              (assoc hooks
                     :throw-if-fatal!
                     (fn [wal-state]
                       (check-admission! state)
                       (phase! :admission-checked state)
                       (when prepared
                         (when-not (and (= (long (:lsn (:expected-root prepared))) (long (:lsn @(:root state))))
                                        (.isHeldByCurrentThread ^ReentrantLock
                                         (:preparation state))
                                        (= (long (:lsn (:root prepared)))
                                           (long @(:next-lsn wal-state))))
                           (throw (ex-info "Stale WAL preparation"
                                           {:error :txlog/stale-preparation
                                            :outcome :not-committed}))))
                       (when-let [check (:throw-if-fatal! hooks)] (check wal-state)))
                     :before-append!
                     (fn [wal-state]
                       (when-let [before (:before-append! hooks)] (before wal-state))
                       (phase! :before-append state)
                       (vreset! (:lsn entry) (long @(:next-lsn wal-state)))
                       (vreset! attempted? true))
                     :register-append!
                     (fn [_ batch]
                       (vreset! (:lsn entry) (long (append/first-lsn batch)))
                       (vreset! (:append-batch entry) batch)
                       (with-lock (:gate state)
                         (.put ^ConcurrentSkipListMap (:entries state)
                               (append/first-lsn batch) entry)
                         (when prepared (vreset! (:root state) (:root prepared)))))))]
         (vreset! (:append-batch entry) batch)
         (phase! :receipt-published entry)
         entry)
       (catch Throwable e
         (if @attempted?
           (do (fail! state :append-failed e)
               (throw (outcome state entry :append-failed e)))
           (do (release! reservation) (throw e))))))))

(defn acquire-preparation!
  "Acquire ordered preparation after capacity admission. Its token never owns
  the native writer and must be released before any durability/application wait."
  [state reservation timeout-ms]
  (when-not (and (identical? state (:runtime reservation))
                 (not (.get ^AtomicBoolean (:released? reservation))))
    (throw (IllegalArgumentException. "A live reservation is required")))
  (let [^ReentrantLock turn (:preparation state)
        acquired? (if timeout-ms
                    (.tryLock turn (max 0 (long timeout-ms)) TimeUnit/MILLISECONDS)
                    (do (.lockInterruptibly turn) true))]
    (when-not acquired?
      (throw (ex-info "Timed out acquiring WAL preparation"
                      {:error :txlog/preparation-timeout :outcome :not-committed
                       :retryable? true})))
    (try
      (check-admission! state)
      (let [token {:runtime state :generation (:generation state)
                   :thread (Thread/currentThread) :released? (AtomicBoolean. false)}]
        (phase! :preparation-acquired token)
        token)
      (catch Throwable e (.unlock turn) (throw e)))))

(defn ^:redef release-preparation!
  "Release the ordered preparation turn. Redefinable so timing probes can
  measure how long the turn is actually held."
  [token]
  (when-not (identical? (:thread token) (Thread/currentThread))
    (throw (IllegalStateException. "Preparation must be released by its owner")))
  (when (.compareAndSet ^AtomicBoolean (:released? token) false true)
    (.unlock ^ReentrantLock (:preparation (:runtime token)))
    (prune! (:runtime token))))

(defn capture-view
  "Pin a matched native base and immutable head. open-reader returns
   {:reader r :base-lsn n :close! f}; base must come from that reader's own
   native transaction. The caller supplies the ordered head it prepared
   against, so a body can run without the preparation turn; a stale head is
   rejected here or, at the latest, by append-batch!'s token-guarded install.
   A reader opened just before an application commit can lag the published
   marker; re-open it a bounded number of times. A failed capture always
   closes its reader."
  [state base-root open-reader]
  (check-admission! state)
  (let [lease (lifetime/enter! (:lifetime state))
        head-lsn (long (:lsn base-root))]
    (try
      (loop [attempt 0]
        (let [pin (Object.)
              _ (with-lock (:gate state)
                  (let [published (long @(:published state))]
                    (vswap! (:pins state) assoc pin published)))
              opened (try (open-reader)
                          (catch Throwable e
                            (with-lock (:gate state)
                              (vswap! (:pins state) dissoc pin))
                            (throw e)))
              outcome
              (try
                (let [base (long (:base-lsn opened))
                      published (with-lock (:gate state)
                                  (check-admission! state)
                                  (long @(:published state)))]
                  (when-not (<= (long published) base head-lsn)
                    (throw (ex-info "Native snapshot does not match WAL head"
                                    {:error :txlog/view-mismatch :base-lsn base
                                     :published published :head head-lsn
                                     :state-lsn (long (:lsn @(:root state)))
                                     :attempt attempt})))
                  (with-lock (:gate state)
                    (vswap! (:pins state) assoc pin base))
                  (assoc opened :root base-root :pin pin :lease lease :runtime state
                         :generation (:generation state) :released? (AtomicBoolean. false)
                         :reader-closed? (AtomicBoolean. false)))
                (catch Throwable e
                  (with-lock (:gate state) (vswap! (:pins state) dissoc pin))
                  ((:close! opened))
                  (if (and (= :txlog/view-mismatch (:error (ex-data e)))
                           (< attempt 8))
                    ::retry
                    (throw e))))]
          (if (= ::retry outcome)
            (do (LockSupport/parkNanos 1000) (recur (inc attempt)))
            outcome)))
      (catch Throwable e (.close ^AutoCloseable lease) (throw e)))))

(defn- close-view-reader! [snapshot]
  (when (and (:close! snapshot)
             (.compareAndSet ^AtomicBoolean (:reader-closed? snapshot) false true))
    (try ((:close! snapshot))
         (finally (.close ^AutoCloseable (:lease snapshot))))))

(defn detach-view!
  "Close a captured native reader on its owning thread while retaining its
  immutable-root pin. The returned descriptor may cross threads; it contains
  no reader or native lease and must still be released with release-view!."
  [snapshot]
  (close-view-reader! snapshot)
  (dissoc snapshot :reader :close! :lease :root))

(defn release-view! [snapshot]
  (when (.compareAndSet ^AtomicBoolean (:released? snapshot) false true)
    (try (close-view-reader! snapshot)
         (finally
           (let [state (:runtime snapshot)]
             (with-lock (:gate state)
               (vswap! (:pins state) dissoc (:pin snapshot)))
             (prune! state))))))

(defn- release-completed-entries!
  "Under gate. Drop rows and return capacity for completed entries whose deltas
  are already absent from the published root. Never runs the tree rebuild."
  [state]
  (let [pruned (long @(:pruned state))
        floor (reduce min pruned (vals @(:pins state)))
        ^ConcurrentSkipListMap entries (:entries state)
        released?
        (reduce
         (fn [released? [lsn entry]]
           (if @(:complete? entry)
             (do
               (vreset! (:rows entry) nil)
               (.remove entries lsn entry)
               (or (release-under-gate! state (:reservation entry)) released?))
             released?))
         false (.headMap entries (Long/valueOf (long floor)) true))]
    (when (and released? (pos? (aget ^longs (:capacity-waiters state) 0)))
      (.signalAll ^Condition (:capacity-changed state)))))

(defonce ^:private reclaim-executor
  (Executors/newSingleThreadExecutor
   (reify ThreadFactory
     (newThread [_ r] (doto (Thread. r "datalevin-reclaim") (.setDaemon true))))))

(def ^:private reclaim-passes-per-task 8)

(defn- reclaim-pending? [state]
  (> (long @(:published state)) (long @(:pruned state))))

(defn- run-prune-pass!
  "One coalesced, bounded reclamation pass. Rebuilds at most :prune-step records
  of the published prefix so a caller never pays for an unbounded rebuild.
  view/prune is pure, computed from a root snapshot outside the gate, and
  installed only while that exact root is still current; the reservation/row
  scan runs under the gate and does no tree work."
  [state]
  (let [^ReentrantLock prune-lock (:prune-lock state)]
    (when (.tryLock prune-lock)
      (try
        (let [published (long @(:published state))
              pruned    (long @(:pruned state))
              target    (min published (+ pruned (long (:prune-step state))))]
          (when (> target pruned)
            (let [base @(:root state)
                  next-root (do (phase! :before-delta-prune state)
                                (view/prune base target))]
              (with-lock (:gate state)
                (when (and (identical? base @(:root state))
                           (> target (long @(:pruned state))))
                  ;; Deterministic seam: a root publication from a concurrent
                  ;; append must wait for this install, never interleave.
                  (phase! :before-prune-install state)
                  (vreset! (:root state) next-root)
                  (vreset! (:pruned state) target)))))
          (with-lock (:gate state) (release-completed-entries! state)))
        (finally (.unlock prune-lock))))))

(defn- schedule-reclaim!
  "Coalesce reclamation and finish it off the caller's path. A queued or running
  pass absorbs later requests; remaining work is rescheduled after each bounded
  batch of passes, so one state cannot monopolize the shared reclaimer."
  [state]
  (when (.compareAndSet ^AtomicBoolean (:prune-pending? state) false true)
    (.execute ^ExecutorService reclaim-executor
              (fn []
                (try
                  (dotimes [_ reclaim-passes-per-task]
                    (when (and (nil? @(:failure state)) (reclaim-pending? state))
                      (run-prune-pass! state)))
                  (finally (.set ^AtomicBoolean (:prune-pending? state) false)))
                (when (and (nil? @(:failure state)) (reclaim-pending? state))
                  (schedule-reclaim! state))))))

(defn prune!
  "Bounded reclamation coalesced to the publication watermark. A publication
  that advances past one pass is finished by the separate reclaimer rather than
  by looping on the caller's return path. Never takes the preparation lock and
  never waits: a concurrent pass owns this round."
  [state]
  (run-prune-pass! state)
  (when (reclaim-pending? state) (schedule-reclaim! state)))

(defn seal-through!
  "Fix application membership at the captured appended-prefix boundary. Later
  arrivals cannot join these ranges even if their bytes share the same force."
  [state target]
  (with-lock (:gate state)
    (loop [lo (inc (long @(:sealed state)))]
      (when (<= lo (long target))
        (let [entries
              (loop [lsn lo bytes 0 entries []]
                (if (or (> lsn (long target))
                        (>= (count entries) (long (:range-max-records state))))
                  entries
                  (let [entry (.get ^ConcurrentSkipListMap (:entries state) lsn)]
                    (when-not entry
                      (throw (ex-info "Missing retained WAL entry"
                                      {:error :txlog/pending-gap :lsn lsn})))
                    (let [size (long (:bytes (:reservation entry)))]
                      (if (and (seq entries)
                               (> (+ bytes size) (long (:range-max-bytes state))))
                        entries
                        (recur (inc lsn) (+ bytes size) (conj entries entry)))))))
              hi (long @(:lsn (peek entries)))
              range (->ApplicationRange
                     lo hi entries (volatile! nil)
                     (->ApplicationCompletion (.newCondition ^ReentrantLock (:gate state))
                                               (long-array 1) (volatile! false)))]
          (doseq [entry entries] (vreset! (:range entry) range))
          (vreset! (:sealed state) hi)
          (recur (inc hi)))))))

(defn capture-ready-sync-prefix!
  "Extend a claimed force to a bounded, already-appended prefix on its own
  segment. No preparation, collector, or native writer is awaited here. The
  append registry is published before the sync manager advertises each LSN."
  [state round ch]
  (phase! :before-sync-prefix-capture round)
  (let [base (long (:target-lsn round))
        latest (long @(:last-appended-lsn (:sync-manager (:wal state))))
        target (min latest (+ base (long (:max-requests state))))
        entry (when (> target base)
                (.get ^ConcurrentSkipListMap (:entries state)
                      (Long/valueOf target)))
        batch (some-> entry :append-batch deref)]
    ;; An undurable claimed prefix pins its segment against rotation. Checking
    ;; the final entry's channel still protects this boundary if rotation changes.
    (if (and batch (identical? ch (append/channel batch))) target base)))

(defn- eligible? [state range]
  (or (= :relaxed (:durability-profile (:wal state)))
      (<= (long (:hi range))
          (long @(:last-durable-lsn (:sync-manager (:wal state)))))))

(defn- signal-completion-waiter!
  "Under gate, offer progress to one parked caller in the indicated LSN prefix.
  Its condition is shared by the record, including callers parked before seal."
  [state after through]
  (when-let [waiting (.higherEntry ^TreeMap (:completion-waiters state) (long after))]
    (when (<= (long (.getKey waiting)) (long through))
      (let [entry (.getValue waiting)]
        (phase! :completion-notified entry)
        (.signal ^Condition (:changed @(:wait-state entry)))
        true))))

(defn- signal-next-sync!
  "The next force needs one caller from the uncovered tail, not a broadcast to
  every follower of the settled round. Called under gate."
  [state]
  (when (and (nil? @(:sync-completion state)) (nil? @(:failure state)))
    (signal-completion-waiter!
     state @(:last-durable-lsn (:sync-manager (:wal state))) Long/MAX_VALUE)))

(defn- signal-next-applicant!
  "Under gate, wake one registered waiter to help the oldest eligible prefix.
  Its own range can be later: a prefix with no original caller must still
  progress. Only waiters are consulted here, so eligibility scanning of the
  retained index stays confined to mark-eligible!. A later waiting range is
  eligible only when every earlier range is, because durability is a watermark."
  [state]
  (when (and (nil? @(:owner state)) (nil? @(:failure state)))
    (let [^TreeMap waiting (:application-waiters state)]
      (or (when-let [entry (.higherEntry waiting (long @(:published state)))]
            (let [range (.getValue entry)]
              (when (eligible? state range)
                (phase! :application-notified range)
                (.signal ^Condition (:changed (:completion range)))
                true)))
          (signal-completion-waiter!
           state @(:published state)
           @(:last-durable-lsn (:sync-manager (:wal state))))))))

(defn- mark-eligible! [state]
  (with-lock (:gate state)
    (let [^ConcurrentSkipListMap entries (:entries state)
          cursor (:next-unmarked state)]
      ;; claim! can mark/apply a range before this hook visits it. Pruning may
      ;; then remove its entries, so resume no earlier than the published floor.
      (loop [lsn (max (long @cursor) (inc (long @(:published state))))]
        (vreset! cursor lsn)
        (when-let [entry (.get entries (Long/valueOf lsn))]
          (when-let [range @(:range entry)]
            (when (eligible? state range)
              (when-not @(:deadline range)
                (vreset! (:deadline range)
                         (lifetime/deadline (:apply-timeout-ms state)))
                ;; New eligibility can extend the coalesced application prefix.
                (vreset! (:ready-prefix state) nil))
              (recur (inc (long (:hi range)))))))))
    (signal-next-applicant! state)))

(defn- next-range [state]
  (some-> (.get ^ConcurrentSkipListMap (:entries state)
                (inc (long @(:published state)))) :range deref))

(defn- ready-application-prefix
  "Combine sealed, durable ranges only at the native application turn. Sync
  boundaries stay fixed; no later append can join a range already being applied."
  [state first-range now]
  (let [^ConcurrentSkipListMap index (:entries state)
        next (some-> (.get index (Long/valueOf (inc (long (:hi first-range)))))
                     :range deref)]
    (if (or (nil? next)
            (not (eligible? state next))
            (>= (count (:entries first-range)) (long (:range-max-records state))))
      first-range
      (let [first-entries (:entries first-range)
            first-count (count first-entries)
            first-bytes (reduce (fn [n entry]
                                  (+ n (long (:bytes (:reservation entry)))))
                                0 first-entries)
            max-count (long (:range-max-records state))
            max-bytes (long (:range-max-bytes state))]
        (loop [hi (long (:hi first-range))
               n first-count
               bytes first-bytes
               entries (transient first-entries)
               deadline (long @(:deadline first-range))
               joined? false]
          (let [next (some-> (.get index (Long/valueOf (inc hi))) :range deref)
                next-entries (:entries next)
                next-count (count next-entries)
                next-bytes (reduce (fn [n entry]
                                     (+ n (long (:bytes (:reservation entry)))))
                                   0 next-entries)]
            (if (and next (eligible? state next)
                     (<= (+ n next-count) max-count)
                     (<= (+ bytes next-bytes) max-bytes))
              (let [limit (or @(:deadline next)
                              (let [value (lifetime/deadline (:apply-timeout-ms state))]
                                (vreset! (:deadline next) value)
                                value))]
                (if (<= (long limit) now)
                  (if joined?
                    (->ApplicationRange (:lo first-range) hi
                                        (persistent! entries) (volatile! deadline) nil)
                    first-range)
                  (recur (long (:hi next)) (+ n next-count)
                         (+ bytes next-bytes)
                         (reduce conj! entries next-entries)
                         (min deadline (long limit)) true)))
              (if joined?
                (->ApplicationRange (:lo first-range) hi (persistent! entries)
                                    (volatile! deadline) nil)
                first-range))))))))

(defn await-publication!
  "Wait without a reader snapshot for a native prefix to be published. Public
  readers never expose a commit that a late application failure has fenced."
  [state lsn]
  (loop []
    (let [done?
          (with-lock (:gate state)
            (check-admission! state)
            (if (<= (long lsn) (long @(:published state)))
              true
              (let [deadline (some-> (next-range state) :deadline deref)
                    remaining (when deadline (- (long deadline) (lifetime/nano-time)))]
                (if (or (nil? remaining) (pos? (long remaining)))
                  (do
                    (try (if remaining
                           (.awaitNanos ^Condition (:publication-changed state) (long remaining))
                           (.await ^Condition (:publication-changed state)))
                         (catch InterruptedException e
                           (.interrupt (Thread/currentThread))
                           (throw e)))
                    false)
                  ::expired))))]
      (when (= ::expired done?)
        (fail! state :application-timeout
               (ex-info "Native commit was not published before its deadline"
                        {:error :txlog/application-timeout})))
      (when-not (true? done?) (recur)))))

(defn- ready-claim-prefix
  "Under gate. Reuse the application prefix already computed for this
  publication epoch, so a burst of resource-free claim takeovers shares one
  coalesced range instead of rebuilding it per caller. Newly eligible
  durability and a new publication both invalidate the cache below."
  [state range now]
  (let [published (long @(:published state))
        cached @(:ready-prefix state)]
    (if (and (some? cached) (= (long (:published cached)) published))
      (:range cached)
      (let [ready (ready-application-prefix state range now)]
        (phase! :application-prefix-computed published)
        (vreset! (:ready-prefix state) {:published published :range ready})
        ready))))

(defn- claim!
  "Called under gate. A resource-free :claimed owner has not entered native
  work and is replaceable: its later apply is token-fenced, so only one caller
  can transition to native ownership. Once :applying, ownership is never
  replaced while the thread is live; native work ends through completion or
  failure. No claimed-owner polling interval is used."
  [state]
  (when-let [range (next-range state)]
      (when (eligible? state range)
        (when-not @(:deadline range)
          (vreset! (:deadline range) (lifetime/deadline (:apply-timeout-ms state))))
        (let [now (lifetime/nano-time)
              previous @(:owner state)
              transferable? (or (nil? previous)
                                (= :claimed (:phase previous)))]
          (when (and (nil? @(:failure state))
                     (< now (long @(:deadline range)))
                     transferable?)
            (let [ready (ready-claim-prefix state range now)
                  owner (->ApplicationOwner ready (Object.) :claimed (:generation state)
                                            (Thread/currentThread))]
              (vreset! (:owner state) owner)
              ;; Keep a helper runnable across the resource-free claim seam.
              ;; A healthy owner enters native work before that caller checks;
              ;; a stopped owner can be replaced without waking the whole range.
              (signal-completion-waiter!
               state @(:published state)
               @(:last-durable-lsn (:sync-manager (:wal state))))
              owner))))))

(defn- complete-application!
  "Publish each original range's shared completion once, including ranges
  coalesced into one native commit. Called under gate after store publication."
  [entries]
  (doseq [entry entries]
    (vreset! (:complete? entry) true)
    (when-let [waiting @(:wait-state entry)]
      (when (pos? (aget ^longs (:waiters waiting) 0))
        (.signalAll ^Condition (:changed waiting))))
    (let [range @(:range entry)
          completion (:completion range)]
      (when-not @(:complete? completion)
        (vreset! (:complete? completion) true)
        (phase! :application-completed range)
        (when (pos? (aget ^longs (:waiters completion) 0))
          (.signalAll ^Condition (:changed completion)))))))

(defn- apply-owned! [state owner]
  (let [range (:range owner)
        entries (:entries range)
        lease (with-lock (:gate state)
                (when (and (identical? owner @(:owner state))
                           (identical? (:generation owner) (:generation state))
                           (nil? @(:failure state)))
                  (when (.isInterrupted ^Thread (:thread owner))
                    (throw (InterruptedException. "Application owner interrupted before native entry")))
                  (let [lease (lifetime/enter! (:lifetime state))]
                    (vreset! (:owner state) (assoc owner :phase :applying))
                    (doseq [entry entries] (vreset! (:applied? entry) :unknown))
                    lease)))]
    (when lease
      (try
        (phase! :application-acquired range)
        ((:apply-range! state) entries (:token owner))
        (doseq [entry entries] (vreset! (:applied? entry) true))
        (phase! :after-native-commit range)
        (phase! :before-publication range)
        (with-lock (:gate state)
          (vreset! (:applied state) (long (:hi range)))
          (when-not @(:failure state)
            (vreset! (:published state) (long (:hi range)))
            (vreset! (:ready-prefix state) nil)
            (complete-application! entries)
            (.signalAll ^Condition (:publication-changed state)))
          ;; Native application and publication are done. Pruning may rebuild
          ;; persistent nodes; let the next range take the application turn.
          (when (identical? (:token owner) (:token @(:owner state)))
            (vreset! (:owner state) nil))
          (signal-next-applicant! state)
          (phase! :after-publication range))
        ;; Publication is complete. Reclaim off the caller's return path; the
        ;; scheduled pass is coalesced and bounded, and admission pressure
        ;; triggers it promptly.
        (schedule-reclaim! state)
        (catch Throwable e
          (when (contains? (ex-data e) :applied?)
            (doseq [entry entries] (vreset! (:applied? entry) (:applied? (ex-data e)))))
          (fail! state :application-failed e))
        (finally
          (.close ^AutoCloseable lease)
          (with-lock (:gate state)
            (when (identical? (:token owner) (:token @(:owner state)))
              (vreset! (:owner state) nil)
              (signal-next-applicant! state))))))))

(defn- await-application-turn!
  "Check/claim/register atomically under gate, then wait on this range's own
  condition. Wake on completion, eligibility/turn handoff, failure, or the
  real range deadline; there is no claimed-owner polling interval."
  [state entry]
  (with-lock (:gate state)
    (if (or @(:complete? entry) @(:failure state))
      nil
      (or (claim! state)
          (let [range @(:range entry)
                completion (:completion range)
                next-limit (some-> (next-range state) :deadline deref)
                own-limit (some-> range :deadline deref)
                limit (when (or next-limit own-limit)
                        (min (long (or next-limit Long/MAX_VALUE))
                             (long (or own-limit Long/MAX_VALUE))))
                remaining (when limit (- (long limit) (lifetime/nano-time)))]
            (when-not completion
              (throw (ex-info "Appended WAL entry has no application range"
                              {:error :txlog/pending-gap :lsn @(:lsn entry)})))
            (if (and remaining (not (pos? (long remaining))))
              ::expired
              (let [^longs waiters (:waiters completion)
                    ^TreeMap waiting (:application-waiters state)]
                (when (zero? (aget waiters 0)) (.put waiting (:lo range) range))
                (aset waiters 0 (inc (aget waiters 0)))
                (try
                  (phase! :application-waiting range)
                  (if remaining
                    (.awaitNanos ^Condition (:changed completion) (long remaining))
                    (.await ^Condition (:changed completion)))
                  nil
                  (finally
                    (aset waiters 0 (dec (aget waiters 0)))
                    (when (zero? (aget waiters 0)) (.remove waiting (:lo range))))))))))))

(defn- await-application!
  [state entry]
  (loop []
    (cond
      @(:complete? entry)
      (:result entry)
      @(:failure state)
      (let [{:keys [reason cause]} @(:failure state)]
        (throw (outcome state entry reason cause)))
      :else
      (let [owner (await-application-turn! state entry)]
        (cond
          (= ::expired owner)
            (fail! state :application-timeout
                   (ex-info "WAL application turn timed out"
                            {:error :txlog/application-timeout}))
          owner
          (try
            (phase! :application-claimed owner)
            (apply-owned! state owner)
            (catch Throwable e
              ;; Include failures/interruption between claim and native entry.
              ;; Fence before releasing the claim so no successor starts first.
              (fail! state :application-wait-failed e)
              (throw e))
            (finally
              (with-lock (:gate state)
                (when (and (identical? (:token owner) (:token @(:owner state)))
                           (= :claimed (:phase @(:owner state))))
                  (vreset! (:owner state) nil)
                  (signal-next-applicant! state))))))
        (recur)))))

(defn- await-sync-progress!
  "A strict follower stays on its record's condition through sync and native
  application. Only publication/failure or an available helping turn ends this
  wait. Register and test under gate so seal, force and publication cannot lose
  a notification. Deadlines change from WAL to application without renewal."
  [state entry completion]
  (let [lsn (long @(:lsn entry))
        manager (:sync-manager (:wal state))]
    (with-lock (:gate state)
      (loop []
        (cond
          @(:complete? entry) nil
          @(:error completion) (throw @(:error completion))
          @(:failure state)
          (let [{:keys [reason cause]} @(:failure state)]
            ;; Outcome verification may enter WAL I/O. Classify only after
            ;; releasing gate, just as the ordinary application wait does.
            (throw (ex-info "WAL completion fenced"
                            {:error ::completion-failed
                             :reason (if (= :wal-failed reason) :wal-wait-failed reason)}
                            cause)))
          :else
          (let [durable? (<= lsn (long @(:last-durable-lsn manager)))
                owner @(:owner state)]
            (if (if durable?
                  (or (nil? owner) (= :claimed (:phase owner)))
                  (not (identical? completion @(:sync-completion state))))
              ;; Keep another tail caller runnable if this one is stopped
              ;; between the notification and its next sync claim.
              (when-not durable? (signal-next-sync! state))
              (let [deadline (if durable?
                               (min (long (or (some-> (next-range state) :deadline deref)
                                              Long/MAX_VALUE))
                                    (long (or (some-> @(:range entry) :deadline deref)
                                              Long/MAX_VALUE)))
                               (long (append/deadline-ns @(:append-batch entry))))
                    remaining (- deadline (lifetime/nano-time))]
                (when-not (pos? remaining)
                  (throw (if durable?
                           (ex-info "WAL application turn timed out"
                                    {:error :txlog/application-timeout})
                           (ex-info "Timed out waiting for durable LSN"
                                    {:type :txlog/commit-timeout :lsn lsn
                                     :timeout-ms (append/timeout-ms @(:append-batch entry))}))))
                (let [waiting (or @(:wait-state entry)
                                  (let [waiting (->CompletionWaiters
                                                 (.newCondition ^ReentrantLock (:gate state))
                                                 (long-array 1))]
                                    (vreset! (:wait-state entry) waiting)
                                    waiting))
                      ^longs waiters (:waiters waiting)
                      ^TreeMap index (:completion-waiters state)]
                  (when (zero? (aget waiters 0)) (.put index lsn entry))
                  (aset waiters 0 (inc (aget waiters 0)))
                  (try
                    (phase! :completion-waiting entry)
                    (.awaitNanos ^Condition (:changed waiting) remaining)
                    (finally
                      (aset waiters 0 (dec (aget waiters 0)))
                      (when (zero? (aget waiters 0)) (.remove index lsn)))))
                (recur)))))))))

(defn- complete-sync-target!
  "One active round owns WAL completion. Arrivals join that round and recheck
  their own LSN after it settles, including when force captured a wider prefix.
  Only then may an uncovered caller capture the next already-appended target."
  [state entry]
  (let [wal-state (:wal state)
        manager (:sync-manager wal-state)
        relaxed? (= :relaxed (:durability-profile wal-state))
        lsn (long @(:lsn entry))]
    (loop []
      (when-not (and (not relaxed?) (<= lsn (long @(:last-durable-lsn manager))))
        (phase! :before-sync-target-claim entry)
        (let [[completion owner?]
              (with-lock (:gate state)
                ;; Another caller can finish and prune the target before we
                ;; acquire the gate. Recheck before looking up append metadata.
                (if (and (not relaxed?) (<= lsn (long @(:last-durable-lsn manager))))
                  [nil false]
                  (do
                    (check-admission! state)
                    (if-let [active @(:sync-completion state)]
                      [active false]
                      (let [target (max lsn (long @(:last-appended-lsn manager)))
                            target-entry (.get ^ConcurrentSkipListMap (:entries state)
                                               (Long/valueOf target))
                            batch (some-> target-entry :append-batch deref)]
                        (when-not batch
                          (throw (ex-info "Appended WAL target has no retained append metadata"
                                          {:error :txlog/pending-gap :lsn target})))
                        (let [completion (->SyncCompletion target batch
                                                           (when relaxed? (CountDownLatch. 1))
                                                           (volatile! nil))]
                          (vreset! (:sync-completion state) completion)
                          [completion true]))))))]
          (when completion
            (if owner?
              (try
                ;; The owner keeps its original deadline even if a later append
                ;; becomes the target. Joining/retrying never renews a deadline.
                (let [deadline (min (long (append/deadline-ns @(:append-batch entry)))
                                    (long (append/deadline-ns (:batch completion))))]
                  (phase! :sync-target-owner completion)
                  (wal/complete-prefix! wal-state (:batch completion)
                                        (:target completion) deadline (:hooks state)))
                (catch Throwable e
                  (vreset! (:error completion) e)
                  (throw e))
                (finally
                  (with-lock (:gate state)
                    ;; Retain terminal failures until fencing reaches all
                    ;; callers; no successor can start in that interval.
                    (when-not @(:error completion)
                      (vreset! (:sync-completion state) nil)
                      (signal-next-sync! state))
                    (when-let [done (:done completion)]
                      (.countDown ^CountDownLatch done)))))
              (do
                (phase! :sync-target-joined completion)
                (if relaxed?
                  (let [deadline (long (append/deadline-ns @(:append-batch entry)))
                        remaining (- deadline (lifetime/nano-time))]
                    (when-not (or (.await ^CountDownLatch (:done completion)
                                          (max 0 remaining) TimeUnit/NANOSECONDS)
                                  (zero? (.getCount ^CountDownLatch (:done completion))))
                      (throw (ex-info "Timed out waiting for durable LSN"
                                      {:type :txlog/commit-timeout :lsn lsn
                                       :timeout-ms (append/timeout-ms @(:append-batch entry))})))
                    (when-let [error @(:error completion)] (throw error)))
                  (await-sync-progress! state entry completion))))
            ;; A notification is not proof of durability. An uncovered tail
            ;; still needs the next round before acknowledgement.
            (when-not relaxed? (recur))))))))

(defn await!
  "Wait for this receipt and help the oldest eligible prefix. No native or
  insertion ownership is held while waiting for another applicant."
  [state entry]
  (when-not (identical? state (:runtime (:reservation entry)))
    (throw (IllegalArgumentException. "Receipt belongs to another runtime")))
  (when-let [error @(:error entry)] (throw error))
  (if @(:complete? entry)
    (:result entry)
    (do
      (try
        (complete-sync-target! state entry)
        (when (= :relaxed (:durability-profile (:wal state)))
          (seal-through! state @(:last-appended-lsn (:sync-manager (:wal state))))
          (mark-eligible! state))
        (catch Throwable e
          (when (instance? InterruptedException e) (.interrupt (Thread/currentThread)))
          (let [data (ex-data e)
                reason (cond
                         (= ::completion-failed (:error data)) (:reason data)
                         (= :txlog/commit-timeout (:type data)) :wal-wait-timeout
                         (= :txlog/application-timeout (:error data)) :application-timeout
                         :else :wal-wait-failed)]
            (fail! state reason e)
            (throw (outcome state entry reason e)))))
      (try
        (await-application! state entry)
        (catch Throwable e
          (when (instance? InterruptedException e) (.interrupt (Thread/currentThread)))
          (if (#{:txlog/write-committed :txlog/write-indeterminate} (:error (ex-data e)))
            (throw e)
            (do (fail! state :application-wait-failed e)
                (throw (outcome state entry :application-wait-failed e)))))))))

(defn await-prefix!
  "A read-only preparation may observe pending roots, but its result cannot
  escape before that prefix satisfies durability and publication. It may help
  its predecessor through the same receipt, without appending another record."
  [state lsn]
  (check-admission! state)
  (when (> (long lsn) (long @(:published state)))
    (if-let [entry (.get ^ConcurrentSkipListMap (:entries state) (long lsn))]
      (do (await! state entry) nil)
      ;; Pruning may win between the first watermark read and the lookup.
      (when (> (long lsn) (long @(:published state)))
        (check-admission! state)
        (throw (ex-info "Observed WAL prefix has no retained receipt"
                        {:error :txlog/pending-gap :lsn lsn}))))))

(defn close!
  "Fence the engine and drain native users before adapter-owned teardown.
  A failed drain deliberately retains every entry and the protocol lease."
  [state timeout-ms teardown]
  (fail! state :closing nil)
  (let [deadline (lifetime/deadline timeout-ms)]
    ;; A blocked force or append also retains its channel/protocol ownership.
    ;; Draining native users alone cannot authorize WAL teardown or reopening.
    (lifetime/close! (:io-lifetime state) timeout-ms (fn []))
    (lifetime/close! (:lifetime state)
                     (when timeout-ms
                       (max 0 (quot (- deadline (lifetime/nano-time)) 1000000)))
                     teardown)))
