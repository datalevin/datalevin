(ns datalevin.txlog.wal-only-test
  "Contract tests for the WAL-only interface used by the new write protocol.

  These drive a real private-WAL runtime and exercise only the WAL boundary:
  bound runtime control, one group record per prepared batch, logical-weight
  registration and policy/force completion. No native application is involved."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.batch.wal :as wal-adapter]
            [datalevin.tx-group.batch.worker :as worker]
            [datalevin.tx-group.phase :as phase]
            [datalevin.txlog :as wal]
            [datalevin.txlog.append :as append]
            [datalevin.txlog.segment :as segment]
            [datalevin.util :as u])
  (:import [java.io Closeable]))

(defn- with-runtime [opts f]
  (let [dir (u/tmp-dir (str "wal-only-" (random-uuid)))
        {:keys [state]}
        (wal/init-runtime-state
         (merge {:dir dir :wal? true :wal-shared? false
                 :wal-durability-profile :strict :wal-sync-mode :fsync
                 :wal-segment-prealloc? false :wal-commit-wait-ms 10000} opts)
         nil)]
    (try (f state)
         (finally
           (doseq [key [:segment-channel :sync-lock-channel]]
             (when-let [^Closeable channel (some-> (get state key) deref)]
               (.close channel)))
           (u/delete-files dir)))))

(defn- rows [n] [[:put "data" n (str n) :long :string]])

(defn- caught [f] (try (f) (catch Throwable t t)))

(defn- control
  "A minimal WAL-only control: no pending roots, application ranges or
  completion indexes, only I/O lifetime, admission and failure seams."
  []
  {:check-admission! (fn [])
   :throw-if-fatal! (fn [_state])
   :before-append! (fn [_state])
   :mark-fatal! (fn [_state _error])})

(defn- status [state] (wal/sync-manager-state (:sync-manager state)))

(deftest wal-ownership-wait-uses-one-absolute-deadline
  (doseq [supplied-deadline? [false true]]
    (let [manager (wal/new-sync-manager {})
          state {:sync-manager manager :commit-wait-ms 25}
          owner (#'wal/claim-wal-ownership! state 0)
          waiter (future
                   (caught #(#'wal/claim-wal-ownership!
                             state (if supplied-deadline?
                                     (+ (System/nanoTime) 25000000) 0))))
          result (try (deref waiter 1000 ::timeout)
                      (finally (#'wal/release-wal-ownership! state owner)))]
      (is (= :txlog/wal-ownership-timeout (:error (ex-data result))))
      ;; Keep the test safe even if a regression lets the waiter claim after
      ;; release rather than timing out.
      (let [settled (deref waiter 1000 ::timeout)]
        (when-not (or (= ::timeout settled) (instance? Throwable settled))
          (#'wal/release-wal-ownership! state settled))))))

(deftest wal-adapter-preserves-the-batch-deadline-during-append-ownership-wait
  (with-runtime
    {}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (let [owner (#'wal/claim-wal-ownership! state 0)
            c (batch/create
               (executor/create
                (wal-adapter/branch state)
                (reify executor/INativeBranch
                  (apply-rows! [_ _ _] (throw (AssertionError. "native must not run"))))
                (constantly 1)))
            write (future
                    (caught #(batch/submit!
                              c {:allowance 1024 :timeout-ms 100
                                 :data {:wal-body (wal/prepare-append-body (rows 1) {})}})))
            result (try (deref write 1000 ::timeout)
                        (finally (#'wal/release-wal-ownership! state owner)))]
        (is (= :txlog/wal-ownership-timeout (:error (ex-data result))))
        (is (= :not-committed (:outcome (ex-data result))))
        (is (= 1 @(:next-lsn state)))
        (is (batch/await-quiescence! c 1000))
        (is (zero? (:requests (batch/usage c))))
        (is (not= ::timeout (deref write 1000 ::timeout)))))))

(deftest after-append-observer-failure-releases-the-real-wal-span
  (doseq [schedule [:inline :parallel]
          profile [:strict :relaxed]]
    (with-runtime
      {:wal-durability-profile profile :wal-full-prefix? true
       :wal-group-commit 100 :wal-group-commit-ms 0}
      (fn [state]
        (wal/bind-runtime-control! state (control))
        (let [boom (ex-info "after append failed" {})
              commits (atom 0)
              c (batch/create
                 (executor/create
                  (wal-adapter/branch state)
                  (reify executor/INativeBranch
                    (apply-rows! [_ _ gate]
                      (gate)
                      (swap! commits inc)
                      (object-array [:ok])))
                  (constantly 1) {:schedule-fn (constantly schedule)}))
              uninstall (phase/observe!
                         (fn [event _]
                           (when (= :wal-appended event) (throw boom))))]
          (try
            (let [error (caught #(batch/submit!
                                 c {:allowance 1024
                                    :data {:wal-body (wal/prepare-append-body (rows 1) {})}}))]
              (is (= :indeterminate (:outcome (ex-data error))))
              (is (= 1 (:txlog-lsn (ex-data error))))
              (is (identical? boom (ex-cause error))))
            (is (= 2 @(:next-lsn state)))
            (is (zero? @commits))
            (is (false? (batch/serving? c)))
            (is (batch/await-quiescence! c 1000))
            (is (zero? (:requests (batch/usage c))))
            (is (nil? @(:wal-owner (:sync-manager state))))
            ;; A later ownership claim is no longer stuck behind the abandoned
            ;; append. No force was needed to release the failed span.
            (let [token (#'wal/claim-wal-ownership! state 0)]
              (#'wal/release-wal-ownership! state token))
            (is (zero? (:last-durable-lsn (status state))))
            (finally (uninstall))))))))

(deftest force-failure-before-durability-reports-indeterminate
  (doseq [schedule [:inline :parallel]]
    (with-runtime
      {:wal-durability-profile :strict :wal-sync-mode :fsync
       :wal-full-prefix? true :wal-group-commit 100 :wal-group-commit-ms 0}
      (fn [state]
        (wal/bind-runtime-control! state (control))
        (let [fault (java.io.IOException. "injected force failure")
              commits (atom 0)
              c (batch/create
                 (executor/create
                  (wal-adapter/branch state)
                  (reify executor/INativeBranch
                    (apply-rows! [_ _ gate]
                      (gate)
                      (swap! commits inc)
                      (object-array [:ok])))
                  (constantly 1) {:schedule-fn (constantly schedule)}))]
          (try
            (with-redefs [segment/phase!
                          (fn [event _]
                            (when (= :force-started event) (throw fault)))]
              (let [error (caught #(batch/submit!
                                    c {:allowance 1024
                                       :data {:wal-body (wal/prepare-append-body (rows 1) {})}}))]
                (is (= :indeterminate (:outcome (ex-data error))))
                (is (= :txlog/write-indeterminate (:error (ex-data error))))
                (is (= :appended (:wal-status (ex-data error))))
                (is (= 1 (:txlog-lsn (ex-data error))))
                (is (= 1 (:lsn (ex-data error))) "record identity is retained")
                (is (some? (:checksum (ex-data error))))
                (is (identical? fault (ex-cause error)) "original cause is retained")
                (is (zero? @commits))))
            (finally
              (is (batch/await-quiescence! c 1000))
              (is (zero? (:requests (batch/usage c)))))))))))

(deftest record-write-failure-reports-indeterminate-before-append-publication
  (doseq [schedule [:inline :parallel]
          profile [:strict :relaxed]]
    (with-runtime
      {:wal-durability-profile profile :wal-full-prefix? true
       :wal-group-commit 100 :wal-group-commit-ms 0}
      (fn [state]
        (let [fault (java.io.IOException. "record written but append failed")
              commits (atom 0)
              c (batch/create
                 (executor/create
                  (wal-adapter/branch state)
                  (reify executor/INativeBranch
                    (apply-rows! [_ _ gate]
                      (gate)
                      (swap! commits inc)
                      (object-array [:ok])))
                  (constantly 1) {:schedule-fn (constantly schedule)}))]
          (wal/bind-runtime-control!
           state (assoc (control) :on-failure! #(batch/fence! c %)))
          (with-redefs [segment/phase!
                        (fn [event _]
                          (when (= :record-bytes-completely-written event)
                            (throw fault)))]
            (let [error (caught #(batch/submit!
                                  c {:allowance 1024
                                     :data {:wal-body (wal/prepare-append-body (rows 1) {})}}))]
              (is (= :txlog/write-indeterminate (:error (ex-data error))))
              (is (= :indeterminate (:outcome (ex-data error))))
              (is (= 1 (:txlog-lsn (ex-data error))))
              (is (= {:lsn 1 :segment-id 1 :offset 0}
                     (select-keys (ex-data error) [:lsn :segment-id :offset])))
              (is (false? (:retryable? (ex-data error))))
              (is (nil? (:wal-status (ex-data error)))
                  "a failed writer has not established a complete append")
              (is (identical? fault (ex-cause error)))
              (is (identical? fault @(:fatal-error state)))))
          (is (zero? @commits))
          (is (false? (batch/serving? c)))
          (is (batch/await-quiescence! c 1000))
          (is (zero? (:requests (batch/usage c))))
          (is (nil? @(:wal-owner (:sync-manager state))))
          (is (zero? (:last-appended-lsn (status state))))
          (is (zero? (:last-durable-lsn (status state))))
          (is (= 1 (count (:records (segment/scan-segment
                                     (wal/segment-path (:dir state) 1)))))
              "a complete replayable record can exist before append publication"))))))

(deftest grouped-record-write-failure-preserves-the-attempted-record
  (doseq [append-fn [wal/append-prepared-group! wal/begin-prepared-group!]
          value-size [1 100000]
          notification-throws? [false true]]
    (with-runtime
      {:wal-full-prefix? true}
      (fn [state]
        (let [fault (java.io.IOException. "group write failed")
              notified (atom nil)
              inputs (mapv (fn [key]
                             [[:put "data" key (apply str (repeat value-size "x"))
                               :long :string]])
                           [1 2])
              bodies (mapv #(wal/prepare-append-body % {}) inputs)]
          (wal/bind-runtime-control!
           state (assoc (control) :on-failure!
                        (fn [error]
                          (reset! notified error)
                          (when notification-throws?
                            (throw (IllegalStateException. "notification failed"))))))
          (with-redefs [segment/phase!
                        (fn [event _]
                          (when (= :record-bytes-completely-written event)
                            (throw fault)))]
            (let [error (caught #(append-fn state 1 bodies))]
              (is (= :indeterminate (:outcome (ex-data error))))
              (is (= :txlog/write-indeterminate (:error (ex-data error))))
              (is (= 1 (:txlog-lsn (ex-data error))))
              (is (identical? fault (ex-cause error)))
              (is (identical? error @notified))))
          (is (nil? @(:wal-owner (:sync-manager state))))
          (let [records (:records (segment/scan-segment
                                   (wal/segment-path (:dir state) 1)))]
            (is (= 1 (count records)))
            (is (= (vec (mapcat identity inputs))
                   (:ops (wal/decode-commit-row-payload (:body (first records))))))))))))

(deftest compatibility-prepared-write-failure-preserves-original-exception
  (with-runtime
    {}
    (fn [state]
      (let [fault (java.io.IOException. "compatibility record write failed")
            marked (atom nil)]
        (with-redefs [segment/phase!
                      (fn [event _]
                        (when (= :record-bytes-completely-written event)
                          (throw fault)))]
          (let [error (caught #(wal/append-prepared-batch-pending!
                               state 1 [(wal/prepare-append-body (rows 1) {})]
                               {:mark-fatal! (fn [_ error] (reset! marked error))}))]
            (is (identical? fault error))
            (is (identical? fault @marked))
            (is (identical? fault @(:fatal-error state)))))
        (is (= 1 (count (:records (segment/scan-segment
                                   (wal/segment-path (:dir state) 1))))))))))

(deftest pre-append-rejection-does-not-claim-an-uncertain-record
  (doseq [schedule [:inline :parallel]]
    (with-runtime
      {:wal-full-prefix? true}
      (fn [state]
        (let [fault (ex-info "append admission rejected" {:reason :test-rejection})
              commits (atom 0)
              c (batch/create
                 (executor/create
                  (wal-adapter/branch state)
                  (reify executor/INativeBranch
                    (apply-rows! [_ _ gate]
                      (gate)
                      (swap! commits inc)
                      (object-array [:ok])))
                  (constantly 1) {:schedule-fn (constantly schedule)}))]
          (wal/bind-runtime-control!
           state (assoc (control) :before-append! (fn [_] (throw fault))))
          (let [error (caught #(batch/submit!
                                c {:allowance 1024
                                   :data {:wal-body (wal/prepare-append-body (rows 1) {})}}))]
            (is (= :not-committed (:outcome (ex-data error))))
            (is (= :test-rejection (:reason (ex-data error))))
            (is (nil? (:txlog-lsn (ex-data error))))
            (is (identical? fault (ex-cause error))))
          (is (zero? @commits))
          (is (false? (batch/serving? c)))
          (is (batch/await-quiescence! c 1000))
          (is (zero? (:requests (batch/usage c))))
          (is (nil? @(:wal-owner (:sync-manager state))))
          (is (empty? (:records (segment/scan-segment
                                 (wal/segment-path (:dir state) 1))))))))))

(deftest after-sync-failure-after-durability-reports-committed
  (doseq [schedule [:inline :parallel]]
    (with-runtime
      {:wal-durability-profile :strict :wal-sync-mode :fsync
       :wal-full-prefix? true :wal-group-commit 100 :wal-group-commit-ms 0}
      (fn [state]
        (let [fault (java.io.IOException. "injected after-sync failure")
              c (batch/create
                 (executor/create
                  (wal-adapter/branch state)
                  (reify executor/INativeBranch
                    (apply-rows! [_ _ gate]
                      (gate)
                      (object-array [:ok])))
                  (constantly 1) {:schedule-fn (constantly schedule)}))]
          (wal/bind-runtime-control! state
            (assoc (control) :after-sync! (fn [_ _] (throw fault))))
          (try
            (let [error (caught #(batch/submit!
                                  c {:allowance 1024
                                     :data {:wal-body (wal/prepare-append-body (rows 1) {})}}))]
              (is (= :committed (:outcome (ex-data error))))
              (is (= :txlog/write-committed (:error (ex-data error))))
              (is (= :durable (:wal-status (ex-data error))))
              (is (= 1 (:txlog-lsn (ex-data error))))
              (is (= 1 (:lsn (ex-data error))) "record identity is retained")
              (is (some? (:checksum (ex-data error))))
              (is (identical? fault (ex-cause error)) "original cause is retained")
              (is (= 1 (:last-durable-lsn (status state)))
                  "durability had already advanced past the record's LSN"))
            (finally
              (is (batch/await-quiescence! c 1000))
              (is (zero? (:requests (batch/usage c)))))))))))

(deftest an-interrupt-after-preparation-rejects-without-closing-the-wal
  ;; A real probe: an interrupted preparer used to publish and lead, so the WAL
  ;; channel's interruptible I/O threw ClosedByInterruptException and fenced the
  ;; runtime. The interrupt must reject only this request.
  (doseq [schedule [:inline :parallel]]
    (with-runtime
      {:wal-durability-profile :strict :wal-sync-mode :fsync
       :wal-full-prefix? true :wal-group-commit 100 :wal-group-commit-ms 0}
      (fn [state]
        (wal/bind-runtime-control! state (control))
        (let [body {:wal-body (wal/prepare-append-body (rows 1) {})}
              commits (atom 0)
              c (batch/create
                 (executor/create
                  (wal-adapter/branch state)
                  (reify executor/INativeBranch
                    (apply-rows! [_ _ gate]
                      (gate)
                      (swap! commits inc)
                      (object-array [:ok])))
                  (constantly 1) {:schedule-fn (constantly schedule)}))]
          (try
            (let [thrown (try
                           (batch/submit!
                            c {:allowance 1024 :data body
                               :prepare (fn [_]
                                          (.interrupt (Thread/currentThread))
                                          body)})
                           nil
                           (catch Throwable t t))]
              (is (instance? Throwable thrown))
              (is (= :txlog/write-interrupted (:error (ex-data thrown))))
              (is (= :not-committed (:outcome (ex-data thrown))))
              (is (zero? @commits) "the interrupted request never runs a branch")
              (is (.isOpen ^java.nio.channels.FileChannel
                           @(:segment-channel state))
                  "the WAL channel is not closed")
              (is (= 1 @(:next-lsn state)) "nothing was appended")
              (is (batch/serving? c) "the runtime is not fenced")
              (is (batch/await-quiescence! c 1000))
              (is (zero? (:requests (batch/usage c))))
              (is (= :ok (batch/submit! c {:allowance 1024 :data body}))
                  "the runtime remains usable"))
            (finally
              ;; Do not leak the interrupt into cleanup/later tests.
              (Thread/interrupted))))))))

(deftest wal-only-insertion-requires-a-bound-runtime-control
  (with-runtime
    {}
    (fn [state]
      (let [bodies [(wal/prepare-append-body (rows 1) {})]]
        (is (= :txlog/no-runtime-control
               (:type (ex-data (caught #(wal/append-prepared-group!
                                         state 1 bodies))))))
        (testing "the rejected insertion wrote nothing and did not advance"
          (is (= 1 @(:next-lsn state)))
          (is (zero? (:last-appended-lsn (status state)))))))))

(deftest wal-only-force-requires-a-bound-runtime-control
  (with-runtime
    {}
    (fn [state]
      (is (= :txlog/no-runtime-control
             (:type (ex-data (caught #(wal/force-through! state 1 0)))))))))

(deftest wal-only-strict-group-appends-policies-and-registers-weight-once
  (with-runtime
    {}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (let [inputs (mapv rows [1 2 3])
            bodies (mapv #(wal/prepare-append-body % {}) inputs)
            batch (wal/append-prepared-group! state 1 bodies)]
        (is (= [1 1] [(append/first-lsn batch) (append/last-lsn batch)]))
        (testing "the group registers its logical weight before durability"
          (is (= 3 (:unsynced-count (status state))))
          (is (= 1 (:last-appended-lsn (status state)))))
        (testing "strict policy completion requires and reaches configured durability"
          (is (true? (wal/complete-policy! state batch 0)))
          (is (= 1 (:last-durable-lsn (status state))))
          (is (zero? (:unsynced-count (status state)))))
        (testing "one physical record holds all three logical requests in order"
          (let [records (mapv #(wal/decode-commit-row-payload (:body %))
                              (:records (segment/scan-segment
                                         (wal/segment-path (:dir state) 1))))]
            (is (= [1] (mapv :lsn records)))
            (is (= [(vec (mapcat identity inputs))] (mapv :ops records)))))))))

(deftest wal-only-relaxed-below-threshold-is-policy-complete-not-durable
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 10 :wal-group-commit-ms 0}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (let [bodies (mapv #(wal/prepare-append-body (rows %) {}) [1 2 3])
            batch (wal/append-prepared-group! state 1 bodies)]
        (testing "relaxed append registers weight and arms no force below threshold"
          (is (= 3 (:unsynced-count (status state))))
          (is (false? @(:sync-requested? (:sync-manager state)))))
        (testing "policy completion returns appended/not-yet-durable without forcing"
          (is (false? (wal/complete-policy! state batch 0)))
          (is (= 0 (:last-durable-lsn (status state))))
          (is (= 3 (:unsynced-count (status state)))))))))

(deftest wal-only-relaxed-threshold-arms-without-forcing
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 3 :wal-group-commit-ms 0}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (let [bodies (mapv #(wal/prepare-append-body (rows %) {}) [1 2 3])
            batch (wal/append-prepared-group! state 1 bodies)]
        (testing "the count trigger arms a pending force for the maintenance owner"
          (is (true? @(:sync-requested? (:sync-manager state)))))
        (testing "policy completion returns appended/not-yet-durable without forcing"
          (is (false? (wal/complete-policy! state batch 0)))
          (is (= 0 (:last-durable-lsn (status state))))
          (is (= 3 (:unsynced-count (status state)))))
        (testing "the maintenance owner can then force the armed prefix"
          (is (:synced? (wal/force-through! state 1 0)))
          (is (= 1 (:last-durable-lsn (status state))))
          (is (zero? (:unsynced-count (status state)))))))))

(deftest wal-only-maintenance-deadline-tracks-the-armed-prefix
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 3 :wal-group-commit-ms 0}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (testing "an idle runtime has no maintenance deadline"
        (is (zero? (wal/maintenance-deadline-ns state))))
      (let [bodies (mapv #(wal/prepare-append-body (rows %) {}) [1 2 3])
            _ (wal/append-prepared-group! state 1 bodies)]
        (testing "an armed force is due now"
          (is (<= (wal/maintenance-deadline-ns state) (System/nanoTime))))
        (is (:synced? (wal/service-pending-sync! state 0)))
        (testing "a serviced runtime returns to no deadline"
          (is (zero? (wal/maintenance-deadline-ns state))))))))

(deftest wal-only-service-maintenance-drives-a-due-time-trigger
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 100 :wal-group-commit-ms 1}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (let [bodies (mapv #(wal/prepare-append-body (rows %) {}) [1 2])
            _ (wal/append-prepared-group! state 1 bodies)]
        (Thread/sleep 5)
        (let [result (wal/service-maintenance! state 0)]
          (is (:synced? result))
          (is (= 1 (:last-durable-lsn result))))
        (is (zero? (:unsynced-count (status state))))
        (is (zero? (wal/maintenance-deadline-ns state)))))))

(deftest wal-only-append-policy-holds-ownership-across-the-span
  ;; Regression: append released WAL ownership before policy completion
  ;; reacquired it, so a force could claim the gap and make policy completion
  ;; wait behind the force.
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 100 :wal-group-commit-ms 0}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (let [bodies (mapv #(wal/prepare-append-body (rows %) {}) [1 2])
            batch (wal/begin-prepared-group! state 1 bodies)
            forced (promise)
            force-thread (future (deliver forced (wal/force-through! state 1 0)))]
        (try
          (Thread/sleep 100)
          (testing "the force cannot claim ownership while the span holds it"
            (is (not (realized? forced)))
            (is (some? @(:wal-owner (:sync-manager state)))))
          (testing "policy completion releases the span and the force proceeds"
            (is (false? (wal/finish-prepared-group! state batch 0)))
            (is (:synced? (deref forced 5000 ::timeout))))
          (finally
            (deref force-thread 5000 nil)))))))

(deftest wal-only-maintenance-services-an-armed-force-and-clears-the-flag
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 3 :wal-group-commit-ms 0}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (let [bodies (mapv #(wal/prepare-append-body (rows %) {}) [1 2 3])
            batch (wal/append-prepared-group! state 1 bodies)]
        (is (false? (wal/complete-policy! state batch 0)))
        (is (true? (wal/pending-sync? state)))
        (let [result (wal/service-pending-sync! state 0)]
          (is (:synced? result))
          (is (= 1 (:last-durable-lsn result)))
          (is (false? (wal/pending-sync? state)))
          (is (zero? (:unsynced-count (status state)))))))))

(deftest wal-only-maintenance-is-a-no-op-when-nothing-is-armed
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 10 :wal-group-commit-ms 0}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (is (nil? (wal/service-pending-sync! state 0)))
      (is (false? (wal/pending-sync? state))))))

(deftest wal-only-force-through-covers-the-appended-prefix
  (with-runtime
    {}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (let [bodies (mapv #(wal/prepare-append-body (rows %) {}) [1 2])
            _ (wal/append-prepared-group! state 1 bodies)
            result (wal/force-through! state 1 0)]
        (is (:synced? result))
        (is (= {:target-lsn 1 :last-appended-lsn 1 :last-durable-lsn 1
                :pending-count 0}
               (select-keys result
                            [:target-lsn :last-appended-lsn
                             :last-durable-lsn :pending-count])))
        (is (= 1 (:last-durable-lsn (status state))))))))

;; The full-prefix bookkeeping mode is an explicit opt-in used by the new
;; protocol; compatibility keeps the weighted-tail accounting unchanged.

(defn- register-append! [manager lsn request-count]
  (wal/append-sync-transition! manager lsn (wal/now-ms)
                               {:begin? false :request-count request-count}))

(deftest full-prefix-success-covers-the-captured-prefix-and-clears-weight
  (let [m (wal/new-sync-manager {:last-durable-lsn 0 :last-appended-lsn 0
                                 :group-commit 100 :group-commit-ms 0
                                 :full-prefix? true})]
    (testing "full-prefix mode retains no per-LSN weight tail"
      (is (true? (:full-prefix? m)))
      (is (false? (:track-trailing? m))))
    (register-append! m 1 1)
    (register-append! m 2 3)
    (is (zero? (.size ^java.util.ArrayDeque (:pending-group-counts m))))
    (is (zero? @(:pending-group-extra-count m)))
    (is (= {:last-appended-lsn 2 :last-durable-lsn 0 :unsynced-count 4}
           (select-keys (wal/sync-manager-state m)
                        [:last-appended-lsn :last-durable-lsn :unsynced-count])))
    (testing "a confirmed force covers A and clears U wholesale"
      (wal/complete-sync-success! m 1 (wal/now-ms) :forced)
      (is (= {:last-appended-lsn 2 :last-durable-lsn 2 :unsynced-count 0
              :pending-count 0}
             (select-keys (wal/sync-manager-state m)
                          [:last-appended-lsn :last-durable-lsn
                           :unsynced-count :pending-count]))))))

(deftest full-prefix-grouped-appends-retain-only-the-logical-weight-total
  (let [m (wal/new-sync-manager {:full-prefix? true :group-commit 10000})]
    (doseq [lsn (range 1 101)] (register-append! m lsn 8))
    (is (= 800 @(:unsynced-count m)))
    (is (zero? (.size ^java.util.ArrayDeque (:pending-group-counts m))))
    (is (zero? @(:pending-group-extra-count m)))
    (wal/complete-sync-success! m 100 (wal/now-ms) :forced)
    (is (= 100 @(:last-durable-lsn m)))
    (is (zero? @(:unsynced-count m)))))

(deftest default-sync-manager-keeps-weighted-tail-accounting
  (let [m (wal/new-sync-manager {:last-durable-lsn 0 :last-appended-lsn 0
                                 :group-commit 100 :group-commit-ms 0})]
    (is (true? (:track-trailing? m)))
    (is (false? (boolean (:full-prefix? m))))
    (register-append! m 1 1)
    (register-append! m 2 3)
    (testing "a partial force keeps the newer record's logical weight"
      (wal/complete-sync-success! m 1 (wal/now-ms) :forced)
      (is (= {:last-durable-lsn 1 :unsynced-count 3}
             (select-keys (wal/sync-manager-state m)
                          [:last-durable-lsn :unsynced-count]))))))

;; Integration: an inline relaxed write arms a force on the leader, wakes the
;; worker, and the worker forces the prefix through the real txlog boundary.
(deftest wal-only-worker-services-inline-relaxed-writes
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 1 :wal-group-commit-ms 0
     :wal-full-prefix? true}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (let [wal-branch (wal-adapter/branch state)
            w (worker/for-wal wal-branch :name "test-inline-wal-worker")
            native (reify executor/INativeBranch
                     (apply-rows! [_ b before-commit]
                       (before-commit)
                       (let [n (batch/batch-count b)
                             values (object-array n)]
                         (dotimes [i n]
                           (aset values i
                                 (:result (batch/data (batch/batch-at b i)))))
                         values)))
            exec (executor/create wal-branch native
                                  #(long @(:next-lsn state))
                                  {:schedule-fn (constantly :inline)
                                   :wake-maintenance! (:wake! w)})
            c (batch/create exec {:limits (charge/resolve-limits nil)})
            rows [[:put "data" 1 "v" :long :string]]]
        (try
          (is (= :ok (batch/submit!
                      c {:allowance 1024
                         :data {:rows rows
                                :wal-body (wal/prepare-append-body rows {})
                                :result :ok}})))
          (is (loop [n 0]
                (cond
                  (= 1 (long (:last-durable-lsn (status state)))) true
                  (< n 400) (do (Thread/sleep 5) (recur (inc n)))
                  :else false))
              "the worker forces the armed relaxed prefix")
          (is (zero? (:unsynced-count (status state))))
          (is (zero? (wal/maintenance-deadline-ns state)))
          (finally ((:close! w))))))))

;; The exclusive WAL owner: a force in progress blocks a concurrent append until
;; it releases, so full-prefix accounting cannot absorb a racing record.
(deftest wal-only-force-owns-the-wal-against-a-concurrent-append
  (with-runtime
    {}
    (fn [state]
      (let [entered (promise) release (promise)
            ctl (assoc (control)
                       :before-sync! (fn [_state _round]
                                       (deliver entered true)
                                       (deref release 10000 false)))]
        (wal/bind-runtime-control! state ctl)
        (wal/append-prepared-group! state 1 [(wal/prepare-append-body (rows 1) {})])
        (let [force-job (future (wal/force-through! state 1 0))]
          (try
            (is (true? (deref entered 5000 false))
                "the force reached its owned sync round")
            (let [append-job (future
                              (wal/append-prepared-group!
                               state 2 [(wal/prepare-append-body (rows 2) {})]))]
              (testing "a concurrent append waits for WAL ownership"
                (is (= ::blocked (deref append-job 150 ::blocked))))
              (deliver release true)
              (is (:synced? (deref force-job 5000 ::timeout)))
              (is (some? (deref append-job 5000 ::timeout))))
            (finally
              (deliver release true)
              (deref force-job 5000 nil))))))))

(deftest wal-only-full-prefix-runtime-clears-weight-on-force
  (with-runtime
    {:wal-durability-profile :strict :wal-full-prefix? true}
    (fn [state]
      (wal/bind-runtime-control! state (control))
      (testing "the private opener selects full-prefix accounting"
        (is (true? (:full-prefix? (:sync-manager state))))
        (is (false? (:track-trailing? (:sync-manager state)))
            "full-prefix drops the per-LSN weight tail even under strict"))
      (let [bodies (mapv #(wal/prepare-append-body (rows %) {}) [1 2 3])
            _ (wal/append-prepared-group! state 1 bodies)]
        (is (= 3 (:unsynced-count (status state))))
        (is (:synced? (wal/force-through! state 1 0)))
        (is (= 1 (:last-durable-lsn (status state))))
        (is (zero? (:unsynced-count (status state))))))))
