(ns datalevin.tx-group-batch-real-path-test
  "Smallest real blind-write path: a live private-WAL LMDB environment driven
  through the new-protocol collector, real txlog branch and real native writer.

  This is the M0 wiring smoke test. It writes a blind row, verifies the native
  store sees it, and verifies one durable WAL record was produced. It does not
  yet reopen/recover or measure throughput."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.bits :as bits]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.factory :as factory]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.private :as private]
            [datalevin.tx-group.batch.stage :as stage]
            [datalevin.tx-group.phase :as phase]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [java.nio ByteBuffer]
           [java.util.concurrent ConcurrentLinkedQueue CountDownLatch
            TimeUnit]))

(defn- put-long! [c k v]
  (batch/submit!
   c {:allowance (charge/blind-allowance {:declared-bytes 128 :scratch-bytes 16384})
      :prepare (fn [_]
                 (let [key (ByteBuffer/allocate 9)
                       value (ByteBuffer/allocate 9)
                       _ (bits/put-buffer key k :long)
                       _ (bits/put-buffer value v :long)
                       rows (java.util.Collections/singletonList
                             (l/kv-tx :put "data" (.array key) (.array value) :raw :raw))]
                   {:rows rows :wal-body (wal/prepare-append-body rows {})
                    :result :ok}))}))

(deftest private-opener-prepares-during-real-io-with-only-one-active-batch
  (let [dir (u/tmp-dir (str "wal-private-overlap-" (random-uuid)))
        opts {:dir dir :db-identity (str (random-uuid))
              :wal-durability-profile :strict :wal-sync-mode :fsync
              :wal-segment-prealloc? false}
        environment (private/open! opts)
        alias (private/open! (assoc opts :dir (str dir "/.")))
        {:keys [raw wal-state]} (env/resources environment)
        c (env/collector alias)
        events (ConcurrentLinkedQueue.)
        uninstall (phase/observe! (fn [event _]
                                    (.add events [(System/nanoTime) event])))
        go (CountDownLatch. 1)]
    (try
      (i/open-dbi raw "data")
      (is (identical? (env/collector environment) c))
      (env/close! environment)
      (let [writers (mapv (fn [k] (future (.await go)
                                         (dotimes [v 100] (put-long! c k v))))
                          (range 8))]
        (.countDown go)
        (doseq [f writers] (is (not= ::timeout (deref f 30000 ::timeout)))))
      (uninstall)
      (doseq [k (range 8)] (is (= 99 (i/get-value raw "data" k :long :long))))
      (let [trace (map second (sort-by first (vec events)))
            seen (set trace)
            overlap (reduce (fn [{:keys [active] :as state} event]
                              (case event
                                :batch-sealed (do (is (not active)) (assoc state :active true))
                                :batch-retired (assoc state :active false :executing false)
                                (:inline-execution :worker-dispatch) (assoc state :executing true)
                                :execution-complete (assoc state :executing false)
                                :caller-prepared (if (:executing state)
                                                   (update state :overlap inc) state)
                                state))
                            {:active false :executing false :overlap 0} trace)]
        (is (pos? (:overlap overlap)) "caller preparation overlaps real branch execution")
        (doseq [event [:caller-preparation :caller-prepared :ready-published
                       :selection-start :batch-sealed :ordered-work :schedule-selected
                       :inline-execution :worker-dispatch :wal-start :wal-appended
                       :wal-complete
                       :native-start :native-applied :before-commit-wait
                       :before-commit-ready :native-committed :execution-complete
                       :joint-publication :batch-retired :next-activation]]
          (is (contains? seen event) (str "missing real phase " event))))
      (is (= (batch/published-lsn c)
             (:last-durable-lsn (wal/sync-manager-state (:sync-manager wal-state)))))
      (is (zero? (:requests (batch/usage c))))
      (is (zero? (:bytes (batch/usage c))))
      (finally
        (uninstall)
        (env/close! alias)
        (u/delete-files dir)))))

(defn- encoded-long [v]
  (let [buffer (ByteBuffer/allocate 9)]
    (bits/put-buffer buffer v :long)
    (.array buffer)))

(defn- increment-key!
  "One ordered read-modify-write request for `k`, declaring no allowance of its
  own so the collector's configured RMW allowance applies."
  [c k]
  (batch/submit!
   c {:op (fn [tx]
            (let [key (encoded-long k)
                  seen (bits/read-buffer
                        (ByteBuffer/wrap
                         (or (stage/tx-get tx "data" key) (encoded-long 0)))
                        :long)]
              (stage/tx-put! tx "data" key (encoded-long (inc seen)))
              seen))}))

(defn- increment!
  "One ordered read-modify-write request: read one encoded long key, write it
  plus one, and return the value the body saw (0 when the key was absent)."
  [c]
  (increment-key! c 1))

(deftest an-ordered-body-reads-the-pinned-writer-and-commits-incrementally
  (let [dir (u/tmp-dir (str "wal-private-rmw-" (random-uuid)))
        opts {:dir dir :db-identity (str (random-uuid))
              :wal-durability-profile :strict :wal-sync-mode :fsync
              :wal-segment-prealloc? false}
        environment (private/open! opts)
        {:keys [raw wal-state]} (env/resources environment)
        c (env/collector environment)]
    (try
      (i/open-dbi raw "data")
      (testing "a body reads the pinned native snapshot and commits its write"
        (is (= 0 (increment! c)))
        (is (= 1 (i/get-value raw "data" 1 :long :long))))
      (testing "the next body reads the previous body's committed write"
        (is (= 1 (increment! c)))
        (is (= 2 (i/get-value raw "data" 1 :long :long))))
      (testing "each committed body produced exactly one durable WAL record"
        (let [{:keys [last-appended-lsn last-durable-lsn]}
              (wal/sync-manager-state (:sync-manager wal-state))]
          (is (= 2 last-appended-lsn))
          (is (= 2 last-durable-lsn))))
      (testing "no charges or batches were retained afterwards"
        (is (zero? (:requests (batch/usage c))))
        (is (zero? (:bytes (batch/usage c))))
        (is (batch/serving? c)))
      (finally
        (env/close! environment)
        (u/delete-files dir)))))

(deftest native-body-reads-use-the-common-api-without-a-wal-budget-check
  ;; Regression: the pinned native base detached values without charging the
  ;; reading descriptor, so a request could copy more than it owned.
  (let [dir (u/tmp-dir (str "wal-private-read-charge-" (random-uuid)))
        opts {:dir dir :db-identity (str (random-uuid))
              :wal-durability-profile :strict :wal-sync-mode :fsync
              :wal-segment-prealloc? false}
        environment (private/open! opts)
        {:keys [raw]} (env/resources environment)
        c (env/collector environment)
        big (byte-array 64)]
    (try
      (i/open-dbi raw "data")
      (java.util.Arrays/fill big (byte 7))
      ;; Seed one native value through the blind path.
      (is (= :ok (batch/submit!
                  c {:allowance (charge/blind-allowance {:declared-bytes 64
                                                         :scratch-bytes 4096})
                     :prepare (fn [_]
                                (let [key (encoded-long 1)
                                      rows (java.util.Collections/singletonList
                                            (l/kv-tx :put "data" key big :raw :raw))]
                                  {:rows rows
                                   :wal-body (wal/prepare-append-body rows {})
                                   :result :ok}))})))
      (is (some? (i/get-value raw "data" 1 :long :raw)))
      (testing "native read results remain application-owned in either write mode"
        (let [seen (batch/submit!
                    c {:allowance (+ charge/request-control-bundle 256)
                       :op (fn [tx] (i/get-value tx "data" (encoded-long 1) :raw :raw))})]
          (is (java.util.Arrays/equals ^bytes big ^bytes seen))
          (is (batch/serving? c))))
      (finally
        (env/close! environment)
        (u/delete-files dir)))))

(deftest a-blind-and-an-ordered-request-commit-under-one-real-collector
  ;; Regression for the prepaid blind request: a blind member reaching a batch
  ;; with an ordered body is published for visibility only, and must not be
  ;; charged again. Its charge already equals its allowance, so a second charge
  ;; would reject it and take the whole batch down with it.
  (let [dir (u/tmp-dir (str "wal-private-mixed-" (random-uuid)))
        opts {:dir dir :db-identity (str (random-uuid))
              :wal-durability-profile :strict :wal-sync-mode :fsync
              :wal-segment-prealloc? false}
        environment (private/open! opts)
        {:keys [raw wal-state]} (env/resources environment)
        c (env/collector environment)
        base (long (:last-durable-lsn (wal/sync-manager-state
                                       (:sync-manager wal-state))))
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        ;; Two publications mean both queued requests are waiting in the queue.
        queued (CountDownLatch. 2)
        batches (atom [])
        appended (atom 0)
        ;; The phase seam holds one observer, so it counts publications and
        ;; records each sealed batch's member kinds: `true` is a member with an
        ;; ordered body.
        observe (phase/observe!
                 (fn [event context]
                   (case event
                     :ready-published (.countDown queued)
                     :batch-sealed (swap! batches conj
                                          (mapv #(some? (batch/op
                                                         (batch/batch-at context %)))
                                                (range (batch/batch-count context))))
                     :wal-appended (swap! appended inc)
                     nil)))]
    (try
      (i/open-dbi raw "data")
      (let [blocker (future
                     (batch/submit! c
                                    {:op (fn [_tx]
                                           (.countDown entered)
                                           (.await release 5 TimeUnit/SECONDS)
                                           :blocked)}))]
        (is (.await entered 5 TimeUnit/SECONDS) "the blocking body never ran")
        (let [blind (future (put-long! c 1 7))
              body (future (increment-key! c 2))]
          (is (.await queued 5 TimeUnit/SECONDS)
              "both queued requests never published")
          (.countDown release)
          (is (= :blocked (deref blocker 30000 ::timeout)))
          (is (= :ok (deref blind 30000 ::timeout)))
          (is (= 0 (deref body 30000 ::timeout)))
          (testing "both rows committed and the runtime stayed healthy"
            (is (= 7 (i/get-value raw "data" 1 :long :long)))
            (is (= 1 (i/get-value raw "data" 2 :long :long)))
            (is (batch/serving? c))
            (is (zero? (:requests (batch/usage c)))))
          (testing "the blocker ran alone and wrote no record"
            (is (some #{[true]} @batches) (str "sealed " (pr-str @batches))))
          (testing "every batch that wrote rows appended exactly one record"
            ;; Whether the queued pair is sealed as one prefix or as two batches
            ;; depends on when each reaches the queue head, and a mixed batch
            ;; carries both members' bodies in one group record. What is fixed is
            ;; one record per writing batch, so the durable LSN tracks the
            ;; appends. The mixed-batch contract itself is covered
            ;; deterministically in `datalevin.tx-group-batch-rmw-test`.
            (let [durable (long (:last-durable-lsn
                                 (wal/sync-manager-state
                                  (:sync-manager wal-state))))]
              (is (pos? @appended))
              (is (= @appended (- durable (long base)))
                  (str "appended " @appended " sealed " (pr-str @batches)
                       " durable " durable))))))
      (finally
        (observe)
        (env/close! environment)
        (u/delete-files dir)))))

(defn- control []
  {:check-admission! (fn [])
   :throw-if-fatal! (fn [_state])
   :before-append! (fn [_state])
   :mark-fatal! (fn [_state _error])})

(deftest real-blind-write-reaches-lmdb-and-one-durable-wal-record
  (let [dir (u/tmp-dir (str "wal-real-path-" (random-uuid)))
        opts {:wal? true :wal-shared? false :wal-sync-mode :fsync
              :db-identity (str (random-uuid))
              :wal-segment-prealloc? false :snapshot-scheduler? false}
        db (l/open-kv dir opts)]
    (try
      (i/open-dbi db "data" {})
      (let [raw (kv/raw-lmdb db)
            wal-state (wal/state db)
            base (long (:last-durable-lsn (wal/sync-manager-state
                                           (:sync-manager wal-state))))
            runtime (factory/collector
                     wal-state raw
                     {:runtime-control (control)
                      :limits (charge/resolve-limits nil)})
            c (:collector runtime)
            rows [[:put "data" 1 "v" :long :string]]]
        (try
          (testing "the collector accepts one blind write end to end"
            (is (= :ok (batch/submit!
                        c {:allowance 1024
                           :data {:rows rows
                                  :wal-body (wal/prepare-append-body rows {})
                                  :result :ok}}))))
          (testing "the native transaction committed the row"
            (is (= "v" (i/get-value raw "data" 1 :long :string))))
          (testing "the WAL advanced by exactly one durable record"
            (let [after (wal/sync-manager-state (:sync-manager wal-state))]
              (is (= (inc base) (:last-appended-lsn after)))
              (is (= (inc base) (:last-durable-lsn after)))))
          (finally
            ((:close! runtime)))))
      (finally
        (i/close-kv db)
        (u/delete-files dir)))))

(deftest a-fence-before-join-preserves-a-real-durable-write
  (let [dir (u/tmp-dir (str "wal-real-join-fence-" (random-uuid)))
        db (l/open-kv dir {:wal? true :wal-shared? false
                           :wal-durability-profile :strict
                           :wal-sync-mode :fsync
                           :db-identity (str (random-uuid))
                           :wal-segment-prealloc? false
                           :snapshot-scheduler? false})]
    (try
      (i/open-dbi db "data" {})
      (let [raw (kv/raw-lmdb db)
            state (wal/state db)
            base (:last-durable-lsn (wal/sync-manager-state (:sync-manager state)))
            runtime (factory/collector state raw {:runtime-control (control)})
            c (:collector runtime)
            failure (ex-info "fenced before join"
                             {:error :txlog/runtime-fenced :outcome :not-committed})
            uninstall (phase/observe!
                       (fn [event _]
                         (when (= :execution-complete event)
                           (batch/fence! c failure))))
            rows [[:put "data" 1 "v" :long :string]]]
        (try
          (let [thrown (try
                         (batch/submit! c {:allowance 1024
                                           :data {:rows rows
                                                  :wal-body (wal/prepare-append-body rows {})
                                                  :result :ok}})
                         nil
                         (catch Throwable t t))]
            (is (= :txlog/write-committed (:error (ex-data thrown))))
            (is (= :committed (:outcome (ex-data thrown))))
            (is (= :durable (:wal-status (ex-data thrown))))
            (is (= (inc base) (:txlog-lsn (ex-data thrown))))
            (is (identical? failure (ex-cause thrown)))
            (is (= "v" (i/get-value raw "data" 1 :long :string)))
            (is (= (inc base)
                   (:last-durable-lsn (wal/sync-manager-state (:sync-manager state)))))
            (is (not (batch/serving? c))))
          (finally
            (uninstall)
            ((:close! runtime)))))
      (finally
        (i/close-kv db)
        (u/delete-files dir)))))

(deftest a-native-failure-after-wal-durability-reports-the-committed-outcome
  ;; Regression: with a real strict WAL the durable LSN advanced, but the caller
  ;; received only :txlog/native-apply-failed. The established committed outcome
  ;; must be preserved even though the native overlay did not commit.
  (let [dir (u/tmp-dir (str "wal-real-committed-" (random-uuid)))
        opts {:wal? true :wal-shared? false :wal-sync-mode :fsync
              :db-identity (str (random-uuid))
              :wal-segment-prealloc? false :snapshot-scheduler? false}
        db (l/open-kv dir opts)]
    (try
      (i/open-dbi db "data" {})
      (let [raw (kv/raw-lmdb db)
            wal-state (wal/state db)
            base (long (:last-durable-lsn (wal/sync-manager-state
                                           (:sync-manager wal-state))))
            runtime (factory/collector
                     wal-state raw
                     {:runtime-control (control)
                      :limits (charge/resolve-limits nil)
                      :native-opts
                      {:apply-fn (fn [_raw body before-commit]
                                   ;; The WAL gate passes, then the native commit
                                   ;; fails: durability is already established.
                                   (body :fake-wdb)
                                   (before-commit :fake-wdb
                                                  {:operation :close-transact-kv})
                                   (throw (ex-info "native boom"
                                                   {:error :txlog/native-apply-failed})))
                       :transact! (fn [_wdb _rows])}})
            c (:collector runtime)
            rows [[:put "data" 1 "v" :long :string]]
            thrown (try
                     (batch/submit!
                      c {:allowance 1024
                         :data {:rows rows
                                :wal-body (wal/prepare-append-body rows {})
                                :result :ok}})
                     nil
                     (catch Throwable t t))]
        (try
          (is (instance? Throwable thrown))
          (is (= :txlog/write-committed (:error (ex-data thrown))))
          (is (= :committed (:outcome (ex-data thrown))))
          (is (= :durable (:wal-status (ex-data thrown))))
          (is (= :txlog/native-apply-failed
                 (:error (ex-data (ex-cause thrown)))))
          (testing "the durable WAL record is retained despite the native failure"
            (is (= (inc base)
                   (:last-durable-lsn
                    (wal/sync-manager-state (:sync-manager wal-state))))))
          (is (not (batch/serving? c)))
          (finally
            ((:close! runtime)))))
      (finally
        (i/close-kv db)
        (u/delete-files dir)))))
