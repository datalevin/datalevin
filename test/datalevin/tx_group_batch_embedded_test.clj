(ns datalevin.tx-group-batch-embedded-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.kv.scheduler :as scheduler]
            [datalevin.kv.txlog :as kvtx]
            [datalevin.lmdb :as l]
            [datalevin.scan :as scan]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.phase :as phase]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [java.util.concurrent CountDownLatch TimeUnit]
           [java.io IOException]
           [java.util.concurrent.atomic AtomicBoolean]))

(def opts {:wal? true :snapshot-scheduler? false :wal-segment-prealloc? false})
(def ^:dynamic *caller* nil)

(defn- grouped-updates! [db ops]
  (let [collector (:collector (:independent-control @(i/kv-info db)))
        paused (CountDownLatch. 1) release (CountDownLatch. 1)
        ready (CountDownLatch. (count ops)) first? (AtomicBoolean. true)
        jobs (atom [])
        uninstall (phase/observe!
                   (fn [event _]
                     (case event
                       :joint-publication
                       (when (.compareAndSet first? true false)
                         (.countDown paused) (.await release 10 TimeUnit/SECONDS))
                       :ready-published
                       (when-not (.get first?) (.countDown ready))
                       nil)))]
    (try
      (let [seed (future (batch/submit! collector {:op (fn [_] :seed)}))]
        (swap! jobs conj seed)
        (is (.await paused 10 TimeUnit/SECONDS))
        (let [requests (mapv (fn [idx op]
                               (let [job (future (try (op) (catch Throwable t t)))
                                     remaining (- (count ops) idx 1)
                                     deadline (+ (System/nanoTime) 10000000000)]
                                 ;; Publish in order so the failing middle body
                                 ;; deterministically precedes its successor.
                                 (is (loop []
                                       (cond
                                         (= remaining (.getCount ready)) true
                                         (>= (System/nanoTime) deadline) false
                                         :else (do (Thread/sleep 1) (recur)))))
                                 job))
                             (range) ops)]
          (swap! jobs into requests)
          (is (.await ready 10 TimeUnit/SECONDS))
          (.countDown release)
          (is (= :seed (deref seed 10000 ::timeout)))
          (mapv #(deref % 10000 ::timeout) requests)))
      (finally
        (.countDown release)
        (doseq [job @jobs] (deref job 10000 nil))
        (uninstall)))))

(deftest default-relaxed-groups-count-requests-and-abort-failed-batches
  (let [dir (u/tmp-dir (str "m1-relaxed-count-" (random-uuid)))
        db (d/open-kv dir (assoc opts :wal-durability-profile :relaxed
                               :wal-group-commit 4 :wal-group-commit-ms 0
                               :snapshot-bootstrap-force? false))
        metrics #(wal/sync-manager-state (:sync-manager (wal/state db)))]
    (try
      (d/open-dbi db "data")
      (d/transact-kv db "data" [[:put 1 0]] :long :long)
      (kv/force-txlog-sync! db)
      (let [before (metrics)
            increment #(d/update-kv db "data" 1 inc :long :long)]
        (is (= [:transacted :transacted] (grouped-updates! db [increment increment])))
        (let [after (metrics)]
          (is (= (inc (:last-appended-lsn before)) (:last-appended-lsn after)))
          (is (= (:last-durable-lsn before) (:last-durable-lsn after)))
          (is (= [1 2] ((juxt :pending-count :unsynced-count) after))))
        (is (= [:transacted :transacted] (grouped-updates! db [increment increment])))
        ;; Relaxed acknowledgements can precede the worker's requested sync.
        (let [deadline (+ (System/nanoTime) 10000000000)]
          (loop []
            (when (and (< (:last-durable-lsn (metrics))
                          (+ 2 (:last-appended-lsn before)))
                       (< (System/nanoTime) deadline))
              (Thread/sleep 1) (recur))))
        (let [after (metrics)]
          (is (= (+ 2 (:last-appended-lsn before))
                 (:last-appended-lsn after) (:last-durable-lsn after)))
          (is (= :batch-count (:last-sync-reason after)))
          (is (zero? (:unsynced-count after))))
        (let [failure (ex-info "bad request" {}) before (metrics)
              successor? (atom false)
              bad #(d/update-kv db "data" 1 (fn [_] (throw failure)) :long :long)
              successor #(d/update-kv db "data" 1
                                      (fn [value] (reset! successor? true) (inc value))
                                      :long :long)
              results (grouped-updates! db [increment bad successor])]
          (is (every? #(identical? failure %) results))
          (is (false? @successor?))
          (is (= (:last-appended-lsn before) (:last-appended-lsn (metrics))))
          (is (= 4 (d/get-value db "data" 1 :long :long)))
          (is (= :transacted (increment)))
          (is (= 5 (d/get-value db "data" 1 :long :long)))))
      (finally (d/close-kv db)))
    (try
      (let [reopened (d/open-kv dir)]
        (try (is (= 5 (d/get-value reopened "data" 1 :long :long)))
             (finally (d/close-kv reopened))))
      (finally (u/delete-files dir)))))

(deftest default-startup-floor-failure-aborts-writer-without-closing-store
  (let [dir (u/tmp-dir (str "m1-invalid-floor-" (random-uuid)))
        db (d/open-kv dir opts)
        raw (kv/raw-lmdb db)
        called? (atom false)]
    (try
      (d/open-dbi db "data")
      (d/with-transaction-kv [tx db]
        (d/transact-kv tx "data" [[:put 0 0]] :long :long))
      (i/transact-kv raw
                     [[:put c/kv-info c/wal-snapshot-current-lsn
                       :invalid-floor :keyword :data]])
      (is (thrown? ClassCastException
                   (d/with-transaction-kv [_ db] (reset! called? true))))
      (is (false? @called?))
      (is (nil? @(l/write-txn db)))
      (is (false? (i/closed-kv? db)))
      (is (= 0 (d/get-value db "data" 0 :long :long)))
      (i/transact-kv raw
                     [[:del c/kv-info c/wal-snapshot-current-lsn :keyword]])
      (d/with-transaction-kv [tx db]
        (d/transact-kv tx "data" [[:put 1 1]] :long :long))
      (is (= 1 (d/get-value db "data" 1 :long :long)))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest default-opener-dynamic-catalog-and-existing-store
  (let [dir (u/tmp-dir (str "m1-existing-" (random-uuid)))]
    (try
      ;; Seed the existing storage format through the low-level opener.
      (let [old (l/open-kv dir opts)]
        (try (i/open-dbi old "data")
             (i/transact-kv old "data" [[:put 1 2]] :long :long)
             (finally (i/close-kv old))))
      (let [db (d/open-kv dir opts)]
        (try
          (is (:embedded? (:independent-control @(i/kv-info db))))
          (is (= 2 (d/get-value db "data" 1 :long :long)))
          (d/open-list-dbi db "list")
          (d/put-list-items db "list" 1 [3 2 1] :long :long)
          (is (= [1 2 3] (vec (d/get-list db "list" 1 :long :long))))
          (d/del-list-items db "list" 1 [2] :long :long)
          (is (= [1 3] (vec (d/get-list db "list" 1 :long :long))))
          (d/clear-dbi db "list")
          (is (empty? (d/get-list db "list" 1 :long :long)))
          (d/drop-dbi db "list")
          (is (= ["data"] (vec (d/list-dbis db))))
          (is (map? (d/create-snapshot! db)))
          (is (seq (d/list-snapshots db)))
          (is (map? (kv/force-txlog-sync! db)))
          (finally (d/close-kv db))))
      (let [db (d/open-kv dir opts)]
        (try (is (= 2 (d/get-value db "data" 1 :long :long)))
             (is (= :transacted (d/update-kv db "data" 1 inc :long :long)))
             (is (= 3 (d/get-value db "data" 1 :long :long)))
             (finally (d/close-kv db))))
      (finally (u/delete-files dir)))))

(deftest default-conditional-and-manual-transactions
  (let [dir (u/tmp-dir (str "m1-flags-" (random-uuid))) db (d/open-kv dir opts)]
    (try
      (d/open-dbi db "data")
      (is (= :transacted (d/transact-kv db "data" [[:put 1 2 [:nooverwrite]]] :long :long)))
      (is (thrown? Exception (d/transact-kv db "data" [[:put 1 3 [:nooverwrite]]] :long :long)))
      (is (= 2 (d/get-value db "data" 1 :long :long)))
      (is (= :transacted (d/transact-kv db "data" [[:put 2 4 [:append]]] :long :long)))
      (is (thrown? Exception
                   (d/with-transaction-kv [tx db]
                     (d/transact-kv tx "data" [[:put 3 5]] :long :long)
                     (try (d/transact-kv tx "data" [[:put 1 9 [:nooverwrite]]] :long :long)
                          (catch Exception _ nil)))))
      (is (nil? (d/get-value db "data" 3 :long :long)))
      (doseq [op [#(d/open-dbi % "bad") #(d/clear-dbi % "data") #(d/drop-dbi % "data")]]
        (is (thrown? Exception (d/with-transaction-kv [tx db] (op tx)))))
      (locking (l/write-txn db)
        (let [tx (i/open-transact-kv db)]
          (try (d/transact-kv tx "data" [[:put 4 6]] :long :long)
               (finally (i/close-transact-kv db)))))
      (is (= 6 (d/get-value db "data" 4 :long :long)))
      (is (= :transacted (d/update-kv db "data" 1 inc :long :long)))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest default-preserves-large-inputs-and-caller-bindings
  (let [dir (u/tmp-dir (str "m1-context-" (random-uuid))) db (d/open-kv dir opts)
        entered (CountDownLatch. 1) release (CountDownLatch. 1)]
    (try
      (d/open-dbi db "data")
      (let [owner (future (d/with-transaction-kv [_tx db]
                            (.countDown entered) (.await release)))
            _ (is (.await entered 10 TimeUnit/SECONDS))
            caller (binding [*caller* :kept]
                     (future (d/update-kv db "data" 1 (fn [_] *caller*) :long :keyword)))]
        (.countDown release)
        (is (not= ::timeout (deref owner 10000 ::timeout)))
        (is (= :transacted (deref caller 10000 ::timeout)))
        (is (= :kept (d/get-value db "data" 1 :long :keyword))))
      (let [value (byte-array (* 2 1024 1024) (byte 7))]
        (d/with-transaction-kv [tx db]
          (d/transact-kv tx "data" [[:put 2 value]] :long :bytes)
          (is (= (alength value) (alength ^bytes (d/get-value tx "data" 2 :long :bytes)))))
        (is (= (seq value) (seq (d/get-value db "data" 2 :long :bytes)))))
      (finally (.countDown release) (d/close-kv db) (u/delete-files dir)))))

(deftest default-concurrent-updates-and-durability
  (doseq [profile [:strict :relaxed :extra]]
    (let [dir (u/tmp-dir (str "m1-concurrent-" (random-uuid)))
          db (d/open-kv dir (assoc opts :wal-durability-profile profile))]
      (try
        (d/open-dbi db "data")
        (let [writers (doall (repeatedly 8 #(future (dotimes [_ 20]
                                                    (d/update-kv db "data" 1 (fnil inc 0) :long :long)))))]
          (doseq [writer writers] (is (not= ::timeout (deref writer 30000 ::timeout)))))
        (is (= 160 (d/get-value db "data" 1 :long :long)))
        (let [wm (d/txlog-watermarks db)]
          (is (= (:last-committed-lsn wm) (:last-applied-lsn wm))))
        (finally (d/close-kv db) (u/delete-files dir))))))

(deftest default-map-growth-replays-frozen-writes-without-repeating-body
  (let [dir (u/tmp-dir (str "m1-growth-" (random-uuid)))
        db (d/open-kv dir (assoc opts :mapsize 1)) calls (atom 0)]
    (try
      (d/open-dbi db "data")
      (d/with-transaction-kv [tx db]
        (swap! calls inc)
        (d/transact-kv tx "data" (mapv #(vector :put % (byte-array 4096)) (range 2000)) :long :bytes)
        (is (= 4096 (alength ^bytes (d/get-value tx "data" 1999 :long :bytes))))
        (d/transact-kv tx "data" [[:put 2000 (byte-array 4096)]] :long :bytes))
      (is (= 1 @calls))
      (is (= 2001 (d/entries db "data")))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest default-close-waits-for-owned-native-commit
  (let [dir (u/tmp-dir (str "m1-close-" (random-uuid))) db (d/open-kv dir opts)
        entered (CountDownLatch. 1) release (CountDownLatch. 1)]
    (try
      (d/open-dbi db "data")
      (let [owner (future (d/with-transaction-kv [tx db]
                            (d/transact-kv tx "data" [[:put 1 7]] :long :long)
                            (.countDown entered) (.await release) :done))]
        (is (.await entered 10 TimeUnit/SECONDS))
        (let [closing (future (d/close-kv db))]
          (is (= ::waiting (deref closing 20 ::waiting)))
          (.countDown release)
          (is (= :done (deref owner 10000 ::timeout)))
          (is (not= ::timeout (deref closing 10000 ::timeout)))))
      (let [reopened (d/open-kv dir opts)]
        (try (is (= 7 (d/get-value reopened "data" 1 :long :long)))
             (finally (d/close-kv reopened))))
      (finally (.countDown release) (d/close-kv db) (u/delete-files dir)))))

(defn- fail-native-reader! [tx failure]
  (let [db (kv/raw-lmdb tx) native (i/get-dbi db "data" false)
        broken (reify l/IDB
                 (put-read-key [_ rtx key kt] (l/put-read-key native rtx key kt))
                 (get-kv [_ _] (throw failure)))
        reader (with-meta (reify i/ILMDB
                            (check-ready [_] (i/check-ready db))
                            (get-dbi [_ _ _] broken)
                            l/IWriting
                            (writing? [_] true)
                            (write-txn [_] (l/write-txn db))) (meta db))]
    (scan/get-value reader "data" 1 :long :long true)))

(deftest default-native-reader-failure-fences-even-when-caught
  (doseq [caught? [false true :abort]]
    (let [dir (u/tmp-dir (str "m1-reader-" (random-uuid))) db (d/open-kv dir opts)]
      (try
        (d/open-dbi db "data")
        (is (thrown? Throwable
                     (d/with-transaction-kv [tx db]
                       (d/transact-kv tx "data" [[:put 1 7]] :long :long)
                       (try (fail-native-reader! tx (IOException. "native reader"))
                            (catch Throwable t (when-not caught? (throw t))))
                       (when (= :abort caught?) (d/abort-transact-kv tx)))))
        (is (not (batch/serving? (get-in @(i/kv-info db) [:independent-control :collector]))))
        (is (nil? (d/get-value db "data" 1 :long :long)))
        (is (thrown? Throwable (d/transact-kv db "data" [[:put 2 8]] :long :long)))
        (is (thrown? Throwable (d/open-dbi db "after-failure")))
        (finally (d/close-kv db) (u/delete-files dir))))))

(deftest default-owner-interruption-restores-flag-without-fencing
  (let [dir (u/tmp-dir (str "m1-interrupt-" (random-uuid))) db (d/open-kv dir opts)]
    (try
      (d/open-dbi db "data")
      (let [result @(future
                      (try (d/with-transaction-kv [tx db]
                             (d/transact-kv tx "data" [[:put 1 7]] :long :long)
                             (throw (InterruptedException. "owner")))
                           (catch Throwable t {:error t :interrupted? (.isInterrupted (Thread/currentThread))})
                           (finally (Thread/interrupted))))]
        (is (:interrupted? result))
        (is (= :txlog/write-interrupted (:error (ex-data (:error result))))))
      (is (nil? (d/get-value db "data" 1 :long :long)))
      (is (= :transacted (d/transact-kv db "data" [[:put 2 8]] :long :long)))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest default-existing-options-and-wal-fault-hooks
  (doseq [stage [nil :txlog-append :txlog-sync]]
    (let [dir (u/tmp-dir (str "m1-hooks-" (random-uuid)))
          db (d/open-kv dir (assoc opts :wal-group-commit 0 :write-batch-size 8192))]
      (try
        (d/open-dbi db "data")
        (if stage
          (do
            (kvtx/set-storage-fault-hook! (fn [context]
                                           (when (= stage (:stage context))
                                             (throw (IOException. (name stage))))))
            (is (thrown? Throwable (d/transact-kv db "data" [[:put 1 7]] :long :long)))
            (is (not (batch/serving? (get-in @(i/kv-info db) [:independent-control :collector])))))
          (is (= :transacted (d/transact-kv db "data" [[:put 1 7]] :long :long))))
        (finally (kvtx/clear-storage-fault-hook!) (d/close-kv db) (u/delete-files dir))))))

(deftest default-opener-honors-existing-idle-batch-window
  (let [dir (u/tmp-dir (str "m1-idle-burst-" (random-uuid)))
        db (d/open-kv dir (assoc opts :write-batch-size 4 :write-batch-delay-us 1000000))
        jobs (atom [])]
    (try
      (d/open-dbi db "data")
      (let [collector (get-in @(i/kv-info db) [:independent-control :collector])
            before (:last-committed-lsn (d/txlog-watermarks db))
            first-job (future (d/transact-kv db "data" [[:put 0 0]] :long :long))]
        (swap! jobs conj first-job)
        (let [deadline (+ (System/nanoTime) 5000000000)]
          (loop []
            (when (and (zero? (:requests (batch/admitted-usage collector)))
                       (< (System/nanoTime) deadline))
              (Thread/sleep 1)
              (recur))))
        (is (not (realized? first-job)))
        (doseq [key [1 2 3]]
          (swap! jobs conj (future (d/transact-kv db "data" [[:put key key]] :long :long))))
        (is (= [:transacted :transacted :transacted :transacted]
               (mapv #(deref % 10000 ::timeout) @jobs)))
        (is (= (inc before) (:last-committed-lsn (d/txlog-watermarks db))))
        (doseq [key (range 4)] (is (= key (d/get-value db "data" key :long :long)))))
      (finally
        (doseq [job @jobs] (deref job 10000 nil))
        (d/close-kv db)
        (u/delete-files dir)))))

(deftest default-snapshot-does-not-defer-a-foreign-writer-when-disabled
  (let [dir (u/tmp-dir (str "m1-snapshot-contention-" (random-uuid)))
        db (d/open-kv dir (assoc opts :snapshot-defer-on-contention? false))
        entered (promise) release (promise) jobs (atom [])]
    (try
      (d/open-dbi db "data")
      (let [raw (kv/raw-lmdb db)
            owner (future (d/with-transaction-kv [tx db]
                            (d/transact-kv tx "data" [[:put 1 7]] :long :long)
                            (deliver entered true)
                            (deref release 10000 ::timeout))) ]
        (swap! jobs conj owner)
        (is (deref entered 10000 false))
        ;; Enable the explicit scheduler probe without starting a poll thread.
        (vswap! (i/kv-info raw) assoc :snapshot-scheduler? true)
        (let [copy (future (#'scheduler/maybe-run-snapshot-scheduler! raw))
              deadline (+ (System/nanoTime) 5000000000)]
          (swap! jobs conj copy)
          (loop []
            (when (and (nil? (:snapshot-scheduler-last-run-start-ms @(i/kv-info raw)))
                       (< (System/nanoTime) deadline))
              (Thread/sleep 1)
              (recur)))
          (is (some? (:snapshot-scheduler-last-run-start-ms @(i/kv-info raw))))
          (deliver release :done)
          (is (= :done (deref owner 10000 ::timeout)))
          (is (map? (deref copy 10000 ::timeout)))
          (is (= 7 (d/get-value db "data" 1 :long :long)))
          (is (seq (d/list-snapshots db)))))
      (finally
        (deliver release :done)
        (doseq [job @jobs] (deref job 10000 nil))
        (d/close-kv db)
        (u/delete-files dir)))))
