(ns datalevin.relaxed-group-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.conn :as conn]
            [datalevin.core :as d]
            [datalevin.interpret :as inter]
            [datalevin.kv :as kv]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [datalevin.conn SyncQueuedResult]
           [datalevin.storage Store]
           [datalevin.tx_group Group]
           [java.util ArrayDeque]
           [java.util.concurrent ConcurrentLinkedQueue]
           [java.util.concurrent.atomic AtomicLong]
           [java.util.concurrent.locks ReentrantLock]
           [org.eclipse.collections.impl.list.mutable FastList]))

(use-fixtures :each db-fixture)

(def ^:private opts
  {:wal? true :wal-durability-profile :relaxed :wal-shared? false
   :wal-group-commit 4 :wal-group-commit-ms 0
   :wal-segment-prealloc? false :snapshot-bootstrap-force? false})

(defn- metrics [db]
  (wal/sync-manager-state (:sync-manager (wal/state db))))

(defn- grouped! [^Group g ops]
  (let [^ReentrantLock lock (.-lock g)
        ^ConcurrentLinkedQueue queue (.-queue g)
        jobs (atom [])]
    (.lock lock)
    (try
      (reset! jobs (mapv (fn [op] (future (try (op) (catch Throwable t t)))) ops))
      (is (loop [attempt 0]
            (cond
              (= (count ops) (.size queue)) true
              (= 1000 attempt) false
              :else (do (Thread/sleep 5) (recur (inc attempt))))))
      (finally (.unlock lock)))
    (mapv #(deref % 10000 ::timeout) @jobs)))

(deftest sync-counts-logical-requests-and-preserves-an-in-flight-tail
  (let [mgr (wal/new-sync-manager {:group-commit 5 :group-commit-ms 0
                                   :track-trailing? false})]
    (is (nil? (wal/append-sync-transition! mgr 1 1 {:request-count 2})))
    ;; Reobserving a record must not count its group again.
    (is (nil? (wal/append-sync-transition! mgr 1 2 {:request-count 2})))
    (is (= [1 2] ((juxt :pending-count :unsynced-count) (wal/sync-manager-state mgr))))
    (is (nil? (wal/append-sync-transition! mgr 2 3 {:request-count 2})))
    (is (= {:target-lsn 3 :reason :batch-count}
           (wal/append-sync-transition! mgr 3 4)))
    ;; These two groups arrive after the first sync captured its target.
    (is (nil? (wal/append-sync-transition! mgr 4 5 {:request-count 3})))
    (is (nil? (wal/append-sync-transition! mgr 5 6 {:request-count 2})))
    (wal/complete-sync-success! mgr 3 7 :batch-count)
    (is (= [2 5] ((juxt :pending-count :unsynced-count) (wal/sync-manager-state mgr))))
    (is (= {:target-lsn 5 :reason :batch-count} (wal/begin-sync! mgr)))
    (wal/complete-sync-success! mgr 5 8 :batch-count)
    (is (= [0 0] ((juxt :pending-count :unsynced-count) (wal/sync-manager-state mgr))))
    (is (.isEmpty ^ArrayDeque (:pending-group-counts mgr)))
    (is (zero? @(:pending-group-extra-count mgr)))))

(deftest weighted-counts-survive-sync-failure-and-time-triggered-sync
  (let [mgr (wal/new-sync-manager {:group-commit 100 :group-commit-ms 10
                                   :last-sync-ms 0 :track-trailing? false})
        failure (ex-info "sync failed" {})]
    (is (nil? (wal/append-sync-transition! mgr 1 1 {:request-count 4})))
    (is (= {:request? true :reason :batch-time} (wal/request-sync-if-needed! mgr 10)))
    (is (= {:target-lsn 1 :reason :batch-time} (wal/begin-sync! mgr)))
    (wal/complete-sync-failure! mgr failure)
    (is (= 4 (:unsynced-count (wal/sync-manager-state mgr))))
    (wal/reset-sync-health! mgr)
    (wal/request-sync-now! mgr)
    (is (= {:target-lsn 1 :reason :forced} (wal/begin-sync! mgr)))
    (wal/complete-sync-success! mgr 1 11 :forced)
    (is (zero? (:unsynced-count (wal/sync-manager-state mgr))))))

(deftest relaxed-kv-groups-sync-by-request-count-and-isolate-failures
  (let [dir (u/tmp-dir (str "relaxed-group-" (random-uuid)))
        db (d/open-kv dir opts)]
    (try
      (d/open-dbi db "counter")
      (d/transact-kv db [[:put "counter" 1 0 :long :long]])
      (kv/force-txlog-sync! db)
      (let [g (kv/write-group db :kv)
            before (metrics db)
            increment #(d/update-kv db "counter" 1 inc :long :long)]
        (is (some? g))
        (is (= [:transacted :transacted] (grouped! g [increment increment])))
        (let [after (metrics db)]
          (is (= (inc (:last-appended-lsn before)) (:last-appended-lsn after)))
          (is (= (:last-durable-lsn before) (:last-durable-lsn after)))
          (is (= [1 2] ((juxt :pending-count :unsynced-count) after))))
        (is (= [:transacted :transacted] (grouped! g [increment increment])))
        (let [after (metrics db)]
          (is (= (+ 2 (:last-appended-lsn before))
                 (:last-appended-lsn after) (:last-durable-lsn after)))
          (is (= :batch-count (:last-sync-reason after)))
          (is (zero? (:unsynced-count after))))
        (is (= 4 (d/get-value db "counter" 1 :long :long)))
        (d/with-transaction-kv [tx db]
          (is (nil? (kv/write-group tx :kv)))
          ;; Explicit caller-supplied transactions retain one-record accounting.
          (dotimes [_ 4] (d/update-kv tx "counter" 1 inc :long :long)))
        (is (= [1 1] ((juxt :pending-count :unsynced-count) (metrics db))))
        (kv/force-txlog-sync! db)
        (let [failure (ex-info "bad request" {})
              before (metrics db)
              bad #(d/update-kv db "counter" 1 (fn [_] (throw failure)) :long :long)
              results (grouped! g [increment bad increment])
              after (metrics db)]
          (is (= :transacted (first results) (last results)))
          (is (identical? failure (second results)))
          (is (= (+ 2 (:last-appended-lsn before)) (:last-appended-lsn after)))
          (is (= (:last-durable-lsn before) (:last-durable-lsn after)))
          (is (= [2 2] ((juxt :pending-count :unsynced-count) after)))
          (is (= 10 (d/get-value db "counter" 1 :long :long)))))
      (finally (d/close-kv db)))
    (try
      (let [reopened (d/open-kv dir)]
        (try (is (= 10 (d/get-value reopened "counter" 1 :long :long)))
             (finally (d/close-kv reopened))))
      (finally (u/delete-files dir)))))

(defn- datalog-batch! [cn txs]
  (let [results (mapv (fn [_] (promise)) txs)
        requests (FastList.)
        ^AtomicLong pending (#'conn/sync-queue-pending-counter cn)]
    (doseq [[tx result] (map vector txs results)]
      (.add requests (conn/->SyncQueuedReq tx nil result)))
    (.addAndGet pending (count txs))
    (#'conn/run-sync-queued-dl-batch! cn requests)
    (doseq [result results]
      (is (nil? (.-error ^SyncQueuedResult @result))))))

(deftest local-datalog-queue-counts-combined-and-individual-requests
  (doseq [path [:blind :general :individual]]
    (let [dir (u/tmp-dir (str "relaxed-datalog-" (random-uuid)))
          cn (d/create-conn
               dir {:key {:db/valueType :db.type/long :db/unique :db.unique/value}
                    :body (cond-> {:db/valueType :db.type/string}
                            (= path :individual) (assoc :db/fulltext true))}
               opts)
          db (d/datalog-kv cn)]
      (try
        (when (= path :individual)
          ;; Instantiate the synchronous search engine before queue dispatch.
          (d/transact! cn [{:key 0 :body "seed"}]))
        (kv/force-txlog-sync! db)
        (let [before (metrics db)
              txs (mapv (fn [n]
                          (if (= path :general)
                            [[:db/add n :body (str "text" n)]]
                            [{:key n :body (str "text" n)}])) [1 2])]
          (datalog-batch! cn txs)
          (let [after (metrics db)
                records (if (= path :individual) 2 1)]
            (is (= records (- (:last-appended-lsn after) (:last-appended-lsn before))))
            (is (= [records 2] ((juxt :pending-count :unsynced-count) after)))
            (is (= (:last-durable-lsn before) (:last-durable-lsn after)))))
        (finally (d/close cn) (u/delete-files dir))))))

(def increment-remote (inter/inter-fn [v] (inc (long v))))
(def increment-datalog
  (inter/inter-fn [db]
    [[:db/add 1 :counter
      (inc (long (:counter (datalevin.core/pull db [:counter] 1))))]]))

(deftest remote-relaxed-kv-and-datalog-share-native-commits
  (let [root (u/tmp-dir (str "remote-relaxed-group-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port})]
    (try
      (server/start srv)
      (doseq [api [:kv :datalog]]
        (let [name (str "relaxed-" (clojure.core/name api))
              uri (str "dtlv://datalevin:datalevin@localhost:" port "/" name)
              handles (mapv (fn [_]
                              (if (= api :kv)
                                (d/open-kv uri opts)
                                (d/create-conn uri {:counter {:db/valueType :db.type/long}}
                                                 opts))) (range 2))
              first-handle (first handles)
              store (#'server/get-store srv name false)
              db (if (= api :kv) store (.-lmdb ^Store store))
              g (kv/write-group db (if (= api :kv) :server-kv :server-datalog))
              ops (mapv (fn [handle]
                          (if (= api :kv)
                            #(d/update-kv handle "counter" 1 increment-remote :long :long)
                            #(d/transact-ack! handle [[:db.fn/call increment-datalog]])))
                        handles)]
          (try
            (if (= api :kv)
              (do (d/open-dbi first-handle "counter")
                  (d/transact-kv first-handle [[:put "counter" 1 0 :long :long]]))
              (d/transact-ack! first-handle [{:db/id 1 :counter 0}]))
            (kv/force-txlog-sync! db)
            (let [before (metrics db)]
              (is (= [:transacted :transacted] (grouped! g ops)))
              (let [after (metrics db)]
                (is (= (inc (:last-appended-lsn before)) (:last-appended-lsn after)))
                (is (= [1 2] ((juxt :pending-count :unsynced-count) after)))
                (is (= (:last-durable-lsn before) (:last-durable-lsn after))))
              (is (= [:transacted :transacted] (grouped! g ops)))
              (let [after (metrics db)]
                (is (= (+ 2 (:last-appended-lsn before))
                       (:last-appended-lsn after) (:last-durable-lsn after)))
                (is (zero? (:unsynced-count after)))))
            (is (= 4 (if (= api :kv)
                       (d/get-value first-handle "counter" 1 :long :long)
                       (:counter (d/pull @first-handle [:counter] 1)))))
            (finally
              (doseq [handle handles]
                (if (= api :kv) (d/close-kv handle) (d/close handle)))))))
      (finally (server/stop srv) (u/delete-files root)))))
