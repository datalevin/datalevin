(ns datalevin.server.capture-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.embedding :as emb]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.secondary-index :as si]
            [datalevin.server.capture :as capture]
            [datalevin.server.copy :as copy]
            [datalevin.storage :as st]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]
            [taoensso.nippy :as nippy])
  (:import [datalevin.vector VectorIndex]
           [java.util.concurrent CountDownLatch Semaphore TimeUnit]
           [java.util.concurrent.locks ReentrantReadWriteLock]))

(use-fixtures :once db-fixture)

(def ^:dynamic *history-read!* (fn []))
(deftype HistoryValue [])
(nippy/extend-freeze HistoryValue ::history-value [_value _out])
(nippy/extend-thaw ::history-value [_]
  (*history-read!*)
  (HistoryValue.))

(defn- blocking-provider [source entered ^CountDownLatch release]
  (reify emb/IEmbeddingProvider
    (embedding [_ items _]
      (let [raw (kv/raw-lmdb (capture/source-kv @source))
            ^VectorIndex index (st/embedding-index-by-domain @source c/default-domain)]
        (deliver entered
                 {:lmdb-locked? (Thread/holdsLock (l/write-txn raw))
                  :vector-locked? (.isWriteLocked ^ReentrantReadWriteLock (.-vec-lock index))
                  :writer-slot-held? kv/*server-write-slot-held?*}))
      (when-not (.await release 5 TimeUnit/SECONDS)
        (throw (ex-info "Test provider was not released" {})))
      (mapv (fn [_] (float-array [1 0])) items))
    (embedding-dimensions [_] 2)
    (embedding-metadata [_]
      {:embedding/provider {:id :test :model-id "test/late-capture"}
       :embedding/output {:dimensions 2 :normalize? false}})
    (close-provider [_])))

(deftest late-embedding-jobs-drain-outside-capture-locks
  (doseq [compact? [false true] wal? [false true] expired-lease? [false true]]
    (testing (str {:compact? compact? :wal? wal? :expired-lease? expired-lease?})
      (let [root (u/tmp-dir (str "capture-late-embedding-" (random-uuid)))
            dest (str root "/copy")
            source-ref (volatile! nil)
            entered (promise)
            release (CountDownLatch. 1)
            gate (Semaphore. 1)
            source (st/open (str root "/source")
                     {:text {:db/valueType :db.type/string :db/embedding true}}
                     {:wal? wal?
                      :embedding-opts {:provider :custom :indexing-mode :async}
                      :embedding-providers {:custom (blocking-provider source-ref entered release)}})
            kv (capture/source-kv source)
            ref [1 (get-in (i/schema source) [:text :db/aid]) "late"]
            job (cond-> (si/make-job {:type :embedding :op :add :domain c/default-domain
                                     :ref ref :value "late" :tx (i/max-tx source)})
                  expired-lease? (si/claimed-job "expired-worker" 0))
            injected? (atom false)
            first-result (promise)
            jobs (atom [])
            options {:standalone? true
                     :authorize-write! (fn [])
                     :admit-write (fn [f] (f))
                     :with-writer
                     (fn [f]
                       (.acquire gate)
                       (try
                         ;; Insert durable work after the pre-drain, without
                         ;; waking a background worker that could claim it.
                         (when (compare-and-set! injected? false true)
                           (i/transact-kv kv [(si/job-tx job)]))
                         (binding [kv/*server-write-slot-held?* true] (f))
                         (finally (.release gate))))}]
        (vreset! source-ref source)
        (try
          (i/open-dbi kv "data")
          (u/create-dirs dest)
          (let [capture-job
                (future
                  (let [result (capture/attempt! source dest compact? options)]
                    (deliver first-result result)
                    (let [deadline (+ (System/nanoTime) 5000000000)]
                      (loop [result result]
                        (if (= :retry result)
                          (if (< (System/nanoTime) deadline)
                            (recur (capture/attempt! source dest compact? options))
                            (throw (ex-info "Test capture kept retrying" {})))
                          result)))))]
            (swap! jobs conj capture-job)
            (is (= {:lmdb-locked? false :vector-locked? false :writer-slot-held? false}
                   (deref entered 5000 ::timeout)))
            (is (= :retry (deref first-result 0 ::pending)))
            (let [writer (future
                           (.acquire gate)
                           (try
                             (binding [kv/*server-write-slot-held?* true]
                               (i/transact-kv kv [[:put "data" :value "unblocked"]]))
                             (finally (.release gate))))]
              (swap! jobs conj writer)
              (is (= :transacted (deref writer 1000 ::timeout)))
              (is (not (realized? capture-job)))
              (.countDown release)
              (is (= :transacted (deref writer 5000 ::timeout))))
            (let [result (deref capture-job 5000 ::timeout)]
              (is (map? result))
              (is (= 1 (:completed-count (st/secondary-index-status source))))
              (copy/finalize-copy! dest "source" result)
              (spit (str dest "/VERSION") c/version)
              (with-open [restored (st/open dest)]
                (is (= "unblocked" (i/get-value (capture/source-kv restored) "data" :value)))
                (is (= [[1.0 0.0]]
                       (mapv vec (i/get-vec (st/embedding-index-by-domain restored c/default-domain)
                                           ref)))))))
          (finally
            (.countDown release)
            (doseq [job @jobs] (deref job 5000 ::timeout))
            (i/close source)
            (u/delete-files root)))))))

(deftest late-failed-jobs-reject-capture-before-checkpointing
  (let [root (u/tmp-dir (str "capture-late-failure-" (random-uuid)))
        dest (str root "/copy")
        source (st/open (str root "/source")
                 {:text {:db/valueType :db.type/string :db/fulltext true}}
                 {:search-opts {:indexing-mode :async}})
        kv (capture/source-kv source)
        job (si/failed-job
              (si/make-job {:type :fulltext :op :add :domain c/default-domain
                            :ref 1 :value "late" :tx (i/max-tx source)})
              (ex-info "Late index failure" {}))]
    (try
      (u/create-dirs dest)
      ;; Rewriting one retained row must invalidate preflight even though the
      ;; job count and Datalog transaction watermark do not change.
      (i/transact-kv kv [(si/job-tx (si/completed-job job))])
      (let [options {:standalone? true
                     :authorize-write! (fn [])
                     :admit-write (fn [f] (f))
                     :with-writer
                     (fn [f]
                       (i/transact-kv kv [(si/job-tx job)])
                       (binding [kv/*server-write-slot-held?* true] (f)))}
            _ (is (= :retry (capture/attempt! source dest false options)))
            error (try
                    (capture/attempt! source dest false options)
                    nil
                    (catch Exception e e))]
        (is (= :copy/index-flush-failed (:error (ex-data error))))
        (is (not (u/file-exists (str dest "/data.mdb")))))
      (finally (i/close source) (u/delete-files root)))))

(deftest completed-history-is-read-without-write-exclusion-and-revalidated
  (let [root (u/tmp-dir (str "capture-history-" (random-uuid)))
        dest (str root "/copy")
        source (st/open (str root "/source") nil {:wal? false})
        kv (capture/source-kv source)
        job (si/completed-job
              (si/make-job {:type :fulltext :op :add :domain c/default-domain
                            :ref 1 :value (HistoryValue.) :tx 1}))
        failed (si/failed-job job (ex-info "Concurrent job failure" {}))
        entered (promise)
        release (CountDownLatch. 1)
        reads (atom 0)
        protected-reads (atom 0)
        tasks (atom [])
        observe! (fn []
                   (when (or kv/*server-write-slot-held?*
                             (Thread/holdsLock (l/write-txn kv)))
                     (swap! protected-reads inc))
                   (when (= 1 (swap! reads inc))
                     (deliver entered true)
                     (when-not (.await release 5 TimeUnit/SECONDS)
                       (throw (ex-info "History reader was not released" {})))))
        options {:standalone? true :authorize-write! (fn [])
                 :admit-write (fn [f] (f))
                 :with-writer (fn [f] (binding [kv/*server-write-slot-held?* true] (f)))}]
    (try
      (i/transact-kv kv (mapv #(si/job-tx (assoc job :job/id [:fulltext c/default-domain % 0]
                                              :job/tx %))
                            (range 1 129)))
      (u/create-dirs dest)
      (let [capture-job (future
                          (binding [*history-read!* observe!]
                            (try (capture/attempt! source dest false options)
                                 (catch Exception e e))))]
        (swap! tasks conj capture-job)
        (is (= true (deref entered 5000 ::timeout)))
        (let [writer (future (i/transact-kv kv [(si/job-tx failed)]))]
          (swap! tasks conj writer)
          (is (= :transacted (deref writer 1000 ::timeout)))
          (.countDown release)
          (is (= :transacted (deref writer 5000 ::timeout))))
        (is (= :retry (deref capture-job 5000 ::timeout)))
        (is (not (u/file-exists (str dest "/data.mdb")))))
      (i/transact-kv kv [(si/job-tx job)])
      (binding [*history-read!* observe!]
        (is (map? (capture/attempt! source dest false options))))
      (is (pos? @reads))
      (is (zero? @protected-reads))
      (finally
        (.countDown release)
        (doseq [task @tasks] (deref task 5000 ::timeout))
        (i/close source)
        (u/delete-files root)))))

(deftest job-history-reader-protects-decoding-against-close
  (let [root (u/tmp-dir (str "capture-history-close-" (random-uuid)))
        source (st/open root nil {:wal? false})
        kv (capture/source-kv source)
        entered (promise)
        closing (promise)
        release (CountDownLatch. 1)
        tasks (atom [])]
    (try
      (i/transact-kv kv
        [(si/job-tx (si/completed-job
                     (si/make-job {:type :fulltext :op :add :domain c/default-domain
                                   :ref 1 :tx 1 :value (HistoryValue.)})))])
      (let [reader (future
                     (binding [*history-read!* #(do
                                                 (deliver entered true)
                                                 (.await release 5 TimeUnit/SECONDS))]
                       (st/secondary-index-jobs source)))]
        (swap! tasks conj reader)
        (is (= true (deref entered 5000 ::timeout)))
        (let [closer (future (deliver closing true) (i/close source) :closed)]
          (swap! tasks conj closer)
          (is (= true (deref closing 5000 ::timeout)))
          (is (= ::blocked (deref closer 50 ::blocked)))
          (.countDown release)
          (is (= 1 (count (deref reader 5000 ::timeout))))
          (is (= :closed (deref closer 5000 ::timeout)))
          (is (empty? (st/secondary-index-jobs source)))))
      (finally
        (.countDown release)
        (doseq [task @tasks] (deref task 5000 ::timeout))
        (i/close source)
        (u/delete-files root)))))
