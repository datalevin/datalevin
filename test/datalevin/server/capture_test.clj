(ns datalevin.server.capture-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.embedding :as emb]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.secondary-index :as si]
            [datalevin.server.api :as api]
            [datalevin.server.capture :as capture]
            [datalevin.server.copy :as copy]
            [datalevin.storage :as st]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]
            [datalevin.vector :as v]
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

(defn- open-client-index! [source state opts]
  (api/new-vector-index
    {:get-store (fn [_ _] source)
     :db-state (fn [_ _] @state)
     :update-db (fn [_ _ f] (swap! state f))
     :update-client (fn [_ _ _])
     :write-message (fn [_ message] message)
     :with-index-write-admission (fn [_ _ f] (f))}
    nil nil :client {:args ["source" opts]})
  (:index @state))

(deftest cold-vector-capture-shares-client-initialization
  (doseq [compact? [false true] wal? [false true]]
    (testing (str {:compact? compact? :wal? wal?})
      (let [root (u/tmp-dir (str "capture-cold-vector-" (random-uuid)))
            dest (str root "/copy")
            source (d/open-kv (str root "/source") {:wal? wal?})
            opts {:dimensions 2}
            state (atom {})
            reads (atom 0)
            opening (promise)
            client-started (promise)
            mutated (promise)
            release (CountDownLatch. 1)
            tasks (atom [])
            options {:standalone? true :authorize-write! (fn [])
                     :admit-write (fn [f] (f))
                     :with-writer
                     (fn [f]
                       (when-not (= true (deref mutated 5000 ::timeout))
                         (throw (ex-info "Client mutation did not finish" {})))
                       (binding [kv/*server-write-slot-held?* true] (f)))}]
        (try
          (binding [v/*submit-async-vec-save* (constantly nil)]
            (let [index (v/new-vector-index source opts)]
              (i/add-vec index :before (float-array [1 0]))
              (i/close-vecs index)))
          ;; Pause actual checkpoint loading, after capture has read the domain
          ;; catalog but before the cold native index is published.
          (i/transact-kv source
            [[:put c/vec-meta-dbi c/default-domain
              (assoc (i/get-value source c/vec-meta-dbi c/default-domain :string :data)
                     ::loading (HistoryValue.)) :string :data]])
          (u/create-dirs dest)
          (let [capture-job
                (future
                  (binding [*history-read!*
                            #(when (= 2 (swap! reads inc))
                               (deliver opening (Thread/holdsLock source))
                               (when-not (.await release 5 TimeUnit/SECONDS)
                                 (throw (ex-info "Cold loader was not released" {}))))]
                    (capture/attempt! source dest compact? options)))]
            (swap! tasks conj capture-job)
            (is (= true (deref opening 5000 ::timeout)))
            (let [client-job
                  (future
                    (deliver client-started true)
                    (binding [v/*submit-async-vec-save* (constantly nil)]
                      (i/add-vec (open-client-index! source state opts)
                                 :later (float-array [0 1])))
                    (deliver mutated true)
                    :mutated)]
              (swap! tasks conj client-job)
              (is (= true (deref client-started 5000 ::timeout)))
              (is (= ::blocked (deref client-job 100 ::blocked)))
              (.countDown release)
              (is (= :mutated (deref client-job 5000 ::timeout))))
            (let [result (deref capture-job 5000 ::timeout)]
              (is (map? result))
              (copy/finalize-copy! dest "source" result)
              (spit (str dest "/VERSION") c/version)
              (let [restored (d/open-kv dest)]
                (try
                  (let [index (v/open-vector-index restored opts)]
                    (is (seq (i/get-list restored (str c/default-domain "/" c/vec-refs)
                                         :later :data :id)))
                    (is (= [[0.0 1.0]] (mapv vec (i/get-vec index :later))))
                    (is (= [:later] (i/search-vec index (float-array [0 1]) {:top 1})))
                    (is (= 2 (:size (i/vecs-info index)))))
                  (finally (d/close-kv restored))))))
          (finally
            (.countDown release)
            (doseq [task @tasks] (deref task 5000 ::timeout))
            (d/close-kv source)
            (u/delete-files root)))))))

(deftest client-reuses-vector-index-opened-for-capture
  (let [root (u/tmp-dir (str "capture-vector-reuse-" (random-uuid)))
        source (d/open-kv root)
        opts {:dimensions 2}]
    (try
      (i/close-vecs (v/new-vector-index source opts))
      (let [index (v/open-vector-index source opts)]
        (binding [v/*submit-async-vec-save* (constantly nil)]
          (i/add-vec index :pending (float-array [0 1])))
        (let [client-index (open-client-index! source (atom {}) opts)]
          (is (identical? index client-index))
          (is (= [:pending] (i/search-vec client-index (float-array [0 1]))))))
      (finally
        (d/close-kv source)
        (u/delete-files root)))))

(deftest late-cold-vector-domain-retries-before-initialization
  (let [root (u/tmp-dir (str "capture-late-domain-" (random-uuid)))
        dest (str root "/copy")
        source (d/open-kv (str root "/source"))
        injected? (atom false)
        opts {:dimensions 2 :domain "late"}
        options {:standalone? true :authorize-write! (fn [])
                 :admit-write (fn [f] (f))
                 :with-writer
                 (fn [f]
                   (when (compare-and-set! injected? false true)
                     (binding [v/*submit-async-vec-save* (constantly nil)]
                       (let [index (v/new-vector-index source opts)]
                         (i/add-vec index :late (float-array [0 1]))
                         (i/close-vecs index))))
                   (binding [kv/*server-write-slot-held?* true] (f)))}]
    (try
      (u/create-dirs dest)
      (is (= :retry (capture/attempt! source dest false options)))
      (let [result (capture/attempt! source dest false options)]
        (is (map? result))
        (copy/finalize-copy! dest "source" result)
        (spit (str dest "/VERSION") c/version)
        (let [restored (d/open-kv dest)]
          (try
            (is (= [:late] (i/search-vec (v/open-vector-index restored opts)
                                        (float-array [0 1]))))
            (finally (d/close-kv restored)))))
      (finally
        (d/close-kv source)
        (u/delete-files root)))))

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

(deftest unrelated-commits-do-not-invalidate-capture-job-history
  (doseq [compact? [false true] wal? [false true]]
    (testing (str {:compact? compact? :wal? wal?})
      (let [root (u/tmp-dir (str "capture-unrelated-writes-" (random-uuid)))
            dest (str root "/copy")
            source (st/open (str root "/source") nil {:wal? wal?})
            kv (capture/source-kv source)
            commits (atom 0)
            protected-reads (atom 0)
            options {:standalone? true :authorize-write! (fn [])
                     :admit-write (fn [f] (f))
                     :with-writer
                     (fn [f]
                       ;; Also commit after the scan, before acquiring the gate.
                       (i/transact-kv kv [[:put "unrelated" :after-scan true]])
                       (binding [kv/*server-write-slot-held?* true] (f)))}]
        (try
          (i/open-dbi kv "unrelated")
          (i/transact-kv kv
            (mapv #(si/job-tx
                     (si/completed-job
                       (si/make-job {:type :fulltext :op :add :domain c/default-domain
                                     :ref % :value (HistoryValue.) :tx %})))
                  (range 128)))
          (u/create-dirs dest)
          (let [result
                (binding [*history-read!*
                          (fn []
                            (when (or kv/*server-write-slot-held?*
                                      (Thread/holdsLock (l/write-txn kv)))
                              (swap! protected-reads inc))
                            ;; A commit at every decoded row makes the race
                            ;; deterministic without timers or a quiet window.
                            (i/transact-kv kv [[:put "unrelated" :during-scan
                                               (swap! commits inc)]]))]
                  (capture/attempt! source dest compact? options))]
            (is (map? result))
            (is (= 128 @commits))
            (is (zero? @protected-reads))
            (when (map? result)
              (copy/finalize-copy! dest "source" result)
              (spit (str dest "/VERSION") c/version)
              (let [restored (d/open-kv dest)]
                (try
                  (is (= 128 (i/get-value restored "unrelated" :during-scan)))
                  (is (= true (i/get-value restored "unrelated" :after-scan)))
                  (finally (d/close-kv restored))))))
          (finally
            (i/close source)
            (u/delete-files root)))))))

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
