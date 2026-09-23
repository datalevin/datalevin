(ns datalevin.strict-group-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.built-ins :as bi]
            [datalevin.client :as client]
            [datalevin.client-op :as cop]
            [datalevin.conn :as conn]
            [datalevin.core :as d]
            [datalevin.constants :as c]
            [datalevin.embedding :as emb]
            [datalevin.interpret :as inter]
            [datalevin.kv :as kv]
            [datalevin.kv.txlog :as kvtx]
            [datalevin.lmdb :as l]
            [datalevin.server :as server]
            [datalevin.storage :as s]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [datalevin.tx_group Group]
           [datalevin.conn SyncQueuedResult]
           [datalevin.remote DatalogStore]
           [datalevin.storage Store]
           [java.io IOException]
           [java.util.concurrent ConcurrentLinkedQueue]
           [java.util.concurrent.atomic AtomicLong]
           [org.eclipse.collections.impl.list.mutable FastList]))

(use-fixtures :each db-fixture)

(def opts {:wal? true :wal-durability-profile :strict :wal-shared? false
           :wal-segment-prealloc? false :snapshot-bootstrap-force? false
           :flags (conj c/default-env-flags :writemap)})

(def ^:dynamic *submitted-value* nil)

(defn- local-datalog-batch! [db txs]
  ;; Submit a complete group directly so visibility checks do not depend on
  ;; whether the background worker happens to combine concurrent callers.
  (let [results (mapv (fn [_] (promise)) txs)
        requests (FastList.)
        ^AtomicLong pending (#'conn/sync-queue-pending-counter db)]
    (doseq [[idx tx result] (map vector (range) txs results)]
      (.add requests (conn/->SyncQueuedReq tx {:request idx} result)))
    (.addAndGet pending (count txs))
    (#'conn/run-sync-queued-dl-batch! db requests)
    (is (zero? (.get pending)))
    (mapv (fn [result]
            (let [^SyncQueuedResult result @result]
              {:report (.-report result) :error (.-error result)}))
          results)))

(deftest local-datalog-group-observes-preceding-writes
  (doseq [prepare? [false true]
          cache-limit [0 16]]
    (binding [c/*use-prepare-path* prepare?]
      (let [path (u/tmp-dir (str "local-group-rmw-" (random-uuid)))
            db (d/create-conn path {:counter {:db/valueType :db.type/long}}
                              (assoc opts :cache-limit cache-limit))]
        (try
          (d/transact! db [{:db/id 1 :counter 0}])
          (let [before (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db)))
                observed (atom [])
                increment (fn [tx-db]
                            (let [value (:counter (d/pull tx-db [:counter] 1))]
                              (swap! observed conj value)
                              ;; Preparing a group must not mutate the overlays
                              ;; of the DB published to concurrent readers.
                              (is (empty? (:eavt @db)))
                              (is (empty? (:avet @db)))
                              (is (= 0 (deref (future (:counter (d/pull @db [:counter] 1)))
                                               5000 ::timeout)))
                              [[:db/add 1 :counter (inc value)]]))
                results (local-datalog-batch!
                          db (repeat 3 [[:db.fn/call increment]]))
                reports (mapv :report results)
                after (d/txlog-watermarks (d/datalog-kv db))]
            (is (every? nil? (map :error results)))
            (is (= [0 1 2] @observed))
            (is (= 3 (:counter (d/pull @db [:counter] 1))))
            (is (= [{:request 0} {:request 1} {:request 2}]
                   (mapv :tx-meta reports)))
            (is (apply < (map #(get-in % [:tempids :db/current-tx]) reports)))
            (doseq [report reports]
              (is (not (l/writing? (.-lmdb ^Store (:store (:db-before report))))))
              (is (= 3 (:counter (d/pull (:db-before report) [:counter] 1))))
              (is (identical? @db (:db-after report)))
              (is (= 3 (:counter (d/pull (:db-after report) [:counter] 1)))))
            (is (= (inc before) (:last-committed-lsn after)))
            (is (= (:last-committed-lsn after) (:last-durable-lsn after))))
          (finally (d/close db) (u/delete-files path)))))))

(deftest local-datalog-group-resolves-new-entities-and-retractions
  (let [path (u/tmp-dir (str "local-group-entities-" (random-uuid)))
        schema {:key {:db/unique :db.unique/identity}
                :counter {:db/valueType :db.type/long}}
        db (d/create-conn path schema opts)]
    (try
      (let [results
            (local-datalog-batch!
              db [[[:db.fn/call
                    (fn [_] [{:db/id -1 :key "one" :counter 0}])]]
                  [[:db.fn/call
                    (fn [tx-db]
                      (is (= 0 (:counter (d/pull tx-db [:counter] [:key "one"]))))
                      [[:db/add [:key "one"] :counter 1]
                       {:db/id -1 :key "two" :counter 10}])]]
                  [[:db.fn/call
                    (fn [tx-db]
                      (is (= 1 (:counter (d/pull tx-db [:counter] [:key "one"]))))
                      (is (= 10 (:counter (d/pull tx-db [:counter] [:key "two"]))))
                      [[:db/retractEntity [:key "one"]]
                       [:db/add [:key "two"] :counter 11]])]]])
            reports (mapv :report results)]
        (is (every? nil? (map :error results)))
        (is (< (get-in reports [0 :tempids -1]) (get-in reports [1 :tempids -1])))
        (is (= #{["two" 11]}
               (d/q '[:find ?key ?n :where [?e :key ?key] [?e :counter ?n]] @db))))
      (d/close db)
      (let [reopened (d/create-conn path)]
        (try
          (is (nil? (d/pull @reopened [:counter] [:key "one"])))
          (is (= 11 (:counter (d/pull @reopened [:counter] [:key "two"]))))
          (finally (d/close reopened))))
      (finally (d/close db) (u/delete-files path)))))

(deftest local-datalog-group-rolls-back-on-function-error
  (let [path (u/tmp-dir (str "local-group-abort-" (random-uuid)))
        db (d/create-conn path {:counter {:db/valueType :db.type/long}} opts)]
    (try
      (d/transact! db [{:db/id 1 :counter 0}])
      (let [before (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db)))
            failure (ex-info "failed transaction function" {})
            results (local-datalog-batch!
                      db [[[:db/add 1 :counter 1]]
                          [[:db.fn/call (fn [_] (throw failure))]]])]
        (is (every? #(some? (:error %)) results))
        (is (every? #(nil? (:report %)) results))
        (is (= 0 (:counter (d/pull @db [:counter] 1))))
        (is (= before (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db)))))
        (d/transact! db [[:db/add 1 :counter 2]])
        (is (= 2 (:counter (d/pull @db [:counter] 1)))))
      (finally (d/close db) (u/delete-files path)))))

(deftest local-datalog-secondary-engines-retain-individual-transactions
  (let [path (u/tmp-dir (str "local-group-secondary-" (random-uuid)))
        schema {:text {:db/valueType :db.type/string :db/fulltext true}
                :doc {:db/valueType :db.type/idoc :db/domain "docs"}}
        db (d/create-conn path schema opts)
        matches #(into #{} (map first) (bi/fulltext @db % nil))
        idoc-q '[:find [?e ...] :in $ ?query
                 :where [(idoc-match $ :doc ?query nil) [[?e _]]]]]
    (try
      (d/transact! db [{:db/id 1 :text "red fox" :doc {:counter 0}}])
      (is (= #{1} (matches "red")))
      (let [before (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db)))
            results
            (local-datalog-batch!
              db [[[:db/add 1 :text "blue bird"] [:db/add 1 :doc {:counter 1}]]
                  [[:db.fn/call (fn [_] (throw (ex-info "abort" {})))]]
                  [[:db.fn/call
                    (fn [tx-db]
                      (is (= {:text "blue bird" :doc {:counter 1}}
                             (d/pull tx-db [:text :doc] 1)))
                      [[:db/add 1 :text "green frog"]
                       [:db/add 1 :doc {:counter 2}]])]]])]
        (is (= [false true false] (mapv #(some? (:error %)) results)))
        (is (= [true false true] (mapv #(some? (:report %)) results)))
        (is (= (+ before 2)
               (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db)))))
        (is (= #{1} (matches "green")))
        (is (empty? (matches "red")))
        (is (empty? (matches "blue")))
        (is (= [1] (d/q idoc-q @db {:counter 2})))
        (is (empty? (d/q idoc-q @db {:counter 0})))
        (is (empty? (d/q idoc-q @db {:counter 1}))))
      (finally (d/close db) (u/delete-files path)))))

(defn- group-embedding-provider []
  (reify emb/IEmbeddingProvider
    (embedding [_ items _] (mapv (fn [_] (float-array [1.0 0.0])) items))
    (embedding-metadata [_]
      {:embedding/provider {:kind :test :id :strict-group}
       :embedding/output {:dimensions 2}})
    (embedding-dimensions [_] 2)
    (close-provider [_] nil)))

(deftest local-datalog-async-secondary-engines-share-atomic-commits
  (doseq [kind [:fulltext :vector :embedding :idoc]
          named? [false true]]
    (testing (str kind ", domain options " named?)
      (let [path (u/tmp-dir (str "local-group-async-" (random-uuid)))
            domain (if (or (= kind :vector) (and (= kind :idoc) (not named?)))
                     "value" (if named? "docs" c/default-domain))
            domain-opts (cond-> {:indexing-mode :async}
                          (#{:vector :embedding} kind) (assoc :dimensions 2))
            value-schema
            (case kind
              :fulltext (cond-> {:db/valueType :db.type/string :db/fulltext true}
                          named? (assoc :db.fulltext/domains [domain]))
              :vector {:db/valueType :db.type/vec}
              :embedding (cond-> {:db/valueType :db.type/string :db/embedding true}
                           named? (assoc :db.embedding/domains [domain]))
              :idoc (cond-> {:db/valueType :db.type/idoc}
                      named? (assoc :db/domain domain)))
            option-key (case [kind named?]
                         [:fulltext false] :search-opts
                         [:fulltext true] :search-domains
                         [:vector false] :vector-opts
                         [:vector true] :vector-domains
                         [:embedding false] :embedding-opts
                         [:embedding true] :embedding-domains
                         [:idoc false] :idoc-opts
                         [:idoc true] :idoc-domains)
            store-opts (cond-> (assoc opts option-key
                                     (if named? {domain domain-opts} domain-opts))
                         (= kind :embedding)
                         (assoc :embedding-providers {:default (group-embedding-provider)}))
            db (d/create-conn path {:counter {:db/valueType :db.type/long}
                                    :value value-schema} store-opts)
            increment (fn [tx-db]
                        (let [n (inc (:counter (d/pull tx-db [:counter] 1)))
                              value (case kind
                                      :idoc {:counter n}
                                      :vector (float-array [n 1.0])
                                      (nth ["red fox" "blue bird" "green frog" "amber fish"]
                                           (dec n)))]
                          [[:db/add 1 :counter n] [:db/add 1 :value value]]))]
        (try
          (d/transact! db [{:db/id 1 :counter 0}])
          ;; Block job claiming while counting source commits and inspecting
          ;; queued jobs. The normal worker runs after this lock is released.
          (locking (.-write-txn ^Store (:store @db))
            (let [before (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db)))
                  results (local-datalog-batch! db (repeat 3 [[:db.fn/call increment]]))
                  after (d/txlog-watermarks (d/datalog-kv db))
                  jobs (s/secondary-index-jobs (:store @db))]
              (is (every? nil? (map :error results)))
              (is (= 3 (:counter (d/pull @db [:counter] 1))))
              (is (= (inc before) (:last-committed-lsn after)))
              (is (= (:last-committed-lsn after) (:last-durable-lsn after)))
              (is (= (if (= kind :idoc) 3 5) (count jobs)))
              (is (= #{kind} (set (map :job/type jobs))))
              (is (= #{:pending} (set (map :job/status jobs))))
              (is (= 3 (count (set (map :job/tx jobs)))))
              (let [failed (local-datalog-batch!
                             db [[[:db.fn/call increment]]
                                 [[:db.fn/call (fn [_] (throw (ex-info "abort" {})))]]])]
                (is (every? #(some? (:error %)) failed))
                (is (= 3 (:counter (d/pull @db [:counter] 1))))
                (is (= (:last-committed-lsn after)
                       (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db)))))
                (is (= (mapv :job/id jobs)
                       (mapv :job/id (s/secondary-index-jobs (:store @db))))))))
          (is (:caught-up? (d/wait-for-secondary-index db {:timeout-ms 5000 :poll-ms 10})))
          (let [rows (case kind
                       :fulltext (bi/fulltext @db "green" {:domains [domain]})
                       :vector (bi/vec-neighbors @db :value (float-array [3.0 1.0]) {:top 10})
                       :embedding (bi/embedding-neighbors @db "green frog"
                                                          {:domains [domain] :top 10})
                       :idoc (bi/idoc-match @db :value {:counter 3} nil))]
            (is (= [[1 :value]] (mapv #(vec (take 2 %)) rows))))
          (finally (d/close db) (u/delete-files path)))))))

(deftest local-datalog-async-idoc-patches-share-commit
  (let [path (u/tmp-dir (str "local-group-async-patch-" (random-uuid)))
        db (d/create-conn path {:doc {:db/valueType :db.type/idoc}}
                          (assoc opts :idoc-opts {:indexing-mode :async}))]
    (try
      (d/transact! db [{:db/id 1 :doc {:n 0}}])
      (is (:caught-up? (d/wait-for-secondary-index db {:timeout-ms 5000})))
      (locking (.-write-txn ^Store (:store @db))
        (let [before (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db)))
              results (local-datalog-batch!
                        db (repeat 3 [[:db.fn/patchIdoc 1 :doc
                                       [[:update [:n] :inc]]]]))]
          (is (every? nil? (map :error results)))
          (is (= {:n 3} (:doc (d/pull @db [:doc] 1))))
          (is (= (inc before)
                 (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db)))))
          (is (= [[1 :doc {:n 0}]] (mapv vec (bi/idoc-match @db :doc {:n 0} nil))))))
      (is (:caught-up? (d/wait-for-secondary-index db {:timeout-ms 5000})))
      (is (= [[1 :doc {:n 3}]] (mapv vec (bi/idoc-match @db :doc {:n 3} nil))))
      (is (empty? (bi/idoc-match @db :doc {:n 0} nil)))
      (finally (d/close db) (u/delete-files path)))))

(deftest local-datalog-mixed-indexing-modes-retain-individual-commits
  (doseq [[value-schema domain-opts]
          [[{:db/valueType :db.type/string :db/fulltext true :db.fulltext/autoDomain true}
            {:search-opts {:indexing-mode :async}}]
           [{:db/valueType :db.type/string :db/fulltext true
             :db.fulltext/domains ["async" "sync"]}
            {:search-domains {"async" {:indexing-mode :async} "sync" {}}}]
           [{:db/valueType :db.type/vec}
            {:vector-opts {:dimensions 2 :indexing-mode :async}
             :vector-domains {"value" {:dimensions 2 :indexing-mode :sync}}}]
           [{:db/valueType :db.type/idoc :db/domain "docs"}
            {:idoc-opts {:indexing-mode :async}
             :idoc-domains {"docs" {:indexing-mode :sync}}}]]]
    (let [path (u/tmp-dir (str "local-group-mixed-" (random-uuid)))
          db (d/create-conn path {:counter {:db/valueType :db.type/long}
                                  :value value-schema} (merge opts domain-opts))]
      (try
        (d/transact! db [{:db/id 1 :counter 0}])
        (let [before (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db)))
              results (local-datalog-batch!
                        db (repeat 2 [[:db.fn/call
                                       (fn [tx-db]
                                         [[:db/add 1 :counter
                                           (inc (:counter (d/pull tx-db [:counter] 1)))]])]]))]
          (is (every? nil? (map :error results)))
          (is (= 2 (:counter (d/pull @db [:counter] 1))))
          (is (= (+ before 2)
                 (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv db))))))
        (finally (d/close db) (u/delete-files path))))))

(defn- await! [pred]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (cond (pred) true
            (> (System/nanoTime) deadline) false
            :else (do (Thread/sleep 1) (recur))))))

(defn- queued [^Group g]
  (.size ^ConcurrentLinkedQueue (.-queue g)))

(defn- with-kv [f]
  (let [path (u/tmp-dir (str "strict-group-" (random-uuid)))
        db (d/open-kv path opts)]
    (try
      (d/open-dbi db "counter")
      (d/transact-kv db [[:put "counter" 1 0 :id :long]])
      (f db path)
      (finally (kvtx/clear-storage-fault-hook!)
               (d/close-kv db) (u/delete-files path)))))

(deftest queued-rmw-shares-a-durable-commit-and-preserves-every-update
  (with-kv
    (fn [db path]
      (let [g (kv/strict-write-group db :kv)
            entered (promise)
            release (promise)
            before (:last-committed-lsn (d/txlog-watermarks db))
            first-job (future (d/update-kv db "counter" 1
                                          (fn [old]
                                            (deliver entered true)
                                            (assert (deref release 10000 false))
                                            (inc old)) :id :long))
            jobs (atom [])]
        (try
          (is (true? (deref entered 10000 false)))
          (reset! jobs (mapv (fn [n]
                              (future
                                (binding [*submitted-value* n]
                                  (d/update-kv db "counter" 1
                                               (fn [old]
                                                 (is (= n *submitted-value*))
                                                 (inc old)) :id :long))))
                            (range 7)))
          (is (await! #(= 7 (queued g))))
          (is (= 0 (d/get-value db "counter" 1 :id :long)))
          (is (not-any? realized? @jobs))
          (deliver release true)
          (doseq [job (cons first-job @jobs)]
            (is (= :transacted (deref job 10000 ::timeout))))
          (is (= 8 (d/get-value db "counter" 1 :id :long)))
          (let [marks (d/txlog-watermarks db)]
            (is (= (+ before 2) (:last-committed-lsn marks)))
            (is (= (:last-committed-lsn marks) (:last-durable-lsn marks))))
          (d/close-kv db)
          (let [reopened (d/open-kv path opts)]
            (try (is (= 8 (d/get-value reopened "counter" 1 :id :long)))
                 (finally (d/close-kv reopened))))
          (finally
            (deliver release true)
            (doseq [job (cons first-job @jobs)] (deref job 10000 nil))))))))

(deftest bad-request-rolls-back-group-and-does-not-fail-other-callers
  (with-kv
    (fn [db _]
      (let [g (kv/strict-write-group db :kv)
            entered (promise) release (promise)
            first-job (future (d/update-kv db "counter" 1
                                          (fn [old] (deliver entered true)
                                            (assert (deref release 10000 false))
                                            (inc old)) :id :long))
            jobs (atom [])
            failure (IllegalArgumentException. "bad update")]
        (try
          (is (deref entered 10000 false))
          (doseq [f [inc (fn [_] (throw failure)) inc]]
            (swap! jobs conj (future (try (d/update-kv db "counter" 1 f :id :long)
                                         (catch Throwable t t))))
            (is (await! #(= (count @jobs) (queued g)))))
          (deliver release true)
          (is (= :transacted (deref first-job 10000 ::timeout)))
          (let [[a b c] (mapv #(deref % 10000 ::timeout) @jobs)]
            (is (= :transacted a c))
            (is (identical? failure b)))
          (is (= 3 (d/get-value db "counter" 1 :id :long)))
          (finally (deliver release true)
                   (doseq [job (cons first-job @jobs)] (deref job 10000 nil))))))))

(deftest custom-commit-hooks-and-explicit-transactions-retain-their-path
  (with-kv
    (fn [db _]
      (let [before (atom 0) after (atom [])]
        (binding [cpp/*before-write-commit-fn* (fn [_] (swap! before inc))
                  kvtx/*after-txlog-append-fn* #(swap! after conj (:txlog-lsn %))]
          (is (nil? (kv/strict-write-group db :kv)))
          (is (= :transacted (d/update-kv db "counter" 1 inc :id :long))))
        (is (= 1 @before (count @after))))
      (d/with-transaction-kv [tx db]
        (is (nil? (kv/strict-write-group tx :kv)))
        (d/update-kv tx "counter" 1 inc :id :long)
        (d/abort-transact-kv tx))
      (is (= 1 (d/get-value db "counter" 1 :id :long))))))

(deftest flush-completion-blocks-success-and-native-publication
  (doseq [fail? [false true]]
    (testing (str "failure=" fail?)
      (with-kv
        (fn [db _]
          (let [entered (promise) release (promise)
                failure (IOException. "injected sync failure")
                before (:last-durable-lsn (d/txlog-watermarks db))]
            (kvtx/set-storage-fault-hook!
              (fn [{:keys [stage]}]
                (when (= stage :txlog-sync)
                  (deliver entered true)
                  (assert (deref release 10000 false))
                  (when fail? (throw failure)))))
            (let [job (future (try (d/update-kv db "counter" 1 inc :id :long)
                                   (catch Throwable t t)))]
              (try
                (is (deref entered 10000 false))
                (is (= ::waiting (deref job 20 ::waiting)))
                (is (= before @(:last-durable-lsn (:sync-manager (wal/state db)))))
                (is (= 0 (d/get-value db "counter" 1 :id :long)))
                (deliver release true)
                (let [result (deref job 10000 ::timeout)]
                  (if fail?
                    (is (instance? Throwable result))
                    (do (is (= :transacted result))
                        (is (= 1 (d/get-value db "counter" 1 :id :long))))))
                (finally (deliver release true) (deref job 10000 nil))))))))))

(def remote-inc (inter/inter-fn [old] (inc (long old))))
(def datalog-inc
  (inter/inter-fn [db eid]
    [[:db/add eid :counter (inc (long (:counter (datalevin.core/pull db [:counter] eid))))]]))

(deftest remote-concurrent-rmw-preserves-values-and-datalog-identity
  (let [root (u/tmp-dir (str "strict-group-remote-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port})
        uri #(str "dtlv://datalevin:datalevin@localhost:" port "/" %)]
    (try
      (server/start srv)
      (doseq [api [:kv :datalog]]
        (testing (name api)
          (let [options (assoc opts :client-opts {:pool-size 1})
                handles (mapv (fn [_]
                                (if (= api :kv)
                                  (d/open-kv (uri "kv") options)
                                  (d/create-conn (uri "dl")
                                                 {:counter {:db/valueType :db.type/long}
                                                  :key {:db/unique :db.unique/identity}}
                                                 options)))
                              (range 8))]
            (try
              (let [first-handle (first handles)]
                (if (= api :kv)
                  (do (d/open-dbi first-handle "counter")
                      (d/transact-kv first-handle [[:put "counter" 1 0 :id :long]]))
                  (d/transact! first-handle [{:db/id 1 :key "one" :counter 0}
                                             {:db/ident :inc-counter :db/fn datalog-inc}]))
                (let [start (promise)
                      jobs (mapv (fn [handle]
                                   (future @start
                                           (dotimes [_ 20]
                                             (if (= api :kv)
                                               (d/update-kv handle "counter" 1 remote-inc :id :long)
                                               (d/transact! handle [[:inc-counter 1]])))
                                           true)) handles)]
                  (deliver start true)
                  (doseq [job jobs] (is (true? (deref job 30000 ::timeout)))))
                (if (= api :kv)
                  (is (= 160 (d/get-value first-handle "counter" 1 :id :long)))
                  (is (= {:counter 160 :key "one"}
                         (d/pull @first-handle [:counter :key] [:key "one"])))))
              (finally (doseq [handle handles]
                         (if (= api :kv) (d/close-kv handle) (d/close handle))))))))
      (finally (server/stop srv) (u/delete-files root)))))

(deftest grouped-datalog-responses-match-their-durable-replay-records
  (let [root (u/tmp-dir (str "strict-group-replay-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port})
        handles (atom [])
        jobs (atom [])
        entered (promise) release (promise)]
    (try
      (server/start srv)
      (reset! handles
              (mapv (fn [_]
                      (d/create-conn
                        (str "dtlv://datalevin:datalevin@localhost:" port "/dl")
                        {:counter {:db/valueType :db.type/long}}
                        (assoc opts :client-opts {:pool-size 1})))
                    (range 4)))
      (let [store ^Store (#'server/get-store srv "dl" false)
            lmdb (.-lmdb store)
            g (kv/strict-write-group lmdb :server-datalog)
            before (:last-committed-lsn (d/txlog-watermarks lmdb))
            clients (mapv #(.-client ^DatalogStore (:store @%)) @handles)
            messages (mapv (fn [idx]
                             (let [kind (if (even? idx) :tx-data :tx-data+db-info)
                                   txs [[:db/add (inc idx) :counter idx]]]
                               {:type kind :mode :request :writing? false
                                :args ["dl" txs false]
                                :client-op-id (str (random-uuid))
                                :client-op-hash (cop/request-hash
                                                 (cop/tx-request-payload kind "dl" txs false))
                                :client-op-response-kind kind}))
                           (range 4))
            first? (atom true)]
        (kvtx/set-storage-fault-hook!
          (fn [{:keys [stage]}]
            (when (and (= stage :txlog-sync)
                       (compare-and-set! first? true false))
              (deliver entered true)
              (assert (deref release 10000 false)))))
        (swap! jobs conj (future (client/request (clients 0) (messages 0))))
        (is (deref entered 10000 false))
        (doseq [idx (range 1 4)]
          (swap! jobs conj (future (client/request (clients idx) (messages idx))))
          (is (await! #(= idx (queued g)))))
        (deliver release true)
        (let [responses (mapv #(deref % 10000 ::timeout) @jobs)]
          (is (= (+ before 2) (:last-committed-lsn (d/txlog-watermarks lmdb))))
          (doseq [idx (range 4)]
            (let [response (responses idx)
                  message (messages idx)
                  record (d/get-value lmdb c/ha-client-ops
                                      (cop/kv-info-key (:client-op-id message))
                                      :string :data)
                  replay (client/request (clients idx) message)
                  metadata #(select-keys % [:tempids :db-info :new-attributes])]
              (is (= :command-complete (:type response) (:type replay)))
              (is (= (metadata (:result response))
                     (metadata (cop/record-response record))
                     (metadata (:result replay))))))))
      (finally
        (deliver release true)
        (doseq [job @jobs] (deref job 10000 nil))
        (kvtx/clear-storage-fault-hook!)
        (doseq [conn @handles] (d/close conn))
        (server/stop srv)
        (u/delete-files root)))))
