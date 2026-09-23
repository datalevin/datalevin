(ns datalevin.idoc-async-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.built-ins :as bi]
            [datalevin.core :as d]
            [datalevin.idoc :as idoc]
            [datalevin.secondary-index :as si]
            [datalevin.server :as server]
            [datalevin.storage :as s]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u]
            [datalevin.validate :as vld])
  (:import [datalevin.storage Store]))

(use-fixtures :each db-fixture)

(def opts {:wal? true :wal-durability-profile :strict :wal-shared? false
           :wal-segment-prealloc? false :snapshot-bootstrap-force? false
           :idoc-opts {:indexing-mode :async}})

(def schema {:doc {:db/valueType :db.type/idoc :db/domain "docs"}})

(def query '[:find ?e ?doc :in $ ?query
             :where [(idoc-match $ :doc ?query) [[?e _ ?doc]]]])

(defn- jobs [conn] (s/secondary-index-jobs (:store @conn)))
(defn- matches [conn n] (d/q query @conn {:n n}))
(defn- index-count [conn]
  (idoc/doc-count ((s/store-idoc-indices (:store @conn)) "docs")))

(defn- with-conn [f]
  (let [path (u/tmp-dir (str "idoc-async-" (random-uuid)))
        conn (d/create-conn path schema opts)]
    (try (f conn path)
         (finally (d/close conn) (u/delete-files path)))))

(deftest async-idoc-pull-patch-delete-and-query-cache
  (with-conn
    (fn [conn _]
      (locking (.-write-txn ^Store (:store @conn))
        (let [giant {:n 0 :padding (apply str (repeat 2000 "x"))}
              small {:n 0}
              prepared (d/prepare-q @conn query)]
          (d/transact! conn [{:db/id 1 :doc giant} {:db/id 2 :doc small}])
          (is (= giant (:doc (d/pull @conn [:doc] 1))))
          (is (empty? (matches conn 0)))
          (is (empty? (d/execute-prepared prepared [{:n 0}])))
          (is (= [:idoc] (mapv :job/type (jobs conn))))
          (is (= 1 (:completed-count (d/process-secondary-index-jobs! conn))))
          (is (= #{[1 giant] [2 small]} (matches conn 0)))
          (is (= #{[1 giant] [2 small]}
                 (d/execute-prepared prepared [{:n 0}])))
          (d/transact! conn [[:db.fn/patchIdoc 1 :doc [[:set [:n] 1]]]])
          (is (= 1 (:n (:doc (d/pull @conn [:doc] 1)))))
          (is (= #{[2 small]} (matches conn 0))
              "A lagging giant reference whose source was replaced is skipped")
          (is (empty? (matches conn 1)))
          (is (= 1 (:completed-count (d/process-secondary-index-jobs! conn))))
          (is (= #{[1 (assoc giant :n 1)]} (matches conn 1)))
          (is (= 2 (index-count conn)))
          (d/transact! conn [[:db/retractEntity 1]])
          (is (empty? (matches conn 1)))
          (d/process-secondary-index-jobs! conn)
          (is (= 1 (index-count conn)))
          (is (= #{[2 small]} (matches conn 0))))))))

(deftest async-idoc-failed-commit-blocks-later-deltas-and-retries
  (with-conn
    (fn [conn _]
      (locking (.-write-txn ^Store (:store @conn))
        (d/transact! conn [{:db/id 1 :doc {:n 1}}])
        (d/transact! conn [{:db/id 1 :doc {:n 2}}])
        (let [commits (atom 0)
              result (binding [cpp/*before-write-commit-fn*
                               (fn [_]
                                 ;; Claim succeeds; the following index+ack
                                 ;; commit fails before WAL append.
                                 (when (= 2 (swap! commits inc))
                                   (throw (ex-info "idoc index commit failed" {}))))]
                       (d/process-secondary-index-jobs! conn))]
          (is (= 1 (:failed-count result)))
          (is (zero? (:completed-count result)))
          (is (= #{"docs"} (:blocked-idoc-domains result)))
          (is (false? (boolean (#'s/runnable-secondary-index-pending? result))))
          (is (= #{:failed :pending} (set (map :job/status (jobs conn)))))
          (is (zero? (index-count conn)))
          (is (= {:n 2} (:doc (d/pull @conn [:doc] 1)))))
        (is (zero? (:processed-count (d/process-secondary-index-jobs! conn))))
        (is (= 2 (:completed-count
                   (d/process-secondary-index-jobs! conn {:retry-failed? true}))))
        (is (= #{[1 {:n 2}]} (matches conn 2)))
        (is (empty? (matches conn 1)))
        (is (= 1 (index-count conn)))
        (is (zero? (:processed-count (d/process-secondary-index-jobs! conn))))
        (is (= 1 (index-count conn)))))))

(deftest async-idoc-live-lease-blocks-the-next-domain-job
  (with-conn
    (fn [conn _]
      (locking (.-write-txn ^Store (:store @conn))
        (d/transact! conn [{:db/id 1 :doc {:n 1}}])
        (d/transact! conn [{:db/id 1 :doc {:n 2}}])
        (let [head (first (sort-by :job/tx (jobs conn)))
              claimed (si/claimed-job head "other-worker" (+ (System/currentTimeMillis) 60000))]
          (d/transact-kv (d/datalog-kv conn) [(si/job-tx claimed)])
          (is (zero? (:processed-count (d/process-secondary-index-jobs! conn))))
          (is (zero? (index-count conn)))
          (d/transact-kv (d/datalog-kv conn)
                         [(si/job-tx (assoc claimed :job/lease-until-ms 0))])
          (is (= 2 (:completed-count (d/process-secondary-index-jobs! conn))))
          (is (= #{[1 {:n 2}]} (matches conn 2)))
          (is (= 1 (index-count conn))))))))

(deftest async-idoc-reopens-with-pending-giant-deltas
  (with-conn
    (fn [conn path]
      (locking (.-write-txn ^Store (:store @conn))
        (d/transact! conn [{:db/id 1 :doc {:n 0 :padding (apply str (repeat 2000 "z"))}}])
        (dotimes [n 130]
          (d/transact! conn [[:db.fn/patchIdoc 1 :doc [[:set [:n] (inc n)]]]]))
        (is (= 131 (count (jobs conn))))
        (is (zero? (index-count conn)))
        (d/close conn))
      (let [reopened (d/create-conn path)]
        (try
          (is (:caught-up? (d/wait-for-secondary-index reopened
                                                      {:timeout-ms 15000 :poll-ms 10})))
          (is (= 130 (:n (:doc (d/pull @reopened [:doc] 1)))))
          (is (= #{1} (set (map first (matches reopened 130)))))
          (is (empty? (matches reopened 0)))
          (is (empty? (matches reopened 129)))
          (is (= 1 (index-count reopened)))
          (is (= 131 (:completed-count (d/secondary-index-status reopened))))
          (finally (d/close reopened)))))))

(deftest async-idoc-cardinality-many-and-json
  (let [path (u/tmp-dir (str "idoc-async-many-" (random-uuid)))
        conn (d/create-conn path
                            {:doc {:db/valueType :db.type/idoc :db/domain "docs"
                                   :db/cardinality :db.cardinality/many}
                             :json {:db/valueType :db.type/idoc :db/idocFormat :json}}
                            opts)]
    (try
      (locking (.-write-txn ^Store (:store @conn))
        (d/transact! conn [{:db/id 1 :doc [{:n 1} {:n 2}]
                           :json "{\"n\":1}"}])
        (d/transact! conn [[:db.fn/patchIdoc 1 :doc {:n 1} [[:set [:n] 3]]]
                           [:db/add 1 :json "{\"n\":2}"]])
        (d/process-secondary-index-jobs! conn)
        (is (empty? (matches conn 1)))
        (is (= #{[1 {:n 2}]} (matches conn 2)))
        (is (= #{[1 {:n 3}]} (matches conn 3)))
        (is (= 2 (index-count conn)))
        (is (= [[1 :json {"n" 2}]]
               (mapv vec (bi/idoc-match @conn :json {"n" 2} nil)))))
      (finally (d/close conn) (u/delete-files path)))))

(deftest async-idoc-job-snapshot-coordinates-with-close
  (with-conn
    (fn [conn _]
      (let [store ^Store (:store @conn)
            entered (promise)
            reader (locking (.-write-txn store)
                     (d/transact! conn [{:db/id 1 :doc {:n 0}}])
                     (let [reader (future
                                    (deliver entered true)
                                    (s/secondary-index-jobs store))]
                       (is (true? (deref entered 5000 ::timeout)))
                       (is (= ::blocked (deref reader 50 ::blocked)))
                       (d/close conn)
                       reader))]
        (is (= [] (deref reader 5000 ::timeout)))
        (is (zero? (:processed-count (s/process-secondary-index-jobs! store))))))))

(deftest async-idoc-concurrent-processors-preserve-delta-order
  (with-conn
    (fn [conn _]
      (let [processors
            (locking (.-write-txn ^Store (:store @conn))
              (dotimes [n 32]
                (d/transact! conn [{:db/id 1 :doc {:n n}}]))
              ;; Both callers race with the automatic worker when the writer
              ;; monitor is released. Every delta must apply exactly once.
              (mapv (fn [_]
                      (future
                        (dotimes [_ 16]
                          (d/process-secondary-index-jobs! conn {:max-jobs 2}))
                        true))
                    (range 2)))]
        (doseq [processor processors]
          (is (true? (deref processor 10000 ::timeout))))
      (is (:caught-up? (d/wait-for-secondary-index conn {:timeout-ms 5000})))
      (is (= #{[1 {:n 31}]} (matches conn 31)))
      (is (empty? (matches conn 30)))
      (is (= 1 (index-count conn)))
      (is (= 32 (:completed-count (d/secondary-index-status conn))))))))

(deftest remote-async-idoc-writes-and-prepared-queries
  (let [path (u/tmp-dir (str "remote-idoc-async-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root path :port port})]
    (try
      (server/start srv)
      (let [conn (d/create-conn
                   (str "dtlv://datalevin:datalevin@localhost:" port "/docs")
                   schema opts)]
        (try
          (let [store (#'server/get-store srv "docs" false)
                prepared (d/prepare-q @conn query)
                wait! #(s/wait-for-secondary-index store {:timeout-ms 5000})]
            (is (s/async-idoc-domain? store "docs"))
            (is (not (s/synchronous-secondary-indexing? store)))
            (d/transact! conn [{:db/id 1 :doc {:n 0}}])
            (is (:caught-up? (wait!)))
            (is (= #{[1 {:n 0}]} (d/execute-prepared prepared [{:n 0}])))
            (d/transact! conn [[:db.fn/patchIdoc 1 :doc [[:set [:n] 1]]]])
            (is (= {:n 1} (:doc (d/pull @conn [:doc] 1))))
            (is (:caught-up? (wait!)))
            (is (= #{[1 {:n 1}]} (d/execute-prepared prepared [{:n 1}])))
            (is (empty? (d/execute-prepared prepared [{:n 0}])))
            (d/transact! conn [[:db/retractEntity 1]])
            (is (:caught-up? (wait!)))
            (is (empty? (matches conn 1))))
          (finally (d/close conn))))
      (finally (server/stop srv) (u/delete-files path)))))

(deftest async-idoc-options-and-domain-overrides
  (doseq [config [{:idoc-opts {:indexing-mode :unknown}}
                  {:idoc-domains {"docs" {:indexing-mode :unknown}}}]]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Idoc indexing mode"
                          (vld/validate-idoc-options config))))
  (let [path (u/tmp-dir (str "idoc-async-options-" (random-uuid)))
        conn (d/create-conn path
                            (assoc schema :sync {:db/valueType :db.type/idoc :db/domain "sync"})
                            (assoc opts :idoc-domains
                                   {"docs" {:indexed-paths [:n]}
                                    "sync" {:indexing-mode :sync}}))]
    (try
      (locking (.-write-txn ^Store (:store @conn))
        (d/transact! conn [{:db/id 1 :doc {:n 1} :sync {:n 1}}])
        (is (s/synchronous-secondary-indexing? (:store @conn)))
        (is (empty? (matches conn 1)))
        (is (= [[1 :sync {:n 1}]] (mapv vec (bi/idoc-match @conn :sync {:n 1} nil))))
        (is (= #{"docs"} (set (map :job/domain (jobs conn)))))
        (d/process-secondary-index-jobs! conn)
        (is (= #{[1 {:n 1}]} (matches conn 1))))
      (finally (d/close conn) (u/delete-files path)))))
