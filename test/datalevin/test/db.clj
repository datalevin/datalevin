(ns ^:no-doc datalevin.test.db
  (:require
   [clojure.data]
   [clojure.test :as t :refer [is are deftest testing]]
   [datalevin.binding.cpp]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as l]
   [datalevin.query.plan :as qplan]
   [datalevin.storage :as s]
   [datalevin.util :as u :refer [defrecord-updatable]])
  (:import
   [java.lang Thread$State]
   [java.util UUID]
   [java.util.concurrent Callable CountDownLatch ExecutorService Future
    TimeUnit]))

;;
;; verify that defrecord-updatable works with compiler/core macro configuration
;; define dummy class which redefines hash, could produce either
;; compiler or runtime error
;;
(defrecord-updatable HashBeef [x]
  clojure.lang.IHashEq (hasheq [hb] 0xBEEF))

(def ^:private stale-query-cache-coordination (atom nil))

(defn stale-query-cache-pass?
  [value]
  (when-let [{:keys [query-value release]} @stale-query-cache-coordination]
    (deliver query-value value)
    (deref release 5000 ::timeout))
  true)

(deftest test-defrecord-updatable
  (is (= 0xBEEF (-> (map->HashBeef {:x :ignored}) hash))))

(deftest giant-id-refreshes-once-after-acquiring-write-lock
  (binding [c/*db-background-sampling?* false]
    (doseq [wal? [false true]]
      (testing (str "wal=" wal?)
        (let [dir       (u/tmp-dir (str "giant-id-boundary-" (UUID/randomUUID)))
              conn-a    (d/create-conn dir {:item/data {} :item/text {}}
                                       {:wal? wal?})
              conn-b    (d/create-conn dir)
              value     (fn [n] {:body (.repeat "x" 1000) :revision n})
              refresh   @#'s/init-max-gt
              refreshes (atom [])]
          (try
            (d/with-transaction [cn conn-a]
              (d/transact! cn [{:db/id 1 :item/data (value 0)
                               :item/text (.repeat "y" 1000)}]))
            (is (< (i/max-gt (:store @conn-b)) (i/max-gt (:store @conn-a))))
            (with-redefs-fn
              {#'s/init-max-gt
               (fn [lmdb]
                 (swap! refreshes conj [(Thread/holdsLock (l/write-txn lmdb))
                                       (l/writing? lmdb)])
                 (refresh lmdb))}
              (fn []
                (d/with-transaction [cn conn-b]
                  ;; The stale allocator is refreshed before the user body runs.
                  (is (= [[true true]] @refreshes))
                  (is (= (i/max-gt (:store @conn-a))
                         (i/max-gt (:store @cn))))
                  (d/transact! cn [{:db/id 1 :item/data (value 1)}])
                  (d/with-transaction [nested cn]
                    (d/transact! nested [{:db/id 1 :item/data (value 2)}]))
                  (d/update-schema cn {:item/text {:db/valueType :db.type/string}})
                  (is (= [[true true]] @refreshes)))
                (is (= [[true true]] @refreshes))
                (reset! refreshes [])
                (d/transact! conn-b [{:db/id 1 :item/data (value 3)}])
                (is (= 1 (count @refreshes)))
                (is (true? (ffirst @refreshes)))
                (reset! refreshes [])
                (d/with-transaction [cn conn-b]
                  (is (= [[true true]] @refreshes))
                  (d/transact! cn [{:db/id 1 :item/data (value 4)}]))
                (is (= [[true true]] @refreshes))
                ;; Server transactions open the KV transaction separately.
                (reset! refreshes [])
                (let [store (:store @conn-b)
                      lmdb  (d/datalog-kv conn-b)
                      wlmdb (locking (l/write-txn lmdb)
                              (i/open-transact-kv lmdb))]
                  (try
                    (let [wstore (s/transfer store wlmdb)]
                      (is (= [[true true]] @refreshes))
                      (s/transfer wstore wlmdb)
                      (is (= [[true true]] @refreshes)))
                    (finally
                      (i/abort-transact-kv lmdb)
                      (i/close-transact-kv lmdb))))))
            (is (= (value 4) (:item/data (d/entity @conn-b 1))))
            (is (= (.repeat "y" 1000) (:item/text (d/entity @conn-b 1))))
            (finally
              (d/close conn-b)
              (d/close conn-a)
              (u/delete-files dir))))))))

(deftest large-map-updates-through-multiple-connections
  (binding [c/*db-background-sampling?* false]
    (doseq [wal? [false true]
            explicit? [false true]]
      (testing (str "wal=" wal? ", explicit=" explicit?)
        (let [dir     (u/tmp-dir (str "shared-giants-" (UUID/randomUUID)))
              schema  {:item/name {:db/valueType :db.type/string}
                       :item/data {}}
              value   (fn [n] {:body (.repeat "x" 1000) :revision n})
              conn-a  (d/create-conn dir schema {:wal? wal?})
              conn-b  (d/create-conn dir)
              conn-c  (volatile! nil)
              write!  (fn [conn n]
                        (let [tx [{:db/id 1 :item/data (value n)}]]
                          (if explicit?
                            (d/with-transaction [cn conn]
                              (d/transact! cn tx))
                            (d/transact! conn tx))))]
          (try
            (try
              (d/transact! conn-a [{:db/id 1 :item/name "one"}
                                  {:db/id 2 :item/name "two"}])
              (d/with-transaction [cn conn-a]
                (d/transact! cn [{:db/id 1 :item/data (value 0)}
                                {:db/id 2 :item/data (value -1)}]))
              (write! conn-b 1)
              (write! conn-a 2)
              ;; Opening another connection must not revive an old allocator.
              (vreset! conn-c (d/create-conn dir))
              (write! @conn-c 3)
              (testing "aborted allocations cannot cause a later collision"
                (d/with-transaction [cn conn-a]
                  (d/transact! cn [{:db/id 1 :item/data (value 100)}])
                  (d/abort-transact cn))
                (is (= (value 3) (:item/data (d/entity @conn-b 1))))
                (write! conn-b 4))
              (testing "failed transactions preserve the committed values"
                (is (thrown-with-msg?
                      clojure.lang.ExceptionInfo #"rollback giant update"
                      (d/with-transaction [cn conn-a]
                        (d/transact! cn [{:db/id 1 :item/data (value 101)}])
                        (throw (ex-info "rollback giant update" {})))))
                (is (= (value 4) (:item/data (d/entity @conn-b 1))))
                (write! @conn-c 5))
              (doseq [conn [conn-a conn-b @conn-c]]
                (is (= (value 5) (:item/data (d/entity @conn 1))))
                (is (= (value -1) (:item/data (d/entity @conn 2))))
                (is (= 1 (count (d/datoms @conn :eav 1 :item/data)))))
              (finally
                (when @conn-c (d/close @conn-c))
                (d/close conn-b)
                (d/close conn-a)))
            (with-open [^java.io.Closeable conn (d/create-conn dir)]
              (is (= (value 5) (:item/data (d/entity @conn 1))))
              (is (= (value -1) (:item/data (d/entity @conn 2)))))
            (finally
              (u/delete-files dir))))))))

(deftest concurrent-large-map-transactions-use-distinct-ids
  (binding [c/*db-background-sampling?* false]
    (doseq [wal? [false true]]
      (let [dir    (u/tmp-dir (str "concurrent-giants-" (UUID/randomUUID)))
            conn-a (d/create-conn dir {:item/data {}} {:wal? wal?})
            conn-b (d/create-conn dir)
            value  (fn [e n] {:body (.repeat "x" 1000) :entity e :revision n})
            start  (CountDownLatch. 1)
            run!   (fn [conn e]
                     (future
                       (.await start)
                       (dotimes [n 10]
                         (d/with-transaction [cn conn]
                           (d/transact! cn [{:db/id e :item/data (value e n)}])
                           (d/transact! cn [{:db/id e :item/data (value e (inc n))}])))
                       :done))]
        (try
          (d/transact! conn-a [{:db/id 1 :item/data (value 1 -1)}
                              {:db/id 2 :item/data (value 2 -1)}])
          (let [a (run! conn-a 1)
                b (run! conn-b 2)]
            (.countDown start)
            (try
              (is (= :done (deref a 10000 ::timeout)))
              (is (= :done (deref b 10000 ::timeout)))
              (finally
                (future-cancel a)
                (future-cancel b))))
          (is (= (value 1 10) (:item/data (d/entity @conn-a 1))))
          (is (= (value 2 10) (:item/data (d/entity @conn-a 2))))
          (is (= 2 (d/entries (d/datalog-kv conn-a) c/giants)))
          (finally
            (.countDown start)
            (d/close conn-b)
            (d/close conn-a)
            (u/delete-files dir)))))))

(deftest schema-migration-refreshes-large-value-ids
  (binding [c/*db-background-sampling?* false]
    (let [dir    (u/tmp-dir (str "migrate-shared-giants-" (UUID/randomUUID)))
          conn-a (d/create-conn dir {:item/data {}})
          conn-b (d/create-conn dir)
          value  (.repeat "x" 1000)]
      (try
        (d/with-transaction [cn conn-a]
          (d/transact! cn [{:db/id 1 :item/data value}
                          {:db/id 2 :item/data (str value "2")}]))
        (d/update-schema conn-b {:item/data {:db/valueType :db.type/string}})
        (is (= value (:item/data (d/entity @conn-b 1))))
        (is (= (str value "2") (:item/data (d/entity @conn-b 2))))
        (d/transact! conn-b [{:db/id 1 :item/data (str value "3")}])
        (is (= (str value "3") (:item/data (d/entity @conn-b 1))))
        (is (= 2 (d/entries (d/datalog-kv conn-b) c/giants)))
        (finally
          (d/close conn-b)
          (d/close conn-a)
          (u/delete-files dir))))))

(deftest deterministic-sample-cache-key-includes-seed
  (let [cache-key
        (fn [seed]
          (binding [u/*reservoir-sampling-seed* seed]
            (#'db/sample-init-cache-key
             :movie/title 1000 [[[:closed "A"] [:closed "Z"]]] nil true)))]
    (is (= (cache-key 42) (cache-key 42)))
    (is (not= (cache-key 42) (cache-key 43)))
    (is (not= (cache-key nil) (cache-key 42)))))

(deftest deterministic-entity-sample-cache-key-includes-seed-and-budget
  (let [cache-key
        (fn [seed budget]
          (binding [u/*reservoir-sampling-seed* seed
                    c/init-exec-size-threshold budget]
            (#'db/e-sample-cache-key :movie/title 10000)))]
    (is (= (cache-key 42 1000) (cache-key 42 1000)))
    (is (not= (cache-key 42 1000) (cache-key 43 1000)))
    (is (not= (cache-key 42 1000) (cache-key 42 4000)))))

(deftest empty-query-cache-skips-datom-touch-summary
  (let [dir  (u/tmp-dir (str "empty-query-cache-" (UUID/randomUUID)))
        conn (d/create-conn dir {:item/value {}})]
    (try
      (let [[cache generation] (db/cache-token (:store @conn))
            summary-var        (ns-resolve 'datalevin.db 'tx-touch-summary)]
        (.clear ^datalevin.utl.LRUCache cache)
        (with-redefs-fn
          {summary-var
           (fn [_]
             (throw (ex-info "empty cache should not summarize datoms" {})))}
          #(d/transact! conn [{:db/id 1 :item/value 100}]))
        (is (> (.generation ^datalevin.utl.LRUCache cache)
               (long generation))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest stale-query-result-cannot-repopulate-cache-after-commit
  (let [dir          (u/tmp-dir
                      (str "stale-query-cache-" (UUID/randomUUID)))
        conn         (d/create-conn dir {:item/value {}})
        query        '[:find ?value .
                       :where
                       [1 :item/value ?value]
                       [(datalevin.test.db/stale-query-cache-pass? ?value)
                        ?passed]
                       [(= true ?passed)]]
        query-result (promise)
        release      (promise)
        stale-query  (atom nil)]
    (try
      (d/transact! conn [{:db/id 1 :item/value 100}])
      (db/refresh-cache (:store @conn))
      (let [db-before @conn]
        (reset! stale-query-cache-coordination
                {:query-value query-result
                 :release release})
        (reset! stale-query
                (future (d/q query db-before)))
        (is (= 100 (deref query-result 5000 ::timeout)))
        (d/transact! conn [{:db/id 1 :item/value 101}])
        (deliver release true)
        (is (= 100 (deref @stale-query 5000 ::timeout)))
        ;; The old reader may return its own snapshot, but must not publish that
        ;; result into the cache after the committing transaction invalidated it.
        (is (= 101 (d/q query @conn))))
      (finally
        (reset! stale-query-cache-coordination nil)
        (deliver release true)
        (when-let [query-future @stale-query]
          (future-cancel query-future))
        (d/close conn)
        (u/delete-files dir)))))

(deftest query-plan-pool-shuts-down-with-last-lmdb-executors
  (let [^ExecutorService pool (#'qplan/get-pipe-thread-pool)]
    (is (= :started
           (.get ^Future (.submit pool ^Callable (fn [] :started)))))
    (is (not (.isShutdown pool)))
    (l/shutdown-last-lmdb-executors!)
    (is (.isShutdown pool))
    (is (.awaitTermination pool 1 TimeUnit/SECONDS))
    (let [^ExecutorService new-pool (#'qplan/get-pipe-thread-pool)]
      (try
        (is (not (identical? pool new-pool)))
        (is (not (.isShutdown new-pool)))
        (is (= :restarted
               (.get ^Future
                     (.submit new-pool ^Callable (fn [] :restarted)))))
        (finally
          (qplan/shutdown-pipe-thread-pool!))))))

(deftest read-transaction-acquisition-preserves-cause
  (let [dir      "/tmp/datalevin-reader-error"
        cause    (IllegalArgumentException. "native reader failure")
        error-fn (ns-resolve 'datalevin.binding.cpp
                             'read-transaction-acquisition-error)
        error    (error-fn dir cause)]
    (is (= "Failed to acquire LMDB read transaction: native reader failure"
           (ex-message error)))
    (is (= {:type      :lmdb/read-transaction
            :operation :acquire
            :dir       dir
            :cause     "native reader failure"}
           (ex-data error)))
    (is (identical? cause (ex-cause error)))))

(defn- wait-until-blocked
  [^Thread thread]
  (loop [attempt 0]
    (cond
      (= Thread$State/BLOCKED (.getState thread)) true
      (not (.isAlive thread))                     false
      (< attempt 1000) (do
                         (Thread/sleep 1)
                         (recur (unchecked-inc-int attempt)))
      :else false)))

(deftest close-kv-locks-before-native-teardown
  (let [dir           (u/tmp-dir
                       (str "close-kv-lock-test-" (UUID/randomUUID)))
        lmdb          (d/open-kv dir)
        write-lock    (l/write-txn lmdb)
        hooks         (var-get (ns-resolve 'datalevin.binding.cpp
                                           'shutdown-hooks))
        close-started (CountDownLatch. 1)
        close-error   (atom nil)
        closer        (Thread.
                       (fn []
                         (.countDown close-started)
                         (try
                           (d/close-kv lmdb)
                           (catch Throwable e
                             (reset! close-error e)))))]
    (try
      (is (contains? @hooks dir))
      (locking write-lock
        (.start closer)
        (is (.await close-started 1 TimeUnit/SECONDS))
        (is (wait-until-blocked closer))
        ;; Closing used to unregister the hook and release native resources
        ;; before acquiring this lock, allowing a shutdown-hook double-close.
        (is (contains? @hooks dir)))
      (.join closer 5000)
      (is (not (.isAlive closer)))
      (is (nil? @close-error))
      (is (d/closed-kv? lmdb))
      (is (not (contains? @hooks dir)))
      (finally
        (when (.isAlive closer)
          (.interrupt closer)
          (.join closer 5000))
        (when-not (d/closed-kv? lmdb)
          (d/close-kv lmdb))
        (u/delete-files dir)))))

(deftest persisted-dbi-is-lazily-restored-after-reopen
  (let [dir (u/tmp-dir (str "lazy-dbi-reopen-" (UUID/randomUUID)))]
    (try
      (let [kv-store (d/open-kv dir)]
        (d/open-dbi kv-store "values"
                    {:key-size 64 :val-size 512})
        (d/transact-kv kv-store [[:put "values" :key {:value 42}]])
        (d/close-kv kv-store))
      ;; A named DBI's metadata is durable, but its native handle is not. Reads
      ;; should lazily restore the known handle rather than require another
      ;; explicit open call after an environment/server restart.
      (let [reopened (d/open-kv dir)]
        (try
          (is (= [[:key {:value 42}]]
                 (d/get-range reopened "values" [:all])))
          (finally
            (d/close-kv reopened))))
      (finally
        (u/delete-files dir)))))

(deftest stale-ha-replay-does-not-regress-materialized-payload
  (let [dir (u/tmp-dir (str "stale-ha-replay-" (UUID/randomUUID)))]
    (try
      (let [kv-store (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi kv-store "values")
          (is (= :transacted
                 (d/transact-kv kv-store
                                [[:put "values" :key :older]])))
          (let [older-record (last (d/open-tx-log kv-store 1))]
            (is (= :transacted
                   (d/transact-kv kv-store
                                  [[:put "values" :key :newer]])))
            (let [newer-record (last (d/open-tx-log kv-store 1))
                  result (kv/mirror-replayed-txlog-record!
                          kv-store
                          older-record
                          nil
                          {:replay-skipped? true})]
              (is (< (long (:lsn older-record))
                     (long (:lsn newer-record))))
              (is (:skipped? result))
              (is (not (:replayed? result)))
              (is (= :newer (d/get-value kv-store "values" :key)))))
          (finally
            (d/close-kv kv-store))))
      (finally
        (u/delete-files dir)))))

(deftest ha-replay-repairs-wal-ahead-of-materialized-payload
  (let [dir (u/tmp-dir (str "ha-replay-unapplied-tail-"
                            (UUID/randomUUID)))]
    (try
      (let [kv-store (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi kv-store "values")
          (is (= :transacted
                 (d/transact-kv kv-store
                                [[:put "values" :key :expected]])))
          (let [record (last (d/open-tx-log kv-store 1))
                lsn (long (:lsn record))]
            ;; Model a crash after WAL append but before the corresponding
            ;; payload and materialization floor were committed to LMDB.
            (is (= :transacted
                   (kv/transact-kv-without-txlog!
                    kv-store
                    [[:put "values" :key :stale]
                     [:put c/kv-info c/wal-local-payload-lsn
                      (dec lsn) :keyword :data]])))
            (let [result (kv/mirror-replayed-txlog-record!
                          kv-store
                          record
                          nil
                          {:replay-skipped? true})]
              (is (:skipped? result))
              (is (:replayed? result))
              (is (= :expected (d/get-value kv-store "values" :key)))
              (is (= lsn
                     (long (i/get-value kv-store
                                        c/kv-info
                                        c/wal-local-payload-lsn
                                        :keyword
                                        :data))))))
          (finally
            (d/close-kv kv-store))))
      (finally
        (u/delete-files dir)))))

(defn- now [] (System/currentTimeMillis))

(deftest test-uuid
  (let [now-ms (loop []
                 (let [ts (now)]
                   (if (> ^long (mod ts 1000) 900) ;; sleeping over end of a second
                     (recur)
                     ts)))
        now    (int (/ ^long now-ms 1000))]
    (is (= (* 1000 now) (d/squuid-time-millis (d/squuid))))
    (is (not= (d/squuid) (d/squuid)))
    (is (= (subs (str (d/squuid)) 0 8)
           (subs (str (d/squuid)) 0 8)))))
