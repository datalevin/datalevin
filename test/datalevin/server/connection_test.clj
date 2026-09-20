(ns datalevin.server.connection-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.remote :as remote]
   [datalevin.server :as server]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.server Server]
   [java.nio.channels SelectionKey]
   [java.util UUID]
   [java.util.concurrent Semaphore]))

(use-fixtures :once db-fixture)

(defn- with-server [f]
  (let [root (u/tmp-dir (str "connection-owner-" (UUID/randomUUID)))
        port (allocate-port)
        srv (server/create {:root root :port port})]
    (try
      (server/start srv)
      (f srv (str "dtlv://datalevin:datalevin@localhost:" port "/pinned"))
      (finally (server/stop srv) (u/delete-files root)))))

(deftest pooled-kv-transactions-stay-on-one-connection-and-thread-test
  (with-server
    (fn [_ uri]
      (let [kv (d/open-kv uri {:client-opts {:pool-size 3} :wal? true})
            calls (atom [])
            handlers @#'server/message-handler-map
            tracked (into {}
                          (map (fn [[type handler]]
                                 [type (fn [srv key message]
                                         (when (or (:writing? message)
                                                   (= type :open-transact-kv))
                                           (swap! calls conj
                                                  [key (Thread/currentThread)
                                                   (:context @(.attachment ^SelectionKey key))]))
                                         (handler srv key message))]))
                          handlers)]
        (try
          (d/open-dbi kv "data")
          (d/transact-kv kv [[:put "data" :key :before]])
          (with-redefs-fn
            {#'server/message-handler-map tracked}
            #(doseq [abort? [false true]]
               (reset! calls [])
               (d/with-transaction-kv [tx kv]
                 (d/transact-kv tx [[:put "data" :key :inside]])
                 (is (= :inside (d/get-value tx "data" :key)))
                 (d/with-transaction-kv [nested tx]
                   (is (= :inside (d/get-value nested "data" :key))))
                 (is (= :before (d/get-value kv "data" :key)))
                 (when abort? (d/abort-transact-kv tx)))
               (is (<= 5 (count @calls)))
               (is (some? (nth (first @calls) 2))
                   "the connection context is present for transaction commands")
               (is (= 1 (count (set @calls)))
                   "open, reads, writes, abort and close share an owner and context")
               (is (= (if abort? :before :inside) (d/get-value kv "data" :key)))
               (d/transact-kv kv [[:put "data" :key :before]])))
          (finally (d/close-kv kv)))))))

(deftest cancelled-transaction-cannot-commit-on-a-replacement-socket-test
  (with-server
    (fn [^Server srv uri]
      (let [kv (d/open-kv uri {:client-opts {:pool-size 3} :wal? true})]
        (try
          (d/open-dbi kv "data")
          (let [tx (d/begin-kv-transaction kv)]
            (d/transact-kv tx [[:put "data" :key :discarded]])
            (let [key (:runner-skey (get (.-dbs srv) "pinned"))
                  id (:connection-id @(.attachment ^SelectionKey key))
                  ^Thread thread (get (:connection-threads (.-execution srv)) id)]
              (server/cancel-connection! srv id)
              (.join thread 5000)
              (is (not (.isAlive thread)))
              (is (nil? (:runner (get (.-dbs srv) "pinned"))))
              (is (= 1 (.availablePermits ^Semaphore (:lock (get (.-dbs srv) "pinned"))))))
            (is (thrown? clojure.lang.ExceptionInfo (d/commit-kv-transaction tx)))
            (is (nil? (d/get-value kv "data" :key))))
          (finally (d/close-kv kv)))))))

(deftest datalog-transaction-exposes-kv-operations-test
  (with-server
    (fn [_ uri]
      (doseq [wal? [false true]]
        (let [conn (d/create-conn (str uri "-mixed-" wal?)
                                  {:value {:db/valueType :db.type/keyword}}
                                  {:client-opts {:pool-size 3 :time-out 5000}
                                   :kv-opts {:wal? wal?}})
              kv (d/datalog-kv conn)]
          (try
            (is (= (d/dir kv) (d/dir (d/datalog-kv @conn))))
            (d/open-dbi kv "state")
            (d/open-list-dbi kv "items")
            (doseq [outcome [:commit :abort :exception]]
              (testing (str "WAL " wal? ", " outcome)
                (d/transact! conn [{:db/id 1 :value :before}])
                (d/transact-kv kv "state" [[:put :key :before]])
                (d/clear-dbi kv "items")
                (let [run (fn []
                            (d/with-transaction [tx conn]
                              (let [tx-kv (d/datalog-kv tx)]
                                (d/transact! tx [[:db/add 1 :value :inside]])
                                (d/transact-kv tx-kv "state" [[:put :key :inside]])
                                (is (= :inside (d/get-value tx-kv "state" :key)))
                                (is (= :inside (d/get-value (d/datalog-kv @tx)
                                                           "state" :key)))
                                (is (= [[:key :inside]]
                                       (d/get-range tx-kv "state" [:all])))
                                (is (= [:inside] (remote/get-values tx-kv "state" [:key])))
                                (is (= :inside ((d/prepare-get-value tx-kv "state")
                                                :key)))
                                (d/put-list-items tx-kv "items" :key [1 2] :data :long)
                                (is (= [1 2] (d/get-list tx-kv "items" :key :data :long)))
                                (d/with-transaction [nested tx]
                                  (d/with-transaction-kv [nested-kv (d/datalog-kv nested)]
                                    (is (= :inside (d/get-value nested-kv "state" :key)))))
                                (is (= :inside (:value (d/pull @tx [:value] 1))))
                                (is (= :before (d/get-value kv "state" :key)))
                                (is (= :before (:value (d/pull @conn [:value] 1))))
                                (is (empty? (d/get-list kv "items" :key :data :long)))
                                (case outcome
                                  :abort (d/abort-transact tx)
                                  :exception (throw (ex-info "rollback mixed writes" {}))
                                  nil))))]
                  (if (= outcome :exception)
                    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                          #"rollback mixed writes" (run)))
                    (run)))
                (let [expected (if (= outcome :commit) :inside :before)]
                  (is (= expected (d/get-value kv "state" :key)))
                  (is (= expected (:value (d/pull @conn [:value] 1)))))
                (is (= (if (= outcome :commit) [1 2] [])
                       (vec (d/get-list kv "items" :key :data :long))))))
            ;; Copy-in writes and copy-out reads must use the Datalog runner too.
            (d/open-dbi kv "bulk")
            (let [n (inc c/+wire-datom-batch-size+)
                  rows (mapv (fn [i] [i i]) (range n))]
              (d/with-transaction [tx conn]
                (let [tx-kv (d/datalog-kv tx)]
                  (d/transact-kv tx-kv "bulk"
                                 (mapv (fn [[k v]] [:put k v]) rows) :long :long)
                  (is (= rows (d/get-range tx-kv "bulk" [:all] :long :long)))
                  (is (zero? (d/entries kv "bulk")))))
              (is (= n (d/entries kv "bulk"))))
            (d/close conn)
            (is (d/closed-kv? kv))
            (finally (d/close conn))))))))
