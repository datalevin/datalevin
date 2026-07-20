(ns datalevin.test.server
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.interpret :as i]
   [datalevin.remote :as r]
   [datalevin.server :as srv]
   [datalevin.test.core :as tc]
   [datalevin.util :as u])
  (:import
   [clojure.lang ExceptionInfo]
   [datalevin.db DB]
   [datalevin.remote DatalogStore]
   [datalevin.server Server]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap ThreadPoolExecutor]
   [java.util.concurrent.locks ReentrantReadWriteLock]))

(defn- wait-until
  [pred ^long timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (pred) true
        (< (System/currentTimeMillis) deadline)
        (do
          (Thread/sleep 10)
          (recur))
        :else false))))

(deftest server-worker-executor-is-bounded-test
  (let [port   (tc/allocate-port)
        dir    (u/tmp-dir (str "server-worker-executor-test-"
                               (UUID/randomUUID)))
        ^Server server (binding [c/*db-background-sampling?* false]
                         (srv/create {:port              port
                                      :root              dir
                                      :worker-threads    3
                                      :worker-queue-size 5}))]
    (try
      (let [^ThreadPoolExecutor executor (.-work-executor server)]
        (is (instance? ThreadPoolExecutor executor))
        (is (= 3 (.getCorePoolSize executor)))
        (is (= 3 (.getMaximumPoolSize executor)))
        (is (= 5 (.remainingCapacity (.getQueue executor)))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))

(deftest runtime-store-read-access-timeout-test
  (let [port   (tc/allocate-port)
        dir    (u/tmp-dir (str "runtime-store-read-timeout-test-"
                               (UUID/randomUUID)))
        db-name (str "runtime-store-read-timeout-"
                     (UUID/randomUUID))
        ^Server server (binding [c/*db-background-sampling?* false]
                         (srv/create {:port port
                                      :root dir}))
        ^ReentrantReadWriteLock lock (ReentrantReadWriteLock. true)
        write-lock (.writeLock lock)
        called? (atom false)]
    (try
      (.put ^ConcurrentHashMap (.-dbs server)
            db-name
            {:runtime-access-lock lock})
      (.lock write-lock)
      (let [result
            (future
              (srv/with-db-runtime-store-read-access-timeout
               server
               db-name
               25
               ::timeout
               (fn []
                 (reset! called? true)
                 ::ran)))]
        (try
          (is (= ::timeout (deref result 1000 ::hung)))
          (is (false? @called?))
          (finally
            (future-cancel result))))
      (finally
        (when (.isWriteLockedByCurrentThread lock)
          (.unlock write-lock))
        (srv/stop server)
        (u/delete-files dir)))))

(deftest disconnected-remote-transaction-releases-writer-test
  (let [port    (tc/allocate-port)
        dir     (u/tmp-dir (str "remote-transaction-disconnect-test-"
                                (UUID/randomUUID)))
        db-name (str "remote-transaction-disconnect-" (UUID/randomUUID))
        uri     (str "dtlv://" c/default-username ":" c/default-password
                     "@127.0.0.1/" db-name)
        ^Server server (binding [c/*db-background-sampling?* false]
                         (srv/create {:port port :root dir}))]
    (try
      (srv/start server)
      (binding [cl/*default-port* port
                c/*db-background-sampling?* false]
        (let [conn     (d/create-conn
                        uri {:name {:db/valueType :db.type/string}})
              store    (.-store ^DB @conn)
              tx-store (r/open-transact store)
              tx-client (.-client ^DatalogStore tx-store)]
          (try
            (is (some? (get-in (.-dbs server) [db-name :runner])))
            (cl/close-pool (cl/get-pool tx-client))
            (is (wait-until
                 #(nil? (get-in (.-dbs server) [db-name :runner]))
                 3000))
            (let [conn2  (d/create-conn
                          uri {:name {:db/valueType :db.type/string}})
                  result (future
                           (d/transact! conn2 [{:name "after-disconnect"}]))]
              (try
                (is (not= ::timeout (deref result 3000 ::timeout)))
                (finally
                  (future-cancel result)
                  (d/close conn2))))
            (finally
              (d/close conn)))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))

(deftest remote-transaction-close-is-client-bound-test
  (let [port    (tc/allocate-port)
        dir     (u/tmp-dir (str "remote-transaction-owner-test-"
                                (UUID/randomUUID)))
        db-name (str "remote-transaction-owner-" (UUID/randomUUID))
        uri     (str "dtlv://" c/default-username ":" c/default-password
                     "@127.0.0.1/" db-name)
        ^Server server (binding [c/*db-background-sampling?* false]
                         (srv/create {:port port :root dir}))]
    (try
      (srv/start server)
      (binding [cl/*default-port* port
                c/*db-background-sampling?* false]
        (let [conn         (d/create-conn
                            uri {:name {:db/valueType :db.type/string}})
              store        (.-store ^DB @conn)
              tx-store     (r/open-transact store)
              other-client (cl/new-client uri)
              runner       (get-in (.-dbs server) [db-name :runner])]
          (try
            (let [e (try
                      (cl/normal-request other-client :close-transact
                                         [db-name] true)
                      nil
                      (catch ExceptionInfo e
                        e))]
              (is (some? e))
              (is (= :ha/write-indeterminate
                     (get-in (ex-data e) [:err-data :error])))
              (is (= :transaction-owner-mismatch
                     (get-in (ex-data e) [:err-data :reason])))
              (is (identical? runner
                              (get-in (.-dbs server) [db-name :runner]))))
            (finally
              (cl/close-client other-client)
              (r/abort-transact tx-store)
              (d/close conn)))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))

(deftest remote-transaction-close-without-open-is-indeterminate-test
  (let [port    (tc/allocate-port)
        dir     (u/tmp-dir (str "remote-transaction-missing-test-"
                                (UUID/randomUUID)))
        db-name (str "remote-transaction-missing-" (UUID/randomUUID))
        uri     (str "dtlv://" c/default-username ":" c/default-password
                     "@127.0.0.1/" db-name)
        ^Server server (binding [c/*db-background-sampling?* false]
                         (srv/create {:port port :root dir}))]
    (try
      (srv/start server)
      (binding [cl/*default-port* port
                c/*db-background-sampling?* false]
        (let [conn  (d/create-conn
                     uri {:name {:db/valueType :db.type/string}})
              store (.-store ^DB @conn)]
          (try
            (let [e (try
                      (cl/normal-request (.-client ^DatalogStore store)
                                         :close-transact
                                         [db-name]
                                         true)
                      nil
                      (catch ExceptionInfo e
                        e))]
              (is (some? e))
              (is (= :ha/write-indeterminate
                     (get-in (ex-data e) [:err-data :error])))
              (is (= :missing-transaction
                     (get-in (ex-data e) [:err-data :reason]))))
            (finally
              (d/close conn)))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))

(deftest remote-abort-allows-idempotent-wrapper-close-test
  (let [port    (tc/allocate-port)
        dir     (u/tmp-dir (str "remote-transaction-abort-test-"
                                (UUID/randomUUID)))
        dl-name (str "remote-datalog-abort-" (UUID/randomUUID))
        kv-name (str "remote-kv-abort-" (UUID/randomUUID))
        uri     (fn [db-name]
                  (str "dtlv://" c/default-username ":" c/default-password
                       "@127.0.0.1/" db-name))
        ^Server server (binding [c/*db-background-sampling?* false]
                         (srv/create {:port port :root dir}))]
    (try
      (srv/start server)
      (binding [cl/*default-port* port
                c/*db-background-sampling?* false]
        (let [conn (d/create-conn
                    (uri dl-name)
                    {:value {:db/valueType :db.type/string}})]
          (try
            (d/transact! conn [{:db/id 1 :value "before"}])
            (d/with-transaction [tx-conn conn]
              (d/transact! tx-conn [{:db/id 1 :value "discarded"}])
              (d/abort-transact tx-conn))
            (is (= "before"
                   (d/q '[:find ?value .
                          :in $ ?entity
                          :where [?entity :value ?value]]
                        @conn 1)))
            (finally
              (d/close conn))))
        (let [kv (d/open-kv (uri kv-name))]
          (try
            (d/open-dbi kv "data")
            (d/transact-kv kv [[:put "data" :key :before]])
            (d/with-transaction-kv [tx-kv kv]
              (d/transact-kv tx-kv [[:put "data" :key :discarded]])
              (d/abort-transact-kv tx-kv))
            (is (= :before (d/get-value kv "data" :key)))
            (finally
              (d/close-kv kv)))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))

(deftest remote-query-uses-server-safe-resolver-test
  (let [port   (tc/allocate-port)
        dir    (u/tmp-dir (str "server-safe-query-test-" (UUID/randomUUID)))
        db-name (str "server-safe-query-" (UUID/randomUUID))
        uri    (str "dtlv://" c/default-username ":" c/default-password
                    "@127.0.0.1/" db-name)
        server (binding [c/*db-background-sampling?* false]
                 (srv/create {:port port
                              :root dir}))]
    (try
      (srv/start server)
      (binding [cl/*default-port* port
                c/*db-background-sampling?* false]
        (let [conn (d/create-conn uri {:name {:db/valueType :db.type/string}})]
          (try
            (d/transact! conn [{:name "Ada"}])
            (is (= "Ada!"
                   (d/q '[:find ?out .
                          :in $
                          :where
                          [?e :name ?name]
                          [(str ?name "!") ?out]]
                        @conn)))
            (is (= "Ada!"
                   (d/q '[:find ?out .
                          :in $ ?f
                          :where
                          [?e :name ?name]
                          [(?f ?name) ?out]]
                        @conn
                        (i/inter-fn [s]
                          (clojure.core/str s "!")))))
            (is (thrown-with-msg?
                  ExceptionInfo #"Server query cannot call unregistered function"
                  (d/q '[:find ?out .
                         :in $
                         :where
                         [?e :name ?name]
                         [(clojure.core/str ?name "!") ?out]]
                       @conn)))
            (finally
              (d/close conn)))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))
