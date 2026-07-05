(ns datalevin.test.server
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.interpret :as i]
   [datalevin.server :as srv]
   [datalevin.test.core :as tc]
   [datalevin.util :as u])
  (:import
   [clojure.lang ExceptionInfo]
   [datalevin.server Server]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap ThreadPoolExecutor]
   [java.util.concurrent.locks ReentrantReadWriteLock]))

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
