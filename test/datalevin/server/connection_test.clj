(ns datalevin.server.connection-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.core :as d]
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
