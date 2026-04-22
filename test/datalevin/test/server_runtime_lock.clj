(ns datalevin.test.server-runtime-lock
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.server :as srv])
  (:import
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue CountDownLatch
    TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean]))

(defn- test-server
  []
  (let [dbs (ConcurrentHashMap.)]
    (.put dbs "db" {})
    (srv/->Server (AtomicBoolean. true)
                  0
                  ""
                  0
                  nil
                  nil
                  (ConcurrentLinkedQueue.)
                  nil
                  nil
                  nil
                  (ConcurrentHashMap.)
                  dbs)))

(deftest runtime-read-access-uses-message-db-name-test
  (let [server         (test-server)
        writer-entered (CountDownLatch. 1)
        release-writer (CountDownLatch. 1)
        reader-entered (CountDownLatch. 1)
        writer         (future
                         (#'srv/with-db-runtime-store-swap
                          server
                          "db"
                          (fn []
                            (.countDown writer-entered)
                            (.await release-writer 5 TimeUnit/SECONDS))))
        _              (is (.await writer-entered 1 TimeUnit/SECONDS))
        reader         (future
                         (#'srv/with-db-runtime-read-access
                          server
                          {:type :open-kv
                           :db-name "DB"}
                          (fn []
                            (.countDown reader-entered)
                            :read)))]
    (try
      (is (false? (.await reader-entered 100 TimeUnit/MILLISECONDS)))
      (.countDown release-writer)
      (is (= :read (deref reader 1000 ::timeout)))
      (is (true? (deref writer 1000 ::timeout)))
      (is (.await reader-entered 1 TimeUnit/SECONDS))
      (finally
        (.countDown release-writer)
        (future-cancel writer)
        (future-cancel reader)))))
