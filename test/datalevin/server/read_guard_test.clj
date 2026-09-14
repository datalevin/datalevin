(ns datalevin.server.read-guard-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.interface :as i]
   [datalevin.server :as server]
   [datalevin.server.handlers :as handlers]
   [datalevin.storage :as storage]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.server Server]
   [datalevin.storage Store]))

(use-fixtures :once db-fixture)

(defn- with-store [f]
  (let [dir (u/tmp-dir (str "read-floor-" (random-uuid)))
        conn (d/create-conn dir)]
    (try
      (d/transact! conn [{:probe/value 1}])
      (f (:store @conn))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest satisfied-read-floor-does-not-wait-for-the-writer-test
  (with-store
    (fn [^Store store]
      (let [current (long (i/max-tx store))
            read (locking (.-write-txn store)
                   (let [read (future (storage/sync-max-tx-floor! store current))]
                     (is (= current (deref read 1000 ::timeout)))
                     read))]
        (is (= current (deref read 5000 ::timeout)))))))

(deftest advancing-read-floor-rechecks-after-waiting-for-the-writer-test
  (with-store
    (fn [^Store store]
      (let [current (long (i/max-tx store))
            entered (promise)
            read (locking (.-write-txn store)
                   (let [read (future
                                (deliver entered true)
                                (storage/sync-max-tx-floor! store (inc current)))]
                     (is (true? (deref entered 5000 false)))
                     (is (= ::waiting (deref read 100 ::waiting)))
                     (.advance-max-tx store)
                     (.advance-max-tx store)
                     read))]
        (is (= (+ current 2) (deref read 5000 ::timeout)))
        (is (= (+ current 2) (i/max-tx store)))
        (is (= (+ current 4) (storage/sync-max-tx-floor! store (+ current 4))))))))

(deftest read-floor-uses-durable-progress-when-memory-is-ahead-test
  (with-store
    (fn [^Store store]
      (let [committed (long (i/max-tx store))
            deps {:db-state (fn [_ _] {})}]
        ;; Model a cursor ahead of the durable metadata. The reader must still
        ;; reject a floor that the committed data does not satisfy.
        (.advance-max-tx store)
        (is (nil? (#'handlers/ensure-ha-read-floor!
                    deps nil "db" false {:ha-read-min-tx committed} store)))
        (let [error (try
                      (#'handlers/ensure-ha-read-floor!
                        deps nil "db" false {:ha-read-min-tx (inc committed)} store)
                      nil
                      (catch clojure.lang.ExceptionInfo e (ex-data e)))]
          (is (= :read-floor-not-satisfied (:reason error)))
          (is (= committed (:ha-local-max-tx error))))))))

(deftest existing-runtime-lock-does-not-wait-for-the-db-registry-test
  (let [root (u/tmp-dir (str "runtime-read-lock-" (random-uuid)))
        ^Server srv (server/create {:root root :port 0})]
    (try
      (let [lock (#'server/get-runtime-access-lock srv "db")
            lookup (locking (.-dbs srv)
                     (let [lookup (future (#'server/get-runtime-access-lock srv "db"))]
                       (is (identical? lock (deref lookup 1000 ::timeout)))
                       lookup))]
        (is (identical? lock (deref lookup 5000 ::timeout))))
      (let [start (promise)
            lookups (mapv (fn [_]
                            (future @start (#'server/get-runtime-access-lock srv "new-db")))
                          (range 8))]
        (deliver start true)
        (let [locks (mapv #(deref % 5000 ::timeout) lookups)]
          (is (not-any? #{::timeout} locks))
          (is (every? #(identical? (first locks) %) locks))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))
