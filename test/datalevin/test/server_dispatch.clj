(ns datalevin.test.server-dispatch
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.db :as db]
   [datalevin.interface :as i]
   [datalevin.server.handlers :as handlers]
   [datalevin.server.dispatch :as dispatch]))

(defn- fake-store
  [dir max-tx last-modified]
  (reify i/IStore
    (opts [_] {:cache-limit 16})
    (dir [_] dir)
    (last-modified [_] last-modified)
    (max-tx [_] max-tx)))

(deftest runtime-read-access-open-exemptions-test
  (let [runtime-read-access-message? #'dispatch/runtime-read-access-message?]
    (testing "open handlers take the runtime-store write lock themselves"
      (is (false? (runtime-read-access-message? {:type :open})))
      (is (false? (runtime-read-access-message? {:type :open-kv}))))
    (testing "ordinary read handlers stay protected by the read guard"
      (is (true? (runtime-read-access-message? {:type :ha-watermark}))))
    (testing "write handlers are not wrapped in the read guard"
      (is (false? (runtime-read-access-message?
                    {:type :transact :writing? true}))))))

(deftest ha-rejoin-unconfirmed-local-tail-test
  (let [tail (#'handlers/ha-rejoin-unconfirmed-local-tail
              {:ha-role :follower
               :ha-node-id 1
               :ha-authority-owner-node-id 2
               :ha-authority-lease {:leader-last-applied-lsn 10}}
              {:last-committed-lsn 14
               :last-durable-lsn 13
               :last-applied-lsn 12}
              12)]
    (is (true? (:ha-rejoin-unconfirmed-local-tail? tail)))
    (is (= 4 (:ha-rejoin-unconfirmed-local-tail-lsn-count tail)))
    (is (= 4 (:ha-rejoin-unconfirmed-local-committed-lsn-count tail)))
    (is (= 3 (:ha-rejoin-unconfirmed-local-durable-lsn-count tail)))
    (is (= 2 (:ha-rejoin-unconfirmed-local-applied-lsn-count tail)))
    (is (= 10 (:ha-rejoin-authority-confirmed-lsn tail)))))

(deftest ha-rejoin-unconfirmed-local-tail-omits-non-rejoin-test
  (let [watermarks {:last-committed-lsn 14
                    :last-durable-lsn 13
                    :last-applied-lsn 12}]
    (is (nil? (#'handlers/ha-rejoin-unconfirmed-local-tail
               {:ha-role :leader
                :ha-node-id 1
                :ha-authority-owner-node-id 1
                :ha-authority-lease {:leader-last-applied-lsn 10}}
               watermarks
               12)))
    (is (nil? (#'handlers/ha-rejoin-unconfirmed-local-tail
               {:ha-role :follower
                :ha-node-id 1
                :ha-authority-owner-node-id 2
                :ha-authority-lease {:leader-last-applied-lsn 14}}
               watermarks
               12)))))

(deftest ha-read-floor-refreshes-ha-store-cache-test
  (let [store     (fake-store (str "/tmp/dtlv-test-cache-"
                                   (java.util.UUID/randomUUID))
                              12
                              2)
        deps      {:db-state (fn [_ db-name]
                               (is (= "db" db-name))
                               {:ha-role :follower})}]
    (try
      (db/refresh-cache store 1)
      (db/cache-put store :sentinel :stale)
      (is (= :stale (db/cache-get store :sentinel)))
      (#'handlers/ensure-ha-read-floor!
       deps ::server "db" false {:ha-read-min-tx 12} store)
      (is (nil? (db/cache-get store :sentinel)))
      (finally
        (db/remove-cache store)))))

(deftest ha-read-floor-does-not-refresh-non-ha-cache-test
  (let [store     (fake-store (str "/tmp/dtlv-test-cache-"
                                   (java.util.UUID/randomUUID))
                              12
                              2)
        deps      {:db-state (fn [_ _] {})}]
    (try
      (db/refresh-cache store 1)
      (db/cache-put store :sentinel :stale)
      (#'handlers/ensure-ha-read-floor!
       deps ::server "db" false {:ha-read-min-tx 12} store)
      (is (= :stale (db/cache-get store :sentinel)))
      (finally
        (db/remove-cache store)))))

(deftest ha-read-floor-rejects-before-cache-refresh-test
  (let [store     (fake-store (str "/tmp/dtlv-test-cache-"
                                   (java.util.UUID/randomUUID))
                              11
                              2)
        deps      {:db-state (fn [_ _]
                               {:ha-role :follower
                                :ha-members []})}]
    (try
      (db/refresh-cache store 1)
      (db/cache-put store :sentinel :stale)
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"HA read floor not satisfied"
           (#'handlers/ensure-ha-read-floor!
            deps ::server "db" false {:ha-read-min-tx 12} store)))
      (is (= :stale (db/cache-get store :sentinel)))
      (finally
        (db/remove-cache store)))))
