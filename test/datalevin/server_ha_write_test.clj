(ns datalevin.server-ha-write-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ha]
   [datalevin.interpret :as interp]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.server :as srv]
   [datalevin.server-ha-test-support :refer :all]
   [datalevin.storage :as st]
   [datalevin.udf :as udf]
   [datalevin.util :as u]
   [datalevin.validate :as vld]
   [taoensso.timbre :as log])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.server Server]
   [java.net ConnectException]
   [java.nio.channels ClosedChannelException]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]
   [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures :once quiet-server-ha-logs-fixture)

(deftest ha-write-admission-allows-valid-leader-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)]
    (try
      (let [acquire (ha/try-acquire-lease
                     (:ha-authority runtime)
                     {:db-identity (:db-identity opts)
                      :leader-node-id (:ha-node-id opts)
                      :leader-endpoint "10.0.0.12:8898"
                      :lease-renew-ms (:ha-lease-renew-ms opts)
                      :lease-timeout-ms (:ha-lease-timeout-ms opts)
                      :leader-last-applied-lsn 1
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})
            state (-> runtime
                      (assoc :ha-role :leader
                             :ha-authority-owner-node-id (:ha-node-id opts)
                             :ha-lease-until-ms (+ now-ms 10000)
                             :ha-leader-term (:term acquire)
                             :ha-authority-term (:term acquire)))
            server (fake-server-with-db-state "orders" state)]
        (is (:ok? acquire))
        (is (nil? (#'srv/ha-write-admission-error
                   server {:type :transact-kv :args ["orders"]}))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-write-admission-rejects-leader-when-udf-readiness-required-and-missing-test
  (let [dir        (u/tmp-dir (str "ha-write-udf-missing-" (UUID/randomUUID)))
        descriptor {:udf/lang :test
                    :udf/kind :tx-fn
                    :udf/id   :bank/transfer}
        conn       (d/create-conn dir {:udf/ok {:db/valueType :db.type/boolean}})
        store      (.-store ^DB @conn)
        server     (fake-server-with-db-state "orders" {})
        opts       (valid-ha-opts)
        runtime    (#'srv/start-ha-authority "orders" opts)]
    (try
      (d/transact! conn [{:db/ident :bank/transfer
                          :db/udf   descriptor}])
      (binding [srv/*server-runtime-opts-fn*
                (fn [_ _ _ _]
                  {:ha-require-udf-ready? true})]
        (#'srv/add-store server "orders" store))
      (#'srv/update-db server "orders"
                       #(merge % (acquire-leader-state runtime opts)))
      (let [err   (#'srv/ha-write-admission-error
                   server {:type :tx-data :args ["orders" [] false]})
            state (get-in (.-dbs server) ["orders"])]
        (is (= :ha/write-rejected (:error err)))
        (is (= :udf-not-ready (:reason err)))
        (is (false? (:retryable? err)))
        (is (= :bank/transfer
               (get-in err [:udf-missing 0 :db/ident])))
        (is (false? (:udf-ready? state)))
        (is (= :bank/transfer
               (get-in state [:udf-missing 0 :db/ident]))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)
        (d/close conn)
        (u/delete-files dir)))))

(deftest ha-write-admission-allows-leader-when-udf-ready-test
  (let [dir        (u/tmp-dir (str "ha-write-udf-ready-" (UUID/randomUUID)))
        descriptor {:udf/lang :test
                    :udf/kind :tx-fn
                    :udf/id   :bank/transfer}
        registry   (doto (udf/create-registry)
                     (udf/register! descriptor
                       (fn [_]
                         [[:db/add 1 :udf/ok true]])))
        conn       (d/create-conn dir {:udf/ok {:db/valueType :db.type/boolean}})
        store      (.-store ^DB @conn)
        server     (fake-server-with-db-state "orders" {})
        opts       (valid-ha-opts)
        runtime    (#'srv/start-ha-authority "orders" opts)]
    (try
      (d/transact! conn [{:db/ident :bank/transfer
                          :db/udf   descriptor}])
      (binding [srv/*server-runtime-opts-fn*
                (fn [_ _ _ _]
                  {:udf-registry registry
                   :ha-require-udf-ready? true})]
        (#'srv/add-store server "orders" store))
      (#'srv/update-db server "orders"
                       #(merge % (acquire-leader-state runtime opts)))
      (let [err   (#'srv/ha-write-admission-error
                   server {:type :tx-data :args ["orders" [] false]})
            state (get-in (.-dbs server) ["orders"])]
        (is (nil? err))
        (is (true? (:udf-ready? state)))
        (is (= [] (:udf-missing state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)
        (d/close conn)
        (u/delete-files dir)))))

(deftest ha-control-quorum-loss-demotes-leader-and-blocks-writes-test
  (let [authority (reify ha/ILeaseAuthority
                    (start-authority! [this] this)
                    (stop-authority! [this] this)
                    (read-lease [_ _]
                      (throw (ex-info "quorum lost"
                                      {:error :ha/control-timeout})))
                    (try-acquire-lease [_ _]
                      (throw (ex-info "must-not-acquire" {})))
                    (renew-lease [_ _]
                      {:ok? false
                       :reason :control-timeout})
                    (read-membership-hash [_]
                      "hash-a")
                    (init-membership-hash! [_ _]
                      (throw (ex-info "unsupported" {})))
                    (read-voters [_] [])
                    (replace-voters! [_ _]
                      (throw (ex-info "unsupported" {}))))
        leader-state
        {:ha-authority authority
         :ha-db-identity "db-a"
         :ha-role :leader
         :ha-node-id 2
         :ha-local-endpoint "10.0.0.12:8898"
         :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                      {:node-id 2 :endpoint "10.0.0.12:8898"}
                      {:node-id 3 :endpoint "10.0.0.13:8898"}]
         :ha-membership-hash "hash-a"
         :ha-authority-membership-hash "hash-a"
         :ha-authority-owner-node-id 2
         :ha-authority-term 3
         :ha-lease-until-ms (+ (System/currentTimeMillis) 10000)
         :ha-lease-renew-ms 5000
         :ha-lease-timeout-ms 15000
         :ha-leader-term 3
         :ha-leader-last-applied-lsn 7
         :ha-last-authority-refresh-ms (System/currentTimeMillis)}
        demoting-state (#'srv/ha-renew-step "orders" leader-state)
        demoting-server (fake-server-with-db-state "orders" demoting-state)
        demoting-err (#'srv/ha-write-admission-error
                      demoting-server
                      {:type :transact-kv :args ["orders"]})
        follower-state (#'srv/ha-renew-step
                        "orders"
                        (assoc demoting-state
                               :ha-demotion-drain-until-ms
                               (dec (System/currentTimeMillis))))
        follower-server (fake-server-with-db-state "orders" follower-state)
        follower-err (#'srv/ha-write-admission-error
                      follower-server
                      {:type :transact-kv :args ["orders"]})]
    (is (= :demoting (:ha-role demoting-state)))
    (is (= :renew-failed (:ha-demotion-reason demoting-state)))
    (is (= (+ (long (:ha-demoted-at-ms demoting-state))
              (long (:ha-demotion-drain-ms demoting-state)))
           (:ha-demotion-drain-until-ms demoting-state)))
    (is (= :control-timeout
           (get-in demoting-state [:ha-demotion-details :reason])))
    (is (= :ha/write-rejected (:error demoting-err)))
    (is (= :demoting (:reason demoting-err)))
    (is (true? (:retryable? demoting-err)))
    (is (= :follower (:ha-role follower-state)))
    (is (= :ha/write-rejected (:error follower-err)))
    (is (= :not-leader (:reason follower-err)))))

(deftest ha-write-admission-rejects-follower-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        state (assoc runtime :ha-role :follower)
        server (fake-server-with-db-state "orders" state)
        err (#'srv/ha-write-admission-error
             server {:type :transact-kv :args ["orders"]})]
    (try
      (is (= :ha/write-rejected (:error err)))
      (is (= :not-leader (:reason err)))
      (is (true? (:retryable? err)))
      (is (= "orders" (:db-name err)))
      (is (= ["10.0.0.11:8898" "10.0.0.12:8898" "10.0.0.13:8898"]
             (:ha-retry-endpoints err)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-write-admission-rejects-lease-expired-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)
        state (-> runtime
                  (assoc :ha-role :leader
                         :ha-authority-owner-node-id (:ha-node-id opts)
                         :ha-lease-until-ms (dec now-ms)
                         :ha-leader-term 3
                         :ha-authority-term 3))
        server (fake-server-with-db-state "orders" state)
        err (#'srv/ha-write-admission-error
             server {:type :transact-kv :args ["orders"]})]
    (try
      (is (= :ha/write-rejected (:error err)))
      (is (= :lease-expired (:reason err)))
      (is (true? (:retryable? err)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-write-admission-rejects-term-mismatch-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)
        state (-> runtime
                  (assoc :ha-role :leader
                         :ha-authority-owner-node-id (:ha-node-id opts)
                         :ha-lease-until-ms (+ now-ms 10000)
                         :ha-leader-term 7
                         :ha-authority-term 6))
        server (fake-server-with-db-state "orders" state)
        err (#'srv/ha-write-admission-error
             server {:type :transact-kv :args ["orders"]})]
    (try
      (is (= :ha/write-rejected (:error err)))
      (is (= :term-mismatch (:reason err)))
      (is (true? (:retryable? err)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-write-admission-rejects-membership-hash-mismatch-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)
        state (-> runtime
                  (assoc :ha-role :leader
                         :ha-authority-owner-node-id (:ha-node-id opts)
                         :ha-lease-until-ms (+ now-ms 10000)
                         :ha-leader-term 3
                         :ha-authority-term 3
                         :ha-membership-mismatch? true
                         :ha-authority-membership-hash "authority-hash"))
        server (fake-server-with-db-state "orders" state)
        err (#'srv/ha-write-admission-error
             server {:type :transact-kv :args ["orders"]})]
    (try
      (is (= :ha/write-rejected (:error err)))
      (is (= :membership-hash-mismatch (:reason err)))
      (is (false? (:retryable? err)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-write-admission-owner-mismatch-prioritizes-owner-endpoint-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)
        state (-> runtime
                  (assoc :ha-role :leader
                         :ha-authority-owner-node-id 1
                         :ha-authority-lease {:leader-endpoint "10.0.0.11:8898"}
                         :ha-lease-until-ms (+ now-ms 10000)
                         :ha-leader-term 7
                         :ha-authority-term 7))
        server (fake-server-with-db-state "orders" state)
        err (#'srv/ha-write-admission-error
             server {:type :transact-kv :args ["orders"]})]
    (try
      (is (= :ha/write-rejected (:error err)))
      (is (= :owner-mismatch (:reason err)))
      (is (true? (:retryable? err)))
      (is (= "10.0.0.11:8898" (:ha-authoritative-leader-endpoint err)))
      (is (= ["10.0.0.11:8898" "10.0.0.12:8898" "10.0.0.13:8898"]
             (:ha-retry-endpoints err)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))
