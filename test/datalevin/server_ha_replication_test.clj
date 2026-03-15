(ns datalevin.server-ha-replication-test
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

(deftest ha-endpoint-uri-uses-ha-client-credentials-test
  (let [uri (#'dha/ha-endpoint-uri
             "orders"
             "10.0.0.11:8898"
             {:ha-client-credentials
              {:username "ha-replica"
               :password "p@ss:word"}})]
    (is (= "dtlv://ha-replica:p%40ss:word@10.0.0.11:8898/orders"
           uri))
    (is (= {:username "ha-replica"
            :password "p@ss:word"}
           (cl/parse-user-info (java.net.URI. uri))))
    (is (= (str "dtlv://"
                c/default-username
                ":"
                c/default-password
                "@10.0.0.11:8898/orders")
           (#'dha/ha-endpoint-uri "orders"
                                  "10.0.0.11:8898"
                                  {})))))

(deftest ha-renew-step-follower-syncs-txlog-and-updates-replica-floor-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-lsn (atom 0)
        applied (atom [])
        reported (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            leader-endpoint "10.0.0.11:8898"
            acquire (ha/try-acquire-lease
                     authority
                     {:db-identity (:ha-db-identity runtime)
                      :leader-node-id 1
                      :leader-endpoint leader-endpoint
                      :lease-renew-ms (:ha-lease-renew-ms runtime)
                      :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                      :leader-last-applied-lsn 2
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ _ from-lsn _]
                            (is (= 1 from-lsn))
                            [{:lsn 1 :ops [[:put "a" "k1" "v1"]]}
                             {:lsn 2 :ops [[:put "a" "k2" "v2"]]}])
                          #'dha/apply-ha-follower-txlog-record!
                          (fn [state record]
                            (swap! applied conj (:lsn record))
                            (reset! local-lsn (long (:lsn record)))
                            state)
                          #'dha/read-ha-local-last-applied-lsn
                          (fn [_] @local-lsn)
                          #'dha/report-ha-replica-floor!
                          (fn [db-name m endpoint applied-lsn]
                            (reset! reported
                                    {:db-name db-name
                                     :ha-node-id (:ha-node-id m)
                                     :leader-endpoint endpoint
                                     :applied-lsn applied-lsn})
                            {:ok? true})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (:ok? acquire))
        (is (= [1 2] @applied))
        (is (= :follower (:ha-role next-state)))
        (is (= 2 (:ha-local-last-applied-lsn next-state)))
        (is (= 3 (:ha-follower-next-lsn next-state)))
        (is (= 2 (:ha-follower-last-batch-size next-state)))
        (is (nil? (:ha-follower-sync-backoff-ms next-state)))
        (is (nil? (:ha-follower-next-sync-not-before-ms next-state)))
        (is (nil? (:ha-follower-degraded? next-state)))
        (is (nil? (:ha-follower-last-error next-state)))
        (is (= {:db-name "orders"
                :ha-node-id 2
                :leader-endpoint leader-endpoint
                :applied-lsn 2}
               @reported)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-replica-floor-transport-failure-classification-test
  (is (true? (#'dha/ha-replica-floor-transport-failure?
              (ex-info "Error sending message and receiving response: #error {:cause \"Socket channel is closed.\"}"
                       {:msg {:type :authentication}}))))
  (is (true? (#'dha/ha-replica-floor-transport-failure?
              (ex-info "Error sending message and receiving response"
                       {:msg {:type :authentication}}
                       (ex-info "Socket channel is closed." {})))))
  (is (true? (#'dha/ha-replica-floor-transport-failure?
              (ex-info "Unable to connect to server"
                       {:host "127.0.0.1" :port 8898}
                       (ConnectException. "Connection refused")))))
  (is (false? (#'dha/ha-replica-floor-transport-failure?
               (ex-info "replica floor update failed"
                        {:db-name "orders"}
                        (RuntimeException. "boom"))))))

(deftest ha-replica-floor-reset-required-classification-test
  (is (true? (#'dha/ha-replica-floor-reset-required?
              (ex-info
               "Request to Datalevin server failed: \"Replica floor LSN cannot move backward\""
               {:type :txlog-update-replica-floor!
                :err-data {:type :txlog/invalid-floor-provider-state
                           :replica-id 2
                           :old-lsn 50
                           :new-lsn 17}}))))
  (is (true? (#'dha/ha-replica-floor-reset-required?
              (ex-info "Replica floor LSN cannot move backward"
                       {:type :txlog/invalid-floor-provider-state
                        :replica-id 2
                        :old-lsn 50
                        :new-lsn 17}))))
  (is (false? (#'dha/ha-replica-floor-reset-required?
               (ex-info "replica floor update failed"
                        {:type :txlog-update-replica-floor!
                         :err-data {:type :txlog/io-error
                                    :replica-id 2}})))))

(deftest sync-ha-follower-batch-clears-stale-leader-replica-floor-after-reset-test
  (let [leader-endpoint "10.0.0.11:8898"
        local-lsn (atom 16)
        applied (atom [])
        calls (atom [])
        now-ms (System/currentTimeMillis)
        follower-runtime {:ha-node-id 2
                          :ha-local-endpoint "10.0.0.12:8898"
                          :ha-lease-renew-ms 1000}
        lease {:leader-node-id 1
               :leader-endpoint leader-endpoint
               :leader-last-applied-lsn 17}]
    (let [sync (with-redefs [dha/fetch-ha-leader-txlog-batch
                             (fn [_ _ endpoint from-lsn _]
                               (is (= leader-endpoint endpoint))
                               (is (= 17 from-lsn))
                               [{:lsn 17 :ops [[:put "a" "k17" "v17"]]}])
                             dha/apply-ha-follower-txlog-record!
                             (fn [state record]
                               (swap! applied conj (:lsn record))
                               (reset! local-lsn (long (:lsn record)))
                               state)
                             dha/read-ha-local-last-applied-lsn
                             (fn [_] @local-lsn)
                             dha/report-ha-replica-floor!
                             (fn [_ m endpoint applied-lsn]
                               (swap! calls conj [:report endpoint applied-lsn
                                                  (:ha-node-id m)])
                               (when (= 1 (count @calls))
                                 (throw
                                  (ex-info
                                   "Request to Datalevin server failed: \"Replica floor LSN cannot move backward\""
                                   {:type :txlog-update-replica-floor!
                                    :err-data {:type :txlog/invalid-floor-provider-state
                                               :replica-id 2
                                               :old-lsn 50
                                               :new-lsn 17}})))
                               {:ok? true})
                             dha/clear-ha-replica-floor!
                             (fn [_ m endpoint]
                               (swap! calls conj [:clear endpoint (:ha-node-id m)])
                               {:ok? true})]
                 (#'dha/sync-ha-follower-batch
                  "orders" follower-runtime lease 17 now-ms))
          state (:state sync)]
      (is (= [17] @applied))
      (is (= [[:report leader-endpoint 17 2]
              [:clear leader-endpoint 2]
              [:report leader-endpoint 17 2]]
             @calls))
      (is (= 17 (:applied-lsn sync)))
      (is (= 17 (:ha-local-last-applied-lsn state)))
      (is (= 18 (:ha-follower-next-lsn state)))
      (is (nil? (:ha-follower-last-error state))))))

(deftest sync-ha-follower-batch-empty-batch-preserves-current-local-floor-test
  (let [leader-endpoint "10.0.0.11:8898"
        calls (atom [])
        now-ms (System/currentTimeMillis)
        follower-runtime {:ha-node-id 2
                          :ha-local-endpoint "10.0.0.12:8898"
                          :ha-local-last-applied-lsn 3
                          :ha-lease-renew-ms 1000}
        lease {:leader-node-id 1
               :leader-endpoint leader-endpoint
               :leader-last-applied-lsn 3}]
    (let [sync (with-redefs-fn
                 {#'dha/fetch-ha-follower-records-with-gap-fallback
                  (fn [_ _ _ next-lsn _]
                    (is (= 4 next-lsn))
                    {:records []
                     :source-endpoint leader-endpoint
                     :source-order [leader-endpoint]})
                  #'dha/report-ha-replica-floor!
                  (fn [_ m endpoint applied-lsn]
                    (swap! calls conj [:report endpoint applied-lsn
                                       (:ha-node-id m)])
                    {:ok? true})}
                 (fn []
                   (#'dha/sync-ha-follower-batch
                    "orders" follower-runtime lease 4 now-ms)))
          state (:state sync)]
      (is (= [] (:records sync)))
      (is (= 3 (:applied-lsn sync)))
      (is (= 3 (:ha-local-last-applied-lsn state)))
      (is (= 4 (:ha-follower-next-lsn state)))
      (is (= [[:report leader-endpoint 3 2]]
             @calls))
      (is (nil? (:ha-follower-last-error state))))))

(deftest ha-follower-replica-floor-keeps-leader-retention-safe-while-fresh-test
  (let [leader-dir (u/tmp-dir (str "ha-leader-retention-"
                                   (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-retention-"
                                     (UUID/randomUUID)))
        leader-endpoint "10.0.0.11:8898"
        leader-store (st/open leader-dir nil
                              {:db-name "orders"
                               :db-identity "db-retention"
                               :wal? true
                               :wal-replica-floor-ttl-ms 50})
        follower-store (st/open follower-dir nil
                                {:db-name "orders"
                                 :db-identity "db-retention"
                                 :wal? true})
        leader-kv (.-lmdb leader-store)
        follower-kv (.-lmdb follower-store)
        leader-db (kv/wrap-lmdb leader-kv)
        follower-runtime {:store follower-store
                          :ha-node-id 2
                          :ha-local-endpoint "10.0.0.12:8898"
                          :ha-lease-renew-ms 1000}
        lease {:leader-node-id 1
               :leader-endpoint leader-endpoint
               :leader-last-applied-lsn 2}
        now-ms (System/currentTimeMillis)]
    (try
      (i/open-dbi leader-kv "a")
      (i/open-dbi follower-kv "a")
      (i/transact-kv leader-kv
                     [[:put "a" "k1" "v1"]
                      [:put "a" "k2" "v2"]])
      (with-redefs [dha/fetch-ha-leader-txlog-batch
                    (fn [_ _ endpoint from-lsn upto-lsn]
                      (is (= leader-endpoint endpoint))
                      (kv/open-tx-log leader-db from-lsn upto-lsn))
                    dha/fetch-ha-endpoint-watermark-lsn
                    (fn [_ _ endpoint]
                      (is (= leader-endpoint endpoint))
                      {:reachable? true
                       :last-applied-lsn
                       (:last-applied-lsn (kv/txlog-watermarks leader-db))})
                    dha/report-ha-replica-floor!
                    (fn [_ m endpoint applied-lsn]
                      (is (= leader-endpoint endpoint))
                      (kv/txlog-update-replica-floor! leader-db
                                                      (:ha-node-id m)
                                                      applied-lsn))]
        (let [sync-1 (#'dha/sync-ha-follower-batch
                      "orders" follower-runtime lease 1 now-ms)
              state-1 (:state sync-1)
              synced-lsn (:applied-lsn sync-1)]
          (is (seq (:records sync-1)))
          (is (= synced-lsn
                 (:ha-local-last-applied-lsn state-1)))
          (is (= (inc synced-lsn)
                 (:ha-follower-next-lsn state-1)))
          (i/transact-kv leader-kv
                         [[:put "a" "k3" "v3"]
                          [:put "a" "k4" "v4"]])
          (let [retention-1 (kv/txlog-retention-state leader-db)]
            (is (= synced-lsn
                   (get-in retention-1 [:floors :replica-floor-lsn])))
            (is (= synced-lsn
                   (:required-retained-floor-lsn retention-1)))
            (is (contains? (set (:floor-limiters retention-1))
                           :replica-floor-lsn)))
          (Thread/sleep 150)
          (let [retention-stale (kv/txlog-retention-state leader-db)
                _ (kv/txlog-update-replica-floor! leader-db 2 synced-lsn)
                retention-2 (kv/txlog-retention-state leader-db)]
            (is (zero? (get-in retention-stale
                               [:floor-providers :replica :active-count])))
            (is (= (:required-retained-floor-lsn retention-stale)
                   (get-in retention-stale
                           [:floors :snapshot-floor-lsn])))
            (is (not-any? #{:replica-floor-lsn}
                          (:floor-limiters retention-stale)))
            (is (= synced-lsn
                   (:required-retained-floor-lsn retention-2)))
            (is (contains? (set (:floor-limiters retention-2))
                           :replica-floor-lsn)))))
      (finally
        (when-not (i/closed? follower-store)
          (i/close follower-store))
        (when-not (i/closed? leader-store)
          (i/close leader-store))
        (u/delete-files follower-dir)
        (u/delete-files leader-dir)))))

(deftest ha-apply-follower-txlog-record-replays-real-datalog-rows-test
  (let [schema {:drill/key {:db/valueType :db.type/string
                            :db/unique :db.unique/identity}
                :drill/value {:db/valueType :db.type/string}}
        value-query '[:find ?v .
                      :in $ ?k
                      :where
                      [?e :drill/key ?k]
                      [?e :drill/value ?v]]
        leader-dir (u/tmp-dir (str "ha-leader-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}
        leader-conn (d/get-conn leader-dir schema opts)]
    (try
      (d/transact! leader-conn [{:drill/key "seed" :drill/value "v1"}])
      (let [leader-store (st/open leader-dir nil opts)
            follower-store (st/open follower-dir schema opts)]
        (try
          (let [record (->> (kv/open-tx-log-rows (.-lmdb leader-store) 1 64)
                            ;; :tx-kind only distinguishes vector checkpoints from
                            ;; everything else, so select the actual datalog tx.
                            (filter (fn [record]
                                      (some (fn [[op _dbi k]]
                                              (and (= op :put)
                                                   (= k :max-tx)))
                                            (:rows record))))
                            last)]
            (is record)
            (is (seq (:rows record)))
            (#'dha/apply-ha-follower-txlog-record! {:store follower-store}
                                                   record)
            (is (= "v1"
                   (d/q value-query
                        (db/new-db follower-store)
                        "seed"))))
          (finally
            (i/close follower-store)
            (i/close leader-store))))
      (finally
        (d/close leader-conn)
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest ha-apply-follower-txlog-record-replays-installed-db-fn-test
  (let [padding (apply str (repeat 512 "bank-transfer-padding-"))
        tx-fn (interp/inter-fn [db from-id to-id amount]
                               (let [_ padding]
                                 []))
        tx-fn-query '[:find ?e .
                      :where
                      [?e :db/ident :bank/transfer]
                      [?e :db/fn ?fn]]
        leader-dir (u/tmp-dir (str "ha-leader-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}
        leader-conn (d/get-conn leader-dir nil opts)
        leader-store-v (volatile! nil)
        follower-store-v (volatile! nil)]
    (try
      (d/transact! leader-conn [{:db/ident :bank/transfer
                                 :db/fn tx-fn}])
      (vreset! leader-store-v (st/open leader-dir nil opts))
      (vreset! follower-store-v (st/open follower-dir nil opts))
      (let [record (txlog-record-with-max-tx @leader-store-v 2)]
        (is record)
        (is (seq (:rows record)))
        (let [next-state (#'dha/apply-ha-follower-txlog-record!
                          {:store @follower-store-v}
                          record)]
          (vreset! follower-store-v (:store next-state)))
        (let [tx-fn-eid (d/q tx-fn-query
                             (db/new-db @follower-store-v))]
          (is (integer? tx-fn-eid))
          (is (fn? (i/ea-first-v @follower-store-v
                                 tx-fn-eid
                                 :db/fn)))))
      (finally
        (when-let [follower-store @follower-store-v]
          (when-not (i/closed? follower-store)
            (i/close follower-store)))
        (when-let [leader-store @leader-store-v]
          (when-not (i/closed? leader-store)
            (i/close leader-store)))
        (d/close leader-conn)
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest ha-apply-follower-txlog-record-replays-installed-db-udf-test
  (let [descriptor {:udf/lang :test
                    :udf/kind :tx-fn
                    :udf/id   :bank/transfer}
        tx-udf-query '[:find ?e .
                       :where
                       [?e :db/ident :bank/transfer]
                       [?e :db/udf ?descriptor]]
        leader-dir (u/tmp-dir (str "ha-leader-udf-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-udf-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}
        leader-conn (d/get-conn leader-dir nil opts)
        leader-store-v (volatile! nil)
        follower-store-v (volatile! nil)]
    (try
      (d/transact! leader-conn [{:db/ident :bank/transfer
                                 :db/udf descriptor}])
      (vreset! leader-store-v (st/open leader-dir nil opts))
      (vreset! follower-store-v (st/open follower-dir nil opts))
      (let [record (txlog-record-with-max-tx @leader-store-v 2)]
        (is record)
        (is (seq (:rows record)))
        (let [next-state (#'dha/apply-ha-follower-txlog-record!
                          {:store @follower-store-v}
                          record)]
          (vreset! follower-store-v (:store next-state)))
        (let [tx-udf-eid (d/q tx-udf-query
                              (db/new-db @follower-store-v))]
          (is (integer? tx-udf-eid))
          (is (= descriptor
                 (i/ea-first-v @follower-store-v
                               tx-udf-eid
                               :db/udf)))))
      (finally
        (when-let [follower-store @follower-store-v]
          (when-not (i/closed? follower-store)
            (i/close follower-store)))
        (when-let [leader-store @leader-store-v]
          (when-not (i/closed? leader-store)
            (i/close leader-store)))
        (d/close leader-conn)
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest ha-apply-follower-txlog-record-replays-network-open-tx-log-rows-test
  (let [server-root (u/tmp-dir (str "ha-network-leader-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-network-follower-" (UUID/randomUUID)))
        db-name (str "orders-" (UUID/randomUUID))
        opts {:db-name db-name
              :db-identity "db-1"
              :wal? true}
        server (srv/create {:port 0 :root server-root})
        remote-conn* (atom nil)
        follower-conn* (atom nil)
        follower-store* (atom nil)
        client* (atom nil)]
    (try
      (srv/start server)
      (let [port (.getLocalPort (.socket (.-server-socket ^Server server)))
            uri (str "dtlv://datalevin:datalevin@localhost:" port "/" db-name)]
        (reset! remote-conn* (d/create-conn uri e2e-ha-schema opts))
        (reset! follower-conn* (d/create-conn follower-dir e2e-ha-schema opts))
        (d/transact! @remote-conn* [{:drill/key "seed"
                                     :drill/value "v1"}])
        (d/transact! @follower-conn* [{:drill/key "seed"
                                       :drill/value "v1"}])
        (d/transact! @remote-conn* [{:drill/key "post-failover"
                                     :drill/value "v2"}])
        (reset! client* (cl/new-client
                         (str "dtlv://datalevin:datalevin@localhost:" port)
                         {:pool-size 1
                          :time-out 5000}))
        (d/close @follower-conn*)
        (reset! follower-conn* nil)
        (reset! follower-store* (st/open follower-dir nil opts))
        (let [records (vec (cl/normal-request @client*
                                              :open-tx-log-rows
                                              [db-name 1 64]
                                              false))
              record (latest-datalog-txlog-record records)
              next-state (-> (#'dha/apply-ha-follower-txlog-record!
                              {:store @follower-store*}
                              record)
                             (#'dha/refresh-ha-local-dt-db))]
          (is record)
          (is (seq (:rows record)))
          (with-open [fresh-store (st/open follower-dir nil opts)]
            (is (= "v2"
                   (d/q e2e-ha-value-query
                        (db/new-db fresh-store)
                        "post-failover"))))))
      (finally
        (when-let [client @client*]
          (cl/disconnect client))
        (when-let [store @follower-store*]
          (when-not (i/closed? store)
            (i/close store)))
        (when-let [conn @follower-conn*]
          (d/close conn))
        (when-let [conn @remote-conn*]
          (d/close conn))
        (srv/stop server)
        (u/delete-files server-root)
        (u/delete-files follower-dir)))))

(deftest sync-ha-follower-batch-replays-network-open-tx-log-rows-test
  (let [server-root (u/tmp-dir (str "ha-network-sync-leader-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-network-sync-follower-" (UUID/randomUUID)))
        db-name (str "orders-" (UUID/randomUUID))
        opts {:db-name db-name
              :db-identity "db-1"
              :wal? true}
        server (srv/create {:port 0 :root server-root})
        remote-conn* (atom nil)
        follower-conn* (atom nil)
        leader-endpoint* (atom nil)
        reported-lsn* (atom nil)]
    (try
      (srv/start server)
      (let [port (.getLocalPort (.socket (.-server-socket ^Server server)))
            uri (str "dtlv://datalevin:datalevin@localhost:" port "/" db-name)]
        (reset! leader-endpoint* (str "127.0.0.1:" port))
        (reset! remote-conn* (d/create-conn uri e2e-ha-schema opts))
        (reset! follower-conn* (d/create-conn follower-dir e2e-ha-schema opts))
        (d/transact! @remote-conn* [{:drill/key "seed"
                                     :drill/value "v1"}])
        (d/transact! @follower-conn* [{:drill/key "seed"
                                       :drill/value "v1"}])
        (d/transact! @remote-conn* [{:drill/key "post-failover"
                                     :drill/value "v2"}])
        (let [follower-db @@follower-conn*
              follower-store (:store follower-db)
              follower-state {:store follower-store
                              :dt-db follower-db
                              :ha-node-id 2
                              :ha-role :follower
                              :ha-local-endpoint "127.0.0.1:0"
                              :ha-lease-renew-ms 1000}
              lease {:leader-node-id 1
                     :leader-endpoint @leader-endpoint*
                     :leader-last-applied-lsn 4}
              now-ms (System/currentTimeMillis)]
          (with-redefs [dha/report-ha-replica-floor!
                        (fn [_ _ endpoint applied-lsn]
                          (is (= @leader-endpoint* endpoint))
                          (reset! reported-lsn* applied-lsn)
                          {:ok? true})]
            (let [sync (#'dha/sync-ha-follower-batch
                        db-name
                        follower-state
                        lease
                        4
                        now-ms)
                  state (:state sync)
                  records (:records sync)
                  last-record (peek records)]
              (is (seq records))
              (is (= (:applied-lsn sync)
                     (:ha-local-last-applied-lsn state)))
              (is (= (:applied-lsn sync)
                     @reported-lsn*))
              (is (= (:applied-lsn sync)
                     (:lsn last-record)))
              (is (= (count records)
                     (:ha-follower-last-batch-size state)))
              (is (= (inc (long (:lsn last-record)))
                     (:ha-follower-next-lsn state)))
              (is (= "v2"
                     (d/q e2e-ha-value-query
                          (db/new-db (:store state))
                          "post-failover")))))))
      (finally
        (when-let [conn @follower-conn*]
          (d/close conn))
        (when-let [conn @remote-conn*]
          (d/close conn))
        (srv/stop server)
        (u/delete-files server-root)
        (u/delete-files follower-dir)))))

(deftest ha-apply-follower-txlog-record-advances-max-gt-on-open-store-test
  (let [schema {:giant/key {:db/valueType :db.type/long
                            :db/unique :db.unique/identity}
                :giant/version {:db/valueType :db.type/long}
                :giant/payload {:db/valueType :db.type/string}}
        payload-1 (apply str (repeat 600 "seed-payload-"))
        payload-2 (apply str (repeat 600 "next-payload-"))
        leader-dir (u/tmp-dir (str "ha-leader-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}
        leader-conn (d/get-conn leader-dir schema opts)
        leader-store-v (volatile! nil)
        follower-store-v (volatile! nil)]
    (try
      (d/transact! leader-conn [{:db/id "giant-1"
                                 :giant/key 1
                                 :giant/version 1
                                 :giant/payload payload-1}])
      (vreset! leader-store-v (st/open leader-dir nil opts))
      (vreset! follower-store-v (st/open follower-dir schema opts))
      (let [record
            (->> (kv/open-tx-log-rows (.-lmdb @leader-store-v) 1 64)
                 (filter (fn [record]
                           (some (fn [[op dbi]]
                                   (and (= op :put)
                                        (= dbi c/giants)))
                                 (:rows record))))
                 last)]
        (is record)
        (is (seq (:rows record)))
        (let [before-max-gt (long (i/max-gt @follower-store-v))
              next-state (#'dha/apply-ha-follower-txlog-record!
                          {:store @follower-store-v}
                          record)]
          (vreset! follower-store-v (:store next-state))
          (is (> (long (i/max-gt @follower-store-v))
                 before-max-gt)))
        (is (= payload-1
               (:giant/payload
                (d/entity (db/new-db @follower-store-v)
                          [:giant/key 1]))))
        (i/load-datoms @follower-store-v
                       [(dd/datom 1001 :giant/key 2)
                        (dd/datom 1001 :giant/version 1)
                        (dd/datom 1001 :giant/payload payload-2)])
        (is (= payload-2
               (:giant/payload
                (d/entity (db/new-db @follower-store-v)
                          [:giant/key 2])))))
      (finally
        (when-let [follower-store @follower-store-v]
          (when-not (i/closed? follower-store)
            (i/close follower-store)))
        (when-let [leader-store @leader-store-v]
          (when-not (i/closed? leader-store)
            (i/close leader-store)))
        (d/close leader-conn)
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest ha-renew-step-follower-sync-detects-lsn-gap-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        applied (atom [])
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 3
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ _ _ _]
                            [{:lsn 3 :ops [[:put "a" "k3" "v3"]]}])
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.13:8898"
                              {:reachable? true :last-applied-lsn 3}
                              (throw (ex-info "unexpected-endpoint"
                                              {:endpoint endpoint}))))
                          #'dha/fetch-ha-endpoint-snapshot-copy!
                          (fn [_ _ endpoint _]
                            (throw (ex-info "snapshot unavailable"
                                            {:error :ha/follower-snapshot-unavailable
                                             :endpoint endpoint})))
                          #'dha/apply-ha-follower-txlog-record!
                          (fn [_ record]
                            (swap! applied conj record))
                          #'dha/read-ha-local-last-applied-lsn
                          (fn [_] 0)}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (empty? @applied))
        (is (= :follower (:ha-role next-state)))
        (is (= :sync-failed (:ha-follower-last-error next-state)))
        (is (true? (:ha-follower-degraded? next-state)))
        (is (= :wal-gap (:ha-follower-degraded-reason next-state)))
        (is (= :ha/follower-snapshot-bootstrap-failed
               (get-in next-state
                       [:ha-follower-last-error-details :data :error])))
        (is (= :ha/txlog-gap-unresolved
               (get-in next-state
                       [:ha-follower-last-error-details
                        :data
                        :gap-error
                        :data
                        :error])))
        (is (= #{"10.0.0.11:8898" "10.0.0.13:8898"}
               (into #{}
                     (map :source-endpoint)
                     (get-in next-state
                             [:ha-follower-last-error-details
                              :data
                              :snapshot-errors])))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-follower-sync-empty-batch-with-leader-ahead-detects-gap-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        next-state (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 3
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))]
        (with-redefs-fn
          {#'dha/fetch-ha-leader-txlog-batch
           (fn [_ _ _ _ _]
             [])
           #'dha/fetch-ha-endpoint-watermark-lsn
           (fn [_ _ endpoint]
             (case endpoint
               "10.0.0.11:8898"
               {:reachable? true :last-applied-lsn 3}
               "10.0.0.13:8898"
               {:reachable? true :last-applied-lsn 0}
               (throw (ex-info "unexpected-endpoint"
                               {:endpoint endpoint}))))
           #'dha/fetch-ha-endpoint-snapshot-copy!
           (fn [_ _ endpoint _]
             (throw (ex-info "snapshot unavailable"
                             {:error :ha/follower-snapshot-unavailable
                              :endpoint endpoint})))}
          (fn []
            (reset! next-state (#'srv/ha-renew-step "orders" follower-runtime)))))
      (let [state @next-state
            gap-data (get-in state
                             [:ha-follower-last-error-details
                              :data
                              :gap-error
                              :data])]
        (is (= :follower (:ha-role state)))
        (is (= :sync-failed (:ha-follower-last-error state)))
        (is (true? (:ha-follower-degraded? state)))
        (is (= :wal-gap (:ha-follower-degraded-reason state)))
        (is (= :ha/txlog-gap-unresolved (:error gap-data)))
        (is (= 3
               (get-in gap-data
                       [:gap-errors 0 :data :source-last-applied-lsn]))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-follower-sync-transient-failure-backs-off-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        fetch-calls (atom 0)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 3
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            step-state
            (fn [state]
              (with-redefs-fn
                {#'dha/fetch-ha-leader-txlog-batch
                 (fn [_ _ _ _ _]
                   (swap! fetch-calls inc)
                   (throw (ex-info "transient follower sync failure"
                                   {:error :ha/follower-sync-transient})))
                 #'dha/read-ha-local-last-applied-lsn
                 (fn [_] 0)}
                (fn []
                  (#'srv/ha-renew-step "orders" state))))
            state-1 (step-state follower-runtime)
            state-2 (step-state state-1)]
        (is (= 1 @fetch-calls))
        (is (= :sync-failed (:ha-follower-last-error state-1)))
        (is (integer? (:ha-follower-sync-backoff-ms state-1)))
        (is (integer? (:ha-follower-next-sync-not-before-ms state-1)))
        (is (= :follower (:ha-role state-2)))
        (is (= :sync-failed (:ha-follower-last-error state-2)))
        (is (= 1 @fetch-calls)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-follower-sync-gap-falls-back-to-next-source-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-lsn (atom 0)
        calls (atom [])
        reported (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 2
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint _ _]
                            (swap! calls conj endpoint)
                            (case endpoint
                              "10.0.0.11:8898"
                              [{:lsn 3 :ops [[:put "a" "k3" "v3"]]}]
                              "10.0.0.13:8898"
                              [{:lsn 1 :ops [[:put "a" "k1" "v1"]]}
                               {:lsn 2 :ops [[:put "a" "k2" "v2"]]}]
                              []))
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.13:8898"
                              {:reachable? true :last-applied-lsn 2}
                              (throw (ex-info "unexpected-endpoint"
                                              {:endpoint endpoint}))))
                          #'dha/apply-ha-follower-txlog-record!
                          (fn [state record]
                            (reset! local-lsn (long (:lsn record)))
                            state)
                          #'dha/read-ha-local-last-applied-lsn
                          (fn [_] @local-lsn)
                          #'dha/report-ha-replica-floor!
                          (fn [db-name m endpoint applied-lsn]
                            (reset! reported {:db-name db-name
                                              :ha-node-id (:ha-node-id m)
                                              :leader-endpoint endpoint
                                              :applied-lsn applied-lsn})
                            {:ok? true})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= ["10.0.0.11:8898" "10.0.0.13:8898"] @calls))
        (is (= :follower (:ha-role next-state)))
        (is (= 2 (:ha-local-last-applied-lsn next-state)))
        (is (= 3 (:ha-follower-next-lsn next-state)))
        (is (= "10.0.0.13:8898" (:ha-follower-source-endpoint next-state)))
        (is (= ["10.0.0.11:8898" "10.0.0.13:8898"]
               (:ha-follower-source-order next-state)))
        (is (nil? (:ha-follower-degraded? next-state)))
        (is (= {:db-name "orders"
                :ha-node-id 2
                :leader-endpoint "10.0.0.11:8898"
                :applied-lsn 2}
               @reported)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-follower-sync-source-behind-triggers-snapshot-bootstrap-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 5
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 10
                                        :ha-local-last-applied-lsn 9))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint _ _]
                            (is (contains? #{"10.0.0.11:8898"
                                             "10.0.0.13:8898"}
                                           endpoint))
                            [])
                          #'dha/report-ha-replica-floor!
                          (fn [_ _ _ _]
                            {:ok? true})
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.11:8898"
                              {:reachable? true :last-applied-lsn 5}
                              "10.0.0.13:8898"
                              {:reachable? false
                               :reason :endpoint-watermark-fetch-failed}
                              (throw (ex-info "unexpected-endpoint"
                                              {:endpoint endpoint}))))
                          #'dha/bootstrap-ha-follower-from-snapshot
                          (fn [_ state lease source-order next-lsn bootstrap-now-ms]
                            {:ok? true
                             :state (assoc state
                                           :ha-local-last-applied-lsn 5
                                           :ha-follower-next-lsn 6
                                           :ha-follower-last-bootstrap-ms
                                           bootstrap-now-ms
                                           :ha-follower-bootstrap-source-endpoint
                                           "10.0.0.11:8898"
                                           :ha-follower-bootstrap-snapshot-last-applied-lsn
                                           5
                                           :ha-follower-last-error nil
                                           :ha-follower-degraded? nil)})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :follower (:ha-role next-state)))
        (is (= 5 (:ha-local-last-applied-lsn next-state)))
        (is (= 6 (:ha-follower-next-lsn next-state)))
        (is (= "10.0.0.11:8898"
               (:ha-follower-bootstrap-source-endpoint next-state)))
        (is (= 5
               (:ha-follower-bootstrap-snapshot-last-applied-lsn
                next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-follower-sync-clamps-stale-next-lsn-to-local-state-before-bootstrap-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 26
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 99
                                        :ha-local-last-applied-lsn 26))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint _ _]
                            (is (contains? #{"10.0.0.11:8898"
                                             "10.0.0.13:8898"}
                                           endpoint))
                            [])
                          #'dha/report-ha-replica-floor!
                          (fn [_ _ _ _]
                            {:ok? true})
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.11:8898"
                              {:reachable? true :last-applied-lsn 26}
                              "10.0.0.13:8898"
                              {:reachable? false
                               :reason :endpoint-watermark-fetch-failed}
                              (throw (ex-info "unexpected-endpoint"
                                              {:endpoint endpoint}))))}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :follower (:ha-role next-state)))
        (is (= 26 (:ha-local-last-applied-lsn next-state)))
        (is (= 27 (:ha-follower-next-lsn next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-gap-fallback-source-endpoints-prefers-highest-watermark-followers-test
  (let [m {:ha-local-endpoint "10.0.0.12:8898"
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}
                        {:node-id 3 :endpoint "10.0.0.13:8898"}
                        {:node-id 4 :endpoint "10.0.0.14:8898"}
                        {:node-id 5 :endpoint "10.0.0.15:8898"}]}
        lease {:leader-node-id 1
               :leader-endpoint "10.0.0.11:8898"
               :leader-last-applied-lsn 80}]
    (with-redefs-fn
      {#'dha/fetch-ha-endpoint-watermark-lsn
       (fn [_ _ endpoint]
         (case endpoint
           "10.0.0.11:8898" {:reachable? true :last-applied-lsn 80}
           "10.0.0.13:8898" {:reachable? true :last-applied-lsn 25}
           "10.0.0.14:8898" {:reachable? true :last-applied-lsn 60}
           "10.0.0.15:8898" {:reachable? false
                             :reason :endpoint-watermark-fetch-failed}
           (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))}
      (fn []
        (is (= ["10.0.0.11:8898"
                "10.0.0.14:8898"
                "10.0.0.13:8898"
                "10.0.0.15:8898"]
               (#'dha/ha-gap-fallback-source-endpoints
                "orders" m lease 20)))))))

(deftest ha-gap-fallback-source-endpoints-caps-followers-at-leader-watermark-test
  (let [m {:ha-local-endpoint "10.0.0.12:8898"
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}
                        {:node-id 3 :endpoint "10.0.0.13:8898"}
                        {:node-id 4 :endpoint "10.0.0.14:8898"}]}
        lease {:leader-node-id 1
               :leader-endpoint "10.0.0.11:8898"
               :leader-last-applied-lsn 68}]
    (with-redefs-fn
      {#'dha/fetch-ha-endpoint-watermark-lsn
       (fn [_ _ endpoint]
         (case endpoint
           "10.0.0.11:8898" {:reachable? true :last-applied-lsn 68}
           "10.0.0.13:8898" {:reachable? true :last-applied-lsn 183}
           "10.0.0.14:8898" {:reachable? true :last-applied-lsn 184}
           (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))}
      (fn []
        (is (= ["10.0.0.11:8898"
                "10.0.0.13:8898"
                "10.0.0.14:8898"]
               (#'dha/ha-gap-fallback-source-endpoints
                "orders" m lease 80)))))))

(deftest ha-gap-fallback-source-endpoints-uses-follower-txlog-watermark-test
  (let [m {:ha-local-endpoint "10.0.0.12:8898"
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}
                        {:node-id 3 :endpoint "10.0.0.13:8898"}
                        {:node-id 4 :endpoint "10.0.0.14:8898"}]}
        lease {:leader-node-id 1
               :leader-endpoint "10.0.0.11:8898"
               :leader-last-applied-lsn 68}]
    (with-redefs-fn
      {#'dha/fetch-ha-endpoint-watermark-lsn
       (fn [_ _ endpoint]
         (case endpoint
           "10.0.0.11:8898"
           {:reachable? true
            :last-applied-lsn 68
            :txlog-last-applied-lsn 68}

           "10.0.0.13:8898"
           {:reachable? true
            :last-applied-lsn 183
            :txlog-last-applied-lsn 12}

           "10.0.0.14:8898"
           {:reachable? true
            :last-applied-lsn 17
            :txlog-last-applied-lsn 17}

           (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))}
      (fn []
        (is (= ["10.0.0.11:8898"
                "10.0.0.14:8898"
                "10.0.0.13:8898"]
               (#'dha/ha-gap-fallback-source-endpoints
                "orders" m lease 17)))))))

(deftest ha-source-advertised-last-applied-lsn-caps-follower-at-leader-watermark-test
  (let [m {:ha-local-endpoint "10.0.0.12:8898"
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}
                        {:node-id 3 :endpoint "10.0.0.13:8898"}]}
        lease {:leader-node-id 1
               :leader-endpoint "10.0.0.11:8898"
               :leader-last-applied-lsn 68}]
    (with-redefs-fn
      {#'dha/fetch-ha-endpoint-watermark-lsn
       (fn [_ _ endpoint]
         (case endpoint
           "10.0.0.11:8898" {:reachable? true :last-applied-lsn 68}
           "10.0.0.13:8898" {:reachable? true :last-applied-lsn 183}
           (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))}
      (fn []
        (is (= {:known? true
                :last-applied-lsn 68
                :raw-last-applied-lsn 183
                :leader-safe-lsn 68
                :watermark {:reachable? true :last-applied-lsn 183}}
               (#'dha/ha-source-advertised-last-applied-lsn
                "orders" m lease "10.0.0.13:8898")))))))

(deftest ha-source-advertised-last-applied-lsn-uses-follower-txlog-watermark-test
  (let [m {:ha-local-endpoint "10.0.0.12:8898"
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}
                        {:node-id 3 :endpoint "10.0.0.13:8898"}]}
        lease {:leader-node-id 1
               :leader-endpoint "10.0.0.11:8898"
               :leader-last-applied-lsn 68}]
    (with-redefs-fn
      {#'dha/fetch-ha-endpoint-watermark-lsn
       (fn [_ _ endpoint]
         (case endpoint
           "10.0.0.11:8898"
           {:reachable? true
            :last-applied-lsn 68
            :txlog-last-applied-lsn 68}

           "10.0.0.13:8898"
           {:reachable? true
            :last-applied-lsn 183
            :txlog-last-applied-lsn 12}

           (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))}
      (fn []
        (is (= {:known? true
                :last-applied-lsn 12
                :raw-last-applied-lsn 12
                :leader-safe-lsn 68
                :watermark {:reachable? true
                            :last-applied-lsn 183
                            :txlog-last-applied-lsn 12}}
               (#'dha/ha-source-advertised-last-applied-lsn
                "orders" m lease "10.0.0.13:8898")))))))

(deftest ha-watermark-transport-reuses-cached-client-test
  (#'dha/clear-ha-client-cache!)
  (let [request-clients (atom [])
        new-client-count (atom 0)
        close-count (atom 0)
        client {:id :ha-test-client
                :pool :ha-test-pool}
        m {:ha-local-endpoint "10.0.0.12:8898"
           :ha-lease-renew-ms 1000}]
    (try
      (with-redefs-fn
        {#'dha/open-ha-client
         (fn [_ _ _]
           (swap! new-client-count inc)
           client)
         #'dha/live-ha-client?
         (fn [_]
           true)
         #'dha/ha-client-request
         (fn [client' op _ _]
           (swap! request-clients conj [client' op])
           {:last-applied-lsn 42
            :ha-runtime? false})
         #'dha/close-ha-client!
         (fn [_]
           (swap! close-count inc)
           nil)}
        (fn []
          (is (= {:reachable? true
                  :last-applied-lsn 42
                  :txlog-last-applied-lsn 42
                  :ha-local-last-applied-lsn nil
                  :ha-role nil
                  :ha-runtime? false
                  :ha-control-node-leader? nil
                  :ha-control-node-state nil
                  :source :remote}
                 (#'dha/fetch-ha-endpoint-watermark-lsn
                  "orders" m "10.0.0.11:8898")))
          (is (= {:reachable? true
                  :last-applied-lsn 42
                  :txlog-last-applied-lsn 42
                  :ha-local-last-applied-lsn nil
                  :ha-role nil
                  :ha-runtime? false
                  :ha-control-node-leader? nil
                  :ha-control-node-state nil
                  :source :remote}
                 (#'dha/fetch-ha-endpoint-watermark-lsn
                  "orders" m "10.0.0.11:8898")))
          (#'dha/clear-ha-client-cache!)))
      (is (= 1 @new-client-count))
      (is (= [[client :ha-watermark]
              [client :ha-watermark]]
             @request-clients))
      (is (= 1 @close-count))
      (finally
        (#'dha/clear-ha-client-cache!)))))

(deftest ha-client-cache-open-does-not-serialize-distinct-endpoints-test
  (#'dha/clear-ha-client-cache!)
  (let [client-opts {:time-out 1000}
        bucket-idx  (fn [cache-key]
                      (let [h      (int (hash cache-key))
                            spread (bit-and (bit-xor h
                                                     (unsigned-bit-shift-right
                                                      h 16))
                                             0x7fffffff)]
                        (bit-and 15 spread)))
        uri-pair    (first
                     (for [a (range 11 40)
                           b (range (inc a) 41)
                           :let [uri-a (str "dtlv://"
                                            c/default-username
                                            ":"
                                            c/default-password
                                            "@10.0.0."
                                            a
                                            ":8898/orders")
                                 uri-b (str "dtlv://"
                                            c/default-username
                                            ":"
                                            c/default-password
                                            "@10.0.0."
                                            b
                                            ":8898/orders")
                                 key-a (#'dha/ha-client-cache-key
                                        uri-a client-opts)
                                 key-b (#'dha/ha-client-cache-key
                                        uri-b client-opts)]
                           :when (not= (bucket-idx key-a)
                                       (bucket-idx key-b))]
                       [uri-a uri-b]))
        started-count (atom 0)
        active-opens  (atom 0)
        max-active    (atom 0)
        opened-uris   (atom [])
        both-started  (promise)]
    (try
      (is (some? uri-pair))
      (when uri-pair
        (let [[uri-a uri-b] uri-pair]
          (with-redefs-fn
            {#'dha/open-ha-client
             (fn [uri _ _]
               (let [active-now (swap! active-opens inc)
                     started-now (swap! started-count inc)]
                 (swap! max-active max active-now)
                 (swap! opened-uris conj uri)
                 (when (= 2 started-now)
                   (deliver both-started true))
                 (deref both-started 250 ::timeout)
                 (Thread/sleep 25)
                 (swap! active-opens dec)
                 {:uri uri :pool uri}))
             #'dha/live-ha-client?
             (fn [_]
               true)
             #'dha/close-ha-client!
             (fn [_]
               nil)}
            (fn []
              (let [f1 (future (#'dha/cached-ha-client uri-a "orders"
                                                       client-opts))
                    f2 (future (#'dha/cached-ha-client uri-b "orders"
                                                       client-opts))
                    client-a (deref f1 1000 ::timeout)
                    client-b (deref f2 1000 ::timeout)]
                (is (not= ::timeout client-a))
                (is (not= ::timeout client-b))
                (is (= #{uri-a uri-b} (set @opened-uris)))
                (is (= 2 @max-active))
                (is (not= client-a client-b)))))))
      (finally
        (#'dha/clear-ha-client-cache!)))))

(deftest ha-watermark-transport-falls-back-to-txlog-watermarks-test
  (#'dha/clear-ha-client-cache!)
  (let [request-clients (atom [])
        client {:id :ha-test-client
                :pool :ha-test-pool}]
    (try
      (with-redefs-fn
        {#'dha/open-ha-client
         (fn [_ _ _]
           client)
         #'dha/live-ha-client?
         (fn [_]
           true)
         #'dha/ha-client-request
         (fn [client' op _ _]
           (swap! request-clients conj [client' op])
           (case op
             :ha-watermark
             (throw (ex-info "Unknown message type :ha-watermark"
                             {:error :unknown-message-type}))

             :txlog-watermarks
             {:last-applied-lsn 42}))
         #'dha/close-ha-client!
         (fn [_] nil)}
        (fn []
          (is (= {:reachable? true
                  :last-applied-lsn 42
                  :txlog-last-applied-lsn 42
                  :ha-local-last-applied-lsn nil
                  :ha-role nil
                  :ha-runtime? nil
                  :ha-control-node-leader? nil
                  :ha-control-node-state nil
                  :source :remote}
                 (#'dha/fetch-ha-endpoint-watermark-lsn
                  "orders"
                  {:ha-local-endpoint "10.0.0.12:8898"
                   :ha-lease-renew-ms 1000}
                  "10.0.0.11:8898")))))
      (is (= [[client :ha-watermark]
              [client :txlog-watermarks]]
             @request-clients))
      (finally
        (#'dha/clear-ha-client-cache!)))))

(deftest ha-watermark-transport-prefers-remote-ha-local-lsn-test
  (#'dha/clear-ha-client-cache!)
  (let [client {:id :ha-test-client
                :pool :ha-test-pool}]
    (try
      (with-redefs-fn
        {#'dha/open-ha-client
         (fn [_ _ _]
           client)
         #'dha/live-ha-client?
         (fn [_]
           true)
         #'dha/ha-client-request
         (fn [_ op _ _]
           (is (= :ha-watermark op))
           {:last-applied-lsn 3
            :txlog-last-applied-lsn 37
            :ha-local-last-applied-lsn 3
            :ha-role :follower
            :ha-runtime? true
            :ha-control-node-leader? false
            :ha-control-node-state "STATE_FOLLOWER"})
         #'dha/close-ha-client!
         (fn [_] nil)}
        (fn []
          (is (= {:reachable? true
                  :last-applied-lsn 3
                  :txlog-last-applied-lsn 37
                  :ha-local-last-applied-lsn 3
                  :ha-role :follower
                  :ha-runtime? true
                  :ha-control-node-leader? false
                  :ha-control-node-state "STATE_FOLLOWER"
                  :source :remote-ha-runtime}
                 (#'dha/fetch-ha-endpoint-watermark-lsn
                  "orders"
                  {:ha-local-endpoint "10.0.0.12:8898"
                   :ha-lease-renew-ms 1000}
                  "10.0.0.11:8898")))))
      (finally
        (#'dha/clear-ha-client-cache!)))))

(deftest ha-renew-step-follower-sync-gap-reorders-followers-by-watermark-test
  (let [opts (assoc (valid-ha-opts)
                    :ha-members
                    [{:node-id 1 :endpoint "10.0.0.11:8898"}
                     {:node-id 2 :endpoint "10.0.0.12:8898"}
                     {:node-id 3 :endpoint "10.0.0.13:8898"}
                     {:node-id 4 :endpoint "10.0.0.14:8898"}]
                    :ha-control-plane
                    {:backend :in-memory
                     :group-id (str "ha-test-" (UUID/randomUUID))
                     :local-peer-id "10.0.0.12:7801"
                     :voters [{:peer-id "10.0.0.11:7801"
                               :ha-node-id 1
                               :promotable? true}
                              {:peer-id "10.0.0.12:7801"
                               :ha-node-id 2
                               :promotable? true}
                              {:peer-id "10.0.0.13:7801"
                               :ha-node-id 3
                               :promotable? true}
                              {:peer-id "10.0.0.14:7801"
                               :ha-node-id 4
                               :promotable? true}]})
        runtime (#'srv/start-ha-authority "orders" opts)
        local-lsn (atom 0)
        calls (atom [])
        reported (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 2
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.13:8898"
                              {:reachable? true :last-applied-lsn 1}
                              "10.0.0.14:8898"
                              {:reachable? true :last-applied-lsn 2}
                              (throw (ex-info "unexpected-endpoint"
                                              {:endpoint endpoint}))))
                          #'dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint _ _]
                            (swap! calls conj endpoint)
                            (case endpoint
                              "10.0.0.11:8898"
                              [{:lsn 3 :ops [[:put "a" "k3" "v3"]]}]
                              "10.0.0.14:8898"
                              [{:lsn 1 :ops [[:put "a" "k1" "v1"]]}
                               {:lsn 2 :ops [[:put "a" "k2" "v2"]]}]
                              "10.0.0.13:8898"
                              (throw
                               (ex-info "must-not-try-lower-watermark-source"
                                        {:endpoint endpoint}))
                              []))
                          #'dha/apply-ha-follower-txlog-record!
                          (fn [state record]
                            (reset! local-lsn (long (:lsn record)))
                            state)
                          #'dha/read-ha-local-last-applied-lsn
                          (fn [_] @local-lsn)
                          #'dha/report-ha-replica-floor!
                          (fn [db-name m endpoint applied-lsn]
                            (reset! reported {:db-name db-name
                                              :ha-node-id (:ha-node-id m)
                                              :leader-endpoint endpoint
                                              :applied-lsn applied-lsn})
                            {:ok? true})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= ["10.0.0.11:8898" "10.0.0.14:8898"] @calls))
        (is (= :follower (:ha-role next-state)))
        (is (= "10.0.0.14:8898" (:ha-follower-source-endpoint next-state)))
        (is (= ["10.0.0.11:8898" "10.0.0.14:8898" "10.0.0.13:8898"]
               (:ha-follower-source-order next-state)))
        (is (= {:db-name "orders"
                :ha-node-id 2
                :leader-endpoint "10.0.0.11:8898"
                :applied-lsn 2}
               @reported)))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest copy-response-meta-includes-ha-snapshot-fields-test
  (let [dir (u/tmp-dir (str "ha-copy-meta-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity (str "db-" (UUID/randomUUID))}
        store (st/open dir nil opts)]
    (try
      (let [kv (.-lmdb store)
            _ (i/open-dbi kv "a")
            _ (i/transact-kv kv [[:put "a" "k1" "v1"]])
            _ (i/create-snapshot! kv)
            snapshot-lsn
            (long (or (i/get-value kv c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (i/transact-kv kv [[:put "a" "k2" "v2"]])
            watermark-lsn
            (long (or (:last-applied-lsn (i/txlog-watermarks kv)) 0))
            meta (#'srv/copy-response-meta "orders" store {:compact? false})]
        (is (= "orders" (:db-name meta)))
        (is (= (:db-identity opts) (:db-identity meta)))
        (is (= snapshot-lsn (:snapshot-last-applied-lsn meta)))
        (is (= watermark-lsn (:payload-last-applied-lsn meta)))
        (is (> watermark-lsn snapshot-lsn)))
      (finally
        (i/close store)
        (u/delete-files dir)))))

(deftest copy-response-meta-includes-db-identity-for-raw-lmdb-test
  (let [dir (u/tmp-dir (str "ha-copy-meta-raw-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity (str "db-" (UUID/randomUUID))}
        store (st/open dir nil opts)
        lmdb (.-lmdb store)]
    (try
      (let [_ (i/open-dbi lmdb "a")
            _ (i/transact-kv lmdb [[:put "a" "k1" "v1"]])
            _ (i/create-snapshot! lmdb)
            snapshot-lsn
            (long (or (i/get-value lmdb c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (i/transact-kv lmdb [[:put "a" "k2" "v2"]])
            watermark-lsn
            (long (or (:last-applied-lsn (i/txlog-watermarks lmdb)) 0))
            meta (#'srv/copy-response-meta "orders" lmdb {:compact? false})]
        (is (= "orders" (:db-name meta)))
        (is (= (:db-identity opts) (:db-identity meta)))
        (is (= snapshot-lsn (:snapshot-last-applied-lsn meta)))
        (is (= watermark-lsn (:payload-last-applied-lsn meta)))
        (is (> watermark-lsn snapshot-lsn)))
      (finally
        (i/close store)
        (u/delete-files dir)))))

(deftest copy-response-meta-prefers-lmdb-payload-lsn-over-runtime-watermark-test
  (let [dir (u/tmp-dir (str "ha-copy-meta-payload-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity (str "db-" (UUID/randomUUID))}
        store (st/open dir nil opts)]
    (try
      (let [kv (.-lmdb store)
            _ (i/open-dbi kv "a")
            _ (i/transact-kv kv [[:put "a" "k1" "v1"]])
            _ (i/create-snapshot! kv)
            snapshot-lsn
            (long (or (i/get-value kv c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (i/transact-kv kv [[:put "a" "k2" "v2"]])
            runtime-lsn
            (#'dha/read-ha-local-last-applied-lsn
             {:store store :ha-role :leader})
            _ (is (> runtime-lsn snapshot-lsn))
            meta (with-redefs [dha/read-ha-snapshot-payload-lsn
                               (fn [_] snapshot-lsn)]
                   (#'srv/copy-response-meta
                    "orders" store {:compact? false}))]
        (is (= runtime-lsn
               (#'dha/read-ha-local-last-applied-lsn
                {:store store :ha-role :leader})))
        (is (= snapshot-lsn (:payload-last-applied-lsn meta)))
        (is (not= runtime-lsn (:payload-last-applied-lsn meta))))
      (finally
        (i/close store)
        (u/delete-files dir)))))

(deftest persist-ha-local-applied-lsn-bypasses-local-txlog-test
  (let [dir (u/tmp-dir (str "ha-local-applied-persist-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :wal? true})]
    (try
      (let [kv (.-lmdb store)
            _ (i/open-dbi kv "a")
            _ (i/transact-kv kv [[:put "a" "k1" "v1"]])
            watermark-before
            (long (or (:last-applied-lsn (i/txlog-watermarks kv)) 0))
            _ (#'dha/persist-ha-local-applied-lsn!
               {:store store}
               watermark-before)
            watermark-after
            (long (or (:last-applied-lsn (i/txlog-watermarks kv)) 0))
            persisted-lsn
            (long (or (i/get-value kv c/kv-info
                                   c/ha-local-applied-lsn
                                   :keyword :data)
                      0))]
        (is (pos? watermark-before))
        (is (= watermark-before persisted-lsn))
        (is (= watermark-before watermark-after)))
      (finally
        (i/close store)
        (u/delete-files dir)))))

(deftest validate-ha-snapshot-copy-prefers-payload-lsn-test
  (let [db-identity (str "db-" (UUID/randomUUID))
        manifest (#'dha/validate-ha-snapshot-copy!
                  "orders"
                  {:ha-db-identity db-identity}
                  "10.0.0.13:8898"
                  "/tmp/ignored"
                  {:db-name "orders"
                   :db-identity db-identity
                   :snapshot-last-applied-lsn 7
                   :payload-last-applied-lsn 11}
                  11)]
    (is (= "orders" (:db-name manifest)))
    (is (= db-identity (:db-identity manifest)))
    (is (= 7 (:snapshot-last-applied-lsn manifest)))
    (is (= 11 (:payload-last-applied-lsn manifest)))))

(deftest fetch-ha-endpoint-snapshot-copy-unpins-backup-pin-best-effort-test
  (let [opened   (atom nil)
        unpinned (atom [])
        closed   (atom [])]
    (with-redefs-fn
      {#'dha/open-ha-snapshot-remote-store!
       (fn [uri opts]
         (reset! opened {:uri uri :opts opts})
         ::remote-store)
       #'dha/copy-ha-remote-store!
       (fn [store dest compact?]
         (is (= ::remote-store store))
         (is (= "/tmp/ignored" dest))
         (is (false? compact?))
         {:db-name "orders"
          :backup-pin {:pin-id "backup-copy/test-pin"}})
       #'dha/unpin-ha-remote-store-backup-floor!
       (fn [store pin-id]
         (swap! unpinned conj [store pin-id])
         (throw (ex-info "cleanup failed" {:pin-id pin-id})))
       #'dha/close-ha-snapshot-remote-store!
       (fn [store]
         (swap! closed conj store))}
      (fn []
        (is (= {:copy-meta {:db-name "orders"
                            :backup-pin
                            {:pin-id "backup-copy/test-pin"}}}
               (#'dha/fetch-ha-endpoint-snapshot-copy!
                "orders"
                {:ha-lease-renew-ms 1000}
                "10.0.0.11:8898"
                "/tmp/ignored")))
        (is (= [[::remote-store "backup-copy/test-pin"]]
               @unpinned))
        (is (= [::remote-store] @closed))
        (is (= "dtlv://datalevin:datalevin@10.0.0.11:8898/orders"
               (:uri @opened)))))))

(deftest read-ha-local-last-applied-lsn-uses-persisted-ha-lsn-after-install-test
  (let [db-name "orders"
        db-identity (str "db-" (UUID/randomUUID))
        group-id (str "ha-install-" (UUID/randomUUID))
        source-dir (u/tmp-dir (str "ha-source-snapshot-floor-"
                                   (UUID/randomUUID)))
        local-dir (u/tmp-dir (str "ha-local-snapshot-floor-"
                                  (UUID/randomUUID)))
        copy-dir (u/tmp-dir (str "ha-copy-snapshot-floor-"
                                 (UUID/randomUUID)))
        source-store (st/open source-dir nil
                              (-> (valid-ha-opts group-id)
                                  (assoc :db-name db-name
                                         :db-identity db-identity
                                         :ha-node-id 3
                                         :wal? true)
                                  (update :ha-control-plane merge
                                          {:backend :sofa-jraft
                                           :rpc-timeout-ms 5000
                                           :election-timeout-ms 5000
                                           :operation-timeout-ms 30000
                                           :local-peer-id "10.0.0.13:7801"})))
        local-store (st/open local-dir nil
                             (-> (valid-ha-opts group-id)
                                 (assoc :db-name db-name
                                        :db-identity db-identity
                                        :ha-node-id 1
                                        :wal? true)
                                 (update :ha-control-plane merge
                                         {:backend :sofa-jraft
                                          :rpc-timeout-ms 5000
                                          :election-timeout-ms 5000
                                          :operation-timeout-ms 30000
                                          :local-peer-id "10.0.0.11:7801"})))
        install-state (atom nil)
        snapshot-lsn* (atom nil)]
    (try
      (let [source-kv (.-lmdb source-store)
            _ (i/open-dbi source-kv "a")
            _ (doseq [i (range 5)]
                (i/transact-kv source-kv
                               [[:put "a"
                                 (str "k" i)
                                 (str "v" i)]]))
            _ (i/create-snapshot! source-kv)
            snapshot-lsn
            (long (or (i/get-value source-kv c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (reset! snapshot-lsn* snapshot-lsn)
            _ (is (> snapshot-lsn 1))
            _ (i/copy source-kv copy-dir false)
            install-res (#'dha/install-ha-local-snapshot!
                         {:store local-store
                          :ha-db-identity db-identity}
                         copy-dir)]
        (is (:ok? install-res))
        (reset! install-state (:state install-res))
        (let [installed-store (:store @install-state)
              _ (#'dha/persist-ha-local-applied-lsn!
                 {:store installed-store}
                 @snapshot-lsn*)
              installed-kv (.-lmdb installed-store)
              watermark-lsn (long (or (:last-applied-lsn
                                       (i/txlog-watermarks installed-kv))
                                      0))]
          (is (= @snapshot-lsn*
                 (i/get-value installed-kv c/kv-info
                              c/ha-local-applied-lsn
                              :keyword :data)))
          (is (<= watermark-lsn @snapshot-lsn*))
          (is (= 1
                 (i/get-value installed-kv c/opts
                              :ha-node-id :attr :data)))
          (is (= @snapshot-lsn*
                 (dha/read-ha-local-last-applied-lsn
                  {:store installed-store})))))
      (finally
        (when-let [store (:store @install-state)]
          (when-not (identical? store local-store)
            (when-not (i/closed? store)
              (i/close store))))
        (when-not (i/closed? local-store)
          (i/close local-store))
        (when-not (i/closed? source-store)
          (i/close source-store))
        (u/delete-files copy-dir)
        (u/delete-files local-dir)
        (u/delete-files source-dir)))))

(deftest failed-snapshot-install-state-reopens-local-store-on-next-sync-test
  (let [db-name "orders"
        db-identity (str "db-" (UUID/randomUUID))
        group-id (str "ha-install-restore-failure-" (UUID/randomUUID))
        local-dir (u/tmp-dir (str "ha-local-snapshot-restore-failure-"
                                  (UUID/randomUUID)))
        local-store (st/open local-dir nil
                             (-> (valid-ha-opts group-id)
                                 (assoc :db-name db-name
                                        :db-identity db-identity
                                        :ha-node-id 1
                                        :wal? true)
                                 (update :ha-control-plane merge
                                         {:backend :sofa-jraft
                                          :rpc-timeout-ms 5000
                                          :election-timeout-ms 5000
                                          :operation-timeout-ms 30000
                                          :local-peer-id "10.0.0.11:7801"})))
        recovered-store (atom nil)]
    (try
      (i/open-dbi (.-lmdb local-store) "a")
      (i/transact-kv (.-lmdb local-store) [[:put "a" "k1" "v1"]])
      (i/close local-store)
      (reset! recovered-store
              (:store (#'dha/reopen-ha-local-store-if-needed
                       {:store local-store
                        :dt-db nil})))
      (is (instance? Store @recovered-store))
      (is (not (identical? local-store @recovered-store)))
      (is (= 1
             (:ha-node-id (i/opts @recovered-store))))
      (finally
        (when-let [store @recovered-store]
          (when-not (i/closed? store)
            (i/close store)))
        (when-not (i/closed? local-store)
          (i/close local-store))
        (u/delete-files local-dir)))))

(deftest read-ha-local-last-applied-lsn-falls-back-to-cached-state-when-store-closed-test
  (let [dir (u/tmp-dir (str "ha-closed-store-fallback-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :db-identity (str "db-" (UUID/randomUUID))
                                :wal? true})
        state {:store store
               :ha-local-last-applied-lsn 0}]
    (try
      (is (= 7
             (#'dha/persist-ha-local-applied-lsn! state 7)))
      (i/close store)
      (let [closed-state (assoc state :ha-local-last-applied-lsn 9)]
        (is (= 9
               (dha/read-ha-local-last-applied-lsn closed-state)))
        (is (= 11
               (#'dha/persist-ha-local-applied-lsn! closed-state 11))))
      (finally
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest persist-ha-local-applied-lsn-uses-runtime-db-store-when-primary-store-closed-test
  (let [dir           (u/tmp-dir (str "ha-runtime-store-" (UUID/randomUUID)))
        opts          {:db-name "orders"
                       :db-identity (str "db-" (UUID/randomUUID))
                       :wal? true}
        primary-store (st/open dir nil opts)
        runtime-store (st/open dir nil opts)
        runtime-db    (db/new-db runtime-store)
        state         {:store primary-store
                       :dt-db runtime-db}]
    (try
      (i/close primary-store)
      (is (some? (#'dha/raw-local-kv-store state)))
      (#'dha/persist-ha-local-applied-lsn! state 42)
      (is (= 42
             (#'dha/persist-ha-local-applied-lsn! state 42)))
      (finally
        (when-not (i/closed? runtime-store)
          (i/close runtime-store))
        (u/delete-files dir)))))

(deftest read-ha-local-last-applied-lsn-prefers-follower-ha-floor-test
  (let [dir (u/tmp-dir (str "ha-follower-watermark-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :db-identity (str "db-" (UUID/randomUUID))
                                :wal? true})
        lmdb (.-lmdb ^Store store)]
    (try
      (i/open-dbi lmdb "a")
      (i/transact-kv lmdb [[:put "a" "k1" "v1"]])
      (let [initial-watermark-lsn (long (or (:last-applied-lsn
                                             (i/txlog-watermarks lmdb))
                                            0))]
        (is (pos? initial-watermark-lsn))
        (#'dha/persist-ha-local-applied-lsn!
         {:store store}
         (+ initial-watermark-lsn 7))
        (let [watermark-lsn (long (or (:last-applied-lsn
                                       (i/txlog-watermarks lmdb))
                                      0))]
          (is (>= watermark-lsn initial-watermark-lsn))
          (is (= (+ initial-watermark-lsn 7)
                 (dha/read-ha-local-last-applied-lsn
                  {:store store
                   :ha-role :follower
                   :ha-local-last-applied-lsn (+ watermark-lsn 11)})))
          (is (= (+ initial-watermark-lsn 7)
                 (dha/read-ha-local-last-applied-lsn
                  {:store store
                   :ha-role :leader})))))
      (finally
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest read-ha-local-last-applied-lsn-leader-includes-payload-lsn-when-watermark-stale-test
  (let [dir (u/tmp-dir (str "ha-leader-payload-watermark-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :db-identity (str "db-" (UUID/randomUUID))
                                :wal? true})
        lmdb (.-lmdb ^Store store)]
    (try
      (i/open-dbi lmdb "a")
      (i/transact-kv lmdb [[:put "a" "k1" "v1"]])
      (let [payload-lsn (long (or (i/get-value lmdb c/kv-info
                                               c/wal-local-payload-lsn
                                               :keyword :data)
                                  0))]
        (is (pos? payload-lsn))
        (with-redefs [kv/txlog-watermarks (fn [_]
                                            {:last-applied-lsn 0})]
          (is (= payload-lsn
                 (dha/read-ha-local-last-applied-lsn
                  {:store store
                   :ha-role :leader})))))
      (finally
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest refresh-ha-local-watermarks-prefers-follower-ha-floor-over-raw-watermark-test
  (let [dir (u/tmp-dir (str "ha-refresh-follower-watermark-"
                            (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :db-identity (str "db-" (UUID/randomUUID))
                                :wal? true})
        lmdb (.-lmdb ^Store store)]
    (try
      (#'dha/persist-ha-local-applied-lsn! {:store store} 42)
      (let [watermark-lsn (long (or (:last-applied-lsn
                                     (i/txlog-watermarks lmdb))
                                    0))]
        (is (pos? watermark-lsn))
        (is (= 42
             (:ha-local-last-applied-lsn
              (#'dha/refresh-ha-local-watermarks
               {:store store
                :ha-role :follower
                :ha-local-last-applied-lsn 0})))))
      (finally
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest refresh-ha-local-watermarks-leader-includes-payload-lsn-when-watermark-stale-test
  (let [dir (u/tmp-dir (str "ha-refresh-leader-payload-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :db-identity (str "db-" (UUID/randomUUID))
                                :wal? true})
        lmdb (.-lmdb ^Store store)]
    (try
      (i/open-dbi lmdb "a")
      (i/transact-kv lmdb [[:put "a" "k1" "v1"]])
      (let [payload-lsn (long (or (i/get-value lmdb c/kv-info
                                               c/wal-local-payload-lsn
                                               :keyword :data)
                                  0))]
        (is (pos? payload-lsn))
        (with-redefs [kv/txlog-watermarks (fn [_]
                                            {:last-applied-lsn 0})]
          (let [refreshed (#'dha/refresh-ha-local-watermarks
                           {:store store
                            :ha-role :leader
                            :ha-local-last-applied-lsn 0})]
            (is (= payload-lsn
                   (:ha-local-last-applied-lsn refreshed)))
            (is (= payload-lsn
                   (:ha-leader-last-applied-lsn refreshed))))))
      (finally
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest stop-ha-runtime-persists-authoritative-local-applied-lsn-for-rejoin-test
  (let [dir (u/tmp-dir (str "ha-stop-runtime-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name "orders"
                                :db-identity (str "db-" (UUID/randomUUID))
                                :wal? true})
        lmdb (.-lmdb ^Store store)]
    (try
      (i/open-dbi lmdb "a")
      (i/transact-kv lmdb [[:put "a" "k1" "v1"]])
      (let [watermark-lsn (long (or (:last-applied-lsn
                                     (i/txlog-watermarks lmdb))
                                    0))
            authoritative-lsn (long (max 1 (dec watermark-lsn)))]
        (is (pos? watermark-lsn))
        (is (> watermark-lsn authoritative-lsn))
        (with-redefs [srv/*stop-ha-renew-loop-fn* (fn [_] nil)
                      srv/*stop-ha-authority-fn* (fn [_ _] nil)]
          (#'srv/stop-ha-runtime
           "orders"
           {:store store
            :ha-role :leader
            :ha-node-id 1
            :ha-local-last-applied-lsn watermark-lsn
            :ha-authority-lease {:leader-last-applied-lsn authoritative-lsn}}))
        (is (= authoritative-lsn
               (i/get-value lmdb c/kv-info
                            c/ha-local-applied-lsn
                            :keyword :data))))
      (finally
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest sync-ha-follower-batch-reopens-closed-local-store-test
  (let [leader-endpoint "10.0.0.11:8898"
        leader-dir (u/tmp-dir (str "ha-leader-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-" (UUID/randomUUID)))
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}
        follower-store-v (volatile! nil)
        recovered-store-v (volatile! nil)]
    (try
      (let [leader-store (st/open leader-dir nil opts)]
        (try
          (let [leader-kv (.-lmdb leader-store)
                follower-store (st/open follower-dir nil opts)
                follower-kv (.-lmdb follower-store)
                follower-db (db/new-db follower-store)
                follower-runtime {:store follower-store
                                  :dt-db follower-db
                                  :ha-node-id 2
                                  :ha-local-endpoint "10.0.0.12:8898"
                                  :ha-lease-renew-ms 1000}
                lease {:leader-node-id 1
                       :leader-endpoint leader-endpoint
                       :leader-last-applied-lsn 2}
                now-ms (System/currentTimeMillis)
                leader-db (kv/wrap-lmdb leader-kv)]
            (i/open-dbi leader-kv "a")
            (i/open-dbi follower-kv "a")
            (i/transact-kv leader-kv
                           [[:put "a" "k1" "v1"]
                            [:put "a" "k2" "v2"]])
            (vreset! follower-store-v follower-store)
            (i/close follower-store)
            (with-redefs [dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint from-lsn upto-lsn]
                            (is (= leader-endpoint endpoint))
                            (kv/open-tx-log leader-db from-lsn upto-lsn))
                          dha/report-ha-replica-floor!
                          (fn [_ _ endpoint _]
                            (is (= leader-endpoint endpoint))
                            {:ok? true})]
              (let [sync (#'dha/sync-ha-follower-batch
                          "orders" follower-runtime lease 1 now-ms)
                    state (:state sync)
                    recovered-store (:store state)
                    recovered-kv (.-lmdb recovered-store)]
                (vreset! recovered-store-v recovered-store)
                (is (seq (:records sync)))
                (is (instance? Store recovered-store))
                (is (not (i/closed? recovered-store)))
                (is (pos? (long (:ha-local-last-applied-lsn state))))
                (is (= (inc (long (:ha-local-last-applied-lsn state)))
                       (:ha-follower-next-lsn state)))
                (is (= "v1" (i/get-value recovered-kv "a" "k1")))
                (is (= "v2" (i/get-value recovered-kv "a" "k2"))))))
          (finally
            (when-not (i/closed? leader-store)
              (i/close leader-store)))))
      (finally
        (when-let [store @recovered-store-v]
          (when-not (identical? store @follower-store-v)
            (when-not (i/closed? store)
              (i/close store))))
        (when-let [store @follower-store-v]
          (when-not (i/closed? store)
            (i/close store)))
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest sync-ha-follower-batch-clears-cached-miss-before-publishing-dt-db-test
  (let [leader-endpoint "10.0.0.11:8898"
        leader-dir (u/tmp-dir (str "ha-leader-cache-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-cache-" (UUID/randomUUID)))
        opts {:db-name "ha-e2e"
              :db-identity "db-1"
              :wal? true}]
    (try
      (let [leader-conn (d/create-conn leader-dir e2e-ha-schema opts)
            follower-conn (d/create-conn follower-dir e2e-ha-schema opts)
            leader-store-v (volatile! nil)]
        (try
          (doseq [conn [leader-conn follower-conn]]
            (d/transact! conn [{:drill/key "seed"
                                :drill/value "v1"}]))
          (is (nil? (d/q e2e-ha-value-query
                         @follower-conn
                         "post-failover")))
          (d/transact! leader-conn [{:drill/key "post-failover"
                                     :drill/value "v2"}])
          (vreset! leader-store-v (st/open leader-dir nil opts))
          (let [leader-store @leader-store-v
                follower-db @follower-conn
                follower-store (:store follower-db)
                follower-lsn (long (or (i/get-value (.-lmdb follower-store)
                                                    c/kv-info
                                                    c/wal-local-payload-lsn
                                                    :keyword :data)
                                       0))
                leader-lsn (long (or (i/get-value (.-lmdb leader-store)
                                                  c/kv-info
                                                  c/wal-local-payload-lsn
                                                  :keyword :data)
                                     0))
                lease {:leader-node-id 1
                       :leader-endpoint leader-endpoint
                       :leader-last-applied-lsn leader-lsn}
                now-ms (System/currentTimeMillis)]
            (with-redefs [dha/fetch-ha-leader-txlog-batch
                          (fn [_ _ endpoint from-lsn upto-lsn]
                            (is (= leader-endpoint endpoint))
                            (kv/open-tx-log-rows
                             (kv/wrap-lmdb (.-lmdb leader-store))
                             from-lsn
                             upto-lsn))
                          dha/report-ha-replica-floor!
                          (fn [_ _ endpoint _]
                            (is (= leader-endpoint endpoint))
                            {:ok? true})]
              (let [sync (#'dha/sync-ha-follower-batch
                          "ha-e2e"
                          {:store follower-store
                           :dt-db follower-db
                           :ha-node-id 2
                           :ha-local-endpoint "10.0.0.12:8898"
                           :ha-lease-renew-ms 1000}
                          lease
                          (inc follower-lsn)
                          now-ms)
                    state (:state sync)]
                (is (= "v2"
                       (d/q e2e-ha-value-query
                            (:dt-db state)
                            "post-failover"))))))
          (finally
            (when-let [store @leader-store-v]
              (when-not (i/closed? store)
                (i/close store)))
            (d/close leader-conn)
            (d/close follower-conn))))
      (finally
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest e2e-local-read-value-prefers-published-dt-db-test
  (let [published-dir (u/tmp-dir (str "ha-published-db-" (UUID/randomUUID)))
        stale-dir (u/tmp-dir (str "ha-stale-store-" (UUID/randomUUID)))
        opts {:db-name "ha-e2e"
              :db-identity "db-1"
              :wal? true}]
    (try
      (let [published-conn (d/create-conn published-dir e2e-ha-schema opts)
            stale-store (st/open stale-dir e2e-ha-schema opts)]
        (try
          (d/transact! published-conn [{:drill/key "post-failover"
                                        :drill/value "v2"}])
          (let [server (fake-server-with-db-state
                        "ha-e2e"
                        {:dt-db @published-conn
                         :store stale-store})
                ctx {:servers {1 server}
                     :db-name "ha-e2e"}]
            (is (= "v2"
                   (e2e-local-read-value ctx 1 "post-failover"))))
          (finally
            (d/close published-conn)
            (when-not (i/closed? stale-store)
              (i/close stale-store)))))
      (finally
        (u/delete-files published-dir)
        (u/delete-files stale-dir)))))

(deftest apply-ha-follower-txlog-record-replays-unique-identity-register-updates-test
  (let [leader-dir (u/tmp-dir (str "ha-leader-register-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-register-" (UUID/randomUUID)))
        schema {:register/key {:db/valueType :db.type/long
                               :db/unique :db.unique/identity}
                :register/value {:db/valueType :db.type/long}}
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}]
    (try
      (let [leader-conn (d/create-conn leader-dir schema opts)
            follower-store (st/open follower-dir schema opts)]
        (try
          (d/transact! leader-conn
                       (mapv (fn [k]
                               {:db/id (str "register-" k)
                                :register/key (long k)
                                :register/value 0})
                             (range 4)))
          (d/transact! leader-conn [{:register/key 0 :register/value 1}])
          (d/transact! leader-conn [{:register/key 1 :register/value 2}])
          (d/transact! leader-conn [{:register/key 0 :register/value 3}])
          (d/transact! leader-conn [[:db/cas
                                     [:register/key 1]
                                     :register/value
                                     2
                                     4]])
          (let [leader-store (:store @leader-conn)
                leader-kv (.-lmdb leader-store)
                records (vec (kv/open-tx-log-rows leader-kv 1 nil))
                follower-runtime {:store follower-store
                                  :dt-db (db/new-db follower-store)
                                  :ha-node-id 2}
                next-state (reduce #'dha/apply-ha-follower-txlog-record!
                                   follower-runtime
                                   records)
                rows (d/q '[:find ?key ?value
                            :where
                            [?e :register/key ?key]
                            [?e :register/value ?value]]
                          (db/new-db (:store next-state)))
                values (into {}
                             (map (fn [[k v]]
                                    [(long k) (long v)]))
                             rows)]
            (is (= {0 3, 1 4, 2 0, 3 0} values)))
          (finally
            (d/close leader-conn)
            (when-not (i/closed? follower-store)
              (i/close follower-store)))))
      (finally
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest apply-ha-follower-txlog-record-mirrors-local-txlog-test
  (let [leader-dir (u/tmp-dir (str "ha-leader-replay-watermark-" (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-replay-watermark-" (UUID/randomUUID)))
        schema {:register/key {:db/valueType :db.type/long
                               :db/unique :db.unique/identity}
                :register/value {:db/valueType :db.type/long}}
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}]
    (try
      (let [leader-conn (d/create-conn leader-dir schema opts)
            follower-store (st/open follower-dir schema opts)]
        (try
          (d/transact! leader-conn
                       [{:db/id "register-0"
                         :register/key 0
                         :register/value 0}])
          (d/transact! leader-conn [{:register/key 0 :register/value 7}])
          (let [leader-store (:store @leader-conn)
                leader-kv (.-lmdb leader-store)
                follower-kv (.-lmdb follower-store)
                record (txlog-record-with-max-tx leader-store 2)
                from-lsn (long (:lsn record))
                follower-runtime {:store follower-store
                                  :dt-db (db/new-db follower-store)
                                  :ha-node-id 2}
                local-records-before
                (vec (kv/open-tx-log-rows follower-kv from-lsn nil))
                next-state (#'dha/apply-ha-follower-txlog-record!
                            follower-runtime
                            record)
                follower-store-after (:store next-state)
                follower-kv-after (.-lmdb ^Store follower-store-after)
                local-records-after
                (vec (kv/open-tx-log-rows follower-kv-after from-lsn nil))
                payload-lsn
                (long (or (i/get-value follower-kv-after
                                       c/kv-info
                                       c/wal-local-payload-lsn
                                       :keyword :data)
                          0))
                record-lsn (long (:lsn record))]
            (is (= []
                   (mapv :lsn local-records-before)))
            (is (= [record-lsn]
                   (mapv :lsn local-records-after)))
            (is (= [(:tx-kind record)]
                   (mapv :tx-kind local-records-after)))
            (is (= [(count (:rows record))]
                   (mapv (comp count :rows) local-records-after)))
            (is (= record-lsn payload-lsn)))
          (finally
            (d/close leader-conn)
            (when-not (i/closed? follower-store)
              (i/close follower-store)))))
      (finally
        (u/delete-files leader-dir)
        (u/delete-files follower-dir)))))

(deftest apply-ha-follower-txlog-record-preserves-next-local-lsn-test
  (let [leader-dir (u/tmp-dir (str "ha-leader-promote-floor-" (UUID/randomUUID)))
        promoted-dir (u/tmp-dir (str "ha-promoted-floor-" (UUID/randomUUID)))
        schema {:register/key {:db/valueType :db.type/long
                               :db/unique :db.unique/identity}
                :register/value {:db/valueType :db.type/long}}
        opts {:db-name "orders"
              :db-identity "db-1"
              :wal? true}
        leader-conn (d/create-conn leader-dir schema opts)
        promoted-store-v (volatile! nil)]
    (try
      (d/transact! leader-conn [{:register/key 0 :register/value 0}])
      (vreset! promoted-store-v (st/open promoted-dir schema opts))
      (let [leader-store (:store @leader-conn)
            promoted-store @promoted-store-v
            record (txlog-record-with-max-tx leader-store 2)
            promoted-state (#'dha/apply-ha-follower-txlog-record!
                            {:store promoted-store
                             :dt-db (db/new-db promoted-store)}
                            record)
            promoted-store (:store promoted-state)
            promoted-kv (.-lmdb ^Store promoted-store)
            expected-next-lsn (unchecked-inc (long (:lsn record)))
            next-row (fn []
                       (peek (vec (kv/open-tx-log-rows promoted-kv 1 nil))))]
        (vreset! promoted-store-v promoted-store)
        (is (= expected-next-lsn
               (long (or (:next-lsn (kv/txlog-watermarks promoted-kv)) 0))))
        (i/open-dbi promoted-kv "scratch")
        (i/transact-kv promoted-kv [[:put "scratch" "k2" "v2"]])
        (let [row (next-row)]
          (is (= expected-next-lsn
                 (long (:lsn row))))))
      (finally
        (when-let [promoted-store @promoted-store-v]
          (when-not (i/closed? promoted-store)
            (i/close promoted-store)))
        (d/close leader-conn)
        (u/delete-files leader-dir)
        (u/delete-files promoted-dir)))))

(deftest ha-renew-step-follower-gap-bootstraps-from-snapshot-copy-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-dir (u/tmp-dir (str "ha-local-snapshot-" (UUID/randomUUID)))
        source-dir (u/tmp-dir (str "ha-source-snapshot-" (UUID/randomUUID)))
        db-identity (:ha-db-identity runtime)
        local-store (st/open local-dir nil {:db-name "orders"
                                            :db-identity db-identity
                                            :wal? true})
        source-store (st/open source-dir nil {:db-name "orders"
                                              :db-identity db-identity
                                              :wal? true})
        snapshot-lsn* (atom nil)
        next-state (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [local-kv (.-lmdb local-store)
            source-kv (.-lmdb source-store)
            _ (i/open-dbi source-kv "a")
            _ (i/transact-kv source-kv
                             [[:put "a" "k1" "v1"]
                              [:put "a" "k2" "v2"]])
            _ (i/create-snapshot! source-kv)
            snapshot-lsn
            (long (or (i/get-value source-kv c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (reset! snapshot-lsn* snapshot-lsn)
            authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity db-identity
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn (inc snapshot-lsn)
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :store local-store
                                        :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))]
        (with-redefs-fn
          {#'dha/fetch-ha-leader-txlog-batch
           (fn [_ _ endpoint _ _]
             (case endpoint
               "10.0.0.11:8898"
               [{:lsn (inc snapshot-lsn)
                 :ops [[:put "a" "k3" "v3"]]}]
               "10.0.0.13:8898"
               [{:lsn (inc snapshot-lsn)
                 :ops [[:put "a" "k3" "v3"]]}]
               []))
           #'dha/fetch-ha-endpoint-watermark-lsn
           (fn [_ _ endpoint]
             (case endpoint
               "10.0.0.11:8898"
               {:reachable? true :last-applied-lsn (inc snapshot-lsn)}
               "10.0.0.13:8898"
               {:reachable? true :last-applied-lsn snapshot-lsn}
               (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))
           #'dha/fetch-ha-endpoint-snapshot-copy!
           (fn [_ _ endpoint dest-dir]
             (case endpoint
               "10.0.0.11:8898"
               (throw (ex-info "leader snapshot unavailable"
                               {:error :ha/follower-snapshot-unavailable}))
               "10.0.0.13:8898"
               {:copy-meta
                (assoc (i/copy source-kv dest-dir false)
                       :db-name "orders"
                       :db-identity db-identity
                       :snapshot-last-applied-lsn snapshot-lsn)}
               (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))
           #'dha/report-ha-replica-floor!
           (fn [_ _ _ _]
             {:ok? true})}
          (fn []
            (reset! next-state (#'srv/ha-renew-step "orders" follower-runtime)))))
      (let [state @next-state
            next-kv (.-lmdb (:store state))]
        (i/open-dbi next-kv "a")
        (is (= :follower (:ha-role state)))
        (is (= (inc @snapshot-lsn*) (:ha-local-last-applied-lsn state)))
        (is (= (inc @snapshot-lsn*)
               (i/get-value next-kv c/kv-info
                            c/ha-local-applied-lsn
                            :keyword :data)))
        (is (= (+ 2 @snapshot-lsn*) (:ha-follower-next-lsn state)))
        (is (= "10.0.0.13:8898"
               (:ha-follower-bootstrap-source-endpoint state)))
        (is (integer? (:ha-follower-last-bootstrap-ms state)))
        (is (nil? (:ha-follower-degraded? state)))
        (is (= "v2" (i/get-value next-kv "a" "k2")))
        (is (= "v3" (i/get-value next-kv "a" "k3"))))
      (finally
        (when-let [store (:store @next-state)]
          (when-not (i/closed? store)
            (i/close store)))
        (when-not (i/closed? source-store)
          (i/close source-store))
        (when-not (i/closed? local-store)
          (i/close local-store))
        (#'srv/stop-ha-authority "orders" runtime)
        (u/delete-files local-dir)
        (u/delete-files source-dir)))))

(deftest ha-renew-step-follower-gap-bootstraps-from-copy-payload-newer-than-snapshot-floor-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-dir (u/tmp-dir (str "ha-local-copy-payload-" (UUID/randomUUID)))
        source-dir (u/tmp-dir (str "ha-source-copy-payload-" (UUID/randomUUID)))
        db-identity (:ha-db-identity runtime)
        local-store (st/open local-dir nil {:db-name "orders"
                                            :db-identity db-identity
                                            :wal? true})
        source-store (st/open source-dir nil {:db-name "orders"
                                              :db-identity db-identity
                                              :wal? true})
        snapshot-lsn* (atom nil)
        payload-lsn* (atom nil)
        next-state (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [local-kv (.-lmdb local-store)
            source-kv (.-lmdb source-store)
            _ (i/open-dbi source-kv "a")
            _ (i/transact-kv source-kv [[:put "a" "k1" "v1"]])
            _ (i/create-snapshot! source-kv)
            snapshot-lsn
            (long (or (i/get-value source-kv c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            _ (i/transact-kv source-kv [[:put "a" "k2" "v2"]])
            payload-lsn
            (long (or (:last-applied-lsn (i/txlog-watermarks source-kv)) 0))
            _ (reset! snapshot-lsn* snapshot-lsn)
            _ (reset! payload-lsn* payload-lsn)
            authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity db-identity
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn (inc payload-lsn)
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :store local-store
                                        :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))]
        (with-redefs-fn
          {#'dha/fetch-ha-leader-txlog-batch
           (fn [_ _ endpoint _ _]
             (case endpoint
               "10.0.0.11:8898"
               [{:lsn (inc payload-lsn)
                 :ops [[:put "a" "k3" "v3"]]}]
               "10.0.0.13:8898"
               [{:lsn (inc payload-lsn)
                 :ops [[:put "a" "k3" "v3"]]}]
               []))
           #'dha/fetch-ha-endpoint-watermark-lsn
           (fn [_ _ endpoint]
             (case endpoint
               "10.0.0.11:8898"
               {:reachable? true :last-applied-lsn (inc payload-lsn)}
               "10.0.0.13:8898"
               {:reachable? true :last-applied-lsn payload-lsn}
               (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))
           #'dha/fetch-ha-endpoint-snapshot-copy!
           (fn [_ _ endpoint dest-dir]
             (case endpoint
               "10.0.0.11:8898"
               (throw (ex-info "leader snapshot unavailable"
                               {:error :ha/follower-snapshot-unavailable}))
               "10.0.0.13:8898"
               {:copy-meta
                (assoc (i/copy source-kv dest-dir false)
                       :db-name "orders"
                       :db-identity db-identity
                       :snapshot-last-applied-lsn snapshot-lsn
                       :payload-last-applied-lsn payload-lsn)}
               (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))
           #'dha/report-ha-replica-floor!
           (fn [_ _ _ _]
             {:ok? true})}
          (fn []
            (reset! next-state (#'srv/ha-renew-step "orders" follower-runtime)))))
      (let [state @next-state
            next-kv (.-lmdb (:store state))]
        (i/open-dbi next-kv "a")
        (is (= :follower (:ha-role state)))
        (is (= (inc @payload-lsn*) (:ha-local-last-applied-lsn state)))
        (is (= (inc @payload-lsn*)
               (i/get-value next-kv c/kv-info
                            c/ha-local-applied-lsn
                            :keyword :data)))
        (is (= (+ 2 @payload-lsn*) (:ha-follower-next-lsn state)))
        (is (= "10.0.0.13:8898"
               (:ha-follower-bootstrap-source-endpoint state)))
        (is (> @payload-lsn* @snapshot-lsn*))
        (is (= @snapshot-lsn*
               (:ha-follower-bootstrap-snapshot-last-applied-lsn state)))
        (is (nil? (:ha-follower-degraded? state)))
        (is (= "v1" (i/get-value next-kv "a" "k1")))
        (is (= "v2" (i/get-value next-kv "a" "k2")))
        (is (= "v3" (i/get-value next-kv "a" "k3"))))
      (finally
        (when-let [store (:store @next-state)]
          (when-not (i/closed? store)
            (i/close store)))
        (when-not (i/closed? source-store)
          (i/close source-store))
        (when-not (i/closed? local-store)
          (i/close local-store))
        (#'srv/stop-ha-authority "orders" runtime)
        (u/delete-files local-dir)
        (u/delete-files source-dir)))))

(deftest bootstrap-ha-follower-from-snapshot-does-not-poison-floor-when-copy-payload-exceeds-copied-data-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-dir (u/tmp-dir (str "ha-local-copy-floor-" (UUID/randomUUID)))
        source-dir (u/tmp-dir (str "ha-source-copy-floor-" (UUID/randomUUID)))
        db-identity (:ha-db-identity runtime)
        local-store (st/open local-dir nil {:db-name "orders"
                                            :db-identity db-identity
                                            :wal? true})
        source-store (st/open source-dir nil {:db-name "orders"
                                              :db-identity db-identity
                                              :wal? true})
        snapshot-lsn* (atom nil)
        poisoned-payload-lsn* (atom nil)
        bootstrap-state (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [local-kv (.-lmdb local-store)
            source-kv (.-lmdb source-store)
            _ (i/open-dbi local-kv "a")
            _ (i/open-dbi source-kv "a")
            _ (i/transact-kv source-kv [[:put "a" "k1" "v1"]])
            _ (i/create-snapshot! source-kv)
            snapshot-lsn
            (long (or (i/get-value source-kv c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            poisoned-payload-lsn (+ snapshot-lsn 3)
            _ (reset! snapshot-lsn* snapshot-lsn)
            _ (reset! poisoned-payload-lsn* poisoned-payload-lsn)
            authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity db-identity
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn poisoned-payload-lsn
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :store local-store
                                        :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))
            lease {:leader-node-id 1
                   :leader-endpoint "10.0.0.11:8898"
                   :leader-last-applied-lsn poisoned-payload-lsn}
            bootstrap
            (with-redefs-fn
              {#'dha/fetch-ha-leader-txlog-batch
               (fn [_ _ endpoint from-lsn _]
                 (is (contains? #{"10.0.0.11:8898"
                                  "10.0.0.13:8898"}
                                endpoint))
                 (is (= (inc snapshot-lsn) from-lsn))
                 [])
               #'dha/fetch-ha-endpoint-watermark-lsn
               (fn [_ _ endpoint]
                 (case endpoint
                   "10.0.0.11:8898"
                   {:reachable? true
                    :last-applied-lsn poisoned-payload-lsn}
                   "10.0.0.13:8898"
                   {:reachable? true
                    :last-applied-lsn poisoned-payload-lsn}
                   (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))
               #'dha/fetch-ha-endpoint-snapshot-copy!
               (fn [_ _ endpoint dest-dir]
                 (is (= "10.0.0.13:8898" endpoint))
                 {:copy-meta
                  (assoc (i/copy source-kv dest-dir false)
                         :db-name "orders"
                         :db-identity db-identity
                         :snapshot-last-applied-lsn snapshot-lsn
                         :payload-last-applied-lsn poisoned-payload-lsn)})}
              (fn []
                (#'dha/bootstrap-ha-follower-from-snapshot
                 "orders"
                 follower-runtime
                 lease
                 ["10.0.0.13:8898"]
                 1
                 now-ms)))]
        (reset! bootstrap-state (:state bootstrap))
        (let [state @bootstrap-state
              next-kv (.-lmdb (:store state))]
          (i/open-dbi next-kv "a")
          (is (false? (:ok? bootstrap)))
          (is (= @snapshot-lsn* (:ha-local-last-applied-lsn state)))
          (is (= @snapshot-lsn*
                 (i/get-value next-kv c/kv-info
                              c/ha-local-applied-lsn
                              :keyword :data)))
          (is (= (inc @snapshot-lsn*) (:ha-follower-next-lsn state)))
          (is (= @snapshot-lsn*
                 (:ha-follower-bootstrap-snapshot-last-applied-lsn state)))
          (is (= "10.0.0.13:8898"
                 (:ha-follower-bootstrap-source-endpoint state)))
          (is (= "v1" (i/get-value next-kv "a" "k1")))
          (is (nil? (i/get-value next-kv "a" "k2")))
          (is (= @snapshot-lsn*
                 (get-in bootstrap
                         [:errors 0 :data :snapshot-last-applied-lsn])))
          (is (= @snapshot-lsn*
                 (get-in bootstrap
                         [:errors 0 :data :installed-last-applied-lsn])))
          (is (= (inc @snapshot-lsn*)
                 (get-in bootstrap
                         [:errors 0 :data :resume-next-lsn])))
          (is (not= @poisoned-payload-lsn*
                    (:ha-local-last-applied-lsn state)))))
      (finally
        (when-let [store (:store @bootstrap-state)]
          (when-not (i/closed? store)
            (i/close store)))
        (when-not (i/closed? source-store)
          (i/close source-store))
        (when-not (i/closed? local-store)
          (i/close local-store))
        (#'srv/stop-ha-authority "orders" runtime)
        (u/delete-files local-dir)
        (u/delete-files source-dir)))))

(deftest ha-renew-step-follower-gap-snapshot-bootstrap-rejects-db-identity-mismatch-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        local-dir (u/tmp-dir (str "ha-local-snapshot-mismatch-"
                                  (UUID/randomUUID)))
        local-store (st/open local-dir nil {:db-name "orders"
                                            :db-identity (:ha-db-identity runtime)
                                            :wal? true})
        next-state (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 3
                :now-ms now-ms
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :store local-store
                                        :ha-role :follower
                                        :ha-follower-next-lsn 1
                                        :ha-local-last-applied-lsn 0))]
        (with-redefs-fn
          {#'dha/fetch-ha-leader-txlog-batch
           (fn [_ _ endpoint _ _]
             (case endpoint
               "10.0.0.11:8898"
               [{:lsn 3 :ops [[:put "a" "k3" "v3"]]}]
               "10.0.0.13:8898"
               [{:lsn 3 :ops [[:put "a" "k3" "v3"]]}]
               []))
           #'dha/read-ha-local-last-applied-lsn
           (fn [_] 0)
           #'dha/fetch-ha-endpoint-watermark-lsn
           (fn [_ _ endpoint]
             (case endpoint
               "10.0.0.13:8898"
               {:reachable? true :last-applied-lsn 2}
               (throw (ex-info "unexpected-endpoint" {:endpoint endpoint}))))
           #'dha/fetch-ha-endpoint-snapshot-copy!
           (fn [_ m endpoint _]
             (throw (ex-info
                     "HA snapshot copy DB identity mismatch"
                     {:error :ha/follower-snapshot-db-identity-mismatch
                      :db-name "orders"
                      :local-db-identity (:ha-db-identity m)
                      :snapshot-db-identity "db-mismatch"
                      :source-endpoint endpoint})))}
          (fn []
            (reset! next-state (#'srv/ha-renew-step "orders" follower-runtime)))))
      (let [state @next-state]
        (is (= :follower (:ha-role state)))
        (is (true? (:ha-follower-degraded? state)))
        (is (= :wal-gap (:ha-follower-degraded-reason state)))
        (is (some #(= :ha/follower-snapshot-db-identity-mismatch
                      (or (:error %)
                          (get-in % [:data :error])))
                  (get-in state
                          [:ha-follower-last-error-details
                           :data
                           :snapshot-errors])))
        (is (= local-store (:store state))))
      (finally
        (when-not (i/closed? local-store)
          (i/close local-store))
        (#'srv/stop-ha-authority "orders" runtime)
        (u/delete-files local-dir)))))
