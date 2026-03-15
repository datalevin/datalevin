(ns datalevin.server-ha-runtime-test
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

(deftest server-client-disconnect-classification-test
  (is (true? (#'srv/client-disconnect?
              (ClosedChannelException.))))
  (is (true? (#'srv/client-disconnect?
              (ex-info "Error sending message and receiving response"
                       {}
                       (ex-info "Socket channel is closed." {})))))
  (is (false? (#'srv/client-disconnect?
               (ex-info "authentication failed" {}
                        (RuntimeException. "boom"))))))

(deftest ha-request-timeout-prefers-control-plane-rpc-budget-test
  (let [m {:ha-lease-renew-ms 1000
           :ha-control-plane {:rpc-timeout-ms 5000}}]
    (is (= 5000 (#'dha/ha-request-timeout-ms m 10000)))
    (is (= 5000 (#'dha/ha-request-timeout-ms m 5000)))
    (is (= 1000 (#'dha/ha-request-timeout-ms
                 {:ha-lease-renew-ms 250}
                 10000)))))

(defn- persisted-ha-runtime-store-opts
  [ha-opts]
  (-> ha-opts
      (dissoc :ha-node-id
              :ha-client-credentials
              :ha-fencing-hook
              :ha-clock-skew-hook)
      (update :ha-control-plane dissoc :local-peer-id :raft-dir)))

(deftest ha-write-admission-error-skips-udf-refresh-for-read-messages-test
  (let [db-name "orders"
        initial-state {:marker :initial}
        ^Server server (fake-server-with-db-state db-name initial-state)
        calls (atom [])]
    (binding [srv/*ensure-udf-readiness-state-fn*
              (fn [m]
                (swap! calls conj (:marker m))
                (assoc m :marker :updated))]
      (is (nil? (#'srv/ha-write-admission-error
                 server
                 {:type :get-value
                  :args [db-name "a" "k"]})))
      (is (empty? @calls))
      (is (identical? initial-state (get (.-dbs server) db-name)))
      (is (nil? (#'srv/ha-write-admission-error
                 server
                 {:type :open-dbi
                  :args [db-name "__ha_probe" nil]})))
      (is (= [:initial] @calls))
      (let [state (get (.-dbs server) db-name)]
        (is (= :updated (:marker state)))
        (is (not (identical? initial-state state)))))))

(deftest ha-write-admission-error-refreshes-udf-state-for-tx-data-messages-test
  (let [db-name "orders"
        initial-state {:marker :initial}
        ^Server server (fake-server-with-db-state db-name initial-state)
        calls (atom [])]
    (binding [srv/*ensure-udf-readiness-state-fn*
              (fn [m]
                (swap! calls conj (:marker m))
                (assoc m :marker :updated))]
      (is (nil? (#'srv/ha-write-admission-error
                 server
                 {:type :tx-data
                  :args [db-name [] false]})))
      (is (= [:initial] @calls))
      (is (= :updated (:marker (get (.-dbs server) db-name))))
      (#'srv/update-db server db-name #(assoc % :marker :initial))
      (reset! calls [])
      (is (nil? (#'srv/ha-write-admission-error
                 server
                 {:type :tx-data+db-info
                  :args [db-name [] false]})))
      (is (= [:initial] @calls))
      (is (= :updated (:marker (get (.-dbs server) db-name)))))))

(deftest add-store-reopens-datalog-store-after-closed-lmdb-race-test
  (let [dir (u/tmp-dir (str "server-add-store-race-" (UUID/randomUUID)))
        server (srv/create {:port 0 :root dir})
        db-name "orders"
        store-dir (#'srv/db-dir dir db-name)
        schema {:name {:db/valueType :db.type/string}}
        opts {:db-name db-name
              :wal? true}
        store (st/open store-dir schema opts)
        original-new-db db/new-db
        fail-once? (atom true)]
    (try
      (with-redefs [db/new-db
                    (fn
                      ([store]
                       (if (compare-and-set! fail-once? true false)
                         (do
                           (i/close store)
                           (throw
                            (AssertionError.
                             "Assert failed: LMDB env is closed.\n(not (.closed-kv? this))")))
                         (original-new-db store)))
                      ([store info]
                       (if (compare-and-set! fail-once? true false)
                         (do
                           (i/close store)
                           (throw
                            (AssertionError.
                             "Assert failed: LMDB env is closed.\n(not (.closed-kv? this))")))
                         (original-new-db store info))))]
        (let [added-store (#'srv/add-store server db-name store)]
          (is (instance? Store added-store))
          (is (not (identical? store added-store)))
          (is (false? (i/closed? added-store)))
          (is (= added-store (#'srv/get-store server db-name)))
          (is (some? (#'srv/get-db server db-name)))
          (is (= c/e0 (i/init-max-eid added-store)))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))

(deftest transient-write-open-race-classifies-invalid-argument-store-test
  (let [dir (u/tmp-dir (str "server-open-write-txn-retry-"
                            (UUID/randomUUID)))
        db-name "orders"
        store (st/open dir nil {:db-name db-name
                                :wal? true})]
    (try
      (is (true? (#'srv/transient-write-open-race?
                  (ex-info "Invalid argument" {})
                  store)))
      (is (false? (#'srv/transient-write-open-race?
                   (ex-info "boom" {})
                   store)))
      (i/close store)
      (is (true? (#'srv/transient-write-open-race?
                  (ex-info "LMDB env is closed" {})
                  store)))
      (finally
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest add-store-recovers-ha-snapshot-backup-marker-on-startup-test
  (let [dir (u/tmp-dir (str "server-add-store-snapshot-recover-" (UUID/randomUUID)))
        server (srv/create {:port 0 :root dir})
        db-name "orders"
        store-dir (#'srv/db-dir dir db-name)
        schema {:name {:db/valueType :db.type/string}}
        group-id (str "ha-test-" (UUID/randomUUID))
        opts (-> (valid-ha-opts group-id)
                 (assoc :db-name db-name
                        :wal? true)
                 (update :ha-control-plane merge
                         {:backend :sofa-jraft
                          :rpc-timeout-ms 5000
                          :election-timeout-ms 5000
                          :operation-timeout-ms 30000
                          :local-peer-id "10.0.0.12:7801"}))
        backup-dir (str store-dir ".ha-backup-startup-recover")
        marker-path (#'dha/ha-snapshot-install-marker-path store-dir)
        partial-store-v (volatile! nil)]
    (try
      (let [original-store (st/open store-dir schema opts)]
        (try
          (let [original-kv (.-lmdb ^Store original-store)]
            (i/open-dbi original-kv "a")
            (i/transact-kv original-kv [[:put "a" "k1" "v1"]]))
          (finally
            (when-not (i/closed? original-store)
              (i/close original-store)))))
      (when (u/file-exists backup-dir)
        (u/delete-files backup-dir))
      (#'dha/write-ha-snapshot-install-marker!
       store-dir
       {:backup-dir backup-dir
        :db-name db-name
        :stage :backup-moved})
      (#'dha/move-path! store-dir backup-dir)
      (u/create-dirs store-dir)
      (let [partial-store (st/open store-dir schema opts)
            added-store (#'srv/add-store server db-name partial-store false)
            added-kv (.-lmdb ^Store added-store)]
        (vreset! partial-store-v partial-store)
        (is (instance? Store added-store))
        (is (not (identical? partial-store added-store)))
        (is (false? (i/closed? added-store)))
        (is (= "v1" (i/get-value added-kv "a" "k1")))
        (is (not (u/file-exists marker-path)))
        (is (not (u/file-exists backup-dir)))
        (is (= added-store (#'srv/get-store server db-name))))
      (finally
        (srv/stop server)
        (when-let [partial-store @partial-store-v]
          (when-not (i/closed? partial-store)
            (i/close partial-store)))
        (u/delete-files dir)))))

(deftest open-store-recovers-ha-snapshot-backup-marker-before-opening-test
  (let [root (u/tmp-dir (str "server-open-store-snapshot-recover-" (UUID/randomUUID)))
        db-name "orders"
        store-dir (#'srv/db-dir root db-name)
        schema {:name {:db/valueType :db.type/string}}
        opts {:db-name db-name
              :wal? true}
        backup-dir (str store-dir ".ha-backup-open-store-recover")
        marker-path (#'dha/ha-snapshot-install-marker-path store-dir)
        opened-store-v (volatile! nil)]
    (try
      (let [original-store (st/open store-dir schema opts)]
        (try
          (let [original-kv (.-lmdb ^Store original-store)]
            (i/open-dbi original-kv "a")
            (i/transact-kv original-kv [[:put "a" "k1" "v1"]]))
          (finally
            (when-not (i/closed? original-store)
              (i/close original-store)))))
      (when (u/file-exists backup-dir)
        (u/delete-files backup-dir))
      (#'dha/write-ha-snapshot-install-marker!
       store-dir
       {:backup-dir backup-dir
        :db-name db-name
        :stage :backup-moved})
      (#'dha/move-path! store-dir backup-dir)
      (u/create-dirs store-dir)
      (let [opened-store (#'srv/open-store root db-name nil true)
            opened-kv (.-lmdb ^Store opened-store)]
        (vreset! opened-store-v opened-store)
        (i/open-dbi opened-kv "a")
        (is (instance? Store opened-store))
        (is (= "v1" (i/get-value opened-kv "a" "k1")))
        (is (not (u/file-exists marker-path)))
        (is (not (u/file-exists backup-dir))))
      (finally
        (when-let [opened-store @opened-store-v]
          (when-not (i/closed? opened-store)
            (i/close opened-store)))
        (u/delete-files root)))))

(deftest add-store-can-publish-datalog-store-without-activating-runtime-test
  (let [dir (u/tmp-dir (str "server-add-store-kv-only-" (UUID/randomUUID)))
        server (srv/create {:port 0 :root dir})
        db-name "orders"
        store-dir (#'srv/db-dir dir db-name)
        schema {:name {:db/valueType :db.type/string}}
        store (st/open store-dir schema {:db-name db-name
                                         :wal? true})]
    (try
      (binding [srv/*server-runtime-opts-fn*
                (fn [_ _ _ _]
                  (valid-ha-opts))]
        (#'srv/add-store server db-name store false))
      (let [state (get-in (.-dbs server) [db-name])]
        (is (identical? store (:store state)))
        (is (= store (#'srv/get-store server db-name)))
        (is (nil? (:dt-db state)))
        (is (nil? (:ha-authority state)))
        (is (nil? (:ha-runtime-opts state)))
        (is (nil? (:ha-renew-loop-future state)))
        (is (nil? (:ha-follower-loop-future state))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))

(deftest add-store-attaches-server-udf-registry-to-published-db-test
  (let [dir        (u/tmp-dir (str "server-udf-runtime-" (UUID/randomUUID)))
        descriptor {:udf/lang :test
                    :udf/kind :tx-fn
                    :udf/id   :bank/transfer}
        registry   (doto (udf/create-registry)
                     (udf/register! descriptor
                       (fn [_]
                         [[:db/add 1 :udf/ok true]])))
        conn       (d/create-conn dir {:udf/ok {:db/valueType :db.type/boolean}})
        store      (.-store ^DB @conn)
        server     (fake-server-with-db-state "orders" {})]
    (try
      (d/transact! conn [{:db/ident :bank/transfer
                          :db/udf   descriptor}])
      (binding [srv/*server-runtime-opts-fn*
                (fn [_ _ _ _]
                  {:udf-registry registry})]
        (#'srv/add-store server "orders" store))
      (let [dt-db  (get-in (.-dbs server) ["orders" :dt-db])
            report (d/with dt-db [[:bank/transfer]])]
        (is (identical? registry (db/udf-registry dt-db)))
        (is (= true (some-> report :tx-data first :v))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest remote-open-existing-datalog-db-does-not-synthesize-schema-txlog-row-test
  (let [dir (u/tmp-dir (str "server-open-existing-db-" (UUID/randomUUID)))
        ^Server server (srv/create {:port 0 :root dir})
        db-name "orders"
        schema {:name {:db/valueType :db.type/string}}
        conn-1-v (volatile! nil)
        conn-2-v (volatile! nil)]
	    (try
	      (srv/start server)
	      (let [port (.getLocalPort (.socket (.-server-socket server)))
	            uri (e2e-db-uri (str "127.0.0.1:" port) db-name)
	            conn-1 (d/create-conn uri
                                  schema
                                  {:client-opts e2e-ha-conn-client-opts})
            _ (vreset! conn-1-v conn-1)
            before-lsn (long (or (:last-applied-lsn
                                  (i/txlog-watermarks
                                   (.-lmdb ^Store (#'srv/get-store server db-name))))
                                 0))
            conn-2 (d/create-conn uri
                                  schema
                                  {:client-opts e2e-ha-conn-client-opts})
            _ (vreset! conn-2-v conn-2)
	            after-lsn (long (or (:last-applied-lsn
	                                 (i/txlog-watermarks
	                                  (.-lmdb ^Store (#'srv/get-store server db-name))))
	                                0))]
	        (is (= before-lsn after-lsn)))
	      (finally
	        (when-let [conn @conn-2-v]
	          (d/close conn))
	        (when-let [conn @conn-1-v]
          (d/close conn))
        (srv/stop server)
        (u/delete-files dir)))))

(deftest ensure-ha-runtime-restarts-on-ha-config-change-test
  (let [root "/srv/dtlv"
        db-name "orders"
        old-ha-opts (valid-ha-opts "ha-restart")
        new-ha-opts (assoc old-ha-opts
                           :ha-client-credentials
                           {:username "ha-replica"
                            :password "secret"})
        old-m {:ha-authority ::old-authority
               :ha-runtime-opts (resolved-ha-runtime-opts
                                 root db-name old-ha-opts)
               :keep :value}
        stopped (atom [])
        started (atom [])]
    (binding [srv/*consensus-ha-opts-fn* (constantly new-ha-opts)
              srv/*stop-ha-renew-loop-fn*
              (fn [m]
                (swap! stopped conj [:renew (:ha-authority m)]))
              srv/*stop-ha-follower-sync-loop-fn*
              (fn [m]
                (swap! stopped conj [:follower (:ha-authority m)]))
              srv/*stop-ha-authority-fn*
              (fn [db-name' m]
                (swap! stopped conj [:authority db-name'
                                     (:ha-authority m)]))
              srv/*start-ha-authority-fn*
              (fn [db-name' ha-opts]
                (swap! started conj [db-name' ha-opts])
                {:ha-authority ::new-authority
                 :ha-role :follower})]
      (let [next-m (#'srv/ensure-ha-runtime root db-name old-m ::store)
            expected-opts (resolved-ha-runtime-opts root db-name new-ha-opts)]
        (is (= [[:renew ::old-authority]
                [:follower ::old-authority]
                [:authority db-name ::old-authority]]
               @stopped))
        (is (= [[db-name expected-opts]]
               @started))
        (is (= ::new-authority (:ha-authority next-m)))
        (is (= :value (:keep next-m)))
        (is (= expected-opts (:ha-runtime-opts next-m)))))))

(deftest ensure-ha-runtime-restarts-when-authority-is-stopped-test
  (let [root "/srv/dtlv"
        db-name "orders"
        ha-opts (valid-ha-opts "ha-stopped")
        runtime (#'srv/start-ha-authority db-name ha-opts)
        authority (:ha-authority runtime)
        state {:ha-authority authority
               :ha-runtime-opts (resolved-ha-runtime-opts
                                 root db-name ha-opts)
               :keep :value}
        stopped (atom [])
        started (atom [])]
    (try
      (ha/stop-authority! authority)
      (binding [srv/*consensus-ha-opts-fn* (constantly ha-opts)
                srv/*stop-ha-renew-loop-fn*
                (fn [m]
                  (swap! stopped conj [:renew (:ha-authority m)]))
                srv/*stop-ha-follower-sync-loop-fn*
                (fn [m]
                  (swap! stopped conj [:follower (:ha-authority m)]))
                srv/*stop-ha-authority-fn*
                (fn [db-name' m]
                  (swap! stopped conj [:authority db-name'
                                       (:ha-authority m)]))
                srv/*start-ha-authority-fn*
                (fn [db-name' ha-opts']
                  (swap! started conj [db-name' ha-opts'])
                  {:ha-authority ::restarted-authority
                   :ha-role :follower})]
        (let [next-m (#'srv/ensure-ha-runtime root db-name state ::store)
              expected-opts (resolved-ha-runtime-opts root db-name ha-opts)]
          (is (= [[:renew authority]
                  [:follower authority]
                  [:authority db-name authority]]
                 @stopped))
          (is (= [[db-name expected-opts]]
                 @started))
          (is (= ::restarted-authority (:ha-authority next-m)))
          (is (= :value (:keep next-m)))
          (is (= expected-opts (:ha-runtime-opts next-m)))))
      (finally
        (when (some? authority)
          (ha/stop-authority! authority))))))

(deftest ensure-ha-runtime-ignores-non-ha-option-change-test
  (let [root "/srv/dtlv"
        db-name "orders"
        ha-opts (valid-ha-opts "ha-stable")
        runtime-ha-opts (resolved-ha-runtime-opts
                         root db-name ha-opts)
        runtime-local-opts (dha/select-ha-runtime-local-opts
                            runtime-ha-opts)
        state {:ha-authority ::existing-authority
               :ha-runtime-opts runtime-ha-opts
               :keep :value}
        stopped (atom [])
        started (atom [])]
    (binding [srv/*consensus-ha-opts-fn*
              (constantly (assoc ha-opts :cache-limit 2048))
              srv/*stop-ha-renew-loop-fn*
              (fn [m]
                (swap! stopped conj [:renew (:ha-authority m)]))
              srv/*stop-ha-follower-sync-loop-fn*
              (fn [m]
                (swap! stopped conj [:follower (:ha-authority m)]))
              srv/*stop-ha-authority-fn*
              (fn [db-name' m]
                (swap! stopped conj [:authority db-name'
                                     (:ha-authority m)]))
              srv/*start-ha-authority-fn*
              (fn [db-name' ha-opts]
                (swap! started conj [db-name' ha-opts])
                {:ha-authority ::unexpected
                 :ha-role :follower})]
      (let [next-m (#'srv/ensure-ha-runtime root db-name state ::store)]
        (is (= (assoc state :ha-runtime-local-opts runtime-local-opts)
               next-m))
        (is (empty? @stopped))
        (is (empty? @started))))))

(deftest ensure-ha-runtime-keeps-node-local-ha-config-when-store-only-has-persisted-opts-test
  (let [root "/srv/dtlv"
        db-name "orders"
        ha-opts (-> (valid-ha-opts "ha-persisted-only")
                    (assoc :ha-client-credentials
                           {:username "ha-replica"
                            :password "secret"}))
        persisted-ha-opts (persisted-ha-runtime-store-opts ha-opts)
        expected-opts (resolved-ha-runtime-opts root db-name ha-opts)
        expected-local-opts (dha/select-ha-runtime-local-opts expected-opts)
        state {:ha-authority ::existing-authority
               :ha-runtime-opts expected-opts
               :keep :value}
        stopped (atom [])
        started (atom [])]
    (binding [srv/*consensus-ha-opts-fn* (constantly persisted-ha-opts)
              srv/*stop-ha-renew-loop-fn*
              (fn [m]
                (swap! stopped conj [:renew (:ha-authority m)]))
              srv/*stop-ha-follower-sync-loop-fn*
              (fn [m]
                (swap! stopped conj [:follower (:ha-authority m)]))
              srv/*stop-ha-authority-fn*
              (fn [db-name' m]
                (swap! stopped conj [:authority db-name'
                                     (:ha-authority m)]))
              srv/*start-ha-authority-fn*
              (fn [db-name' ha-opts']
                (swap! started conj [db-name' ha-opts'])
                {:ha-authority ::unexpected
                 :ha-role :follower})]
      (let [next-m (#'srv/ensure-ha-runtime root db-name state ::store)]
        (is (= (assoc state :ha-runtime-local-opts expected-local-opts)
               next-m))
        (is (empty? @stopped))
        (is (empty? @started))))))

(deftest stop-ha-runtime-preserves-node-local-ha-config-for-restart-test
  (let [root "/srv/dtlv"
        db-name "orders"
        ha-opts (-> (valid-ha-opts "ha-stop-restart")
                    (assoc :ha-client-credentials
                           {:username "ha-replica"
                            :password "secret"}))
        persisted-ha-opts (persisted-ha-runtime-store-opts ha-opts)
        runtime-ha-opts (resolved-ha-runtime-opts root db-name ha-opts)
        runtime-local-opts (dha/select-ha-runtime-local-opts runtime-ha-opts)
        state {:ha-authority ::existing-authority
               :ha-runtime-opts runtime-ha-opts
               :ha-role :follower
               :keep :value}
        stopped (atom [])
        started (atom [])]
    (binding [srv/*consensus-ha-opts-fn* (constantly persisted-ha-opts)
              srv/*stop-ha-renew-loop-fn*
              (fn [m]
                (swap! stopped conj [:renew (:ha-authority m)]))
              srv/*stop-ha-follower-sync-loop-fn*
              (fn [m]
                (swap! stopped conj [:follower (:ha-authority m)]))
              srv/*stop-ha-authority-fn*
              (fn [db-name' m]
                (swap! stopped conj [:authority db-name'
                                     (:ha-authority m)]))
              srv/*start-ha-authority-fn*
              (fn [db-name' ha-opts']
                (swap! started conj [db-name' ha-opts'])
                {:ha-authority ::restarted-authority
                 :ha-role :follower})]
      (let [stopped-state (#'srv/stop-ha-runtime db-name state)]
        (is (= [[:renew ::existing-authority]
                [:follower ::existing-authority]
                [:authority db-name ::existing-authority]]
               @stopped))
        (is (= runtime-local-opts
               (:ha-runtime-local-opts stopped-state)))
        (reset! stopped [])
        (let [next-m (#'srv/ensure-ha-runtime
                      root db-name stopped-state ::store)]
        (is (= [[db-name runtime-ha-opts]]
               @started))
        (is (= runtime-ha-opts (:ha-runtime-opts next-m)))
        (is (= runtime-local-opts
               (:ha-runtime-local-opts next-m)))
        (is (= ::restarted-authority (:ha-authority next-m)))
        (is (= :value (:keep next-m)))
        (is (= [[:renew nil]
                [:follower nil]
                [:authority db-name nil]]
               @stopped)))))))

(deftest add-store-preserves-node-local-ha-runtime-opts-without-publishing-them-in-store-opts-test
  (let [dir (u/tmp-dir (str "server-add-store-ha-open-opts-"
                            (UUID/randomUUID)))
        db-name "orders"
        full-ha-opts (-> (valid-ha-opts "ha-add-store-open-opts")
                         (assoc :db-name db-name
                                :wal? true)
                         (assoc-in [:ha-control-plane :backend]
                                   :sofa-jraft)
                         (assoc-in [:ha-control-plane :rpc-timeout-ms]
                                   5000)
                         (assoc-in [:ha-control-plane :election-timeout-ms]
                                   5000)
                         (assoc-in [:ha-control-plane :operation-timeout-ms]
                                   30000)
                         (assoc :ha-client-credentials
                                {:username "ha-replica"
                                 :password "secret"}))
        persisted-ha-opts (persisted-ha-runtime-store-opts full-ha-opts)
        runtime-ha-opts (resolved-ha-runtime-opts dir db-name full-ha-opts)
        store (st/open (str dir u/+separator+ db-name) nil persisted-ha-opts)
        ^Server server (srv/create {:port 0 :root dir})]
    (try
      (.put (.-dbs server) db-name {:ha-runtime-opts runtime-ha-opts})
      (#'srv/add-store server db-name store false)
      (let [published-state (get (.-dbs server) db-name)
            published-store (:store published-state)]
        (is (false? (i/closed? published-store)))
        (is (= runtime-ha-opts
               (:ha-runtime-opts published-state)))
        (is (= (dha/select-ha-runtime-local-opts runtime-ha-opts)
               (:ha-runtime-local-opts published-state)))
        (is (nil? (:ha-node-id (i/opts published-store))))
        (is (nil? (:ha-client-credentials (i/opts published-store))))
        (is (nil? (get-in (i/opts published-store)
                          [:ha-control-plane :local-peer-id])))
        (is (= (:db-identity full-ha-opts)
               (i/get-value (.-lmdb ^Store published-store)
                            c/opts
                            :db-identity
                            :attr
                            :data))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))

(deftest assoc-opt-rolls-back-store-opts-when-ha-runtime-restart-fails-test
  (let [db-name "orders"
        dir (u/tmp-dir (str "server-assoc-opt-rollback-"
                            (UUID/randomUUID)))
        full-ha-opts (-> (valid-ha-opts "ha-assoc-opt-rollback")
                         (assoc :db-name db-name
                                :wal? true)
                         (assoc-in [:ha-control-plane :backend]
                                   :sofa-jraft)
                         (assoc-in [:ha-control-plane :rpc-timeout-ms]
                                   5000)
                         (assoc-in [:ha-control-plane :election-timeout-ms]
                                   5000)
                         (assoc-in [:ha-control-plane :operation-timeout-ms]
                                   30000))
        store (st/open dir nil (persisted-ha-runtime-store-opts full-ha-opts))
        original-store-opts (i/opts store)
        mutated-drain-ms 2000
        mutated-runtime-opts (resolved-ha-runtime-opts
                              dir db-name
                              (assoc full-ha-opts
                                     :ha-demotion-drain-ms
                                     mutated-drain-ms))
        original-runtime-opts (resolved-ha-runtime-opts
                               dir db-name full-ha-opts)
        state {:ha-authority ::old-authority
               :ha-runtime-opts original-runtime-opts
               :ha-runtime-local-opts
               (dha/select-ha-runtime-local-opts original-runtime-opts)
               :ha-role :follower
               :ha-local-last-applied-lsn 0
               :store store
               :dt-db (db/new-db store)}
        ^Server server (srv/create {:port 0 :root dir})
        started (atom [])]
    (try
      (.put (.-dbs server) db-name state)
      (binding [srv/*stop-ha-renew-loop-fn* (constantly nil)
                srv/*stop-ha-follower-sync-loop-fn* (constantly nil)
                srv/*stop-ha-authority-fn* (constantly nil)
                srv/*start-ha-authority-fn*
                (fn [db-name' ha-opts]
                  (swap! started conj [db-name' ha-opts])
                  (if (= mutated-drain-ms
                         (:ha-demotion-drain-ms ha-opts))
                    (throw (ex-info "boom" {:stage :restart}))
                    {:ha-role :follower}))]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"boom"
             (#'srv/apply-assoc-opt!
              server db-name store false
              :ha-demotion-drain-ms
              mutated-drain-ms)))
        (let [next-state (get (.-dbs server) db-name)
              restored-store (:store next-state)
              restored-records (kv/open-tx-log-rows
                                (.-lmdb ^Store restored-store) 1 128)
              drain-values
              (->> restored-records
                   (mapcat :rows)
                   (keep (fn [[op dbi k v]]
                           (when (and (= op :put)
                                      (= dbi c/opts)
                                      (= k :ha-demotion-drain-ms))
                             v))))]
          (is (= (:ha-demotion-drain-ms original-store-opts)
                 (:ha-demotion-drain-ms (i/opts restored-store))))
          (is (some #(= mutated-drain-ms %) drain-values))
          (is (= (:ha-demotion-drain-ms original-store-opts)
                 (last drain-values)))
          (is (= [db-name mutated-runtime-opts]
                 (first @started)))
          (is (<= 1 (count @started) 2))
          (when (= 2 (count @started))
            (is (= [db-name original-runtime-opts]
                   (second @started))))
          (is (= original-runtime-opts
                 (:ha-runtime-opts next-state)))
          (is (= (dha/select-ha-runtime-local-opts original-runtime-opts)
                 (:ha-runtime-local-opts next-state)))
          (is (= :follower (:ha-role next-state)))
          (let [started-before @started
                txlog-count-before (count restored-records)
                noop-result (#'srv/apply-assoc-opt!
                             server db-name restored-store false
                             :ha-demotion-drain-ms
                             (:ha-demotion-drain-ms original-store-opts))]
            (is (= (:ha-demotion-drain-ms original-store-opts)
                   (:ha-demotion-drain-ms noop-result)))
            (is (= started-before @started))
            (is (= original-runtime-opts
                   (:ha-runtime-opts (get (.-dbs server) db-name))))
            (is (= txlog-count-before
                   (count (kv/open-tx-log-rows
                           (.-lmdb ^Store
                                  (:store (get (.-dbs server) db-name)))
                           1 128)))))))
      (finally
        (srv/stop server)
        (when-let [current-store (:store (get (.-dbs server) db-name))]
          (when-not (identical? current-store store)
            (i/close current-store)))
        (try
          (i/close store)
          (catch Throwable _ nil))
        (u/delete-files dir)))))

(deftest assoc-opt-republishes-runtime-db-after-successful-ha-runtime-restart-test
  (let [db-name "orders"
        dir (u/tmp-dir (str "server-assoc-opt-republish-"
                            (UUID/randomUUID)))
        full-ha-opts (-> (valid-ha-opts "ha-assoc-opt-republish")
                         (assoc :db-name db-name
                                :wal? true)
                         (assoc-in [:ha-control-plane :backend]
                                   :sofa-jraft)
                         (assoc-in [:ha-control-plane :rpc-timeout-ms]
                                   5000)
                         (assoc-in [:ha-control-plane :election-timeout-ms]
                                   5000)
                         (assoc-in [:ha-control-plane :operation-timeout-ms]
                                   30000))
        persisted-ha-opts (persisted-ha-runtime-store-opts full-ha-opts)
        store (st/open dir nil persisted-ha-opts)
        original-runtime-opts (resolved-ha-runtime-opts
                               dir db-name full-ha-opts)
        mutated-drain-ms 2000
        mutated-runtime-opts (resolved-ha-runtime-opts
                              dir db-name
                              (assoc full-ha-opts
                                     :ha-demotion-drain-ms
                                     mutated-drain-ms))
        original-db (db/new-db store)
        state {:ha-authority ::old-authority
               :ha-runtime-opts original-runtime-opts
               :ha-runtime-local-opts
               (dha/select-ha-runtime-local-opts original-runtime-opts)
               :ha-role :follower
               :ha-local-last-applied-lsn 0
               :store store
               :dt-db original-db}
        ^Server server (srv/create {:port 0 :root dir})
        started (atom [])
        stopped (atom [])]
    (try
      (.put (.-dbs server) db-name state)
      (binding [srv/*stop-ha-renew-loop-fn*
                (fn [m]
                  (swap! stopped conj [:renew (:ha-authority m)]))
                srv/*stop-ha-follower-sync-loop-fn*
                (fn [m]
                  (swap! stopped conj [:follower (:ha-authority m)]))
                srv/*stop-ha-authority-fn*
                (fn [db-name' m]
                  (swap! stopped conj [:authority db-name'
                                       (:ha-authority m)]))
                srv/*start-ha-authority-fn*
                (fn [db-name' ha-opts]
                  (swap! started conj [db-name' ha-opts])
                  {:ha-authority ::new-authority
                   :ha-role :follower})]
        (#'srv/apply-assoc-opt! server db-name store false
                                :ha-demotion-drain-ms
                                mutated-drain-ms)
        (let [next-state (get (.-dbs server) db-name)
              next-store ^Store (:store next-state)
              next-db ^DB (:dt-db next-state)
              report (d/with next-db [{:db/ident :orders/status}])]
          (is (= [[:renew ::old-authority]
                  [:follower ::old-authority]
                  [:authority db-name ::old-authority]]
                 (take 3 @stopped)))
          (is (= [[db-name mutated-runtime-opts]]
                 @started))
          (is (= mutated-drain-ms
                 (:ha-demotion-drain-ms (i/opts next-store))))
          (is (= mutated-runtime-opts
                 (:ha-runtime-opts next-state)))
          (is (= (dha/select-ha-runtime-local-opts mutated-runtime-opts)
                 (:ha-runtime-local-opts next-state)))
          (is (instance? DB next-db))
          (is (not (identical? original-db next-db)))
          (is (identical? next-store (.-store next-db)))
          (is (some #(= :orders/status (:v %))
                    (:tx-data report)))))
      (finally
        (srv/stop server)
        (when-let [current-store (:store (get (.-dbs server) db-name))]
          (when-not (identical? current-store store)
            (i/close current-store)))
        (try
          (i/close store)
          (catch Throwable _ nil))
        (u/delete-files dir)))))

(deftest ha-follower-sync-loop-merges-side-effect-state-after-concurrent-db-update-test
  (let [db-name "orders"
        running? (AtomicBoolean. true)
        initial-state {:ha-authority ::authority
                       :ha-db-identity db-name
                       :ha-membership-hash "membership-v1"
                       :ha-node-id 1
                       :ha-follower-loop-running? running?
                       :ha-role :follower
                       :ha-local-last-applied-lsn 0
                       :marker :initial}
        ^Server server (fake-server-with-db-state db-name initial-state)
        stopped-latch (java.util.concurrent.CountDownLatch. 1)
        sync-started (promise)
        allow-sync (promise)
        sync-future (atom nil)
        updater-future (atom nil)]
    (try
      (.set ^AtomicBoolean (.-running server) true)
      (binding [srv/*ha-follower-sync-step-fn*
                (fn [_ m]
                  (deliver sync-started true)
                  (deref allow-sync 1000 ::timeout)
                  (assoc m
                         :ha-local-last-applied-lsn 7
                         :ha-follower-next-lsn 8
                         :ha-follower-last-sync-ms 42
                         :ha-follower-last-error nil
                         :marker :stale-sync-state))]
        (reset! sync-future
                (future (#'srv/run-ha-follower-sync-loop
                         server db-name running? stopped-latch)))
        (is (true? (deref sync-started 1000 false)))
        (reset! updater-future
                (future
                  (#'srv/update-db server db-name
                                   #(assoc % :concurrent? true
                                           :marker :concurrent-update))))
        (deliver allow-sync true)
        (is (not= ::timeout (deref @updater-future 1000 ::timeout)))
        (.set running? false)
        (let [deadline (+ (System/currentTimeMillis) 1000)]
          (loop [state (get (.-dbs server) db-name)]
            (if (or (= 7 (:ha-local-last-applied-lsn state))
                    (>= (System/currentTimeMillis) deadline))
              (do
                (is (= 7 (:ha-local-last-applied-lsn state)))
                (is (= 8 (:ha-follower-next-lsn state)))
                (is (= 42 (:ha-follower-last-sync-ms state)))
                (is (:concurrent? state))
                (is (= :concurrent-update (:marker state))))
              (do
                (Thread/sleep 10)
                (recur (get (.-dbs server) db-name))))))
        (let [state (get (.-dbs server) db-name)]
          (is (= 7 (:ha-local-last-applied-lsn state)))
          (is (= 8 (:ha-follower-next-lsn state)))
          (is (= 42 (:ha-follower-last-sync-ms state)))
          (is (:concurrent? state))
          (is (= :concurrent-update (:marker state)))))
      (finally
        (.set ^AtomicBoolean (.-running server) false)
        (.set running? false)
        (when-let [f @sync-future]
          (future-cancel f))
        (when-let [f @updater-future]
          (future-cancel f))))))

(deftest ha-follower-sync-loop-persists-follower-progress-before-cas-race-test
  (let [db-name "orders"
        dir (u/tmp-dir (str "ha-renew-persist-race-" (UUID/randomUUID)))
        store (st/open dir nil {:db-name db-name
                                :db-identity (str "db-" (UUID/randomUUID))
                                :wal? true})
        running? (AtomicBoolean. true)
        initial-state {:store store
                       :ha-authority ::authority
                       :ha-db-identity (:db-identity (i/opts store))
                       :ha-membership-hash "membership-v1"
                       :ha-node-id 1
                       :ha-follower-loop-running? running?
                       :ha-role :follower
                       :ha-local-last-applied-lsn 0
                       :marker :initial}
        ^Server server (fake-server-with-db-state db-name initial-state)
        stopped-latch (java.util.concurrent.CountDownLatch. 1)
        sync-started (promise)
        allow-sync (promise)
        sync-future (atom nil)
        updater-future (atom nil)]
    (try
      (.set ^AtomicBoolean (.-running server) true)
      (binding [srv/*ha-follower-sync-step-fn*
                (fn [_ m]
                  (deliver sync-started true)
                  (deref allow-sync 1000 ::timeout)
                  (i/transact-kv (.-lmdb ^Store (:store m))
                                 c/kv-info
                                 [[:put c/wal-snapshot-current-lsn 7]]
                                 :keyword :data)
                  (assoc m
                         :ha-local-last-applied-lsn 7
                         :ha-follower-next-lsn 8
                         :ha-follower-last-sync-ms 42
                         :marker :stale-sync-state))]
        (reset! sync-future
                (future (#'srv/run-ha-follower-sync-loop
                         server db-name running? stopped-latch)))
        (is (true? (deref sync-started 1000 false)))
        (reset! updater-future
                (future
                  (#'srv/update-db server db-name
                                   #(assoc % :ha-role :leader
                                           :marker :concurrent-update))))
        (deliver allow-sync true)
        (is (not= ::timeout (deref @updater-future 1000 ::timeout)))
        (let [deadline (+ (System/currentTimeMillis) 1000)]
          (loop []
            (let [persisted-lsn
                  (long (or (i/get-value (.-lmdb ^Store store)
                                         c/kv-info
                                         c/ha-local-applied-lsn
                                         :keyword :data)
                            0))]
              (if (or (= 7 persisted-lsn)
                      (>= (System/currentTimeMillis) deadline))
                (is (= 7 persisted-lsn))
                (do
                  (Thread/sleep 10)
                  (recur))))))
        (.set running? false)
        (let [deadline (+ (System/currentTimeMillis) 1000)]
          (loop [state (get (.-dbs server) db-name)]
            (if (or (and (= :leader (:ha-role state))
                         (= :concurrent-update (:marker state)))
                    (>= (System/currentTimeMillis) deadline))
              (do
                (is (= :leader (:ha-role state)))
                (is (= :concurrent-update (:marker state)))
                (is (= 7 (dha/read-ha-local-last-applied-lsn state))))
              (do
                (Thread/sleep 10)
                (recur (get (.-dbs server) db-name)))))))
      (finally
        (.set ^AtomicBoolean (.-running server) false)
        (.set running? false)
        (when-let [f @sync-future]
          (future-cancel f))
        (when-let [f @updater-future]
          (future-cancel f))
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))

(deftest ha-renew-loop-progresses-while-follower-sync-is-blocked-test
  (let [db-name "orders"
        renew-running? (AtomicBoolean. true)
        follower-running? (AtomicBoolean. true)
        initial-state {:ha-authority ::authority
                       :ha-db-identity db-name
                       :ha-membership-hash "membership-v1"
                       :ha-node-id 1
                       :ha-renew-loop-running? renew-running?
                       :ha-follower-loop-running? follower-running?
                       :ha-role :follower
                       :ha-local-last-applied-lsn 0
                       :marker :initial}
        ^Server server (fake-server-with-db-state db-name initial-state)
        sync-stopped-latch (java.util.concurrent.CountDownLatch. 1)
        renew-stopped-latch (java.util.concurrent.CountDownLatch. 1)
        sync-started (promise)
        allow-sync (promise)
        renew-started (promise)
        sync-future (atom nil)
        renew-future (atom nil)]
    (try
      (.set ^AtomicBoolean (.-running server) true)
      (binding [srv/*ha-follower-sync-step-fn*
                (fn [_ m]
                  (deliver sync-started true)
                  (deref allow-sync 1000 ::timeout)
                  (assoc m :ha-follower-last-sync-ms 42))
                srv/*ha-renew-step-fn*
                (fn [_ m]
                  (deliver renew-started true)
                  (.set renew-running? false)
                  (assoc m
                         :ha-last-authority-refresh-ms 99
                         :marker :renewed))]
        (reset! sync-future
                (future
                  (#'srv/run-ha-follower-sync-loop
                   server db-name follower-running? sync-stopped-latch)))
        (is (true? (deref sync-started 1000 false)))
        (reset! renew-future
                (future
                  (#'srv/run-ha-renew-loop
                   server db-name renew-running? renew-stopped-latch)))
        (is (true? (deref renew-started 1000 false)))
        (let [deadline (+ (System/currentTimeMillis) 1000)]
          (loop []
            (let [state (get (.-dbs server) db-name)]
              (if (or (= :renewed (:marker state))
                      (>= (System/currentTimeMillis) deadline))
                (do
                  (is (= :renewed (:marker state)))
                  (is (= 99 (:ha-last-authority-refresh-ms state))))
                (do
                  (Thread/sleep 10)
                  (recur))))))
        (deliver allow-sync true)
        (let [deadline (+ (System/currentTimeMillis) 1000)]
          (loop []
            (let [state (get (.-dbs server) db-name)]
              (if (or (= 42 (:ha-follower-last-sync-ms state))
                      (>= (System/currentTimeMillis) deadline))
                (is (= 42 (:ha-follower-last-sync-ms state)))
                (do
                  (Thread/sleep 10)
                  (recur)))))))
      (finally
        (.set ^AtomicBoolean (.-running server) false)
        (.set renew-running? false)
        (.set follower-running? false)
        (deliver allow-sync true)
        (when-let [f @sync-future]
          (future-cancel f))
        (when-let [f @renew-future]
          (future-cancel f))))))

(deftest ha-renew-loop-retries-after-transient-step-error-test
  (let [db-name "orders"
        running? (AtomicBoolean. true)
        initial-state {:ha-authority ::authority
                       :ha-db-identity db-name
                       :ha-membership-hash "membership-v1"
                       :ha-node-id 1
                       :ha-renew-loop-running? running?
                       :ha-role :follower
                       :marker :initial}
        ^Server server (fake-server-with-db-state db-name initial-state)
        stopped-latch (java.util.concurrent.CountDownLatch. 1)
        calls (atom 0)
        recovered (promise)
        renew-future (atom nil)]
    (try
      (.set ^AtomicBoolean (.-running server) true)
      (binding [srv/*ha-renew-step-fn*
                (fn [_ m]
                  (let [n (swap! calls inc)]
                    (if (= 1 n)
                      (throw (ex-info "boom" {:stage :renew}))
                      (do
                        (.set running? false)
                        (deliver recovered true)
                        (assoc m :marker :renew-recovered)))))]
        (reset! renew-future
                (future
                  (#'srv/run-ha-renew-loop
                   server db-name running? stopped-latch)))
        (is (true? (deref recovered 2000 false)))
        (is (>= @calls 2))
        (is (= :renew-recovered
               (:marker (get (.-dbs server) db-name)))))
      (finally
        (.set ^AtomicBoolean (.-running server) false)
        (.set running? false)
        (when-let [f @renew-future]
          (future-cancel f))))))

(deftest ha-follower-sync-loop-retries-after-transient-step-error-test
  (let [db-name "orders"
        running? (AtomicBoolean. true)
        initial-state {:ha-authority ::authority
                       :ha-db-identity db-name
                       :ha-membership-hash "membership-v1"
                       :ha-node-id 1
                       :ha-follower-loop-running? running?
                       :ha-role :follower
                       :marker :initial}
        ^Server server (fake-server-with-db-state db-name initial-state)
        stopped-latch (java.util.concurrent.CountDownLatch. 1)
        calls (atom 0)
        recovered (promise)
        sync-future (atom nil)]
    (try
      (.set ^AtomicBoolean (.-running server) true)
      (binding [srv/*ha-follower-sync-step-fn*
                (fn [_ m]
                  (let [n (swap! calls inc)]
                    (if (= 1 n)
                      (throw (ex-info "boom" {:stage :sync}))
                      (do
                        (.set running? false)
                        (deliver recovered true)
                        (assoc m :marker :sync-recovered)))))]
        (reset! sync-future
                (future
                  (#'srv/run-ha-follower-sync-loop
                   server db-name running? stopped-latch)))
        (is (true? (deref recovered 2000 false)))
        (is (>= @calls 2))
        (is (= :sync-recovered
               (:marker (get (.-dbs server) db-name)))))
      (finally
        (.set ^AtomicBoolean (.-running server) false)
        (.set running? false)
        (when-let [f @sync-future]
          (future-cancel f))))))
