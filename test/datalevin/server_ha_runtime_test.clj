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
        (is (nil? (:ha-renew-loop-future state))))
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
        state {:ha-authority ::existing-authority
               :ha-runtime-opts (resolved-ha-runtime-opts
                                 root db-name ha-opts)
               :keep :value}
        stopped (atom [])
        started (atom [])]
    (binding [srv/*consensus-ha-opts-fn*
              (constantly (assoc ha-opts :cache-limit 2048))
              srv/*stop-ha-renew-loop-fn*
              (fn [m]
                (swap! stopped conj [:renew (:ha-authority m)]))
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
        (is (= state next-m))
        (is (empty? @stopped))
        (is (empty? @started))))))

(deftest ha-renew-loop-merges-side-effect-state-after-concurrent-db-update-test
  (let [db-name "orders"
        running? (AtomicBoolean. true)
        initial-state {:ha-authority ::authority
                       :ha-db-identity db-name
                       :ha-membership-hash "membership-v1"
                       :ha-node-id 1
                       :ha-renew-loop-running? running?
                       :ha-role :follower
                       :ha-local-last-applied-lsn 0
                       :marker :initial}
        ^Server server (fake-server-with-db-state db-name initial-state)
        renew-started (promise)
        allow-renew (promise)
        renew-future (atom nil)
        updater-future (atom nil)]
    (try
      (.set ^AtomicBoolean (.-running server) true)
      (binding [srv/*ha-renew-step-fn*
                (fn [_ m]
                  (deliver renew-started true)
                  (deref allow-renew 1000 ::timeout)
                  (assoc m
                         :ha-local-last-applied-lsn 7
                         :ha-follower-next-lsn 8
                         :ha-follower-last-sync-ms 42
                         :ha-follower-last-error nil
                         :marker :stale-renew-state))]
        (reset! renew-future
                (future (#'srv/run-ha-renew-loop server db-name running?)))
        (is (true? (deref renew-started 1000 false)))
        (reset! updater-future
                (future
                  (#'srv/update-db server db-name
                                   #(assoc % :concurrent? true
                                           :marker :concurrent-update))))
        (deliver allow-renew true)
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
        (when-let [f @renew-future]
          (future-cancel f))
        (when-let [f @updater-future]
          (future-cancel f))))))

(deftest ha-renew-loop-persists-follower-progress-before-cas-race-test
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
                       :ha-renew-loop-running? running?
                       :ha-role :follower
                       :ha-local-last-applied-lsn 0
                       :marker :initial}
        ^Server server (fake-server-with-db-state db-name initial-state)
        renew-started (promise)
        allow-renew (promise)
        renew-future (atom nil)
        updater-future (atom nil)]
    (try
      (.set ^AtomicBoolean (.-running server) true)
      (binding [srv/*ha-renew-step-fn*
                (fn [_ m]
                  (deliver renew-started true)
                  (deref allow-renew 1000 ::timeout)
                  (assoc m
                         :ha-local-last-applied-lsn 7
                         :ha-follower-next-lsn 8
                         :ha-follower-last-sync-ms 42
                         :marker :stale-renew-state))]
        (reset! renew-future
                (future (#'srv/run-ha-renew-loop server db-name running?)))
        (is (true? (deref renew-started 1000 false)))
        (reset! updater-future
                (future
                  (#'srv/update-db server db-name
                                   #(assoc % :ha-role :leader
                                           :marker :concurrent-update))))
        (deliver allow-renew true)
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
        (let [state (get (.-dbs server) db-name)]
          (is (= :leader (:ha-role state)))
          (is (= :concurrent-update (:marker state)))
          (is (= 7 (dha/read-ha-local-last-applied-lsn state)))))
      (finally
        (.set ^AtomicBoolean (.-running server) false)
        (.set running? false)
        (when-let [f @renew-future]
          (future-cancel f))
        (when-let [f @updater-future]
          (future-cancel f))
        (when-not (i/closed? store)
          (i/close store))
        (u/delete-files dir)))))
