(ns datalevin.test.conn
  (:require
   [clojure.string :as str]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.binding.cpp :as cpp]
   [datalevin.conn :as dc]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.interface :as i]
   [datalevin.constants :as c]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as lmdb]
   [datalevin.storage :as s]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
   [java.nio ByteBuffer]
   [java.util Arrays Date UUID]))

(use-fixtures :each db-fixture)

(defn- conn-store
  [conn]
  (.-store ^DB @conn))

(defn- conn-env-opts
  [conn]
  (i/env-opts (.-lmdb ^Store (conn-store conn))))

(defn- txlog-ops
  [db]
  (mapcat :ops (kv/open-tx-log db 1)))

(defn- txlog-dbi-registration-op
  [ops op dbi-name]
  (some (fn [row]
          (when (and (= op (nth row 0 nil))
                     (= c/kv-info (nth row 1 nil))
                     (= [:dbis dbi-name] (nth row 2 nil)))
            row))
        ops))

(defn- copy-snapshot-files-to-env!
  [snapshot-path env-dir]
  (doseq [^java.io.File f (or (u/list-files snapshot-path) [])
          :when (.isFile f)
          :when (not= "snapshot.edn" (.getName f))]
    (u/copy-file (.getPath f)
                 (str env-dir u/+separator+ (.getName f)))))

(defn- first-wal-segment-path
  [txlog-dir]
  (->> (or (u/list-files txlog-dir) [])
       (map #(.getPath ^java.io.File %))
       (filter #(str/ends-with? % ".wal"))
       sort
       first))

(defn- test-ha-opts
  []
  (let [group-id    (str "test-ha-group-" (UUID/randomUUID))
        db-identity (str "test-ha-db-" (UUID/randomUUID))
        members     [{:node-id 1 :endpoint "127.0.0.1:19001"}
                     {:node-id 2 :endpoint "127.0.0.1:19002"}
                     {:node-id 3 :endpoint "127.0.0.1:19003"}]
        voters      [{:peer-id "127.0.0.1:19101"
                      :ha-node-id 1
                      :promotable? true}
                     {:peer-id "127.0.0.1:19102"
                      :ha-node-id 2
                      :promotable? true}
                     {:peer-id "127.0.0.1:19103"
                      :ha-node-id 3
                      :promotable? true}]]
    {:db-identity db-identity
     :ha-mode :consensus-lease
     :ha-lease-renew-ms 1000
     :ha-lease-timeout-ms 5000
     :ha-promotion-base-delay-ms 100
     :ha-promotion-rank-delay-ms 200
     :ha-max-promotion-lag-lsn 0
     :ha-clock-skew-budget-ms 1000
     :ha-members members
     :ha-control-plane {:backend :sofa-jraft
                        :group-id group-id
                        :voters voters
                        :rpc-timeout-ms 1000
                        :election-timeout-ms 1000
                        :operation-timeout-ms 1000}}))

(deftest test-datalog-wal-default-is-opt-in
  (let [dir  (u/tmp-dir (str "test-datalog-wal-default-"
                             (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (try
      (is (false? (:wal? (conn-env-opts conn))))
      (is (false? (:wal? (i/opts (conn-store conn)))))
      (is (not (u/file-exists (str dir u/+separator+ "txlog"))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest new-db-does-not-enqueue-empty-secondary-index-work-test
  (let [dir      (u/tmp-dir (str "test-empty-secondary-index-work-"
                                 (UUID/randomUUID)))
        enqueued (atom 0)]
    (try
      (with-redefs [s/enqueue-secondary-index-work!
                    (fn [store]
                      (swap! enqueued inc)
                      store)]
        (let [conn (d/create-conn dir)]
          (try
            (is (zero? @enqueued))
            (finally
              (d/close conn)))))
      (finally
        (u/delete-files dir)))))

(deftest test-datalog-wal-opt-in-defaults-to-relaxed
  (let [dir  (u/tmp-dir (str "test-datalog-wal-relaxed-"
                             (UUID/randomUUID)))
        conn (d/create-conn dir nil {:wal? true})]
    (try
      (is (true? (:wal? (conn-env-opts conn))))
      (is (= :relaxed (:wal-durability-profile (conn-env-opts conn))))
      (is (true? (:wal? (i/opts (conn-store conn)))))
      (is (= :relaxed
             (:wal-durability-profile (i/opts (conn-store conn)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-datalog-kv-exposes-backing-kv-handle
  (let [dir  (u/tmp-dir (str "test-datalog-kv-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (try
      (let [kv (d/datalog-kv conn)]
        (is (= dir (d/dir kv)))
        (d/open-dbi kv "app-state")
        (d/transact-kv kv "app-state" [[:put "k" "v"]] :string :string)
        (is (= "v" (d/get-value kv "app-state" "k" :string :string true)))
        (is (= kv (d/datalog-kv @conn))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-kv-wal-opt-in-defaults-to-relaxed
  (let [dir (u/tmp-dir (str "test-kv-wal-relaxed-"
                            (UUID/randomUUID)))
        db  (d/open-kv dir {:wal? true})]
    (try
      (is (= :relaxed (:durability-profile (d/txlog-watermarks db))))
      (finally
        (d/close-kv db)
        (u/delete-files dir)))))

(deftest test-kv-wal-logs-dbi-lifecycle
  (let [dir  (u/tmp-dir (str "test-kv-wal-dbi-lifecycle-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-durability-profile :strict
              :snapshot-bootstrap-force? false}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "live" {:key-size 64})
          (d/drop-dbi db "live")
          (kv/force-txlog-sync! db)
          (let [ops    (txlog-ops db)
                put-op (txlog-dbi-registration-op ops :put "live")
                del-op (txlog-dbi-registration-op ops :del "live")]
            (is (some? put-op))
            (is (= [:keyword :string] (nth put-op 4)))
            (is (= :data (nth put-op 5)))
            (is (some? del-op))
            (is (= [:keyword :string] (nth del-op 3))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-datalog-ha-forces-safe-wal
  (let [dir  (u/tmp-dir (str "test-datalog-ha-wal-"
                             (UUID/randomUUID)))
        conn (d/create-conn dir nil (test-ha-opts))]
    (try
      (is (true? (:wal? (conn-env-opts conn))))
      (is (= :strict (:wal-durability-profile (conn-env-opts conn))))
      (is (true? (:wal? (i/opts (conn-store conn)))))
      (is (= :strict
             (:wal-durability-profile (i/opts (conn-store conn)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-datalog-ha-reopen-persists-wal-runtime-opts-for-kv-open
  (let [dir (u/tmp-dir (str "test-datalog-ha-wal-reopen-"
                            (UUID/randomUUID)))]
    (try
      (let [conn (d/create-conn dir)]
        (d/close conn))
      (let [conn (d/create-conn dir nil (test-ha-opts))]
        (try
          (is (true? (:wal? (conn-env-opts conn))))
          (is (= :strict (:wal-durability-profile (conn-env-opts conn))))
          (finally
            (d/close conn))))
      (let [db (d/open-kv dir)]
        (try
          (is (true? (i/get-value db c/kv-info :wal? :keyword :data)))
          (is (= :strict
                 (i/get-value db c/kv-info :wal-durability-profile
                              :keyword :data)))
          (is (true? (:wal? (kv/txlog-watermarks db))))
          (is (= :strict (:durability-profile (kv/txlog-watermarks db))))
          (is (vector? (vec (kv/open-tx-log-rows db 1 1))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-datalog-ha-rejects-relaxed-wal
  (let [dir (u/tmp-dir (str "test-datalog-ha-relaxed-"
                            (UUID/randomUUID)))]
    (try
      (is (thrown-with-msg?
            Exception
            #"Consensus-lease HA requires :wal-durability-profile :strict or :extra"
            (d/create-conn dir nil
                           (assoc (test-ha-opts)
                                  :wal-durability-profile :relaxed))))
      (finally
        (u/delete-files dir)))))

(deftest test-close
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (is (not (d/closed? conn)))
    (d/close conn)
    (is (d/closed? conn))
    (is (nil? @conn))
    (u/delete-files dir)))

(deftest test-update-schema
  (let [dir1  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        dir2  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn1 (d/create-conn dir1)
        aid0   (count c/implicit-schema)
        s     {:a/b {:db/valueType :db.type/string}}
        s1    {:c/d {:db/valueType :db.type/string}}
        txs   [{:c/d "cd" :db/id -1}
               {:a/b "ab" :db/id -2}]
        conn2 (d/create-conn dir2 s)]
    (is (= (d/schema conn2) (d/update-schema conn1 s)))
    (d/update-schema conn1 s1)
    (is (= (d/schema conn1) (-> (merge c/implicit-schema s s1)
                                (assoc-in [:a/b :db/aid] aid0)
                                (assoc-in [:c/d :db/aid] (inc aid0)))))
    (d/transact! conn1 txs)
    (is (= 2 (count (d/datoms @conn1 :eav))))

    (is (thrown-with-msg? Exception #"Cannot delete attribute"
                          (d/update-schema conn1 {} #{:c/d})))

    (d/transact! conn1 [[:db/retractEntity 1]])
    (is (= (d/schema conn2)
           (d/update-schema conn1 {} #{:c/d})
           (d/schema conn1)))

    (d/update-schema conn1 nil nil {:a/b :e/f})
    (is (= (d/schema conn1) (assoc c/implicit-schema :e/f
                                   {:db/valueType :db.type/string
                                    :db/aid       aid0})))

    (d/close conn1)
    (d/close conn2)
    (u/delete-files dir1)
    (u/delete-files dir2)))

(deftest test-update-schema-1
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)
        aid0 (count c/implicit-schema)]
    (d/update-schema conn {:things {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:things :db/aid] aid0))))
    (d/update-schema conn {:stuff {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:things :db/aid] aid0)
                               (assoc-in [:stuff :db/aid] (inc aid0)))))
    (d/update-schema conn {} [:things])
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:stuff :db/aid] (inc aid0)))))
    (d/update-schema conn {:things {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:stuff :db/aid] (inc aid0))
                               (assoc-in [:things :db/aid] (+ aid0 2)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-update-schema-ensure-no-duplicate-aids
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (d/update-schema conn {:up/a {}})
    (d/transact! conn [{:foo 1}])
    (let [aids (map :db/aid (vals (d/schema conn)))]
      (is (= (count aids) (count (set aids))))
      (d/close conn)
      (u/delete-files dir))))

(deftest test-update-schema-validates-new-attrs
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    ;; :db/isComponent true requires :db/valueType :db.type/ref
    (is (thrown-with-msg?
          Exception #"isComponent.*should also have.*ref"
          (d/update-schema conn {:bad/attr {:db/isComponent true
                                            :db/valueType   :db.type/string}})))
    ;; invalid :db/valueType
    (is (thrown-with-msg?
          Exception #"Bad attribute specification"
          (d/update-schema conn {:bad/attr {:db/valueType :db.type/bogus}})))
    ;; invalid :db/cardinality
    (is (thrown-with-msg?
          Exception #"Bad attribute specification"
          (d/update-schema conn {:bad/attr {:db/cardinality :db.cardinality/bogus}})))
    ;; valid schema still works
    (d/update-schema conn {:good/attr {:db/valueType :db.type/string}})
    (is (:good/attr (d/schema conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-live-idoc-schema-initializes-index-and-logs-dbis
  (let [dir  (u/tmp-dir (str "test-live-idoc-schema-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-durability-profile :strict
              :snapshot-bootstrap-force? false}
        conn (d/create-conn dir nil opts)]
    (try
      (d/update-schema conn
                       {:doc/idoc {:db/valueType :db.type/idoc
                                   :db/domain "profiles"}})
      (d/transact! conn [{:db/id 1
                          :doc/idoc {:status "active"}}])
      (is (= {:status "active"}
             (:doc/idoc (d/entity @conn 1))))
      (let [lmdb (.-lmdb ^Store (conn-store conn))
            ops  (txlog-ops lmdb)]
        (is (some? (txlog-dbi-registration-op ops
                                              :put
                                              "profiles/doc-ref")))
        (is (some? (txlog-dbi-registration-op ops
                                              :put
                                              "profiles/doc-index")))
        (is (some? (txlog-dbi-registration-op ops
                                              :put
                                              "profiles/path-dict"))))
      (d/close conn)
      (let [conn2 (d/create-conn dir nil opts)]
        (try
          (is (= {:status "active"}
                 (:doc/idoc (d/entity @conn2 1))))
          (finally
            (d/close conn2))))
      (finally
        (when-not (d/closed? conn)
          (d/close conn))
        (u/delete-files dir)))))

(deftest test-ways-to-create-conn-1
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (is (= #{} (set (d/datoms @conn :eav))))
    (is (= c/implicit-schema (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-ways-to-create-conn-2
  (let [schema { :aka { :db/cardinality :db.cardinality/many
                        :db/aid         (count c/implicit-schema)}}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/create-conn dir schema)]
    (is (= #{} (set (d/datoms @conn :eav))))
    (is (= (db/-schema @conn) (merge schema c/implicit-schema)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-ways-to-create-conn-3
  (let [datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-datoms datoms dir)]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [schema { :aka { :db/cardinality :db.cardinality/many
                        :db/aid         (count c/implicit-schema)}}
        datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-datoms datoms dir schema)]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-db (d/init-db datoms dir))]
    (is (thrown-with-msg? Exception
                          #"init-db expects list of Datoms, got "
                          (d/init-db [[:add -1 :name "Ivan"]
                                      {:add -1 :age 35}])))
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [schema { :aka { :db/cardinality :db.cardinality/many
                        :db/aid         (count c/implicit-schema)}}
        datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")
                 (d/datom 1 :aka "danger")
                 (d/datom 1 :aka "fun")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-db (-> (d/empty-db dir schema)
                                   (d/fill-db datoms)))]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-recreate-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (u/tmp-dir (str "recreate-conn-test-" (UUID/randomUUID)))
        conn   (d/create-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/create-conn dir schema)]
      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eav))))
      (d/close conn2))
    (u/delete-files dir)))

(deftest test-open-kv-enables-virtual-thread-safe-reader-slots
  (let [dir (u/tmp-dir (str "open-kv-flags-test-" (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir)]
        (try
          (is (= c/default-env-flags (d/get-env-flags db)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-kv-without-value-compression
  (let [dir (u/tmp-dir (str "with-tx-kv-no-compress-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir {:wal? false})]
        (try
          (d/open-dbi db "a")
          (let [tx (i/open-transact-kv db)]
            (try
              (is (= :transacted
                     (d/transact-kv tx [[:put "a" :k :v]])))
              (is (= :v (d/get-value tx "a" :k)))
              (is (nil? (d/get-value db "a" :k)))
              (finally
                (i/close-transact-kv db))))
          (is (= :v (d/get-value db "a" :k)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-kv-aborts-on-exception
  (let [dir (u/tmp-dir (str "with-tx-kv-abort-on-error-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir {:wal? false})]
        (try
          (d/open-dbi db "a")
          (is (thrown-with-msg?
               Exception
               #"boom"
               (d/with-transaction-kv [tx db]
                 (d/transact-kv tx [[:put "a" :k :v]])
                 (throw (ex-info "boom" {})))))
          (is (nil? (d/get-value db "a" :k)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :after :ok]])))
          (is (= :ok (d/get-value db "a" :after)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-kv-evaluates-db-once
  (let [dir (u/tmp-dir (str "with-tx-kv-eval-once-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db    (d/open-kv dir {:wal? false})
            calls (atom 0)
            get-db
            (fn []
              (swap! calls inc)
              db)]
        (try
          (d/open-dbi db "a")
          (is (= 1
                 (d/with-transaction-kv [tx (get-db)]
                   (d/transact-kv tx [[:put "a" :k 1]])
                   (d/get-value tx "a" :k))))
          (is (= 1 @calls))
          (is (= 1 (d/get-value db "a" :k)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-aborts-on-exception
  (let [dir    (u/tmp-dir (str "with-tx-abort-on-error-test-"
                               (UUID/randomUUID)))
        schema {:name {:db/valueType :db.type/string}}]
    (try
      (let [conn (d/create-conn dir schema {:wal? false})]
        (try
          (is (thrown-with-msg?
               Exception
               #"boom"
               (d/with-transaction [tx conn]
                 (d/transact! tx [{:db/id 1 :name "partial"}])
                 (throw (ex-info "boom" {})))))
          (is (empty?
               (d/q '[:find [?e ...]
                      :where [?e :name "partial"]]
                    @conn)))
          (d/transact! conn [{:db/id 2 :name "after"}])
          (is (= #{2}
                 (set (d/q '[:find [?e ...]
                             :where [?e :name "after"]]
                           @conn))))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-evaluates-conn-once
  (let [dir    (u/tmp-dir (str "with-tx-eval-once-test-"
                               (UUID/randomUUID)))
        schema {:counter {:db/valueType :db.type/long}}]
    (try
      (let [conn  (d/create-conn dir schema {:wal? false})
            calls (atom 0)
            get-conn
            (fn []
              (swap! calls inc)
              conn)]
        (try
          (is (= 1
                 (d/with-transaction [tx (get-conn)]
                   (d/transact! tx [{:db/id 1 :counter 1}])
                   (d/q '[:find ?c .
                          :where [1 :counter ?c]]
                        @tx))))
          (is (= 1 @calls))
          (is (= 1
                 (d/q '[:find ?c .
                        :where [1 :counter ?c]]
                      @conn)))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files dir)))))

(deftest test-open-kv-rejects-second-local-handle-in-process
  (let [dir (u/tmp-dir (str "open-kv-duplicate-handle-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db1 (d/open-kv dir)]
        (try
          (d/open-dbi db1 "a")
          (d/transact-kv db1 [[:put "a" :k :v]])
          (let [result (try
                         (let [db2 (d/open-kv dir)]
                           (try
                             {:status :opened}
                             (finally
                               (d/close-kv db2))))
                         (catch Exception e
                           {:status  :error
                            :message (ex-message e)}))]
            (is (= :error (:status result)) result)
            (is (re-find #"Please do not open multiple LMDB connections"
                         (:message result)))
            (is (= :v
                   (d/get-value db1 "a" :k))))
          (finally
            (d/close-kv db1))))
      (let [db2 (d/open-kv dir)]
        (try
          (d/open-dbi db2 "a")
          (is (= :v
                 (d/get-value db2 "a" :k)))
          (finally
            (d/close-kv db2))))
      (finally
        (u/delete-files dir)))))

(deftest test-get-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (u/tmp-dir (str "get-conn-test-" (UUID/randomUUID)))
        conn   (d/get-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/get-conn dir schema)]
      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eav))))
      (d/close conn2))
    (u/delete-files dir)))

(deftest test-get-conn-existing-store-opens-lmdb-once
  (let [schema {:name {:db/valueType :db.type/string}}
        dir    (u/tmp-dir (str "get-conn-open-once-test-" (UUID/randomUUID)))]
    (try
      (let [conn (d/get-conn dir schema)]
        (try
          (d/transact! conn [{:db/id -1 :name "Namebo"}])
          (finally
            (d/close conn))))
      (let [open-count (atom 0)
            orig-open  lmdb/open-kv]
        (with-redefs [lmdb/open-kv (fn
                                     ([dir]
                                      (swap! open-count inc)
                                      (orig-open dir))
                                     ([dir opts]
                                      (swap! open-count inc)
                                      (orig-open dir opts)))]
          (let [conn (d/get-conn dir schema)]
            (try
              (is (= 1 @open-count))
              (is (= 1 (d/q '[:find (count ?e) .
                              :where
                              [?e :name]]
                            @conn)))
              (finally
                (d/close conn))))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-conn
  (let [dir (u/tmp-dir (str "with-conn-test-" (UUID/randomUUID)))]
    (d/with-conn [conn dir]
      (d/transact! conn [{:db/id      -1
                          :name       "something"
                          :updated-at (Date.)}])
      (is (= 2 (count (d/datoms @conn :eav)))))
    (u/delete-files dir)))

(deftest test-relaxed-transact-uses-queued-path
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :relaxed
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (dotimes [i 32]
          (d/transact! conn [{:db/id i :k i}])))
      (is (= 32 (count @paths)))
      (is (every? #{:queued-relaxed} @paths))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-strict-transact-prefers-direct-path-when-idle
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :strict
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (dotimes [i 16]
          (d/transact! conn [{:db/id i :k i}])))
      (is (= 16 (count @paths)))
      (is (every? #{:direct-wal-idle-strict} @paths))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-extra-transact-prefers-direct-path-when-idle
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :extra
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (dotimes [i 16]
          (d/transact! conn [{:db/id i :k i}])))
      (is (= 16 (count @paths)))
      (is (every? #{:direct-wal-idle-extra} @paths))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-strict-transact-async-no-stall
  (let [n    256
        conn (d/create-conn nil
                            {:k {:db/valueType :db.type/long}}
                            {:wal? true
                             :wal-durability-profile :strict
                             :kv-opts {:inmemory? true}})]
    (try
      (let [futs    (doall
                      (for [i (range n)]
                        (d/transact-async conn [{:db/id i :k i}])))
            results (doall (map #(deref % 10000 ::timeout) futs))]
        (is (not-any? #{::timeout} results))
        (is (= n
               (d/q '[:find (count ?e) .
                      :where [?e :k]]
                    (d/db conn)))))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-transact-async-callback-exception-does-not-stall-result
  (let [conn (d/create-conn nil
                            {:k {:db/valueType :db.type/long}}
                            {:wal? true
                             :wal-durability-profile :strict
                             :kv-opts {:inmemory? true}})
        fut  (d/transact-async conn
                               [{:db/id 1 :k 1}]
                               nil
                               (fn [_]
                                 (throw (ex-info "callback failed" {}))))]
    (try
      (is (not= ::timeout (deref fut 2000 ::timeout)))
      (is (= 1
             (d/q '[:find (count ?e) .
                    :where [?e :k]]
                  (d/db conn))))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(defn- wait-until
  [pred timeout-ms]
  (let [^long timeout-ms timeout-ms]
    (loop [elapsed 0]
      (cond
        (pred) true
        (>= ^long elapsed timeout-ms) false
        :else
        (do
          (Thread/sleep 25)
          (recur (+ elapsed 25)))))))

(deftest test-strict-with-transaction-transact-uses-direct-path
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :strict
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (d/with-transaction [cn conn]
          (dotimes [i 8]
            (d/transact! cn [{:db/id (- (inc i)) :k i}]))))
      (is (= 8 (count @paths)))
      (is (every? #{:direct-no-wal} @paths))
      (is (= 8
             (d/q '[:find (count ?e) .
                    :where [?e :k]]
                  (d/db conn))))
      (finally
        (d/close conn)))))

(deftest test-wal-rejects-commit-before-txlog-append
  (let [dir  (u/tmp-dir (str "wal-cross-process-recovery-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (thrown-with-msg?
               Exception
               #"forced commit failure"
               (binding [cpp/*before-write-commit-fn*
                         (fn [ctx]
                           (when (= (:operation ctx) :close-transact-kv)
                             (throw (ex-info "forced commit failure"
                                             {:type ::forced-commit-failure}))))]
                 (d/transact-kv db [[:put "a" :k :v]]))))
          (finally
            (d/close-kv db))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (nil? (d/get-value db "a" :k)))
          (is (= [1]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (= 1
                 (get-in (kv/read-commit-marker db) [:current :applied-lsn])))
          (is (= 1
                 (:last-applied-lsn (kv/txlog-watermarks db))))
          (is (:ok? (kv/verify-commit-marker! db)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-explicit-write-transaction-rejects-commit-before-txlog-append
  (let [dir  (u/tmp-dir (str "wal-explicit-write-transaction-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (let [wdb (i/open-transact-kv db)]
            (is (= :transacted
                   (d/transact-kv wdb [[:put "a" :k :v]])))
            (is (thrown-with-msg?
                 Exception
                 #"forced commit failure"
                 (binding [cpp/*before-write-commit-fn*
                           (fn [ctx]
                             (when (= (:operation ctx) :close-transact-kv)
                               (throw (ex-info "forced commit failure"
                                               {:type ::forced-commit-failure}))))]
                   (i/close-transact-kv db)))))
          (is (nil? (d/get-value db "a" :k)))
          (is (= [1]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (is (= :v2
                 (d/get-value db "a" :k2)))
          (is (= [1 2]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-after-append-hook-runs-after-local-commit
  (let [dir  (u/tmp-dir (str "wal-after-append-hook-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}
        seen (atom nil)]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (binding [kv/*after-txlog-append-fn*
                    (fn [{:keys [txlog-lsn] :as ctx}]
                      (reset! seen
                              {:ctx ctx
                               :value (d/get-value db "a" :k)
                               :watermarks (kv/txlog-watermarks db)
                               :txlog-lsn txlog-lsn}))]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" :k :v]]))))
          (is (= :v (:value @seen)))
          (is (= 2 (long (:txlog-lsn @seen))))
          (is (= 2
                 (long (get-in @seen [:watermarks :last-applied-lsn]))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-after-append-hook-runs-for-explicit-transaction-close
  (let [dir  (u/tmp-dir (str "wal-after-append-explicit-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}
        seen (atom nil)]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (let [wdb (i/open-transact-kv db)]
            (is (= :transacted
                   (d/transact-kv wdb [[:put "a" :k :v]])))
            (binding [kv/*after-txlog-append-fn*
                      (fn [{:keys [txlog-lsn] :as ctx}]
                        (reset! seen
                                {:ctx ctx
                                 :value (d/get-value db "a" :k)
                                 :watermarks (kv/txlog-watermarks db)
                                 :txlog-lsn txlog-lsn}))]
              (is (= :committed
                     (i/close-transact-kv db)))))
          (is (= :v (:value @seen)))
          (is (= 2 (long (:txlog-lsn @seen))))
          (is (= 2
                 (long (get-in @seen [:watermarks :last-applied-lsn]))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-after-append-hook-failure-is-post-commit
  (let [dir  (u/tmp-dir (str "wal-after-append-hook-failure-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (thrown-with-msg?
               Exception
               #"forced publish failure"
               (binding [kv/*after-txlog-append-fn*
                         (fn [_]
                           (throw (ex-info "forced publish failure"
                                           {:type ::forced-publish-failure})))]
                 (d/transact-kv db [[:put "a" :k :v]]))))
          (is (= :v
                 (d/get-value db "a" :k)))
          (is (= 2
                 (long (:last-applied-lsn (kv/txlog-watermarks db)))))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (is (= [:v :v2]
                 [(d/get-value db "a" :k)
                  (d/get-value db "a" :k2)]))
          (is (= [1 2 3]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-open-failure-still-allows-reopen
  (let [dir                    (u/tmp-dir (str "wal-process-lock-failure-test-"
                                               (UUID/randomUUID)))
        bootstrap-disabled-opts {:wal? true
                                 :snapshot-bootstrap-force? false}
        bootstrap-opts          {:wal? true
                                 :snapshot-bootstrap-force? true}]
    (try
      (let [db (d/open-kv dir bootstrap-disabled-opts)]
        (try
          (d/open-dbi db "bootstrap")
          (d/transact-kv db [[:put "bootstrap" :k :v]])
          (finally
            (d/close-kv db))))
      (let [{:keys [db error]}
            (binding [kv/*wal-snapshot-copy-failpoint*
                      (fn [_]
                        (throw (ex-info "forced snapshot bootstrap failure"
                                        {:type ::snapshot-bootstrap-failed})))]
              (try
                {:db (d/open-kv dir bootstrap-opts)}
                (catch Exception e
                  {:error e})))]
        (when db
          (d/close-kv db))
        (is (instance? clojure.lang.ExceptionInfo error))
        (is (re-find #"forced snapshot bootstrap failure"
                     (.getMessage ^Exception error))))
      (let [db (d/open-kv dir bootstrap-opts)]
        (try
          (is (not (d/closed-kv? db)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-open-fallback-restores-snapshot-with-clean-txlog-runtime
  (let [dir      (u/tmp-dir (str "wal-snapshot-fallback-test-"
                                 (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts     {:wal? true
                  :snapshot-bootstrap-force? false}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (d/create-snapshot! db)
          (is (seq (d/list-snapshots db)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (finally
            (d/close-kv db))))
      (let [segment-path (->> (or (u/list-files txlog-dir) [])
                              (map #(.getPath ^java.io.File %))
                              (filter #(str/ends-with? % ".wal"))
                              sort
                              first)]
        (is (string? segment-path))
        (with-open [raf (java.io.RandomAccessFile. ^String segment-path "rw")]
          (.seek raf 0)
          (.write raf (byte-array [0 0 0 0]))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v1
                 (d/get-value db "a" :k1)))
          (is (nil? (d/get-value db "a" :k2)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k3 :v3]])))
          (is (= :v3
                 (d/get-value db "a" :k3)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-replay-restores-live-created-dbi-from-snapshot-tail
  (let [dir  (u/tmp-dir (str "wal-live-dbi-snapshot-tail-test-"
                             (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts {:wal? true
              :wal-durability-profile :strict
              :snapshot-bootstrap-force? false}]
    (try
      (let [snapshot (atom nil)]
        (let [db (d/open-kv dir opts)]
          (try
            (reset! snapshot (:snapshot (d/create-snapshot! db)))
            (d/open-dbi db "live" {:key-size 64})
            (is (= :transacted
                   (d/transact-kv db [[:put "live" :k :v]])))
            (finally
              (d/close-kv db))))
        (copy-snapshot-files-to-env! (:path @snapshot) dir)
        (let [applied-lsn (long (:applied-lsn @snapshot))]
          (txlog/write-meta-file!
           (txlog/meta-path txlog-dir)
           {:last-committed-lsn applied-lsn
            :last-durable-lsn applied-lsn
            :last-applied-lsn applied-lsn
            :segment-id 1
            :segment-offset 0
            :updated-ms (System/currentTimeMillis)}))
        (let [db (d/open-kv dir opts)]
          (try
            (is (map? (i/dbi-opts db "live")))
            (is (= :v
                   (d/get-value db "live" :k)))
            (finally
              (d/close-kv db)))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-snapshot-fallback-failure-keeps-original-lmdb-files
  (let [dir       (u/tmp-dir (str "wal-snapshot-fallback-safe-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        data-path (str dir u/+separator+ c/data-file-name)
        opts      {:wal? true
                   :snapshot-bootstrap-force? false}]
    (try
      (let [snapshot-path (atom nil)]
        (let [db (d/open-kv dir opts)]
          (try
            (d/open-dbi db "a")
            (is (= :transacted
                   (d/transact-kv db [[:put "a" :k1 :v1]])))
            (d/create-snapshot! db)
            (reset! snapshot-path (:path (first (d/list-snapshots db))))
            (is (= :transacted
                   (d/transact-kv db [[:put "a" :k2 :v2]])))
            (finally
              (d/close-kv db))))
        (let [before (java.nio.file.Files/readAllBytes
                      (.toPath (java.io.File. data-path)))
              snapshot-data-path (str @snapshot-path u/+separator+
                                      c/data-file-name)
              segment-path (first-wal-segment-path txlog-dir)]
          (is (string? segment-path))
          (with-open [raf (java.io.RandomAccessFile.
                           ^String snapshot-data-path
                           "rw")]
            (.setLength raf 0)
            (.write raf (.getBytes "not-an-lmdb-snapshot" "UTF-8")))
          (with-open [raf (java.io.RandomAccessFile.
                           ^String segment-path
                           "rw")]
            (.seek raf 0)
            (.write raf (byte-array [0 0 0 0])))
          (is (thrown-with-msg?
               Exception
               #"Txn-log recovery failed after snapshot restore attempts"
               (d/open-kv dir opts)))
          (is (Arrays/equals
               before
               (java.nio.file.Files/readAllBytes
                (.toPath (java.io.File. data-path)))))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-refresh-shared-state-clamps-stale-meta-segment-offset
  (let [dir       (u/tmp-dir (str "wal-stale-meta-offset-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts      {:wal? true}
        segment-path
        (fn []
          (->> (or (u/list-files txlog-dir) [])
               (map #(.getPath ^java.io.File %))
               (filter #(str/ends-with? % ".wal"))
               sort
               last))]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (finally
            (d/close-kv db))))
      (let [path (segment-path)]
        (is (string? path))
        ;; Leave the WAL meta file intact but wipe the active segment bytes to
        ;; simulate recovery paths where metadata survives while the segment
        ;; tail is rebuilt from scratch.
        (with-open [raf (java.io.RandomAccessFile. ^String path "rw")]
          (.setLength raf 0)))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v1
                 (d/get-value db "a" :k1)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (finally
            (d/close-kv db))))
      (let [path (segment-path)]
        (with-open [raf (java.io.RandomAccessFile. ^String path "r")]
          (let [magic (byte-array 4)]
            (is (= 4 (.read raf magic)))
            (is (= [0x44 0x4c 0x57 0x4c]
                   (mapv (fn [b] (bit-and 0xff (int b))) magic))))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v2
                 (d/get-value db "a" :k2)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-open-repairs-closed-preallocated-segment-tail
  (let [dir       (u/tmp-dir (str "wal-closed-prealloc-tail-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        state     (atom nil)]
    (try
      (u/create-dirs txlog-dir)
      (let [closed-path (txlog/segment-path txlog-dir 1)
            active-path (txlog/segment-path txlog-dir 2)]
        (txlog/prepare-segment! closed-path 4096)
        (with-open [^java.nio.channels.FileChannel ch
                    (txlog/open-segment-channel closed-path)]
          (txlog/append-record-at!
           ch
           0
           (txlog/encode-commit-row-payload 1 1 [])))
        (with-open [^java.nio.channels.FileChannel _
                    (txlog/open-segment-channel active-path)])
        (is (:preallocated-tail?
             (txlog/scan-segment closed-path
                                 {:allow-preallocated-tail? true
                                  :collect-records?         false})))
        (is (thrown-with-msg?
             Exception
             #"Txn-log segment corruption"
             (txlog/scan-segment closed-path {:collect-records? false})))
        (reset! state
                (:state (txlog/init-runtime-state {:dir dir} {})))
        (is (= 2 (long @(:segment-id @state))))
        (is (= 2 (long @(:next-lsn @state))))
        (let [file (java.io.File. ^String closed-path)
              scan (txlog/scan-segment closed-path
                                       {:collect-records? false})]
          (is (false? (:partial-tail? scan)))
          (is (= (long (:valid-end scan))
                 (long (.length file))))))
      (finally
        (when-let [state @state]
          (when-let [ch @(:segment-channel state)]
            (.close ^java.io.Closeable ch)))
        (u/delete-files dir)))))

(deftest test-wal-open-truncates-torn-active-tail-checksum-mismatch
  (let [dir       (u/tmp-dir (str "wal-torn-active-tail-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        state     (atom nil)]
    (try
      (u/create-dirs txlog-dir)
      (let [active-path (txlog/segment-path txlog-dir 1)]
        (txlog/prepare-segment! active-path 4096)
        (let [valid-payload (txlog/encode-commit-row-payload 1 1 [])
              bad-payload   (txlog/encode-commit-row-payload 2 2 [])
              bad-record    (txlog/encode-record bad-payload)
              header-len    (alength ^bytes (txlog/encode-record
                                              (byte-array 0)))
              body-fragment (max 1 (min 8
                                         (dec (alength ^bytes bad-payload))))
              torn-len      (+ header-len body-fragment)
              valid-end
              (with-open [^java.nio.channels.FileChannel ch
                          (txlog/open-segment-channel active-path)]
                (let [{:keys [size]} (txlog/append-record-at!
                                      ch
                                      0
                                      valid-payload)
                      tail-offset (long size)
                      ^ByteBuffer torn-bf (ByteBuffer/wrap
                                           ^bytes bad-record
                                           0
                                           torn-len)]
                  (.position ch tail-offset)
                  (while (.hasRemaining torn-bf)
                    (.write ch torn-bf))
                  tail-offset))]
          (let [scan (txlog/scan-segment
                      active-path
                      {:allow-preallocated-tail? true
                       :collect-records?         false})]
            (is (:partial-tail? scan))
            (is (not (:preallocated-tail? scan)))
            (is (:checksum-mismatch-tail? scan))
            (is (= valid-end (long (:valid-end scan)))))
          (is (thrown-with-msg?
               Exception
               #"Txn-log segment corruption"
               (txlog/scan-segment active-path {:collect-records? false})))
          (reset! state
                  (:state (txlog/init-runtime-state {:dir dir} {})))
          (is (= 2 (long @(:next-lsn @state))))
          (is (= valid-end (long @(:segment-offset @state))))
          (let [file (java.io.File. ^String active-path)
                scan (txlog/scan-segment active-path
                                         {:collect-records? false})]
            (is (= valid-end (long (.length file))))
            (is (false? (:partial-tail? scan)))
            (is (= valid-end (long (:valid-end scan)))))))
      (finally
        (when-let [state @state]
          (when-let [ch @(:segment-channel state)]
            (.close ^java.io.Closeable ch)))
        (u/delete-files dir)))))

(deftest test-wal-commit-meta-segment-offset-matches-segment-end
  (let [dir       (u/tmp-dir (str "wal-commit-meta-offset-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts      {:wal? true}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (finally
            (d/close-kv db))))
      (let [{:keys [file]} (last (txlog/segment-files txlog-dir))
            meta-state (get-in (txlog/read-meta-file (txlog/meta-path txlog-dir))
                               [:current])
            scan (txlog/scan-segment (.getPath ^java.io.File file)
                                     {:allow-preallocated-tail? true})
            end-offset (txlog/segment-end-offset scan)]
        (is (map? meta-state))
        (is (= (long end-offset)
               (long (:segment-offset meta-state)))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-replay-aligns-runtime-cursor-to-persisted-payload-floor
  (let [dir  (u/tmp-dir (str "wal-replay-align-floor-test-"
                             (UUID/randomUUID)))
        opts {:wal? true}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          ;; Build a local WAL tail that ends at LSN 14, then simulate a
          ;; snapshot-installed follower whose persisted payload floor has
          ;; already advanced to LSN 15 before the runtime txlog cursor is
          ;; realigned.
          (dotimes [i 13]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" i i]]))))
          (let [state (txlog/state db)]
            (is (some? state))
            (is (= 15 (long @(:next-lsn state))))
            (is (= :transacted
                   (i/transact-kv db
                                  c/kv-info
                                  [[:put c/wal-local-payload-lsn 15]]
                                  :keyword
                                  :data)))
            (let [res (kv/mirror-replayed-txlog-record!
                       db
                       {:lsn 16
                        :ha-term 7
                        :rows [[:put "a" :replayed :ok]]})]
              (is (= 16 (long (:lsn res))))
              (is (not (:skipped? res)))
              (is (= 17 (long @(:next-lsn state))))
              (is (= :ok
                     (d/get-value db "a" :replayed)))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-replay-preserves-lookup-ref-cas
  (let [source-dir (u/tmp-dir (str "wal-replay-cas-source-"
                                   (UUID/randomUUID)))
        target-dir (u/tmp-dir (str "wal-replay-cas-target-"
                                   (UUID/randomUUID)))
        schema     {:identity/key   {:db/valueType :db.type/string
                                      :db/unique :db.unique/identity}
                    :identity/value {:db/valueType :db.type/long}}
        opts       {:wal? true
                    :wal-durability-profile :strict}]
    (try
      (let [source-conn (d/create-conn source-dir schema opts)
            target-conn (d/create-conn target-dir schema opts)]
        (try
          (d/with-transaction [tx source-conn]
            (d/transact! tx
                         [{:db/id -1
                           :identity/key "identity-1"
                           :identity/value 0}]))
          (d/with-transaction [tx source-conn]
            (d/transact! tx
                         [[:db/cas [:identity/key "identity-1"]
                           :identity/value 0 1]
                          [:db/cas [:identity/key "identity-1"]
                           :identity/value 1 2]]))
          (let [source-kv (.-lmdb ^Store (conn-store source-conn))
                target-kv (.-lmdb ^Store (conn-store target-conn))
                records   (kv/open-tx-log source-kv 1)]
            (doseq [record records]
              (kv/mirror-replayed-txlog-record! target-kv record))
            (d/close target-conn)
            (let [target-conn' (d/create-conn target-dir schema opts)]
              (try
                (is (= 2 (:identity/value
                          (d/entity @target-conn'
                                    [:identity/key "identity-1"]))))
                (finally
                  (d/close target-conn')))))
          (finally
            (d/close source-conn)
            (when-not (d/closed? target-conn)
              (d/close target-conn)))))
      (finally
        (u/delete-files source-dir)
        (u/delete-files target-dir)))))

(deftest test-wal-one-shot-write-uses-explicit-lmdb-write-transaction
  (let [dir  (u/tmp-dir (str "wal-one-shot-write-test-" (UUID/randomUUID)))
        ops* (atom [])]
    (try
      (let [db (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi db "a")
          (binding [cpp/*before-write-commit-fn*
                    (fn [{:keys [operation]}]
                      (swap! ops* conj operation))]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" :k :v]]))))
          (is (= [:close-transact-kv] @ops*))
          (is (= :v
                 (d/get-value db "a" :k)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))
