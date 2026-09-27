(ns datalevin.ha.follower-batch-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.binding.cpp :as cpp]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.ha.replication :as repl]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.txlog :as wal]
   [datalevin.txlog.transfer :as transfer]
   [datalevin.util :as u])
  (:import [datalevin.db DB]
           [datalevin.storage Store]
           [java.util Arrays Random]))

(use-fixtures :each db-fixture)

(defn- with-pair [opts f]
  (let [root (u/tmp-dir (str "follower-batch-" (random-uuid)))
        opts (merge {:wal? true :wal-durability-profile :strict
                     :wal-segment-prealloc? false} opts)
        source (d/open-kv (str root "/source") opts)
        target (d/open-kv (str root "/target") opts)]
    (try
      (doseq [db [source target]] (d/open-dbi db "data"))
      (f source target opts)
      (finally
        (d/close-kv source)
        (d/close-kv target)
        (u/delete-files root)))))

(defn- source-records [source target values]
  (let [from @(:next-lsn (wal/state target))]
    (binding [wal/*commit-payload-ha-term* 7]
      (doseq [value values]
        (d/transact-kv source [[:put "data" :value value]])))
    (transfer/decode-batch (kv/open-tx-log-batch source from Long/MAX_VALUE))))

(defn- payload-floor [db]
  (long (or (i/get-value db c/kv-info c/wal-local-payload-lsn :keyword :data) 0)))

(defn- sync-count [state]
  (let [metrics (wal/sync-manager-state (:sync-manager state))]
    (+ (long (:forced-sync-count metrics)) (long (:batched-sync-count metrics)))))

(deftest replay-shares-durability-and-native-commit
  (doseq [mode [:fdatasync :fsync]]
    (with-pair
      {:wal-sync-mode mode}
      (fn [source target _]
        (let [records (source-records source target (range 16))
              state (wal/state target)
              old-floor (payload-floor target)
              seen (atom [])
              commits (atom [])
              before (sync-count state)
              result
              (binding [cpp/*before-write-commit-fn* #(swap! commits conj (:operation %))]
                (kv/mirror-replayed-txlog-records!
                 target records
                 (fn [writing _]
                   (swap! seen conj (i/get-value writing "data" :value))
                   (is (= old-floor (payload-floor target)))
                   (is (nil? (d/get-value target "data" :value)))
                   [])))
              last-lsn (:lsn (peek records))]
          (is (= (into [nil] (range 15)) @seen))
          (is (= [:close-transact-kv] @commits))
          (is (= 1 (- (long (sync-count state)) (long before))))
          (is (= last-lsn (:lsn result) (payload-floor target)))
          (is (= last-lsn @(:meta-last-applied-lsn state)))
          (is (= last-lsn (:last-durable-lsn
                           (wal/sync-manager-state (:sync-manager state)))))
          (is (= 15 (d/get-value target "data" :value)))
          (let [identity #(select-keys (assoc % :rows (or (:rows %) (:ops %)))
                                       [:lsn :tx-time :ha-term :checksum :rows])]
            (is (= (mapv identity records)
                   (mapv identity (kv/open-tx-log target (:lsn (first records)))))))
          (testing "repeating a fetched page cannot regress state or consume LSNs"
            (let [next-lsn @(:next-lsn state)]
              (kv/mirror-replayed-txlog-records! target records nil)
              (is (= next-lsn @(:next-lsn state)))
              (is (= 15 (d/get-value target "data" :value)))))
          (testing "an existing LSN with a different term is rejected"
            (is (thrown? clojure.lang.ExceptionInfo
                         (kv/mirror-replayed-txlog-records!
                          target [(assoc (first records) :ha-term 8)] nil)))))))))

(deftest replay-aborts-materialization-and-recovers-durable-batch
  (with-pair
    {}
    (fn [source target opts]
      (let [records (source-records source target [1 2 3])
            floor (payload-floor target)
            state (wal/state target)
            dir (i/env-dir target)]
        (binding [cpp/*before-write-commit-fn*
                  (fn [_] (throw (ex-info "injected commit failure" {})))]
          (is (thrown? clojure.lang.ExceptionInfo
                       (kv/mirror-replayed-txlog-records! target records nil))))
        (is (nil? (d/get-value target "data" :value)))
        (is (= floor (payload-floor target)))
        (is (< (long @(:meta-last-applied-lsn state))
               (long (:lsn (peek records)))))
        (is (some? @(:fatal-error state)))
        (is (= (:lsn (peek records))
               (:last-durable-lsn (wal/sync-manager-state (:sync-manager state)))))
        (d/close-kv target)
        (let [reopened (d/open-kv dir opts)]
          (try
            (d/open-dbi reopened "data")
            (is (= 3 (d/get-value reopened "data" :value)))
            (is (= (:lsn (peek records)) (payload-floor reopened)))
            (finally (d/close-kv reopened))))))))

(deftest replay-rejects-gaps-before-appending
  (with-pair
    {}
    (fn [source target _]
      (let [records (source-records source target [1 2 3])
            state (wal/state target)
            next-lsn @(:next-lsn state)]
        (is (thrown? clojure.lang.ExceptionInfo
                     (kv/mirror-replayed-txlog-records!
                      target [(first records) (last records)] nil)))
        (is (= next-lsn @(:next-lsn state)))
        (is (nil? (d/get-value target "data" :value)))
        (is (nil? @(:fatal-error state)))
        (is (thrown? clojure.lang.ExceptionInfo
                     (repl/apply-ha-follower-txlog-records!
                      {:store target :ha-authority-term 7}
                      [(first records) (assoc (second records) :ha-term 8)])))
        (is (= next-lsn @(:next-lsn state)))))))

(deftest replay-retries-materialization-without-appending-again
  (with-pair
    {:mapsize 1}
    (fn [source target _]
      (let [state (wal/state target)
            next-lsn (long @(:next-lsn state))
            random (Random. 42)
            values (mapv (fn [_] (let [b (byte-array 600000)] (.nextBytes random b) b))
                         (range 4))
            _ (doseq [n (range 4)]
                (d/transact-kv source [[:put "data" n (nth values n) :long :bytes]]))
            records (vec (kv/open-tx-log source next-lsn))
            attempts (atom 0)]
        (kv/mirror-replayed-txlog-records!
         target records
         (fn [_ record]
           (when (= (:lsn record) (:lsn (first records)))
             (swap! attempts inc))
           nil))
        (is (> (long @attempts) 1))
        (is (= (+ next-lsn 4) @(:next-lsn state)))
        (is (Arrays/equals ^bytes (peek values)
                           ^bytes (d/get-value target "data" 3 :long :bytes)))))))

(deftest datalog-batch-sees-preceding-cardinality-one-and-giant-updates
  (let [root (u/tmp-dir (str "follower-datalog-batch-" (random-uuid)))
        schema {:item/key {:db/valueType :db.type/long :db/unique :db.unique/identity}
                :item/value {:db/valueType :db.type/long}
                :item/payload {:db/valueType :db.type/string}}
        opts {:wal? true :wal-durability-profile :strict}
        source (d/create-conn (str root "/source") schema opts)
        target (d/create-conn (str root "/target") schema opts)
        payload #(apply str (repeat 2000 (str "payload-" %)))
        entity (fn [v] {:item/key 1 :item/value v :item/payload (payload v)})]
    (try
      (doseq [conn [source target]] (d/transact! conn [(entity 29)]))
      (doseq [v [13 9 5]] (d/transact! source [(entity v)]))
      (let [store (.-store ^DB @target)
            target-kv (.-lmdb ^Store store)
            source-kv (.-lmdb ^Store (.-store ^DB @source))
            records (transfer/decode-batch
                      (kv/open-tx-log-batch source-kv @(:next-lsn (wal/state target-kv))
                                            Long/MAX_VALUE))
            commits (atom 0)]
        (is (= 3 (count records)))
        (binding [cpp/*before-write-commit-fn* (fn [_] (swap! commits inc))]
          (repl/apply-ha-follower-txlog-records! {:store store} records))
        (is (= 1 @commits))
        (db/refresh-cache store)
        (let [db (db/new-db store)
              result (d/entity db [:item/key 1])]
          (is (= 5 (:item/value result)))
          (is (= (payload 5) (:item/payload result)))
          (is (= 3 (count (i/get-list target-kv c/eav
                                     (d/entid db [:item/key 1]) :id :avg)))))
        (is (= (i/max-tx (.-store ^DB @source)) (i/max-tx store))))
      (finally
        (d/close source)
        (d/close target)
        (u/delete-files root)))))

(deftest replay-refreshes-schema-between-data-groups
  (let [root (u/tmp-dir (str "follower-batch-schema-" (random-uuid)))
        schema {:item/key {:db/valueType :db.type/long :db/unique :db.unique/identity}
                :item/value {:db/valueType :db.type/long}}
        opts {:wal? true :wal-durability-profile :strict}
        source (d/create-conn (str root "/source") schema opts)
        target (d/create-conn (str root "/target") schema opts)
        target-store (volatile! (.-store ^DB @target))]
    (try
      (doseq [v [1 2]] (d/transact! source [{:item/key 1 :item/value v}]))
      (d/update-schema source {:item/next {:db/valueType :db.type/long}})
      (doseq [v [3 4]] (d/transact! source [{:item/key 1 :item/next v}]))
      (let [source-kv (.-lmdb ^Store (.-store ^DB @source))
            target-kv (.-lmdb ^Store @target-store)
            records (transfer/decode-batch
                      (kv/open-tx-log-batch source-kv @(:next-lsn (wal/state target-kv))
                                            Long/MAX_VALUE))
            next-state (repl/apply-ha-follower-txlog-records!
                        {:store @target-store} records)]
        (vreset! target-store (:store next-state))
        (is (not (identical? (.-store ^DB @target) @target-store)))
        (db/refresh-cache @target-store)
        (let [result (d/entity (db/new-db @target-store) [:item/key 1])]
          (is (= 2 (:item/value result)))
          (is (= 4 (:item/next result))))
        (is (= (:lsn (peek records))
               (payload-floor (.-lmdb ^Store @target-store)))))
      (finally
        (d/close source)
        (i/close @target-store)
        (d/close target)
        (u/delete-files root)))))
