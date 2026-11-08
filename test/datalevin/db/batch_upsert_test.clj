(ns datalevin.db.batch-upsert-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.conn :as conn]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.batch.embedded :as embedded]
            [datalevin.tx-group.compat :as group]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:key {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :alternate {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :token {:db/valueType :db.type/string :db/unique :db.unique/value}
   :text {:db/valueType :db.type/string}
   :bytes {:db/valueType :db.type/bytes}
   :many {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}})

(def ^:private options
  {:background-sampling? false :snapshot-scheduler? false})

(defn- seed! [connection]
  (d/transact! connection [{:db/id 1 :key "one" :alternate "first" :text "old-one"}
                          {:db/id 2 :key "two" :text "old-two"}]))

(defn- report-data [report]
  {:datoms (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data report))
   :tempids (:tempids report) :tx-meta (:tx-meta report)
   :max-eid (:max-eid (:db-after report))
   :max-tx (:max-tx (:db-after report))})

(deftest collided-batches-normalize-once-on-local-write-paths
  (doseq [wal? [false true] collected? [false true] explicit? [false true]]
    (binding [embedded/*defer-attachment?* (not collected?)
              group/*enabled?* collected?]
      (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
        (seed! connection)
        (let [conversions (atom 0)
              value (fn [s] (proxy [Object] []
                              (toString [] (swap! conversions inc) s)))
              paths (atom [])
              run (fn [tx]
                    (when explicit?
                      (d/transact! tx [[:db/add 1 :text "pending"]]))
                    (let [report (binding [conn/*local-wal-tx-path-observer*
                                           #(swap! paths conj %)]
                                   (d/transact! tx
                                     [{:db/id -1 :key "one" :text (value "new-one")}
                                      {:db/id "second" :key "two" :text (value "new-two")}]
                                     {:request :upserts}))]
                      (is (= 2 @conversions) (str [wal? collected? explicit?]))
                      (is (= [:identity-upsert] @paths))
                      (is (= [[1 (if explicit? "pending" "old-one") false]
                              [1 "new-one" true] [2 "old-two" false] [2 "new-two" true]]
                             (mapv (juxt :e :v datom/datom-added) (:tx-data report))))
                      (is (= {-1 1 "second" 2 :db/current-tx (:max-tx (:db-after report))}
                             (:tempids report)))
                      (is (= {:request :upserts} (:tx-meta report)))
                      (is (= ["new-one" "new-two"]
                             (mapv #(:text (d/entity @tx %)) [1 2])))))]
          (if explicit? (d/with-transaction [tx connection] (run tx)) (run connection))
          (is (= ["new-one" "new-two"]
                 (mapv #(:text (d/entity @connection %)) [1 2]))))))))

(deftest normalized-insert-upsert-plans-match-general-reports
  (doseq [forms [[{:db/id -1 :key "one" :text "changed"}
                 {:db/id "new" :key "new" :text "inserted"}]
                [{:key "new" :text "inserted"} {:key "two" :text "changed"}]
                [{:key "one" :text "old-one"} {:key "two" :text "old-two"}]]]
    (with-open [connection (d/create-conn nil schema options)]
      (seed! connection)
      (let [before @connection
            expected (db/prepare-local-tx-data
                       (db/->TxReport before (db/transfer before (:store before)) [] {} nil)
                       forms)
            prepared (db/prepare-blind-local-tx before forms true false)
            report (db/stamp-blind-local-upsert-tx before prepared nil)]
        (is (some? report))
        (is (= (report-data expected) (report-data report)))
        (is (identical? (:eavt before) (:eavt (:db-after report))))
        (is (= (report-data expected) (report-data (d/transact! connection forms))))))))

(deftest upsert-fallback-preserves-pending-identity-and-rollback
  (doseq [wal? [false true]]
    (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
      (seed! connection)
      (d/with-transaction [tx connection]
        (d/transact! tx [{:key "pending" :text "first"}])
        (let [report (d/transact! tx [{:db/id "a" :key "pending" :text "second"}
                                     {:db/id "b" :key "one" :text "changed"}])]
          (is (= "second" (:text (d/entity @tx (get (:tempids report) "a")))))
          (is (= 1 (get (:tempids report) "b"))))
        (d/abort-transact tx))
      (is (nil? (d/entity @connection [:key "pending"])))
      (is (= "old-one" (:text (d/entity @connection 1))))
      (d/with-transaction [tx connection]
        (let [paths (atom [])
              report (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
                       (d/transact! tx [{:db/id "a" :key "one" :text "intermediate"}
                                       {:db/id "b" :alternate "first" :text "final"}]))]
          (is (= [:general] @paths))
          (is (= "final" (:text (d/entity @tx 1))))
          (is (= 1 (get (:tempids report) "a") (get (:tempids report) "b"))))
        (is (thrown? Exception
                     (d/transact! tx [{:key "one" :token "duplicate" :text "bad"}
                                     {:key "two" :token "duplicate" :text "bad"}])))
        (is (= "final" (:text (d/entity @tx 1)))))
      (is (= "old-two" (:text (d/entity @connection 2)))))))

(deftest late-collisions-restore-reservations-before-reusing-preparation
  (doseq [wal? [false true]]
    (binding [embedded/*defer-attachment?* false group/*enabled?* true]
      (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
        (seed! connection)
        (d/with-transaction [tx connection]
          (d/transact! tx [[:db/add 2 :text "accepted-prefix"]])
          (let [conversions (atom 0)
                value (proxy [Object] [] (toString [] (swap! conversions inc) "normalized"))
                report (d/transact! tx [{:db/id "new" :key "new" :text value}
                                       {:db/id "old" :key "one" :text value}])]
            (is (= 2 @conversions))
            (is (= 3 (get (:tempids report) "new")))
            (is (= 1 (get (:tempids report) "old")))
            (is (= 3 (:max-eid (:db-after report))))
            (is (= "accepted-prefix" (:text (d/entity @tx 2))))
            (is (= "normalized" (:text (d/entity @tx 3))))
            (is (= 3 (count (d/datoms @tx :ave :key))))
            (let [next-report (d/transact! tx [{:db/id "next" :key "next" :text "next"}])]
              (is (= 4 (get (:tempids next-report) "next"))))))
        (is (= "normalized" (:text (d/entity @connection 1))))
        (is (= "next" (:text (d/entity @connection 4))))))))

(deftest normalized-upserts-keep-byte-content-equality-and-giant-retractions
  (doseq [wal? [false true] size [3 2048]]
    (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
      (seed! connection)
      (let [payload (byte-array size (byte 1))]
        (d/transact! connection [{:db/id 1 :bytes payload} {:db/id 2 :bytes payload}])
        (is (empty? (:tx-data
                      (d/transact! connection [{:key "one" :bytes (aclone payload)}
                                              {:key "two" :bytes (aclone payload)}]))))
        (let [replacement (byte-array size (byte 2))
              report (d/transact! connection [{:key "one" :bytes replacement}
                                             {:key "two" :bytes replacement}])]
          (is (= [false true false true] (mapv datom/datom-added (:tx-data report))))
          (is (= (vec replacement) (vec (:bytes (d/entity @connection 1)))))
          (is (= (vec replacement) (vec (:bytes (d/entity @connection 2))))))))))

(deftest batch-upserts-revalidate-schema-and-retain-unsupported-shapes
  (with-open [connection (d/create-conn nil schema options)]
    (seed! connection)
    (doseq [forms [[{:key "one" :many ["a"]} {:key "two" :many ["b"]}]
                   [{:key "one" :alternate "first"} {:key "two"}]
                   [{:key "one" :token "new"} {:key "two" :token "other"}]]]
      (let [prepared (db/prepare-blind-local-tx @connection forms true false)]
        (is (some? prepared))
        (is (nil? (db/stamp-blind-local-upsert-tx @connection prepared nil)))))
    (let [prepared (db/prepare-blind-local-tx @connection
                     [{:key "one" :text "new"} {:key "two" :text "new"}] true false)]
      (d/update-schema connection {:text {:db/cardinality :db.cardinality/many}})
      (is (nil? (db/stamp-blind-local-upsert-tx @connection prepared nil)))))
  (with-open [connection (d/create-conn nil schema (assoc options :auto-entity-time? true))]
    (let [prepared (db/prepare-blind-local-tx @connection
                     [{:key "one"} {:key "two"}] true false)]
      (is (some? prepared))
      (is (nil? (db/stamp-blind-local-upsert-tx @connection prepared nil))))))

(deftest top-level-collision-retry-refreshes-peer-allocation-floors
  (doseq [wal? [false true]]
    (binding [embedded/*defer-attachment?* true group/*enabled?* false]
      (let [dir (u/tmp-dir (str "batch-upsert-peer-" (random-uuid)))]
        (try
          (with-open [peer (d/create-conn dir schema (assoc options :wal? wal?))
                      connection (d/create-conn dir schema (assoc options :wal? wal?))]
            (seed! peer)
            (d/transact! peer [{:db/id 100 :text "peer"}])
            (let [peer-tx (:max-tx @peer)
                  report (d/transact! connection [{:db/id "old" :key "one" :text "changed"}
                                                 {:db/id "new" :key "new" :text "inserted"}])]
              (is (= 1 (get (:tempids report) "old")))
              (is (= 101 (get (:tempids report) "new")))
              (is (= (inc (long peer-tx)) (:max-tx (:db-after report))))
              (is (= "peer" (:text (d/entity @connection 100))))
              (is (= "inserted" (:text (d/entity @connection 101))))))
          (finally (u/delete-files dir)))))))
