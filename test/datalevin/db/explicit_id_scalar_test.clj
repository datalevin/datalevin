(ns datalevin.db.explicit-id-scalar-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.walk :as walk]
            [datalevin.conn :as conn]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.batch.embedded :as embedded]
            [datalevin.tx-group.compat :as group]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:value {:db/valueType :db.type/long}
   :text {:db/valueType :db.type/string}
   :flag {:db/valueType :db.type/boolean}
   :bytes {:db/valueType :db.type/bytes}
   :opaque {}
   :many {:db/valueType :db.type/long :db/cardinality :db.cardinality/many}
   :reference {:db/valueType :db.type/ref}
   :key {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :token {:db/valueType :db.type/string :db/unique :db.unique/value}
   :part {:db/valueType :db.type/long}
   :part2 {:db/valueType :db.type/long}
   :pair {:db/tupleAttrs [:part :part2]}})

(def ^:private options
  {:background-sampling? false :snapshot-scheduler? false})

(defn- seed! [connection]
  (d/transact! connection [{:db/id 1 :value 0 :text "old" :flag true}
                            {:db/id 2 :value 10 :key "two"}]))

(defn- report-data [report]
  (walk/postwalk #(if (bytes? %) (vec %) %)
                 {:datoms (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data report))
                  :tempids (:tempids report) :tx-meta (:tx-meta report)
                  :max-eid (:max-eid (:db-after report))}))

(deftest explicit-maps-share-scalar-plans-and-report-order
  (with-open [connection (d/create-conn nil schema options)]
    (seed! connection)
    (let [before @connection
          forms [(array-map :db/id 1 :text 42 :value 2 :flag false
                            :bytes (byte-array [1 2]) :opaque {:nested [1 2]})
                 {:db/id (int 2) :value 11}]
          operations (vec (mapcat (fn [entity]
                                    (for [[attr value] entity :when (not= attr :db/id)]
                                      [:db/add (:db/id entity) attr value])) forms))
          meta {:request :maps}
          expected (db/prepare-local-tx-data
                     (db/->TxReport before (db/transfer before (:store before)) [] {} meta)
                     forms)]
      (doseq [defer? [false true]
              input [forms (apply list forms) (map identity forms)]]
        (let [prepared (db/prepare-scalar-update-tx
                         before input {:defer-entity-resolution? defer?})
              vector-plan (db/prepare-scalar-update-tx before operations)
              report (db/stamp-scalar-update-tx before prepared meta)]
          (is (= (:entries vector-plan) (:entries prepared)))
          (is (= (report-data expected) (report-data report)))
          (is (identical? (:eavt before) (:eavt (:db-after report))))
          (is (identical? (:avet before) (:avet (:db-after report)))))))))

(deftest explicit-maps-use-scalar-dispatch-on-local-write-paths
  (doseq [wal? [false true] collected? [false true] explicit? [false true]]
    (binding [embedded/*defer-attachment?* (not collected?)
              group/*enabled?* collected?]
      (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
        (seed! connection)
        (let [paths (atom [])
              run (fn [tx]
                    (let [report (d/transact! tx [{:db/id 1 :value 2}] {:request :map})]
                      (is (= [[0 false] [2 true]]
                             (mapv (juxt :v datom/datom-added) (:tx-data report))))
                      (is (= {:request :map} (:tx-meta report)))
                      (is (= 2 (:value (d/entity @tx 1)))))
                    (d/transact! tx [(array-map :db/id 1 :value 3 :flag false)
                                     [:db/add 2 :value 12]])
                    (is (= [3 false 12] [(:value (d/entity @tx 1))
                                         (:flag (d/entity @tx 1))
                                         (:value (d/entity @tx 2))])))]
          (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
            (if explicit?
              (d/with-transaction [tx connection] (run tx))
              (run connection)))
          (is (= [:scalar-update :scalar-update] @paths)
              (str [wal? collected? explicit?])))))))

(deftest rejected-map-batches-do-not-correct-the-prefix
  (with-open [connection (d/create-conn nil schema options)]
    (seed! connection)
    (let [conversions (atom 0)
          value (proxy [Object] [] (toString [] (swap! conversions inc) "converted"))
          prefix [{:db/id 1 :text value}]]
      (doseq [defer? [false true]
              tail [{:db/id 1 :text "duplicate"}
                    {:db/id 1 :missing value} {:db/id 1 :text nil}
                    {:db/id 1 :many [1 2]} {:db/id 1 :reference [:key "two"]}
                    {:db/id 1 :key "identity"} {:db/id 1 :token "unique"}
                    {:db/id 1 :part 1} {:db/id 1 :pair [1 2]}
                    {:db/id 1 :_reference 2} {:db/id 1 "text" value}
                    {:db/id 1} (with-meta {:db/id 2 :text value} {:probe true})
                    {:text value} {:db/id nil :text value} {:db/id -1 :text value}
                    {:db/id "new" :text value} {:db/id [:key "two"] :text value}
                    {:db/id 999 :text value} {:db/id 0 :text value}
                    {:db/id 999999999999999999999N :text value}
                    [:db/retract 1 :value 0] [:db.fn/call (fn [_] [])]]
              forms [(conj prefix tail) (cons tail prefix)]]
        (is (nil? (db/prepare-scalar-update-tx
                    @connection forms {:defer-entity-resolution? defer?})))
        (is (zero? @conversions)))
      (is (some? (db/prepare-scalar-update-tx @connection prefix)))
      (is (= 1 @conversions)))))

(deftest repeated-map-updates-retain-interpreter-order-and-rollback
  (doseq [wal? [false true]]
    (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
      (seed! connection)
      (let [conversions (atom 0)
            value (proxy [Object] [] (toString [] (swap! conversions inc) "converted"))
            paths (atom [])
            report (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
                     (d/transact! connection [{:db/id 1 :text value}
                                               {:db/id 1 :text "final"}]))]
        (is (= [:general] @paths))
        (is (= 1 @conversions))
        (is (= [["old" false] ["converted" true] ["converted" false] ["final" true]]
               (mapv (juxt :v datom/datom-added) (:tx-data report))))
        (d/with-transaction [tx connection]
          (d/transact! tx [{:db/id 1 :value 9}])
          (is (= 9 (:value (d/entity @tx 1))))
          (d/abort-transact tx))
        (is (= 0 (:value (d/entity @connection 1))))))))

(deftest map-scalar-plans-retain-schema-and-automatic-time-guards
  (with-open [connection (d/create-conn nil schema options)]
    (seed! connection)
    (let [prepared (db/prepare-scalar-update-tx @connection [{:db/id 1 :value 2}])]
      (is (some? prepared))
      (d/update-schema connection {:value {:db/cardinality :db.cardinality/many}})
      (is (nil? (db/stamp-scalar-update-tx @connection prepared nil)))
      (is (nil? (db/prepare-scalar-update-tx @connection [{:db/id 1 :value 2}])))))
  (with-open [connection (d/create-conn nil schema (assoc options :auto-entity-time? true))]
    (seed! connection)
    (is (nil? (db/prepare-scalar-update-tx @connection [{:db/id 1 :value 2}])))))

(deftest map-batches-and-wide-maps-preserve-general-report-order
  (let [fields (mapv #(keyword "field" (str %)) (range 8))
        wide-schema (merge schema (zipmap fields (repeat {:db/valueType :db.type/long})))
        wide-map (into (array-map :db/id 1) (map #(vector % 1) fields))]
    (with-open [connection (d/create-conn nil wide-schema options)]
      (seed! connection)
      (d/transact! connection (mapv #(hash-map :db/id % :value 0) (range 3 129)))
      (doseq [forms [(mapv #(hash-map :db/id % :value 1) (range 1 129))
                     [wide-map]
                     [(array-map :text "new" :db/id (int 1) :value 3)
                      [:db/add [:key "two"] :text "second"]]]]
        (let [before @connection
              expected (db/prepare-local-tx-data
                         (db/->TxReport before (db/transfer before (:store before)) [] {} nil)
                         forms)]
          (doseq [defer? [false true]
                  input [forms (apply list forms) (map identity forms)]]
            (let [prepared (db/prepare-scalar-update-tx before input
                             {:defer-entity-resolution? defer?})
                  report (db/stamp-scalar-update-tx before prepared nil)]
              (is (some? prepared))
              (is (= (report-data expected) (report-data report)))
              (is (identical? (:eavt before) (:eavt (:db-after report)))))))))))

(deftest map-and-operation-aliases-preserve-correction-and-rejection-order
  (with-open [connection (d/create-conn nil schema options)]
    (seed! connection)
    (let [conversions (atom 0)
          value (proxy [Object] [] (toString [] (swap! conversions inc) "corrected"))]
      (doseq [defer? [false true]
              forms [[{:db/id (int 1) :text value} [:db/add 1 :text value]]
                     [[:db/add 1 :text value] {:db/id 1N :text value}]
                     [{:db/id 2 :text value} [:db/add [:key "two"] :text value]]
                     [[:db/add [:key "two"] :text value] {:db/id 2 :text value}]]]
        (reset! conversions 0)
        (let [prepared (db/prepare-scalar-update-tx @connection forms
                         {:defer-entity-resolution? defer?})]
          (if prepared
            (do (is defer?)
                (is (= 2 @conversions))
                (is (nil? (db/stamp-scalar-update-tx @connection prepared nil))))
            (is (zero? @conversions)))))
      (is (= "old" (:text (d/entity @connection 1)))))))
