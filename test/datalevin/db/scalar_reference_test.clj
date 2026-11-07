(ns datalevin.db.scalar-reference-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.db.tx.common :as common]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [db-fixture]])
  (:import [java.util HashMap]
           [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]))

(use-fixtures :each db-fixture)

(def ^:private fields (mapv #(keyword "field" (str %)) (range 7)))
(def ^:private schema
  (assoc (zipmap fields (repeat {:db/valueType :db.type/long}))
         :key {:db/valueType :db.type/string :db/unique :db.unique/identity}
         :alias {:db/valueType :db.type/string :db/unique :db.unique/value}
         :text {:db/valueType :db.type/string}))
(def ^:private options {:background-sampling? false :snapshot-scheduler? false})

(defn- seed! [conn]
  (d/transact! conn [(assoc (zipmap fields (repeat 0))
                            :db/id 1 :key "one" :alias "alias" :db/ident :entity/one :text "old")
                     (assoc (zipmap fields (repeat 20)) :db/id 2 :key "two")]))

(defn- batch-context []
  (let [pending (LongObjectHashMap.) unique (HashMap.)]
    (reify common/ScalarPendingIndex
      (pending-scalar-index [_] pending)
      (pending-unique-index [_] unique)
      (retain-pending-index? [_] false)
      (prepare-pending-index! [_ _] nil)
      (materialize-scalar-pending! [_ _] nil)
      (discard-scalar-pending! [_] (.clear pending) (.clear unique)))))

(defn- observed-store
  ([store reads] (observed-store store reads nil))
  ([store reads schema-reads]
   (reify i/IStore
     (schema [_] (i/schema store))
     (rschema [_]
       (when schema-reads (swap! schema-reads inc))
       (i/rschema store))
     (opts [_] (i/opts store))
     (av-first-e [_ attr value]
       (swap! reads conj [attr value])
       (i/av-first-e store attr value))
     (ea-first-v [_ e attr] (i/ea-first-v store e attr)))))

(defn- datoms [report]
  (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data report)))

(deftest seven-deferred-forms-share-one-identity-and-schema-read
  (with-open [conn (d/create-conn nil schema options)]
    (seed! conn)
    (let [reads (atom []) schema-reads (atom 0)
          database (db/transfer @conn (observed-store (:store @conn) reads schema-reads))
          forms (mapv #(vector :db/add [:key "one"] % 1) fields)
          prepared (db/prepare-scalar-update-tx
                     database forms {:defer-entity-resolution? true})]
      (is (empty? @reads))
      (reset! schema-reads 0)
      (binding [common/*batch-prepare* (batch-context)]
        (let [report (db/stamp-scalar-update-tx database prepared {:probe true})]
          (is (= [[:key "one"]] @reads))
          (is (= 1 @schema-reads))
          (is (= 14 (count (:tx-data report))))
          (is (= {:probe true} (:tx-meta report))))))))

(deftest scalar-requests-resolve-each-distinct-reference-once
  (with-open [conn (d/create-conn nil schema options)]
    (seed! conn)
    (doseq [batched? [false true] deferred? [false true]
            refs [[[:key "one"]] [:entity/one] [1]
                  (repeat 7 [:key "one"])
                  (repeatedly 7 #(vector :key "one"))
                  (repeat 7 :entity/one)
                  [[:key "one"] [:alias "alias"] :entity/one 1
                   [:key "one"] [:alias "alias"] :entity/one]
                  [[:key "one"] [:key "two"] [:key "one"] [:key "two"]
                   [:key "one"] [:key "two"] [:key "one"]]]]
      (let [reads (atom [])
            database (db/transfer @conn (observed-store (:store @conn) reads))
            operations (mapv #(vector :db/add %1 %2 1) refs fields)
            expected-ids (mapv #(if (= [:key "two"] %) 2 1) refs)
            expected (db/stamp-scalar-update-tx
                       database (db/prepare-scalar-update-tx
                                  database (mapv #(vector :db/add %1 %2 1) expected-ids fields))
                       {:probe true})
            context (when batched? (batch-context))]
        (binding [common/*batch-prepare* context]
          (let [prepared (db/prepare-scalar-update-tx
                           database operations {:defer-entity-resolution? deferred?})]
            (when deferred? (is (empty? @reads)))
            (let [report (db/stamp-scalar-update-tx database prepared {:probe true})
                  expected-reads (vec (distinct (keep #(cond (keyword? %) [:db/ident %]
                                                            (vector? %) %) refs)))]
              (is (= expected-reads @reads))
              (is (= (datoms expected) (datoms report)))
              (is (= (:tempids expected) (:tempids report)))
              (is (= {:probe true} (:tx-meta report))))))))))

(deftest resolved-aliases-and-repeated-pairs-retain-scalar-rejection
  (with-open [conn (d/create-conn nil schema options)]
    (seed! conn)
    (doseq [deferred? [false true]
            refs [[[:key "one"] [:alias "alias"]]
                  [[:key "one"] :entity/one]
                  [1 [:key "one"]]
                  [[:key "one"] [:key "one"]]]]
      (let [reads (atom []) conversions (atom 0)
            value (proxy [Object] [] (toString [] (swap! conversions inc) "new"))
            database (db/transfer @conn (observed-store (:store @conn) reads))
            operations (mapv #(vector :db/add % :text value) refs)]
        (binding [common/*batch-prepare* (batch-context)]
          (let [prepared (db/prepare-scalar-update-tx
                           database operations {:defer-entity-resolution? deferred?})]
            (if prepared
              (do (is deferred?)
                  (is (= 2 @conversions))
                  (is (nil? (db/stamp-scalar-update-tx database prepared nil))))
              (is (zero? @conversions)))))
        (is (= "old" (:text (d/entity @conn 1))))))))

(deftest prepared-reference-plans-observe-current-owners-on-each-request
  (with-open [conn (d/create-conn nil schema options)]
    (seed! conn)
    (let [reads (atom []) store (observed-store (:store @conn) reads)
          database #(db/transfer @conn store)
          forms (mapv #(vector :db/add [:key "one"] % 1) fields)
          prepared (db/prepare-scalar-update-tx (database) forms
                                                 {:defer-entity-resolution? true})]
      (binding [common/*batch-prepare* (batch-context)]
        (is (= #{1} (set (map :e (:tx-data (db/stamp-scalar-update-tx (database) prepared nil)))))))
      (d/transact! conn [[:db/retract 1 :key "one"]])
      (d/transact! conn [[:db/add 2 :key "one"]])
      (binding [common/*batch-prepare* (batch-context)]
        (is (= #{2} (set (map :e (:tx-data (db/stamp-scalar-update-tx (database) prepared nil)))))))
      (is (= [[:key "one"] [:key "one"]] @reads))
      (let [missing (db/prepare-scalar-update-tx
                      (database) (mapv #(vector :db/add [:key "later"] % 1) fields)
                      {:defer-entity-resolution? true})]
        (binding [common/*batch-prepare* (batch-context)]
          (is (nil? (db/stamp-scalar-update-tx (database) missing nil))))
        (d/transact! conn [{:db/id 3 :key "later"}])
        (binding [common/*batch-prepare* (batch-context)]
          (is (= #{3} (set (map :e (:tx-data (db/stamp-scalar-update-tx (database) missing nil)))))))
        (is (= [[:key "later"] [:key "later"]] (vec (take-last 2 @reads))))))))

(deftest scalar-reference-plans-observe-pending-owners-and-aborts
  (doseq [wal? [false true]]
    (with-open [conn (d/create-conn nil schema (assoc options :wal? wal?))]
      (seed! conn)
      (d/with-transaction [tx conn]
        (d/transact! tx [[:db/retract 1 :key "one"]])
        (d/transact! tx [[:db/add 2 :key "one"]])
        (d/transact! tx (mapv #(vector :db/add [:key "one"] % 1) fields))
        (is (= (repeat 7 1) (map #(get (d/entity @tx 2) %) fields)))
        (is (= (repeat 7 0) (map #(get (d/entity @tx 1) %) fields)))
        (d/transact! tx [[:db/retract 2 :key "one"]])
        (d/transact! tx [[:db/add 1 :key "one"]])
        (d/transact! tx (mapv #(vector :db/add [:key "one"] % 2) fields))
        (is (= (repeat 7 2) (map #(get (d/entity @tx 1) %) fields)))
        (is (= (repeat 7 1) (map #(get (d/entity @tx 2) %) fields)))
        (d/abort-transact tx))
      (is (= "one" (:key (d/entity @conn 1))))
      (is (= "two" (:key (d/entity @conn 2))))
      (is (= (repeat 7 0) (map #(get (d/entity @conn 1) %) fields)))
      (is (= (repeat 7 20) (map #(get (d/entity @conn 2) %) fields))))))
