(ns datalevin.db.mixed-attr-info-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.test.core :refer [db-fixture]]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:counter {:db/valueType :db.type/long}
   :value {:db/valueType :db.type/long}
   :label {:db/valueType :db.type/string}
   :link {:db/valueType :db.type/ref}
   :part {:db/valueType :db.type/long}
   :key {:db/tupleAttrs [:link :part] :db/unique :db.unique/value}
   :alias {:db/valueType :db.type/tuple
           :db/tupleTypes [:db.type/ref :db.type/long]}
   :token {:db/valueType :db.type/string :db/unique :db.unique/value}})

(def ^:private opts {:wal? true :background-sampling? false :snapshot-scheduler? false})

(def ^:private first-entity
  {:db/id "first" :value 10 :label 123 :link 2 :part 1
   :alias [2 1] :token "fresh-a"})

(def ^:private second-entity
  {:db/id "second" :value 20 :label 456 :link "first" :part 2
   :alias [2 2] :token "fresh-b"})

(defn- prepare [conn first-map second-map]
  (db/prepare-mixed-add-tx @conn
    [[:db/add 1 :counter 1] first-map second-map] nil))

(defn- values-of [report attr]
  (mapv :v (filter #(= attr (:a %)) (:tx-data report))))

(deftest cached-classification-keeps-per-entity-values-and-uniqueness
  (with-open [conn (d/create-conn nil schema opts)]
    (d/transact! conn [{:db/id 1 :counter 0} {:db/id 2 :token "taken"}])
    (let [before @conn
          indexes [(vec (:eavt before)) (vec (:avet before))]
          report (prepare conn first-entity second-entity)
          first-id (get (:tempids report) "first")]
      (is (some? report))
      (is (= [10 20] (values-of report :value)))
      (is (= ["123" "456"] (values-of report :label)))
      (is (= [2 first-id] (values-of report :link)))
      (is (= [[2 1] [first-id 2]] (values-of report :key)))
      (doseq [[label invalid]
              [[:unresolved-reference (assoc second-entity :link "missing")]
               [:unresolved-tuple-reference (assoc second-entity :alias ["first" 2])]
               [:nil-value (assoc second-entity :value nil)]
               [:duplicate-value (assoc second-entity :token "fresh-a")]
               [:stored-value (assoc second-entity :token "taken")]
               [:duplicate-tuple (assoc second-entity :link 2 :part 1)]]]
        (testing (name label)
          (is (nil? (prepare conn first-entity invalid)))
          (is (= indexes [(vec (:eavt before)) (vec (:avet before))]))))
      (is (some? (prepare conn first-entity second-entity))))))

(deftest cached-classification-still-validates-each-value
  (with-open [conn (d/create-conn nil schema (assoc opts :validate-data? true))]
    (d/transact! conn [{:db/id 1 :counter 0} {:db/id 2 :token "taken"}])
    (let [first-map (assoc first-entity :label "first")
          second-map (assoc second-entity :label "second")]
      (is (some? (prepare conn first-map second-map)))
      (is (thrown-with-msg? Exception #"Invalid data"
            (prepare conn first-map (assoc second-map :value "not a long"))))
      (is (some? (prepare conn first-map second-map))))))

(deftest attribute-classification-does-not-survive-a-preparation-call
  (with-open [conn (d/create-conn nil schema opts)]
    (d/transact! conn [{:db/id 1 :counter 0} {:db/id 2 :token "taken"}])
    (is (some? (prepare conn first-entity second-entity)))
    (d/update-schema conn {:value {:db/cardinality :db.cardinality/many}})
    (is (nil? (prepare conn first-entity second-entity)))
    (is (some? (prepare conn (dissoc first-entity :value)
                            (dissoc second-entity :value))))
    (d/update-schema conn {:key {:db/unique :db.unique/identity}})
    (is (nil? (prepare conn (dissoc first-entity :value)
                           (dissoc second-entity :value)))))
  (with-open [conn (d/create-conn nil schema opts)]
    (d/transact! conn [{:db/id 1 :counter 0} {:db/id 2 :token "taken"}])
    (is (some? (prepare conn first-entity second-entity)))
    (d/update-schema conn {:counter+part {:db/tupleAttrs [:counter :part]}})
    (is (nil? (prepare conn first-entity second-entity)))))
