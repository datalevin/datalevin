(ns datalevin.db.mixed-planner-passes-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.constants :as c]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.db.tx.common :as common]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [db-fixture]])
  (:import [java.util HashMap]
           [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:counter {:db/valueType :db.type/long}
   :label {:db/valueType :db.type/string}
   :extra {:db/valueType :db.type/long}
   :token {:db/valueType :db.type/string :db/unique :db.unique/value}})
(def ^:private opts {:background-sampling? false :snapshot-scheduler? false})

(defn- request [n]
  (into [[:db/add 1 :counter 1] [:db/add 1 :label "changed"]]
        (map (fn [index]
               {:db/id (str "new-" index) :counter index :extra index
                :label "new" :token (str "token-" index)}))
        (range (- n 2))))

(defn- datoms [report]
  (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data report)))

(defn- prepare [database entities]
  (db/prepare-mixed-add-tx database entities nil (:max-eid database)))

(defn- interpreted [database entities]
  (db/prepare-local-tx-data
    (db/->TxReport database (db/transfer database (:store database)) [] {} nil)
    entities))

(deftest preflight-and-small-plans-preserve-validation-and-report-order
  (with-open [conn (d/create-conn nil schema opts)]
    (d/transact! conn [{:db/id 1 :counter 0 :label "before"}])
    (let [database @conn
          indexes [(vec (:eavt database)) (vec (:avet database))]]
      (doseq [n [31 32 129]]
        (testing (str n " forms")
          (let [entities (request n)
                fast (prepare database entities)
                slow (interpreted database entities)]
            (is (some? fast))
            (is (= (datoms slow) (datoms fast)))
            (is (= (:tempids slow) (:tempids fast)))
            (is (= (select-keys (:db-after slow) [:max-eid :max-tx])
                   (select-keys (:db-after fast) [:max-eid :max-tx])))
            (doseq [invalid [(conj entities [:db/add 1 :counter 2])
                             (assoc entities 0 [:db/add 99999 :counter 2])
                             (assoc entities 0 [:db/add 1 :token "not-updatable"])]]
              (is (nil? (prepare database invalid))))
            (is (= indexes [(vec (:eavt database)) (vec (:avet database))]))))))))

(deftest late-unique-conflicts-discard-partial-datoms
  (doseq [wal? [false true]]
    (with-open [conn (d/create-conn nil schema (assoc opts :wal? wal?))]
      (d/transact! conn [{:db/id 1 :counter 0 :label "before"}
                        {:db/id 2 :token "taken"}])
      (d/with-transaction [tx conn]
        (let [database @tx entities (request 130)
              indexes [(vec (:eavt database)) (vec (:avet database))]
              count-before (d/entries (d/datalog-kv tx) c/eav)
              max-before (i/init-max-eid (:store database))
              valid (prepare database entities)]
          (is (some? valid))
          (doseq [token ["token-0" "taken"]]
            (is (nil? (prepare database
                               (assoc-in entities [(dec (count entities)) :token] token))))
            (is (= indexes [(vec (:eavt database)) (vec (:avet database))]))
            (is (= count-before (d/entries (d/datalog-kv tx) c/eav)))
            (is (= max-before (i/init-max-eid (:store database)))))
          (is (= (datoms valid) (datoms (prepare database entities))))
          (is (= "before" (:label (d/entity @tx 1)))))
        (d/abort-transact tx)))))

(deftest tuple-bookkeeping-keeps-only-normalized-sources
  (let [tuple-schema (merge schema
                       {:left {:db/valueType :db.type/string}
                        :right {:db/valueType :db.type/long}
                        :other {:db/valueType :db.type/long}
                        :pair {:db/tupleAttrs [:left :right]}
                        :second-pair {:db/tupleAttrs [:left :other]}})]
    (with-open [conn (d/create-conn nil tuple-schema opts)]
      (d/transact! conn [{:db/id 1 :counter 0 :label "before"}])
      (doseq [n [4 64]]
        (let [entities
              (into [[:db/add 1 :counter 1]]
                    (map (fn [index]
                           (cond-> {:db/id (str "new-" index) :counter index :extra index}
                             (not= 0 (mod index 4)) (assoc :left index)
                             (#{2 3} (mod index 4)) (assoc :right index)
                             (= 3 (mod index 4)) (assoc :other (inc index)))))
                    (range n))
              fast (prepare @conn entities)
              slow (interpreted @conn entities)
              tuples #{:pair :second-pair}
              normal #(remove (fn [d] (tuples (nth d 1))) (datoms %))
              derived #(set (filter (fn [d] (tuples (:a d))) (:tx-data %)))]
          (is (some? fast))
          (is (= (normal slow) (normal fast)))
          (is (= (derived slow) (derived fast)))
          (is (= (:tempids slow) (:tempids fast)))
          (is (= n (count (filter #(= :extra (:a %)) (:tx-data fast)))))
          (is (some #(and (= :pair (:a %)) (= ["1" nil] (:v %))) (:tx-data fast))))))))

(deftest late-uniqueness-checks-respect-pending-overlays
  (doseq [mode [:scalar :sorted]]
    (testing (name mode)
      (with-open [conn (d/create-conn nil schema opts)]
        (d/transact! conn [{:db/id 1 :counter 0 :label "before"}])
        (d/with-transaction [tx conn]
          (let [view (assoc (db/transfer @tx (:store @tx)) :max-eid 2)
                pending (LongObjectHashMap.)
                unique (HashMap.)
                context (if (= mode :scalar)
                          (reify common/ScalarPendingIndex
                            (pending-scalar-index [_] pending)
                            (pending-unique-index [_] unique)
                            (retain-pending-index? [_] false)
                            (prepare-pending-index! [_ _])
                            (materialize-scalar-pending! [_ _])
                            (discard-scalar-pending! [_]))
                          {:test true})]
            (binding [common/*batch-prepare* context]
              (doseq [row [(datom/datom 1 :counter 0 100 false)
                           (datom/datom 1 :counter 8 100 true)
                           (datom/datom 2 :token "pending" 100 true)]]
                (common/stage-batch-datom! view row))
              (let [entities (request 130)
                    indexes [(vec (:eavt view)) (vec (:avet view))]
                    valid (prepare view entities)]
                (is (some? valid))
                (is (= [8 1] (mapv :v (filter #(= :counter (:a %))
                                             (take 2 (:tx-data valid))))))
                (is (nil? (prepare view (assoc-in entities [129 :token] "pending"))))
                (is (= 8 (common/ea-first-v view 1 :counter)))
                (is (= 2 (common/av-first-e view :token "pending")))
                (is (= indexes [(vec (:eavt view)) (vec (:avet view))]))
                (is (= (datoms valid) (datoms (prepare view entities)))))))
          (d/abort-transact tx))))))
