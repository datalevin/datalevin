(ns datalevin.db.tuple-tempid-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.test.core :refer [db-fixture]]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:pair {:db/valueType :db.type/tuple :db/tupleType :db.type/ref}
   :mixed {:db/valueType :db.type/tuple
           :db/tupleTypes [:db.type/ref :db.type/string :db.type/ref :db.type/ref]}
   :name {:db/valueType :db.type/string}
   :key {:db/valueType :db.type/string :db/unique :db.unique/identity}})

(def ^:private options
  {:background-sampling? false :snapshot-scheduler? false :validate-data? true})

(deftest tuple-forward-references-allocate-distinct-entities
  (doseq [wal? [false true]]
    (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
      (let [report (d/transact! connection [{:db/id "holder" :pair ["a" "b"]}
                                           {:db/id "a" :name "Alice"}
                                           {:db/id "b" :name "Bob"}])
            holder (get (:tempids report) "holder")
            alice (get (:tempids report) "a")
            bob (get (:tempids report) "b")]
        (is (= 3 (count (set [holder alice bob]))))
        (is (= [alice bob] (:pair (d/entity @connection holder))))
        (is (= "Alice" (:name (d/entity @connection alice))))
        (is (= "Bob" (:name (d/entity @connection bob))))
        (is (= #{[alice "Alice"] [bob "Bob"]}
               (d/q '[:find ?e ?name :where [?e :name ?name]] @connection)))))))

(deftest repeated-tuple-references-reuse-current-tempid-mappings
  (doseq [wal? [false true] prepare? [false true]
          [alice-tempid bob-tempid] [["a" "b"] [-1 -2]]
          attr [:pair :mixed]]
    (binding [c/*use-prepare-path* prepare?]
      (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
        (let [value (if (= attr :pair)
                      [alice-tempid bob-tempid alice-tempid]
                      [alice-tempid "a" bob-tempid alice-tempid])
              report (d/transact! connection [{:db/id "holder" attr value}
                                               {:db/id alice-tempid :name "Alice"}
                                               {:db/id bob-tempid :name "Bob"}])
              holder (get (:tempids report) "holder")
              alice (get (:tempids report) alice-tempid)
              bob (get (:tempids report) bob-tempid)
              expected (if (= attr :pair) [alice bob alice] [alice "a" bob alice])]
          (is (= 3 (count (set [holder alice bob]))))
          (is (= expected (get (d/entity @connection holder) attr)))
          (is (= "Alice" (:name (d/entity @connection alice))))
          (is (= "Bob" (:name (d/entity @connection bob))))
          (is (= 3 (:max-eid (:db-after report)))))))))

(deftest tuple-references-reuse-earlier-allocations-across-tuples
  (doseq [wal? [false true]]
    (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
      (let [report (d/transact! connection [{:db/id "a" :name "Alice"}
                                           {:db/id "holder" :pair ["a" "b"]}
                                           {:db/id "other" :pair ["b" "a" "b"]}
                                           {:db/id "b" :name "Bob"}])
            ids (:tempids report)
            alice (ids "a") bob (ids "b")]
        (is (= 4 (count (set (map ids ["a" "b" "holder" "other"])))))
        (is (= [alice bob] (:pair (d/entity @connection (ids "holder")))))
        (is (= [bob alice bob] (:pair (d/entity @connection (ids "other")))))
        (is (= "Alice" (:name (d/entity @connection alice))))
        (is (= "Bob" (:name (d/entity @connection bob))))
        (is (= 4 (:max-eid (:db-after report))))))))

(deftest tuple-forward-references-survive-late-identity-upserts
  (doseq [wal? [false true] prepare? [false true]]
    (binding [c/*use-prepare-path* prepare?]
      (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
        (d/transact! connection [{:db/id 10 :key "alice" :name "Alice"}
                                {:db/id 20 :key "bob" :name "Bob"}])
        (let [report (d/transact! connection [{:db/id "holder" :pair ["a" "b" "a"]}
                                             {:db/id "a" :key "alice" :name "Alice updated"}
                                             {:db/id "b" :key "bob" :name "Bob updated"}])
              holder (get (:tempids report) "holder")]
          (is (= 10 (get (:tempids report) "a")))
          (is (= 20 (get (:tempids report) "b")))
          (is (= [10 20 10] (:pair (d/entity @connection holder))))
          (is (= "Alice updated" (:name (d/entity @connection 10))))
          (is (= "Bob updated" (:name (d/entity @connection 20))))
          (is (= #{10 20 holder} (set (map :e (d/datoms @connection :eav)))))
          (is (= 21 (:max-eid (:db-after report)))))))))

(deftest missing-tuple-tempids-fail-without-publishing-partial-entities
  (doseq [wal? [false true]]
    (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
      (d/transact! connection [{:db/id 1 :name "Holder"}])
      (is (thrown? Exception
                   (d/transact! connection [[:db/add 1 :pair ["missing" "defined"]]
                                            {:db/id "defined" :name "Bob"}])))
      (is (= #{[1 "Holder"]} (d/q '[:find ?e ?name :where [?e :name ?name]] @connection)))
      (is (nil? (:pair (d/entity @connection 1))))
      (is (= 1 (:max-eid @connection))))))

(deftest tuple-tempid-failures-restore-pending-state-and-outer-aborts
  (doseq [wal? [false true]]
    (with-open [connection (d/create-conn nil schema (assoc options :wal? wal?))]
      (d/transact! connection [{:db/id 1 :name "Holder"}])
      (d/with-transaction [tx connection]
        (d/transact! tx [{:db/id 1 :name "Pending holder"}])
        (is (thrown? Exception
                     (d/transact! tx [[:db/add 1 :pair ["missing" "defined"]]
                                      {:db/id "defined" :name "Bob"}])))
        (is (= "Pending holder" (:name (d/entity @tx 1))))
        (is (nil? (:pair (d/entity @tx 1))))
        (let [report (d/transact! tx [[:db/add 1 :pair ["a" "b"]]
                                      {:db/id "a" :name "Alice"}
                                      {:db/id "b" :name "Bob"}])]
          (is (= 2 (get (:tempids report) "a")))
          (is (= 3 (get (:tempids report) "b")))
          (is (= [2 3] (:pair (d/entity @tx 1))))
          (is (= "Alice" (:name (d/entity @tx 2))))
          (is (= "Bob" (:name (d/entity @tx 3)))))
        (d/abort-transact tx))
      (is (= #{[1 "Holder"]} (d/q '[:find ?e ?name :where [?e :name ?name]] @connection)))
      (is (nil? (:pair (d/entity @connection 1))))
      (is (= 1 (:max-eid @connection))))))
