(ns datalevin.db.regular-tx-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [datalevin.core :as d]
   [datalevin.datom :as datom]
   [datalevin.db :as db]
   [datalevin.db.tx.execute :as execute]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:row/part {:db/valueType :db.type/long :db/noindex true}
   :row/number {:db/valueType :db.type/long :db/noindex true}
   :row/key {:db/tupleAttrs [:row/part :row/number]
             :db/unique :db.unique/value}
   :row/value {:db/valueType :db.type/long}
   :row/name {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :row/child {:db/valueType :db.type/ref :db/isComponent true}})

(deftest resolved-reference-map-eligibility
  (testing "resolved references can use the flat map path"
    (is (#'execute/regular-map? schema
         {:db/id -1 :row/child 1 :row/part 2 :row/number 1})))
  (testing "reference forms requiring resolution or expansion retain the general path"
    (doseq [ref [-2 "child" [:row/name "child"] {:row/name "child"}
                 :db/current-tx :child nil 1.5]]
      (is (not (#'execute/regular-map? schema {:db/id -1 :row/child ref}))
          (pr-str ref)))))

(deftest regular-maps-compose-with-complex-operations
  (doseq [wal? [false true]]
    (let [dir (u/tmp-dir (str "regular-tx-" (random-uuid)))
          conn (d/create-conn dir schema {:wal? wal?})]
      (try
        (d/transact! conn [{:db/id 1 :row/part 1 :row/number 1 :row/value 10}
                           {:db/id 2 :row/name "existing" :row/value 20}])
        (testing "flat map allocation survives a later identity upsert retry"
          (let [report (d/transact! conn
                                   [{:db/id "alias" :row/value 21}
                                    [:db/add "alias" :row/name "existing"]
                                    {:db/id "alias" :row/value 22}])]
            (is (= 2 (get (:tempids report) "alias")))
            (is (vector? (:tx-data report)))
            (is (every? datom/datom? (:tx-data report)))
            (is (= 22 (:row/value (d/entity @conn 2))))))
        (testing "tuple sources, nested refs, callbacks and CAS share pending state"
          (let [report
                (d/transact!
                  conn
                  [[:db/add 1 :row/value 11]
                   {:db/id "new" :row/part 2 :row/number 1 :row/value 30}
                   {:db/id "parent" :row/child {:db/id "new" :row/value 31}}
                   [:db.fn/call
                    (fn [database]
                      (let [eid (d/entid database [:row/key [2 1]])]
                        (is (some? eid))
                        [[:db/cas eid :row/value 31 32]]))]])
                eid (get (:tempids report) "new")
                parent (get (:tempids report) "parent")]
            (is (= eid (d/entid @conn [:row/key [2 1]])))
            (is (= 32 (:row/value (d/entity @conn eid))))
            (is (= eid (:db/id (:row/child (d/entity @conn parent)))))
            (is (= 11 (:row/value (d/entity @conn 1))))))
        (testing "a finalized report remains immutable after another transaction"
          (let [report (d/transact! conn [{:db/id 1 :row/value 12}])
                saved (mapv (juxt :e :a :v datom/datom-added) (:tx-data report))]
            (is (vector? (:tx-data report)))
            (d/transact! conn [{:db/id 1 :row/value 11}])
            (is (= saved (mapv (juxt :e :a :v datom/datom-added) (:tx-data report))))))
        (testing "a later constraint failure rolls back preceding ordinary writes"
          (is (thrown? Exception
                       (d/transact! conn
                                    [[:db/add 1 :row/value 99]
                                     {:db/id -1 :row/part 1 :row/number 1}])))
          (is (= 11 (:row/value (d/entity @conn 1)))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest ordered-map-and-add-updates-match-a-register-model
  (let [dir (u/tmp-dir (str "regular-tx-model-" (random-uuid)))
        conn (d/create-conn dir schema {:wal? true})]
    (try
      (d/transact! conn [{:db/id 1 :row/part 1 :row/number 1 :row/value 0}])
      (let [before @conn
            property
            (prop/for-all
              [steps (gen/vector (gen/tuple gen/boolean
                                            (gen/choose 1 8)
                                            (gen/choose -10 10)) 1 60)]
              (let [txs (mapv (fn [[map? eid value]]
                                (if map?
                                  {:db/id eid :row/value value}
                                  [:db/add eid :row/value value])) steps)
                    expected (reduce (fn [m [_ eid value]] (assoc m eid value))
                                     {1 0} steps)
                    report (db/prepare-local-tx-data
                             (db/->TxReport before
                                            (db/transfer before (:store before))
                                            [] {} nil)
                             txs)
                    actual (reduce (fn [state datom]
                                     (if (= :row/value (:a datom))
                                       (if (datom/datom-added datom)
                                         (assoc state (:e datom) (:v datom))
                                         (if (= (get state (:e datom)) (:v datom))
                                           (dissoc state (:e datom))
                                           (reduced ::invalid-retraction)))
                                       state))
                                   {1 0} (:tx-data report))]
                (= expected actual)))
            result (tc/quick-check 100 property :seed 42)]
        (is (:pass? result) (pr-str result)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
