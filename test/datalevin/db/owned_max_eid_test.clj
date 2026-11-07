(ns datalevin.db.owned-max-eid-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.conn :as conn]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:value {:db/valueType :db.type/long}
   :name {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :link {:db/valueType :db.type/ref}
   :tags {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}})

(defn- with-connections [wal? f]
  (let [dir (u/tmp-dir (str "owned-max-eid-" (random-uuid)))
        options {:wal? wal? :background-sampling? false :snapshot-scheduler? false}]
    (try
      (with-open [first-conn (d/create-conn dir schema options)
                  stale-conn (d/create-conn dir schema options)]
        (d/transact! first-conn [{:db/id 1 :name "one" :value 1}
                                {:db/id 100 :value 100}])
        (is (< (:max-eid @stale-conn) 100))
        (f first-conn stale-conn))
      (finally (u/delete-files dir)))))

(deftest preparation-outside-the-writer-refreshes-a-stale-maximum
  (with-connections true
    (fn [_ stale]
      (let [database @stale
            mixed (db/prepare-mixed-add-tx database
                    [[:db/add 1 :value 2] {:db/id -1 :value 3}] nil)
            general (db/prepare-local-tx-data
                      (db/->TxReport database (db/transfer database (:store database))
                                     [] {} nil)
                      [{:db/id -1 :value 4}])]
        (is (= 101 (get (:tempids mixed) -1)))
        (is (= 101 (get (:tempids general) -1)))
        (is (= 101 (:max-eid (:db-after mixed))))
        (is (= 101 (:max-eid (:db-after general))))))))

(defn- exercise-owned-writer [tx]
  (is (= 100 (:max-eid @tx)))
  (let [mixed (d/transact! tx [[:db/add 1 :value 2] {:db/id -1 :value 3}])
        general (d/transact! tx [[:db.fn/call (fn [_] [{:db/id -1 :value 4}])]])]
    (is (= 101 (get (:tempids mixed) -1)))
    (is (= 102 (get (:tempids general) -1)))
    (is (not (contains? general :datalevin.db.tx.execute/max-eid-before))))
  (d/transact! tx [[:db/add 1000 :value 5]])
  (d/transact! tx [[:db/retractEntity 1000]])
  (let [mixed (d/transact! tx [[:db/add 1 :value 6] {:db/id -1 :value 7}])
        ;; A collection prevents flat-upsert pre-resolution. The forward ref
        ;; allocates a temporary target before the identity match forces retry.
        retry (d/transact! tx [{:db/id "new" :value 8 :link "existing"}
                               {:db/id "existing" :name "one" :tags ["a"]}])]
    (is (= 1001 (get (:tempids mixed) -1)))
    (is (= 1002 (get (:tempids retry) "new")))
    (is (= 1 (get (:tempids retry) "existing")))
    (is (= 1002 (:max-eid @tx)))
    (is (not (contains? retry :datalevin.db.tx.execute/max-eid-before))))
  (is (= 8 (:value (d/entity @tx 1002))))
  (is (= 6 (:value (d/entity @tx 1)))))

(deftest owned-requests-reuse-the-current-maximum-across-paths-and-retries
  (doseq [wal? [false true]
          compatibility? [false true]
          prepare? [false true]]
    (testing (str {:wal? wal? :compatibility? compatibility? :prepare? prepare?})
      (binding [c/*use-prepare-path* prepare?]
        (with-connections wal?
          (fn [_ stale]
            (if compatibility?
              (conn/with-compatibility-transaction [tx stale nil]
                (exercise-owned-writer tx))
              (d/with-transaction [tx stale]
                (exercise-owned-writer tx)))
            (is (= 1002 (:max-eid @stale)))
            (is (= 8 (:value (d/entity @stale 1002))))))))))
