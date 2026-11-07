(ns datalevin.db.incoming-refs-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.datom :as datom]
   [datalevin.db :as db]
   [datalevin.db.tx.common :as txcommon]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:value {:db/valueType :db.type/long}
   :link {:db/valueType :db.type/ref}
   :links {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :child {:db/valueType :db.type/ref :db/isComponent true}})

(defn- with-store [wal? f]
  (let [dir (u/tmp-dir (str "incoming-refs-" (random-uuid)))
        conn (d/create-conn dir schema {:wal? wal? :snapshot-scheduler? false})]
    (try
      (f conn)
      (finally (d/close conn) (u/delete-files dir)))))

(defn- rows [datoms]
  (set (map datom/datom-eav datoms)))

(deftest incoming-references-merge-latest-pending-and-stored-values
  (with-store false
    (fn [conn]
      (d/transact! conn [{:db/id 1 :value 1 :link 1}
                        {:db/id 2 :value 2}
                        {:db/id 10 :link 1 :links [1 2]}
                        {:db/id 11 :link 1 :links [1 2]}
                        {:db/id 12 :child 1}])
      (let [view (db/transfer @conn (:store @conn))
            stored (rows (txcommon/v-datoms view 1))]
        (binding [txcommon/*batch-prepare* {:test true}]
          (doseq [[e a v added?] [[10 :link 1 false] [10 :link 2 true]
                                  [11 :link 1 true]
                                  [10 :links 1 false] [10 :links 2 false]
                                  [10 :links 1 true] [11 :links 1 false]
                                  [20 :link 1 true] [20 :link 1 false]
                                  [21 :link 1 false] [21 :link 1 true]
                                  [22 :links 1 true] [22 :links 2 true]
                                  [23 :child 1 true] [24 :value 1 true]]]
            (txcommon/stage-batch-datom! view (datom/datom e a v 100 added?)))
          (let [indexes [(vec (:eavt view)) (vec (:avet view))]
                incoming (txcommon/v-datoms view 1)]
            (is (= #{[1 :link 1] [11 :link 1] [10 :links 1] [12 :child 1]
                     [21 :link 1] [22 :links 1] [23 :child 1]}
                   (rows incoming)))
            (is (= (count incoming) (count (rows incoming))))
            (is (every? datom/datom-added incoming))
            (is (= #{[10 :link 2] [11 :links 2] [22 :links 2]}
                   (rows (txcommon/v-datoms view 2))))
            (is (empty? (txcommon/v-datoms view 99)))
            (is (= indexes [(vec (:eavt view)) (vec (:avet view))]))))
        (testing "ordinary reads still use the stored references"
          (is (= stored (rows (txcommon/v-datoms view 1)))))))))

(deftest entity-retraction-cleans-up-pending-references-and-components
  (with-store true
    (fn [conn]
      (d/transact! conn [{:db/id 1 :value 1 :child 2}
                        {:db/id 2 :value 2 :child 3}
                        {:db/id 3 :value 3}
                        {:db/id 4 :value 4}
                        {:db/id 10 :links [1 2 3 4]}
                        {:db/id 11 :links [2 4]}])
      (let [txs [[:db/add 20 :child 21]
                 [:db/add 21 :child 22]
                 [:db/add 22 :value 22]
                 [:db/add 12 :link 2]
                 [:db/add 13 :links 3]
                 [:db/retract 13 :links 3]
                 [:db/retract 11 :links 2]
                 [:db/add 14 :link 1]
                 [:db/add 14 :link 4]
                 [:db/add 15 :value 2]
                 [:db/add 16 :links 21]
                 [:db/add 17 :link 22]
                 [:db/retractEntity 1]
                 [:db/retractEntity 20]
                 [:db/retractEntity 1]]
            before @conn
            view (db/transfer before (:store before))
            prepared (binding [txcommon/*batch-prepare* {:test true}]
                       (db/prepare-local-tx-data
                         (db/->TxReport before view [] {} nil) txs))
            expected #{[4 :value 4] [10 :links 4] [11 :links 4]
                       [14 :link 4] [15 :value 2]}
            apply-datom (fn [state row]
                          ((if (datom/datom-added row) conj disj)
                           state (datom/datom-eav row)))]
        (is (= expected (reduce apply-datom (rows (d/datoms before :eav))
                                (:tx-data prepared))))
        (d/transact! conn txs)
        (is (= expected (rows (d/datoms @conn :eav))))))))
