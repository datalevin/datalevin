(ns datalevin.db.grouped-values-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.db.tx.common :as common]
            [datalevin.test.core :refer [db-fixture]])
  (:import [java.util HashMap]
           [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:value {:db/valueType :db.type/long :db/noindex true}
   :flag {:db/valueType :db.type/boolean}
   :text {:db/valueType :db.type/string}
   :bytes {:db/valueType :db.type/bytes}
   :amount {:db/valueType :db.type/bigdec}
   :absent {:db/valueType :db.type/long}
   :parent {:db/valueType :db.type/ref}
   :part {:db/valueType :db.type/long}
   :key {:db/tupleAttrs [:parent :part] :db/unique :db.unique/value}})

(defn- read-values [view e attrs]
  (let [read (when (:store view) (common/ea-first-v-reader view))]
    (mapv #(if read (read e %) (common/ea-first-v view e %)) attrs)))

(defn- comparable [values]
  (mapv #(if (bytes? %) (vec %) %) values))

(deftest grouped-reads-match-point-reads-and-own-decoded-values
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (let [text (apply str (repeat 2000 "value"))
          bytes (byte-array 2000 (byte 3))]
      (d/transact! conn [{:db/id 1 :value 5 :flag false :text text
                         :bytes bytes :amount 1.00M}
                        {:db/id 2 :value 7}])
      (doseq [attrs [[:value :flag :text :bytes :amount :absent]
                     [:absent :amount :bytes :text :flag :value]
                     [:value] []]
              eid [1 2 999]]
        (is (= (comparable (mapv #(common/ea-first-v @conn eid %) attrs))
               (comparable (read-values @conn eid attrs)))))
      (d/with-transaction [tx conn]
        (let [[saved] (read-values @tx 1 [:bytes :text :flag])]
          (is (some? (common/ea-first-v-reader @tx)))
          (dotimes [_ 3]
            (is (= [text false] (read-values @tx 1 [:text :flag]))))
          (is (= (vec bytes) (vec saved)))))
      (is (= "1.00" (str (first (read-values @conn 1 [:amount]))))))))

(deftest grouped-reads-merge-the-sorted-pending-index
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :value 5 :flag false :text "stored"}])
    (d/with-transaction [tx conn]
      (let [view (db/transfer @tx (:store @tx))]
        (binding [common/*batch-prepare* {:test true}]
          (doseq [datom [(datom/datom 1 :value 5 100 false)
                         (datom/datom 1 :value 8 100 true)
                         (datom/datom 1 :flag false 100 false)
                         (datom/datom 1 :absent 9 100 true)]]
            (common/stage-batch-datom! view datom))
          (doseq [attrs [[:value :flag :text :absent]
                         [:text :absent :flag :value]]]
            (is (some? (common/ea-first-v-reader view)))
            (is (= (mapv #(common/ea-first-v view 1 %) attrs)
                   (read-values view 1 attrs))))
          (is (= [8 nil "stored" 9]
                 (read-values view 1 [:value :flag :text :absent]))))))
    (is (= [5 false "stored" nil]
           (read-values @conn 1 [:value :flag :text :absent])))))

(deftest prepared-reads-merge-the-scalar-pending-index
  (let [pending (LongObjectHashMap.)
        attrs (doto (HashMap.)
                (.put :value (datom/datom 1 :value 8 100 true))
                (.put :flag (datom/datom 1 :flag false 100 true))
                (.put :absent (datom/datom 1 :absent 9 100 false)))
        context (reify common/ScalarPendingIndex
                  (pending-scalar-index [_] pending)
                  (pending-unique-index [_] nil)
                  (retain-pending-index? [_] false)
                  (prepare-pending-index! [_ _])
                  (materialize-scalar-pending! [_ _])
                  (discard-scalar-pending! [_]))]
    (.put pending (long 1) attrs)
    (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
      (d/transact! conn [{:db/id 1 :value 5 :flag true :absent 9 :text "native"}])
      (d/with-transaction [tx conn]
        (binding [common/*batch-prepare* context]
          (is (some? (common/ea-first-v-reader @tx)))
          (is (= [8 false nil "native"]
                 (read-values @tx 1 [:value :flag :absent :text]))))))))

(deftest interleaved-entity-updates-retain-report-order
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :value 5 :flag false}
                      {:db/id 2 :value 7 :flag true}])
    (let [txs [[:db/add 2 :value 8]
               {:db/id "new" :parent 1 :part 9}
               [:db/add 1 :flag true]
               [:db/add 2 :flag false]
               [:db/add 1 :value 6]]
          view @conn
          fast (db/prepare-mixed-add-tx view txs nil)
          slow (db/prepare-local-tx-data
                 (db/->TxReport view (db/transfer view (:store view)) [] {} nil) txs)
          datoms #(mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data %))]
      (is (some? fast))
      (is (= (datoms slow) (datoms fast)))
      (is (= (:tempids slow) (:tempids fast))))))

(deftest prepared-reader-survives-an-intervening-point-read
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :value 5 :flag false}
                      {:db/id 2 :value 7 :flag true}])
    (d/with-transaction [tx conn]
      (let [read (common/ea-first-v-reader @tx)]
        (is (= 5 (read 1 :value)))
        (is (= 7 (common/ea-first-v @tx 2 :value)))
        (is (false? (read 1 :flag)))))))
