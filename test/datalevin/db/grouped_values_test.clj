(ns datalevin.db.grouped-values-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.db.tx.common :as common]
            [datalevin.index :as idx]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.storage :as storage]
            [datalevin.test.core :refer [db-fixture]])
  (:import [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]))

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
  (let [read (when (:store view) (storage/ea-first-v-reader (:store view)))]
    (mapv #(if read (read e %) (common/ea-first-v view e %)) attrs)))

(defn- comparable [values]
  (mapv #(if (bytes? %) (vec %) %) values))

(deftest point-reads-resolve-attribute-properties-once
  (with-open [conn (d/create-conn nil (assoc schema :opaque {})
                                 {:wal? false :background-sampling? false})]
    (let [opaque {:nested [1 2]}]
      (d/transact! conn [{:db/id 1 :value 5 :flag false :text "text" :opaque opaque}])
      (let [schema (i/schema (:store @conn))
            calls (atom 0)
            lookup (fn [attr] (swap! calls inc) (schema attr))
            read (fn [lmdb attr]
                   (reset! calls 0)
                   (let [value (#'storage/read-ea-first-value
                                 lmdb lookup 1 attr idx/avg-buffer->v)]
                     (is (= 1 @calls) (str attr))
                     value))
            attrs [:value :flag :text :opaque :absent :unknown]
            expected [5 false "text" opaque nil nil]]
        (is (= expected (mapv #(read (d/datalog-kv conn) %) attrs)))
        (d/with-transaction-kv [writer (d/datalog-kv conn)]
          (let [native (kv/raw-lmdb writer)]
            (is (l/writing? native))
            (is (Thread/holdsLock (l/write-txn native)))
            (is (not (:datalog-prepare? (l/request-context native))))
            (is (= expected (mapv #(read writer %) attrs)))))))))

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
          (is (some? (storage/ea-first-v-reader (:store @tx))))
          (dotimes [_ 3]
            (is (= [text false] (read-values @tx 1 [:text :flag]))))
          (is (= (vec bytes) (vec saved)))))
      (is (= "1.00" (str (first (read-values @conn 1 [:amount]))))))))

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
      (let [read (storage/ea-first-v-reader (:store @tx))]
        (is (= 5 (read 1 :value)))
        (is (= 7 (common/ea-first-v @tx 2 :value)))
        (is (false? (read 1 :flag)))))))

(deftest prepared-native-reader-is-used-only-for-unresolved-pending-attributes
  (doseq [mode [:scalar :sorted]]
    (with-open [conn (d/create-conn nil schema
                      {:wal? false :background-sampling? false})]
      (let [bytes (byte-array 2048 (byte 7))]
        (d/transact! conn [{:db/id 1 :value 5 :flag true :text "stored"
                           :bytes bytes :amount 1.00M}])
        (d/with-transaction [tx conn]
          (let [view (db/transfer @tx (:store @tx))
                pending (LongObjectHashMap.)
                context (if (= mode :scalar)
                          (reify common/ScalarPendingIndex
                            (pending-scalar-index [_] pending)
                            (pending-unique-index [_] nil)
                            (retain-pending-index? [_] false)
                            (prepare-pending-index! [_ _])
                            (materialize-scalar-pending! [_ _])
                            (discard-scalar-pending! [_]))
                          {:test true})
                read (storage/ea-first-v-reader (:store view)
                                               idx/avg-buffer->prepared-value)
                calls (atom [])
                read-stored (fn [e a] (swap! calls conj [e a]) (read e a))]
            (is (some? read))
            (binding [common/*batch-prepare* context]
              (doseq [row [(datom/datom 1 :value 5 100 false)
                            (datom/datom 1 :value 8 100)
                            (datom/datom 1 :flag true 100 false)
                            (datom/datom 1 :flag false 100)
                            (datom/datom 1 :text "stored" 100 false)]]
                (common/stage-batch-datom! view row))
              (let [attrs [:value :flag :text :bytes :amount :absent]
                    expected (mapv #(common/ea-first-v view 1 %) attrs)
                    values (mapv #(idx/logical-value
                                    (common/preparation-first-v view 1 % read-stored))
                                 attrs)]
                (is (= (comparable expected) (comparable values)))
                (is (= [8 false nil (vec bytes) 1.00M nil] (comparable values)))
                (is (= [[1 :bytes] [1 :amount] [1 :absent]] @calls))
                ;; An ordinary internal read can reuse the writer buffers.
                (is (= 1.00M (common/ea-first-v view 1 :amount)))
                (is (= (vec bytes)
                       (vec (idx/logical-value
                              (common/preparation-first-v view 1 :bytes read-stored)))))
                (let [before @calls]
                  (is (= 8 (common/preparation-first-v view 1 :value read-stored)))
                  (is (false? (common/preparation-first-v view 1 :flag read-stored)))
                  (is (nil? (common/preparation-first-v view 1 :text read-stored)))
                  (is (= before @calls)))))))))))
