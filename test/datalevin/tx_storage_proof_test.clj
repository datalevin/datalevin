(ns datalevin.tx-storage-proof-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.db.tx.compile :as compile]
            [datalevin.index :as idx]
            [datalevin.interface :as i]
            [datalevin.prepare :as prepare]
            [datalevin.storage :as storage])
  (:import [datalevin.db.tx.plan MixedOperands]
           [java.util HashMap]))

(def schema {:value {:db/aid 1 :db/valueType :db.type/long}
             :text {:db/aid 2 :db/valueType :db.type/string :db/noindex true}})

(deftest resolved-independence-belongs-to-one-trace-and-schema
  (let [epoch (atom schema)
        store (reify i/IStore (schema [_] @epoch))
        datoms [(datom/datom 1 :value 0 1 false) (datom/datom 1 :value 1)]
        report {:db-after {:store store} :tx-data datoms
                :datalevin.db/independent-scalar? [schema datoms]}]
    (is (db/request-storage-independent? report))
    (is (not (db/request-storage-independent? (assoc report :tx-data (conj datoms (datom/datom 2 :value 2))))))
    (is (not (db/request-storage-independent? (assoc report :tx-data (with-meta datoms {:copy true})))))
    (is (not (db/request-storage-independent? (assoc report :datalevin.db/independent-scalar? true))))
    (reset! epoch (with-meta schema {:new-epoch true}))
    (is (not (db/request-storage-independent? report)))
    (is (not (contains? (db/clear-compiled-request-route report) :datalevin.db/independent-scalar?)))))

(deftest group-metadata-seeds-owned-physical-entries-from-logical-bindings
  (let [context (prepare/->PrepareCtx schema {} {})
        plan (compile/compile-tx context [[:db/add 1 :value 7] {:text "new"}])
        bindings (.-infos ^MixedOperands (:entries plan))
        lookups (atom [])
        epoch (fn [a] (swap! lookups conj a) (schema a))
        physical (HashMap.)]
    (is (= :mixed (:strategy plan)))
    (#'storage/seed-storage-attributes! physical epoch epoch bindings)
    (let [first-info (.get physical :text)]
      (is (= [2 :db.type/string true (:text schema)] (vec first-info)))
      (is (identical? (:text schema) (aget ^objects first-info 3)))
      (is (not (identical? first-info (.get bindings :text))))
      (dotimes [_ 8]
        (#'storage/seed-storage-attributes! physical epoch epoch bindings)
        (#'storage/group-storage-attr-info! physical epoch :text))
      (is (empty? @lookups))
      (is (identical? first-info (.get physical :text)))
      (#'storage/group-storage-attr-info! physical epoch :missing)
      (is (= [:missing] @lookups))
      ;; Clearing request scratch cannot alter the group's encoding metadata.
      (.clear ^HashMap bindings)
      (is (= [2 :db.type/string true (:text schema)] (vec (.get physical :text)))))))

(deftest stale-and-incomplete-metadata-uses-current-schema-resolution
  (let [context (prepare/->PrepareCtx schema {} {})
        plan (compile/compile-tx context [[:db/add 1 :value 7] {:text "new"}])
        bindings (.-infos ^MixedOperands (:entries plan))
        changed (assoc-in schema [:text :db/noindex] false)
        attrs (HashMap.)]
    (#'storage/seed-storage-attributes! attrs changed schema bindings)
    (is (.isEmpty attrs))
    (let [info (#'storage/group-storage-attr-info! attrs changed :text)]
      (is (= [2 :db.type/string false (:text changed)] (vec info)))
      (is (= (idx/value-type (:text changed)) (aget ^objects info 1))))))
