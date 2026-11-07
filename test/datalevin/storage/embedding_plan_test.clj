(ns datalevin.storage.embedding-plan-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.embedding :as emb]
            [datalevin.interface :as i]
            [datalevin.storage :as storage]
            [datalevin.test.core :refer [db-fixture]])
  (:import [java.util IdentityHashMap]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:text {:db/valueType :db.type/string :db/embedding true}
   :ordinary {:db/valueType :db.type/string}})

(defn- options [calls]
  (let [metadata {:embedding/provider {:kind :test :id :plan}
                  :embedding/output {:dimensions 2}}
        provider (reify emb/IEmbeddingProvider
                   (embedding [_ items _]
                     (swap! calls conj (vec items))
                     (mapv (fn [_] (float-array [1.0 0.0])) items))
                   (embedding-metadata [_] metadata)
                   (embedding-dimensions [_] 2)
                   (close-provider [_] nil))]
    {:embedding-providers {:default provider}
     :embedding-opts {:dimensions 2 :embedding-metadata metadata}
     :background-sampling? false :snapshot-scheduler? false}))

(deftest embedding-plans-preserve-datom-identity-and-filter-non-additions
  (let [calls (atom [])]
    (with-open [conn (d/create-conn nil schema (options calls))]
      (let [store (:store @conn)
            ordinary (datom/datom 1 :ordinary "ordinary")
            retract (datom/datom 1 :text "old" 1 false)
            non-string (datom/datom 1 :text 42)
            addition (datom/datom 1 :text "new")]
        (is (nil? (storage/prepare-embedding-plan store [ordinary retract non-string])))
        (is (empty? @calls))
        (let [^IdentityHashMap plan
              (storage/prepare-embedding-plan store [ordinary retract addition])]
          (is (= 1 (.size plan)))
          (is (.containsKey plan addition))
          (is (not (.containsKey plan (datom/datom 1 :text "new"))))
          (is (= [[{:text "new" :attr :text :ref [1 :text "new"]
                    :kind :document :domain c/default-domain}]] @calls)))))))

(deftest ordinary-commits-and-new-embedding-attributes-share-the-gate
  (doseq [wal? [false true]]
    (let [calls (atom [])]
      (with-open [conn (d/create-conn nil schema (assoc (options calls) :wal? wal?))]
        (d/transact! conn (mapv #(hash-map :db/id % :ordinary (str %)) (range 1 65)))
        (is (empty? @calls))
        (is (= "1" (:ordinary (d/entity @conn 1))))
        (d/transact! conn [{:db/id 1 :ordinary "changed" :text "embedded"}])
        (is (= 1 (count @calls)))
        (is (= "embedded" (:text (d/entity @conn 1))))
        (is (= "changed" (:ordinary (d/entity @conn 1))))
        (d/transact! conn [[:db/retract 1 :text "embedded"]])
        (is (= 1 (count @calls)))
        (is (nil? (:text (d/entity @conn 1))))
        (d/update-schema conn {:later {:db/valueType :db.type/string :db/embedding true}})
        (is (= #{:text :later} (:db/embedding (i/rschema (:store @conn)))))
        (d/transact! conn [{:db/id 1 :later "later"}])
        (is (= 2 (count @calls)))
        (is (= "later" (:later (d/entity @conn 1))))))))
