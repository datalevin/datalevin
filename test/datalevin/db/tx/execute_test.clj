(ns datalevin.db.tx.execute-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.db.tx.execute :as execute]
   [datalevin.interface :as i]
   [datalevin.storage.schema :as schema])
  (:import
   [java.util Comparator]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(defn- execute-with-reads
  [schema stored entities]
  (let [reads   (atom [])
        rschema (schema/schema->rschema schema)
        store   (reify i/IStore
                  (opts [_] {})
                  (schema [_] schema)
                  (rschema [_] rschema)
                  (attrs [_] {})
                  (ea-first-datom [_ e a]
                    (swap! reads conj [:ea e a])
                    (some #(when (and (= e (:e %)) (= a (:a %))) %)
                          stored))
                  (fetch [_ datom]
                    (swap! reads conj [:eav (:e datom) (:a datom) (:v datom)])
                    (filter #(= datom %) stored))
                  (av-first-e [_ a v]
                    (swap! reads conj [:av a v])
                    (some #(when (and (= a (:a %)) (= v (:v %))) (:e %))
                          stored)))
        db      {:store   store
                 :max-eid (reduce max c/e0 (map :e stored))
                 :max-tx  c/tx0
                 :eavt    (TreeSortedSet. ^Comparator d/cmp-datoms-eavt)
                 :avet    (TreeSortedSet. ^Comparator d/cmp-datoms-avet)}
        report  (execute/execute-tx-loop
                  {:db-before db :db-after db :tx-data [] :tempids {}}
                  entities 0)]
    [report @reads]))

(deftest new-entity-skips-persisted-value-lookups
  (let [fields         (mapv #(keyword (str "field" %)) (range 10))
        schema         (assoc (zipmap fields (repeat {:db/noindex true}))
                              :ycsb/key {:db/unique :db.unique/value})
        entity         (assoc (zipmap fields (repeat "value"))
                              :db/id -1 :ycsb/key "new")
        [report reads] (execute-with-reads schema [] [entity])]
    (is (= 11 (count (:tx-data report))))
    (is (= 11 (count (get-in report [:db-after :eavt]))))
    (is (= [[1 :ycsb/key "new"]]
           (mapv d/datom-eav (get-in report [:db-after :avet]))))
    (is (= [[:av :ycsb/key "new"]] reads)
        "The unique key still checks other entities; no EAV reads are needed")))

(deftest new-entity-keeps-transaction-local-values
  (let [[report reads]
        (execute-with-reads
          {:value {:db/noindex true}
           :tags {:db/cardinality :db.cardinality/many :db/noindex true}} []
          [[:db/add -1 :value "first"]
           [:db/add -1 :value "second"]
           [:db/add -1 :value "second"]
           [:db/retract 1 :value "second"]
           [:db/add -1 :value "third"]
           [:db/add -1 :tags "a"]
           [:db/add -1 :tags "a"]
           [:db/add -1 :tags "b"]
           [:db/retract 1 :tags "a"]
           [:db/add -1 :tags "a"]])]
    (is (empty? reads))
    (is (empty? (get-in report [:db-after :avet])))
    (is (= [[1 :value "first" true]
            [1 :value "first" false]
            [1 :value "second" true]
            [1 :value "second" false]
            [1 :value "third" true]
            [1 :tags "a" true]
            [1 :tags "b" true]
            [1 :tags "a" false]
            [1 :tags "a" true]]
           (mapv (juxt :e :a :v d/datom-added) (:tx-data report))))
    (is (= #{[1 :value "third"] [1 :tags "a"] [1 :tags "b"]}
           (set (map d/datom-eav (get-in report [:db-after :eavt])))))))

(deftest existing-entities-still-read-persisted-values
  (let [[report reads]
        (execute-with-reads
          {:value {} :tags {:db/cardinality :db.cardinality/many}}
          [(d/datom 5 :tags "a") (d/datom 10 :value "old")]
          [[:db/add 20 :value "new"]
           [:db/add 10 :value "updated"]
           [:db/add 5 :tags "a"]])]
    (is (= [[:ea 10 :value] [:eav 5 :tags "a"]] reads))
    (is (= [[20 :value "new" true]
            [10 :value "old" false]
            [10 :value "updated" true]]
           (mapv (juxt :e :a :v d/datom-added) (:tx-data report))))))

(deftest new-tempid-can-upsert-an-existing-entity
  (doseq [prepare? [false true]]
    (testing (str "prepare path " prepare?)
      (binding [c/*use-prepare-path* prepare?]
        (let [[report reads]
              (execute-with-reads
                {:name {:db/unique :db.unique/identity} :value {}}
                [(d/datom 10 :name "existing") (d/datom 10 :value "old")]
                [[:db/add -1 :value "updated"]
                 [:db/add -1 :name "existing"]])]
          (is (= 10 (get-in report [:tempids -1])))
          (is (= [[10 :value "old" false]
                  [10 :value "updated" true]]
                 (mapv (juxt :e :a :v d/datom-added) (:tx-data report))))
          (is (= [[:ea 10 :value] [:ea 10 :name]]
                 (filterv #(= :ea (first %)) reads))))))))

(deftest new-entity-still-enforces-uniqueness
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unique constraint"
        (execute-with-reads
          {:key {:db/unique :db.unique/value}}
          [(d/datom 10 :key "existing")]
          [[:db/add -1 :key "existing"]]))))
