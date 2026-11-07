(ns datalevin.storage.embedding-plan-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.embedding :as emb]
            [datalevin.interface :as i]
            [datalevin.storage :as storage]
            [datalevin.test.core :refer [db-fixture]])
  (:import [datalevin.storage Store]
           [java.util IdentityHashMap]))

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
     :background-sampling? false :snapshot-scheduler? false :validate-data? true}))

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

(defn- embedded-texts [conn]
  (set (d/q '[:find [?value ...]
              :where
              [(embedding-neighbors $ "query" {:domains ["datalevin"] :top 20})
               [[_ _ ?value]]]]
            @conn)))

(deftest overwritten-texts-only-embed-the-surviving-addition
  (doseq [wal? [false true]
          giant? [false true]]
    (let [calls (atom [])
          suffix (if giant? (.repeat "x" 2048) "")
          old (str "original " suffix)
          intermediate (str "intermediate " suffix)
          final (str "final " suffix)
          schema (assoc-in schema [:text :db/fulltext] true)]
      (with-open [conn (d/create-conn nil schema (assoc (options calls) :wal? wal?))]
        (d/transact! conn [{:db/id 1 :text old}])
        (reset! calls [])
        (let [report (d/transact! conn [[:db/add 1 :text intermediate]
                                        [:db/add 1 :text final]])
              ^Store store (:store @conn)]
          (is (= [[old false] [intermediate true] [intermediate false] [final true]]
                 (mapv (juxt :v datom/datom-added) (:tx-data report))))
          (is (= [[final]] (mapv #(mapv :text %) @calls)))
          (is (= #{final} (embedded-texts conn)))
          (is (= final (:text (d/entity @conn 1))))
          (is (= 1 (i/doc-count (get (.-search-engines store) c/default-domain))))
          (is (empty? (i/search (get (.-search-engines store) c/default-domain)
                               "intermediate"))))))))

(deftest cancellations-preserve-many-values-and-giant-readditions
  (let [calls (atom [])
        schema (assoc-in schema [:text :db/cardinality] :db.cardinality/many)
        kept (str "kept " (.repeat "x" 2048))]
    (with-open [conn (d/create-conn nil schema (options calls))]
      (d/transact! conn [{:db/id 1 :text kept}])
      (reset! calls [])
      (let [report (d/transact! conn [[:db/retract 1 :text kept]
                                      [:db/add 1 :text "discarded"]
                                      [:db/retract 1 :text "discarded"]
                                      [:db/add 1 :text kept]
                                      [:db/add 1 :text "also kept"]
                                      [:db/add 2 :text "discarded"]])]
        (is (= 6 (count (:tx-data report))))
        (is (= #{kept "also kept" "discarded"}
               (set (map :text (mapcat identity @calls)))))
        (is (= #{kept "also kept" "discarded"} (embedded-texts conn)))))))

(deftest cancelled-bulk-additions-and-reused-datom-instances
  (let [calls (atom [])]
    (with-open [conn (d/create-conn nil schema (options calls))]
      (let [store (:store @conn)
            index (storage/embedding-index-by-domain store c/default-domain)
            addition (datom/datom 1 :text (str "giant " (.repeat "x" 2048)))
            deletion (datom/delete addition)]
        (i/load-datoms store [addition deletion])
        (is (empty? @calls))
        (is (zero? (:size (i/vecs-info index))))
        ;; Reusing one object at distinct positions must retain the final add.
        (i/load-datoms store [addition deletion addition])
        (is (= 1 (:size (i/vecs-info index))))
        (is (= [(:v addition)] (mapv :v (i/e-datoms store 1))))
        (i/load-datoms store [(datom/delete addition)])
        (let [ordinary (datom/datom 1 :text "already stored")]
          (i/load-datoms store [ordinary])
          (i/load-datoms store [ordinary (datom/delete ordinary)])
          (is (zero? (:size (i/vecs-info index))))
          (is (empty? (i/e-datoms store 1))))))))

(deftest commit-segments-and-validation-keep-their-order
  (let [calls (atom [])]
    (with-open [conn (d/create-conn nil schema (options calls))]
      (d/transact! conn [{:db/id 1 :ordinary "seed"}])
      (d/with-transaction [tx conn]
        (d/transact! tx [[:db/add 1 :text "intermediate"]])
        (is (= [["intermediate"]] (mapv #(mapv :text %) @calls)))
        (d/transact! tx [[:db/add 1 :text "final"]])
        (is (= [["intermediate"] ["final"]] (mapv #(mapv :text %) @calls))))
      (reset! calls [])
      (is (thrown? Exception (d/transact! conn [[:db/add 1 :text "overwritten"]
                                                [:db/cas 1 :ordinary "wrong" "new"]
                                                [:db/add 1 :text "valid"]])))
      (is (empty? @calls))
      (is (thrown? Exception (d/transact! conn [[:db/add 1 :text 42]
                                                [:db/add 1 :text "valid"]])))
      (is (empty? @calls))
      (is (= "final" (:text (d/entity @conn 1)))))))

(deftest asynchronous-secondary-index-jobs-omit-cancelled-texts
  (let [calls (atom [])
        schema (assoc-in schema [:text :db/fulltext] true)
        options (-> (options calls)
                    (assoc-in [:embedding-opts :indexing-mode] :async)
                    (assoc :search-opts {:indexing-mode :async}))]
    (with-open [conn (d/create-conn nil schema options)]
      (let [report (d/transact! conn [{:db/id 1 :text "intermediate"}
                                      {:db/id 1 :text "final"}])]
        (is (= [["intermediate" true] ["intermediate" false] ["final" true]]
               (mapv (juxt :v datom/datom-added) (:tx-data report))))
        (is (= [[:fulltext :add "final"] [:embedding :add "final"]]
               (mapv (juxt :job/type :job/op :job/value)
                     (sort-by :job/ordinal
                              (storage/secondary-index-jobs (:store @conn))))))
        (is (:caught-up? (d/wait-for-secondary-index conn {:timeout-ms 10000})))
        (is (= [["final"]] (mapv #(mapv :text %) @calls)))
        (is (= #{"final"} (embedded-texts conn)))))))
