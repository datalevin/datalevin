(ns datalevin.db.blind-passes-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.test.core :refer [db-fixture]]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:value {:db/valueType :db.type/long}
   :text {:db/valueType :db.type/string}
   :ref {:db/valueType :db.type/ref}
   :double {:db/valueType :db.type/double}
   :tags {:db/valueType :db.type/long :db/cardinality :db.cardinality/many}})

(def ^:private options
  {:background-sampling? false :snapshot-scheduler? false})

(defn- counted-map [source walks]
  (proxy [clojure.lang.APersistentMap clojure.lang.IKVReduce] []
    (valAt
      ([key] (get source key))
      ([key absent] (get source key absent)))
    (count [] (count source))
    (seq [] (seq source))
    (kvreduce [f init]
      (swap! walks inc)
      (reduce-kv f init source))))

(defn- interpreted [database entities]
  (db/prepare-local-tx-data
    (db/->TxReport database (db/transfer database (:store database)) [] {} nil)
    entities))

(defn- datoms [report]
  (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data report)))

(deftest bulk-canonical-values-visit-entity-keys-once
  (with-open [conn (d/create-conn nil schema options)]
    (d/transact! conn [{:db/id 1 :value 0}])
    (doseq [n [16 32 128] collect? [false true] tempids? [false true]]
      (let [walks (atom 0)
            entities (mapv #(cond-> (array-map :value (long %) :text "ready" :ref 1)
                              tempids? (assoc :db/id (str "new-" %)))
                           (range n))
            prepared (db/prepare-blind-local-tx
                       @conn (mapv #(counted-map % walks) entities) true collect?)
            report (db/stamp-blind-local-tx @conn prepared nil)
            general (interpreted @conn entities)]
        (is (some? prepared))
        (is (= (* n (if (< n 32) 2 1)) @walks))
        (is (= (datoms general) (datoms report)))
        (is (= (:tempids general) (:tempids report)))))))

(defn- counted-value [conversions]
  (proxy [Object] []
    (toString [] (swap! conversions inc) "converted")))

(deftest bulk-preflight-precedes-coercion-and-value-errors
  (with-open [conn (d/create-conn nil schema (assoc options :validate-data? true))]
    (let [prefix (vec (repeat 32 {:value 1}))]
      (doseq [collect? [false true]
              invalid [{:value nil} {:value "wrong"} {:double Double/NaN}
                       {:ref -1} {:tags [1 nil]}]]
        (is (nil? (db/prepare-blind-local-tx
                    @conn (into prefix [invalid {:unknown 1}]) true collect?))))
      (is (thrown-with-msg? Exception #"Cannot store nil"
                            (db/prepare-blind-local-tx
                              @conn (conj prefix {:value nil}) true false)))))
  (with-open [conn (d/create-conn nil schema options)]
    (let [conversions (atom 0)
          entities (mapv (fn [_] {:text (counted-value conversions)}) (range 32))]
      (doseq [collect? [false true]
              tail [{:unknown 1} {:ref [:db/ident :target]}]]
        (is (nil? (db/prepare-blind-local-tx @conn (conj entities tail) true collect?)))
        (is (zero? @conversions)))
      (let [report (db/stamp-blind-local-tx
                     @conn (db/prepare-blind-local-tx @conn entities true false) nil)]
        (is (= 32 @conversions))
        (is (= (repeat 32 "converted") (map :v (:tx-data report))))))))

(deftest bulk-fallback-normalizes-once-on-local-write-paths
  (doseq [wal? [false true] explicit? [false true]]
    (testing (str [wal? explicit?])
      (with-open [conn (d/create-conn nil schema (assoc options :wal? wal?))]
        (d/transact! conn [{:db/id 1 :db/ident :target}])
        (let [conversions (atom 0)
              entities (conj (mapv (fn [i] {:db/id (str "new-" i)
                                            :text (counted-value conversions)})
                                   (range 32))
                             {:ref [:db/ident :target]})
              run (fn [tx]
                    (let [report (d/transact! tx entities)]
                      (is (= 32 @conversions))
                      (is (= 33 (count (:tx-data report))))
                      (is (= "converted"
                             (:text (d/entity @tx (get (:tempids report) "new-31")))))))]
          (if explicit?
            (d/with-transaction [tx conn] (run tx))
            (run conn)))))))
