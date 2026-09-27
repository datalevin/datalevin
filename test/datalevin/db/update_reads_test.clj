(ns ^{:clj-kondo/config
      '{:lint-as {datalevin.db.update-reads-test/with-conn clojure.core/let}}}
  datalevin.db.update-reads-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.datom :as datom]
   [datalevin.db :as db]
   [datalevin.interface :as i]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def fields (mapv #(keyword "item" (str "field" %)) (range 10)))
(def schema
  (assoc (zipmap fields (repeat {:db/valueType :db.type/string :db/noindex true}))
         :item/key {:db/valueType :db.type/string :db/unique :db.unique/identity}
         (fields 1) {:db/valueType :db.type/boolean :db/noindex true}
         :item/untouched {:db/cardinality :db.cardinality/many :db/noindex true}))

(defmacro with-conn [[conn wal?] & body]
  `(let [path# (u/tmp-dir (str "update-reads-" (random-uuid)))
         ~conn (d/get-conn path# schema {:wal? ~wal?})]
     (try ~@body
          (finally (d/close ~conn) (u/delete-files path#)))))

(defn- datoms [report]
  (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data report)))

(defn- initial-entity [key]
  (assoc (zipmap (butlast fields) (repeat "old"))
         :item/key key (fields 1) false
         (fields 2) (apply str (repeat 1000 "g"))
         :item/untouched #{"leave" "alone"}))

(defn- update-entity [key]
  (assoc (zipmap fields (repeat "new")) :item/key key
         (fields 0) "old" (fields 1) true))

(deftest selective-upsert-reads-preserve-point-read-results
  (doseq [wal? [false true]]
    (with-conn [conn wal?]
      (d/transact! conn [(initial-entity "existing")])
      (let [entity (assoc (update-entity "existing") :db/id "upsert")
            prepared (db/prepare-blind-local-tx @conn [entity] true false)
            [selected upsert?] (db/stamp-blind-local-identity-tx @conn prepared {:test true})
            [points] (db/stamp-blind-local-identity-tx
                       @conn (assoc prepared :identity-upsert-read nil) {:test true})]
        (is (some? (:identity-upsert-read prepared)))
        (is upsert?)
        (is (= (datoms points) (datoms selected)))
        (is (= (:tempids points) (:tempids selected)))
        (is (= 17 (count (:tx-data selected))))
        (is (= {:test true} (:tx-meta selected)))
        (let [report (d/transact! conn [entity])
              eid (get (:tempids report) "upsert")]
          (is (= (datoms selected) (datoms report)))
          (is (= (dissoc entity :db/id)
                 (select-keys (into {} (d/touch (d/entity @conn eid)))
                              (cons :item/key fields))))
          (is (= #{"leave" "alone"} (:item/untouched (d/entity @conn eid)))))
        (is (empty? (:tx-data (d/transact! conn [entity])))))
      (testing "single-field updates retain their point read"
        (let [prepared (db/prepare-blind-local-tx
                         @conn [{:item/key "existing" (fields 0) "single"}] true false)]
          (is (nil? (:identity-upsert-read prepared)))
          (is (= 2 (count (:tx-data (first (db/stamp-blind-local-identity-tx
                                            @conn prepared nil)))))))))))

(deftest scalar-updates-group-selective-reads-by-entity
  (with-conn [conn true]
    (d/transact! conn [(initial-entity "wide") (initial-entity "narrow")])
    (let [wide (d/entid @conn [:item/key "wide"])
          narrow (d/entid @conn [:item/key "narrow"])
          reads (atom [])
          store (:store @conn)
          counted-store (reify i/IStore
                          (schema [_] (i/schema store))
                          (opts [_] (i/opts store))
                          (rschema [_] (i/rschema store))
                          (av-first-e [_ a v]
                            (swap! reads conj [a v])
                            (i/av-first-e store a v)))
          changes (update-entity "wide")
          operations (conj (mapv #(vector :db/add [:item/key "wide"] % (changes %))
                                 (reverse fields))
                           [:db/add [:item/key "narrow"] (fields 0) "single"])
          prepared (db/prepare-scalar-update-tx (assoc @conn :store counted-store)
                                                 operations)]
      (is (= [[:item/key "wide"] [:item/key "narrow"]] @reads))
      (is (= #{wide} (set (keys (:entity-reads prepared)))))
      (let [selected (db/stamp-scalar-update-tx @conn prepared nil)
            points (db/stamp-scalar-update-tx @conn (assoc prepared :entity-reads nil) nil)]
        (is (= (datoms points) (datoms selected)))
        (is (= 19 (count (:tx-data selected))))
        (is (= (datoms selected) (datoms (d/transact! conn operations)))))
      (is (= "single" (get (d/entity @conn narrow) (fields 0)))))))

(deftest prepared-projections-read-values-at-stamping-time
  (with-conn [conn true]
    (d/transact! conn [(initial-entity "existing")])
    (let [entity (update-entity "existing")
          prepared (db/prepare-blind-local-tx @conn [entity] true false)
          scalar (db/prepare-scalar-update-tx
                   @conn (mapv #(vector :db/add [:item/key "existing"] % (entity %)) fields))]
      (d/transact! conn [[:db/add [:item/key "existing"] (fields 3) "intervening"]])
      (doseq [report [(first (db/stamp-blind-local-identity-tx @conn prepared nil))
                      (db/stamp-scalar-update-tx @conn scalar nil)]]
        (is (some #(and (= (fields 3) (:a %)) (= "intervening" (:v %))
                        (not (datom/datom-added %)))
                  (:tx-data report)))))))
