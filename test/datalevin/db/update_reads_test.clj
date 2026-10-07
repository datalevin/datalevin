(ns ^{:clj-kondo/config
      '{:lint-as {datalevin.db.update-reads-test/with-conn clojure.core/let}}}
  datalevin.db.update-reads-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.datom :as datom]
   [datalevin.db :as db]
   [datalevin.db.tx.common :as txcommon]
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

(deftest scalar-projection-cutoff-preserves-update-results
  (doseq [wal? [false true]]
    (with-conn [conn wal?]
      (d/transact! conn [(initial-entity "existing")])
      (let [eid (d/entid @conn [:item/key "existing"])]
        (doseq [width [4 7 8 10]]
          (let [attrs (take width fields)
                entity (select-keys (update-entity "existing") attrs)
                upsert (db/prepare-blind-local-tx
                         @conn [(assoc entity :item/key "existing")] true false)
                scalar (db/prepare-scalar-update-tx
                         @conn (mapv #(vector :db/add eid % (entity %)) attrs))
                projected? (>= width 8)]
            (is (= projected? (some? (:identity-upsert-read upsert))))
            (is (= projected? (contains? (:entity-reads scalar) eid)))
            (is (= (datoms (first (db/stamp-blind-local-identity-tx
                                   @conn (assoc upsert :identity-upsert-read nil) nil)))
                   (datoms (first (db/stamp-blind-local-identity-tx @conn upsert nil)))))
            (is (= (datoms (db/stamp-scalar-update-tx
                            @conn (assoc scalar :entity-reads nil) nil))
                   (datoms (db/stamp-scalar-update-tx @conn scalar nil))))))))))

(deftest collected-scalar-projections-preserve-pending-values-and-retractions
  (with-conn [conn true]
    (d/transact! conn [(initial-entity "wide")])
    (let [eid (d/entid @conn [:item/key "wide"])
          pending-db (db/transfer @conn (:store @conn))
          tx (inc (:max-tx pending-db))
          changes (assoc (zipmap fields (repeat "new"))
                         (fields 1) false (fields 3) "old")
          prepared (db/prepare-scalar-update-tx
                     pending-db (mapv #(vector :db/add eid % (changes %)) fields)
                     {:defer-entity-resolution? true})]
      (is (= #{eid} (set (keys (:entity-reads prepared)))))
      (doseq [d [(datom/datom eid (fields 0) "old" tx false)
                 (datom/datom eid (fields 0) "aaa" tx)
                 (datom/datom eid (fields 1) false tx false)
                 (datom/datom eid (fields 1) true tx)
                 (datom/datom eid (fields 2) (get (initial-entity "wide") (fields 2)) tx false)
                 (datom/datom eid :item/untouched "leave" tx false)]]
        (txcommon/stage-batch-datom! pending-db d))
      (binding [txcommon/*batch-prepare* {}]
        (let [selected (db/stamp-scalar-update-tx pending-db prepared nil)
              points (db/stamp-scalar-update-tx
                       pending-db (assoc prepared :entity-reads nil) nil)]
          (is (= (datoms points) (datoms selected)))
          (is (= 16 (count (:tx-data selected))))
          (is (some #(and (= "aaa" (:v %)) (not (datom/datom-added %)))
                    (:tx-data selected)))
          (is (some #(and (= true (:v %)) (not (datom/datom-added %)))
                    (:tx-data selected)))))
      (testing "a fully pending projection sees the current values without native state"
        (binding [txcommon/*batch-prepare* {}]
          (doseq [d (:tx-data (db/stamp-scalar-update-tx pending-db prepared nil))]
            (txcommon/stage-batch-datom! pending-db d)))
        (doseq [attr fields]
          (txcommon/stage-batch-datom!
            pending-db (datom/datom eid attr (changes attr) (inc tx))))
        (let [store (:store pending-db)
              schema-only (reify i/IStore
                            (schema [_] (i/schema store))
                            (opts [_] (i/opts store)))]
          (binding [txcommon/*batch-prepare* {}]
            (is (empty? (:tx-data (db/stamp-scalar-update-tx
                                   (assoc pending-db :store schema-only) prepared nil)))))))
      (is (= "old" (get (d/entity @conn eid) (fields 0))))
      (is (= false (get (d/entity @conn eid) (fields 1)))))))

(deftest deferred-scalar-preparation-resolves-identities-only-when-stamping
  (with-conn [conn true]
    (d/transact! conn [(initial-entity "first") (initial-entity "second")])
    (let [first-eid (d/entid @conn [:item/key "first"])
          second-eid (d/entid @conn [:item/key "second"])
          store (:store @conn)
          schema-only (reify i/IStore
                        (schema [_] (i/schema store))
                        (opts [_] (i/opts store))
                        (rschema [_] (i/rschema store))
                        (av-first-e [_ _ _] (throw (ex-info "early identity read" {}))))
          txs (mapv #(vector :db/add [:item/key "first"] % "updated")
                    [(fields 0) (fields 2) (fields 3) (fields 4)])
          prepared (db/prepare-scalar-update-tx (assoc @conn :store schema-only)
                                                txs {:defer-entity-resolution? true})]
      (is (:deferred-entity-resolution? prepared))
      (d/transact! conn [[:db/retract first-eid :item/key "first"]])
      (d/transact! conn [[:db/add second-eid :item/key "first"]])
      (let [report (db/stamp-scalar-update-tx @conn prepared nil)]
        (is (some? report))
        (is (= #{second-eid} (set (map :e (:tx-data report)))))))))

(deftest deferred-scalar-aliases-retain-general-update-ordering
  (with-conn [conn true]
    (d/transact! conn [(initial-entity "one")])
    (let [eid (d/entid @conn [:item/key "one"])
          prepared (db/prepare-scalar-update-tx
                     @conn [[:db/add eid (fields 0) "first"]
                            [:db/add [:item/key "one"] (fields 0) "last"]]
                     {:defer-entity-resolution? true})]
      (is (some? prepared))
      (is (nil? (db/stamp-scalar-update-tx @conn prepared nil))))))
