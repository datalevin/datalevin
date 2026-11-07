(ns datalevin.db.mixed-preflight-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.conn :as conn]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.db.tx.common :as common]
            [datalevin.test.core :refer [db-fixture]]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:counter {:db/valueType :db.type/long}
   :label {:db/valueType :db.type/string}
   :name {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :link {:db/valueType :db.type/ref}})

(defn- prefix [n value]
  (into [[:db/add 1 :counter 1]]
        (map (fn [i] {:db/id (str "new-" i) :counter (long i) :label value}))
        (range n)))

(defn- unsupported-tails []
  [[:retraction [:db/retract 1 :counter 1]]
   [:lookup-ref [:db/add [:name "existing"] :counter 2]]
   [:identity {:db/id "alias" :name "existing"}]
   [:metadata (with-meta [:db/add 1 :label "changed"] {:source :test})]
   [:duplicate-ea [:db/add 1 :counter 2]]
   [:duplicate-tempid {:db/id "new-0" :label "replacement"}]])

(deftest late-fallback-declines-before-correcting-any-values
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :counter 0 :name "existing"}])
    (let [corrections (atom 0)
          value (proxy [Object] [] (toString [] (swap! corrections inc) "corrected"))
          txs (prefix 128 value)
          database @conn
          indexes [(vec (:eavt database)) (vec (:avet database))]]
      ;; Duplicate tempids remain a planner check for ordered map resolution.
      (doseq [[label tail] (unsupported-tails)
              :when (not= label :duplicate-tempid)]
        (testing (name label)
          (is (nil? (db/prepare-mixed-add-tx database (conj txs tail) nil
                                           (:max-eid database))))
          (is (zero? @corrections))
          (is (= indexes [(vec (:eavt database)) (vec (:avet database))]))))
      (testing "an eligible request still corrects each string exactly once"
        (let [report (db/prepare-mixed-add-tx database txs nil (:max-eid database))]
          (is (some? report))
          (is (= 128 @corrections))
          (is (= 128 (count (filter #(= :label (:a %)) (:tx-data report))))))))))

(defn- report-datoms [report]
  (mapv (juxt :e :a :v datom/datom-added) (:tx-data report)))

(deftest late-fallback-retains-ordered-general-resolution
  (doseq [wal? [false true]]
    (with-open [conn (d/create-conn nil schema {:wal? wal? :snapshot-scheduler? false
                                               :background-sampling? false})]
      (d/transact! conn [{:db/id 1 :counter 0 :name "existing"}])
      (doseq [[label tail] (unsupported-tails)]
        (testing (str wal? " " (name label))
          (d/with-transaction [tx conn]
            (let [database @tx
                  txs (conj (prefix 64 "value") tail)
                  expected (db/prepare-local-tx-data
                             (db/->TxReport database
                               (db/transfer database (:store database)) [] {} nil)
                             txs 0)
                  paths (atom [])
                  actual (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
                           (d/transact! tx txs))]
              (is (= [:general] @paths))
              (is (= (:tempids expected) (:tempids actual)))
              (is (= (report-datoms expected) (report-datoms actual))))
            (d/abort-transact tx)))))))

(deftest implicit-identities-and-forward-references-retain-general-resolution
  (with-open [conn (d/create-conn nil (dissoc schema :name)
                                 {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :db/ident :root :counter 0}])
    (is (= #{:db/ident} (common/attrs-by @conn :db.unique/identity)))
    (let [report (d/with (db/transfer @conn (:store @conn))
                        [{:db/id "new" :link "existing" :counter 1}
                         {:db/id "existing" :db/ident :root :counter 2}]
                        nil true)
          after (:db-after report)]
      (is (= 1 (get (:tempids report) "existing")))
      (is (= 2 (get (:tempids report) "new")))
      (is (= 2 (:counter (d/entity after :root))))
      (is (= 1 (:db/id (:link (d/entity after 2))))))))
