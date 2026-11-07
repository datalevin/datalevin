(ns datalevin.db.interpreter-schema-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db.tx.execute :as execute]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [db-fixture]])
  (:import [java.util Comparator]
           [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(use-fixtures :each db-fixture)

(def ^:private base-schema
  {:left {:db/valueType :db.type/long}
   :right {:db/valueType :db.type/long}})
(def ^:private options {:background-sampling? false :snapshot-scheduler? false})
(defn- ave-attrs [report] (set (map :a (:avet (:db-after report)))))
(defn- clean-report? [report]
  (not-any? #(= "datalevin.db.tx.execute" (namespace %)) (keys report)))

(deftest schema-feature-combinations-preserve-simulated-indexes
  (binding [c/*use-prepare-path* false]
    (doseq [noindex? [false true] tuples? [false true]]
      (testing (str "noindex=" noindex? ", tuples=" tuples?)
        (let [schema (cond-> base-schema
                       noindex? (assoc-in [:left :db/noindex] true)
                       noindex? (assoc-in [:right :db/noindex] true)
                       tuples? (assoc :pair {:db/tupleAttrs [:left :right]}))]
          (with-open [conn (d/create-conn nil schema options)]
            (d/transact! conn [{:db/id 1 :left 1 :right 2}])
            (let [report (d/tx-data->simulated-report
                           @conn [[:db/add 1 :left 5]
                                  {:db/id 2 :left 6 :right 7}])
                  after (:db-after report)]
              (is (= (cond-> (if noindex? #{} #{:left :right}) tuples? (conj :pair))
                     (ave-attrs report)))
              (is (= {:left 5 :right 2} (d/pull after [:left :right] 1)))
              (is (= {:left 6 :right 7} (d/pull after [:left :right] 2)))
              (is (= #{[5]} (d/q '[:find ?v :in $ ?e :where [?e :left ?v]] after 1)))
              (when tuples?
                (is (= [5 2] (:pair (d/entity after 1))))
                (is (= [6 7] (:pair (d/entity after 2)))))
              (is (clean-report? report))
              (is (= {:left 1 :right 2} (d/pull @conn [:left :right] 1))))))))))

(deftest schema-classifications-refresh-between-attempts-and-after-abort
  (binding [c/*use-prepare-path* false]
    (doseq [wal? [false true]]
      (with-open [conn (d/create-conn nil base-schema (assoc options :wal? wal?))]
        (is (= #{:left :right}
               (ave-attrs (d/tx-data->simulated-report @conn [[:db/add 1 :left 3]
                                                             [:db/add 1 :right 4]]))))
        (d/with-transaction [tx conn]
          (d/update-schema tx {:left {:db/noindex true}
                               :right {:db/noindex true}
                               :pair {:db/tupleAttrs [:left :right]}})
          (let [report (d/transact! tx [[:db/add 1 :left 3] [:db/add 1 :right 4]])]
            (is (= [3 4] (:pair (d/entity @tx 1))))
            (is (not-any? #{:left :right} (map :a (:avet (:db-after report)))))
            (is (clean-report? report)))
          (d/abort-transact tx))
        (is (empty? (:db/noindex (i/rschema (:store @conn)))))
        (is (empty? (:db/attrTuples (i/rschema (:store @conn)))))
        (let [report (d/tx-data->simulated-report @conn [[:db/add 1 :left 7]
                                                        [:db/add 1 :right 8]])]
          (is (= #{:left :right} (ave-attrs report)))
          (is (clean-report? report)))
        (d/update-schema conn {:left {:db/noindex true}
                               :right {:db/noindex true}})
        (is (empty? (ave-attrs (d/tx-data->simulated-report @conn [[:db/add 1 :left 3]]))))
        (d/index-attr conn :left)
        (d/index-attr conn :right)
        (is (= #{:left} (ave-attrs (d/tx-data->simulated-report @conn [[:db/add 1 :left 3]]))))))))

(deftest empty-features-avoid-extra-per-datom-schema-classification
  (let [reads (atom 0)
        schema {:value {:db/valueType :db.type/long}}
        store (reify i/IStore
                (schema [_] schema)
                (rschema [_] (swap! reads inc) {})
                (opts [_] {})
                (init-max-eid [_] 0))
        database {:store store :max-eid 0 :max-tx c/tx0
                  :eavt (TreeSortedSet. ^Comparator datom/cmp-datoms-eavt)
                  :avet (TreeSortedSet. ^Comparator datom/cmp-datoms-avet)}
        report (execute/execute-tx-loop
                 {:db-before database :db-after database :tx-data [] :tempids {}}
                 (mapv (fn [e] {:db/id e :value e}) (range 1 65)) 0)]
    (is (= 64 (count (:tx-data report))))
    (is (= 64 (count (:avet (:db-after report)))))
    ;; Uniqueness validation still reads the schema. AVET publication and tuple
    ;; maintenance must not each add another read for every datom.
    (is (< @reads 128))
    (is (clean-report? report))))
