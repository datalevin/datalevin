(ns datalevin.db.redundant-add-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [datalevin.db.tx.execute :as execute]
            [datalevin.interface :as i]
            [datalevin.storage.schema :as schema])
  (:import [java.util Comparator]
           [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(defn- prepare
  [attrs stored entities & [prefix]]
  (let [reads (atom [])
        store (reify i/IStore
                (opts [_] {})
                (schema [_] attrs)
                (rschema [_] (schema/schema->rschema attrs))
                (init-max-eid [_] (reduce max c/e0 (map :e stored)))
                (ea-first-datom [_ e a]
                  (swap! reads conj [e a])
                  (some #(when (= [e a] [(:e %) (:a %)]) %) stored)))
        db {:store store :max-eid (reduce max c/e0 (map :e stored))
            :max-tx c/tx0
            :eavt (TreeSortedSet. ^Comparator d/cmp-datoms-eavt)
            :avet (TreeSortedSet. ^Comparator d/cmp-datoms-avet)}]
    [(execute/execute-tx-loop
       {:db-before db :db-after db :tx-data (or prefix []) :tempids {}}
       entities 0)
     @reads]))

(def ^:private attrs
  {:value {:db/valueType :db.type/long}
   :link {:db/valueType :db.type/ref}})

(deftest identical-ordinary-inputs-read-an-unchanged-entity-once
  (doseq [entity [{:db/id 5 :value 42} [:db/add 5 :value 42]]
          shared? [false true]]
    (let [[report reads] (prepare attrs [(d/datom 5 :value 42)]
                                  (if shared?
                                    (vec (repeat 256 entity))
                                    (mapv (fn [_] (into (empty entity) entity))
                                          (range 256))))]
      (is (= [[5 :value]] reads))
      (is (empty? (:tx-data report)))
      (is (= (inc c/tx0) (:max-tx (:db-after report))))
      (is (not (contains? report ::execute/reference-free?)))))
  (testing "a new tempid still allocates and publishes its first addition"
    (let [[report reads] (prepare attrs [] (repeat 256 {:db/id "new" :value 42}))]
      (is (empty? reads))
      (is (= 1 (get (:tempids report) "new")))
      (is (= [[1 :value 42]] (mapv d/datom-eav (:tx-data report)))))))

(deftest reference-free-redundancy-needs-no-tempid-evidence
  (let [datom (d/datom 5 :value 42)
        report (#'execute/transact-same-value
                 {:tx-data [] ::execute/reference-free? true} datom nil)
        general (#'execute/transact-same-value {:tx-data []} datom nil)]
    (is (not (contains? report ::execute/tx-redundant)))
    (is (= [datom] (::execute/tx-redundant general)))))

(deftest reference-tempids-from-a-prefix-still-consume-redundant-evidence
  (let [[initial _] (prepare attrs [(d/datom 5 :value 42)] [])
        db (:db-before initial)
        report (execute/execute-tx-loop
                 {:db-before db :db-after db :tx-data []
                  :tempids {"target" 5} ::execute/value-tempids {5 "target"}}
                 (repeat 2 {:db/id 5 :value 42}) 0)]
    (is (empty? (:tx-data report)))
    (is (= 5 (get (:tempids report) "target")))
    (is (not (contains? report ::execute/value-tempids)))
    (is (not (contains? report ::execute/tx-redundant)))))

(deftest ordinary-planning-preserves-replacements-and-supplied-retractions
  (let [[report _] (prepare attrs [(d/datom 5 :value 42)]
                            [[:db/add 5 :value 43] [:db/add 5 :value 42]
                             [:db/add 5 :value 42]])]
    (is (= [[42 false] [43 true] [43 false] [42 true] [42 true]]
           (mapv (juxt :v d/datom-added) (:tx-data report)))))
  (let [[report reads] (prepare attrs [(d/datom 5 :value 42)]
                                  (repeat 2 [:db/add 5 :value 42])
                                  [(d/datom 5 :value 42 c/tx0 false)])]
    (is (= [[5 :value]] reads))
    (is (= [false true true] (mapv d/datom-added (:tx-data report))))))

(deftest only-provably-ordinary-inputs-enter-the-plan
  (let [[report _] (prepare attrs [] [])
        db (:db-before report)]
    (doseq [entity [{:db/id 1 :link 2}
                   {:db/id 1 :value (Object.)}
                   [:db/add 1 :value nil]
                   (with-meta [:db/add 1 :value 42] {:source :test})
                   [:db/retract 1 :value 42]
                   [:db/cas 1 :value 42 42]
                   [:db.fn/call (fn [_] [])]]]
      (is (nil? (#'execute/plan-ordinary-additions db [entity] true))
          (pr-str entity)))
    (doseq [separator [nil false]]
      (is (nil? (#'execute/plan-ordinary-additions
                  db [{:db/id 1 :value 42} separator {:db/id 1 :link 2}] true))))
    (doseq [extra [{:value {:db/valueType :db.type/long :db.attr/preds 'some-predicate}}
                  {:value {:db/valueType :custom/type}}
                  {:tuple {:db/tupleAttrs [:value]}}]]
      (let [[report _] (prepare (merge attrs extra) [] [])]
        (is (nil? (#'execute/plan-ordinary-additions
                    (:db-before report) [{:db/id 1 :value 42}] true)))))))
