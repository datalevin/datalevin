(ns datalevin.tx-auto-template-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.db.tx.template :as template]
            [datalevin.prepare :as prepare]
            [datalevin.prepared :as prepared])
  (:import [datalevin.db.tx.plan BlindEntity TxParam]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def context
  (prepare/->PrepareCtx
    {:value {:db/aid 3 :db/valueType :db.type/long}
     :text {:db/aid 4 :db/valueType :db.type/string}
     :key {:db/aid 5 :db/valueType :db.type/string :db/unique :db.unique/identity}}
    {} {:validate-data? true}))

(defn select-plan [registry context forms]
  (template/automatic-plan registry context forms (template/automatic-key forms)))

(deftest automatic-blind-templates-reuse-one-layout-without-retaining-values
  (let [registry (prepared/handle-cache) events (atom [])
        first-input [(array-map :key "first" :value 1)]
        next-input (FastList. ^java.util.Collection
                             [(array-map :key "second" :value 2)
                              (array-map :key "third" :value 3)])]
    (binding [template/*automatic-observer* #(swap! events conj [%1 %2])]
      (let [one (select-plan registry context first-input)
            two (select-plan registry context next-input)
            compiled (second (first @events))]
        (is (= :blind (:strategy one) (:strategy two)))
        (is (= [:compiled :hit] (mapv first @events)))
        (is (identical? compiled (second (second @events))))
        (is (every? #(instance? TxParam %) (vals (:source (first (:operations compiled))))))
        (is (= [[:key "first" :value 1]]
               (mapv #(vec (.-attrs ^BlindEntity %)) (:entries one))))
        (is (= [[:key "second" :value 2] [:key "third" :value 3]]
               (mapv #(vec (.-attrs ^BlindEntity %)) (:entries two))))))))

(deftest automatic-scalar-templates-bind-map-and-vector-operands-directly
  (doseq [forms [[[:db/add 1 :value 7] [:db/add 2 :value 8]]
                 [(array-map :db/id 1 :text "one" :value 7)
                  (array-map :db/id 2 :text "two" :value 8)]]]
    (let [registry (prepared/handle-cache)
          plan (select-plan registry context forms)]
      (is (= (if (map? (first forms)) :scalar :scalar-operations) (:strategy plan)))
      (is (= (if (map? (first forms))
               [[1 :text "one"] [1 :value 7] [2 :text "two"] [2 :value 8]]
               forms) (:entries plan)))
      (is (= 1 (.size registry))))))

(deftest automatic-plans-refresh-schema-and-options-and-decline-duplicate-values
  (let [registry (prepared/handle-cache)
        forms [{:key "same"}]
        one (select-plan registry context forms)
        changed (assoc context :opts {:validate-data? false})
        two (select-plan registry changed forms)]
    (is (some? one))
    (is (identical? changed (:ctx two)))
    (is (nil? (select-plan registry changed [{:key "same"} {:key "same"}])))
    (is (nil? (select-plan registry context [{:db/id -1 :value 1} {:db/id -1 :value 2}])))
    (is (nil? (select-plan registry context [[:db/add 1 :value 1] [:db/add 1 :value 2]])))))

(deftest automatic-matching-preserves-demand-and-rejection-boundaries
  (let [registry (prepared/handle-cache) effects (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host")
               (meta [] (swap! effects conj :meta) nil))
        lazy-input (lazy-seq (swap! effects conj :lazy) (list {:value 1}))
        custom (proxy [FastList] []
                 (iterator [] (swap! effects conj :iterator) (throw (Exception.))))]
    (select-plan registry context [{:text "canonical"}])
    (doseq [forms [[{:text host}] [{:text "canonical"} {:value 2}]
                   [{:text "canonical"} [:db.fn/call host]]
                   [(with-meta {:text "canonical"} {:source :annotation})]
                   [[:db/add 1 :text host]] lazy-input custom]]
      (is (nil? (select-plan registry context forms))))
    (is (empty? @effects))
    ;; Valid programs may be retained even when a particular binding declines.
    (is (= 2 (.size registry)))))

(deftest automatic-cache-has-the-shared-handle-bound
  (let [registry (prepared/handle-cache)
        attrs (mapv #(keyword (str "field" %)) (range (inc prepared/max-handles)))
        context (assoc context :schema (into {} (map-indexed
                                                 (fn [i attr] [attr {:db/aid (+ i 3) :db/valueType :db.type/long}])
                                                 attrs)))]
    (doseq [attr attrs] (is (some? (select-plan registry context [{attr 1}]))))
    (is (= prepared/max-handles (.size registry)))
    (is (not (.containsKey registry (template/automatic-key [{(first attrs) 2}]))))))

(deftest automatic-blind-layout-preserves-wide-array-map-order
  (let [attrs (mapv #(keyword (str "field" %)) (range 16))
        context (assoc context :schema (into {} (map-indexed
                                                 (fn [i attr] [attr {:db/aid (+ i 3) :db/valueType :db.type/long}])
                                                 attrs)))
        row (apply array-map (mapcat #(vector % 1) attrs))
        plan (select-plan (prepared/handle-cache) context [row])]
    (is (= :blind (:strategy plan)))
    (is (= (vec (mapcat #(vector % 1) attrs)) (vec (.-attrs ^BlindEntity (first (:entries plan))))))))

(deftest automatic-scalar-operations-reuse-immutable-rows-and-snapshot-mutable-batches
  (let [registry (prepared/handle-cache)
        rows [[:db/add 1 :value 7] [:db/add 2 :value 8]]
        vector-plan (select-plan registry context rows)
        mutable (FastList. ^java.util.Collection rows)
        mutable-plan (select-plan registry context mutable)]
    (is (= :scalar-operations (:strategy vector-plan) (:strategy mutable-plan)))
    (is (identical? rows (:entries vector-plan)))
    (.set mutable 0 [:db/add 1 :value 99])
    (is (= rows (:entries mutable-plan)))
    (is (nil? (template/automatic-key [[:db/add 1 :value 1] {:text "different"}])))))
