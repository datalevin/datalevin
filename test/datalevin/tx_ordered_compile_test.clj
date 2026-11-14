(ns datalevin.tx-ordered-compile-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.db.tx.compile :as compile]
            [datalevin.prepare :as prepare])
  (:import [datalevin.db.tx.plan TxPlan]))

(def context
  (prepare/->PrepareCtx
    {:value {:db/aid 1 :db/valueType :db.type/long}
     :name {:db/aid 2 :db/valueType :db.type/string}
     :key {:db/aid 3 :db/valueType :db.type/string :db/unique :db.unique/identity}
     :friend {:db/aid 4 :db/valueType :db.type/ref}
     :many {:db/aid 5 :db/valueType :db.type/string :db/cardinality :db.cardinality/many}}
    {:db.type/ref #{:friend}}
    {:validate-data? true}))

(deftest dependent-operations-retain-original-forms
  (let [source (with-meta [:db/cas [:key "one"] :value 1 (int 2)] {:source :cas})
        input [[:db/add 1 :value 1] source [:db/retract 1 :value 2]
               [:db.fn/retractAttribute 1 :many] [:db.fn/retractEntity 2]]
        plan (compile/compile-tx context input)
        operation (second (:entries plan))]
    (is (= :ordered (compile/literal-request-shape input)))
    (is (instance? TxPlan plan))
    (is (= :ordered (:strategy plan)))
    (is (identical? context (:ctx plan)))
    (is (identical? input (:entries plan)))
    (is (identical? source operation))
    (is (= :db/cas (first operation)))
    (is (= [:key "one"] (second operation)))
    (is (instance? Integer (nth operation 4)))
    (is (nil? (:tx-time plan)))
    (is (nil? (:report plan)))))

(deftest ordered-plans-retain-reference-stamps-for-writer-preparation
  (let [source (with-meta [:db/add 1 :friend 2 888] {:source :reference})
        input [source [:db/retract 1 :many "one"]]
        p (compile/compile-tx context input)]
    (is (= :ordered (:strategy p)))
    (is (identical? input (:entries p)))
    (is (identical? source (first (:entries p))))
    (is (= 888 (nth (first (:entries p)) 4)))
    (is (= {:source :reference} (meta (first (:entries p))))))
  (let [p (compile/compile-tx context [[:db/add 1 :value 1] [:db/add 1 :value 2]])]
    (is (= :ordered (:strategy p)))
    (is (nil? (compile/compile-tx context [[:db/add 1 :value 1] [:db/add 1 :value 2]]
                {:literal-shape :scalar-operations})))))

(deftest complete-ordered-proof-declines-before-host-or-lazy-effects
  (let [effects (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! effects conj :meta) nil)
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))
        tail (lazy-seq (swap! effects conj :tail) (list [:db/add 1 :name "late"]))
        prefix [:db/add 1 :value 1]]
    (doseq [input [[prefix [:db/cas 1 :value 1 host]]
                   [prefix [:db/add host :name "host"]]
                   [prefix [:db.fn/call host]]
                   [prefix tail] tail
                   [prefix (with-meta [:db/retract 1 :value 1] {:db/internal true})]
                   (with-meta [prefix [:db/retract 1 :value 1]] {:source host})]]
      (is (nil? (compile/compile-tx context input))))
    (is (nil? (compile/compile-tx (assoc-in context [:opts :auto-entity-time?] true)
                [prefix [:db/retract 1 :value 1]])))
    (is (nil? (compile/compile-tx
                (assoc-in context [:schema :value :db.attr/preds] [host])
                [prefix [:db/retract 1 :value 1]])))
    (is (empty? @effects))))

(deftest flat-identity-analysis-is-pure-and-independent-of-native-resolution
  (let [effects (atom [])
        host (proxy [Object] []
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))]
    (is (= 3 (compile/flat-upsert-map-proof context {:db/id "new" :key "one" :value 1} false)))
    (is (= 1 (compile/flat-upsert-map-proof context {:db/id "new" :value nil} false)))
    (is (zero? (compile/flat-upsert-map-proof context {:db/id "new" :name host} false)))
    (is (zero? (compile/flat-upsert-map-proof context (sorted-map :name "custom") false)))
    (is (zero? (compile/flat-upsert-map-proof context (array-map host "key") false)))
    (is (empty? @effects))))

(deftest ordered-source-updates-decline-derived-user-predicates-before-effects
  (let [calls (atom 0)
        ctx (-> context
                (assoc-in [:schema :tuple] {:db/aid 6 :db/valueType :db.type/tuple
                                           :db/tupleAttrs [:value :name]
                                           :db.attr/preds [(fn [_] (swap! calls inc))]})
                (assoc-in [:rschema :db/attrTuples] {:value {:tuple 0} :name {:tuple 1}}))]
    (is (nil? (compile/compile-tx ctx [[:db/add 1 :value 1] [:db/retract 1 :value 1]])))
    (is (zero? @calls))))

(deftest declarative-map-forms-retain-symbolic-graph-operands
  (let [ctx (assoc-in context [:schema :refs]
              {:db/aid 6 :db/valueType :db.type/tuple :db/tupleTypes [:db.type/ref :db.type/ref]})
        input [{:db/id "early" :friend "later"} {:db/id "later" :refs ["early" "later"]}]
        plan (compile/compile-tx ctx input)]
    (is (= :ordered (:strategy plan)))
    (is (identical? input (:entries plan))))
  (let [input [{:db/id "parent" :friend {:name "nested"}}]
        plan (compile/compile-tx context input)]
    (is (= :ordered (:strategy plan)))
    (is (identical? input (:entries plan)))
    (is (identical? (first input) (first (:entries plan))))))

(deftest graph-probing-never-forces-values-or-normalizes-private-ids
  (let [effects (atom [])
        host (proxy [Object] []
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))
        tail (lazy-seq (swap! effects conj :tail) (list "lazy"))
        ctx (assoc-in context [:schema :refs]
              {:db/aid 6 :db/valueType :db.type/tuple :db/tupleTypes [:db.type/ref :db.type/ref]})]
    (doseq [input [[{:db/id "new" :friend {:name host}}]
                   [{:db/id "new" :refs tail}]
                   [{:db/id host :name "name"}]
                   [{:db/id nil :name "name"}]
                   [{:db/id :entity/one :name "name"}]]]
      (is (nil? (compile/compile-tx ctx input))))
    (is (nil? (compile/compile-tx ctx [(with-meta {:name "new"} {:source host})])))
    (is (empty? @effects))))

(deftest independent-prefix-proofs-use-the-existing-scalar-plan
  (let [input [[:db/add 1 :value 1] [:db/add 2 :name "two"] [:db/cas 1 :value 1 2]]
        plan (compile/compile-tx context input)
        prefix (:prefix-plan plan)]
    (is (= :ordered (:strategy plan)))
    (is (identical? input (:entries plan)))
    (is (= 2 (:prefix-count plan)))
    (is (= :scalar (:strategy prefix)))
    (is (identical? context (:ctx prefix)))
    (is (= [[1 :value 1] [2 :name "two"]] (:entries prefix)))))

(deftest tuple-lookup-and-materialized-datom-operands-stay-symbolic
  (let [ctx (assoc-in context [:schema :tuple-key]
              {:db/aid 6 :db/valueType :db.type/tuple :db/tupleTypes [:db.type/long :db.type/string]
               :db/unique :db.unique/identity})
        p (compile/compile-tx ctx [[:db/add [:tuple-key [7 "one"]] :value 1]
                                   [:db/cas [:tuple-key [7 "one"]] :value 1 2]])]
    (is (= :ordered (:strategy p)))
    (is (= [:tuple-key [7 "one"]] (second (first (:entries p)))))))

(deftest untyped-data-keeps-the-dynamic-route
  (let [ctx (assoc-in context [:schema :opaque] {:db/aid 6})]
    (is (nil? (compile/compile-tx ctx [[:db/add 1 :opaque "new"] [:db/retract 1 :opaque "old"]])))
    (is (nil? (compile/compile-tx ctx [[:db/retractEntity 1]])))))

(deftest tuple-source-proofs-exclude-dynamic-components
  (let [ctx (-> context
                (assoc-in [:schema :dynamic] {:db/aid 8})
                (assoc-in [:schema :derived] {:db/aid 9 :db/valueType :db.type/tuple
                                             :db/tupleAttrs [:value :dynamic]})
                (assoc-in [:rschema :db/attrTuples] {:value {:derived 0} :dynamic {:derived 1}}))]
    (is (nil? (compile/compile-tx ctx [[:db/add 1 :value 1] [:db/cas 1 :value 1 2]])))))
