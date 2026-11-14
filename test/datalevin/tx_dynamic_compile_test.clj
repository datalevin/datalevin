(ns datalevin.tx-dynamic-compile-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.db.tx.compile :as compile]
            [datalevin.prepare :as prepare]))

(def context (prepare/->PrepareCtx {:doc {:db/valueType :db.type/idoc}} {} {}))

(deftest opaque-program-compilation-does-not-demand-input-or-values
  (let [effects (atom [])
        input (reify clojure.lang.Sequential clojure.lang.Seqable
                (seq [_] (swap! effects conj :seq) (throw (AssertionError. "speculative demand"))))
        program (compile/compile-tx context input {:dynamic? true})]
    (is (= :dynamic (:strategy program)))
    (is (identical? input (:entries program)))
    (is (identical? context (:ctx program)))
    (is (empty? @effects))))

(deftest request-shapes-never-bind-opaque-ids
  (let [effects (atom [])
        host (proxy [Number] []
               (longValue [] (swap! effects conj :long) 1)
               (intValue [] 1) (doubleValue [] 1.0) (floatValue [] 1.0))]
    (is (nil? (compile/literal-request-shape [{:db/id host :value 1}])))
    (is (nil? (compile/literal-request-shape [{:db/id 9223372036854775808N :value 1}])))
    (is (= :blind (compile/literal-request-shape [{:value host}])))
    (is (= :blind (compile/literal-request-shape [{:db/id "new" :value host}])))
    (is (= :scalar-operations (compile/literal-request-shape [[:db/add 1 :value host]])))
    (is (empty? @effects))))

(deftest patch-plans-retain-lazy-operations-without-document-reads
  (let [effects (atom [])
        operations (lazy-seq (swap! effects conj :patch) (list [:set [:n] 2]))
        source [:db.fn/patchIdoc 1 :doc operations]
        program (compile/compile-patch-region context source 1 :doc operations)
        operation (first (:entries program))]
    (is (= :patch (:strategy program)))
    (is (= :patch (compile/literal-request-shape [source])))
    (is (= :patch (:strategy (compile/compile-tx context [source]))))
    (is (nil? (compile/compile-tx context
                [[:db.fn/patchIdoc 1 :doc {:n 1} operations]]
                {:request-shape :patch :ordered? false})))
    (is (identical? source (nth operation 3)))
    (is (identical? operations (nth operation 2)))
    (is (nil? (compile/compile-patch-region
                (assoc-in context [:schema :doc :db.attr/preds] 'user/pred?)
                source 1 :doc operations)))
    (is (empty? @effects))))
