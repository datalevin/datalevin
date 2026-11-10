(ns datalevin.tx-compile-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.db.tx.compile :as compile]
            [datalevin.prepare :as prepare])
  (:import [datalevin.db.tx.plan MixedAttrInfo MixedInsert MixedOperands MixedUpdate TxPlan]
           [java.util HashMap]))

(def ctx
  (prepare/->PrepareCtx
    {:value {:db/aid 3 :db/valueType :db.type/long}
     :text {:db/aid 4 :db/valueType :db.type/string}
     :identity {:db/aid 5 :db/valueType :db.type/string :db/unique :db.unique/identity}
     :reference {:db/aid 6 :db/valueType :db.type/ref}}
    {} {:validate-data? true}))

(deftest mixed-operands-retain-order-and-symbolic-identities
  (let [forms [[:db/add 7 :value 2]
               (array-map :text "parent" :db/id "parent" :reference "parent")
               {:db/id -1 :reference "parent" :value 3}
               [:db/add 2 :text "updated"]]
        plan (compile/compile-mixed-region ctx forms)
        ^MixedOperands operands (:entries plan)
        entries (.-entries operands)
        ^MixedInsert insert (nth entries 1)]
    (is (instance? TxPlan plan))
    (is (identical? ctx (:ctx plan)))
    (is (= :mixed (:strategy plan)))
    (is (= [MixedUpdate MixedInsert MixedInsert MixedUpdate] (mapv class entries)))
    (is (= 7 (.-required-eid operands)))
    (is (true? (.-prepare-reader? operands)))
    (is (= "parent" (.-tempid insert)))
    (is (= [:text "parent" :reference "parent"]
           (into [] (comp (partition-all 3) (mapcat #(take 2 %)))
                 (seq (.-attrs insert)))))))

(deftest mixed-compilation-declines-without-callbacks-or-realization
  (let [effects (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! effects conj :meta) nil)
               (toString [] (swap! effects conj :string) "host")
               (hashCode [] (swap! effects conj :hash) 0))
        update [:db/add 1 :value 1]
        insert {:db/id "new" :text "new"}
        lazy-input (lazy-seq (swap! effects conj :realize) (list update insert))]
    (doseq [forms [lazy-input [update host] [update {:text host}]
                   [update {:db/id host :text "new"}]
                   [update {:db/id nil :text "new"}]
                   [update {:db/id "a" :reference "later"} {:db/id "later" :text "later"}]
                   [update insert insert]
                   [update insert update]
                   [update {:db/id 3 :text "explicit"}]
                   [update {:identity "identity"}]
                   [update {:text 42}]
                   [update {:reference [1]}]
                   [update (with-meta insert {:ordered true})]
                   (with-meta [update insert] {:ordered true})]]
      (is (nil? (compile/compile-mixed-region ctx forms))))
    (is (nil? (compile/compile-mixed-region
                (assoc ctx :opts {:auto-entity-time? true}) [update insert])))
    (is (empty? @effects))))

(deftest ordinary-attribute-proofs-keep-writer-obligations-distinct
  (let [effects (atom [])
        schema {:value {:db/valueType :db.type/long}
                :left {:db/valueType :db.type/long}
                :right {:db/valueType :db.type/long}
                :pair {:db/valueType :db.type/tuple :db/tupleAttrs [:left :right]
                       :db/unique :db.unique/value}
                :token {:db/valueType :db.type/string :db/unique :db.unique/value}
                :identity {:db/valueType :db.type/string :db/unique :db.unique/identity}
                :reference {:db/valueType :db.type/ref}
                :many {:db/valueType :db.type/long :db/cardinality :db.cardinality/many}
                :custom {:db/valueType :app/custom}
                :predicate {:db/valueType :db.type/long
                            :db.attr/preds [(fn [_] (swap! effects conj :predicate))]}}
        sources {:left {:pair 0} :right {:pair 1}}
        infos (HashMap.)]
    ;; Built-in attributes remain eligible in a schema with unrelated custom
    ;; attributes. Unique/reference/tuple obligations still belong to the writer.
    (doseq [[attr expected]
            [[:value [true true true]]
             [:left [true true false]]
             [:pair [true false false]]
             [:token [true true false]]
             [:identity [false false false]]
             [:reference [true true false]]
             [:many [false false false]]
             [:custom [false false false]]
             [:predicate [false false false]]]]
      (let [^MixedAttrInfo info (compile/ordinary-attr-info! infos schema sources attr)]
        (is (= expected [(.-ordinary? info) (.-insert? info) (.-update? info)])
            (str attr))))
    (is (compile/ordinary-unique-attrs-present? infos))
    (is (empty? @effects))
    ;; An unsupported derived tuple blocks its source insertion proof as well.
    (let [schema (assoc-in schema [:pair :db/unique] :db.unique/identity)
          ^MixedAttrInfo info (compile/ordinary-attr-info! (HashMap.) schema sources :left)]
      (is (true? (.-ordinary? info)))
      (is (false? (.-insert? info))))))

(deftest literal-scalar-plan-owns-one-context-and-operand-stream
  (let [p (compile/compile-tx ctx [[:db/add 1 :value 42] {:db/id 1 :text "a"}])]
    (is (instance? TxPlan p))
    (is (identical? ctx (:ctx p)))
    (is (= :scalar (:strategy p)))
    (is (= [[1 :value 42] [1 :text "a"]] (:entries p)))
    (is (nil? (:entity-reads p)))))

(deftest unsupported-forms-decline-before-effects
  (let [effects (atom [])
        value (proxy [Object] [] (toString [] (swap! effects conj :conversion) "value"))
        tail (lazy-seq (swap! effects conj :tail) (list [:db/add 1 :value 1]))]
    (doseq [forms [tail
                   [[:db/add 1 :text value]]
                   [[:db/add 1 :value 1] [:db/add 1 :value 2]]
                   [[:db/add [:identity "x"] :value 1]]
                   [[:db/add 1 :reference 2]]
                   [{:value 1}]
                   [(with-meta [:db/add 1 :value 1] {:source true})]
                   [[:db/add 1 :identity "x"]]
                   [[:db.fn/call (fn [_] (swap! effects conj :callback))]]]]
      (is (nil? (compile/compile-tx ctx forms))))
    (is (empty? @effects))))

(deftest correction-failure-retains-original-input
  (let [forms [[:db/add 1 :text 42]]
        outcome (compile/compile-tx ctx forms)]
    (is (= :needs-writer (:outcome outcome)))
    (is (identical? forms (:input outcome)))
    (is (identical? ctx (:ctx outcome)))
    ;; Coercion stays on the legacy route until its writer-time obligations
    ;; migrate; the original input is still available to authoritative execution.
    (is (nil? (compile/compile-tx
                (assoc ctx :opts {:validate-data? false}) forms)))))

(deftest canonical-operation-operands-reuse-the-inert-source-buffer
  (let [forms [[:db/add 1 :value 42] [:db/add 2 :text "text"]]
        p (compile/compile-tx ctx forms)]
    (is (= :scalar-operations (:strategy p)))
    (is (identical? forms (:entries p)))
    (is (identical? ctx (:ctx p)))))

(deftest singleton-layouts-retain-order-and-normalize-literal-ids
  (doseq [form [{:db/id 1 :value 42} {:value 42 :db/id 1}
                (assoc clojure.lang.PersistentHashMap/EMPTY :db/id (int 1) :value 42)]]
    (let [p (compile/compile-tx ctx [form])]
      (is (= :scalar (:strategy p)))
      (is (= [[1 :value 42]] (:entries p)))
      (is (instance? Long (ffirst (:entries p))))))
  (doseq [form [{:db/id 1 :text 42} {:text 42 :db/id 1}]]
    (is (= :needs-writer (:outcome (compile/compile-tx ctx [form]))))))

(deftest compact-schema-proof-does-not-hide-exclusion-properties
  (doseq [props [{:db/aid 3 :db/unique :db.unique/identity}
                 {:db/aid 3 :db/cardinality :db.cardinality/many}
                 {:db/aid 3 :db/tupleType :db.type/long}
                 {:db/aid 3 :db/valueType :db.type/ref}
                 {:db/aid 3 :db/valueType :db.type/long :db/fulltext true}
                 {:db/aid 3 :db/valueType :db.type/long :db/vector true}
                 {:db/aid 3 :db/valueType :db.type/long :db/embedding true}]]
    (is (nil? (compile/compile-tx (assoc ctx :schema {:value props})
                                  [[:db/add 1 :value 42]]))))
  (is (nil? (compile/compile-tx (assoc ctx :rschema {:db/attrTuples {:value {:pair 0}}})
                                [[:db/add 1 :value 42]]))))

(deftest legacy-exclusions-precede-reference-value-conversion
  (let [effects (atom [])
        number (proxy [java.math.BigInteger] ["1"]
                 (longValue [] (swap! effects conj :conversion) 1))]
    (doseq [exclusion [{:db/cardinality :db.cardinality/many}
                       {:db/unique :db.unique/identity}
                       {:db/tupleType :db.type/long}
                       {:db.attr/preds [(constantly true)]}]]
      (is (nil? (compile/scalar-attribute-props
                  {:reference (merge {:db/aid 6 :db/valueType :db.type/ref} exclusion)}
                  {} :reference number))))
    (is (empty? @effects))))

(deftest canonical-numeric-classes-still-obey-validation-bounds
  (let [big-ctx (assoc ctx :schema
                       (assoc (:schema ctx) :big {:db/aid 8 :db/valueType :db.type/bigint}))
        too-large (.shiftLeft java.math.BigInteger/ONE 4096)]
    (is (= :needs-writer
           (:outcome (compile/compile-tx big-ctx [[:db/add 1 :big too-large]]))))))

(deftest wrapped-host-numbers-cannot-run-user-code-during-planning
  (let [effects (atom [])
        number (proxy [java.math.BigInteger] ["1"]
                 (bitLength [] 128)
                 (longValue [] (swap! effects conj :long) 1)
                 (toString [] (swap! effects conj :string) "1"))
        wrapped (clojure.lang.BigInt/fromBigInteger number)
        permissive (assoc ctx :opts {:validate-data? false})]
    (is (nil? (compile/compile-tx permissive [[:db/add wrapped :value 1]])))
    (is (nil? (compile/compile-tx permissive [[:db/add 1 :text wrapped]])))
    (is (empty? @effects))))

(deftest canonical-value-proofs-match-the-existing-correction-contract
  (doseq [[vt value] [[:db.type/long 42]
                      [:db.type/string "text"]
                      [:db.type/keyword :value]
                      [:db.type/symbol (with-meta 'value {:source true})]
                      [:db.type/boolean false]
                      [:db.type/uuid (java.util.UUID. 0 1)]
                      [:db.type/instant (java.util.Date. 1234)]
                      [:db.type/bytes (byte-array [1 2])]
                      [:db.type/bigint (biginteger 42)]
                      [:db.type/bigdec 42.00M]
                      [:db.type/float (float 0.25)]
                      [:db.type/double 0.25]
                      [nil 1/2]]]
    (let [context (assoc ctx :schema {:value {:db/aid 3 :db/valueType vt}})
          plan (compile/compile-tx context [[:db/add 1 :value value]])]
      (is (identical? value (prepare/correct-scalar-value true vt value)))
      (is (= :scalar-operations (:strategy plan)))))
  ;; Calendar formatting can consult host callbacks; keep it ordered.
  (is (nil? (compile/compile-tx (assoc ctx :opts {:validate-data? false})
                                [[:db/add 1 :text (java.util.Date. 1234)]]))))
