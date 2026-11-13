(ns datalevin.tx-compile-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.db.tx.compile :as compile]
            [datalevin.db.tx.plan :as plan]
            [datalevin.datom :as datom]
            [datalevin.prepare :as prepare])
  (:import [datalevin.db.tx.plan BlindEntity MixedAttrInfo MixedInsert MixedOperands MixedUpdate TxPlan]
           [java.util HashMap HashSet]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- compile-stage2
  "Keep the accepted Stage 2 proof controls scoped to their original rules."
  ([ctx forms] (compile-stage2 ctx forms nil))
  ([ctx forms options]
   (compile/compile-tx ctx forms (assoc options :ordered? false))))

(deftest scalar-candidate-projection-keeps-values-opaque-and-scratch-owned
  (let [calls (atom [])
        host (proxy [Object] []
               (hashCode [] (swap! calls conj :hash) 0)
               (toString [] (swap! calls conj :string) "host"))
        candidates (doto (FastList.)
                     (.add 1) (.add :text) (.add host) (.add :db.type/string)
                     (.add [:key "one"]) (.add :text) (.add nil) (.add :db.type/string))
        entries (plan/scalar-candidate-entries candidates)]
    (is (identical? host (nth (first entries) 2)))
    (is (nil? (nth (second entries) 2)))
    (is (= [:key "one"] (first (second entries))))
    (is (= 8 (.size candidates)))
    (is (identical? host (.get candidates 2)))
    (is (not (identical? entries (plan/scalar-candidate-entries candidates))))
    (is (empty? @calls))))

(def ctx
  (prepare/->PrepareCtx
    {:value {:db/aid 3 :db/valueType :db.type/long}
     :text {:db/aid 4 :db/valueType :db.type/string}
     :identity {:db/aid 5 :db/valueType :db.type/string :db/unique :db.unique/identity}
     :reference {:db/aid 6 :db/valueType :db.type/ref}}
    {} {:validate-data? true}))

(deftest existing-map-annotations-normalize-without-becoming-datom-metadata
  (let [annotations {:source :map :user/nested ["opaque"]}
        input [(with-meta (array-map :db/id (int 1) :value (int 7) :text "map") annotations)
               (with-meta {:db/id [:identity "one"] :value 8} {})
               (with-meta [:db/add 2 :text "vector" 123] {:source :vector})]
        p (compile-stage2 ctx input)]
    (is (= :explicit-additions (compile/literal-request-shape input)))
    (is (= :scalar-aliases (:strategy p)))
    (is (= [[1 :value 7 nil] [1 :text "map" nil] [[:identity "one"] :value 8 nil]
            [2 :text "vector" 123 {:source :vector}]] (:entries p)))
    (is (identical? annotations (meta (first input))))
    (is (nil? (compile/literal-scalar-shape [(first input)]))))
  (let [context (assoc ctx :rschema {:db.type/ref #{:reference}})
        p (compile-stage2 context
            [(with-meta {:db/id 1 :value 7} {:source :ignored})
             [:db/add 2 :reference 1 123]])]
    (is (= [[1 :value 7 nil] [2 :reference 1 nil]] (:entries p)))))

(deftest existing-map-annotation-declines-do-not-observe-host-values
  (let [effects (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! effects conj :meta) nil)
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))
        tail (lazy-seq (swap! effects conj :tail) (list "lazy"))
        annotations {:source host :user/lazy tail}
        custom (sorted-map-by (fn [a b] (swap! effects conj :compare) (compare a b)) :source :custom)]
    (reset! effects [])
    (is (instance? TxPlan (compile-stage2 ctx [(with-meta {:db/id 1 :value 7} annotations)])))
    (doseq [input [[(with-meta {:value 7} annotations)]
                   [(with-meta {:db/id "new" :value 7} annotations)]
                   [(with-meta {:db/id -1 :value 7} annotations)]
                   [(with-meta {:db/id 1 :value 7} annotations) {:text "new"}]
                   [(with-meta {:db/id :entity/one :value 7} annotations)]
                   [(with-meta {:db/id 1} annotations)]
                   [(with-meta {:db/id 1 :value host} annotations)]
                   [(with-meta {:db/id 1 :value 7} custom)]
                   [(with-meta {:db/id 1 :value 7} (array-map host :key))]
                   [(with-meta {:db/id 1 :value 7} {:db/internal true})]
                   [(with-meta {:db/id 1 :value 7} annotations) [:db/add 1 :value 8]]]]
      (is (nil? (compile-stage2 ctx input))))
    (is (empty? @effects))))

(deftest application-add-metadata-is-an-opaque-owned-operand
  (let [annotations {:source :test :user/note ["nested"]}
        empty-annotations {}
        input [(with-meta [:db/add 1 :value (int 7)] annotations)
               (with-meta [:db/add [:identity "one"] :text "text" 123] empty-annotations)
               {:db/id 2 :value (int 8)}]
        p (compile-stage2 ctx input)]
    (is (= :explicit-additions (compile/literal-request-shape input)))
    (is (true? (:explicit-tx? p)))
    (is (= [[1 :value 7 nil annotations] [[:identity "one"] :text "text" 123 {}]
            [2 :value 8 nil]] (:entries p)))
    (is (identical? annotations (peek (first (:entries p)))))
    (is (identical? empty-annotations (peek (second (:entries p)))))
    (is (identical? annotations (meta (first input))))
    (is (nil? (compile/literal-scalar-shape [(first input)]))))
  (let [input [(with-meta (datom/datom 1 :value 7 123) {:datalevin.db.tx.execute/internal true})]
        p (compile-stage2 ctx input)]
    (is (= [[1 :value 7 123]] (:entries p))))
  (let [context (assoc ctx :rschema {:db.type/ref #{:reference}})
        p (compile-stage2 context
            [(with-meta [:db/add 1 :reference 2 123] {:source :discarded})
             (datom/datom 2 :reference 1 456)])]
    (is (= [[1 :reference 2 nil] [2 :reference 1 456]] (:entries p)))))

(deftest metadata-proof-does-not-observe-values-or-custom-map-behavior
  (let [effects (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! effects conj :meta) nil)
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))
        tail (lazy-seq (swap! effects conj :tail) (list "lazy"))
        annotations {:source host :user/lazy tail}
        input [(with-meta [:db/add 1 :value 7] annotations)]
        custom (sorted-map-by (fn [a b] (swap! effects conj :compare) (compare a b)) :source :custom)
        p (do (reset! effects []) (compile-stage2 ctx input))]
    (is (instance? TxPlan p))
    (is (identical? annotations (peek (first (:entries p)))))
    (doseq [metadata [custom (array-map host :key) {:db/internal true} {:db.fn/call true}
                      {:idoc/patch host} {:datalevin/internal true}
                      {:datalevin.db.tx.execute/internal true}]]
      (is (nil? (compile-stage2 ctx [(with-meta [:db/add 1 :value 7] metadata)]))))
    (is (nil? (compile-stage2 ctx (with-meta input {:source :request}))))
    (is (= [[1 :value 7 nil]]
           (:entries (compile-stage2 ctx [(with-meta {:db/id 1 :value 7} annotations)]))))
    (is (empty? @effects))))

(deftest explicit-additions-retain-supplied-transaction-ids-in-owned-operands
  (let [input [(with-meta (datom/datom 1 :value (int 3) 123 true) {:source :ignored})
               [:db/add [:identity "one"] :text "text" (int 456)]
               {:db/id 2 :value (int 4)} [:db/add 2 :text "default" nil]]
        p (compile-stage2 ctx input)]
    (is (= :explicit-additions (compile/literal-request-shape input)))
    (is (= :scalar-aliases (:strategy p)))
    (is (true? (:explicit-tx? p)))
    (is (= [[1 :value 3 123] [[:identity "one"] :text "text" 456]
            [2 :value 4 nil] [2 :text "default" nil]] (:entries p)))
    (is (every? #(or (nil? (peek %)) (instance? Long (peek %))) (:entries p)))
    (is (= {:source :ignored} (meta (first input))))
    (is (nil? (compile/literal-scalar-shape [(first input)])))
    (is (nil? (compile/literal-scalar-shape [(second input)]))))
  (let [context (assoc-in ctx [:schema :tuple]
                  {:db/aid 7 :db/valueType :db.type/tuple :db/tupleType :db.type/long})
        value [(int 1) 2] p (compile-stage2 context [(datom/datom 1 :tuple value)])]
    (is (= :scalar-aliases (:strategy p)))
    (is (identical? value (nth (first (:entries p)) 2)))))

(deftest scalar-rules-decline-supplied-transaction-slots
  (doseq [tx [nil 123 (int 456)]
          eid [1 (int 1)]
          value [7 (int 7)]
          input [[[:db/add eid :value value tx]]
                 [[:db/add eid :value value tx] [:db/add 2 :text "other"]]
                 [[:db/add 2 :text "other"] [:db/add eid :value value tx]]]]
    (let [p (compile-stage2 ctx input)]
      (is (= :explicit-additions (compile/literal-request-shape input)))
      (is (true? (:explicit-tx? p)))
      (is (= (some-> tx long)
             (nth (first (filter #(= :value (second %)) (:entries p))) 3))))
    (doseq [shape [:scalar :scalar-operations]]
      (is (nil? (compile/compile-scalar-region ctx input shape)))
      (doseq [hint [:literal-shape :request-shape]]
        (is (nil? (compile-stage2 ctx input {hint shape})))))))

(deftest explicit-addition-declines-preserve-original-input-without-host-callbacks
  (let [effects (atom [])
        number (proxy [Number] []
                 (longValue [] (swap! effects conj :long) 123)
                 (intValue [] 123) (floatValue [] 123.0) (doubleValue [] 123.0)
                 (hashCode [] (swap! effects conj :hash) 123)
                 (toString [] (swap! effects conj :string) "host"))
        tail (lazy-seq (swap! effects conj :tail) (list (datom/datom 1 :value 1)))]
    (doseq [input [[(datom/datom 1 :value 2 123 false)]
                   [(datom/datom 0 :value 2)] [(datom/datom -1 :value 2)]
                   [(datom/datom 1 :value number)]
                   [(datom/datom 1 :value "bad")]
                   [[:db/add 1 :value 2 number]]
                   [[:db/add 1 :value 2 0]] [[:db/add 1 :value 2 -1]]
                   [[:db/add 1 :value 2 1.5]] [[:db/add 1 :value 2 9223372036854775808N]]
                   [(with-meta [:db/add 1 :value 2 123] {:datalevin.db.tx.execute/internal true})]
                   [(datom/datom 1 :value 2) {:text "new"}]
                   [(datom/datom 1 :value 2) [:db/add 1 :value 3 123]]
                   [(datom/datom 1 :value 2) tail] tail]]
      (is (nil? (compile-stage2 ctx input))))
    (is (nil? (compile-stage2 (assoc-in ctx [:opts :auto-entity-time?] true)
                [(datom/datom 1 :value 2)])))
    (is (empty? @effects))))

(deftest alias-maps-and-tuple-values-share-the-scalar-alias-layout
  (let [context (assoc-in ctx [:schema :tuple]
                  {:db/aid 7 :db/valueType :db.type/tuple :db/tupleType :db.type/long})
        lookup [:identity "one"] value [(int 1) 2]]
    (doseq [input [[{:db/id lookup :tuple value}]
                   [[:db/add :entity/one :tuple value]]
                   [[:db/add lookup :tuple value]]
                   [{:db/id lookup :tuple value} [:db/add 2 :value 3]]
                   [{:db/id lookup :tuple value} [:db/add :entity/one :text "text"]]]]
      (let [p (compile-stage2 context input)]
        (is (= :scalar-aliases (compile/literal-request-shape input)))
        (is (= :scalar-aliases (:strategy p)))
        (is (identical? value (nth (first (:entries p)) 2)))
        (is (nil? (:entity-reads p)))))
    (is (= :scalar-aliases (:strategy (compile-stage2 ctx [{:db/id lookup :value 3}]))))
    (let [p (compile-stage2 (assoc-in context [:opts :validate-data?] false)
              [(array-map :db/id lookup :tuple value :text 42)])]
      (is (= [[lookup :tuple value] [lookup :text "42"]] (:entries p)))
      (is (identical? value (nth (first (:entries p)) 2))))
    (is (nil? (compile-stage2 context [{:db/id lookup :tuple value}]
                {:literal-shape :scalar})))))

(deftest tuple-alias-proofs-decline-unsupported-shapes-without-host-observation
  (let [effects (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! effects conj :meta) nil)
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))
        tail (lazy-seq (swap! effects conj :tail) (list :identity "one"))
        context (assoc-in ctx [:schema :tuple]
                  {:db/aid 7 :db/valueType :db.type/tuple :db/tupleType :db.type/long})]
    (doseq [input [[{:db/id :entity/one :tuple [1]}]
                   [{:db/id host :tuple [1]}] [{:db/id tail :tuple [1]}]
                   [{:db/id [:identity host] :tuple [1]}]
                   [{:db/id (with-meta [:identity "one"] {:source true}) :tuple [1]}]
                   [(with-meta {:db/id [:identity "one"] :tuple [1]} {:db/internal true})]
                   [{:db/id [:identity "one"] :tuple [host]}]
                   [{:db/id [:identity "one"] :tuple [tail]}]
                   [{:db/id [:identity "one"] :tuple tail}]
                   [{:db/id [:identity "one"] :tuple []}]
                   [{:db/id [:identity "one"] :tuple ["bad"]}]
                   [{:db/id [:identity "one"] :tuple [1]} {:text "new"}]
                   [{:db/id [:identity "one"] :tuple [1]} [:db/add [:identity "one"] :tuple [2]]]
                   [{:db/id [:identity "one"] :tuple [1]} [:db/add [:identity "one"] :tuple [2]]]
                   [{:db/id [:identity "one"]}]]]
      (is (nil? (compile-stage2 context input))))
    (doseq [props [{:db/unique :db.unique/value} {:db/cardinality :db.cardinality/many}
                  {:db.attr/preds [(fn [_] true)]} {:db/tupleAttrs [:value]}]]
      (is (nil? (compile-stage2 (update-in context [:schema :tuple] merge props)
                  [{:db/id [:identity "one"] :tuple [1]}]))))
    (is (nil? (compile-stage2 (assoc-in context [:opts :auto-entity-time?] true)
                [{:db/id [:identity "one"] :tuple [1]}])))
    (is (empty? @effects))))

(deftest independent-tuple-replacements-share-the-mixed-writer-layout
  (let [context (assoc-in ctx [:schema :tuple]
                  {:db/aid 7 :db/valueType :db.type/tuple :db/tupleType :db.type/long})
        value [(int 3) 4]]
    (doseq [input [[[:db/add 1 :tuple value]]
                   [{:db/id (int 1) :tuple value}]
                   [[:db/add 1 :tuple value] [:db/add 2 :text "second"]]
                   [{:db/id 1 :tuple value :value (int 2)}]
                   [{:text "new"} [:db/add 1 :tuple value]]]]
      (let [p (compile-stage2 context input)
            ^MixedOperands operands (:entries p)
            ^MixedUpdate entry (first (filter #(and (instance? MixedUpdate %)
                                                    (= :tuple (.-attr ^MixedUpdate %)))
                                              (.-entries operands)))]
        (is (= :mixed (:strategy p)))
        (is (identical? value (.-value entry)))
        (is (pos? (.-required-eid operands)))
        (is (= (> (count (filter #(instance? MixedUpdate %) (.-entries operands))) 1)
               (.-prepare-reader? operands)))))
    (is (nil? (compile/compile-mixed-region ctx [[:db/add 1 :value 2]])))
    (is (nil? (compile-stage2 context [[:db/add 1 :tuple value]]
                {:literal-shape :scalar-operations})))))

(deftest tuple-replacement-analysis-preserves-legacy-roles-and-decline-purity
  (let [effects (atom []) host (proxy [Object] []
                                (hashCode [] (swap! effects conj :hash) 0)
                                (toString [] (swap! effects conj :string) "host"))
        tail (lazy-seq (swap! effects conj :tail) (list 1))
        context (assoc-in ctx [:schema :tuple]
                  {:db/aid 7 :db/valueType :db.type/tuple :db/tupleType :db.type/long})
        infos (HashMap.)
        ^MixedUpdate raw (compile/mixed-update-operand context infos 1 :tuple host true)]
    (is (identical? host (.-value raw)))
    (is (nil? (compile/mixed-update-operand context infos 1 :tuple host)))
    (is (false? (.-update? ^MixedAttrInfo (.get infos :tuple))))
    (doseq [value [nil [] [nil] ["bad"] [host] tail [tail]
                   (with-meta [1] {:source true})]
            input [[[:db/add 1 :tuple value]]
                   [{:text "new"} {:db/id 1 :tuple value}]]]
      (is (nil? (compile-stage2 context input))))
    (doseq [props [{:db/unique :db.unique/value} {:db/unique :db.unique/identity}
                  {:db/cardinality :db.cardinality/many} {:db/tupleAttrs [:value]}
                  {:db.attr/preds [(fn [_] true)]}]]
      (is (nil? (compile-stage2 (update-in context [:schema :tuple] merge props)
                  [[:db/add 1 :tuple [1]]]))))
    (doseq [input [[[:db/add 1 :tuple [1]] [:db/add (int 1) :tuple [2]]]
                   [{:db/id 1 :tuple [1]} [:db/add 1N :tuple [2]]]
                   [[:db/add 1 :tuple [1]] [:db/add 1 :value 2] [:db/add 1 :tuple [3]]]]]
      (is (nil? (compile-stage2 context input))))
    (is (nil? (compile-stage2 (assoc context :rschema {:db/attrTuples {:tuple {:pair 0}}})
                [[:db/add 1 :tuple [1]]])))
    (is (nil? (compile-stage2 (assoc-in context [:opts :auto-entity-time?] true)
                [[:db/add 1 :tuple [1]]])))
    (is (empty? @effects))))

(deftest explicit-tuple-inserts-retain-component-values-and-shared-operands
  (let [context (update ctx :schema assoc
                  :words {:db/aid 7 :db/valueType :db.type/tuple :db/tupleType :db.type/string}
                  :record {:db/aid 8 :db/valueType :db.type/tuple
                           :db/tupleTypes [:db.type/long :db.type/string]}
                  :refs {:db/aid 9 :db/valueType :db.type/tuple :db/tupleType :db.type/ref})]
    (doseq [validate? [false true]
            [attr value] [[:words ["one" "two"]] [:record [(int 1) "one"]]
                          [:record [nil nil]] [:refs [0 (int 1) (short 2) (byte 3)]]]
            input [[{attr value}]
                   [[:db/add 1 :text "updated"] {attr value}]
                   [{attr value} {:db/id 1 :value (int 2)}]]]
      (let [p (compile-stage2 (assoc-in context [:opts :validate-data?] validate?) input)
            ^MixedOperands operands (:entries p)
            ^MixedInsert insert (first (filter #(instance? MixedInsert %) (.-entries operands)))]
        (is (= :mixed (:strategy p)))
        (is (identical? value (aget ^objects (.-attrs insert) 1)))))
    (doseq [props [{:db/unique :db.unique/identity} {:db/cardinality :db.cardinality/many}
                  {:db.attr/preds [(fn [_] true)]}]]
      (is (nil? (compile-stage2 (update-in context [:schema :words] merge props)
                  [{:words ["one"]}]))))))

(deftest tuple-eligibility-declines-unsafe-components-without-observation
  (let [effects (atom [])
        host (proxy [Number] []
               (longValue [] (swap! effects conj :long) 1)
               (intValue [] 1) (floatValue [] 1.0) (doubleValue [] 1.0)
               (hashCode [] (swap! effects conj :hash) 1)
               (toString [] (swap! effects conj :string) "host"))
        tail (lazy-seq (swap! effects conj :tail) (list 1))
        context (update ctx :schema assoc
                  :tuple {:db/aid 7 :db/valueType :db.type/tuple :db/tupleType :db.type/long}
                  :refs {:db/aid 8 :db/valueType :db.type/tuple :db/tupleType :db.type/ref})]
    (doseq [validate? [false true]
            [attr value] [[:tuple []] [:tuple [nil]] [:tuple ["1"]] [:tuple [host]]
                          [:tuple [tail]] [:tuple tail] [:tuple (list 1)]
                          [:tuple (with-meta [1] {:source true})]
                          [:refs [nil]] [:refs [-1]] [:refs ["self"]] [:refs [1N]]
                          [:refs [:entity/one]] [:refs [[:identity "one"]]]]]
      (let [input [{:db/id "self" attr value}]]
        (is (nil? (compile-stage2 (assoc-in context [:opts :validate-data?] validate?) input)))
        (is (nil? (compile-stage2 context (into [[:db/add 1 :value 2]] input))))))
    (is (empty? @effects))))

(deftest unique-tuple-proofs-preserve-the-shared-av-equality-boundary
  (let [context (update ctx :schema assoc
                  :token {:db/aid 7 :db/valueType :db.type/tuple
                          :db/tupleTypes [:db.type/string :db.type/long]
                          :db/unique :db.unique/value}
                  :times {:db/aid 8 :db/valueType :db.type/tuple :db/tupleType :db.type/instant
                          :db/unique :db.unique/value})]
    (doseq [input [[{:token ["unique" 1]}] [{:token [nil nil]}]
                   [{:times [(java.util.Date. 123)]}]]]
      (is (= :mixed (:strategy (compile-stage2 context input)))))
    (doseq [input [[{:token ["unique" (int 1)]}]
                   [{:token ["duplicate" 1]} {:token ["duplicate" (int 1)]}]
                   [{:times [(java.sql.Timestamp. 123)]}]]]
      (is (nil? (compile-stage2 context input))))
    (is (nil? (compile-stage2 (assoc-in context [:opts :auto-entity-time?] true)
                [{:token ["timestamped" 1]}])))))

(deftest ordinary-insert-plans-reuse-mixed-operands-without-existing-updates
  (let [tuple-ctx (assoc ctx
                   :schema (assoc (:schema ctx) :pair
                             {:db/aid 7 :db/valueType :db.type/tuple
                              :db/tupleAttrs [:reference :value]})
                   :rschema {:db/attrTuples {:reference {:pair 0} :value {:pair 1}}})]
    (doseq [[context forms]
            [[ctx [{:db/id "self" :reference "self" :text "self"}]]
             [ctx [{:db/id (int -1) :reference -1N}
                   {:db/id "child" :reference (short -1)}]]
             [tuple-ctx [{:value 3}]]
             [tuple-ctx [{:db/id -1N :reference (int -1) :value 3}]]]]
      (let [plan (compile-stage2 context forms)
            ^MixedOperands operands (:entries plan)]
        (is (instance? TxPlan plan))
        (is (= :mixed (:strategy plan)))
        (is (every? #(instance? MixedInsert %) (.-entries operands)))
        (is (zero? (.-required-eid operands)))
        (is (false? (.-prepare-reader? operands)))))
    (doseq [forms [[{:text "simple"}] [{:reference 2}]
                   [{:db/id "one" :value 1} {:db/id -1 :value 2}]]]
      (is (= :blind (:strategy (compile-stage2 ctx forms)))))
    (let [unique-ctx (assoc-in ctx [:schema :token]
                      {:db/aid 8 :db/valueType :db.type/string :db/unique :db.unique/value})]
      (is (nil? (compile-stage2 unique-ctx [{:token "same"} {:token "same"}]))))
    (is (nil? (compile-stage2 ctx [{:identity "small"}]
                {:allow-small-unique? false})))
    (is (= :blind (:strategy (compile-stage2 ctx [{:identity "small"}]
                               {:allow-small-unique? true}))))))

(deftest ordinary-insert-selection-declines-unsupported-dependencies-without-effects
  (let [effects (atom [])
        host (proxy [Number] []
               (longValue [] (swap! effects conj :long) -1)
               (intValue [] -1) (floatValue [] -1.0) (doubleValue [] -1.0)
               (hashCode [] (swap! effects conj :hash) -1))
        text (proxy [Object] [] (toString [] (swap! effects conj :string) "host"))
        tail (lazy-seq (swap! effects conj :realize) (list {:text "tail"}))]
    (doseq [forms [[{:db/id "first" :reference "later"} {:db/id "later" :text "later"}]
                   [{:db/id (int -1) :reference -1N} {:db/id -1N :text "duplicate"}]
                   [{:db/id "self" :reference "self" :identity "upsert"}]
                   [{:db/id "self" :reference "self" :text text}]
                   [{:db/id host :reference host}]
                   [{:db/id "self" :reference tail}]
                   [{:db/id "self" :reference {:text "nested"}}]
                   (with-meta [{:db/id "self" :reference "self"}] {:source true})
                   [(with-meta {:db/id "self" :reference "self"} {:source true})]
                   tail]]
      (is (nil? (compile-stage2 ctx forms))))
    (is (nil? (compile-stage2 (assoc-in ctx [:opts :auto-entity-time?] true)
                [{:db/id "self" :reference "self"}])))
    (is (empty? @effects))))

(deftest blind-identity-read-selection-is-shared-and-excludes-the-key
  (let [fields (mapv #(keyword (str "field-" %)) (range 8))
        schema (into {:key {:db/aid 1 :db/valueType :db.type/string}}
                     (map-indexed (fn [index attr] [attr {:db/aid (+ index 2)
                                                        :db/valueType :db.type/long}]) fields))
        attrs (doto (FastList.) (.add :key) (.add "one"))
        _ (doseq [attr (reverse fields)] (.add attrs attr) (.add attrs 1))
        entities (doto (FastList.) (.add (BlindEntity. "new" attrs)))
        read (compile/blind-identity-entity-read schema entities [:key "one"])]
    (is (= fields (vec (.-names ^datalevin.db.tx.plan.ScalarEntityRead read))))
    (is (= (vec (range 2 10)) (vec (.-aids ^datalevin.db.tx.plan.ScalarEntityRead read))))
    (is (nil? (compile/blind-identity-entity-read schema nil nil)))))

(deftest mixed-update-analysis-retains-opaque-values-and-unbound-ids
  (let [calls (atom [])
        host (proxy [Object] []
               (hashCode [] (swap! calls conj :hash) 0)
               (toString [] (swap! calls conj :string) "normalized"))
        ^MixedUpdate entry (compile/mixed-update-operand ctx (HashMap.) Long/MAX_VALUE :text host)]
    (is (= Long/MAX_VALUE (.-e entry)))
    (is (= :text (.-attr entry)))
    (is (identical? host (.-value entry)))
    (is (nil? (compile/compile-mixed-region ctx
                [[:db/add Long/MAX_VALUE :text host] {:text "new"}])))
    (is (empty? @calls))))

(deftest mixed-update-analysis-declines-unsafe-roles-before-scratch-or-callbacks
  (let [calls (atom []) infos (HashMap.)
        host (proxy [Number] []
               (hashCode [] (swap! calls conj :hash) 0)
               (longValue [] (swap! calls conj :long) 1)
               (intValue [] 1) (floatValue [] 1.0) (doubleValue [] 1.0))]
    (doseq [[e a v] [[host :text "value"] [(int 1) :text "value"] [1N :text "value"]
                     [0 :text "value"] [-1 :text "value"] [1 host "value"]
                     [1 :text nil] [1 (keyword "") "value"]]]
      (is (nil? (compile/mixed-update-operand ctx infos e a v))))
    (is (.isEmpty infos))
    (is (empty? @calls))))

(deftest mixed-update-analysis-is-a-shape-proof-without-distinctness
  (let [infos (HashMap.)]
    (doseq [a [:identity :reference :unknown]]
      (is (nil? (compile/mixed-update-operand ctx infos 1 a "value"))))
    (is (nil? (compile/mixed-update-operand
                (assoc-in ctx [:opts :auto-entity-time?] true) infos 1 :text "value")))
    ;; Duplicate E/A and safe coercion are independent, later compiler proofs.
    (is (some? (compile/mixed-update-operand ctx infos 1 :text 42)))
    (is (some? (compile/mixed-update-operand ctx infos 1 :text 43)))
    (is (nil? (compile/compile-mixed-region ctx
                [[:db/add 1 :text "first"] {:text "new"} [:db/add 1 :text "second"]])))))

(deftest compiled-mixed-reference-operands-preserve-legacy-role-boundaries
  (doseq [value [(byte 2) (short 2) (int 2) 2 2N (biginteger 2) Long/MAX_VALUE]]
    (let [infos (HashMap.)
          ^MixedUpdate operand (compile/mixed-update-operand ctx infos 1 :reference value true)
          plan (compile-stage2 (assoc-in ctx [:opts :validate-data?] false)
                 [[:db/add 1 :reference value] {:text "new"}])]
      (is (nil? (compile/mixed-update-operand ctx infos 1 :reference value)))
      (is (identical? value (.-value operand)))
      (is (instance? TxPlan plan))
      (is (= (long value) (.-value ^MixedUpdate
                           (first (.-entries ^MixedOperands (:entries plan))))))))
  (let [calls (atom [])
        host (proxy [Number] []
               (longValue [] (swap! calls conj :long) 2)
               (intValue [] 2) (floatValue [] 2.0) (doubleValue [] 2.0)
               (hashCode [] (swap! calls conj :hash) 2))]
    (doseq [value [nil 0 -1 "new" [:identity "new"] 2.0 2M 3/2
                   9223372036854775808N host]]
      (is (nil? (compile/mixed-update-operand ctx (HashMap.) 1 :reference value true))))
    (doseq [props [{:db/cardinality :db.cardinality/many}
                   {:db/unique :db.unique/value} {:db/unique :db.unique/identity}
                   {:db/fulltext true} {:db/embedding true} {:db/vector true}]]
      (is (nil? (compile-stage2 (update-in ctx [:schema :reference] merge props)
                  [[:db/add 1 :reference 2] {:text "new"}]))))
    (doseq [value [2N (biginteger 2)]]
      ;; A pure correction rejection retains the existing validation route.
      (is (nil? (compile-stage2 ctx [[:db/add 1 :reference value] {:text "new"}]))))
    (is (nil? (compile-stage2 (assoc ctx :rschema {:db/attrTuples {:reference {:pair 0}}})
                [[:db/add 1 :reference 2] {:text "new"}])))
    (is (empty? @calls))))

(deftest mixed-insert-analysis-retains-opaque-symbolic-values
  (let [calls (atom [])
        host (proxy [Object] []
               (hashCode [] (swap! calls conj :hash) 0)
               (toString [] (swap! calls conj :string) "normalized"))
        form (array-map :text host :db/id "self" :reference "self" :value 42)
        ^MixedInsert entry (compile/mixed-insert-operands ctx (HashMap.) form)
        attrs (vec (.-attrs entry))]
    (is (= "self" (.-tempid entry)))
    (is (= [:text :reference :value] (mapv first (partition 3 attrs))))
    (is (identical? host (second attrs)))
    (is (= "self" (nth attrs 4)))
    (is (nil? (compile/compile-mixed-region ctx [[:db/add 1 :value 1] form])))
    (is (empty? @calls))))

(deftest mixed-insert-analysis-declines-unsafe-shapes-without-effects
  (let [calls (atom [])
        host (proxy [Number] []
               (hashCode [] (swap! calls conj :hash) (hash :db/id))
               (equals [_] (swap! calls conj :equals) false)
               (longValue [] (swap! calls conj :long) -1)
               (intValue [] -1) (floatValue [] -1.0) (doubleValue [] -1.0))
        bad-key (assoc clojure.lang.PersistentHashMap/EMPTY host "opaque" :text "text")
        forms [{} {:db/id "empty"} {:db/id 1 :text "existing"}
               {:db/id -1.0 :text "fractional"} {:db/id host :text "numeric"}
               {:identity "identity"} (array-map host "opaque" :text "text") bad-key
               (sorted-map :text "custom") (with-meta {:text "text"} {:source true})]]
    (reset! calls [])
    (doseq [form forms]
      (is (nil? (compile/mixed-insert-operands ctx (HashMap.) form))))
    (is (nil? (compile/mixed-insert-operands
                (assoc-in ctx [:opts :auto-entity-time?] true) (HashMap.) {:text "text"})))
    (is (empty? @calls))))

(deftest mixed-insert-analysis-retains-integer-tempids-and-cross-representation-refs
  (doseq [id [(byte -1) (short -1) (int -1) -1 -1N (biginteger -1)]]
    (let [form {:db/id id :text "integer" :reference -1N}
          ^MixedInsert operand (compile/mixed-insert-operands ctx (HashMap.) form)
          plan (compile/compile-mixed-region ctx [[:db/add 1 :value 7] form])]
      (is (identical? id (.-tempid operand)))
      (is (instance? TxPlan plan))
      (is (identical? id (.-tempid ^MixedInsert
                          (second (.-entries ^MixedOperands (:entries plan))))))
      (is (nil? (compile/compile-mixed-region ctx
                  [[:db/add 1 :value 7] form {:db/id (int -1) :value 8}]))))))

(deftest mixed-insert-operands-own-their-normalization-buffer
  (let [context (assoc ctx :opts {:validate-data? false})
        form {:db/id "new" :text 42}
        infos (HashMap.)
        ^MixedInsert first-entry (compile/mixed-insert-operands context infos form)
        ^MixedInsert second-entry (compile/mixed-insert-operands context infos form)
        compiled (compile/compile-mixed-region context [[:db/add 1 :value 1] form])
        ^MixedInsert compiled-entry (second (.-entries ^MixedOperands (:entries compiled)))]
    (is (not (identical? (.-attrs first-entry) (.-attrs second-entry))))
    (is (= 42 (aget (.-attrs first-entry) 1)))
    (is (= "42" (aget (.-attrs compiled-entry) 1)))
    (is (= 42 (:text form)))))

(deftest blind-operands-own-one-stream-with-symbolic-ids
  (let [plan (compile/compile-blind-region ctx
               [(array-map :text "one" :db/id "one" :value 1)
                {:db/id -1 :reference 7} {}])
        ^BlindEntity entity (first (:entries plan))]
    (is (= :blind (:strategy plan)))
    (is (identical? ctx (:ctx plan)))
    (is (= 2 (count (:entries plan))))
    (is (= "one" (.-tempid entity)))
    (is (= [:text "one" :value 1] (vec (.-attrs entity))))
    (is (not (contains? plan :entities)))
    (is (not (contains? plan :schema-epoch)))
    (is (false? (:has-many? plan)))
    (is (false? (:has-unique? plan)))
    (is (false? (:fuse-unique-inserts? plan))))
  (let [timed (compile/compile-blind-region
                (assoc ctx :opts {:auto-entity-time? true}) [{} {:db/id "empty"}])]
    (is (:auto-entity-time? timed))
    (is (= 2 (count (:entries timed))))))

(deftest blind-compilation-defers-effects-and-unsupported-specializations
  (let [calls (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! calls conj :meta) nil)
               (hashCode [] (swap! calls conj :hash) 0)
               (toString [] (swap! calls conj :string) "converted"))
        tail (lazy-seq (swap! calls conj :realize) (list {:text "late"}))]
    (doseq [forms [tail [host] [{:text host}] [(array-map host "value")]
                   [{:db/id host :text "value"}] [{:db/id nil :text "value"}]
                   [{:db/id 1 :text "existing"}] [{:db/id "one" :text "one"} {:db/id "one"}]
                   [{:reference "one"}] [{:reference 0}] [{:value "coercion"}]
                   [{:identity "one"}] [{:value 1} [:db/add 1 :value 2]]
                   [(with-meta {:value 1} {:source true})]
                   (with-meta [{:value 1}] {:source true})]]
      (is (nil? (compile/compile-blind-region ctx forms))))
    (is (empty? @calls))))

(def ^:private many-ctx
  (assoc ctx
         :schema (merge (:schema ctx)
                        {:numbers {:db/aid 7 :db/valueType :db.type/long
                                   :db/cardinality :db.cardinality/many}
                         :doubles {:db/aid 8 :db/valueType :db.type/double
                                   :db/cardinality :db.cardinality/many}
                         :decimals {:db/aid 9 :db/valueType :db.type/bigdec
                                    :db/cardinality :db.cardinality/many}
                         :times {:db/aid 10 :db/valueType :db.type/instant
                                 :db/cardinality :db.cardinality/many}
                         :byte-values {:db/aid 11 :db/valueType :db.type/bytes
                                       :db/cardinality :db.cardinality/many}
                         :token {:db/aid 12 :db/valueType :db.type/string
                                 :db/unique :db.unique/value}
                         :unique-many {:db/aid 13 :db/valueType :db.type/long
                                       :db/unique :db.unique/value
                                       :db/cardinality :db.cardinality/many}
                         :unindexed {:db/aid 14 :db/valueType :db.type/long :db/noindex true}})
         :rschema {:db.unique/identity #{:identity}}))

(deftest ordinary-proof-keeps-map-operation-and-context-boundaries
  (is (compile/ordinary-analysis-enabled? many-ctx))
  (is (not (compile/ordinary-analysis-enabled?
             (assoc-in many-ctx [:opts :auto-entity-time?] true))))
  (is (not (compile/ordinary-analysis-enabled?
             (assoc-in many-ctx [:rschema :db.type/tuple] #{:tuple}))))
  (doseq [[form flags] [[{:db/id 0 :value 1} 0]
                        [{:db/id "new" :token "one"} 1]
                        [{:value 1} 2]
                        [{:token "one"} 3]
                        [[:db/add 0 :reference 0] 0]
                        [[:db/add 1 :numbers 2] 0]
                        [[:db/add 1 :value 2 12345] 0]
                        [{:db/id -1 :unknown "new attribute"} 0]]]
    (is (= flags (compile/ordinary-addition-flags many-ctx form true false))))
  (doseq [form [{:value 1} {:db/id 1 :numbers [1 2]}
                {:db/id 1 :identity "one"} [:db/add 1 :value nil]
                [:db/add -1 :value 2] [:db/add 1 :reference "new"]
                [:db/add 1 :value 2 :bad-tx]
                (with-meta [:db/add 1 :value 2] {:source true})]]
    (is (nil? (compile/ordinary-addition-flags many-ctx form))))
  (let [private-id (Object.) form {:db/id private-id :value 1}
        entities [form]
        proof (compile/ordinary-addition-proof entities 3 true)]
    (is (nil? (compile/ordinary-addition-flags many-ctx form)))
    (is (= 0 (compile/ordinary-addition-flags many-ctx form false true)))
    (is (identical? entities (:entities proof)))
    (is (= {:entities entities :has-unique? true :implicit-ids? true
            :stage-resolver? true} proof))))

(deftest ordinary-analysis-declines-without-host-or-lazy-effects
  (let [calls (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! calls conj :meta) nil)
               (hashCode [] (swap! calls conj :hash) 0)
               (toString [] (swap! calls conj :string) "host"))
        host-number (proxy [java.math.BigInteger] ["1"]
                      (bitLength [] 128)
                      (longValue [] (swap! calls conj :long) 1)
                      (hashCode [] (swap! calls conj :hash-number) 1))
        wrapped (clojure.lang.BigInt/fromBigInteger host-number)
        lazy-form (lazy-seq (swap! calls conj :realize) (list :db/add 1 :value 2))]
    (doseq [form [host lazy-form {:db/id 1 :value host}
                  {:db/id host :value 1} {:db/id wrapped :value 1}
                  [:db/add wrapped :value 1] [:db/add 1 :reference wrapped]
                  [:db/add 1 :value wrapped] [:db/add 1 :value 2 wrapped]]]
      (is (nil? (compile/ordinary-addition-flags many-ctx form))))
    (is (empty? @calls))))

(deftest canonical-many-operands-retain-the-first-storage-equivalent-value
  (let [timestamp (java.sql.Timestamp. 1000) bytes (byte-array [1 2])
        plan (compile/compile-blind-region many-ctx
               [(array-map :doubles [-0.0 0.0] :decimals [1.00M 1.0M]
                           :times [timestamp (java.util.Date. 1000)]
                           :byte-values [bytes (byte-array [1 2])]
                           :numbers #{1 2})])
        ^BlindEntity entity (first (:entries plan))
        pairs (mapv vec (partition 2 (.-attrs entity)))
        values (into {} pairs)]
    (is (instance? TxPlan plan))
    (is (true? (:has-many? plan)))
    (is (= 6 (count pairs)))
    (is (= (Double/doubleToRawLongBits -0.0)
           (Double/doubleToRawLongBits (:doubles values))))
    (is (= 2 (.scale ^java.math.BigDecimal (:decimals values))))
    (is (identical? timestamp (:times values)))
    (is (identical? bytes (:byte-values values))))
  (let [plan (compile/compile-blind-region many-ctx [{:value 1 :numbers []}])]
    (is (false? (:has-many? plan)))))

(deftest unique-proofs-preserve-eager-deferred-and-fusion-decisions
  (doseq [check? [false true]]
    (let [plan (compile/compile-blind-region many-ctx [{:token "one" :numbers [1 1 2]}]
                 {:check-unique-avs? check?})]
      (is (= [[:token "one"]] (mapv vec (:unique-avs plan))))
      (is (true? (:has-unique? plan)))
      (is (true? (:has-many? plan)))
      (is (true? (:fuse-unique-inserts? plan)))))
  (is (nil? (compile/compile-blind-region many-ctx [{:token "one"} {:token "one"}])))
  (is (instance? TxPlan
                 (compile/compile-blind-region many-ctx [{:token "one"} {:token "one"}]
                   {:check-unique-avs? false})))
  (is (nil? (compile/compile-blind-region many-ctx
              [{:token "one" :unindexed 1} {:token "one"}] {:check-unique-avs? false})))
  (doseq [check? [false true]]
    (is (nil? (compile/compile-blind-region many-ctx [{:unique-many [1 1]}]
                {:check-unique-avs? check?}))))
  (let [plan (compile/compile-blind-region many-ctx [{:db/id "one" :identity "one" :value 1}]
               {:allow-small-unique? true})]
    (is (= [:identity "one"] (:identity-upsert-av plan)))
    (is (true? (:independent-identity-upserts? plan)))
    (is (nil? (:identity-upsert-read plan))))
  (is (nil? (compile/compile-blind-region many-ctx [{:identity "one"}]))))

(deftest many-and-unique-lookahead-does-not-invoke-host-code
  (let [calls (atom [])
        host (proxy [Object] []
               (hashCode [] (swap! calls conj :hash) 0)
               (toString [] (swap! calls conj :string) "host"))
        date (proxy [java.util.Date] [1000]
               (getTime [] (swap! calls conj :clock) 1000))
        lazy-values (lazy-seq (swap! calls conj :realize) (list 1))]
    (doseq [forms [[{:numbers [1 host]}] [{:token host}]
                   [{:times [date]}] [{:numbers lazy-values}]
                   [{:numbers [:identity host]}] [{:unique-many [1 nil]}]]]
      (is (nil? (compile/compile-blind-region many-ctx forms {:allow-small-unique? true}))))
    (is (empty? @calls))))

(deftest blind-coercions-normalize-before-many-and-unique-proofs
  (let [context (assoc many-ctx :opts {:validate-data? false})
        forms [(array-map :db/id "new" :token 42 :numbers [(int 1) 1.9 2N]
                          :doubles [(float -0.0) 0.0] :decimals [1 1.00M]
                          :times [1000 (java.time.Instant/ofEpochMilli 1000)]
                          :reference 2N :text false)]
        p (compile/compile-blind-region context forms {:allow-small-unique? true})
        ^BlindEntity entity (first (:entries p))
        pairs (mapv vec (partition 2 (.-attrs entity)))
        values (group-by first pairs)]
    (is (instance? TxPlan p))
    (is (identical? context (:ctx p)))
    (is (= [[:token "42"]] (mapv vec (:unique-avs p))))
    (is (= [1 2] (mapv second (:numbers values))))
    (is (every? #(instance? Long (second %)) (:numbers values)))
    (is (= (Double/doubleToRawLongBits -0.0)
           (Double/doubleToRawLongBits (second (first (:doubles values))))))
    (is (= [1M] (mapv second (:decimals values))))
    (is (= [(java.util.Date. 1000)] (mapv second (:times values))))
    (is (= [[:reference 2]] (:reference values)))
    (is (= [[:text "false"]] (:text values)))
    (is (true? (:has-many? p)))
    (is (true? (:fuse-unique-inserts? p))))
  (let [context (assoc many-ctx :opts {:validate-data? false})]
    (doseq [check? [false true]]
      (is (nil? (compile/compile-blind-region context [{:unique-many [1 1.9]}]
                  {:allow-small-unique? true :check-unique-avs? check?}))))
    (is (nil? (compile/compile-blind-region context [{:token 42} {:token "42"}])))
    (is (instance? TxPlan
          (compile/compile-blind-region context [{:token 42} {:token "42"}]
            {:check-unique-avs? false})))
    (is (nil? (compile/compile-blind-region context
                [{:token 42 :unindexed 1} {:token "42"}] {:check-unique-avs? false})))
    (doseq [length [100 101]]
      (is (= (= length 100)
             (some? (compile/compile-blind-region context
                      [{:token (biginteger (apply str (repeat length "1")))}]
                      {:allow-small-unique? true})))))
    (let [p (compile/compile-blind-region context [{:identity 'one :value (int 2)}]
              {:allow-small-unique? true})]
      (is (= [:identity "one"] (:identity-upsert-av p)))
      (is (true? (:independent-identity-upserts? p))))))

(deftest blind-coercion-declines-without-callbacks-or-interning
  (let [calls (atom []) word (str "blind-compiler-purity-" (random-uuid))
        context (-> many-ctx (assoc :opts {:validate-data? false})
                    (assoc-in [:schema :word] {:db/aid 15 :db/valueType :db.type/keyword}))
        host (proxy [Object] []
               (hashCode [] (swap! calls conj :hash) 0)
               (toString [] (swap! calls conj :string) "host"))
        values (lazy-seq (swap! calls conj :tail) (list 1))]
    (is (nil? (clojure.lang.Keyword/find word)))
    (doseq [forms [[{:text 42} {:numbers [1 host]}]
                   [{:text 42} {:numbers values}]
                   [{:text 42} {:unknown 1}]
                   [{:db/id "same" :text 42} {:db/id "same" :text 43}]
                   [{:text 42} {:word word}]
                   [{:text (java.util.Date. 1)}]
                   [{:reference 0}] [{:reference 1.0}]
                   [{:text 42} {:reference [:identity "one"]}]]]
      (is (nil? (compile/compile-blind-region context forms))))
    (is (nil? (clojure.lang.Keyword/find word)))
    (is (empty? @calls))))

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

(deftest mixed-coercions-match-scalar-correction-without-changing-input
  (doseq [[vt raw]
          [[:db.type/long (int 123)] [:db.type/long 1.25]
           [:db.type/ref (int 2)] [:db.type/ref 2N] [:db.type/ref (int 0)]
           [:db.type/double (float -0.0)] [:db.type/float 1.25]
           [:db.type/bigint 123N] [:db.type/bigdec 123]
           [:db.type/string 42] [:db.type/string false]
           [:db.type/boolean 0] [:db.type/boolean false]
           [:db.type/instant 123] [:db.type/instant (java.time.Instant/ofEpochMilli 123)]
           [:db.type/uuid "550e8400-e29b-41d4-a716-446655440000"]]
          validate? [false true]
          side (if (= vt :db.type/ref) [:insert] [:insert :update])]
    (let [context (-> ctx (assoc-in [:schema :converted] {:db/aid 7 :db/valueType vt})
                      (assoc :opts {:validate-data? validate?}))
          forms (if (= side :insert)
                  [[:db/add 1 :value 1] {:db/id "new" :converted raw}]
                  [[:db/add 1 :converted raw] {:db/id "new" :text "new"}])
          expected (try {:value (prepare/correct-scalar-value validate? vt raw)}
                        (catch Exception _ {:rejected true}))
          p (compile-stage2 context forms)]
      (if (:rejected expected)
        (is (nil? p))
        (let [^MixedOperands operands (:entries p)
              entries (.-entries operands)
              actual (if (= side :insert)
                       (aget ^objects (.-attrs ^MixedInsert (nth entries 1)) 1)
                       (.-value ^MixedUpdate (nth entries 0)))
              wanted (:value expected)]
          (is (= :mixed (:strategy p)))
          (is (identical? context (:ctx p)))
          (is (= (class wanted) (class actual)))
          (cond
            (instance? Double wanted)
            (is (= (Double/doubleToRawLongBits wanted) (Double/doubleToRawLongBits actual)))
            (instance? Float wanted)
            (is (= (Float/floatToRawIntBits wanted) (Float/floatToRawIntBits actual)))
            :else (is (= wanted actual)))))
      (is (identical? raw (if (= side :insert)
                           (:converted (nth forms 1)) (nth (first forms) 3)))))))

(deftest mixed-coercion-retains-symbolic-refs-and-declines-before-effects
  (let [effects (atom []) word (str "mixed-compiler-purity-" (random-uuid))
        context (-> ctx (assoc :opts {:validate-data? false})
                    (assoc-in [:schema :word] {:db/aid 7 :db/valueType :db.type/keyword})
                    (assoc-in [:schema :uuid] {:db/aid 8 :db/valueType :db.type/uuid}))
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! effects conj :meta) nil)
               (toString [] (swap! effects conj :string) "host")
               (hashCode [] (swap! effects conj :hash) 0))
        number (proxy [java.math.BigInteger] ["1"]
                 (bitLength [] 128)
                 (longValue [] (swap! effects conj :long) 1))
        wrapped (clojure.lang.BigInt/fromBigInteger number)
        tail (lazy-seq (swap! effects conj :tail) (list {:text "late"}))
        update [:db/add 1 :value 1.25]
        forms [update (array-map :db/id "self" :reference "self" :text 42)
               {:db/id -1 :reference "self" :value 2N} {:reference -1}]
        p (compile-stage2 context forms)
        ^MixedOperands operands (:entries p)
        inserts (rest (.-entries operands))]
    (is (= :mixed (:strategy p)))
    (is (= ["self" -1 nil] (mapv #(.-tempid ^MixedInsert %) inserts)))
    (is (= ["self" "self" -1]
           (mapv (fn [^MixedInsert entry]
                   (get (into {} (map #(vector (first %) (second %))
                                     (partition 3 (seq (.-attrs entry))))) :reference)) inserts)))
    (is (nil? (clojure.lang.Keyword/find word)))
    (doseq [forms [[update host] [update {:text host}] [update {:value wrapped}]
                   [update {:reference wrapped}] [update {:word word}]
                   [update {:text (java.util.Date. 1)}] [update {:text (byte-array [1])}]
                   [update {:text tail}] [update {:uuid "invalid"}]
                   [update {:db/id "self" :reference "later"} {:db/id "later" :text 42}]
                   [update {:reference 1.5}] [update {:reference -2}]
                   [update {:text 42} update] [update {:text 42} {:identity "unsupported"}]
                   [update {:text 42} (with-meta {:text "late"} {:source true})]]]
      (is (nil? (compile-stage2 context forms))))
    (is (nil? (clojure.lang.Keyword/find word)))
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
  (let [p (compile-stage2 ctx [[:db/add 1 :value 42] {:db/id 1 :text "a"}])]
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
                   [[:db/add 1 :reference :entity/two]]
                   [(with-meta [:db/add 1 :value 1] {:datalevin.db.tx.execute/internal true})]
                   [[:db/add 1 :identity "x"]]
                   [[:db.fn/call (fn [_] (swap! effects conj :callback))]]]]
      (is (nil? (compile-stage2 ctx forms))))
    (is (empty? @effects))))

(deftest unified-selection-chooses-one-materialized-family
  (doseq [[forms shape strategy]
          [[[[:db/add 1 :value 1]] :scalar-operations :scalar-operations]
           [[{:db/id 1 :value 1}] :scalar :scalar]
           [[{:value 1}] :blind :blind]
           [[{:db/id "new" :value 1} [:db/add 1 :text "one"]] :mixed :mixed]]]
    (is (= shape (compile/literal-request-shape forms)))
    (is (= strategy (:strategy (compile-stage2 ctx forms)))))
  (is (= :mixed (compile/literal-request-shape [{:db/id 1 :value 1} {:value 2}]))))

(deftest mixed-map-replacements-share-ordered-operands-and-normalized-ids
  (let [context (assoc ctx :opts {:validate-data? false})
        forms [(array-map :text 42 :db/id (int 7) :value 1.9)
               {:db/id "self" :reference "self" :text false}
               [:db/add 2N :text :changed/value]
               {:db/id 7N :reference 2}]
        ;; Concrete reference replacements now share the compiled mixed layout.
        _ (is (instance? TxPlan (compile-stage2 context forms)))
        forms (assoc forms 3 {:db/id 7N :untyped 3})
        context (assoc-in context [:schema :untyped] {:db/aid 7})
        plan (compile-stage2 context forms)
        ^MixedOperands operands (:entries plan)
        entries (.-entries operands)
        updates (filter #(instance? MixedUpdate %) entries)]
    (is (= :mixed (:strategy plan)))
    (is (identical? context (:ctx plan)))
    (is (= [MixedUpdate MixedUpdate MixedInsert MixedUpdate MixedUpdate] (mapv class entries)))
    (is (= [[7 :text "42"] [7 :value 1] [2 :text ":changed/value"] [7 :untyped 3]]
           (mapv (fn [^MixedUpdate entry] [(.-e entry) (.-attr entry) (.-value entry)]) updates)))
    (is (every? #(instance? Long (.-e ^MixedUpdate %)) updates))
    (is (= 7 (.-required-eid operands)))
    (is (false? (.-prepare-reader? operands)))
    (is (instance? Integer (:db/id (first forms))))
    (is (= 1.9 (:value (first forms))))))

(deftest mixed-map-id-aliases-and-unsupported-fields-decline-without-effects
  (let [effects (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! effects conj :meta) nil)
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))
        wrapped (clojure.lang.BigInt/fromBigInteger
                  (proxy [java.math.BigInteger] ["1"]
                    (bitLength [] 128)
                    (longValue [] (swap! effects conj :long) 1)))
        context (assoc many-ctx :opts {:validate-data? false})
        insert {:text 42}]
    (doseq [forms [[{:db/id 1 :value 1} insert {:db/id (int 1) :value 2}]
                   [{:db/id 1N :value 1} insert [:db/add (biginteger 1) :value 2]]
                   [[:db/add 1N :value 1] insert [:db/add (int 1) :value 2]]
                   [{:db/id 1 :numbers [1 2]} insert]
                   [{:db/id 1 :identity "one"} insert]
                   [{:db/id 1 :value host} insert]
                   [{:db/id wrapped :value 1} insert]
                   [{:db/id 1} insert]
                   [{:db/id 0 :value 1} insert]
                   [(with-meta {:db/id 1 :value 1} {:source true}) insert]
                   [{:db/id 1 :value 1} insert host]]]
      (is (nil? (compile-stage2 context forms))))
    (is (empty? @effects)))
  (doseq [eid [1 (int 1) 1N (biginteger 1)]]
    (let [plan (compile-stage2 ctx [{:db/id eid :value 1} {:text "new"}])
          ^MixedOperands operands (:entries plan)]
      (is (= :mixed (:strategy plan)))
      (is (true? (.-prepare-reader? ^MixedOperands
                   (:entries (compile-stage2 ctx
                               [{:db/id eid :value 1 :text "updated"} {:text "new"}])))))
      (is (= 1 (.-required-eid operands))))))

(deftest canonical-scalar-aliases-retain-symbolic-entity-operands
  (let [lookup [:identity "one"]
        forms [[:db/add lookup :value 1] [:db/add :entity/one :text "text"]
               {:db/id 2N :value 2}]
        plan (compile-stage2 ctx forms)]
    (is (= :scalar-aliases (compile/literal-request-shape forms)))
    (is (= :scalar-aliases (:strategy plan)))
    (is (identical? ctx (:ctx plan)))
    (is (= [[lookup :value 1] [:entity/one :text "text"] [2 :value 2]]
           (:entries plan)))
    (is (identical? lookup (ffirst (:entries plan))))
    (is (nil? (:entity-reads plan)))
    ;; Alias resolution can make these the same E/A; compilation grants no
    ;; resolved distinctness proof. Runtime must decline before old-value reads.
    (is (instance? TxPlan (compile-stage2 ctx
                           [[:db/add lookup :value 1] [:db/add 1 :value 2]])))))

(deftest scalar-aliases-decline-without-correction-or-host-callbacks
  (let [effects (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! effects conj :meta) nil)
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))
        lazy-ref (lazy-seq (swap! effects conj :lookup-tail) (list :identity "one"))]
    (doseq [forms [[[:db/add host :value 1]]
                   [[:db/add [:identity host] :value 1]]
                   [[:db/add [host "one"] :value 1]]
                   [[:db/add lazy-ref :value 1]]
                   [[:db/add :entity/one :text host]]
                   [[:db/add [:text "one"] :value 1]]
                   [[:db/add [:identity 7] :value 1]]
                   [[:db/add :entity/one :value "invalid"]]
                   [[:db/add :entity/one :text 7]]
                   [[:db/add :entity/one :reference [:identity "one"]]]
                   [[:db/add :entity/one :value 1] {:value 2}]
                   [[:db/add :entity/one :value 1] [:db/add :entity/one :value 2]]
                   [[:db/add (with-meta [:identity "one"] {:source true}) :value 1]]]]
      (is (nil? (compile-stage2 ctx forms))))
    (is (empty? @effects)))
  (is (nil? (compile-stage2 (assoc ctx :opts {:auto-entity-time? true})
                               [[:db/add :entity/one :value 1]]))))

(deftest canonical-reference-values-share-scalar-operands-outside-stage1
  (doseq [[forms strategy]
          [[[[:db/add 1 :reference 2]] :scalar-operations]
           [[{:db/id 1 :reference Long/MAX_VALUE}] :scalar]
           [[[:db/add [:identity "one"] :reference 2]] :scalar-aliases]]]
    (let [p (compile-stage2 ctx forms)]
      (is (= strategy (:strategy p)))
      (is (identical? ctx (:ctx p)))))
  (doseq [forms [[[:db/add 1 :reference 2]] [{:db/id 1 :reference 2}]]]
    (is (nil? (compile-stage2 ctx forms
                {:literal-shape (compile/literal-scalar-shape forms)}))))
  (doseq [value [0 -1 2N "new" :entity/two [:identity "two"]]]
    (is (not (instance? TxPlan (compile-stage2 ctx [[:db/add 1 :reference value]]))))
    (is (nil? (compile-stage2 ctx [[:db/add :entity/one :reference value]]))))
  (let [reference-key (assoc-in ctx [:schema :reference-key]
                               {:db/aid 7 :db/valueType :db.type/ref :db/unique :db.unique/value})]
    (is (= :scalar-aliases
           (:strategy (compile-stage2 reference-key
                        [[:db/add [:reference-key 2] :reference 3]]))))))

(deftest correction-failure-retains-original-input
  (let [forms [[:db/add 1 :text 42]]
        outcome (compile-stage2 ctx forms)]
    (is (= :needs-writer (:outcome outcome)))
    (is (identical? forms (:input outcome)))
    (is (identical? ctx (:ctx outcome)))
    ;; The protected Stage 1 literal proof keeps its earlier admission scope.
    (is (nil? (compile-stage2
                (assoc ctx :opts {:validate-data? false}) forms
                {:literal-shape :scalar-operations})))))

(deftest pure-scalar-coercions-use-the-existing-correction-worker
  (doseq [[vt value]
          [[:db.type/long (int 123)] [:db.type/long 1.25]
           [:db.type/ref 2N] [:db.type/ref (int 2)] [:db.type/double (float -0.0)]
           [:db.type/float 1.25] [:db.type/float Double/MAX_VALUE]
           [:db.type/bigint 123N] [:db.type/bigdec 123]
           [:db.type/string 42] [:db.type/string :scope/value]
           [:db.type/string false] [:db.type/boolean 0]
           [:db.type/instant 123] [:db.type/instant (java.time.Instant/ofEpochMilli 123)]
           [:db.type/uuid "550e8400-e29b-41d4-a716-446655440000"]]
          validate? [false true] e [1 [:identity "one"]]]
    (let [context (-> ctx (assoc-in [:schema :converted] {:db/aid 7 :db/valueType vt})
                      (assoc :opts {:validate-data? validate?}))
          forms [[:db/add e :converted value]]
          expected (try {:value (prepare/correct-scalar-value validate? vt value)}
                        (catch Exception _ {:rejected true}))
          p (compile-stage2 context forms)]
      (if (:rejected expected)
        (is (not (instance? TxPlan p)))
        (let [actual (nth (first (:entries p)) 2) wanted (:value expected)]
          (is (instance? TxPlan p))
          (is (identical? context (:ctx p)))
          (is (= (class wanted) (class actual)))
          (cond
            (instance? Double wanted)
            (is (= (Double/doubleToRawLongBits wanted) (Double/doubleToRawLongBits actual)))
            (instance? Float wanted)
            (is (= (Float/floatToRawIntBits wanted) (Float/floatToRawIntBits actual)))
            :else (is (= wanted actual))))))))

(deftest scalar-coercion-decline-preserves-host-and-interning-boundaries
  (let [effects (atom [])
        host (proxy [Object] []
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))
        tail (lazy-seq (swap! effects conj :tail) (list [:db/add 1 :text 42]))
        word (str "compiler-purity-" (random-uuid))
        context (-> ctx (assoc :opts {:validate-data? false})
                    (assoc-in [:schema :word] {:db/aid 7 :db/valueType :db.type/keyword})
                    (assoc-in [:schema :uuid] {:db/aid 8 :db/valueType :db.type/uuid}))]
    (is (nil? (clojure.lang.Keyword/find word)))
    (doseq [forms [tail [[:db/add 1 :text host]]
                   [[:db/add 1 :text 42] [:db/add 2 :text host]]
                   [[:db/add 1 :text 42] [:db/add 1 :identity "unsupported"]]
                   [[:db/add 1 :text 42] [:db/add 1 :text 43]]
                   [[:db/add 1 :word word]]
                   [[:db/add 1 :text 42] [:db/add 1 :uuid "invalid"]]
                   [[:db/add 1 :text (java.util.Date. 123)]]
                   [[:db/add 1 :text (byte-array [1 2])]]]]
      (is (nil? (compile-stage2 context forms))))
    (is (nil? (clojure.lang.Keyword/find word)))
    (is (empty? @effects))))

(deftest canonical-operation-operands-reuse-the-inert-source-buffer
  (let [forms [[:db/add 1 :value 42] [:db/add 2 :text "text"]]
        p (compile-stage2 ctx forms)]
    (is (= :scalar-operations (:strategy p)))
    (is (identical? forms (:entries p)))
    (is (identical? ctx (:ctx p)))))

(deftest singleton-layouts-retain-order-and-normalize-literal-ids
  (doseq [form [{:db/id 1 :value 42} {:value 42 :db/id 1}
                (assoc clojure.lang.PersistentHashMap/EMPTY :db/id (int 1) :value 42)]]
    (let [p (compile-stage2 ctx [form])]
      (is (= :scalar (:strategy p)))
      (is (= [[1 :value 42]] (:entries p)))
      (is (instance? Long (ffirst (:entries p))))))
  (doseq [form [{:db/id 1 :text 42} {:text 42 :db/id 1}]]
    (is (= :needs-writer (:outcome (compile-stage2 ctx [form]))))))

(deftest compact-schema-proof-does-not-hide-exclusion-properties
  (doseq [props [{:db/aid 3 :db/unique :db.unique/identity}
                 {:db/aid 3 :db/cardinality :db.cardinality/many}
                 {:db/aid 3 :db/tupleType :db.type/long}
                 {:db/aid 3 :db/valueType :db.type/long :db/fulltext true}
                 {:db/aid 3 :db/valueType :db.type/long :db/vector true}
                 {:db/aid 3 :db/valueType :db.type/long :db/embedding true}]]
    (is (nil? (compile-stage2 (assoc ctx :schema {:value props})
                                  [[:db/add 1 :value 42]]))))
  (is (nil? (compile-stage2 (assoc ctx :rschema {:db/attrTuples {:value {:pair 0}}})
                                [[:db/add 1 :value 42]]))))

(deftest stage2-fulltext-strings-retain-the-stage1-and-secondary-scope
  (let [props {:db/aid 3 :db/valueType :db.type/string :db/fulltext true}
        context (assoc ctx :schema {:text props})
        forms [[:db/add 1 :text "searchable"]]]
    (is (= :scalar-operations (:strategy (compile-stage2 context forms))))
    (is (nil? (compile-stage2 context forms {:literal-shape :scalar-operations})))
    (doseq [exclusion [{:db/vector true} {:db/embedding true}
                       {:db/cardinality :db.cardinality/many}
                       {:db/unique :db.unique/identity}
                       {:db.attr/preds [(constantly true)]}
                       {:db/valueType :db.type/long}]]
      (is (nil? (compile-stage2
                  (assoc context :schema {:text (merge props exclusion)}) forms))))
    (is (nil? (compile-stage2
                (assoc context :opts (assoc (:opts context) :auto-entity-time? true)) forms)))))

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
           (:outcome (compile-stage2 big-ctx [[:db/add 1 :big too-large]]))))))

(deftest wrapped-host-numbers-cannot-run-user-code-during-planning
  (let [effects (atom [])
        number (proxy [java.math.BigInteger] ["1"]
                 (bitLength [] 128)
                 (longValue [] (swap! effects conj :long) 1)
                 (toString [] (swap! effects conj :string) "1"))
        wrapped (clojure.lang.BigInt/fromBigInteger number)
        permissive (assoc ctx :opts {:validate-data? false})]
    (is (nil? (compile-stage2 permissive [[:db/add wrapped :value 1]])))
    (is (nil? (compile-stage2 permissive [[:db/add 1 :text wrapped]])))
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
          plan (compile-stage2 context [[:db/add 1 :value value]])]
      (is (identical? value (prepare/correct-scalar-value true vt value)))
      (is (= :scalar-operations (:strategy plan)))))
  ;; Calendar formatting can consult host callbacks; keep it ordered.
  (is (nil? (compile-stage2 (assoc ctx :opts {:validate-data? false})
                                [[:db/add 1 :text (java.util.Date. 1234)]]))))

(defn- mixed-preflight [context infos updates entity kind]
  ((compile/mixed-preflight-checker context infos updates) entity kind))

(deftest mixed-preflight-separates-shape-from-value-correction
  (let [effects (atom [])
        value (proxy [Object] []
                (toString [] (swap! effects conj :string) "opaque")
                (hashCode [] (swap! effects conj :hash) 1))
        infos (HashMap.) updates (HashSet.)]
    (is (true? (mixed-preflight
                 ctx infos updates {:db/id "new" :text value} :insert)))
    (is (true? (mixed-preflight
                 ctx infos updates [:db/add 1 :value value] :update)))
    (is (= #{[1 :value]} (set updates)))
    (is (false? (mixed-preflight
                  ctx infos updates [:db/add 1 :value 2] :update)))
    ;; A preflight shape/attribute proof grants no compiled value eligibility.
    (is (nil? (compile-stage2 ctx [[:db/add 1 :value 1] {:text value}])))
    (is (empty? @effects))))

(deftest mixed-preflight-unsupported-input-adds-no-scratch-or-host-effects
  (let [effects (atom [])
        number (proxy [Number] []
                 (longValue [] (swap! effects conj :long) -1)
                 (intValue [] -1) (floatValue [] -1.0) (doubleValue [] -1.0))
        key (proxy [Object] []
              (hashCode [] (swap! effects conj :hash) 7))
        input (proxy [Object clojure.lang.Seqable clojure.lang.IMeta] []
                (seq [] (swap! effects conj :seq) nil)
                (meta [] (swap! effects conj :meta) nil))]
    (doseq [[form kind] [[{:db/id number :text "new"} :insert]
                        [(array-map :text "new" key 1) :insert]
                        [(sorted-map :text "new") :insert]
                        [input :insert]
                        [[key 1 :value 1] :update]
                        [[:db/add 1 (keyword "") 1] :update]]]
      (let [infos (HashMap.) updates (HashSet.)]
        (is (= ::compile/unsupported-mixed-preflight
               (mixed-preflight ctx infos updates form kind)))
        (is (.isEmpty infos))
        (is (.isEmpty updates))))
    (is (empty? @effects))))

(deftest mixed-preflight-retains-context-and-attribute-exclusions
  (doseq [[form kind] [[{:db/id nil :text "new"} :insert]
                      [{:db/id 1 :text "existing"} :insert]
                      [{:db/id "empty"} :insert]
                      [{:identity "new"} :insert]
                      [{:missing 1} :insert]
                      [(with-meta {:text "new"} {:source true}) :insert]
                      [[:db/add 1 :reference 2] :update]
                      [[:db/add 1 :identity "new"] :update]
                      [[:db/add 1 :value nil] :update]
                      [[:db/retract 1 :value 1] :update]]]
    (is (false? (mixed-preflight ctx (HashMap.) (HashSet.) form kind))))
  (is (false? (mixed-preflight
                (assoc ctx :opts {:auto-entity-time? true})
                (HashMap.) (HashSet.) {:text "timed"} :insert))))

(deftest mixed-distinctness-never-converts-or-hashes-unsafe-keys
  (let [effects (atom [])
        number (proxy [Number] []
                 (longValue [] (swap! effects conj :long) 1)
                 (intValue [] 1) (floatValue [] 1.0) (doubleValue [] 1.0))
        attr (proxy [Object] [] (hashCode [] (swap! effects conj :hash) 1))
        updates (HashSet.)]
    (is (false? (compile/distinct-mixed-update? updates number :value)))
    (is (false? (compile/distinct-mixed-update? updates 1 attr)))
    (is (false? (compile/distinct-mixed-update? updates 1N :value)))
    (is (.isEmpty updates))
    (is (empty? @effects))
    (is (true? (compile/distinct-mixed-update? updates 1 :value)))
    (is (false? (compile/distinct-mixed-update? updates 1 :value)))
    (is (true? (compile/distinct-mixed-update? updates 1 :text)))))
