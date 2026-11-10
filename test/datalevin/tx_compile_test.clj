(ns datalevin.tx-compile-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.db.tx.compile :as compile]
            [datalevin.prepare :as prepare])
  (:import [datalevin.db.tx.plan BlindEntity MixedAttrInfo MixedInsert MixedOperands MixedUpdate TxPlan]
           [java.util HashMap]))

(def ctx
  (prepare/->PrepareCtx
    {:value {:db/aid 3 :db/valueType :db.type/long}
     :text {:db/aid 4 :db/valueType :db.type/string}
     :identity {:db/aid 5 :db/valueType :db.type/string :db/unique :db.unique/identity}
     :reference {:db/aid 6 :db/valueType :db.type/ref}}
    {} {:validate-data? true}))

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
                   [[:db/add 1 :reference :entity/two]]
                   [(with-meta [:db/add 1 :value 1] {:source true})]
                   [[:db/add 1 :identity "x"]]
                   [[:db.fn/call (fn [_] (swap! effects conj :callback))]]]]
      (is (nil? (compile/compile-tx ctx forms))))
    (is (empty? @effects))))

(deftest unified-selection-chooses-one-materialized-family
  (doseq [[forms shape strategy]
          [[[[:db/add 1 :value 1]] :scalar-operations :scalar-operations]
           [[{:db/id 1 :value 1}] :scalar :scalar]
           [[{:value 1}] :blind :blind]
           [[{:db/id "new" :value 1} [:db/add 1 :text "one"]] :mixed :mixed]]]
    (is (= shape (compile/literal-request-shape forms)))
    (is (= strategy (:strategy (compile/compile-tx ctx forms)))))
  (is (nil? (compile/literal-request-shape [{:db/id 1 :value 1} {:value 2}]))))

(deftest canonical-scalar-aliases-retain-symbolic-entity-operands
  (let [lookup [:identity "one"]
        forms [[:db/add lookup :value 1] [:db/add :entity/one :text "text"]
               {:db/id 2N :value 2}]
        plan (compile/compile-tx ctx forms)]
    (is (= :scalar-aliases (compile/literal-request-shape forms)))
    (is (= :scalar-aliases (:strategy plan)))
    (is (identical? ctx (:ctx plan)))
    (is (= [[lookup :value 1] [:entity/one :text "text"] [2 :value 2]]
           (:entries plan)))
    (is (identical? lookup (ffirst (:entries plan))))
    (is (nil? (:entity-reads plan)))
    ;; Alias resolution can make these the same E/A; compilation grants no
    ;; resolved distinctness proof. Runtime must decline before old-value reads.
    (is (instance? TxPlan (compile/compile-tx ctx
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
      (is (nil? (compile/compile-tx ctx forms))))
    (is (empty? @effects)))
  (is (nil? (compile/compile-tx (assoc ctx :opts {:auto-entity-time? true})
                               [[:db/add :entity/one :value 1]]))))

(deftest canonical-reference-values-share-scalar-operands-outside-stage1
  (doseq [[forms strategy]
          [[[[:db/add 1 :reference 2]] :scalar-operations]
           [[{:db/id 1 :reference Long/MAX_VALUE}] :scalar]
           [[[:db/add [:identity "one"] :reference 2]] :scalar-aliases]]]
    (let [p (compile/compile-tx ctx forms)]
      (is (= strategy (:strategy p)))
      (is (identical? ctx (:ctx p)))))
  (doseq [forms [[[:db/add 1 :reference 2]] [{:db/id 1 :reference 2}]]]
    (is (nil? (compile/compile-tx ctx forms
                {:literal-shape (compile/literal-scalar-shape forms)}))))
  (doseq [value [0 -1 2N "new" :entity/two [:identity "two"]]]
    (is (not (instance? TxPlan (compile/compile-tx ctx [[:db/add 1 :reference value]]))))
    (is (nil? (compile/compile-tx ctx [[:db/add :entity/one :reference value]]))))
  (let [reference-key (assoc-in ctx [:schema :reference-key]
                               {:db/aid 7 :db/valueType :db.type/ref :db/unique :db.unique/value})]
    (is (= :scalar-aliases
           (:strategy (compile/compile-tx reference-key
                        [[:db/add [:reference-key 2] :reference 3]]))))))

(deftest correction-failure-retains-original-input
  (let [forms [[:db/add 1 :text 42]]
        outcome (compile/compile-tx ctx forms)]
    (is (= :needs-writer (:outcome outcome)))
    (is (identical? forms (:input outcome)))
    (is (identical? ctx (:ctx outcome)))
    ;; The protected Stage 1 literal proof keeps its earlier admission scope.
    (is (nil? (compile/compile-tx
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
          p (compile/compile-tx context forms)]
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
      (is (nil? (compile/compile-tx context forms))))
    (is (nil? (clojure.lang.Keyword/find word)))
    (is (empty? @effects))))

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
