(ns datalevin.tx-template-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.db.tx.plan :as plan]
            [datalevin.db.tx.execute :as execute]
            [datalevin.db.tx.template :as template]
            [datalevin.datom :as datom]
            [datalevin.prepare :as prepare]))

(def context
  (prepare/->PrepareCtx
    {:value {:db/aid 1 :db/valueType :db.type/long}
     :name {:db/aid 2 :db/valueType :db.type/string}}
    {} {:validate-data? false}))

(deftest compiled-operations-compose-with-fresh-parameter-bindings
  (let [op (template/prepare-operation context
             [:db/add (template/parameter :e) :value (template/parameter :v)])
        a (template/prepare-template context [op] nil)
        b (template/prepare-template context [op [:db/add 2 :name "two"]] nil)
        one (template/bind-template a (template/input-rows a {:e 1 :v (int 7)}))
        two (template/bind-template a (template/input-rows a {:e 2 :v 8}))]
    (is (identical? op (first (:operations a))))
    (is (identical? op (first (:operations b))))
    (is (= [[1 :value 7]] (:entries one)))
    (is (= [[2 :value 8]] (:entries two)))
    (is (instance? Long (nth (first (:entries one)) 2)))
    (is (= [[1 :value 7]] (:entries one)))))

(deftest batch-length-is-independent-of-the-compiled-program
  (let [t (template/prepare-template context
            [[:db/add (template/parameter :e) :value (template/parameter :v)]] {:batch? true})]
    (doseq [n [1 32 1024]]
      (let [rows (mapv #(hash-map :e % :v %) (range 1 (inc n)))
            bound (template/bind-template t (template/input-rows t rows))]
        (is (= n (count (:entries bound))))
        (is (= :scalar (:strategy bound)))))
    (is (= :scalar-sequence (:strategy (template/bind-template t [{:e 1 :v 1} {:e 1 :v 2}]))))))

(deftest input-substitution-preserves-literals-and-source-annotations
  (let [literal (Object.)
        source (with-meta [:db/add [:key (template/parameter :key)] :value
                           {:nested [(template/parameter :value) literal]}]
                          {:source :kept})
        t (template/prepare-template context [source] nil)
        forms (template/materialize t [{:key "one" :value 42}])
        input (template/request-input t [{:key "one" :value 42}])]
    (is (= [:key "one"] (nth (first forms) 1)))
    (is (= 42 (get-in forms [0 3 :nested 0])))
    (is (identical? literal (get-in forms [0 3 :nested 1])))
    (is (= {:source :kept} (meta (first forms))))
    (is (not (realized? (.-forms ^datalevin.db.tx.plan.PreparedTxInput input))))
    (is (= forms (plan/request-forms input)))
    (is (identical? (plan/request-forms input) (plan/request-forms input)))))

(deftest definition-metadata-is-sealed-literal-data
  (let [annotations {:source :kept :details {:values [nil false 7] :tags #{:a :b}
                                          :history '(1 2 3)}}
        nested (with-meta {:value (template/parameter :v)} {:nested :kept})
        source (with-meta [:db/add 1 :value nested] annotations)
        program (template/prepare-template context [source] nil)
        forms (template/materialize program [{:v 42}])]
    (is (identical? annotations (meta (first forms))))
    (is (= {:nested :kept} (meta (get-in forms [0 3]))))
    (is (= 42 (get-in forms [0 3 :value])))))

(deftest metadata-parameters-are-rejected-at-every-literal-boundary
  (let [marker (template/parameter :metadata)
        bad {:nested [marker]}
        forms [(with-meta [:db/add 1 :value 7] bad)
               [:db/add 1 :value (with-meta {:nested 7} bad)]
               [:db/add 1 :value (with-meta [7] bad)]
               [:db/add 1 :value (with-meta #{7} bad)]
               [:db/add 1 :value (with-meta '(7) bad)]
               [:db/add 1 :value (with-meta 'literal bad)]
               [:db/add 1 :value {(with-meta 'key bad) 7}]
               (with-meta [:db/add 1 :value 7]
                 {:nested (with-meta {:value 1} bad)})]]
    (doseq [form forms]
      (let [failure (try (template/prepare-operation context form)
                         nil (catch clojure.lang.ExceptionInfo e e))]
        (is (= :prepared/metadata (:error (ex-data failure))))))
    (is (thrown? clojure.lang.ExceptionInfo
          (template/prepare-template context
            (with-meta [[:db/add 1 :value 7]] bad) nil)))))

(deftest metadata-validation-neither-demands-lazy-data-nor-calls-host-methods
  (let [effects (atom [])
        host (proxy [Object clojure.lang.IMeta] []
               (meta [] (swap! effects conj :meta) {})
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))
        lazy-value (lazy-seq (swap! effects conj :realized) (list 7))]
    (doseq [value [host lazy-value]
            annotations [{:value value} {:nested [{:value value}]}]]
      (let [failure (try (template/prepare-operation context
                          (with-meta [:db/add 1 :value 7] annotations))
                         nil (catch clojure.lang.ExceptionInfo e e))]
        (is (= :prepared/metadata (:error (ex-data failure))))))
    (is (empty? @effects))))

(deftest compatible-binding-does-not-revisit-schema-or-host-values
  (let [reads (atom 0) effects (atom [])
        schema (fn [a] (swap! reads inc) ((:schema context) a))
        ctx (assoc context :schema schema)
        t (template/prepare-template ctx
            [{:name (template/parameter :name) :value (template/parameter :value)}] {:batch? true})
        before @reads
        host (proxy [Object] []
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))]
    (is (= :blind (:strategy (template/bind-template t [{:name "a" :value 1}]))))
    (is (nil? (template/bind-template t [{:name host :value 2}])))
    (is (= before @reads))
    (is (empty? @effects))))

(deftest dependent-operations-read-each-initial-value-once
  (let [forms (mapv #(vector :db/add 1 :value %) (range 1 501))
        t (template/prepare-template context forms nil)
        bound (template/bind-template t [{}])
        reads (atom [])
        report (execute/execute-scalar-sequence
                 {:db-after {:max-tx 0}} (:entries bound) 999
                 (fn [_ e a] (swap! reads conj [e a]) 0))]
    (is (= :scalar-sequence (:strategy bound)))
    (is (= [[1 :value]] @reads))
    (is (= 1000 (count (:tx-data report))))
    (is (= 500 (:v (last (:tx-data report)))))
    (is (= 999 (:max-tx (:db-after report))))))

(deftest fixed-entity-read-layouts-are-reused-between-bindings
  (let [attrs (mapv #(keyword (str "a" %)) (range 8))
        ctx (assoc context :schema (into {} (map-indexed
                                             (fn [i a] [a {:db/aid (inc i) :db/valueType :db.type/long}]) attrs)))
        t (template/prepare-template ctx (mapv #(vector :db/add 1 % (template/parameter :v)) attrs) nil)
        a (template/bind-template t [{:v 1}]) b (template/bind-template t [{:v 2}])]
    (is (some? (:entity-reads a)))
    (is (identical? (:entity-reads a) (:entity-reads b)))
    (is (= (set attrs) (set (map second (:entries a)))))))

(deftest mixed-templates-reuse-metadata-and-own-their-operands
  (let [reads (atom [])
        schema {:value {:db/aid 1 :db/valueType :db.type/long}
                :friend {:db/aid 2 :db/valueType :db.type/ref}
                :left {:db/aid 3 :db/valueType :db.type/long}
                :right {:db/aid 4 :db/valueType :db.type/long}
                :pair {:db/aid 5 :db/tupleAttrs [:left :right]}}
        ctx (prepare/->PrepareCtx #(do (swap! reads conj %) (schema %))
                                  {:db/attrTuples {:left {:pair 0} :right {:pair 1}}}
                                  {:validate-data? false})
        insert (template/prepare-operation ctx
                 (array-map :db/id (template/parameter :id) :friend (template/parameter :owner)
                            :left (template/parameter :left) :right 2))
        t (template/prepare-template ctx [[:db/add 1 :value (template/parameter :v)] insert] nil)
        before @reads
        a (template/bind-template t [{:id -1 :owner -1 :left (int 3) :v (int 4)}])
        b (template/bind-template t [{:id -2 :owner -2 :left 5 :v 6}])
        ^datalevin.db.tx.plan.MixedOperands x (:entries a)
        ^datalevin.db.tx.plan.MixedOperands y (:entries b)]
    (is (= :mixed (:strategy t)))
    (is (identical? insert (second (:operations t))))
    (is (= :mixed (:strategy a)))
    (is (= before @reads))
    (is (= 1 (.-required-eid x)))
    (is (not (identical? (.-entries x) (.-entries y))))
    (is (not (identical? (.-attrs ^datalevin.db.tx.plan.MixedInsert (.get ^java.util.ArrayList (.-entries x) 1))
                         (.-attrs ^datalevin.db.tx.plan.MixedInsert (.get ^java.util.ArrayList (.-entries y) 1)))))
    (is (= 4 (.-value ^datalevin.db.tx.plan.MixedUpdate (.get ^java.util.ArrayList (.-entries x) 0))))))

(deftest mixed-guards-decline-forward-and-lazy-references-without-effects
  (let [effects (atom [])
        ctx (assoc-in context [:schema :friend] {:db/aid 3 :db/valueType :db.type/ref})
        t (template/prepare-template ctx
            [[:db/add 1 :value (template/parameter :v)]
             {:db/id (template/parameter :id) :friend (template/parameter :ref)}] nil)
        reference (lazy-seq (swap! effects conj :forced) (list 1))]
    (is (nil? (template/bind-template t [{:v 1 :id -1 :ref -2}])))
    (is (nil? (template/bind-template t [{:v 1 :id -1 :ref reference}])))
    (is (empty? @effects))))

(deftest ordered-scalar-operations-reuse-layouts-and-read-current-datoms-once
  (let [reads (atom []) metadata-reads (atom 0)
        ctx (assoc context :schema #(do (swap! metadata-reads inc) ((:schema context) %)))
        cas (template/prepare-operation ctx [:db/cas 1 :value (template/parameter :old)
                                             (template/parameter :new)])
        t (template/prepare-template ctx [cas [:db/retract 1 :value]
                                         [:db/add 1 :value 9] [:db.fn/retractAttribute 1 :value]
                                         [:db.fn/cas 1 :value nil 10]] nil)
        before @metadata-reads
        a (template/bind-template t [{:old 0 :new 1}])
        b (template/bind-template t [{:old 0 :new 2}])
        ^datalevin.db.tx.plan.ScalarSequence x (:entries a)
        ^datalevin.db.tx.plan.ScalarSequence y (:entries b)
        report (execute/execute-scalar-program {:db-after {:max-tx 0}} x 7
                 (fn [_ e attr]
                   (swap! reads conj [e attr])
                   (datom/datom e attr 0 2)))]
    (is (= :scalar-program (:strategy a)))
    (is (identical? cas (first (:operations t))))
    (is (identical? (.-slots x) (.-slots y)))
    (is (not (identical? (.-entries x) (.-entries y))))
    (is (= before @metadata-reads))
    (is (= [[1 :value]] @reads))
    (is (= [0 1 1 9 9 10] (mapv :v (:tx-data report))))))

(deftest ordered-binding-declines-host-matches-without-invoking-them
  (let [effects (atom []) host (proxy [Object] []
                                (hashCode [] (swap! effects conj :hash) 1)
                                (equals [_] (swap! effects conj :equals) false))
        t (template/prepare-template context [[:db/cas (template/parameter :e) :value
                                               (template/parameter :old) 1]] nil)]
    (is (nil? (template/bind-template t [{:e 1 :old host}])))
    (is (nil? (template/bind-template t [{:e host :old 0}])))
    (is (empty? @effects))))

(deftest ordered-reference-bindings-reuse-proof-and-fixed-slots
  (let [reads (atom 0)
        schema (assoc (:schema context) :friend {:db/aid 3 :db/valueType :db.type/ref})
        ctx (assoc context :schema #(do (swap! reads inc) (schema %)))
        op (template/prepare-operation ctx [:db/cas 1 :friend (template/parameter :old)
                                            (template/parameter :new)])
        t (template/prepare-template ctx [op [:db/retract 1 :friend]] nil)
        before @reads
        a (template/bind-template t [{:old 2 :new 3}])
        b (template/bind-template t [{:old 3 :new 4}])
        ^datalevin.db.tx.plan.ScalarSequence x (:entries a)
        ^datalevin.db.tx.plan.ScalarSequence y (:entries b)]
    (is (identical? op (first (:operations t))))
    (is (= :scalar-program (:strategy a)))
    (is (= before @reads))
    (is (identical? (.-slots x) (.-slots y)))
    (is (nil? (:resolve-ids? a)))
    (is (not (identical? (.-entries x) (.-entries y))))))

(deftest symbolic-ids-remain-unresolved-until-the-writer
  (let [ctx (assoc-in context [:schema :key]
                       {:db/aid 3 :db/valueType :db.type/string :db/unique :db.unique/identity})
        t (template/prepare-template ctx [[:db/cas (template/parameter :e) :value 0 1]] nil)
        lookup [:key "one"] bound (template/bind-template t [{:e lookup}])
        ^datalevin.db.tx.plan.ScalarSequence operands (:entries bound)]
    (is (= :scalar-program (:strategy bound)))
    (is (:resolve-ids? bound))
    (is (:deferred-entity-resolution? bound))
    (is (identical? lookup (ffirst (.-entries operands))))
    (is (nil? (.-slots operands)))))

(deftest ordered-effect-instructions-retain-attribute-proofs-and-raw-operands
  (let [reads (atom 0)
        schema (assoc (:schema context) :code {:db/aid 3 :db/valueType :db.type/long :db/unique :db.unique/value})
        ctx (assoc context :schema #(do (swap! reads inc) (schema %)))
        source (with-meta [:db/cas 1 :code (template/parameter :old) (template/parameter :new)] {:source :kept})
        op (template/prepare-operation ctx source)
        t (template/prepare-template ctx [op] nil) before @reads
        a (template/bind-template t [{:old 10 :new (int 11)}])
        b (template/bind-template t [{:old 11 :new 12}])
        ^datalevin.db.tx.plan.BoundOperation instruction (first (:entries a))]
    (is (= :ordered-program (:strategy a)))
    (is (identical? op (first (:operations t))))
    (is (= before @reads))
    (is (identical? (get-in op [:ordered-definition :info]) (.-info instruction)))
    (is (= [:db/cas 1 :code 10 11] (plan/operation-form instruction)))
    (is (= {:source :kept} (meta instruction)))
    (is (instance? Integer (nth instruction 4)))
    (is (not (identical? instruction (first (:entries b)))))))
