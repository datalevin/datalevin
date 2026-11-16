(ns datalevin.tx-template-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.db.tx.plan :as plan]
            [datalevin.db.tx.execute :as execute]
            [datalevin.db.tx.template :as template]
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
