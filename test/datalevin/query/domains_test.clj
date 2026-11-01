(ns datalevin.query.domains-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.walk :as walk]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query :as q]
            [datalevin.query.execute :as execute]
            [datalevin.query.optimizer.domains :as domains]
            [datalevin.query.optimizer.selective :as selective]
            [datalevin.query.plan :as plan]
            [datalevin.query.resolve :as resolve])
  (:import [datalevin.utl LRUCache]))

(def schema
  (assoc (zipmap [:customer/key :customer/nation :fact/customer :fact/amount
                 :supplier/key :supplier/nation :nation/key :nation/region
                 :region/key]
                (repeat {:db/valueType :db.type/long}))
         :region/name {:db/valueType :db.type/string}))

(def query
  '[:find ?key ?alias (sum ?amount) :with ?fact
    :where
    [?customer :customer/key ?key]
    [?customer :customer/nation ?nk]
    [?fact :fact/customer ?key]
    [?fact :fact/amount ?amount]
    [?supplier :supplier/key ?key]
    [?supplier :supplier/nation ?nk]
    [?supplier :supplier/nation ?alias]
    [?nation :nation/key ?alias]
    [?nation :nation/region ?rk]
    [?region :region/key ?rk]
    [?region :region/name "ASIA"]])

(def facts
  [{:db/id 1 :region/key 10 :region/name "ASIA"}
   {:db/id 2 :region/key 20 :region/name "EUROPE"}
   {:nation/key 1 :nation/region 10}
   {:nation/key 2 :nation/region 20}
   {:nation/key 3 :nation/region 10}
   {:customer/key 100 :customer/nation 1}
   {:customer/key 200 :customer/nation 2}
   {:customer/key 300 :customer/nation 3}
   {:supplier/key 100 :supplier/nation 1}
   {:supplier/key 200 :supplier/nation 2}
   {:supplier/key 300 :supplier/nation 3}
   {:fact/customer 100 :fact/amount 7}
   {:fact/customer 100 :fact/amount 7}
   {:fact/customer 200 :fact/amount 9}
   {:fact/customer 300 :fact/amount 11}])

(defn- context
  "A costed late-dimension plan, independent of fixture size and sampling.
  Actual query results below are executed by the ordinary engine."
  [database query]
  (let [cols ['?customer #{:customer/key '?key} #{:customer/nation '?nk}]
        columns [(conj cols '?fact)
                 (into cols ['?fact '?supplier #{:supplier/nation '?alias}])
                 (into cols ['?fact '?supplier '?alias '?nation '?rk])
                 (into cols ['?fact '?supplier '?alias '?nation '?rk '?region])]
        trace (mapv (fn [size cost cols]
                      {:size size :cost cost :steps [{:cols cols}]})
                    [10000 40000 40000 10000 2000]
                    [100000 2000000 3000000 4000000 5000000]
                    (cons cols columns))]
    (-> (plan/make-context (parser/parse-query query) false)
        (resolve/resolve-ins [database])
        (assoc :plan {'$ [trace]}))))

(defn- restriction [context]
  (when-let [candidate (first (domains/candidates context))]
    (domains/materialize candidate)))

(defn- restricted-query [context domain]
  (into (vec (take-while #(not= :where %) query))
        (cons :where (get-in (domains/restrict-context context domain)
                            [:parsed-q :qorig-where]))))

(deftest equality-proof-preserves-projection-and-aggregate-multiplicity
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn facts)
    (binding [q/*cache?* false q/*plan-cache* (LRUCache. 32)]
      (let [ctx (context @conn query)
            domain (restriction ctx)
            restricted (restricted-query ctx domain)]
        (is (= [1 3] (:values domain)))
        (is (= '[?alias ?nk] (:equality-vars domain)))
        (is (= '[?customer :customer/nation ?nk] (get-in domain [:target :form])))
        (is (domains/profitable? domain))
        (is (not (domains/profitable? (assoc domain :values [1 2 3]))))
        (is (= (get-in ctx [:parsed-q :qorig-where])
               (vec (butlast (get-in (domains/restrict-context ctx domain)
                                     [:parsed-q :qorig-where])))))
        (dotimes [_ 2]
          (is (= #{[100 1 14] [300 3 11]}
                 (set (d/q query @conn)) (set (d/q restricted @conn))))))
      (testing "domains are rebuilt after writes, including cached plans"
        (d/transact! conn [[:db/add 1 :region/name "OTHER"]
                          [:db/add 2 :region/name "ASIA"]])
        (let [ctx (context @conn query) domain (restriction ctx)]
          (is (= [2] (:values domain)))
          (is (= #{[200 2 9]} (set (d/q (restricted-query ctx domain) @conn))
                 (set (d/q query @conn)))))))))

(deftest many-valued-attributes-do-not-prove-alias-equality
  (with-open [conn (d/create-conn nil
                    (assoc-in schema [:supplier/nation :db/cardinality]
                              :db.cardinality/many)
                    {:background-sampling? false})]
    (d/transact! conn (conj facts {:supplier/key 200 :supplier/nation [1 2]}))
    (is (empty? (domains/candidates (context @conn query))))
    (is (contains? (set (d/q query @conn)) [200 1 9]))))

(deftest complete-domains-only
  (doseq [n [0 128 129]]
    (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
      (d/transact! conn
        (concat [{:region/key 10 :region/name "ASIA"}]
                (for [k (range n)] {:nation/key k :nation/region 10})))
      (let [domain (restriction (context @conn query))]
        (if (<= n 128)
          (is (= (vec (range n)) (:values domain)))
          (is (nil? domain))))))
  (testing "an oversized seed is rejected even if its final keys repeat"
    (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
      (d/transact! conn
        (concat (repeat 129 {:region/key 10 :region/name "ASIA"})
                [{:nation/key 1 :nation/region 10}]))
      (is (nil? (restriction (context @conn query)))))))

(deftest false-is-a-domain-value
  (let [s (reduce #(assoc-in %1 [%2 :db/valueType] :db.type/boolean)
                  schema [:customer/nation :supplier/nation :nation/key])]
    (with-open [conn (d/create-conn nil s {:background-sampling? false})]
      (d/transact! conn [{:region/key 10 :region/name "ASIA"}
                        {:nation/key false :nation/region 10}
                        {:customer/key 100 :customer/nation false}
                        {:supplier/key 100 :supplier/nation false}
                        {:fact/customer 100 :fact/amount 7}])
      (let [ctx (context @conn query) domain (restriction ctx)]
        (is (= [false] (:values domain)))
        (is (= #{[100 false 7]} (set (d/q (restricted-query ctx domain) @conn))))))
  (testing "a false constant can also seed the producer"
    (with-open [conn (d/create-conn nil
                      (assoc-in schema [:region/name :db/valueType] :db.type/boolean)
                      {:background-sampling? false})]
      (d/transact! conn [{:region/key 10 :region/name false}
                        {:nation/key 1 :nation/region 10}])
      (is (= [1] (:values (restriction
                           (context @conn (walk/postwalk-replace
                                            {"ASIA" false} query))))))))))

(deftest proof-stays-inside-positive-native-source-patterns
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn facts)
    (let [ctx (context @conn query)
          forms (get-in ctx [:parsed-q :qorig-where])
          replaced (fn [forms]
                     (-> ctx
                         (assoc-in [:parsed-q :qorig-where] forms)
                         (assoc-in [:parsed-q :qwhere] (parser/parse-where forms))))]
      (testing "optional or negated anchors do not imply a positive domain"
        (doseq [anchor [(list 'not (last forms))
                        (list 'or (last forms) '[?region :region/name "EUROPE"])]]
          (is (empty? (domains/candidates
                        (replaced (conj (pop forms) anchor)))))))
      (testing "different source symbols do not share dimension paths"
        (let [forms (mapv (fn [form]
                            (if (#{'?region '?nation} (first form))
                              (into ['$other] form) form)) forms)]
          (is (empty? (domains/candidates
                        (assoc-in (replaced forms) [:sources '$other] @conn))))))
      (testing "giant literal seeds cannot trigger unbounded collision scans"
        (is (empty? (domains/candidates
                      (replaced (conj (pop forms)
                                      ['?region :region/name
                                       (apply str (repeat 1000 "A"))]))))))
      (testing "pending transaction overlays do not read stale store domains"
        (let [view (:db-after (d/tx-data->simulated-report
                               @conn [[:db/add 1 :region/name "OTHER"]]))]
          (is (empty? (domains/candidates (context view query))))))
      (testing "schema changes invalidate the cardinality-one proof"
        (is (seq (domains/candidates ctx)))
        (d/update-schema conn {:supplier/nation {:db/cardinality :db.cardinality/many}})
        (is (empty? (domains/candidates (context @conn query))))))))

(deftest producer-budget-bounds-the-whole-path
  (with-open [conn (d/create-conn nil
                    (assoc schema :chain/next {:db/valueType :db.type/ref})
                    {:background-sampling? false})]
    (d/transact! conn (for [e (range 1 129)]
                       {:db/id e :region/name "ASIA" :chain/next e}))
    (let [candidate {:anchor {:source @conn :attr :region/name :value "ASIA"}}
          step {:direction :eav :attr :chain/next}]
      (is (= 512 (:producer-rows
                   (domains/materialize (assoc candidate :path (repeat 3 step))))))
      (is (nil? (domains/materialize (assoc candidate :path (repeat 4 step))))))))

(deftest admitted-domain-plan-is-reused
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn facts)
    (binding [q/*cache?* false q/*plan-cache* (LRUCache. 32)]
      (let [sink (volatile! {})
            prepared (binding [plan/*explain* sink plan/*start-time* (System/nanoTime)]
                       (#'selective/propagate-bounded-domain (context @conn query)))
            planned (#'execute/planning prepared)
            forms (get-in prepared [:parsed-q :qorig-where])
            rewritten (into (vec (take-while #(not= :where %) query))
                            (cons :where forms))]
        (is (true? (:selected? (first (:bounded-domain-decisions @sink)))))
        (is (identical? (:graph prepared) (:graph planned)))
        (is (identical? (:plan prepared) (:plan planned)))
        (is (= #{[100 1 14] [300 3 11]} (set (d/q rewritten @conn))))))))

(deftest reverse-hops-require-a-small-population-or-unique-key
  (doseq [unique? [false true]]
    (with-open [conn (d/create-conn nil
                      (cond-> schema
                        unique? (assoc-in [:nation/region :db/unique] :db.unique/value))
                      {:background-sampling? false})]
      (d/transact! conn
        (cons {:region/key 10 :region/name "ASIA"}
              (for [k (range 513)] {:nation/key k :nation/region (+ 10 k)})))
      (let [ctx (context @conn query)]
        (if unique?
          (is (= [0] (:values (restriction ctx))))
          (is (empty? (domains/candidates ctx))))))))
