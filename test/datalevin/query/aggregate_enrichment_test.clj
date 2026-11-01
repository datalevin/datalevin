(ns datalevin.query.aggregate-enrichment-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query.execute :as execute]
            [datalevin.query.execute.enrichment :as enrichment]))

(def schema
  {:customer/key {:db/valueType :db.type/long :db/unique :db.unique/identity}
   :customer/name {:db/valueType :db.type/string}
   :customer/address {:db/valueType :db.type/string}
   :customer/nation {:db/valueType :db.type/long}
   :nation/key {:db/valueType :db.type/long :db/unique :db.unique/identity}
   :nation/name {:db/valueType :db.type/string}
   :line/customer {:db/valueType :db.type/long}
   :line/amount {:db/valueType :db.type/long}})

(def query
  '[:find ?key ?name (sum ?amount) ?address ?nation
    :with ?line
    :order-by [2 :desc 0 :asc] :limit 2
    :where
    [?customer :customer/key ?key]
    [?customer :customer/name ?name]
    [?customer :customer/address ?address]
    [?customer :customer/nation ?nk]
    [?n :nation/key ?nk]
    [?n :nation/name ?nation]
    [?line :line/customer ?key]
    [?line :line/amount ?amount]])

(def facts
  [{:db/id 1 :customer/key 1 :customer/name "A" :customer/address "a" :customer/nation 10}
   {:db/id 2 :customer/key 2 :customer/name "B" :customer/address "b" :customer/nation 10}
   {:db/id 3 :customer/key 3 :customer/name "C" :customer/address "c" :customer/nation 20}
   {:db/id 4 :customer/key 4 :customer/address "missing-name" :customer/nation 10}
   {:db/id 5 :customer/key 5 :customer/name "missing-nation" :customer/address "e" :customer/nation 30}
   {:db/id 10 :nation/key 10 :nation/name "FRANCE"}
   {:db/id 20 :nation/key 20 :nation/name "GERMANY"}
   {:db/id 30 :nation/key 30}
   {:line/customer 1 :line/amount 60} {:line/customer 1 :line/amount 60}
   {:line/customer 2 :line/amount 100} {:line/customer 3 :line/amount 80}
   {:line/customer 4 :line/amount 1000} {:line/customer 5 :line/amount 2000}])

(defn- fixture
  ([f] (fixture schema f))
  ([schema f]
   (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
     (d/transact! conn facts)
     (f conn))))

(defn- reference [query database & inputs]
  (binding [enrichment/*aggregate-enrichment?* false]
    (apply d/q query database inputs)))

(deftest schema-dependencies-defer-payload-and-retain-existence-filters
  (fixture
    (fn [conn]
      (let [actual (d/q query @conn)
            explain (d/explain {:run? true} query @conn)
            decision (:aggregate-enrichment explain)]
        (is (= [[1 "A" 120 "a" "FRANCE"] [2 "B" 100 "b" "FRANCE"]] actual))
        (is (= (reference query @conn) actual))
        (is (= #{'?name '?address '?nation} (set (:deferred-vars decision))))
        (is (= ['?key] (:group-vars decision)))
        (is (= 2 (:selected-count decision)))
        (is (= 3 (count (:presence-clauses decision))))
        (is (true? (get-in decision [:proof :schema-functional-dependencies])))
        (testing "the main scan retains presence but no descriptive payload"
          (is (not-any? #{'?name '?address '?nation}
                        (tree-seq coll? seq (:query-graph explain)))))
        (testing "presence placeholders do not occupy native tuple columns"
          (let [planned (execute/plan-context* (parser/parse-query query) [@conn])
                steps (for [[_ components] (:plan planned)
                            component components node component step (:steps node)] step)
                cols (mapcat #(tree-seq coll? seq (:cols %)) steps)]
            (is (not-any? #(and (symbol? %) (.startsWith (name %) "?placeholder")) cols))))))))

(deftest missing-attributes-filter-before-the-result-window
  (doseq [[entity attr] [[1 :customer/name] [1 :customer/address] [10 :nation/name]]]
    (fixture
      (fn [conn]
        (d/transact! conn [[:db/retract entity attr]])
        (is (= (reference query @conn) (d/q query @conn)))
        (is (some? (:aggregate-enrichment (d/explain {} query @conn)))))))
  (fixture
    (fn [conn]
      (doseq [entity [1 2 3]] (d/transact! conn [[:db/retract entity :customer/name]]))
      (is (empty? (d/q query @conn))))))

(deftest uniqueness-is-a-schema-proof-and-not-an-observation
  (fixture (update schema :customer/key dissoc :db/unique)
    (fn [conn]
      (is (nil? (:aggregate-enrichment (d/explain {:run? true} query @conn))))
      (d/transact! conn [{:customer/key 1 :customer/name "other A"
                         :customer/address "other a" :customer/nation 20}])
      (is (= (reference query @conn) (d/q query @conn)))))
  (fixture
    (fn [conn]
      (is (some? (:aggregate-enrichment (d/explain {} query @conn))))
      (d/update-schema conn {:customer/key {:db/unique :db/retract}})
      (is (nil? (:aggregate-enrichment (d/explain {} query @conn)))))))

(deftest cardinality-many-and-filtered-or-ordered-fields-remain-in-the-input
  (fixture (assoc-in schema [:customer/name :db/cardinality] :db.cardinality/many)
    (fn [conn]
      (d/transact! conn [[:db/add 1 :customer/name "A2"]])
      (let [decision (:aggregate-enrichment (d/explain {} query @conn))]
        (is (not (some #{'?name} (:deferred-vars decision))))
        (is (= (reference query @conn) (d/q query @conn))))))
  (fixture
    (fn [conn]
      (doseq [q [(conj query '[(not= ?name "B")])
                 (-> query (assoc 9 '[1 :asc 0 :asc]))]]
        (is (= (reference q @conn) (d/q q @conn)))
        (is (not (some #{'?name}
                       (:deferred-vars (:aggregate-enrichment (d/explain {} q @conn))))))))))

(deftest offset-having-prepared-and-return-map-shapes-are-preserved
  (fixture
    (fn [conn]
      (let [q (into query '[:offset 1 :having [(> (sum ?amount) 70)]])]
        (is (= (reference q @conn) (d/q q @conn)))
        (is (= [[2 "B" 100 "b" "FRANCE"] [3 "C" 80 "c" "GERMANY"]]
               (d/q q @conn)))
        (is (= (reference q @conn) ((d/prepare-q @conn q) []))))
      (let [q (into query '[:keys key name total address nation])]
        (is (= (reference q @conn) (d/q q @conn)))))))

(deftest retained-entity-ids-prove-dependencies-without-unique-business-keys
  (fixture (update schema :customer/key dissoc :db/unique)
    (fn [conn]
      (let [q '[:find ?customer ?name (sum ?amount) :with ?line
                :order-by [2 :desc 0 :asc] :limit 2
                :where [?customer :customer/key ?key]
                [?customer :customer/name ?name]
                [?line :line/customer ?key] [?line :line/amount ?amount]]]
        (is (= (reference q @conn) (d/q q @conn)))
        (is (= ['?name] (:deferred-vars (:aggregate-enrichment (d/explain {} q @conn)))))))))

(deftest aggregate-column-ordering-and-named-sources-are-preserved
  (fixture
    (fn [conn]
      (let [q '[:find ?key ?name (sum ?amount) (max ?amount) :with ?line
                :order-by [3 :desc 0 :asc] :limit 2
                :where [?customer :customer/key ?key] [?customer :customer/name ?name]
                [?line :line/customer ?key] [?line :line/amount ?amount]]]
        (is (= (reference q @conn) (d/q q @conn))))
      (let [q '[:find ?key ?name (sum ?amount) :with ?line
                :in $facts $dimensions :order-by [2 :desc 0 :asc] :limit 2
                :where [$dimensions ?customer :customer/key ?key]
                [$dimensions ?customer :customer/name ?name]
                [$facts ?line :line/customer ?key] [$facts ?line :line/amount ?amount]]]
        (is (= (reference q @conn @conn) (d/q q @conn @conn)))))))
