(ns datalevin.query.subquery-driver-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]))

(def grouped
  '[:find ?ok (sum ?q) :with ?l
    :having [(> (sum ?q) 300)]
    :where [?l :line/order ?ok] [?l :line/quantity ?q]])

(def query
  {:find ['?name '?ok '?total '?price]
   :order-by [[3 :desc 1 :asc]]
   :limit 100
   :where [[(list 'q grouped '$) '[[?ok ?total]]]
           '[?o :order/key ?ok]
           '[?o :order/customer ?ck]
           '[?o :order/price ?price]
           '[?c :customer/key ?ck]
           '[?c :customer/name ?name]]})

(def expected
  [["C12" 2 320.0 999.0] ["C2" 2 320.0 2.0] ["C1" 1 303.0 1.0]])

(defn- fixture [f]
  (with-open [conn (d/create-conn nil
                    {:line/order {:db/valueType :db.type/long}
                     :line/quantity {:db/valueType :db.type/double}
                     :order/key {:db/valueType :db.type/long}
                     :order/customer {:db/valueType :db.type/long}
                     :order/price {:db/valueType :db.type/double}
                     :customer/key {:db/valueType :db.type/long}
                     :customer/name {:db/valueType :db.type/string}}
                    {:background-sampling? false})]
    (d/transact! conn
      (concat
        (for [c (range 100)] {:customer/key c :customer/name (str "C" c)})
        (for [o (range 1 1001)]
          {:order/key o :order/customer (if (= o 4) 999 (mod o 100))
           :order/price (double o)})
        ;; Non-unique outer keys must retain every matching dimension row.
        [{:order/key 2 :order/customer 12 :order/price 999.0}]
        (for [o (range 1 1002)
              q (get {1 [101.0 101.0 101.0] 2 [80.0 80.0 80.0 80.0]
                      3 [150.0 150.0] 4 [200.0 200.0] 1001 [200.0 200.0]}
                     o [10.0])]
          {:line/order o :line/quantity q})))
    (f conn)))

(deftest grouped-result-drives-indexed-outer-lookups
  (fixture
    (fn [conn]
      (let [explain (d/explain {:run? true} query @conn)]
        (testing "HAVING, identical line witnesses and missing dimensions survive"
          (is (= (set expected) (set (:result explain))))
          (is (= expected (d/q query @conn))))
        (testing "four qualifying groups bind the outer order lookups"
          (is (= [4] (mapv :rows (:pre-materialized-subqueries explain))))
          (is (not-any? #{'[?o :order/key ?ok]} (:opt-clauses explain)))
          (is (empty? (:late-clauses explain)))))
      (testing "existing input bindings intersect the complete grouped relation"
        (is (= (subvec expected 0 2)
               (d/q (assoc query :in '[$ [?ok ...]]) @conn [2])))
        (is (empty? (d/q (assoc query :in '[$ [?ok ...]]) @conn [9999]))))
      (testing "prepared queries use the same execution path"
        (is (= expected ((d/prepare-q @conn query) []))))
      (testing "execution-local bindings are refreshed after transactions"
        (d/transact! conn [{:line/order 5 :line/quantity 400.0}])
        (is (= [(first expected) ["C5" 5 410.0 5.0]
                (second expected) (last expected)]
               (d/q query @conn)))))))

(deftest empty-grouped-result-short-circuits-outer-scans
  (fixture
    (fn [conn]
      (let [empty-grouped (assoc grouped 6 '[(> (sum ?q) 100000)])
            query (assoc-in query [:where 0 0] (list 'q empty-grouped '$))
            explain (d/explain {:run? true} query @conn)]
        (is (empty? (:result explain)))
        (is (= [0] (mapv :rows (:pre-materialized-subqueries explain))))
        (is (not-any? :actual-size (tree-seq coll? seq (:plan explain))))))))

(deftest grouping-keys-drive-lookups-without-having
  (fixture
    (fn [conn]
      (let [inner '{:find [(sum ?quantity) ?order]
                    :with [?line]
                    :in [$ ?minimum]
                    :where [[?line :line/order ?order]
                            [?line :line/quantity ?quantity]
                            [(> ?quantity ?minimum)]]}
            query (assoc-in query [:where 0]
                            [(list 'q inner '$ 75.0) '[[?total ?ok]]])
            expected [(first expected) ["C3" 3 300.0 3.0]
                      (second expected) (last expected)]
            explain (d/explain {:run? true} query @conn)]
        (testing "renamed grouping keys and literal inputs retain every witness"
          (is (= expected (d/q query @conn)))
          (is (= [5] (mapv :rows (:pre-materialized-subqueries explain))))
          (is (not-any? #{'[?o :order/key ?ok]} (:opt-clauses explain)))
          (is (empty? (:late-clauses explain))))
        (testing "input restrictions intersect the grouped keys"
          (is (= [["C3" 3 300.0 3.0]]
                 (d/q (assoc query :in '[$ [?ok ...]]) @conn [3]))))
        (testing "an empty grouped driver short-circuits the outer plan"
          (let [query (assoc-in query [:where 0 0] (list 'q inner '$ 100000.0))
                explain (d/explain {:run? true} query @conn)]
            (is (empty? (:result explain)))
            (is (= [0] (mapv :rows (:pre-materialized-subqueries explain))))))
        (testing "plan-only explanations leave the producer deferred"
          (is (empty? (:pre-materialized-subqueries (d/explain {} query @conn)))))
        (testing "prepared executions refresh grouped totals after writes"
          (let [read! (d/prepare-q @conn query)]
            (is (= expected (read! [])))
            (d/transact! conn [{:line/order 3 :line/quantity 80.0}])
            (is (= (assoc-in expected [1 2] 380.0) (read! [])))))))))

(deftest plan-only-and-correlated-subqueries-stay-deferred
  (fixture
    (fn [conn]
      (let [explain (d/explain {} query @conn)]
        (is (empty? (:pre-materialized-subqueries explain)))
        (is (some #{(first (:where query))} (:late-clauses explain))))
      (let [inner '[:find ?ok (sum ?q) :with ?l :in $ ?owner
                    :having [(> (sum ?q) 300)]
                    :where [?o :order/customer ?owner] [?o :order/key ?ok]
                    [?l :line/order ?ok] [?l :line/quantity ?q]]
            query (assoc-in query [:where 0]
                    [(list 'q inner '$ '?ck) '[[?ok ?total]]])
            explain (d/explain {:run? true} query @conn)]
        (is (= (set expected) (set (:result explain))))
        (is (empty? (:pre-materialized-subqueries explain)))))))

(deftest named-sources-and-query-literals-retain-their-own-variable-scope
  (fixture
    (fn [conn]
      (let [where (into [[(list 'q grouped '$lines) '[[?ok ?total]]]]
                        (map #(into ['$orders] %)) (next (:where query)))
            query (assoc query :in '[$orders $lines] :where where)
            explain (d/explain {:run? true} query @conn @conn)]
        (is (= (set expected) (set (:result explain))))
        (is (= [4] (mapv :rows (:pre-materialized-subqueries explain))))))))
