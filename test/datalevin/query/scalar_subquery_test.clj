(ns datalevin.query.scalar-subquery-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.query :as query]))

(def average
  '[:find (avg ?balance) . :with ?customer
    :where [?customer :customer/balance ?balance] [(> ?balance 0.0)]
    [?customer :customer/phone ?phone]
    [(subs ?phone 0 2) ?country]
    [(or (= ?country "13") (= ?country "17"))]])

(def query
  {:find ['?country '(count ?customer) '(sum ?balance)]
   :order-by [[0 :asc]]
   :where ['[?customer :customer/key ?key]
           '[?customer :customer/phone ?phone]
           '[(subs ?phone 0 2) ?country]
           '[(or (= ?country "13") (= ?country "17"))]
           '[?customer :customer/balance ?balance]
           [(list 'q average '$) '?average]
           '[(> ?balance ?average)]
           '(not [?order :order/customer ?key])]})

(defn- fixture [f]
  (with-open [conn (d/create-conn nil
                    {:customer/key {:db/valueType :db.type/long}
                     :customer/balance {:db/valueType :db.type/double}
                     :customer/phone {:db/valueType :db.type/string}
                     :order/customer {:db/valueType :db.type/long}}
                    {:background-sampling? false})]
    (d/transact! conn
      [{:db/id 1 :customer/key 1 :customer/phone "13-a" :customer/balance 10.0}
       {:db/id 2 :customer/key 2 :customer/phone "13-b" :customer/balance 10.0}
       {:db/id 3 :customer/key 3 :customer/phone "17-a" :customer/balance 40.0}
       {:db/id 4 :customer/key 4 :customer/phone "17-b" :customer/balance 80.0}
       {:db/id 5 :customer/key 5 :customer/phone "99-a" :customer/balance 1000.0}
       {:db/id 6 :customer/key 6 :customer/phone "13-c" :customer/balance -10.0}
       {:order/customer 4}])
    (f conn)))

(deftest independent-average-bounds-the-initial-scan
  (fixture
    (fn [conn]
      (let [explain (d/explain {:run? true} query @conn)
            bounds (get-in explain [:query-graph '$ '?customer :free])]
        (is (= [["17" 1 40.0]] (mapv vec (:result explain))))
        (is (= [1] (mapv :rows (:pre-materialized-subqueries explain))))
        (is (some #(and (= :customer/balance (:attr %))
                        (= [[[:open 35.0] [:closed :db.value/sysMax]]] (:range %)))
                  bounds))
        (is (not-any? #{'[(> ?balance ?average)]} (:late-clauses explain))))
      (testing "the scalar remains available in projections"
        (is (= [["17" 35.0]]
               (mapv vec (d/q (assoc query :find '[?country ?average]) @conn)))))
      (testing "a prepared query recomputes the average after writes"
        (let [read! (d/prepare-q @conn query)]
          (is (= [["17" 1 40.0]] (mapv vec (read! []))))
          (d/transact! conn [{:db/id 1 :customer/balance 110.0}])
          (is (= [["13" 1 110.0]] (mapv vec (read! [])))))))))

(deftest empty-and-correlated-averages-preserve-their-bindings
  (fixture
    (fn [conn]
      (let [empty-average (assoc average 7 '[(> ?balance 2000.0)])
            empty-query (assoc-in query [:where 5 0] (list 'q empty-average '$))
            explain (d/explain {:run? true} empty-query @conn)]
        (is (empty? (:result explain)))
        (is (= [0] (mapv :rows (:pre-materialized-subqueries explain)))))
      (testing "plan-only calls do not execute the subquery"
        (let [explain (d/explain {} query @conn)]
          (is (empty? (:pre-materialized-subqueries explain)))
          (is (some #{(get-in query [:where 5])} (:late-clauses explain)))))
      (testing "correlation through an outer tuple remains deferred"
        (let [inner '[:find (avg ?b) . :with ?c :in $ ?prefix
                      :where [?c :customer/balance ?b]
                      [?c :customer/phone ?p]
                      [(subs ?p 0 2) ?code] [(= ?code ?prefix)]]
              correlated (assoc-in query [:where 5 0] (list 'q inner '$ '?country))
              explain (d/explain {:run? true} correlated @conn)]
          (is (= [["13" 2 20.0]] (mapv vec (:result explain))))
          (is (empty? (:pre-materialized-subqueries explain))))))))

(deftest scalar-range-specialization-preserves-integer-average-boundaries
  (with-open [conn (d/create-conn nil {:n {:db/valueType :db.type/long}}
                                {:background-sampling? false})]
    (d/transact! conn [{:n 1} {:n 2} {:n 4}])
    (doseq [op '[< <= > >=]
            :let [inner '[:find (avg ?x) . :where [?e :n ?x]]
                  form {:find ['?n] :where ['[?e :n ?n]
                                            [(list 'q inner '$) '?mean]
                                            [(list op '?n '?mean)]]}
                  expected (into #{} (filter #((resolve op) (first %) (/ 7.0 3)))
                                 [[1] [2] [4]])]]
      (binding [query/*cache?* false]
        (is (= expected (d/q form @conn)) (str op))))))
