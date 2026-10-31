(ns datalevin.query.implied-filter-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.query.optimizer.range]
            [datalevin.util :as u]))

(def country-filter
  '[(or (and (= ?supp-nation "FRANCE") (= ?cust-nation "GERMANY"))
        (and (= ?supp-nation "GERMANY") (= ?cust-nation "FRANCE")))])

(def shipping-query
  {:find ['?supp-nation '?cust-nation '(sum ?amount)]
   :with ['?l]
   :where ['[?s :supplier/key ?sk]
           '[?s :supplier/nation ?sn]
           '[?n1 :nation/key ?sn]
           '[?n1 :nation/name ?supp-nation]
           '[?l :line/supplier ?sk]
           '[?l :line/order ?ok]
           '[?o :order/key ?ok]
           '[?o :order/customer ?ck]
           '[?c :customer/key ?ck]
           '[?c :customer/nation ?cn]
           '[?n2 :nation/key ?cn]
           '[?n2 :nation/name ?cust-nation]
           '[?l :line/amount ?amount]
           country-filter]})

(deftest country-restrictions-precede-dimension-joins
  (let [dir (u/tmp-dir (str "implied-country-" (random-uuid)))
        schema (into {:nation/name {:db/valueType :db.type/string}}
                     (for [a [:nation/key :supplier/key :supplier/nation
                              :customer/key :customer/nation :order/key
                              :order/customer :line/supplier :line/order
                              :line/amount]]
                       [a {:db/valueType :db.type/long}]))
        conn (d/create-conn dir schema)]
    (try
      (d/transact! conn
        (concat
          (map-indexed (fn [n country]
                         {:nation/key n :nation/name country})
                       ["FRANCE" "GERMANY" "SPAIN"])
          (for [n (range 3)] {:supplier/key n :supplier/nation n})
          (for [n (range 3)] {:customer/key n :customer/nation n})
          (for [n (range 3)] {:order/key n :order/customer n})
          (for [s (range 3) c (range 3) _ (range 2)]
            {:line/supplier s :line/order c :line/amount 10})))
      (testing "the correlated condition and duplicate aggregate witnesses survive"
        (is (= #{["FRANCE" "GERMANY" 20] ["GERMANY" "FRANCE" 20]}
               (set (d/q shipping-query @conn)))))
      (let [explain (d/explain {:run? true} shipping-query @conn)
            graph (:query-graph explain)]
        (testing "each nation scan gets its own restriction"
          (doseq [e ['?n1 '?n2]]
            (is (some #(and (= :nation/name (:attr %)) (:pred %))
                      (get-in graph ['$ e :free])))))
        (testing "the original predicate still enforces the allowed pairs"
          (is (some #{country-filter} (:late-clauses explain)))))
      (testing "scalar inputs can specialize the same filter"
        (let [query (-> shipping-query
                        (assoc :in '[$ ?france ?germany])
                        (assoc-in [:where 13]
                          '[(or (and (= ?supp-nation ?france)
                                     (= ?cust-nation ?germany))
                                (and (= ?supp-nation ?germany)
                                     (= ?cust-nation ?france)))]))]
          (is (= #{["FRANCE" "GERMANY" 20] ["GERMANY" "FRANCE" 20]}
                 (set (d/q query @conn "FRANCE" "GERMANY"))))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest inferred-filters-preserve-boolean-and-equality-semantics
  (let [dir (u/tmp-dir (str "implied-filter-parity-" (random-uuid)))
        conn (d/create-conn dir)
        values ["FRANCE" "GERMANY" "SPAIN" false true 1 1.0 1M
                (byte-array [1 2]) (byte-array [3 4])]
        rows (vec (map-indexed (fn [n [x y]] [(inc n) x y])
                              (for [x values y values] [x y])))
        predicates
        '[(or (and (= ?x "FRANCE") (= ?y "GERMANY"))
              (and (= "GERMANY" ?x) (= "FRANCE" ?y)))
          (or (and (= ?x "FRANCE") (= ?y "GERMANY")) (= ?y "SPAIN"))
          (or (and (= ?x "FRANCE") (= ?y "GERMANY")) (= ?x ?y))
          (or (and (= ?x "FRANCE") (= ?y "GERMANY")) true)
          (and (or (= ?x "FRANCE") (= ?x "GERMANY")) (= ?y "SPAIN"))
          (and (= ?x false) (= ?y true))
          (or (and (= ?x nil) (= ?y "FRANCE"))
              (and (= ?x false) (= ?y "GERMANY")))
          (or (and (= ?x false) (= ?y true)) (and (= ?x true) (= ?y false)))
          (or (and (= ?x 1) (= ?y 1.0)) (and (= ?x 1M) (= ?y 1M)))]
        predicates (conj predicates
                     (list 'or
                       (list 'and (list '= '?x (byte-array [1 2]))
                             '(= ?y "FRANCE"))
                       (list 'and (list '= '?x (byte-array [3 4]))
                             '(= ?y "GERMANY"))))]
    (try
      (d/transact! conn (map (fn [[e x y]] {:db/id e :row/x x :row/y y}) rows))
      (doseq [pred predicates]
        (let [query {:find ['?e]
                     :where ['[?e :row/x ?x] '[?e :row/y ?y] [pred]]}
              reference {:find ['?e] :in '[[[?e ?x ?y]]]
                         :where [[pred]]}]
          (is (= (d/q reference rows) (d/q query @conn)) (pr-str pred))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest inference-requires-a-restriction-in-every-disjunct
  (let [infer @(ns-resolve 'datalevin.query.optimizer.range 'implied-equalities)]
    (is (= #{'?y}
           (set (keys (infer '(or (and (= ?x "A") (= ?y "B"))
                                  (= ?y "C")))))))
    (doseq [form '[(or (= ?x "A") (= ?y "B"))
                  (or (= ?x "A") true)
                  (not (= ?x "A"))
                  (= ?x ?y)
                  (= ?x (identity "A"))
                  (?predicate ?x ?y)
                  (custom/or (= ?x "A") (= ?x "B"))]]
      (is (empty? (infer form)) (pr-str form)))))
