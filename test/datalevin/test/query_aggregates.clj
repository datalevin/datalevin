(ns datalevin.test.query-aggregates
  (:require
   [clojure.test :as t :refer        [is are deftest testing]]
   [datalevin.interpret :as i]
   [datalevin.core :as d]
   [datalevin.timeout :as timeout]))

(defn sort-reverse [xs] (reverse (sort xs)))

(deftest test-derived-aggregate-relations
  (let [a [[1 :x] [2 :x] [3 :y]]
        b [[4 :x] [5 :x] [6 :x] [7 :y]]
        c [[8 :x] [9 :y] [10 :y]]]
    (is (= 8
           (d/q '[:find (sum ?w) .
                  :in $a $b $c
                  :where
                  [(q [:find ?x (count ?a)
                       :in $a
                       :where [$a ?a ?x]]
                      $a)
                   [[?x ?wa]]]
                  [(q [:find ?x (count ?b)
                       :in $b
                       :where [$b ?b ?x]]
                      $b)
                   [[?x ?wb]]]
                  [(q [:find ?x (count ?c)
                       :in $c
                       :where [$c ?c ?x]]
                      $c)
                   [[?x ?wc]]]
                  [(* ?wa ?wb ?wc) ?w]]
                a b c)))))

(deftest test-nested-query-cache-invalidation-and-explain
  (let [conn  (d/create-conn nil {} {:kv-opts {:inmemory? true}})
        query '[:find ?n .
                :where
                [(q [:find (count ?e) .
                     :where [?e :test/value]]
                    $)
                 ?n]]]
    (try
      (d/transact! conn [{:db/id 1 :test/value 1}])
      (is (= 1 (d/q query @conn)))

      ;; The containing query must not return its cached result after a nested
      ;; query's database dependencies change.
      (d/transact! conn [{:db/id 2 :test/value 2}])
      (is (= 2 (d/q query @conn)))

      (let [explanation (d/explain {:run? true} query @conn)]
        (is (= 2 (:result explanation)))
        (is (= 1 (:actual-result-size explanation))))
      (finally
        (d/close conn)))))

(deftest test-nested-query-inherits-deadline
  (let [parent-deadline (+ (System/currentTimeMillis) 60000)
        deadline
        (d/q '[:find ?deadline .
               :in ?capture
               :where
               [(q [:find ?deadline .
                    :in ?f
                    :where [(?f) ?deadline]]
                   ?capture)
                ?deadline]
               :timeout 10000]
             (fn [] timeout/*deadline*))]
    (is (integer? deadline))
    (is (<= deadline (+ (System/currentTimeMillis) 10000)))
    (binding [timeout/*deadline* parent-deadline]
      (is (= parent-deadline (timeout/effective-deadline nil)))
      (is (= parent-deadline (timeout/effective-deadline 120000))))))

(deftest test-aggregates
  (let [monsters [ ["Cerberus" 3]
                  ["Medusa" 1]
                  ["Cyclops" 1]
                  ["Chimera" 1] ]]
    (testing "with"
      (is (= (d/q '[:find ?heads
                    :with ?monster
                    :in   [[?monster ?heads]] ]
                  [ ["Medusa" 1]
                   ["Cyclops" 1]
                   ["Chimera" 1] ])
             [[1] [1] [1]])))

    (testing "Wrong grouping without :with"
      (is (= (d/q '[:find (sum ?heads)
                    :in   [[?monster ?heads]] ]
                  monsters)
             [[4]])))

    (testing "aggregate on strings"
      (is (= (d/q '[:find (max ?monster)
                    :in [[?monster ?heads]]]
                  monsters)
             [["Medusa"]])))

    (testing "Multiple aggregates, correct grouping with :with"
      (is (= (d/q '[ :find (sum ?heads) (min ?heads) (max ?heads) (count ?heads) (count-distinct ?heads)
                    :with ?monster
                    :in   [[?monster ?heads]] ]
                  monsters)
             [[6 1 3 4 2]])))

    (testing "Min and max are using comparator instead of default compare"
      ;; Wrong: using js '<' operator
      ;; (apply min [:a/b :a-/b :a/c]) => :a-/b
      ;; (apply max [:a/b :a-/b :a/c]) => :a/c
      ;; Correct: use IComparable interface
      ;; (sort compare [:a/b :a-/b :a/c]) => (:a/b :a/c :a-/b)
      (is (= (d/q '[:find (min ?x) (max ?x)
                    :in [?x ...]]
                  [:a-/b :a/b])
             [[:a/b :a-/b]]))

      (is (= (d/q '[:find (min 2 ?x) (max 2 ?x)
                    :in [?x ...]]
                  [:a/b :a-/b :a/c])
             [[[:a/b :a/c] [:a/c :a-/b]]])))

    (testing "Grouping and parameter passing"
      (is (= (set (d/q '[:find ?color (max ?amount ?x) (min ?amount ?x)
                         :in   [[?color ?x]] ?amount ]
                       [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                        [:blue 7] [:blue 8]]
                       3))
             #{[:red  [3 4 5] [1 2 3]]
               [:blue [7 8]   [7 8]]})))

    (testing "avg aggregate"
      (is (= (ffirst (d/q '[:find (avg ?x)
                            :in [?x ...]]
                          [10 15 20 35 75]))
             31.0)))

    (testing "median aggregate"
      (is (= (ffirst (d/q '[:find (median ?x)
                            :in [?x ...]]
                          [10 15 20 35 75]))
             20)))

    (testing "variance aggregate"
      (is (= (ffirst (d/q '[:find (variance ?x)
                            :in [?x ...]]
                          [10 15 20 35 75]))
             554.0)))

    (testing "stddev aggregate"
      (is (= (ffirst (d/q '[:find (stddev ?x)
                            :in [?x ...]]
                          [10 15 20 35 75]))
             23.53720459187964)))

    (testing "vec aggregate"
      (let [heads (ffirst (d/q '[:find (vec ?heads)
                                 :with ?monster
                                 :in [[?monster ?heads]]]
                               monsters))]
        (is (vector? heads))
        (is (= {3 1, 1 3} (frequencies heads))))
      (let [groups (into {}
                         (d/q '[:find ?color (vec ?x)
                                :in [[?color ?x]]]
                              [[:red 1] [:red 2] [:blue 7]
                               [:red 3] [:blue 8]]))]
        (is (every? vector? (vals groups)))
        (is (= {1 1, 2 1, 3 1} (frequencies (groups :red))))
        (is (= {7 1, 8 1} (frequencies (groups :blue))))))

    (testing "Custom aggregates"
      (let [data   [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                    [:blue 7] [:blue 8]]
            result #{[:red [5 4 3 2 1]] [:blue [8 7]]}]

        (is (= (set (d/q '[ :find ?color (aggregate ?agg ?x)
                           :in   [[?color ?x]] ?agg ]
                         data
                         sort-reverse))
               result))

        (is (= (set
                 (d/q '[ :find ?color (datalevin.test.query-aggregates/sort-reverse ?x)
                        :in   [[?color ?x]]]
                      data))
               result))))))

(deftest inter-fn-test
  (let [monsters       [ ["Cerberus" 3]
                        ["Medusa" 1]
                        ["Cyclops" 1]
                        ["Chimera" 1] ]
        query-fn       #(d/q '[:find (max ?heads) .
                               :in   [[?monster ?heads]] ]
                             monsters)
        inter-query-fn (i/inter-fn []
                                   (d/q '[:find (max ?heads) .
                                          :in   [[?monster ?heads]] ]
                                        monsters))]
    (is (= 3 (query-fn)))
    (is (= 3 (inter-query-fn)))))

(deftest test-find-expr
  (let [data [["Alice" 10 20]
              ["Alice" 5 15]
              ["Bob" 30 40]]]

    (testing "Basic addition of two aggregates"
      (is (= (set (d/q '[:find ?name (+ (sum ?x) (sum ?y))
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 50] ["Bob" 70]})))

    (testing "IC3-style: standalone aggregates and expression"
      (is (= (set (d/q '[:find ?name (sum ?x) (sum ?y) (+ (sum ?x) (sum ?y))
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 15 35 50] ["Bob" 30 40 70]})))

    (testing "Subtraction"
      (is (= (set (d/q '[:find ?name (- (sum ?y) (sum ?x))
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 20] ["Bob" 10]})))

    (testing "Multiplication with constant"
      (is (= (set (d/q '[:find ?name (* (sum ?x) 10)
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 150] ["Bob" 300]})))

    (testing "Division (average-like)"
      (is (= (set (d/q '[:find ?name (/ (sum ?x) (count ?x))
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 15/2] ["Bob" 30]})))

    (testing "Nested expression: (* 2 (+ (sum ?x) (sum ?y)))"
      (is (= (set (d/q '[:find ?name (* 2 (+ (sum ?x) (sum ?y)))
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 100] ["Bob" 140]})))

    (testing "Complex nested: (+ (* (sum ?x) 2) (/ (sum ?y) 2))"
      (is (= (set (d/q '[:find ?name (+ (* (sum ?x) 2) (/ (sum ?y) 2))
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 95/2] ["Bob" 80]})))

    (testing "Modulo operator"
      (is (= (set (d/q '[:find ?name (mod (sum ?x) 7)
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 1] ["Bob" 2]})))))

(deftest test-having
  (let [data [["Alice" 10 20]
              ["Alice" 5 15]
              ["Bob" 30 40]
              ["Charlie" 0 5]
              ["Charlie" 0 10]]]

    (testing "Filter with pos?"
      (is (= (set (d/q '[:find ?name (sum ?x) (sum ?y)
                         :having [(pos? (sum ?x))]
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 15 35] ["Bob" 30 40]})))

    (testing "Filter with > comparison"
      (is (= (set (d/q '[:find ?name (sum ?x)
                         :having [(> (sum ?x) 20)]
                         :in [[?name ?x ?y]]]
                       data))
             #{["Bob" 30]})))

    (testing "Multiple having predicates (AND semantics)"
      (is (= (set (d/q '[:find ?name (sum ?x) (sum ?y)
                         :having [(pos? (sum ?x))]
                                 [(< (sum ?y) 40)]
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 15 35]})))

    (testing "Having with find expression"
      (is (= (set (d/q '[:find ?name (sum ?x) (sum ?y) (+ (sum ?x) (sum ?y))
                         :having [(pos? (sum ?x))]
                                 [(pos? (sum ?y))]
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 15 35 50] ["Bob" 30 40 70]})))

    (testing "Having with >= comparison"
      (is (= (set (d/q '[:find ?name (sum ?x)
                         :having [(>= (sum ?x) 15)]
                         :in [[?name ?x ?y]]]
                       data))
             #{["Alice" 15] ["Bob" 30]})))

    (testing "No having clause returns all groups"
      (is (= (count (d/q '[:find ?name (sum ?x)
                           :in [[?name ?x ?y]]]
                         data))
             3)))))
