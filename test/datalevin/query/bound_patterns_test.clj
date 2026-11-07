(ns datalevin.query.bound-patterns-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.query :as query]
            [datalevin.test.core :refer [db-fixture]]))

(use-fixtures :each db-fixture)

(def ^:private stock-query
  '[:find (count-distinct ?item) .
    :in $ ?warehouse [?order ...] ?threshold
    :where [?line :line/order ?order] [?line :line/stock ?supply]
    [?supply :stock/item ?item] [?stock :stock/item ?item]
    [?stock :stock/warehouse ?warehouse] [?stock :stock/quantity ?quantity]
    [(< ?quantity ?threshold)]])

(deftest bound-reference-chain
  (with-open [conn (d/create-conn
                    nil {:order/id {:db/unique :db.unique/identity}
                         :line/order {:db/valueType :db.type/ref}
                         :line/stock {:db/valueType :db.type/ref}
                         :stock/item {:db/valueType :db.type/ref}
                         :stock/warehouse {:db/valueType :db.type/ref}
                         :stock/quantity {:db/valueType :db.type/long}}
                    {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :order/id "first"}
                      {:db/id 2 :order/id "second"}
                      {:db/id 10 :stock/item 100 :stock/warehouse 200 :stock/quantity 5}
                      {:db/id 11 :stock/item 100 :stock/warehouse 201 :stock/quantity 50}
                      {:db/id 12 :stock/item 101 :stock/warehouse 200 :stock/quantity 50}
                      {:db/id 13 :stock/item 101 :stock/warehouse 201 :stock/quantity 5}
                      {:db/id 20 :line/order 1 :line/stock 11}
                      {:db/id 21 :line/order 1 :line/stock 11}
                      {:db/id 22 :line/order 2 :line/stock 13}
                      {:db/id 23 :line/order 2}])
    (binding [query/*cache?* false]
      (testing "remote supply uses home quantity; repeated lines count once"
        (is (= 1 (d/q stock-query @conn 200 [1 1 2 999] 10)))
        (is (nil? (d/q stock-query @conn 200 [2] 10)))
        (is (nil? (d/q stock-query @conn 200 [] 10)))
        (let [plan (d/explain {:run? true} stock-query @conn 200 [1 2] 10)
              stages (:input-bound-eav-groups plan)]
          (is (= 1 (:result plan)))
          (is (= 2 (count (filter #(= :bound-ave (:strategy %)) stages))))
          (is (= 3 (count (filter #(= :grouped-bound-eav (:strategy %)) stages))))))
      (testing "lookup references keep their input representation"
        (is (= 1 (d/q stock-query @conn 200 [[:order/id "first"]] 10))))
      (testing "pending data follows the merged database view"
        (let [pending (d/db-with @conn [[:db/add 12 :stock/quantity 4]])]
          (is (= 2 (d/q stock-query pending 200 [1 2] 10))))))))

(deftest bound-scans-retain-correlations-and-many-values
  (with-open [conn (d/create-conn
                    nil {:edge/to {:db/valueType :db.type/ref
                                   :db/cardinality :db.cardinality/many}
                         :node/tag {:db/valueType :db.type/keyword
                                    :db/cardinality :db.cardinality/many}}
                    {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :edge/to [10 11]}
                      {:db/id 2 :edge/to 10}
                      {:db/id 10 :node/tag [:a :b]}
                      {:db/id 11 :node/tag :c}])
    (is (= #{[:x 10 1 :a] [:x 10 1 :b] [:x 10 2 :a] [:x 10 2 :b]
             [:y 11 1 :c]}
           (d/q '[:find ?label ?target ?source ?tag
                  :in $ [[?label ?target]]
                  :where [?source :edge/to ?target] [?target :node/tag ?tag]]
                @conn [[:x 10] [:y 11] [:z 999]])))
    (is (= #{[1 10 :a] [1 10 :b] [1 11 :c] [2 10 :a] [2 10 :b]}
           (d/q '[:find ?source ?target ?tag
                  :in $ [?source ...]
                  :where [?source :edge/to ?target] [?target :node/tag ?tag]]
                @conn [1 2])))
    (testing "fan-out cannot inherit uniqueness of the input key"
      (is (= 3 (d/q '[:find (count ?target) . :with ?source
                      :in $ [?target ...] :where [?source :edge/to ?target]]
                    @conn [10 11])))
      (is (= 5 (d/q '[:find (count ?tag) . :with ?source ?target
                      :in $ [?source ...]
                      :where [?source :edge/to ?target] [?target :node/tag ?tag]]
                    @conn [1 2]))))
    (testing "reverse ref probes retain representation-sensitive key equality"
      (is (= #{} (d/q '[:find ?source ?target :in $ [?target ...]
                         :where [?source :edge/to ?target]]
                       @conn [10N]))))))
