(ns datalevin-tpch.datalevin-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin-tpch.common :as c]
            [datalevin-tpch.datalevin :as dt]))

(defn- tbl-row [table overrides]
  (mapv (fn [attr type]
          (str (get overrides attr
                    (case type :long 1 :double 1.0 :string "fixture"))))
        (c/attrs table) (:types (c/table-specs table))))

(deftest bulk-loader-constructs-disjoint-entities-and-target-references
  (let [overrides {"region" [{:region/regionkey 0} {:region/regionkey 4}]
                   "nation" [{:nation/nationkey 10 :nation/regionkey 0}]
                   "part" [{:part/partkey 1}]
                   "supplier" [{:supplier/suppkey 1 :supplier/nationkey 10}]
                   "partsupp" [{}]
                   "customer" [{:customer/custkey 1 :customer/nationkey 10}]
                   ;; Sparse keys, deliberately out of order.
                   "orders" [{:orders/orderkey 1000000} {:orders/orderkey 1}]
                   "lineitem" [{:lineitem/orderkey 1000000} {:lineitem/orderkey 1}]}
        rows (into {} (for [[table maps] overrides]
                        [table (mapv #(tbl-row table %) maps)]))
        read-rows (fn [table f] (f (rows table)))]
    (with-open [conn (d/create-conn nil dt/schema {:closed-schema? true
                                                 :background-sampling? false})]
      (let [layout (dt/load-data! @conn read-rows)
            eid #(dt/entity-id layout %1 %2)
            entities (mapcat (fn [table]
                               (map :e (d/datoms @conn :ave
                                                (dt/attribute (first (c/attrs table))))))
                             c/table-order)]
        (is (= 11 (count entities) (count (set entities))))
        (is (every? pos? entities))
        (is (= (eid "region" 0) (d/entid @conn [:region/regionkey 0])))
        (is (= (eid "orders" 1000000) (d/entid @conn [:orders/orderkey 1000000])))
        (is (= (eid "orders" 1) (d/entid @conn [:orders/orderkey 1])))
        (doseq [[column [ref target]] dt/references
                :let [table (namespace column)
                      target-key (first (c/attrs target))
                      position (.indexOf ^java.util.List (c/attrs table) column)]]
          (is (= (count (rows table)) (d/count-datoms @conn nil ref nil)))
          (is (nil? ((d/schema conn) column)) "numeric foreign-key copies are absent")
          (is (= (set (map #(Long/parseLong (nth % position)) (rows table)))
                 (set (d/q '[:find [?key ...] :in $ ?ref ?key-attr
                             :where [?e ?ref ?target] [?target ?key-attr ?key]]
                           @conn ref target-key)))))
        (is (= (eid "partsupp" 1)
               (d/entid @conn [:partsupp/part+supplier [(eid "part" 1) (eid "supplier" 1)]])))
        (is (= (eid "lineitem" 1)
               (d/entid @conn [:lineitem/order+linenumber [(eid "orders" 1000000) 1]])))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"outside allocated"
                             (dt/entity-id layout "region" 5)))
        (is (nil? (dt/require-reference-schema! conn)))))))

(deftest composite-primary-keys-survive-bulk-load-and-transactions
  (doseq [[table key-attr a b]
          [["partsupp" :partsupp/part+supplier :partsupp/part :partsupp/supplier]
           ["lineitem" :lineitem/order+linenumber :lineitem/order :lineitem/linenumber]]]
    (testing table
      (with-open [conn (d/create-conn nil dt/schema {:background-sampling? false})]
        (d/fill-db @conn
                   (mapcat (fn [e [av bv]]
                             [(d/datom e a av) (d/datom e b bv)
                              (d/datom e key-attr [av bv])])
                           [101 102 103] [[1 1] [1 2] [2 1]]))
        (is (= [101 102 103]
               (mapv #(d/entid @conn [key-attr %]) [[1 1] [1 2] [2 1]])))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unique constraint"
                             (d/transact! conn [{:db/id 104 a 1 b 1}])))
        (is (empty? (d/datoms @conn :eav 104)))
        (d/transact! conn [[:db/add 101 b 3]])
        (is (nil? (d/entid @conn [key-attr [1 1]])))
        (is (= 101 (d/entid @conn [key-attr [1 3]])))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unique constraint"
                             (d/transact! conn [[:db/add 101 b 2]])))
        (is (= 101 (d/entid @conn [key-attr [1 3]])))))))

(deftest old-scalar-schema-requires-rebuild
  (with-open [conn (d/create-conn nil {:region/regionkey {:db/valueType :db.type/long}})]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"rebuild"
                         (dt/require-reference-schema! conn)))))

(deftest composite-entity-ids-survive-map-growth
  (let [n 2000
        layout {"part" {:base 1 :maximum n}
                "supplier" {:base 3000 :maximum 1}
                "partsupp" {:base 4000 :maximum n :rows n}}
        rows (mapv #(tbl-row "partsupp"
                            {:partsupp/partkey %
                             :partsupp/comment (apply str (repeat 4096 "x"))})
                   (range 1 (inc n)))]
    (with-open [conn (d/create-conn nil dt/schema
                                    {:kv-opts {:mapsize 1}
                                     :background-sampling? false})]
      (#'dt/load-table! @conn layout (fn [_ f] (f rows)) "partsupp")
      (is (= n (d/count-datoms @conn nil :partsupp/part nil)))
      (is (= (set (range 4001 (+ 4001 n)))
             (set (d/q '[:find [?e ...] :where [?e :partsupp/part]] @conn))))
      (is (= 4001 (d/entid @conn [:partsupp/part+supplier [2 3001]])))
      (is (= (+ 4000 n) (d/entid @conn [:partsupp/part+supplier [(inc n) 3001]]))))))
