(ns datalevin.query.existence-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query :as query]
            [datalevin.query.optimizer.existence :as existence]
            [datalevin.util :as u]))

(def query
  '[:find ?supplier (count-distinct ?line)
    :where
    [?s :supplier/key ?supplier]
    [?line :line/supplier ?supplier]
    [?line :line/order ?order]
    [?line :line/receipt ?receipt]
    [?line :line/commit ?commit]
    [(> ?receipt ?commit)]
    [?o :order/key ?order]
    [?o :order/status "F"]
    (not [?other :line/order ?order]
         [?other :line/supplier ?other-supplier]
         [(not= ?other-supplier ?supplier)]
         [?other :line/receipt ?other-receipt]
         [?other :line/commit ?other-commit]
         [(> ?other-receipt ?other-commit)])
    [?witness :line/order ?order]
    [?witness :line/supplier ?witness-supplier]
    [(not= ?witness-supplier ?supplier)]])

(defn- fixture [f]
  (let [dir (u/tmp-dir (str "existence-" (random-uuid)))
        conn (d/create-conn dir)]
    (try
      (d/transact! conn
        (concat
          [{:supplier/key 1} {:supplier/key 2} {:supplier/key 3}]
          (for [order (range 1 6)]
            {:order/key order :order/status (if (= order 5) "O" "F")})
          (map (fn [[order supplier receipt]]
                 {:line/order order :line/supplier supplier
                  :line/receipt receipt :line/commit 10})
               [[1 1 20] [1 2 5] [1 2 6] [1 3 5]
                [2 1 20] [2 2 20]
                [3 1 20] [3 1 25]
                [4 1 5] [4 2 5]
                [5 1 20] [5 2 5]])))
      (f @conn)
      (finally (d/close conn) (u/delete-files dir)))))

(deftest keyed-existence-preserves-results-and-filters-before-fanout
  (fixture
    (fn [db]
      (is (= #{[1 1]} (set (d/q query db))))
      (let [rewritten (existence/rewrite
                        {:parsed-q (parser/parse-query query)
                         :sources {'$ db} :rels []})
            forms (get-in rewritten [:parsed-q :qorig-where])
            plan (d/explain {:run? true} query db)]
        (is (= 2 (count (filter #(and (seq? %) (= 'not-join (first %))) forms))))
        (is (not-any? #(and (vector? %) (= '?witness (first %))) forms))
        (is (empty? (:late-clauses plan)))))))

(deftest witness-bindings-remain-visible-when-requested
  (fixture
    (fn [db]
      (let [forms (:qorig-where (parser/parse-query query))
            counted (into '[:find (count ?witness) . :where] forms)
            with-witness (into '[:find (count ?line) . :with ?witness :where] forms)
            projected (into '[:find ?witness :where] forms)]
        (is (= 3 (d/q counted db)))
        (is (= 3 (d/q with-witness db)))
        (is (= 3 (count (d/q projected db))))))))

(deftest witness-comparisons-can-precede-their-patterns
  (fixture
    (fn [db]
      (let [forms (:qorig-where (parser/parse-query query))
            reordered (into '[:find ?supplier (count-distinct ?line) :where]
                             (concat (drop-last 3 forms) [(last forms)]
                                     (take-last 2 (butlast forms))))
            facts (mapv (fn [datom] [(:e datom) (:a datom) (:v datom)])
                        (d/datoms db :eav))]
        (is (= #{[1 1]} (set (d/q reordered db))))
        (is (= (set (d/q reordered facts)) (set (d/q reordered db))))))))

(deftest comparisons-requiring-other-clauses-stay-late
  (is (not (existence/simple-predicate? '[(> ?x (+ ?y 1))])))
  (is (not (existence/simple-predicate? '[(?pred ?x)])))
  (is (not (existence/simple-predicate? '[(contains? #{1 2} ?x)]))))

(def priority-query
  '[:find ?priority (count-distinct ?order)
    :where
    [?order :order/key ?key]
    [?order :order/priority ?priority]
    [?line :line/order ?key]
    [?line :line/commit ?commit]
    [?line :line/receipt ?receipt]
    [(< ?commit ?receipt)]])

(deftest local-field-comparisons-become-existence-conditions
  (doseq [many? [false true]]
    (let [dir (u/tmp-dir (str "local-witness-" (random-uuid)))
          conn (d/create-conn
                 dir {:order/key {:db/valueType :db.type/long}
                      :order/priority {:db/valueType :db.type/string}
                      :order/region {:db/valueType :db.type/string}
                      :line/order {:db/valueType :db.type/long}
                      :line/region {:db/valueType :db.type/string}
                      :line/commit (cond-> {:db/valueType :db.type/long}
                                     many? (assoc :db/cardinality :db.cardinality/many))
                      :line/receipt {:db/valueType :db.type/long}})]
      (try
        (d/transact! conn
          (concat
            (for [key (range 1 7)]
              {:db/id key :order/key key :order/region "A"
               :order/priority (if (< key 4) "A" "B")})
            ;; Sharing a correlation key must retain both outer entities.
            [{:db/id 7 :order/key 1 :order/priority "A" :order/region "B"}]
            (map #(assoc % :line/region "A")
              [{:db/id 101 :line/order 1 :line/commit 1 :line/receipt 3}
               {:db/id 102 :line/order 1 :line/commit 2 :line/receipt 3}
               {:db/id 103 :line/order 2 :line/commit 3 :line/receipt 3}
               {:db/id 104 :line/order 3 :line/commit 1} ; missing receipt
               {:db/id 105 :line/order 4 :line/receipt 3} ; missing commit
               {:db/id 106 :line/order 5 :line/commit 4 :line/receipt 3}
               {:db/id 107 :line/order 5 :line/commit 1 :line/receipt 3}])
            ;; Make orders the selective side, as in Q4's date range.
            (for [id (range 200 400)]
              {:db/id id :line/order 999 :line/region "A"
               :line/commit 1 :line/receipt 3})))
        (when many? (d/transact! conn [[:db/add 101 :line/commit 10]]))
        (let [database @conn
              forms (:qorig-where (parser/parse-query priority-query))
              facts (mapv (juxt :e :a :v) (d/datoms database :eav))
              plan (d/explain {:run? true} priority-query database)]
          (is (= #{["A" 2] ["B" 1]} (set (d/q priority-query database))))
          (is (= 3 (:actual-result-size plan)))
          (is (some #{"Semi-join by indexed link scan."}
                    (mapcat :steps (get-in plan [:plan '$ 0]))))
          (is (empty? (:late-clauses plan)))
          (let [two-keys (into priority-query
                              '[[?order :order/region ?region]
                                [?line :line/region ?region]])
                explanation (d/explain {:run? true} two-keys database)]
            (is (= #{["A" 1] ["B" 1]} (set (:result explanation))))
            (is (some #{"Semi-join by indexed link scan."}
                      (mapcat :steps (get-in explanation [:plan '$ 0])))))
          (binding [query/*cache?* false
                    c/init-exec-size-threshold 1
                    c/query-partition-min-input-size 1
                    c/query-partition-target-size 2]
            (doseq [parallel? [false true]]
              (is (= #{["A" 2] ["B" 1]}
                     (binding [c/query-partitioned-execution? parallel?]
                       (set (d/q priority-query database))))))
            (when (< 1 (.availableProcessors (Runtime/getRuntime)))
              (let [explain (binding [c/query-partitioned-execution? true]
                              (d/explain {:run? true} priority-query database))]
                (is (some #(some #{:semi-join} (:partitioned-step-types %))
                          (:partitioned-execution explain))))))
          ;; Keep projected and :with witnesses, input bindings, and predicates
          ;; before their patterns semantically identical to the fact source.
          (doseq [query [(into '[:find ?priority (count ?line) :where] forms)
                        (into '[:find ?priority (count ?order) :with ?line :where] forms)
                        (into '[:find ?priority ?commit :where] forms)
                        (into '[:find ?priority (count ?order) :with ?commit :where] forms)
                        (conj priority-query '[(not= ?commit ?receipt)])
                        (into '[:find ?priority (count-distinct ?order)
                                :in $ ?line :where] forms)
                        (into '[:find ?priority (count-distinct ?order) :where]
                              (cons (last forms) (butlast forms)))]]
            (let [inputs (when (some #{:in} query) [102])]
              (is (= (set (apply d/q query facts inputs))
                     (set (apply d/q query database inputs))))))
          (let [new-line {:line/order 2 :line/commit 1 :line/receipt 3}
                pending (:db-after (d/with database [new-line] {} true))]
            (is (= #{["A" 3] ["B" 1]} (set (d/q priority-query pending))))
            (is (= #{["A" 2] ["B" 1]} (set (d/q priority-query database))))
            (d/with-transaction [tx conn]
              (d/transact! tx [new-line])
              (is (= #{["A" 3] ["B" 1]} (set (d/q priority-query @tx)))))))
        (finally (d/close conn) (u/delete-files dir))))))

(deftest constant-range-witnesses-retain-their-scan-plan
  (fixture
    (fn [database]
      (let [query '[:find ?order
                    :where [?o :order/key ?order]
                    [?line :line/order ?order]
                    [?line :line/receipt ?receipt]
                    [(> ?receipt 10)]]
            parsed (parser/parse-query query)
            rewritten (existence/rewrite {:parsed-q parsed :sources {'$ database}
                                         :rels []})]
        (is (= (:qorig-where parsed)
               (get-in rewritten [:parsed-q :qorig-where])))))))
