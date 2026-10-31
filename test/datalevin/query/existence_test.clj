(ns datalevin.query.existence-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
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
