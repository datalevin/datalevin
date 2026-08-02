(ns datalevin-bench.cardinality-oracle-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin-bench.cardinality-oracle :as oracle]
   [datalevin.query-util :as qu]))

(deftest placeholder-entities-get-countable-query-variables
  (let [analysis (oracle/query-analysis
                   '[:find ?name
                     :where
                     [?an :aka-name/name ?name]])
        entity   (first (:entities analysis))
        query-var (get-in analysis [:entity-query-vars entity])
        form     (oracle/subset-count-form analysis #{entity})]
    (is (qu/placeholder? entity))
    (is (qu/free-var? query-var))
    (is (not= entity query-var))
    (is (some #{query-var} (tree-seq coll? seq form)))))
