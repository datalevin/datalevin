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

(deftest link-input-count-excludes-target-local-filters
  (let [analysis (oracle/query-analysis
                   '[:find ?title
                     :where
                     [?t :title/title ?title]
                     [?mi :movie-info/movie ?t]
                     [?mi :movie-info/info ?info]
                     [(= ?info "rating")]])
        request  {:kind :link-input
                  :entities '#{?t}
                  :link-e '?t
                  :target '?mi
                  :type :_ref
                  :attr :movie-info/movie
                  :var nil
                  :attrs nil}
        form     (oracle/link-input-count-form analysis request)
        clauses  (set (drop (inc (.indexOf form :where)) form))]
    (is (contains? clauses '[?t :title/title ?title]))
    (is (contains? clauses '[?mi :movie-info/movie ?t]))
    (is (not (contains? clauses '[?mi :movie-info/info ?info])))
    (is (not (contains? clauses '[(= ?info "rating")])))
    (is (= request (oracle/material-request-key request)))
    (is (= (oracle/material-query-key analysis request)
           (oracle/material-query-key analysis (assoc request :attrs {}))))))
