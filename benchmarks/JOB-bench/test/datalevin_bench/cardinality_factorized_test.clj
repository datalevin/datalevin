(ns datalevin-bench.cardinality-factorized-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin-bench.cardinality-factorized :as factorized]
   [datalevin-bench.cardinality-oracle :as oracle]
   [datalevin-bench.core :as job]
   [datalevin.core :as d]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(def chain-query
  '[:find ?a ?b ?c
    :where
    [?a :a/x ?x]
    [?a :a/keep true]
    [?b :b/x ?x]
    [?b :b/y ?y]
    [?c :c/y ?y]
    [?c :c/keep true]])

(def chain-schema
  {:a/x    {:db/valueType :db.type/long}
   :a/keep {:db/valueType :db.type/boolean}
   :b/x    {:db/valueType :db.type/long}
   :b/y    {:db/valueType :db.type/long}
   :c/y    {:db/valueType :db.type/long}
   :c/keep {:db/valueType :db.type/boolean}})

(deftest exact-tree-sum-product
  (let [dir  (u/tmp-dir (str "factorized-oracle-" (UUID/randomUUID)))
        conn (d/get-conn dir chain-schema)]
    (try
      (d/transact!
        conn
        [{:db/id -1 :a/x 1 :a/keep true}
         {:db/id -2 :a/x 1 :a/keep true}
         {:db/id -3 :a/x 2 :a/keep false}
         {:db/id -4 :b/x 1 :b/y 10}
         {:db/id -5 :b/x 1 :b/y 20}
         {:db/id -6 :b/x 2 :b/y 10}
         {:db/id -7 :c/y 10 :c/keep true}
         {:db/id -8 :c/y 10 :c/keep true}
         {:db/id -9 :c/y 20 :c/keep true}
         {:db/id -10 :c/y 20 :c/keep false}])
      (let [db       (d/db conn)
            analysis (oracle/query-analysis chain-query)
            backend  (factorized/make-backend db analysis)]
        (testing "the full chain preserves weighted join multiplicity"
          (is (= 6 (factorized/factorized-subset-count
                     backend '#{?a ?b ?c} 10000))))
        (testing "connected proper subsets are exact"
          (doseq [entities ['#{?a} '#{?b} '#{?c}
                            '#{?a ?b} '#{?b ?c}]]
            (is (= (oracle/exact-subset-count db analysis entities 10000)
                   (factorized/factorized-subset-count
                     backend entities 10000)))))
        (testing "an indexed-link input excludes the target's local filters"
          (let [request {:kind :link-input
                         :entities '#{?a ?b}
                         :link-e '?b
                         :target '?c
                         :type :val-eq
                         :attr nil
                         :var '?y
                         :attrs {'?b :b/y '?c :c/y}}]
            (is (= (oracle/exact-link-input-count
                     db analysis request 10000 nil)
                   (factorized/factorized-link-input-count
                     backend request 10000)))
            (is (pos? (:point-count-probes
                        (factorized/backend-stats backend))))))
        (testing "messages are reused across related subset counts"
          (is (pos? (:message-cache-hits
                      (factorized/backend-stats backend))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest rejects-cyclic-factor-graphs
  (let [analysis
        (oracle/query-analysis
          '[:find ?a
            :where
            [?a :a/x ?x]
            [?a :a/z ?z]
            [?b :b/x ?x]
            [?b :b/y ?y]
            [?c :c/y ?y]
            [?c :c/z ?z]])]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"acyclic"
          (factorized/subset-problem analysis '#{?a ?b ?c})))))

(deftest job-factor-graphs-are-acyclic
  (doseq [query-sym job/queries
          :let [analysis (oracle/query-analysis
                           (oracle/query-value query-sym))]]
    (is (map? (factorized/subset-problem analysis (:entities analysis)))
        (oracle/query-name query-sym))))
